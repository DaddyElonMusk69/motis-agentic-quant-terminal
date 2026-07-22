from __future__ import annotations

from dataclasses import dataclass
import math
from typing import Mapping

import numpy as np
import torch
from torch import nn
from torch.nn import functional as F

from quant_terminal_worker.signal_discovery.supervised_training_data import (
    TransformedTimeline,
)


MODEL_SCHEMA_VERSION = "motis_supervised_multires_tcn.v1"


@dataclass(frozen=True)
class SequenceChunk:
    decision_ns: np.ndarray
    targets: np.ndarray
    branch_values: dict[str, np.ndarray]
    gather_indices: dict[str, np.ndarray]

    @property
    def size(self) -> int:
        return len(self.targets)


def build_sequence_chunk(
    *,
    decision_ns: np.ndarray,
    targets: np.ndarray,
    timelines: Mapping[str, TransformedTimeline],
) -> SequenceChunk:
    decisions = np.asarray(decision_ns, dtype=np.int64)
    labels = np.asarray(targets, dtype=np.float32)
    if decisions.ndim != 1 or labels.ndim != 1 or len(decisions) != len(labels):
        raise ValueError("chunk decisions and targets must be aligned vectors")
    if not len(decisions):
        raise ValueError("sequence chunks cannot be empty")
    if np.any(np.diff(decisions) < 0):
        raise ValueError("sequence chunk decisions must be chronological")

    branch_values: dict[str, np.ndarray] = {}
    gather_indices: dict[str, np.ndarray] = {}
    for name, timeline in timelines.items():
        latest = np.searchsorted(timeline.available_ns, decisions, side="right") - 1
        first = int(latest[0] - timeline.spec.steps + 1)
        last = int(latest[-1])
        if first < 0:
            raise ValueError(f"{name} lacks complete history for chunk")
        branch_values[name] = np.ascontiguousarray(
            timeline.values[first : last + 1].T,
            dtype=np.float32,
        )
        gather_indices[name] = np.ascontiguousarray(latest - first, dtype=np.int64)
    return SequenceChunk(
        decision_ns=decisions,
        targets=labels,
        branch_values=branch_values,
        gather_indices=gather_indices,
    )


def partition_indices(indices: np.ndarray, chunk_size: int) -> list[np.ndarray]:
    selected = np.asarray(indices, dtype=np.int64)
    if selected.ndim != 1:
        raise ValueError("indices must be a vector")
    if not len(selected):
        return []
    if chunk_size <= 0:
        raise ValueError("chunk_size must be positive")
    chunk_count = max(1, math.ceil(len(selected) / chunk_size))
    return [part for part in np.array_split(selected, chunk_count) if len(part)]


class CausalResidualBlock(nn.Module):
    def __init__(
        self,
        channels: int,
        *,
        kernel_size: int,
        dilation: int,
        dropout: float,
    ) -> None:
        super().__init__()
        self.left_padding = dilation * (kernel_size - 1)
        self.norm = nn.LayerNorm(channels)
        self.depthwise = nn.Conv1d(
            channels,
            channels,
            kernel_size=kernel_size,
            dilation=dilation,
            groups=channels,
        )
        self.pointwise = nn.Conv1d(channels, channels, kernel_size=1)
        self.dropout = nn.Dropout(dropout)

    def forward(self, values: torch.Tensor) -> torch.Tensor:
        normalized = self.norm(values.transpose(1, 2)).transpose(1, 2)
        encoded = self.depthwise(F.pad(normalized, (self.left_padding, 0)))
        encoded = self.pointwise(F.gelu(encoded))
        return values + self.dropout(encoded)


class CausalTemporalBranch(nn.Module):
    def __init__(
        self,
        *,
        input_channels: int,
        hidden_channels: int,
        required_steps: int,
        kernel_size: int,
        dropout: float,
    ) -> None:
        super().__init__()
        self.required_steps = int(required_steps)
        self.kernel_size = int(kernel_size)
        self.dilations = receptive_dilations(required_steps, kernel_size=kernel_size)
        self.depth = len(self.dilations)
        self.input_projection = nn.Conv1d(input_channels, hidden_channels, kernel_size=1)
        self.blocks = nn.ModuleList(
            [
                CausalResidualBlock(
                    hidden_channels,
                    kernel_size=kernel_size,
                    dilation=dilation,
                    dropout=dropout,
                )
                for dilation in self.dilations
            ]
        )
        self.output_norm = nn.LayerNorm(hidden_channels)

    @property
    def receptive_field(self) -> int:
        return 1 + (self.kernel_size - 1) * sum(self.dilations)

    def forward(self, values: torch.Tensor) -> torch.Tensor:
        encoded = self.input_projection(values)
        for block in self.blocks:
            encoded = block(encoded)
        return self.output_norm(encoded.transpose(1, 2)).transpose(1, 2)


class MultiResolutionCausalTCN(nn.Module):
    def __init__(
        self,
        *,
        branch_input_channels: Mapping[str, int],
        branch_steps: Mapping[str, int],
        hidden_channels: int = 24,
        fusion_channels: int = 96,
        kernel_size: int = 2,
        dropout: float = 0.10,
        output_classes: int = 1,
    ) -> None:
        super().__init__()
        if set(branch_input_channels) != set(branch_steps):
            raise ValueError("branch channel and step schemas differ")
        self.branch_names = tuple(branch_input_channels)
        self.branches = nn.ModuleDict(
            {
                name: CausalTemporalBranch(
                    input_channels=int(branch_input_channels[name]),
                    hidden_channels=hidden_channels,
                    required_steps=int(branch_steps[name]),
                    kernel_size=kernel_size,
                    dropout=dropout,
                )
                for name in self.branch_names
            }
        )
        self.fusion = nn.Sequential(
            nn.Linear(len(self.branch_names) * hidden_channels, fusion_channels),
            nn.GELU(),
            nn.Dropout(dropout),
            nn.Linear(fusion_channels, fusion_channels // 2),
            nn.GELU(),
            nn.Dropout(dropout),
        )
        if output_classes <= 0:
            raise ValueError("output_classes must be positive")
        self.output_classes = int(output_classes)
        self.event_head = nn.Linear(fusion_channels // 2, self.output_classes)

    def forward(
        self,
        branch_values: Mapping[str, torch.Tensor],
        gather_indices: Mapping[str, torch.Tensor],
    ) -> torch.Tensor:
        gathered: list[torch.Tensor] = []
        for name in self.branch_names:
            encoded = self.branches[name](branch_values[name])
            positions = gather_indices[name]
            gathered.append(encoded[0].index_select(1, positions).transpose(0, 1))
        fused = self.fusion(torch.cat(gathered, dim=1))
        logits = self.event_head(fused)
        return logits.squeeze(1) if self.output_classes == 1 else logits


def receptive_depth(required_steps: int, *, kernel_size: int) -> int:
    return len(receptive_dilations(required_steps, kernel_size=kernel_size))


def receptive_dilations(required_steps: int, *, kernel_size: int) -> tuple[int, ...]:
    if required_steps <= 1:
        return (1,)
    if kernel_size <= 1:
        raise ValueError("kernel_size must be greater than one")
    dilation_budget = max(1, (required_steps - 1) // (kernel_size - 1))
    dilations: list[int] = []
    used = 0
    power = 1
    while used + power <= dilation_budget:
        dilations.append(power)
        used += power
        power *= 2
    if used < dilation_budget:
        dilations.append(dilation_budget - used)
    return tuple(dilations)
