from __future__ import annotations

import numpy as np
import pandas as pd
import pytest
import torch

from quant_terminal_worker.signal_discovery.supervised_directional_training import (
    CLASS_NAMES,
    DirectionalConfig,
    DirectionalPolicy,
    ResidentTimelines,
    action_metrics,
    apply_policy,
    attach_directional_target,
    contiguous_three_class_window,
    policy_meets_requirements,
    prepare_sequence_chunks,
    ranking_metrics,
    select_operating_policy,
)
from quant_terminal_worker.signal_discovery.supervised_sequence_model import (
    build_sequence_chunk,
)
from quant_terminal_worker.signal_discovery.supervised_training_data import (
    BranchSpec,
    TransformedTimeline,
)


def test_raw_labels_map_to_exact_three_class_target() -> None:
    labels = pd.DataFrame({"raw_label": ["LONG", "SHORT", "NEUTRAL"]})

    prepared = attach_directional_target(labels)

    assert tuple(CLASS_NAMES) == ("LONG", "SHORT", "NEUTRAL")
    assert prepared["class_target"].tolist() == [0, 1, 2]


def test_action_metrics_penalize_wrong_side_and_neutral_false_entry() -> None:
    truth = np.asarray([0, 1, 2, 0, 1, 2])
    prediction = np.asarray([0, 0, 1, 2, 1, 2])

    metrics = action_metrics(truth, prediction)

    assert metrics.entries == 4
    assert metrics.correct_entries == 2
    assert metrics.action_precision == pytest.approx(0.5)
    assert metrics.wrong_side_entries == 1
    assert metrics.neutral_false_entries == 1
    assert metrics.neutral_false_positive_rate == pytest.approx(0.5)
    assert metrics.long_recall == pytest.approx(0.5)
    assert metrics.short_recall == pytest.approx(0.5)


def test_action_policy_requires_both_sides_and_neutral_false_entry_control() -> None:
    target = np.asarray([0, 1, 2] * 20, dtype=np.int64)
    perfect = action_metrics(target, target)
    always_long = action_metrics(target, np.zeros(len(target), dtype=np.int64))
    config = DirectionalConfig()

    assert policy_meets_requirements(perfect, config) is True
    assert policy_meets_requirements(always_long, config) is False


def test_policy_abstains_when_direction_does_not_clear_neutral_and_margin() -> None:
    probabilities = np.asarray(
        [
            [0.70, 0.10, 0.20],
            [0.36, 0.34, 0.30],
            [0.20, 0.25, 0.55],
        ]
    )
    policy = DirectionalPolicy(probability_threshold=0.50, margin_threshold=0.10)

    prediction = apply_policy(probabilities, policy)

    assert prediction.tolist() == [0, 2, 2]


def test_uniform_always_enter_shortcut_cannot_select_a_valid_policy() -> None:
    target = np.asarray(([0] * 31) + ([1] * 31) + ([2] * 38), dtype=np.int64)
    probabilities = np.tile(np.asarray([0.34, 0.33, 0.33]), (len(target), 1))

    policy, _ = select_operating_policy(
        target,
        probabilities,
        config=DirectionalConfig(
            minimum_action_precision=0.55,
            minimum_entry_coverage=0.05,
            minimum_side_coverage=0.01,
        ),
    )

    assert policy is None


def test_policy_frontier_can_select_both_sides_with_exact_precision() -> None:
    target = np.asarray(([0] * 40) + ([1] * 40) + ([2] * 40), dtype=np.int64)
    probabilities = np.full((len(target), 3), 0.1, dtype=np.float64)
    probabilities[np.arange(40), 0] = 0.8
    probabilities[40 + np.arange(40), 1] = 0.8
    probabilities[80 + np.arange(40), 2] = 0.8
    probabilities /= probabilities.sum(axis=1, keepdims=True)

    policy, _ = select_operating_policy(
        target,
        probabilities,
        config=DirectionalConfig(),
    )

    assert policy is not None
    metrics = action_metrics(target, apply_policy(probabilities, policy))
    assert metrics.action_precision == 1.0
    assert metrics.long_coverage > 0.01
    assert metrics.short_coverage > 0.01


def test_overfit_window_requires_all_three_classes() -> None:
    target = np.asarray(([0, 1, 2] * 100), dtype=np.int64)
    indices = np.arange(len(target), dtype=np.int64)

    selected = contiguous_three_class_window(indices, target, rows=96)

    assert set(target[selected]) == {0, 1, 2}


@pytest.mark.parametrize(
    "device",
    [torch.device("cpu")]
    + ([torch.device("mps")] if torch.backends.mps.is_available() else []),
)
def test_resident_inputs_match_legacy_per_chunk_materialization(
    device: torch.device,
) -> None:
    timeline = TransformedTimeline(
        name="test_branch",
        available_ns=np.arange(20, dtype=np.int64) * 10,
        values=np.arange(60, dtype=np.float32).reshape(20, 3),
        channel_names=("a", "b", "c"),
        spec=BranchSpec("test_branch", None, 5, 4),
    )
    timelines = {timeline.name: timeline}
    decision_ns = np.asarray([60, 70, 80, 90, 100], dtype=np.int64)
    targets = np.asarray([0, 1, 2, 0, 1], dtype=np.int64)
    indices = np.arange(len(decision_ns), dtype=np.int64)
    prepared = prepare_sequence_chunks(
        decision_ns=decision_ns,
        timelines=timelines,
        indices=indices,
        chunk_size=len(indices),
        device=device,
        targets=targets,
    )[0]
    resident = ResidentTimelines.from_timelines(timelines, device)
    values, gather = resident.inputs(prepared)
    legacy = build_sequence_chunk(
        decision_ns=decision_ns,
        targets=targets.astype(np.float32),
        timelines=timelines,
    )

    assert torch.equal(
        values["test_branch"].cpu(),
        torch.from_numpy(legacy.branch_values["test_branch"]).unsqueeze(0),
    )
    assert torch.equal(
        gather["test_branch"].cpu(),
        torch.from_numpy(legacy.gather_indices["test_branch"]),
    )
    assert prepared.targets is not None
    assert torch.equal(prepared.targets.cpu(), torch.from_numpy(targets))


def test_ranking_metrics_report_macro_one_vs_rest_pr_auc() -> None:
    target = np.asarray([0, 1, 2, 0, 1, 2])
    probabilities = np.eye(3)[target] * 0.8 + 0.2 / 3
    probabilities /= probabilities.sum(axis=1, keepdims=True)

    metrics = ranking_metrics(target, probabilities)

    assert metrics.macro_pr_auc == pytest.approx(1.0)
    assert metrics.argmax_accuracy == pytest.approx(1.0)
