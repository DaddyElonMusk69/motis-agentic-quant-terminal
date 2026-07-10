import type { DevelopmentQueueRow, Stage0InformationSummary, Stage0UniverseCandidate } from "./api";

export type Stage0InformationTone = "pass" | "risk" | "warn" | "idle";

export function stage0InformationFromCandidate(candidate: Stage0UniverseCandidate | null | undefined): Stage0InformationSummary | null {
  return candidate?.metrics?.stage0_information ?? null;
}

export function stage0InformationFromRow(row: DevelopmentQueueRow | null | undefined): Stage0InformationSummary | null {
  return row?.stage0_information ?? null;
}

export function stage0InformationTone(info: Stage0InformationSummary | null | undefined): Stage0InformationTone {
  const status = String(info?.status ?? "").toLowerCase();
  if (status === "pass") {
    return "pass";
  }
  if (status === "fail") {
    return "risk";
  }
  if (status === "insufficient_sample") {
    return "warn";
  }
  return "idle";
}

export function stage0InformationStatusLabel(info: Stage0InformationSummary | null | undefined): string {
  const status = info?.status;
  if (!status) {
    return "pending";
  }
  return status.replaceAll("_", " ");
}

export function stage0InformationDecisionLabel(info: Stage0InformationSummary | null | undefined): string {
  const reason = info?.decision_reason;
  if (!reason) {
    return "Information gate has not run yet.";
  }
  if (reason === "legacy_no_train_walk_forward_split_configured") {
    return "Legacy compatibility: no train / walk-forward split was configured.";
  }
  return reason.replaceAll("_", " ");
}

export function formatStage0SignedPct(value: number | undefined | null): string {
  if (typeof value !== "number" || Number.isNaN(value)) {
    return "-";
  }
  return `${value >= 0 ? "+" : ""}${value.toFixed(1)}%`;
}

export function formatStage0PValue(value: number | undefined | null): string {
  if (typeof value !== "number" || Number.isNaN(value)) {
    return "-";
  }
  if (value < 0.001) {
    return "<0.001";
  }
  return value.toFixed(3);
}

export function formatStage0Count(value: number | undefined | null): string {
  if (typeof value !== "number" || Number.isNaN(value)) {
    return "-";
  }
  return new Intl.NumberFormat("en-US", { maximumFractionDigits: 0 }).format(value);
}

export function formatStage0Months(info: Stage0InformationSummary | null | undefined): string {
  if (
    typeof info?.monthly_positive_lift_months !== "number"
    || typeof info.monthly_eligible_months !== "number"
  ) {
    return "-";
  }
  return `${formatStage0Count(info.monthly_positive_lift_months)}/${formatStage0Count(info.monthly_eligible_months)}`;
}

export function formatStage0Samples(info: Stage0InformationSummary | null | undefined): string {
  return `T ${formatStage0Count(info?.train_event_count)} / WF ${formatStage0Count(info?.walk_forward_event_count)}`;
}

export function formatStage0PAndQ(info: Stage0InformationSummary | null | undefined): string {
  return `${formatStage0PValue(info?.train_empirical_p_value)} / ${formatStage0PValue(info?.train_q_value)}`;
}

export function formatStage0CompactLine(info: Stage0InformationSummary | null | undefined): string {
  if (!info) {
    return "Info pending";
  }
  return `${stage0InformationStatusLabel(info)} · T ${formatStage0SignedPct(info.train_median_lift_pct)} · WF ${formatStage0SignedPct(info.walk_forward_median_lift_pct)}`;
}

export function formatStage0StatsLine(info: Stage0InformationSummary | null | undefined): string {
  if (!info) {
    return "p/q - · months -";
  }
  return `p/q ${formatStage0PAndQ(info)} · months ${formatStage0Months(info)}`;
}
