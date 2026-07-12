# Signal Engine Builder Ensemble Objectives Design

## Goal

Generalize outcome-first discovery so each prompt may define approximate opportunity-precision and bracket-count-coverage targets. Optimize the final neutral engine stream against those targets without treating downstream directional strategy accuracy as an engine acceptance metric.

## Objective Ownership

The neutral engine owns event selection. Its primary outcome-first metrics are final-stream opportunity precision and distinct approved-bracket coverage after production-equivalent OR composition and global dedupe. The prompt may provide approximate targets, such as roughly 80% for both; they define the desired frontier rather than universal hard thresholds.

The paired strategy owns direction. Strategy validation remains necessary for contract compatibility, but directional accuracy, expected directional R, and Stage 1 profitability do not select engine leaves unless a prompt explicitly asks for joint optimization.

## Ensemble Search

Allow an anchor leaf plus complementary causal leaves. Select each additional leaf by its marginal effect after composing it with the current ensemble and rerunning global dedupe. Track newly covered brackets, new inside-bracket signals, new outside-bracket signals, hard-negative hits, overlap, and complexity.

Apply stability and target proximity primarily to the final ensemble. Individual leaves need a coherent causal regime, independent support outside a single unexplained calendar pocket, nearby-threshold robustness, and positive unique contribution in leave-one-leaf-out ablation. A regime-specific leaf may be absent or weak in some chronological blocks and need not be independently profitable or meet the ensemble's precision/coverage targets.

## Guardrails

Keep walk-forward and OOS sealed. Use only authorized training evidence for leaf selection. Forbid exact dates, outcome timestamps, ids, post hoc tolerance, repeated emissions used only to inflate coverage, and permissive leaves whose gain disappears after global dedupe. Stop at the attainable precision/coverage frontier and reject the requested target when approaching it requires brittle or non-causal rules.

## Skill Changes

Revise `Outcome-First Discovery`, build verification, common mistakes, and the final checklist to:

- honor prompt-defined approximate precision and bracket-count-coverage targets;
- define the two engine metrics precisely on the final deduped stream;
- make aggregate ensemble stability primary;
- relax per-leaf standalone stability/profitability while retaining causal support and ablation guardrails;
- make strategy direction a separate downstream concern;
- report the attainable frontier when targets cannot be reached causally.
