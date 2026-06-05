from datetime import date

from quant_terminal_sdk.walk_forward import WalkForwardTemplate


def test_rolling_walk_forward_template_materializes_train_and_walk_forward_windows():
    template = WalkForwardTemplate(
        template_id="rolling_90d_14d_weekly",
        retrain_cadence="7d",
        train_range="90d",
        walk_forward_range="14d",
        embargo="0d",
        anchor="rolling",
    )

    window = template.materialize(as_of=date(2026, 6, 1))

    assert window.train_start == date(2026, 3, 3)
    assert window.train_end == date(2026, 5, 31)
    assert window.walk_forward_start == date(2026, 6, 1)
    assert window.walk_forward_end == date(2026, 6, 14)
    assert window.template_id == "rolling_90d_14d_weekly"
