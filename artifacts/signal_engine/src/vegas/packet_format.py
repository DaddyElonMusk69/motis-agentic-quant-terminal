from __future__ import annotations

import json
from typing import Any


def dumps_signal_packet(packet: dict[str, Any]) -> str:
    """Serialize a v2 signal packet with candle rows kept compact and readable."""
    lines: list[str] = ["{"]
    _write_field(lines, "schema_version", packet["schema_version"], comma=True, indent=2)
    _write_field(lines, "asset", packet["asset"], comma=True, indent=2)
    _write_field(lines, "timestamp", packet["timestamp"], comma=True, indent=2)
    _write_field(lines, "active_timeframes", packet["active_timeframes"], comma=True, indent=2)
    _write_interactions(lines, packet.get("interactions", []), comma=True)
    _write_charts(lines, packet.get("charts", {}))
    lines.append("}")
    return "\n".join(lines) + "\n"


def write_signal_packet(path: Any, packet: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(dumps_signal_packet(packet))


def _compact(value: Any) -> str:
    return json.dumps(value, separators=(",", ":"))


def _write_field(lines: list[str], key: str, value: Any, *, comma: bool, indent: int) -> None:
    suffix = "," if comma else ""
    lines.append(f'{" " * indent}{json.dumps(key)}: {_compact(value)}{suffix}')


def _write_interactions(lines: list[str], interactions: list[dict[str, Any]], *, comma: bool) -> None:
    lines.append('  "interactions": [')
    for index, interaction in enumerate(interactions):
        suffix = "," if index < len(interactions) - 1 else ""
        lines.append(f"    {_compact(interaction)}{suffix}")
    lines.append(f"  ]{',' if comma else ''}")


def _write_charts(lines: list[str], charts: dict[str, dict[str, Any]]) -> None:
    lines.append('  "charts": {')
    chart_items = list(charts.items())
    for chart_index, (timeframe, chart) in enumerate(chart_items):
        chart_suffix = "," if chart_index < len(chart_items) - 1 else ""
        lines.append(f"    {json.dumps(timeframe)}: {{")
        _write_field(lines, "timeframe", chart["timeframe"], comma=True, indent=6)
        _write_field(lines, "columns", chart["columns"], comma=True, indent=6)
        lines.append('      "completed_candles": [')
        rows = chart.get("completed_candles", [])
        for row_index, row in enumerate(rows):
            row_suffix = "," if row_index < len(rows) - 1 else ""
            lines.append(f"        {_compact(row)}{row_suffix}")
        lines.append("      ],")
        _write_field(
            lines,
            "latest_forming_candle",
            chart["latest_forming_candle"],
            comma=False,
            indent=6,
        )
        lines.append(f"    }}{chart_suffix}")
    lines.append("  }")
