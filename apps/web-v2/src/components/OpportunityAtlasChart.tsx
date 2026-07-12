import { useEffect, useMemo, useRef, useState } from "react";
import {
  CandlestickSeries,
  ColorType,
  createChart,
  type IChartApi,
  type UTCTimestamp
} from "lightweight-charts";
import {
  atlasChartPalette,
  clipEpisodeToRange,
  episodeFill,
  sortAtlasLanes,
  type AtlasVisibleRange
} from "../app/atlasVisualization";
import type {
  SignalDiscoveryAtlasEpisode,
  SignalDiscoveryAtlasVisualization
} from "../app/api";

type OpportunityAtlasChartProps = {
  visualization: SignalDiscoveryAtlasVisualization;
  selectedEpisodeId: string | null;
  onEpisodeSelect: (episode: SignalDiscoveryAtlasEpisode) => void;
};

function epochSeconds(value: string): number {
  return Date.parse(value) / 1000;
}

function initialRange(visualization: SignalDiscoveryAtlasVisualization): AtlasVisibleRange {
  return {
    from: epochSeconds(visualization.window_start),
    to: epochSeconds(visualization.window_end)
  };
}

export function OpportunityAtlasChart({
  visualization,
  selectedEpisodeId,
  onEpisodeSelect
}: OpportunityAtlasChartProps) {
  const chartHostRef = useRef<HTMLDivElement>(null);
  const chartRef = useRef<IChartApi | null>(null);
  const [visibleRange, setVisibleRange] = useState<AtlasVisibleRange>(() =>
    initialRange(visualization)
  );
  const [crosshairTime, setCrosshairTime] = useState<number | null>(null);
  const lanes = useMemo(() => sortAtlasLanes(visualization.lanes), [visualization.lanes]);
  const allEpisodes = useMemo(
    () => lanes.flatMap((lane) => lane.episodes),
    [lanes]
  );

  useEffect(() => {
    const host = chartHostRef.current;
    if (!host) {
      return;
    }
    setVisibleRange(initialRange(visualization));
    const chart = createChart(host, {
      autoSize: false,
      width: Math.max(320, host.clientWidth),
      height: Math.max(360, host.clientHeight),
      layout: {
        background: { type: ColorType.Solid, color: atlasChartPalette.background },
        textColor: atlasChartPalette.text
      },
      grid: {
        vertLines: { color: atlasChartPalette.grid },
        horzLines: { color: atlasChartPalette.grid }
      },
      rightPriceScale: {
        borderColor: atlasChartPalette.border,
        minimumWidth: 72
      },
      timeScale: {
        borderColor: atlasChartPalette.border,
        rightOffset: 4,
        timeVisible: true,
        secondsVisible: false
      },
      crosshair: {
        vertLine: { color: atlasChartPalette.crosshair, labelVisible: true },
        horzLine: { color: atlasChartPalette.crosshair, labelVisible: true }
      }
    });
    chartRef.current = chart;
    const series = chart.addSeries(CandlestickSeries, {
      upColor: atlasChartPalette.up,
      downColor: atlasChartPalette.down,
      borderUpColor: atlasChartPalette.up,
      borderDownColor: atlasChartPalette.down,
      wickUpColor: atlasChartPalette.up,
      wickDownColor: atlasChartPalette.down
    });
    series.setData(
      visualization.candles.map((candle) => ({
        time: epochSeconds(candle.timestamp) as UTCTimestamp,
        open: candle.open,
        high: candle.high,
        low: candle.low,
        close: candle.close
      }))
    );
    chart.timeScale().fitContent();
    const handleRange = (range: { from: unknown; to: unknown } | null) => {
      if (range && typeof range.from === "number" && typeof range.to === "number") {
        setVisibleRange({ from: range.from, to: range.to });
      }
    };
    const handleCrosshair = (param: { time?: unknown }) => {
      setCrosshairTime(typeof param.time === "number" ? param.time : null);
    };
    chart.timeScale().subscribeVisibleTimeRangeChange(handleRange);
    chart.subscribeCrosshairMove(handleCrosshair);
    const resizeObserver = new ResizeObserver(() => {
      chart.applyOptions({
        width: Math.max(320, host.clientWidth),
        height: Math.max(360, host.clientHeight)
      });
    });
    resizeObserver.observe(host);
    return () => {
      resizeObserver.disconnect();
      chart.timeScale().unsubscribeVisibleTimeRangeChange(handleRange);
      chart.unsubscribeCrosshairMove(handleCrosshair);
      chart.remove();
      chartRef.current = null;
    };
  }, [visualization]);

  return (
    <div className="opportunity-atlas-chart">
      <div className="opportunity-atlas-chart__plot">
        <div className="opportunity-atlas-chart__host" ref={chartHostRef} />
        <div className="opportunity-atlas-chart__bands" aria-hidden="true">
          {allEpisodes.map((episode) => {
            const clipped = clipEpisodeToRange(episode, visibleRange);
            const fill = episodeFill(episode.direction);
            if (!clipped || fill === "transparent") {
              return null;
            }
            const span = Math.max(1, visibleRange.to - visibleRange.from);
            return (
              <span
                key={episode.episode_id}
                style={{
                  background: fill,
                  left: `${((clipped.from - visibleRange.from) / span) * 100}%`,
                  width: `${Math.max(0.18, ((clipped.to - clipped.from) / span) * 100)}%`
                }}
              />
            );
          })}
        </div>
      </div>
      <div className="opportunity-atlas-lanes">
        {lanes.map((lane) => (
          <div
            className="opportunity-atlas-lane"
            key={`${lane.entry_delay_minutes}-${lane.horizon_hours}`}
          >
            <span>{lane.entry_delay_minutes}m / {lane.horizon_hours}h</span>
            <div className="opportunity-atlas-lane__track">
              {lane.episodes.map((episode) => {
                const clipped = clipEpisodeToRange(episode, visibleRange);
                const fill = episodeFill(episode.direction);
                if (!clipped || fill === "transparent") {
                  return null;
                }
                const span = Math.max(1, visibleRange.to - visibleRange.from);
                return (
                  <button
                    aria-label={`${episode.direction} episode from ${episode.start_ts} to ${episode.end_ts}`}
                    className={episode.episode_id === selectedEpisodeId ? "is-selected" : undefined}
                    key={episode.episode_id}
                    onClick={() => onEpisodeSelect(episode)}
                    style={{
                      background: fill,
                      left: `${((clipped.from - visibleRange.from) / span) * 100}%`,
                      width: `${Math.max(0.3, ((clipped.to - clipped.from) / span) * 100)}%`
                    }}
                    title={`${episode.direction} · ${episode.timestamp_count} timestamps`}
                    type="button"
                  />
                );
              })}
              {crosshairTime !== null && crosshairTime >= visibleRange.from && crosshairTime <= visibleRange.to ? (
                <i
                  aria-hidden="true"
                  style={{
                    left: `${((crosshairTime - visibleRange.from) / Math.max(1, visibleRange.to - visibleRange.from)) * 100}%`
                  }}
                />
              ) : null}
            </div>
          </div>
        ))}
      </div>
    </div>
  );
}
