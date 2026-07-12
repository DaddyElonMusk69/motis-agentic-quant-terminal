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
  episodeFill,
  positionEpisodeOnLogicalRange,
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

function initialLogicalRange(visualization: SignalDiscoveryAtlasVisualization): AtlasVisibleRange {
  return {
    from: 0,
    to: Math.max(1, visualization.candles.length - 1)
  };
}

export function OpportunityAtlasChart({
  visualization,
  selectedEpisodeId,
  onEpisodeSelect
}: OpportunityAtlasChartProps) {
  const chartHostRef = useRef<HTMLDivElement>(null);
  const chartRef = useRef<IChartApi | null>(null);
  const [visibleLogicalRange, setVisibleLogicalRange] = useState<AtlasVisibleRange>(() =>
    initialLogicalRange(visualization)
  );
  const [crosshairLogical, setCrosshairLogical] = useState<number | null>(null);
  const lanes = useMemo(() => sortAtlasLanes(visualization.lanes), [visualization.lanes]);
  const candleTimes = useMemo(
    () => visualization.candles.map((candle) => epochSeconds(candle.timestamp)),
    [visualization.candles]
  );
  const allEpisodes = useMemo(
    () => lanes.flatMap((lane) => lane.episodes),
    [lanes]
  );

  useEffect(() => {
    const host = chartHostRef.current;
    if (!host) {
      return;
    }
    setVisibleLogicalRange(initialLogicalRange(visualization));
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
    const handleRange = (range: { from: unknown; to: unknown } | null) => {
      if (range && typeof range.from === "number" && typeof range.to === "number") {
        setVisibleLogicalRange({ from: range.from, to: range.to });
      }
    };
    const handleCrosshair = (param: { logical?: unknown }) => {
      setCrosshairLogical(typeof param.logical === "number" ? param.logical : null);
    };
    chart.timeScale().subscribeVisibleLogicalRangeChange(handleRange);
    chart.subscribeCrosshairMove(handleCrosshair);
    chart.timeScale().fitContent();
    handleRange(chart.timeScale().getVisibleLogicalRange());
    const resizeObserver = new ResizeObserver(() => {
      chart.applyOptions({
        width: Math.max(320, host.clientWidth),
        height: Math.max(360, host.clientHeight)
      });
    });
    resizeObserver.observe(host);
    return () => {
      resizeObserver.disconnect();
      chart.timeScale().unsubscribeVisibleLogicalRangeChange(handleRange);
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
            const position = positionEpisodeOnLogicalRange(
              episode,
              candleTimes,
              visibleLogicalRange
            );
            const fill = episodeFill(episode.direction);
            if (!position || fill === "transparent") {
              return null;
            }
            return (
              <span
                key={episode.episode_id}
                style={{
                  background: fill,
                  left: `${position.left * 100}%`,
                  width: `${Math.max(0.18, position.width * 100)}%`
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
                const position = positionEpisodeOnLogicalRange(
                  episode,
                  candleTimes,
                  visibleLogicalRange
                );
                const fill = episodeFill(episode.direction);
                if (!position || fill === "transparent") {
                  return null;
                }
                return (
                  <button
                    aria-label={`${episode.direction} episode from ${episode.start_ts} to ${episode.end_ts}`}
                    className={episode.episode_id === selectedEpisodeId ? "is-selected" : undefined}
                    key={episode.episode_id}
                    onClick={() => onEpisodeSelect(episode)}
                    style={{
                      background: fill,
                      left: `${position.left * 100}%`,
                      width: `${Math.max(0.3, position.width * 100)}%`
                    }}
                    title={`${episode.direction} · ${episode.timestamp_count} timestamps`}
                    type="button"
                  />
                );
              })}
              {crosshairLogical !== null &&
              crosshairLogical >= visibleLogicalRange.from &&
              crosshairLogical <= visibleLogicalRange.to ? (
                <i
                  aria-hidden="true"
                  style={{
                    left: `${((crosshairLogical - visibleLogicalRange.from) /
                      Math.max(1, visibleLogicalRange.to - visibleLogicalRange.from)) * 100}%`
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
