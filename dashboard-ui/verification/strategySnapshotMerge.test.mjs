import assert from "node:assert/strict";
import test from "node:test";
import {
  indicatorDisplayState,
  mergeRestAndRuntimeStrategies,
  mergeRuntimeStrategies,
} from "../src/store/strategySnapshotMerge.js";

const warmed = {
  type: "supertrend",
  atr_period: 46,
  multiplier: 3.9,
  source: "ohlc4",
  atr_method: "sma_tr",
  first_tr: "na",
  bars: 290,
  last_candle_end_ts: 12345,
  value: { trend: 1, atr: 169.37, upper: 180.2, lower: 158.5, buy_signal: false, sell_signal: false },
};

test("a WebSocket frame without indicators retains the warmed REST values", () => {
  const result = mergeRuntimeStrategies(
    [{ strategy_id: "gold_02", state: "flat", supertrend: null }],
    [{ strategy_id: "gold_02", state: "flat", supertrend: warmed }],
  );
  assert.equal(result[0].supertrend.value.trend, 1);
  assert.equal(result[0].supertrend.bars, 290);
});

test("a fresh WebSocket indicator wins over the previous snapshot", () => {
  const fresh = { ...warmed, value: { ...warmed.value, trend: -1, sell_signal: true } };
  const result = mergeRuntimeStrategies(
    [{ strategy_id: "silver_01", state: "short_position", supertrend: fresh }],
    [{ strategy_id: "silver_01", state: "flat", supertrend: warmed }],
  );
  assert.equal(result[0].supertrend.value.trend, -1);
  assert.equal(result[0].state, "short_position");
});

test("an explicit WebSocket warm-up replaces stale REST trend values", () => {
  const warming = { type: "supertrend", bars: 0, value: null };
  const result = mergeRuntimeStrategies(
    [{ strategy_id: "gold_02", state: "flat", supertrend: warming }],
    [{ strategy_id: "gold_02", state: "flat", supertrend: warmed }],
  );
  assert.equal(result[0].supertrend.bars, 0);
  assert.equal(result[0].supertrend.value, null);
});

test("an incomplete WebSocket calculation does not erase a calculated snapshot for the same profile", () => {
  const incomplete = {
    type: "supertrend", atr_period: 46, multiplier: 3.9, source: "ohlc4",
    atr_method: "sma_tr", first_tr: "na", bars: 234,
    last_candle_end_ts: 23456, value: null,
  };
  const result = mergeRuntimeStrategies(
    [{ strategy_id: "gold_02", state: "flat", supertrend: incomplete }],
    [{ strategy_id: "gold_02", state: "flat", supertrend: warmed }],
  );
  assert.equal(result[0].supertrend.value.trend, 1);
  assert.equal(result[0].supertrend.last_candle_end_ts, 12345);
});

test("an incomplete snapshot with changed parameters does not display old bands", () => {
  const changed = {
    ...warmed, atr_period: 12, multiplier: 2.1, bars: 234, value: null,
  };
  const result = mergeRuntimeStrategies(
    [{ strategy_id: "gold_02", state: "flat", supertrend: changed }],
    [{ strategy_id: "gold_02", state: "flat", supertrend: warmed }],
  );
  assert.equal(result[0].supertrend.atr_period, 12);
  assert.equal(result[0].supertrend.value, null);
});

test("indicator display distinguishes real warm-up from missing calculation data", () => {
  assert.equal(indicatorDisplayState({ ...warmed, bars: 10, value: null }).label, "WARMING UP");
  const missing = indicatorDisplayState({ ...warmed, bars: 234, value: null });
  assert.equal(missing.label, "VALUE UNAVAILABLE");
  assert.match(missing.message, /234 candles processed/);
});

test("REST refreshes indicators without regressing runtime position state", () => {
  const updated = { ...warmed, last_candle_end_ts: 23456 };
  const result = mergeRestAndRuntimeStrategies(
    [{ strategy_id: "gold_02", state: "flat", supertrend: updated, quantity: 1 }],
    [{ strategy_id: "gold_02", state: "long_position", supertrend: null, position_side: "LONG" }],
  );
  assert.equal(result[0].state, "long_position");
  assert.equal(result[0].position_side, "LONG");
  assert.equal(result[0].supertrend.last_candle_end_ts, 23456);
});

test("an unavailable indicator stays unavailable instead of inventing a trend", () => {
  const result = mergeRuntimeStrategies(
    [{ strategy_id: "gold_02", state: "flat", supertrend: null }],
    [{ strategy_id: "gold_02", state: "flat", supertrend: null }],
  );
  assert.equal(result[0].supertrend, null);
});
