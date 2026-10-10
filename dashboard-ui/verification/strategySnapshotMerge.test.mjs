import assert from "node:assert/strict";
import test from "node:test";
import {
  mergeRestAndRuntimeStrategies,
  mergeRuntimeStrategies,
} from "../src/store/strategySnapshotMerge.js";

const warmed = {
  type: "supertrend",
  bars: 290,
  last_candle_end_ts: 12345,
  value: { trend: 1, atr: 169.37, buy_signal: false, sell_signal: false },
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
