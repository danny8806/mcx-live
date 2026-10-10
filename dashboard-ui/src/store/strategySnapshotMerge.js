function hasIndicatorSnapshot(supertrend) {
  return Boolean(
    supertrend &&
    typeof supertrend === "object" &&
    (supertrend.type === "supertrend" || Object.hasOwn(supertrend, "value")),
  );
}

function hasCalculatedValue(snapshot) {
  const value = snapshot?.value;
  const finiteValue = (candidate) => candidate !== null &&
    candidate !== undefined && Number.isFinite(Number(candidate));
  return Boolean(
    value && typeof value === "object" &&
    (Number(value.trend) === 1 || Number(value.trend) === -1) &&
    finiteValue(value.atr) && finiteValue(value.upper) &&
    finiteValue(value.lower),
  );
}

function sameIndicatorProfile(left, right) {
  if (!left || !right) return false;
  const fields = ["atr_period", "multiplier", "source", "atr_method", "first_tr"];
  let compared = false;
  for (const field of fields) {
    if (left[field] === undefined || right[field] === undefined) continue;
    compared = true;
    if (left[field] !== right[field]) return false;
  }
  return compared;
}

function isWarmingUp(snapshot) {
  if (!snapshot || typeof snapshot !== "object") return false;
  const bars = Number(snapshot.bars);
  const period = Number(snapshot.atr_period);
  if (!Number.isFinite(bars) || !Number.isFinite(period) || period < 1) return false;
  const requiredBars = period + (snapshot.first_tr === "na" ? 1 : 0);
  return bars < requiredBars;
}

function chooseIndicatorSnapshot(incoming, fallback) {
  if (hasCalculatedValue(incoming)) return incoming;
  // A genuine warm-up/reset is meaningful and must replace an old trend.
  if (isWarmingUp(incoming)) return incoming;
  // The runtime WebSocket may carry a profile and candle count but omit the
  // nested current value. Keep the last calculated value only for the same
  // parameter profile; a changed profile must never display old bands.
  if (hasCalculatedValue(fallback) && sameIndicatorProfile(incoming, fallback)) {
    return fallback;
  }
  if (hasIndicatorSnapshot(incoming)) return incoming;
  return hasIndicatorSnapshot(fallback) ? fallback : incoming ?? fallback ?? null;
}

export function indicatorDisplayState(snapshot) {
  if (!snapshot) {
    return { kind: "unavailable", label: "DATA UNAVAILABLE", message: "No indicator snapshot was received." };
  }
  if (isWarmingUp(snapshot)) {
    const bars = Number(snapshot.bars) || 0;
    const required = Number(snapshot.atr_period) + (snapshot.first_tr === "na" ? 1 : 0);
    return { kind: "warming", label: "WARMING UP", message: `${bars} of ${required} candles needed for this ATR period.` };
  }
  if (hasCalculatedValue(snapshot)) {
    return {
      kind: Number(snapshot.value.trend) === 1 ? "bullish" : "bearish",
      label: Number(snapshot.value.trend) === 1 ? "UPTREND" : "DOWNTREND",
      message: "Current calculation from the latest completed candle.",
    };
  }
  return {
    kind: "unavailable",
    label: "VALUE UNAVAILABLE",
    message: `${Number(snapshot.bars) || 0} candles processed, but ATR/band values are missing.`,
  };
}

/** Combine REST indicator diagnostics with the newest runtime state. */
export function mergeRestAndRuntimeStrategies(restStrategies, runtimeStrategies) {
  const runtimeById = new Map(
    runtimeStrategies.map((strategy) => [strategy.strategy_id, strategy]),
  );

  return restStrategies.map((restStrategy) => {
    const runtime = runtimeById.get(restStrategy.strategy_id);
    if (!runtime) return restStrategy;
    const supertrend = chooseIndicatorSnapshot(
      runtime.supertrend, restStrategy.supertrend,
    );
    return { ...restStrategy, ...runtime, supertrend };
  });
}

/** Preserve the last usable REST indicator when a WebSocket frame omits it. */
export function mergeRuntimeStrategies(runtimeStrategies, previousStrategies) {
  const previousById = new Map(
    previousStrategies.map((strategy) => [strategy.strategy_id, strategy]),
  );

  return runtimeStrategies.map((runtime) => {
    const previous = previousById.get(runtime.strategy_id);
    const supertrend = chooseIndicatorSnapshot(
      runtime.supertrend, previous?.supertrend,
    );
    return { ...previous, ...runtime, supertrend };
  });
}
