function hasIndicatorSnapshot(supertrend) {
  return Boolean(
    supertrend &&
    typeof supertrend === "object" &&
    (supertrend.type === "supertrend" || Object.hasOwn(supertrend, "value")),
  );
}

/** Combine REST indicator diagnostics with the newest runtime state. */
export function mergeRestAndRuntimeStrategies(restStrategies, runtimeStrategies) {
  const runtimeById = new Map(
    runtimeStrategies.map((strategy) => [strategy.strategy_id, strategy]),
  );

  return restStrategies.map((restStrategy) => {
    const runtime = runtimeById.get(restStrategy.strategy_id);
    if (!runtime) return restStrategy;
    const supertrend = hasIndicatorSnapshot(runtime.supertrend)
      ? runtime.supertrend
      : hasIndicatorSnapshot(restStrategy.supertrend)
        ? restStrategy.supertrend
        : runtime.supertrend ?? restStrategy.supertrend ?? null;
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
    const supertrend = hasIndicatorSnapshot(runtime.supertrend)
      ? runtime.supertrend
      : hasIndicatorSnapshot(previous?.supertrend)
        ? previous.supertrend
        : runtime.supertrend ?? previous?.supertrend ?? null;
    return { ...previous, ...runtime, supertrend };
  });
}
