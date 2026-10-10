export type StrategySnapshot = Record<string, unknown> & {
  strategy_id: string;
  supertrend?: unknown;
};

export function indicatorDisplayState(snapshot: unknown): {
  kind: "unavailable" | "warming" | "bullish" | "bearish";
  label: string;
  message: string;
};

export function mergeRestAndRuntimeStrategies(
  restStrategies: StrategySnapshot[],
  runtimeStrategies: StrategySnapshot[],
): StrategySnapshot[];

export function mergeRuntimeStrategies(
  runtimeStrategies: StrategySnapshot[],
  previousStrategies: StrategySnapshot[],
): StrategySnapshot[];
