export type StrategySnapshot = Record<string, unknown> & {
  strategy_id: string;
  supertrend?: unknown;
};

export function mergeRestAndRuntimeStrategies(
  restStrategies: StrategySnapshot[],
  runtimeStrategies: StrategySnapshot[],
): StrategySnapshot[];

export function mergeRuntimeStrategies(
  runtimeStrategies: StrategySnapshot[],
  previousStrategies: StrategySnapshot[],
): StrategySnapshot[];
