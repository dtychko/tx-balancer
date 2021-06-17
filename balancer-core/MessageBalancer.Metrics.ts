export interface MessageBalancerSummaryMetric {
  observe(value: number): void
  observe(labels: LabelValues, value: number): void
}

export interface MessageBalancerGaugeMetric {
  set(value: number): void
  set(labels: LabelValues, value: number): void
}

type LabelValues = {
  [key: string]: string
}

const noop = () => undefined

export const emptyMetric: MessageBalancerSummaryMetric & MessageBalancerGaugeMetric = {
  observe: noop,
  set: noop
}
