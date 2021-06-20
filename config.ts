export const postgresConnectionString = 'postgres://postgres:postgres@localhost:5432/postgres'
export const postgresPoolMax = 2

export const amqpUri = 'amqp://guest:guest@localhost:5672/'
export const inputQueueName = '_tx_balancer_input'
export const responseQueueName = '_tx_balancer_response'

export const outputQueueName = (index: number) => `_tx_balancer_output_${index}`
export const outputMirrorQueueName = (outputQueue: string) => `${outputQueue}.mirror`

export const partitionGroupHeader = 'x-partition-group'
export const partitionKeyHeader = 'x-partition-key'
export const outputQueueCount = 3
export const outputQueueLimit = 100
export const singlePartitionGroupLimit = 50
export const singlePartitionKeyLimit = 10
