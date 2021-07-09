export const postgresConnectionString =
  process.env.POSTGRES_CONNECTION_STRING || 'postgres://postgres:postgres@localhost:5434/postgres'
export const postgresPoolMax = 2

export const amqpUri = process.env.AMQP_URI || 'amqp://guest:guest@localhost:5672/'
export const inputQueueName = '_tx_balancer_input'
export const responseQueueName = '_tx_balancer_response'
export const inputChannelPrefetchCount = 500

export const outputQueueName = (oneBasedIndex: number) => `_tx_balancer_output_${oneBasedIndex}`
export const mirrorQueueName = (outputQueue: string) => `${outputQueue}.mirror`

export const partitionGroupHeader = 'x-partition-group'
export const partitionKeyHeader = 'x-partition-key'
export const outputQueueCount = 3
export const outputQueueLimit = 100
export const singlePartitionGroupLimit = 50
export const singlePartitionKeyLimit = 10
