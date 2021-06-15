export const amqpUri = 'amqp://guest:guest@localhost:5672/'
export const inputQueueName = 'input'
export const responseQueueName = 'response'

export const outputQueueName = (index: number) => `output_${index}`
export const outputMirrorQueueName = (outputQueue: string) => `${outputQueue}.mirror`

export const partitionKeyHeader = 'x-partition-key'
export const outputQueueCount = 3
export const outputQueueLimit = 100
export const singlePartitionKeyLimit = 50