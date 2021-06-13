export const amqpUri = 'amqp://guest:guest@localhost:5672/'
export const inputQueueName = 'input'
export const responseQueueName = 'response'

export const outputQueueName = (index: number) => `output_${index}`
export const outputMirrorQueueName = (index: number) => `${outputQueueName(index)}.mirror`

export const partitionKeyHeader = 'x-partition-key'
