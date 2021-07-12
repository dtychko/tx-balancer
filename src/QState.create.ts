import {ConfirmChannel, Message} from 'amqplib'
import {
  outputQueueCount,
  outputQueueLimit,
  outputQueueName,
  mirrorQueueName,
  partitionGroupHeader,
  partitionKeyHeader,
  singlePartitionGroupLimit,
  singlePartitionKeyLimit,
  responseQueueName
} from './config'
import {QState} from './QState'
import {consumeMirrorQueues, consumeResponseQueue} from './QState.consume'
import {sumCharCodes} from './utils'

export async function createQState(params: {ch: ConfirmChannel; onMessageProcessed: () => void}): Promise<QState> {
  const {ch, onMessageProcessed} = params
  const qState = new QState({
    onMessageProcessed: (_, mirrorDeliveryTag, responseDeliveryTag) => {
      ch.ack({fields: {deliveryTag: mirrorDeliveryTag}} as Message)
      ch.ack({fields: {deliveryTag: responseDeliveryTag}} as Message)
      onMessageProcessed()
    },
    partitionGroupHash: partitionGroup => {
      // Just sum partitionGroup char codes instead of calculating a complex hash
      return sumCharCodes(partitionGroup)
    },
    outputQueueName,
    queueCount: outputQueueCount,
    queueSizeLimit: outputQueueLimit,
    singlePartitionGroupLimit: singlePartitionGroupLimit,
    singlePartitionKeyLimit: singlePartitionKeyLimit
  })

  const consumeParams = {
    ch,
    qState,
    outputQueueCount,
    outputQueueName,
    mirrorQueueName,
    responseQueueName,
    partitionGroupHeader,
    partitionKeyHeader
  }

  await consumeMirrorQueues(consumeParams)
  console.log('started mirror queue consumers')

  await consumeResponseQueue(consumeParams)
  console.log('started response queue consumer')

  return qState
}
