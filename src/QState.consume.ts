import {Channel, ConfirmChannel} from 'amqplib'
import {handleMessage} from './amqp/handleMessage'
import {QState} from './QState'
import MirrorQueueConsumer from './MirrorQueueConsumer'

export async function consumeMirrorQueues(params: {
  ch: ConfirmChannel
  qState: QState
  outputQueueCount: number
  outputQueueName: (oneBasedIndex: number) => string
  mirrorQueueName: (outputQueueName: string) => string
  partitionGroupHeader: string
  partitionKeyHeader: string
}) {
  const {ch, qState, outputQueueCount, outputQueueName, mirrorQueueName, partitionGroupHeader, partitionKeyHeader} =
    params
  const prommises = []

  for (let queueIndex = 1; queueIndex <= outputQueueCount; queueIndex++) {
    const outputQueue = outputQueueName(queueIndex)
    const mirrorQueue = mirrorQueueName(outputQueue)

    prommises.push(
      new MirrorQueueConsumer({
        ch,
        qState,
        onError: err => console.error(err),
        mirrorQueueName: mirrorQueue,
        outputQueueName: outputQueue,
        partitionGroupHeader,
        partitionKeyHeader
      }).init()
    )
  }

  await Promise.all(prommises)
}

export async function consumeResponseQueue(params: {ch: Channel; qState: QState; responseQueueName: string}) {
  const {ch, qState, responseQueueName} = params

  await ch.consume(
    responseQueueName,
    handleMessage(msg => {
      const messageId = msg.properties.messageId
      const deliveryTag = msg.fields.deliveryTag
      const {registered} = qState.registerResponseDeliveryTag(messageId, deliveryTag)

      if (!registered) {
        ch.ack(msg)
      }
    }),
    {noAck: false}
  )
}
