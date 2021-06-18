import {Channel, ConfirmChannel, Message} from 'amqplib'
import {
  outputMirrorQueueName,
  outputQueueCount,
  outputQueueLimit,
  outputQueueName, partitionGroupHeader,
  partitionKeyHeader,
  responseQueueName,
  singlePartitionGroupLimit
} from './config'
import {handleMessage} from './handleMessage'
import {publishAsync} from './publishAsync'
import {QState} from './QState'
import {nanoid} from 'nanoid'

export async function createQState(ch: ConfirmChannel, onMessageProcessed: () => void): Promise<QState> {
  const qState = new QState({
    onMessageProcessed: (_, mirrorDeliveryTag, responseDeliveryTag) => {
      ch.ack({fields: {deliveryTag: mirrorDeliveryTag}} as Message)
      ch.ack({fields: {deliveryTag: responseDeliveryTag}} as Message)
      onMessageProcessed()
    },
    queueCount: outputQueueCount,
    queueSizeLimit: outputQueueLimit,
    singlePartitionGroupLimit: singlePartitionGroupLimit
  })

  await consumeMirrorQueues(ch, qState, outputQueueCount)
  await consumeResponseQueue(ch, qState)

  return qState
}

async function consumeMirrorQueues(ch: ConfirmChannel, qState: QState, queueCount: number) {
  for (let i = 0; i < queueCount; i++) {
    const queueName = outputQueueName(i + 1)
    const mirrorQueueName = outputMirrorQueueName(queueName)

    await consumeMirrorQueue(ch, qState, queueName, mirrorQueueName)
  }
}

async function consumeMirrorQueue(ch: ConfirmChannel, qState: QState, queueName: string, mirrorQueueName: string) {
  return new Promise<void>(async (res, rej) => {
    try {
      const markerMessageId = `__marker/${nanoid()}`
      let isInitialized = false

      await publishAsync(ch, '', mirrorQueueName, Buffer.from(''), {persistent: true, messageId: markerMessageId})

      await ch.consume(
        mirrorQueueName,
        handleMessage(msg => {
          const messageId = msg.properties.messageId
          const deliveryTag = msg.fields.deliveryTag

          if (isInitialized) {
            qState.registerMirrorDeliveryTag(messageId, deliveryTag)
            return
          }

          if (messageId === markerMessageId) {
            isInitialized = true
            ch.ack(msg)
            res()
            return
          }

          const partitionGroup = msg.properties.headers[partitionGroupHeader]
          const partitionKey = msg.properties.headers[partitionKeyHeader]
          qState.restoreMessage(messageId, partitionGroup, partitionKey, queueName)
          qState.registerMirrorDeliveryTag(messageId, deliveryTag)
        }),
        {noAck: false}
      )
    } catch (err) {
      rej(err)
    }
  })
}

async function consumeResponseQueue(ch: Channel, qState: QState) {
  await ch.consume(
    responseQueueName,
    handleMessage(msg => {
      const messageId = msg.properties.messageId
      const deliveryTag = msg.fields.deliveryTag
      qState.registerResponseDeliveryTag(messageId, deliveryTag)
    }),
    {noAck: false}
  )
}
