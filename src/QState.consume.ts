import {Channel, ConfirmChannel} from 'amqplib'
import {nanoid} from 'nanoid'
import {emptyBuffer} from './constants'
import {handleMessage} from './amqp/handleMessage'
import {publishAsync} from './amqp/publishAsync'
import {QState} from './QState'

export async function consumeMirrorQueues(params: {
  ch: ConfirmChannel
  qState: QState
  outputQueueCount: number
  outputQueueName: (oneBasedIndex: number) => string
  mirrorQueueName: (outputQueueName: string) => string
  partitionGroupHeader: string
  partitionKeyHeader: string
}) {
  const {outputQueueCount, outputQueueName, mirrorQueueName} = params
  const prommises = []

  for (let queueIndex = 1; queueIndex <= outputQueueCount; queueIndex++) {
    const outputQueue = outputQueueName(queueIndex)
    const mirrorQueue = mirrorQueueName(outputQueue)

    prommises.push(
      consumeMirrorQueue({
        ...params,
        mirrorQueue,
        outputQueue
      })
    )
  }

  await Promise.all(prommises)
}

async function consumeMirrorQueue(params: {
  ch: ConfirmChannel
  qState: QState
  mirrorQueue: string
  outputQueue: string
  partitionGroupHeader: string
  partitionKeyHeader: string
}) {
  const {ch, qState, outputQueue, mirrorQueue, partitionGroupHeader, partitionKeyHeader} = params

  return new Promise<void>(async (res, rej) => {
    try {
      const markerMessageId = `__marker/${nanoid()}`
      let isInitialized = false

      await publishAsync(ch, '', mirrorQueue, emptyBuffer, {persistent: true, messageId: markerMessageId})

      await ch.consume(
        mirrorQueue,
        handleMessage(msg => {
          const messageId = msg.properties.messageId
          const deliveryTag = msg.fields.deliveryTag

          if (isInitialized) {
            const {registered} = qState.registerMirrorDeliveryTag(messageId, deliveryTag)
            if (!registered) {
              ch.ack(msg)
            }
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
          qState.restoreMessage(messageId, partitionGroup, partitionKey, outputQueue)
          qState.registerMirrorDeliveryTag(messageId, deliveryTag)
        }),
        {noAck: false}
      )
    } catch (err) {
      console.error(err)
      rej(err)
    }
  })
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
