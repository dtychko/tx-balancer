import {Channel, Message} from 'amqplib'
import {
  outputMirrorQueueName,
  outputQueueCount,
  outputQueueLimit,
  outputQueueName,
  partitionKeyHeader,
  responseQueueName,
  singlePartitionKeyLimit
} from './config'
import {handleMessage} from './handleMessage'
import {TxChannel} from './amqp'
import {QStateWithLimits} from './QState'
import * as uuid from 'uuid'

export async function createQState(ch: TxChannel, onMessageProcessed: () => void): Promise<QStateWithLimits> {
  const qState = new QStateWithLimits(
    ackMessages(ch),
    outputQueueCount,
    onMessageProcessed,
    outputQueueLimit,
    singlePartitionKeyLimit
  )

  await consumeOutputMirrorQueues(ch, outputQueueCount, qState)
  await consumeResponseQueue(ch, qState)

  return qState
}

function ackMessages(ch: Channel) {
  return async (outputDeliveryTag: number, responseDeliveryTag: number) => {
    ch.ack({fields: {deliveryTag: outputDeliveryTag}} as Message)
    ch.ack({fields: {deliveryTag: responseDeliveryTag}} as Message)
  }
}

async function consumeOutputMirrorQueues(ch: TxChannel, outputQueueCount: number, qState: QStateWithLimits) {
  const markerMessageId = `__marker/${uuid.v4()}`

  await ch.tx(tx => {
    for (let i = 0; i < outputQueueCount; i++) {
      const queueName = outputMirrorQueueName(outputQueueName(i + 1))
      tx.publish('', queueName, Buffer.from(''), {persistent: true, messageId: markerMessageId})
    }
  })

  for (let i = 0; i < outputQueueCount; i++) {
    await new Promise<void>(async (res, rej) => {
      try {
        const queueName = outputQueueName(i + 1)
        const mirrorQueueName = outputMirrorQueueName(queueName)
        let isInitialized = false

        await ch.consume(
          mirrorQueueName,
          handleMessage(msg => {
            const messageId = msg.properties.messageId
            const deliveryTag = msg.fields.deliveryTag

            if (isInitialized) {
              qState.registerOutputDeliveryTag(messageId, deliveryTag)
              return
            }

            if (messageId === markerMessageId) {
              isInitialized = true
              ch.tx(tx => tx.ack(msg))
              res()
              return
            }

            const partitionKey = msg.properties.headers[partitionKeyHeader]
            qState.registerOutputMessage(messageId, partitionKey, queueName)
            qState.registerOutputDeliveryTag(messageId, deliveryTag)
          }),
          {noAck: false}
        )
      } catch (err) {
        rej(err)
      }
    })
  }
}

async function consumeResponseQueue(ch: Channel, qState: QStateWithLimits) {
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
