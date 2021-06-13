import {ConfirmChannel, ConsumeMessage} from 'amqplib'
import {publishAsync} from './publishAsync'
import {handleMessage} from './handleMessage'
import {checkOutputQueues} from './emitter'

interface QueueState {
  queueName: string
  partitionKeys: Set<string>
  messageCount: number
}

interface Session {
  partitionKey: string
  queueName: string
  messageCount: number
}

interface Message {
  messageId: string
  partitionKey: string
  deliveryTag?: number
  responseDeliveryTag?: number
}

export const maxMessageCountPerQueue = 10

export const queueStateByName = new Map<string, QueueState>()
export const sessionByPartitionKey = new Map<string, Session>()
const messageByMessageId = new Map<string, Message>()

export async function initState(ch: ConfirmChannel) {
  for (const queueName of ['output_1', 'output_2']) {
    await initMirrorQueue(ch, queueName)
  }

  await initResponseQueue(ch)

  return {}
}

export function registerMessage(messageId: string, partitionKey: string, queueName: string, deliveryTag?: number) {
  messageByMessageId.set(messageId, {messageId, partitionKey, deliveryTag})

  let session = sessionByPartitionKey.get(partitionKey)
  if (!session) {
    sessionByPartitionKey.set(partitionKey, {queueName, partitionKey, messageCount: 1})
  } else {
    session.messageCount += 1
  }

  const queueState = queueStateByName.get(queueName)!
  queueState.partitionKeys.add(partitionKey)
  queueState.messageCount += 1
}

function removeMessage(messageId: string) {
  const message = messageByMessageId.get(messageId)
  if (!message) {
    return
  }

  messageByMessageId.delete(messageId)

  const partitionKey = message.partitionKey
  const session = sessionByPartitionKey.get(partitionKey)!
  const queueState = queueStateByName.get(session.queueName)!
  session.messageCount -= 1
  queueState.messageCount -= 1

  if (!session.messageCount) {
    sessionByPartitionKey.delete(partitionKey)
    queueState.partitionKeys.delete(partitionKey)
  }

  if (queueState.messageCount == maxMessageCountPerQueue - 1) {
    process.nextTick(() => {
      checkOutputQueues()
    })
  }
}

async function initMirrorQueue(ch: ConfirmChannel, queueName: string) {
  queueStateByName.set(queueName, {queueName, partitionKeys: new Set<string>(), messageCount: 0})

  const mirrorQueueName = `${queueName}.mirror`
  const markerId = Date.now().toString()
  await publishAsync(ch, '', mirrorQueueName, Buffer.from(markerId), {persistent: true})

  let initialized = false

  return new Promise<void>(async res => {
    await ch.consume(
      mirrorQueueName,
      handleMessage(msg => {
        const messageId = msg.properties.messageId

        if (initialized) {
          const message = messageByMessageId.get(messageId)
          if (!message) {
            // Unknown message, just ack it
            ch.ack(msg)
            return
          }

          if (message.responseDeliveryTag !== undefined) {
            // Mirrored message removal is already scheduled
            // because related response arrived earlier than the message
            ch.ack(msg)
            ch.ack({fields: {deliveryTag: message.responseDeliveryTag}} as ConsumeMessage)
            removeMessage(messageId)
            return
          }

          message.deliveryTag = msg.fields.deliveryTag
          return
        }

        if (msg.content.toString() === markerId) {
          initialized = true
          ch.ack(msg)
          res()
          return
        }

        const partitionKey = msg.properties.headers['x-partition-key']
        registerMessage(messageId, partitionKey, queueName, msg.fields.deliveryTag)
      }),
      {noAck: false}
    )
  })
}

async function initResponseQueue(ch: ConfirmChannel) {
  await ch.consume(
    'response_queue',
    handleMessage(msg => {
      const messageId = msg.properties.messageId
      const message = messageByMessageId.get(messageId)

      if (!message) {
        // Lost response, just ack it
        ch.ack(msg)
        return
      }

      if (message.deliveryTag === undefined) {
        // It could happen that deliveryTag isn't registered yet
        // because response message arrived earlier than mirrored message.
        // The mirrored message removal should be scheduled.
        message.responseDeliveryTag = msg.fields.deliveryTag
        return
      }

      ch.ack({fields: {deliveryTag: message.deliveryTag}} as ConsumeMessage)
      ch.ack(msg)
      removeMessage(messageId)
    }),
    {noAck: false}
  )
}
