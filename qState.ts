import {Channel, Message} from 'amqplib'
import {outputMirrorQueueName, outputQueueName, partitionKeyHeader, responseQueueName} from './config'
import {handleMessage} from './handleMessage'
import {TxChannel} from './amqp'

export async function createQState(
  ch: TxChannel,
  outputQueueCount: number,
  onMessageProcessed: () => void
): Promise<QState> {
  const qState = new QState(ackMessages(ch), outputQueueCount, onMessageProcessed)

  await consumeOutputMirrorQueues(ch, outputQueueCount, qState)
  await consumeResponseQueue(ch, qState)

  return qState
}

function ackMessages(ch: TxChannel) {
  return async (outputDeliveryTag: number, responseDeliveryTag: number) => {
    await ch.tx(tx => {
      tx.ack({fields: {deliveryTag: outputDeliveryTag}} as Message)
      tx.ack({fields: {deliveryTag: responseDeliveryTag}} as Message)
    })
  }
}

async function consumeOutputMirrorQueues(ch: TxChannel, outputQueueCount: number, qState: QState) {
  const markerMessageId = '__marker/' + Date.now().toString()

  return new Promise<void>(async (res, rej) => {
    try {
      await ch.tx(tx => {
        for (let i = 0; i < outputQueueCount; i++) {
          const queueName = outputMirrorQueueName(i + 1)
          tx.publish('', queueName, Buffer.from(''), {persistent: true, messageId: markerMessageId})
        }
      })

      for (let i = 0; i < outputQueueCount; i++) {
        const queueName = outputQueueName(i + 1)
        const mirrorQueueName = outputMirrorQueueName(i + 1)
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
      }
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

class QStateWithLimits {
  // TODO: implement limits
}

export class QState {
  private readonly ackMessages: (deliveryTag1: number, deliveryTag2: number) => Promise<void>
  private readonly onMessageProcessed: () => void

  private readonly outputDeliveryTagsByMessageId = new Map<string, number>()
  private readonly responseDeliveryTagsByMessageId = new Map<string, number>()
  private readonly partitionKeysByMessageId = new Map<string, string>()
  private readonly sessionsByPartitionKey = new Map<string, Session>()
  private readonly queueStatesByQueueName = new Map<string, QueueState>()
  private readonly queueStates = [] as QueueState[]
  // private readonly queueStatesByPartitionKey = new Map<string, QueueState>()

  constructor(
    ackMessages: (deliveryTag1: number, deliveryTag2: number) => Promise<void>,
    queueCount: number,
    onMessageProcessed: () => void
  ) {
    this.ackMessages = ackMessages
    this.onMessageProcessed = onMessageProcessed

    const queueNames = Array.from({length: queueCount}, (_, i) => outputQueueName(i + 1))
    for (const queueName of queueNames) {
      const queueState = {queueName, partitionKeys: new Set<string>(), messageCount: 0}
      this.queueStates.push(queueState)
      this.queueStatesByQueueName.set(queueName, queueState)
    }
  }

  public async registerOutputDeliveryTag(messageId: string, deliveryTag: number) {
    const responseDeliveryTag = this.responseDeliveryTagsByMessageId.get(messageId)
    if (responseDeliveryTag !== undefined) {
      this.responseDeliveryTagsByMessageId.delete(messageId)
      await this.processMessage(messageId, deliveryTag, responseDeliveryTag)
    } else {
      this.outputDeliveryTagsByMessageId.set(messageId, deliveryTag)
    }
  }

  public async registerResponseDeliveryTag(messageId: string, deliveryTag: number) {
    const outputDeliveryTag = this.outputDeliveryTagsByMessageId.get(messageId)
    if (outputDeliveryTag !== undefined) {
      this.outputDeliveryTagsByMessageId.delete(messageId)
      await this.processMessage(messageId, outputDeliveryTag, deliveryTag)
    } else {
      this.responseDeliveryTagsByMessageId.set(messageId, deliveryTag)
    }
  }

  public registerOutputMessage(messageId: string, partitionKey: string, queueName?: string): {queueName: string} {
    const session = this.getOrAddSessions(partitionKey, queueName)
    const queueState = this.getOrAddQueueState(session.queueName)
    // TODO: ? Assert queueState + session limits

    session.messageCount += 1
    queueState.messageCount += 1

    if (session.messageCount === 1) {
      queueState.partitionKeys.add(partitionKey)
    }

    this.partitionKeysByMessageId.set(messageId, partitionKey)

    return {queueName: session.queueName}
  }

  private getOrAddSessions(partitionKey: string, queueName?: string): Session {
    let session = this.sessionsByPartitionKey.get(partitionKey)
    if (session && queueName && session.queueName !== queueName) {
      throw new Error(
        `Can't bind partition key "${partitionKey}" to queue "${queueName}" ` +
          `because it's already bound to queue "${session.queueName}"`
      )
    }

    if (!session) {
      queueName = queueName || this.getQueueNameWithMinMessageCount()
      session = {partitionKey, queueName, messageCount: 0}
      this.sessionsByPartitionKey.set(partitionKey, session)
    }

    return session
  }

  private getOrAddQueueState(queueName: string) {
    let queueState = this.queueStatesByQueueName.get(queueName)
    if (!queueState) {
      throw new Error(`Unknown queue "${queueName}"`)
    }
    return queueState
  }

  private getQueueNameWithMinMessageCount() {
    let result = this.queueStates[0]
    for (let i = 1; i < this.queueStates.length; i++) {
      if (this.queueStates[i].messageCount < result.messageCount) {
        result = this.queueStates[i]
      }
    }

    return result.queueName
  }

  private async processMessage(messageId: string, outputDeliveryTag: number, responseDeliveryTag: number) {
    await this.ackMessages(outputDeliveryTag, responseDeliveryTag)

    const partitionKey = this.partitionKeysByMessageId.get(messageId)!
    this.partitionKeysByMessageId.delete(messageId)

    const session = this.sessionsByPartitionKey.get(partitionKey)!
    session.messageCount -= 1
    if (!session.messageCount) {
      this.sessionsByPartitionKey.delete(partitionKey)
    }

    const queueState = this.queueStatesByQueueName.get(session.queueName)!
    queueState.messageCount -= 1
    if (!session.messageCount) {
      queueState.partitionKeys.delete(session.partitionKey)
    }

    this.onMessageProcessed()
  }
}
