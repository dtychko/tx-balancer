import {MessageProperties} from 'amqplib'
import {Message, MessageCache, MessageStorage} from '../balancer-core'
import PartitionGroupQueue from './PartitionGroupQueue'
import {PartitionGroupGuard} from '../QState'

interface MessageData {
  partitionGroup: string
  partitionKey: string
  content: Buffer
  properties: MessageProperties
}

export interface MessageRef {
  messageId: number
  partitionGroup: string
  partitionKey: string
}

export default class MessageBalancer {
  private readonly storage: MessageStorage
  private readonly cache: MessageCache
  private readonly onPartitionAdded: (partitionGroup: string, partitionKey: string) => void

  private readonly partitionGroupQueue = new PartitionGroupQueue()
  private isInitialized = false

  constructor(params: {
    storage: MessageStorage
    cache: MessageCache
    onPartitionAdded: (partitionGroup: string, partitionKey: string) => void
  }) {
    this.storage = params.storage
    this.cache = params.cache
    this.onPartitionAdded = params.onPartitionAdded
  }

  public async init() {
    const messageCountLimit = 10000
    let messageCount = 0
    let fromRow = 1
    let toRow = 1
    let iteration

    console.log(await ((this.storage as any).db.readStats()))

    for (iteration = 1; ; iteration++) {
      const messages = await this.storage.readPartitionMessagesOrderedById({
        fromRow,
        toRow,
        contentSizeLimit: this.cache.maxSize - this.cache.size()
      })

      if (!messages.length) {
        break
      }

      const partitions = new Set<string>()

      for (const message of messages) {
        const {messageId, partitionGroup, partitionKey} = message
        this.partitionGroupQueue.enqueue(messageId, partitionGroup, partitionKey)

        if (message.type === 'full') {
          this.cache.addMessage(message)
        }

        partitions.add(JSON.stringify({partitionGroup, partitionKey}))
      }

      messageCount += messages.length
      fromRow = toRow + 1
      toRow = toRow + Math.max(1, Math.floor(messageCountLimit / partitions.size))
    }

    this.isInitialized = true
    console.log(`MessageBalancer initialized with ${messageCount} messages after ${iteration} iterations`)
  }

  public async storeMessage(messageData: MessageData) {
    this.assertInitialized()

    const message = await this.storage.createMessage({
      receivedDate: new Date(),
      ...messageData
    })
    const {messageId, partitionGroup, partitionKey} = message

    const {partitionKeyAdded} = this.partitionGroupQueue.enqueue(messageId, partitionGroup, partitionKey)
    this.cache.addMessage(message)

    if (partitionKeyAdded) {
      this.onPartitionAdded(partitionGroup, partitionKey)
    }
  }

  public tryDequeueMessage(canProcess: (partitionGroup: string) => PartitionGroupGuard): MessageRef | undefined {
    this.assertInitialized()
    return this.partitionGroupQueue.tryDequeue(canProcess)
  }

  public async getMessage(messageId: number): Promise<Message> {
    this.assertInitialized()
    return this.cache.getAndRemoveMessage(messageId) || (await this.storage.readMessage(messageId))!
  }

  public async removeMessage(messageId: number) {
    if (messageId % 1000 === 0) {
      console.log(`removeMessage(${messageId})`)
    }

    this.assertInitialized()
    await this.storage.removeMessage(messageId)

    if (messageId % 1000 === 0) {
      console.log(`removeMessage(${messageId}) completed`)
    }
  }

  private assertInitialized() {
    if (!this.isInitialized) {
      throw new Error(`Message service isn't initialized`)
    }
  }
}
