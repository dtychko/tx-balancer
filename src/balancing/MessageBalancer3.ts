import {MessageCache, MessageStorage} from '@targetprocess/balancer-core'
import {MessageProperties} from 'amqplib'
import MessageStorage3 from './MessageStorage3'
import PartitionGroupQueue3 from './PartitionGroupQueue3'
import {PartitionGroupGuard} from '../QState'

interface Message extends MessageData {
  messageId: number
  receivedDate: Date
}

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

export default class MessageBalancer3 {
  private readonly storage: MessageStorage
  private readonly storage3: MessageStorage3
  private readonly cache: MessageCache
  private readonly onPartitionAdded: (partitionGroup: string, partitionKey: string) => void

  private readonly partitionGroupQueue = new PartitionGroupQueue3()
  private isInitialized = false

  constructor(params: {
    storage: MessageStorage
    storage3: MessageStorage3
    cache: MessageCache
    onPartitionAdded: (partitionGroup: string, partitionKey: string) => void
  }) {
    this.storage = params.storage
    this.storage3 = params.storage3
    this.cache = params.cache
    this.onPartitionAdded = params.onPartitionAdded
  }

  public status() {
    return {
      cachedMessageCount: this.cache.count(),
      cachedMessageSize: this.cache.size(),
      ...this.partitionGroupQueue.status()
    }
  }

  public size() {
    return this.partitionGroupQueue.size()
  }

  public async init(params: {perRequestMessageCountLimit: number; initCache: boolean}) {
    const {perRequestMessageCountLimit, initCache} = params
    const startedAt = Date.now()
    let fromRow = 1
    let toRow = 1
    let iteration

    for (iteration = 1; ; iteration++) {
      const contentSizeLimit = initCache ? this.cache.maxSize - this.cache.size() : 0
      const messages = await this.storage3.readPartitionMessagesOrderedById({fromRow, toRow, contentSizeLimit})

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

      fromRow = toRow + 1
      toRow = toRow + Math.max(1, Math.floor(perRequestMessageCountLimit / partitions.size))
    }

    this.isInitialized = true

    const duration = Date.now() - startedAt
    console.log(
      `MessageBalancer initialized with ${this.partitionGroupQueue.size()} messages after ${iteration} iterations in ${duration} ms`
    )
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
    const message = this.cache.getAndRemoveMessage(messageId) || (await this.storage.readMessage(messageId))!
    return message as Message
  }

  public async removeMessage(messageId: number) {
    this.assertInitialized()
    await this.storage.removeMessage(messageId)
  }

  private assertInitialized() {
    if (!this.isInitialized) {
      throw new Error(`Message service isn't initialized`)
    }
  }
}
