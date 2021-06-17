import {Message} from './Db'
import {emptyLogger, MessageBalancerLogger} from './MessageBalancer.Logger'
import {MessageBalancerGaugeMetric} from './MessageBalancer.Metrics'

const defaultMaxSize = 128 * 1024 * 1024 // 128 Mb

export default class MessageCache {
  public readonly maxSize: number
  public readonly maxMessageSize?: number
  private readonly messageCountMetric?: MessageBalancerGaugeMetric
  private readonly messageSizeMetric?: MessageBalancerGaugeMetric
  private readonly logger: MessageBalancerLogger
  private readonly cache = new Map<number, Message>()
  private cacheSize = 0 as number

  constructor(params?: {
    maxSize?: number
    maxMessageSize?: number
    messageCountMetric?: MessageBalancerGaugeMetric
    messageSizeMetric?: MessageBalancerGaugeMetric
    logger?: MessageBalancerLogger
  }) {
    this.maxSize = params?.maxSize || defaultMaxSize
    this.maxMessageSize = params?.maxMessageSize
    this.messageCountMetric = params?.messageCountMetric
    this.messageSizeMetric = params?.messageSizeMetric
    this.logger = params?.logger || emptyLogger
  }

  public addMessage(message: Message): boolean {
    const result = this.addMessageImpl(message)
    if (result) {
      this.updateMetrics()
    }

    return result
  }

  public addMessages(messages: Message[]): number {
    const result = messages.reduce((acc, message) => (this.addMessageImpl(message) ? acc + 1 : acc), 0)
    if (result) {
      this.updateMetrics()
    }

    return result
  }

  private addMessageImpl(message: Message) {
    const {messageId, content} = message
    const messageSize = content.length

    if (this.maxMessageSize && messageSize > this.maxMessageSize) {
      this.logger.log({
        level: 'warn',
        message: `Unable to add message#${messageId} to cache because max message size is exceeded`,
        eventType: 'message-cache/add-message/max-message-size-exceeded',
        meta: {messageSize}
      })

      return false
    }

    if (this.maxSize && messageSize + this.cacheSize > this.maxSize) {
      this.logger.log({
        level: 'warn',
        message: `Unable to add message#${messageId} to cache because max cache size is exceeded`,
        eventType: 'message-cache/add-message/max-cache-size-exceeded',
        meta: {cacheSize: this.cacheSize, messageSize}
      })

      return false
    }

    if (this.cache.has(messageId)) {
      this.logger.log({
        level: 'warn',
        message: `Unable to add message#${messageId} to cache because message with the same id is already added`,
        eventType: 'message-cache/add-message/duplicate-message-id'
      })

      return false
    }

    this.cache.set(message.messageId, message)
    this.cacheSize += message.content.length

    return true
  }

  public getAndRemoveMessage(messageId: number): Message | undefined {
    const message = this.cache.get(messageId)
    if (!message) {
      return undefined
    }

    this.cache.delete(messageId)
    this.cacheSize -= message.content.length

    this.updateMetrics()

    return message
  }

  public removeMessages(shouldRemove: (message: Message) => boolean): number {
    const removeMessageIds = [] as number[]

    for (const [messageId, message] of this.cache.entries()) {
      if (shouldRemove(message)) {
        removeMessageIds.push(messageId)
        this.cacheSize -= message.content.length
      }
    }

    for (const messageId of removeMessageIds) {
      this.cache.delete(messageId)
    }

    this.updateMetrics()

    return removeMessageIds.length
  }

  public count() {
    return this.cache.size
  }

  public size() {
    return this.cacheSize
  }

  public clear() {
    this.cache.clear()
    this.cacheSize = 0

    this.updateMetrics()
  }

  private updateMetrics() {
    if (this.messageCountMetric) {
      this.messageCountMetric.set(this.cache.size)
    }

    if (this.messageSizeMetric) {
      this.messageSizeMetric.set(this.cacheSize)
    }
  }
}
