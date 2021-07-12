import {Db, MessageCache, MessageStorage, migrateDb} from '@targetprocess/balancer-core'
import {Channel, ConfirmChannel, Connection, Message} from 'amqplib'
import {Pool} from 'pg'
import {Publisher} from './amqp/Publisher'
import {assertResources} from './assertResources'
import {Db3} from './balancing/Db3'
import MessageBalancer3 from './balancing/MessageBalancer3'
import MessageStorage3 from './balancing/MessageStorage3'
import {
  inputChannelPrefetchCount,
  inputQueueName,
  mirrorQueueName,
  outputQueueCount,
  outputQueueLimit,
  outputQueueName,
  partitionGroupHeader,
  partitionKeyHeader,
  responseQueueName,
  singlePartitionGroupLimit,
  singlePartitionKeyLimit
} from './config'
import PublishLoop from './PublishLoop'
import {QState} from './QState'
import QueueConsumer from './QueueConsumer'
import {sumCharCodes} from './utils'

export default class Service {
  constructor(private readonly args: {consumeConnection: Connection; publishConnection: Connection; pool: Pool}) {}

  public async start() {
    const inputCh = await this.args.consumeConnection.createChannel()
    const qStateCh = await this.args.consumeConnection.createConfirmChannel()
    const loopCh = await this.args.publishConnection.createConfirmChannel()
    console.log('created channels')

    await inputCh.prefetch(inputChannelPrefetchCount, false)

    // Queues purge should be configurable
    await assertResources(qStateCh, true)
    console.log('asserted resources')

    const publishLoop = this.createPublishLoop()
    console.log('created PublishLoop')

    const messageBalancer = this.createMessageBalancer(publishLoop)
    await messageBalancer.init({perRequestMessageCountLimit: 10000, initCache: true})
    console.log('created and initialized BalancedQueue')

    const qState = await this.createQState(qStateCh, publishLoop)
    console.log('created QState')

    const inputQueueConsumer = this.createInputQueueConsumer(inputCh, messageBalancer)
    await inputQueueConsumer.init()
    console.log('consumed input queue')

    const publisher = new Publisher(loopCh)
    publishLoop.connectTo({publisher, qState, messageBalancer})
    console.log('connected PublishLoop')

    publishLoop.trigger()
    console.log('started PublishLoop')
  }

  public async destroy() {}

  private createPublishLoop() {
    return new PublishLoop({
      mirrorQueueName,
      partitionGroupHeader,
      partitionKeyHeader,
      onError: () => {}
    })
  }

  private createMessageBalancer(publishLoop: PublishLoop) {
    const db = new Db({pool: this.args.pool, useQueryCache: true})
    const db3 = new Db3({pool: this.args.pool})
    const storage = new MessageStorage({db, batchSize: 100})
    const storage3 = new MessageStorage3({db3})
    const cache = new MessageCache({maxSize: 1024 ** 3})

    return new MessageBalancer3({
      storage,
      storage3,
      cache,
      onPartitionAdded: () => publishLoop.trigger()
    })
  }

  private createQState(ch: Channel, publishLoop: PublishLoop) {
    return new QState({
      onMessageProcessed: (_, mirrorDeliveryTag, responseDeliveryTag) => {
        ch.ack({fields: {deliveryTag: mirrorDeliveryTag}} as Message)
        ch.ack({fields: {deliveryTag: responseDeliveryTag}} as Message)
        publishLoop.trigger()
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
  }

  private createMirrorQueueConsumers() {}

  private createResponseQueueConsumer(ch: Channel, qState: QState) {
    return new QueueConsumer({
      ch,
      queueName: responseQueueName,
      processMessage: msg => {
        const messageId = msg.properties.messageId
        const deliveryTag = msg.fields.deliveryTag
        const {registered} = qState.registerResponseDeliveryTag(messageId, deliveryTag)

        return registered ? Promise.resolve() : Promise.resolve({ack: true})
      },
      onError: err => console.error(err)
    })
  }

  private createInputQueueConsumer(ch: Channel, messageBalancer: MessageBalancer3) {
    return new QueueConsumer({
      ch,
      queueName: inputQueueName,
      processMessage: async msg => {
        const {content, properties} = msg
        const partitionGroup = msg.properties.headers[partitionGroupHeader]
        const partitionKey = msg.properties.headers[partitionKeyHeader]
        await messageBalancer.storeMessage({partitionGroup, partitionKey, content, properties})

        return {ack: true}
      },
      onError: err => console.error(err)
    })
  }
}
