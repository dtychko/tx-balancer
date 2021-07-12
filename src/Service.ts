import {Db, MessageCache, MessageStorage, migrateDb} from '@targetprocess/balancer-core'
import {Channel, ConfirmChannel, Connection, Message} from 'amqplib'
import {Pool} from 'pg'
import {Publisher} from './amqp/Publisher'
import {assertResources} from './assertResources'
import {Db3} from './balancing/Db3'
import MessageBalancer3 from './balancing/MessageBalancer3'
import MessageStorage3 from './balancing/MessageStorage3'
import {
  amqpUri,
  inputChannelPrefetchCount,
  inputQueueName,
  mirrorQueueName,
  outputQueueCount,
  outputQueueLimit,
  outputQueueName,
  partitionGroupHeader,
  partitionKeyHeader,
  postgresConnectionString,
  postgresPoolMax,
  responseQueueName,
  singlePartitionGroupLimit,
  singlePartitionKeyLimit
} from './config'
import PublishLoop from './PublishLoop'
import {QState} from './QState'
import QueueConsumer from './QueueConsumer'
import {sumCharCodes} from './utils'
import MirrorQueueConsumer from './MirrorQueueConsumer'
import {connect} from './amqp/connect'

export default class Service {
  private connections: Connection[] = []
  private channels: Channel[] = []
  private publishLoop!: PublishLoop
  private messageBalancer!: MessageBalancer3
  private qState!: QState
  private mirrorQueueConsumers!: MirrorQueueConsumer[]
  private responseQueueConsumer!: QueueConsumer
  private inputQueueConsumer!: QueueConsumer

  public status() {
    return {
      messageBalancerSize: this.messageBalancer && this.messageBalancer.size(),
      qStateSize: this.qState && this.qState.size()
    }
  }

  public async start(pool: Pool) {
    const consumeConnection = await connect(amqpUri)
    const publishConnection = await connect(amqpUri)
    this.connections.push(consumeConnection, publishConnection)
    console.log('created connections')

    const inputCh = await consumeConnection.createChannel()
    const qStateCh = await consumeConnection.createConfirmChannel()
    const loopCh = await publishConnection.createConfirmChannel()
    this.channels.push(inputCh, qStateCh, loopCh)
    console.log('created channels')

    await inputCh.prefetch(inputChannelPrefetchCount, false)

    // Queues purge should be configurable
    await assertResources(qStateCh, false)
    console.log('asserted resources')

    this.publishLoop = createPublishLoop()
    console.log('created PublishLoop')

    this.messageBalancer = createMessageBalancer(pool, this.publishLoop)
    await this.messageBalancer.init({perRequestMessageCountLimit: 10000, initCache: true})
    console.log('created and initialized BalancedQueue')

    this.qState = createQState(qStateCh, this.publishLoop)
    console.log('created QState')

    this.mirrorQueueConsumers = createMirrorQueueConsumers(qStateCh, this.qState)
    await Promise.all(this.mirrorQueueConsumers.map(consumer => consumer.init()))

    this.responseQueueConsumer = createResponseQueueConsumer(qStateCh, this.qState)
    await this.responseQueueConsumer.init()

    this.inputQueueConsumer = createInputQueueConsumer(inputCh, this.messageBalancer)
    await this.inputQueueConsumer.init()
    console.log('consumed input queue')

    const publisher = new Publisher(loopCh)
    this.publishLoop.connectTo({publisher, qState: this.qState, messageBalancer: this.messageBalancer})
    console.log('connected PublishLoop')

    this.publishLoop.trigger()
    console.log('started PublishLoop')
  }

  public async destroy() {
    await Promise.all([
      this.inputQueueConsumer.destroy(),
      this.responseQueueConsumer.destroy(),
      ...this.mirrorQueueConsumers.map(consumer => consumer.destroy()),
      this.publishLoop.destroy()
    ])
    await Promise.all(this.channels.map(ch => ch.close()))
    await Promise.all(this.connections.map(conn => conn.close()))
  }
}

function createPublishLoop() {
  return new PublishLoop({
    mirrorQueueName,
    partitionGroupHeader,
    partitionKeyHeader,
    onError: () => {}
  })
}

function createMessageBalancer(pool: Pool, publishLoop: PublishLoop) {
  const db = new Db({pool, useQueryCache: true})
  const db3 = new Db3({pool})
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

function createQState(ch: Channel, publishLoop: PublishLoop) {
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

function createMirrorQueueConsumers(ch: ConfirmChannel, qState: QState) {
  const consumers = []

  for (let queueIndex = 1; queueIndex <= outputQueueCount; queueIndex++) {
    const outputQueue = outputQueueName(queueIndex)
    const mirrorQueue = mirrorQueueName(outputQueue)

    consumers.push(
      new MirrorQueueConsumer({
        ch,
        qState,
        onError: err => console.error(err),
        mirrorQueueName: mirrorQueue,
        outputQueueName: outputQueue,
        partitionGroupHeader,
        partitionKeyHeader
      })
    )
  }

  return consumers
}

function createResponseQueueConsumer(ch: Channel, qState: QState) {
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

function createInputQueueConsumer(ch: Channel, messageBalancer: MessageBalancer3) {
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
