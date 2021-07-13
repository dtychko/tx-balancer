import {Db, MessageCache, MessageStorage} from '@targetprocess/balancer-core'
import {Channel, ConfirmChannel, Connection, Message} from 'amqplib'
import {Pool} from 'pg'
import {connect} from './amqp/connect'
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
  responseQueueName,
  singlePartitionGroupLimit,
  singlePartitionKeyLimit
} from './config'
import MirrorQueueConsumer from './MirrorQueueConsumer'
import PublishLoop from './PublishLoop'
import {QState} from './QState'
import QueueConsumer from './QueueConsumer'
import {compareExchangeState, deferred} from './stateMachine'
import {sumCharCodes} from './utils'

interface ServiceContext {
  compareExchangeState: (toState: ServiceState, fromState: ServiceState) => boolean
}

interface ServiceState {
  onEnter?: () => void

  start: (pool: Pool) => Promise<void>
  stop: () => Promise<void>
  destroy: () => Promise<void>
}

interface ServiceDependencies {
  consumeConnection?: Connection
  publishConnection?: Connection
  inputCh?: Channel
  qStateCh?: ConfirmChannel
  loopCh?: ConfirmChannel
  publishLoop?: PublishLoop
  messageBalancer?: MessageBalancer3
  qState?: QState
  mirrorQueueConsumers?: MirrorQueueConsumer[]
  responseQueueConsumer?: QueueConsumer
  inputQueueConsumer?: QueueConsumer
}

export default class Service {
  private readonly ctx: ServiceContext
  private readonly state: {value: ServiceState}

  constructor() {
    this.ctx = {
      compareExchangeState: (toState, fromState) => {
        return compareExchangeState(this.state, toState, fromState)
      }
    }

    this.state = {value: new StoppedState(this.ctx)}

    if (this.state.value.onEnter) {
      this.state.value.onEnter()
    }
  }

  public start(pool: Pool) {
    return this.state.value.start(pool)
  }

  public stop() {
    return this.state.value.stop()
  }

  public destroy() {
    return this.state.value.destroy()
  }
}

class StartingState implements ServiceState {
  private deps!: Promise<ServiceDependencies>

  constructor(
    private readonly ctx: ServiceContext,
    private readonly args: {pool: Pool; onStarted: (err?: Error) => void}
  ) {}

  public async onEnter() {
    this.deps = (async () => await createDeps(this.args.pool))()

    try {
      const deps = await this.deps
      this.ctx.compareExchangeState(new StartedState(this.ctx, {deps}), this)
      this.args.onStarted()
    } catch (err) {
      this.ctx.compareExchangeState(new StoppedState(this.ctx), this)
      this.args.onStarted(err)
    }
  }

  public async start() {
    throw new Error('Unable to start. Service is already starting')
  }

  public async stop() {
    throw new Error('Unable to stop. Service is still starting')
  }

  public async destroy() {
    await deferred(onDestroyed => {
      this.ctx.compareExchangeState(new DestroyedState(this.ctx, {deps: this.deps, onDestroyed}), this)
    })
  }
}

class StartedState implements ServiceState {
  constructor(private readonly ctx: ServiceContext, private readonly args: {deps: ServiceDependencies}) {}

  public async start() {
    throw new Error('Unable to start. Service is already started')
  }

  public async stop() {
    await deferred(onStopped => {
      this.ctx.compareExchangeState(new StoppingState(this.ctx, {deps: this.args.deps, onStopped}), this)
    })
  }

  public async destroy() {
    await deferred(onDestroyed => {
      this.ctx.compareExchangeState(
        new DestroyedState(this.ctx, {deps: Promise.resolve(this.args.deps), onDestroyed}),
        this
      )
    })
  }
}

class StoppingState implements ServiceState {
  constructor(
    private readonly ctx: ServiceContext,
    private readonly args: {deps: ServiceDependencies; onStopped: (err?: Error) => void}
  ) {}

  public async onEnter() {
    try {
      await destroyDeps(this.args.deps)

      this.ctx.compareExchangeState(new StoppedState(this.ctx), this)
      this.args.onStopped()
    } catch (err) {
      this.args.onStopped(err)
    }
  }

  public async start() {
    throw new Error('Unable to start. Service is still stopping')
  }

  public async stop() {
    throw new Error('Unable to stop. Service is already stopping')
  }

  public async destroy() {
    // await deferred(onDestroyed => {
    //   this.ctx.compareExchangeState(new DestroyedState(this.ctx, {deps: Promise.resolve({}), onDestroyed}), this)
    // })
  }
}

class StoppedState implements ServiceState {
  constructor(private readonly ctx: ServiceContext) {}

  public async start(pool: Pool) {
    await deferred(onStarted => {
      this.ctx.compareExchangeState(new StartingState(this.ctx, {pool, onStarted}), this)
    })
  }

  public async stop() {
    throw new Error("Unable to stop. Service wasn't started")
  }

  public async destroy() {
    await deferred(onDestroyed => {
      this.ctx.compareExchangeState(new DestroyedState(this.ctx, {deps: Promise.resolve({}), onDestroyed}), this)
    })
  }
}

class ErrorState {}

class DestroyedState implements ServiceState {
  constructor(
    private readonly ctx: ServiceContext,
    private readonly args: {deps: Promise<ServiceDependencies>; onDestroyed: (err?: Error) => void}
  ) {}

  public async onEnter() {
    try {
      const deps = await this.args.deps
      await destroyDeps(deps)

      this.args.onDestroyed()
    } catch (err) {
      this.args.onDestroyed(err)
    }
  }

  public async start(pool: Pool) {
    throw new Error('Unable to start. Service is destroyed')
  }

  public async stop() {
    throw new Error('Unable to stop. Service is destroyed')
  }

  public async destroy() {
    throw new Error('Service is already destroyed')
  }
}

async function createDeps(pool: Pool) {
  const deps: ServiceDependencies = {}

  try {
    deps.consumeConnection = await connect(amqpUri)
    deps.publishConnection = await connect(amqpUri)
    console.log('created connections')

    deps.inputCh = await deps.consumeConnection.createChannel()
    deps.qStateCh = await deps.consumeConnection.createConfirmChannel()
    deps.loopCh = await deps.publishConnection.createConfirmChannel()
    console.log('created channels')

    await deps.inputCh.prefetch(inputChannelPrefetchCount, false)

    // Queues purge should be configurable
    await assertResources(deps.qStateCh, false)
    console.log('asserted resources')

    deps.publishLoop = createPublishLoop()
    console.log('created PublishLoop')

    deps.messageBalancer = createMessageBalancer(pool, deps.publishLoop)
    await deps.messageBalancer.init({perRequestMessageCountLimit: 10000, initCache: true})
    console.log('created and initialized BalancedQueue')

    deps.qState = createQState(deps.qStateCh, deps.publishLoop)
    console.log('created QState')

    deps.mirrorQueueConsumers = createMirrorQueueConsumers(deps.qStateCh, deps.qState)
    await Promise.all(deps.mirrorQueueConsumers.map(consumer => consumer.init()))

    deps.responseQueueConsumer = createResponseQueueConsumer(deps.qStateCh, deps.qState)
    await deps.responseQueueConsumer.init()

    deps.inputQueueConsumer = createInputQueueConsumer(deps.inputCh, deps.messageBalancer)
    await deps.inputQueueConsumer.init()
    console.log('consumed input queue')

    const publisher = new Publisher(deps.loopCh)
    deps.publishLoop.connectTo({publisher, qState: deps.qState, messageBalancer: deps.messageBalancer})
    console.log('connected PublishLoop')

    deps.publishLoop.trigger()
    console.log('started PublishLoop')

    return deps
  } catch (err) {
    await destroyDeps(deps)
    throw err
  }
}

async function destroyDeps(deps: ServiceDependencies) {
  await Promise.all([
    destroySafe(deps.inputQueueConsumer),
    destroySafe(deps.responseQueueConsumer),
    ...(deps.mirrorQueueConsumers || []).map(consumer => destroySafe(consumer)),
    destroySafe(deps.publishLoop)
  ])
  await Promise.all([closeSafe(deps.inputCh), closeSafe(deps.qStateCh), closeSafe(deps.loopCh)])
  await Promise.all([closeSafe(deps.publishConnection), closeSafe(deps.consumeConnection)])
}

async function destroySafe(destroyable?: {destroy: () => Promise<void>}): Promise<Error | void> {
  try {
    if (destroyable) {
      await destroyable.destroy()
    }
  } catch (err) {
    return err
  }
}

async function closeSafe(closable?: Channel | Connection): Promise<Error | void> {
  try {
    if (closable) {
      await closable.close()
    }
  } catch (err) {
    return err
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
