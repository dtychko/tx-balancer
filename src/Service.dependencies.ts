import {Db, MessageCache, MessageStorage, migrateDb} from '@targetprocess/balancer-core'
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
  postgresConnectionString,
  postgresPoolMax,
  responseQueueName,
  singlePartitionGroupLimit,
  singlePartitionKeyLimit
} from './config'
import MirrorQueueConsumer from './MirrorQueueConsumer'
import PublishLoop from './PublishLoop'
import {QState} from './QState'
import QueueConsumer from './amqp/QueueConsumer'
import {sumCharCodes} from './utils'
import {CancellationToken} from './stateMachine'

export interface ServiceDependencies {
  pool?: Pool
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

export async function createDependencies(args: {onError: (err: Error) => void; cancellationToken: CancellationToken}) {
  const {onError, cancellationToken} = args
  const deps: ServiceDependencies = {}

  const throwIfCanceled = <T>(create: () => Promise<T>): Promise<T> => {
    if (cancellationToken.isCanceled) {
      throw new Error('Creation of dependencies is cancelled')
    }

    return create()
  }

  try {
    deps.pool = new Pool({
      connectionString: postgresConnectionString,
      max: postgresPoolMax
    })
    await throwIfCanceled(() => migrateDb({pool: deps.pool!}))
    console.log('migrated DB')

    deps.consumeConnection = await throwIfCanceled(() => connect(amqpUri))
    deps.consumeConnection.on('close', reason => {
      if (reason) {
        onError(new Error(`Connection#consume was unexpectedly closed with reason: ${reason}`))
      }
    })

    deps.publishConnection = await throwIfCanceled(() => connect(amqpUri))
    deps.publishConnection.on('close', reason => {
      if (reason) {
        onError(new Error(`Connection#publish was unexpectedly closed with reason: ${reason}`))
      }
    })
    console.log('created connections')

    deps.inputCh = await throwIfCanceled(async () => await deps.consumeConnection!.createChannel())
    deps.inputCh.on('close', reason => {
      if (reason) {
        onError(new Error(`Channel#input was unexpectedly closed with reason: ${reason}`))
      }
    })

    deps.qStateCh = await throwIfCanceled(async () => await deps.consumeConnection!.createConfirmChannel())
    deps.qStateCh.on('close', reason => {
      if (reason) {
        onError(new Error(`Channel#qState was unexpectedly closed with reason: ${reason}`))
      }
    })

    deps.loopCh = await throwIfCanceled(async () => await deps.publishConnection!.createConfirmChannel())
    deps.loopCh.on('close', reason => {
      if (reason) {
        onError(new Error(`Channel#loop was unexpectedly closed with reason: ${reason}`))
      }
    })
    console.log('created channels')

    await throwIfCanceled(async () => await deps.inputCh!.prefetch(inputChannelPrefetchCount, false))

    // Queues purge should be configurable
    await throwIfCanceled(() => assertResources(deps.qStateCh!, false))
    console.log('asserted resources')

    deps.publishLoop = createPublishLoop({onError})
    console.log('created PublishLoop')

    deps.messageBalancer = createMessageBalancer({pool: deps.pool, publishLoop: deps.publishLoop})
    await throwIfCanceled(() => deps.messageBalancer!.init({perRequestMessageCountLimit: 10000, initCache: true}))
    console.log('created and initialized BalancedQueue')

    deps.qState = createQState({ch: deps.qStateCh, publishLoop: deps.publishLoop})
    console.log('created QState')

    deps.mirrorQueueConsumers = createMirrorQueueConsumers({ch: deps.qStateCh, qState: deps.qState, onError})
    await throwIfCanceled(() => Promise.all(deps.mirrorQueueConsumers!.map(consumer => consumer.init())))

    deps.responseQueueConsumer = createResponseQueueConsumer({ch: deps.qStateCh, qState: deps.qState, onError})
    await throwIfCanceled(() => deps.responseQueueConsumer!.init())

    deps.inputQueueConsumer = createInputQueueConsumer({
      ch: deps.inputCh,
      messageBalancer: deps.messageBalancer,
      onError
    })
    await throwIfCanceled(() => deps.inputQueueConsumer!.init())
    console.log('consumed input queue')

    const publisher = new Publisher(deps.loopCh)
    deps.publishLoop.connectTo({publisher, qState: deps.qState, messageBalancer: deps.messageBalancer})
    console.log('connected PublishLoop')

    deps.publishLoop.trigger()
    console.log('started PublishLoop')

    return deps
  } catch (err) {
    console.error(err)
    await destroyDependencies(deps)
    throw err
  }
}

export async function destroyDependencies(deps: ServiceDependencies) {
  const errors = [
    ...(await Promise.all([
      destroySafe(deps.inputQueueConsumer),
      destroySafe(deps.responseQueueConsumer),
      ...(deps.mirrorQueueConsumers || []).map(consumer => destroySafe(consumer)),
      destroySafe(deps.publishLoop)
    ])),
    ...(await Promise.all([closeSafe(deps.inputCh), closeSafe(deps.qStateCh), closeSafe(deps.loopCh)])),
    ...(await Promise.all([closeSafe(deps.publishConnection), closeSafe(deps.consumeConnection)])),
    await safe(async () => {
      if (deps.pool) {
        await deps.pool.end()
      }
    })
  ].filter(err => err) as Error[]

  if (errors.length) {
    throw new Error(`Unexpected errors (count: ${errors.length}) occurred during destroying dependencies`)
  }
}

async function destroySafe(destroyable?: {destroy: () => Promise<void>}): Promise<Error | undefined> {
  return await safe(async () => {
    if (destroyable) {
      await destroyable.destroy()
    }
  })
}

async function closeSafe(closable?: Channel | Connection): Promise<Error | undefined> {
  return await safe(async () => {
    if (closable) {
      await closable.close()
    }
  })
}

async function safe(action: () => Promise<void>): Promise<Error | undefined> {
  try {
    await action()
    return undefined
  } catch (err) {
    console.error(err)
    return err
  }
}

function createPublishLoop(args: {onError: (err: Error) => void}) {
  const {onError} = args

  return new PublishLoop({
    mirrorQueueName,
    partitionGroupHeader,
    partitionKeyHeader,
    onError
  })
}

function createMessageBalancer(args: {pool: Pool; publishLoop: PublishLoop}) {
  const {pool, publishLoop} = args
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

function createQState(args: {ch: Channel; publishLoop: PublishLoop}) {
  const {ch, publishLoop} = args

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

function createMirrorQueueConsumers(args: {ch: ConfirmChannel; qState: QState; onError: (err: Error) => void}) {
  const {ch, qState, onError} = args
  const consumers = []

  for (let queueIndex = 1; queueIndex <= outputQueueCount; queueIndex++) {
    const outputQueue = outputQueueName(queueIndex)
    const mirrorQueue = mirrorQueueName(outputQueue)

    consumers.push(
      new MirrorQueueConsumer({
        ch,
        qState,
        onError,
        mirrorQueueName: mirrorQueue,
        outputQueueName: outputQueue,
        partitionGroupHeader,
        partitionKeyHeader
      })
    )
  }

  return consumers
}

function createResponseQueueConsumer(args: {ch: Channel; qState: QState; onError: (err: Error) => void}) {
  const {ch, qState, onError} = args

  return new QueueConsumer({
    ch,
    queueName: responseQueueName,
    processMessage: msg => {
      const messageId = msg.properties.messageId
      const deliveryTag = msg.fields.deliveryTag
      const {registered} = qState.registerResponseDeliveryTag(messageId, deliveryTag)

      return registered ? Promise.resolve() : Promise.resolve({ack: true})
    },
    onError
  })
}

function createInputQueueConsumer(args: {
  ch: Channel
  messageBalancer: MessageBalancer3
  onError: (err: Error) => void
}) {
  const {ch, messageBalancer, onError} = args

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
    onError
  })
}
