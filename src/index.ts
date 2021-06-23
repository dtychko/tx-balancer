import {Db, MessageCache, MessageStorage, migrateDb} from '@targetprocess/balancer-core'
import * as amqp from 'amqplib'
import {Db3} from './balancing/Db3'
import MessageBalancer3 from './balancing/MessageBalancer3'
import MessageStorage3 from './balancing/MessageStorage3'
import {emptyBuffer} from './constants'
import PublishLoop from './publishLoop'
import {createQState} from './QState.create'
import {assertResources} from './assertResources'
import {Channel, Connection} from 'amqplib'
import {
  amqpUri,
  inputChannelPrefetchCount,
  inputQueueName,
  outputQueueCount,
  outputQueueName,
  partitionGroupHeader,
  partitionKeyHeader,
  postgresConnectionString,
  postgresPoolMax,
  responseQueueName
} from './config'
import {handleMessage} from './amqp/handleMessage'
import {Pool} from 'pg'
import {Publisher} from './amqp/Publisher'
import {startFakePublisher} from './fake.publisher'

process.on('uncaughtException', err => {
  console.error('[CRITICAL] uncaughtException: ' + err)
})

process.on('unhandledRejection', res => {
  console.error('[CRITICAL] unhandledRejection: ' + res)
})

async function main() {
  const consumeConn = await amqp.connect(amqpUri)
  const publishConn = await amqp.connect(amqpUri)
  const fakeConn = await amqp.connect(amqpUri)
  console.log('connected to RabbitMQ')

  const inputCh = await consumeConn.createChannel()
  const qStateCh = await consumeConn.createConfirmChannel()
  const loopCh = await publishConn.createConfirmChannel()
  console.log('created channels')

  await inputCh.prefetch(inputChannelPrefetchCount, false)

  await assertResources(qStateCh, true)
  console.log('asserted resources')

  const publishLoop = new PublishLoop()
  console.log('created PublishLoop')

  const pool = new Pool({
    connectionString: postgresConnectionString,
    max: postgresPoolMax
  })
  await migrateDb({pool})
  console.log('migrated DB')

  const db = new Db({pool, useQueryCache: true})
  const db3 = new Db3({pool})
  const storage = new MessageStorage({db, batchSize: 100})
  const storage3 = new MessageStorage3({db3})
  const cache = new MessageCache({maxSize: 1024 ** 3})
  const messageBalancer = new MessageBalancer3({
    storage,
    storage3,
    cache,
    onPartitionAdded: _ => publishLoop.trigger()
  })
  console.log('created BalancedQueue')

  await messageBalancer.init({perRequestMessageCountLimit: 10000, initCache: true})
  console.log('initialized BalancedQueue')

  const qState = await createQState({ch: qStateCh, onMessageProcessed: () => publishLoop.trigger()})
  console.log('created QState')

  await consumeInputQueue(inputCh, messageBalancer)
  console.log('consumed input queue')

  publishLoop.connectTo({publisher: new Publisher(loopCh), qState, messageBalancer})
  console.log('connected PublishLoop')

  publishLoop.trigger()
  console.log('started PublishLoop')

  // await startFakeClients(fakeConn, outputQueueCount)
  // console.log('started fake clients')

  const publishedCount = await startFakePublisher(fakeConn)
  console.log(`started fake publisher ${publishedCount}`)

  setInterval(async () => {
    console.log({
      dbMessageCount: (await db.readStats()).messageCount,
      messageBalancerSize: messageBalancer.size(),
      qStateSize: qState.size()
    })
  }, 3000)
}

async function consumeInputQueue(ch: Channel, messageBalancer: MessageBalancer3) {
  await ch.consume(
    inputQueueName,
    handleMessage(async msg => {
      const {content, properties} = msg
      const partitionGroup = msg.properties.headers[partitionGroupHeader]
      const partitionKey = msg.properties.headers[partitionKeyHeader]
      await messageBalancer.storeMessage({partitionGroup, partitionKey, content, properties})
      ch.ack(msg)
    }),
    {noAck: false}
  )
}

main()
