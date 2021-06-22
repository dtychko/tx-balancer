import {Db, MessageCache, MessageStorage, migrateDb} from '@targetprocess/balancer-core'
import * as amqp from 'amqplib'
import {Db3} from './balancing/Db3'
import MessageBalancer3 from './balancing/MessageBalancer3'
import MessageStorage3 from './balancing/MessageStorage3'
import {emptyBuffer} from './constants'
import {publishAsync} from './amqp/publishAsync'
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

  await startFakeClients(fakeConn, outputQueueCount)
  console.log('started fake clients')

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

async function startFakePublisher(conn: Connection) {
  const publishCh = await conn.createConfirmChannel()
  const publisher = new Publisher(publishCh)
  const content = Buffer.from(generateString(100 * 1024))
  let count = 0

  for (let _ = 0; _ < 20; _++) {
    for (let i = 0; i < 10; i++) {
      const promises = [] as Promise<void>[]

      for (let j = 0; j <= 100; j++) {
        promises.push(
          publisher.publishAsync('', inputQueueName, content, {
            persistent: true,
            headers: {
              [partitionGroupHeader]: `account/${i}`,
              [partitionKeyHeader]: j % 2 === 0 ? 'even' : 'odd'
            }
          })
        )

        count += 1
      }

      await Promise.all(promises)
    }
  }

  return count
}

async function startFakeClients(conn: Connection, count: number) {
  const clientCh = await conn.createConfirmChannel()
  const publisher = new Publisher(clientCh)

  for (let i = 0; i < count; i++) {
    await clientCh.consume(
      outputQueueName(i + 1),
      handleMessage(msg => {
        const messageId = msg.properties.messageId

        clientCh.ack(msg)
        publisher.publishAsync('', responseQueueName, emptyBuffer, {persistent: true, messageId})
      }),
      {noAck: false}
    )
  }
}

function generateString(length: number) {
  let text = ''
  const possible = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789'
  for (let i = 0; i < length; i++) {
    text += possible.charAt(Math.floor(Math.random() * possible.length))
  }
  return text
}

main()
