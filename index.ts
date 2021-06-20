import * as amqp from 'amqplib'
import {Message, MessageCache} from './balancer-core'
import MessageBalancer from './balancer-core-3/MessageBalancer'
import {publishAsync} from './publishAsync'
import PublishLoop from './publishLoop'
import {createQState} from './QState.factory'
import {assertResources} from './assertResources'
import {Channel, Connection} from 'amqplib'
import {
  amqpUri,
  inputQueueName,
  outputQueueCount,
  outputQueueName,
  partitionGroupHeader,
  partitionKeyHeader,
  responseQueueName
} from './config'
import {handleMessage} from './handleMessage'

async function main() {
  const conn = await amqp.connect(amqpUri)
  console.log('connected to RabbitMQ')

  const inputCh = await conn.createChannel()
  const qStateCh = await conn.createConfirmChannel()
  const loopCh = await conn.createConfirmChannel()
  console.log('created channels')

  await assertResources(qStateCh, true)
  console.log('asserted resources')

  const publishLoop = new PublishLoop()
  console.log('created PublishLoop')

  const messageBalancer = new MessageBalancer({
    storage: createFakeStorage(),
    cache: new MessageCache(),
    onPartitionAdded: _ => publishLoop.trigger()
  })
  console.log('created BalancedQueue')

  await messageBalancer.init()
  console.log('initialized BalancedQueue')

  const qState = await createQState(qStateCh, () => publishLoop.trigger())
  console.log('created QState')

  await consumeInputQueue(inputCh, messageBalancer)
  console.log('consumed input queue')

  publishLoop.connectTo(loopCh, qState, messageBalancer)
  console.log('connected PublishLoop')

  publishLoop.trigger()
  console.log('started PublishLoop')

  await startFakeClients(conn, outputQueueCount)
  console.log('started fake clients')

  await startFakePublisher(conn)
  console.log('started fake publisher')
}

async function consumeInputQueue(ch: Channel, messageBalancer: MessageBalancer) {
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

function createFakeStorage(): any {
  let messageId = 1

  return {
    createMessage(data: any) {
      return {...data, messageId: messageId++} as any
    },
    removeMessage() {},
    getMessage() {},
    readPartitionGroupMessagesOrderedById() {
      return Promise.resolve(new Map<string, Message[]>())
    }
  }
}

async function startFakePublisher(conn: Connection) {
  const publishCh = await conn.createConfirmChannel()

  for (let _ = 0; _ < 100; _++) {
    for (let i = 0; i < 10; i++) {
      const promises = [] as Promise<void>[]

      for (let j = 0; j <= 100; j++) {
        promises.push(
          publishAsync(publishCh, '', inputQueueName, Buffer.from(`account/${i}/message/${j}`), {
            persistent: true,
            headers: {
              [partitionGroupHeader]: `account/${i}`,
              [partitionKeyHeader]: i % 2 === 0 ? 'even' : 'odd'
            }
          })
        )
      }

      await Promise.all(promises)
    }
  }
}

async function startFakeClients(conn: Connection, count: number) {
  const clientCh = await conn.createConfirmChannel()

  for (let i = 0; i < count; i++) {
    await clientCh.consume(
      outputQueueName(i + 1),
      handleMessage(msg => {
        const messageId = msg.properties.messageId

        clientCh.ack(msg)
        publishAsync(clientCh, '', responseQueueName, Buffer.from(generateString(1024)), {persistent: true, messageId})
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
