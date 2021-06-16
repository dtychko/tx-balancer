import * as amqp from 'amqplib'
import PublishLoop from './publishLoop'
import {createQState} from './QState.factory'
import BalancedQueue from './balancedQueue'
import {assertResources} from './assertResources'
import {Channel} from 'amqplib'
import {inputQueueName, partitionKeyHeader} from './config'
import {handleMessage} from './handleMessage'

async function main() {
  const conn = await amqp.connect('amqp://guest:guest@localhost:5672/')
  console.log('connected to RabbitMQ')

  const inputCh = await conn.createChannel()
  const qStateCh = await conn.createConfirmChannel()
  const loopCh = await conn.createConfirmChannel()
  console.log('created channels')

  await assertResources(qStateCh, true)
  console.log('asserted resources')

  const publishLoop = new PublishLoop()
  console.log('created PublishLoop')

  const balancedQueue = new BalancedQueue<Buffer>(publishLoop.trigger)
  console.log('created BalancedQueue')

  const qState = await createQState(qStateCh, publishLoop.trigger)
  console.log('created QState')

  await consumeInputQueue(inputCh, balancedQueue)
  console.log('consumed input queue')

  publishLoop.connectTo(loopCh, qState, balancedQueue)
  console.log('connected PublishLoop')

  publishLoop.trigger()
  console.log('started PublishLoop')

  // for (let i = 0; i < 5; i++) {
  //   for (let j = 0; j <= i; j++) {
  //     await publishAsync(ch, '', 'input_queue', Buffer.from(`account/${i}/message/${j}`), {
  //       persistent: true,
  //       headers: {
  //         ['x-partition-key']: `account/${i}`
  //       }
  //     })
  //   }
  // }
  // console.log('published messages')
}

async function consumeInputQueue(ch: Channel, balancedQueue: BalancedQueue<Buffer>) {
  await ch.consume(
    inputQueueName,
    handleMessage(msg => {
      const partitionKey = msg.properties.headers[partitionKeyHeader]
      balancedQueue.enqueue(msg.content, partitionKey)
      ch.ack(msg)
    }),
    {noAck: false}
  )
}

main()
