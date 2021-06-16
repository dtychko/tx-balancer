import * as amqp from 'amqplib'
import {createTxChannel} from './amqp'
import {startLoop} from './publishLoop'
import {createQState} from './QState.factory'
import BalancedQueue from './balancedQueue'
import {assertResources} from './assertResources'
import {Channel} from 'amqplib'
import {inputQueueName, partitionKeyHeader} from './config'
import {handleMessage} from './handleMessage'

async function main() {
  const conn = await amqp.connect('amqp://guest:guest@localhost:5672/')
  console.log('connected')

  const inputCh = await conn.createChannel()
  const qStateCh = await createTxChannel(conn)
  const loopCh = await conn.createConfirmChannel()
  console.log('created channels')

  await assertResources(qStateCh, true)
  console.log('asserted resources')

  let scheduleStartLoop: () => void = () => {}
  const balancedQueue = new BalancedQueue<Buffer>(_ => scheduleStartLoop())

  const qState = await createQState(qStateCh, scheduleStartLoop)
  console.log('created QState')

  await consumeInputQueue(inputCh, balancedQueue)
  console.log('consumed input queue')

  scheduleStartLoop = () => {
    startLoop(loopCh, qState, balancedQueue)
  }

  scheduleStartLoop()

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
