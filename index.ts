import * as amqp from 'amqplib'
import {createTxChannel} from './amqp'
import {startLoop} from './publishLoop'
import {createQState} from './QState.factory'
import BalancedQueue from './balancedQueue'
import {assertResources} from './assertResources'

async function main() {
  const conn = await amqp.connect('amqp://guest:guest@localhost:5672/')
  console.log('connected')

  const ch = await createTxChannel(conn)
  console.log('created channel')

  await assertResources(ch, true)
  console.log('asserted resources')

  let scheduleStartLoop: () => void = () => {}

  const qState = await createQState(ch, scheduleStartLoop)
  const balancedQueue = new BalancedQueue<Buffer>(_ => scheduleStartLoop())

  scheduleStartLoop = () => {
    startLoop(ch, qState, balancedQueue)
  }

  // await ch.consume(
  //   'input_queue',
  //   handleMessage(msg => {
  //     const partitionKey = msg.properties.headers['x-partition-key']
  //     balancedQueue.enqueue(msg.content, partitionKey)
  //
  //     process.nextTick(() => {
  //       checkOutputQueues()
  //     })
  //
  //     ch.ack(msg)
  //   }),
  //   {noAck: false}
  // )
  //
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

main()
