import * as amqp from 'amqplib'
import {initState} from './state'
import BalancedQueue from './balancedQueue'
import {publishAsync} from './publishAsync'
import {handleMessage} from './handleMessage'
import {assertResources} from './assertResources'
import {checkOutputQueues, initEmitter} from './emitter'

async function main() {
  const conn = await amqp.connect('amqp://guest:guest@localhost:5672/')
  console.log('connected')
  const ch = await conn.createConfirmChannel()
  console.log('created channel')

  await assertResources(ch, true)
  console.log('asserted resources')

  await initState(ch)
  console.log('initialized state')

  const balancedQueue = new BalancedQueue(pk => {})

  initEmitter(ch, balancedQueue)

  await ch.consume(
    'input_queue',
    handleMessage(msg => {
      const partitionKey = msg.properties.headers['x-partition-key']
      balancedQueue.enqueue(msg.content, partitionKey)

      process.nextTick(() => {
        checkOutputQueues()
      })

      ch.ack(msg)
    }),
    {noAck: false}
  )

  for (let i = 0; i < 5; i++) {
    for (let j = 0; j <= i; j++) {
      await publishAsync(ch, '', 'input_queue', Buffer.from(`account/${i}/message/${j}`), {
        persistent: true,
        headers: {
          ['x-partition-key']: `account/${i}`
        }
      })
    }
  }
  console.log('published messages')
}

main()
