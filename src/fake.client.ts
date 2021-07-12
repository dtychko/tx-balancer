import * as amqp from 'amqplib'
import {amqpUri, outputQueueCount, outputQueueName, responseQueueName} from './config'
import {Connection} from 'amqplib'
import {Publisher} from './amqp/Publisher'
import {handleMessage} from './amqp/handleMessage'
import {emptyBuffer} from './constants'

// async function main() {
//   const fakeConn = await amqp.connect(amqpUri)
//
//   await startFakeClients(fakeConn, outputQueueCount)
//   console.log('started fake clients')
// }

export async function startFakeClients(conn: Connection, count: number) {
  const clientCh = await conn.createConfirmChannel()
  const publisher = new Publisher(clientCh)
  let counter = 0

  for (let i = 0; i < count; i++) {
    await clientCh.consume(
      outputQueueName(i + 1),
      handleMessage(async msg => {
        const messageId = msg.properties.messageId

        clientCh.ack(msg)
        await publisher.publishAsync('', responseQueueName, emptyBuffer, {persistent: true, messageId})

        counter += 1
        if (counter % 1000 === 0) {
          console.log(`fake client: consumed ${counter} messages`)
        }
      }),
      {noAck: false}
    )
  }
}

// main()
