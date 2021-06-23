import * as amqp from 'amqplib'
import {amqpUri, outputQueueCount, outputQueueName, responseQueueName} from './config'
import {Connection} from 'amqplib'
import {Publisher} from './amqp/Publisher'
import {handleMessage} from './amqp/handleMessage'
import {emptyBuffer} from './constants'

async function main() {
  const fakeConn = await amqp.connect(amqpUri)

  await startFakeClients(fakeConn, outputQueueCount)
  console.log('started fake clients')
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

main()
