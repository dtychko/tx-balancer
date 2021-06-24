import {Connection} from 'amqplib'
import {Publisher} from './amqp/Publisher'
import {inputQueueName, partitionGroupHeader, partitionKeyHeader} from './config'

export async function startFakePublisher(conn: Connection) {
  const publishCh = await conn.createConfirmChannel()
  const publisher = new Publisher(publishCh)
  const content = Buffer.from(generateString(1 * 1024))
  let count = 0

  for (let _ = 0; _ < 100; _++) {
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

function generateString(length: number) {
  let text = ''
  const possible = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789'
  for (let i = 0; i < length; i++) {
    text += possible.charAt(Math.floor(Math.random() * possible.length))
  }
  return text
}
