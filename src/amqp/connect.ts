import * as amqp from 'amqplib'
import {Connection} from 'amqplib'

export function connect(amqpUri: string) {
  async function connectInternal(attempt = 1): Promise<Connection> {
    try {
      return await amqp.connect(amqpUri)
    } catch (err) {

      if (attempt >= 15) {
        throw new Error(`Unable to connect to RabbitMQ ${amqpUri}.`)
      }

      await new Promise(res => {
        setTimeout(res, 1000)
      })
      return connectInternal(attempt + 1)
    }
  }

  return connectInternal()
}
