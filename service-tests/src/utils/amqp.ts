import * as amqp from 'amqplib'
import {Channel, ConfirmChannel, Connection, Options} from 'amqplib'
import {amqpUri, inputQueueName, outputQueueCount, outputQueueName, responseQueueName} from '../config'

// tslint:disable:no-console

export async function connectAndCreateChannel(): Promise<[Connection, ConfirmChannel]> {
  const conn = await connect(amqpUri)
  const ch = await conn.createConfirmChannel()
  await ch.prefetch(1000)

  return [conn, ch]
}

function connect(url: string) {
  const attemptLimit = 10
  const attemptTimeout = 1000

  async function connectInternal(attempt: number = 1): Promise<Connection> {
    try {
      const conn = await amqp.connect(url)
      console.log(`[amqp] Connected to ${url}.`)
      return conn
    } catch (err) {
      if (attempt >= attemptLimit) {
        console.error(`[amqp] Unable to connect to ${url} during ${attempt} attempts.`)
        throw new Error(`Unable to connect to ${url}.`)
      }

      console.log(`[amqp] Next attempt to connect to ${url} in ${attemptTimeout} ms ...`, {attempt, attemptTimeout})
      await setTimeoutAsync(attemptTimeout)
      return connectInternal(attempt + 1)
    }
  }

  return connectInternal()
}

export async function assertAllResources(ch: Channel): Promise<void> {
  await ch.assertQueue(inputQueueName, {durable: true})
  await ch.assertQueue(responseQueueName, {durable: true})

  for (let i = 1; i <= outputQueueCount; i++) {
    await ch.assertQueue(outputQueueName(i), {durable: true})
  }
}

export function publishWithConfirmation(
  ch: ConfirmChannel,
  exchnage: string,
  routingKey: string,
  content: Buffer,
  options?: Options.Publish
): Promise<void> {
  return new Promise((res, rej) => {
    ch.publish(exchnage, routingKey, content, options, err => (err ? rej(err) : res()))
  })
}

export async function waitForInputQueueHasConsumers(ch: Channel) {
  while (true) {
    if ((await ch.checkQueue(inputQueueName)).consumerCount) {
      return
    }
  }
}

export async function waitForInputQueueIsEmpty(ch: Channel) {
  while (true) {
    if (!(await ch.checkQueue(inputQueueName)).messageCount) {
      return
    }
  }
}

export const setTimeoutAsync = (timeout: number) => {
  return new Promise(res => {
    setTimeout(res, timeout)
  })
}
