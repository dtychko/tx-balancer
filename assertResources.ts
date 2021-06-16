import {Channel} from 'amqplib'
import {inputQueueName, outputMirrorQueueName, outputQueueCount, outputQueueName, responseQueueName} from './config'

export async function assertResources(ch: Channel, purge: boolean) {
  await ch.assertQueue(inputQueueName, {durable: true})
  await ch.assertQueue(responseQueueName, {durable: true})

  if (purge) {
    await ch.purgeQueue(inputQueueName)
    await ch.purgeQueue(responseQueueName)
  }

  for (let i = 0; i < outputQueueCount; i++) {
    await assertOutputQueue(ch, outputQueueName(i + 1))
  }

  async function assertOutputQueue(ch: Channel, outputQueue: string) {
    await ch.assertQueue(outputQueue, {durable: true})
    await ch.assertQueue(outputMirrorQueueName(outputQueue), {durable: true})

    if (purge) {
      await ch.purgeQueue(outputQueue)
      await ch.purgeQueue(`${outputQueue}.mirror`)
    }
  }
}
