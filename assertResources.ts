import {Channel} from 'amqplib'

export async function assertResources(ch: Channel, purge: boolean) {
  await ch.assertQueue('input_queue', {durable: true})
  await ch.assertExchange('output', 'topic', {durable: true})
  await ch.assertQueue('response_queue', {durable: true})

  if (purge) {
    await ch.purgeQueue('input_queue')
    await ch.purgeQueue('response_queue')
  }

  await assertOutputQueue(ch, 'output_1')
  await assertOutputQueue(ch, 'output_2')

  async function assertOutputQueue(ch: Channel, name: string) {
    await ch.assertQueue(name, {durable: true})
    await ch.assertQueue(`${name}.mirror`, {durable: true})

    if (purge) {
      await ch.purgeQueue(name)
      await ch.purgeQueue(`${name}.mirror`)
    }

    await ch.bindQueue(name, 'output', name)
    await ch.bindQueue(`${name}.mirror`, 'output', name)
  }
}
