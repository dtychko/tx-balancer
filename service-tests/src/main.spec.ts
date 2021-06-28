import {ConfirmChannel, Connection} from 'amqplib'
import {
  assertAllResources,
  connectAndCreateChannel,
  setTimeoutAsync,
  waitForInputQueueHasConsumers,
  waitForInputQueueIsEmpty
} from './utils/amqp'
import {
  inputQueueName,
  outputQueueCount,
  outputQueueName,
  partitionGroupHeader,
  partitionKeyHeader,
  responseQueueName
} from './config'

let conn: Connection
let ch: ConfirmChannel

jest.setTimeout(15000)

beforeAll(async () => {
  ;[conn, ch] = await connectAndCreateChannel()
  await assertAllResources(ch)

  // If input queue has consumer, then Balancer should be started
  await waitForInputQueueHasConsumers(ch)
})

afterAll(async () => {
  for (const closable of [ch, conn]) {
    if (closable) {
      await closable.close()
    }
  }
})

test('consume all published messages', async () => {
  const groupCount = 10
  const keyPerGroupCount = 10
  const messagePerKeyCount = 100

  for (let group = 1; group <= groupCount; group++) {
    for (let key = 1; key <= keyPerGroupCount; key++) {
      for (let num = 1; num <= messagePerKeyCount; num++) {
        ch.publish('', inputQueueName, Buffer.from(`message/${num}`), {
          persistent: true,
          headers: {
            [partitionGroupHeader]: `group/${group}`,
            [partitionKeyHeader]: `key/${key}`
          }
        })
      }
    }
  }

  // Wait for Balancer to process all input messages
  await waitForInputQueueIsEmpty(ch)

  const messages = [] as {partitionGroup: string; partitionKey: string; content: string; queueName: string}[]
  const consumerTags = [] as string[]

  for (let i = 1; i <= outputQueueCount; i++) {
    const queueName = outputQueueName(i)
    const {consumerTag} = await ch.consume(queueName, msg => {
      const messageId = msg!.properties.messageId
      const partitionGroup = msg!.properties.headers[partitionGroupHeader]
      const partitionKey = msg!.properties.headers[partitionKeyHeader]
      const content = msg!.content.toString()

      ch.publish('', responseQueueName, Buffer.alloc(0), {persistent: true, messageId})
      ch.ack(msg!)

      messages.push({partitionGroup, partitionKey, content, queueName})
    })
    consumerTags.push(consumerTag)
  }

  while (messages.length < groupCount * keyPerGroupCount * messagePerKeyCount) {
    await setTimeoutAsync(10)
  }

  for (const consumerTag of consumerTags) {
    await ch.cancel(consumerTag)
  }

  expect(messages.length).toBe(groupCount * keyPerGroupCount * messagePerKeyCount)

  for (let group = 1; group <= groupCount; group++) {
    const groupMessages = messages.filter(m => m.partitionGroup === `group/${group}`)
    expect(new Set<string>(groupMessages.map(m => m.queueName)).size).toBe(1)

    for (let key = 1; key <= keyPerGroupCount; key++) {
      const partitionMessages = groupMessages.filter(m => m.partitionKey === `key/${key}`)
      expect(partitionMessages.length).toBe(messagePerKeyCount)
    }
  }
})

test('respect partition group and key limits', async () => {
  const keyPerGroupCount = 20
  const messagePerKeyCount = 100

  for (let key = 1; key <= keyPerGroupCount; key++) {
    for (let num = 1; num <= messagePerKeyCount; num++) {
      ch.publish('', inputQueueName, Buffer.from(`message/${num}`), {
        persistent: true,
        headers: {
          [partitionGroupHeader]: `group/1`,
          [partitionKeyHeader]: `key/${key}`
        }
      })
    }
  }

  // Wait for Balancer to process all input messages
  await waitForInputQueueIsEmpty(ch)

  const messages = [] as {partitionGroup: string; partitionKey: string; queueName: string; ack: () => void}[]
  const consumerTags = [] as string[]

  for (let i = 1; i <= outputQueueCount; i++) {
    const queueName = outputQueueName(i)
    const {consumerTag} = await ch.consume(queueName, msg => {
      const messageId = msg!.properties.messageId
      const partitionGroup = msg!.properties.headers[partitionGroupHeader]
      const partitionKey = msg!.properties.headers[partitionKeyHeader]

      messages.push({
        partitionGroup,
        partitionKey,
        queueName,
        ack: () => {
          ch.publish('', responseQueueName, Buffer.alloc(0), {persistent: true, messageId})
          ch.ack(msg!)
        }
      })
    })
    consumerTags.push(consumerTag)
  }

  const singlePartitionGroupLimit = 50
  const singlePartitionKeyLimit = 10
  const messageCount = keyPerGroupCount * messagePerKeyCount
  let processedMessageCount = 0

  while (processedMessageCount < messageCount) {
    while (messages.length < Math.min(singlePartitionGroupLimit, messageCount - processedMessageCount)) {
      await setTimeoutAsync(10)
    }

    expect(new Set<string>(messages.map(m => m.queueName)).size).toBe(1)
    expect(messages.length).toBeLessThanOrEqual(singlePartitionGroupLimit)

    const counterByPartitionKey = new Map<string, number>()
    for (const {partitionKey} of messages) {
      counterByPartitionKey.set(partitionKey, (counterByPartitionKey.get(partitionKey) || 0) + 1)
    }

    expect([...counterByPartitionKey.values()].every(x => x <= singlePartitionKeyLimit)).toBeTruthy()

    for (const message of messages) {
      message.ack()
    }

    processedMessageCount += messages.length
    messages.splice(0)
  }

  for (const consumerTag of consumerTags) {
    await ch.cancel(consumerTag)
  }
})
