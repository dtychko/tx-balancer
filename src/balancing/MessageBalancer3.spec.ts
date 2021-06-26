import {Message, MessageCache, MessageStorage} from '@targetprocess/balancer-core'
import {MessageProperties} from 'amqplib'
import {FullOrPartialMessage} from './Db3'
import MessageBalancer3 from './MessageBalancer3'
import MessageStorage3 from './MessageStorage3'

test('assert initialized', async () => {
  const balancer = new MessageBalancer3({
    storage: {} as MessageStorage,
    storage3: {} as MessageStorage3,
    cache: {} as MessageCache,
    onPartitionAdded: () => {}
  })

  await expect(
    balancer.storeMessage({
      partitionGroup: 'group/1',
      partitionKey: 'key/1',
      content: Buffer.alloc(0),
      properties: {} as MessageProperties
    })
  ).rejects.toThrow("Message service isn't initialized")

  expect(() =>
    balancer.tryDequeueMessage(pg => ({canProcessPartitionGroup: () => true, canProcessPartitionKey: () => true}))
  ).toThrow("Message service isn't initialized")

  await expect(balancer.getMessage(1)).rejects.toThrow("Message service isn't initialized")
  await expect(balancer.removeMessage(1)).rejects.toThrow("Message service isn't initialized")
})

test('init', async () => {
  let messageIdCounter = 1

  const messages = [
    createMessage(messageIdCounter++, 'group/1', 'key/1'),
    createMessage(messageIdCounter++, 'group/1', 'key/2'),
    createMessage(messageIdCounter++, 'group/1', 'key/2'),
    createMessage(messageIdCounter++, 'group/2', 'key/1'),
    createMessage(messageIdCounter++, 'group/2', 'key/1'),
    createMessage(messageIdCounter++, 'group/2', 'key/1'),
    createMessage(messageIdCounter++, 'group/3', 'key/1')
  ] as Message[]
  const storage3 = createStorage3(messages)

  const balancer = new MessageBalancer3({
    storage: {} as MessageStorage,
    storage3,

    // TODO: Replace with cacheMock
    cache: new MessageCache(),
    onPartitionAdded: () => {}
  })

  await balancer.init({perRequestMessageCountLimit: 10000, initCache: true})

  expect(balancer.size()).toBe(messages.length)
})

test('storeMessage', async () => {
  const balancer = new MessageBalancer3({
    storage: {} as MessageStorage,
    storage3: {} as MessageStorage3,
    cache: {} as MessageCache,
    onPartitionAdded: () => {}
  })
})

test('getMessage', async () => {
  const cache = {
    getAndRemoveMessage(messageId: number) {
      return messageId === 1 ? createMessage(messageId) : undefined
    }
  } as MessageCache

  const storage = {
    async readMessage(messageId: number) {
      return messageId === 2 ? createMessage(messageId) : Promise.reject(new Error('Unexpected messageId'))
    }
  } as MessageStorage

  const balancer = new MessageBalancer3({
    storage,
    storage3: createStorage3([]),
    cache,
    onPartitionAdded: () => {}
  })

  await balancer.init({perRequestMessageCountLimit: 10000, initCache: false})

  expect(await balancer.getMessage(1)).toEqual(createMessage(1))
  expect(await balancer.getMessage(2)).toEqual(createMessage(2))
  await expect(balancer.getMessage(3)).rejects.toThrow('Unexpected messageId')
})

function createStorage3(messages: Message[]) {
  const partitions = new Map<string, Message[]>()
  for (const message of messages) {
    const {partitionGroup, partitionKey} = message
    const key = JSON.stringify({partitionGroup, partitionKey})
    partitions.set(key, [...(partitions.get(key) || []), message])
  }

  return {
    async readPartitionMessagesOrderedById(spec: {
      fromRow: number
      toRow: number
      contentSizeLimit: number
    }): Promise<FullOrPartialMessage[]> {
      const result = []

      for (const partition of partitions.values()) {
        const partitionSlice = [...partition]
          .sort((a, b) => a.messageId - b.messageId)
          .slice(spec.fromRow - 1, spec.fromRow + spec.toRow - 1)
        result.push(...partitionSlice)
      }

      result.sort((a, b) => a.messageId - b.messageId)

      await sleep()
      return result.map(x => ({type: 'full', ...x}))
    }
  } as MessageStorage3
}

function createMessage(messageId: number, partitionGroup?: string, partitionKey?: string) {
  return {
    messageId,
    partitionGroup: partitionGroup ?? `group/${messageId}`,
    partitionKey: partitionKey ?? `key/${messageId}`,
    content: Buffer.from(`message/${messageId}`),
    properties: {}
  }
}

async function sleep(ms: number = 0) {
  return new Promise(res => {
    setTimeout(res, ms)
  })
}
