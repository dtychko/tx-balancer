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
  let messageId = 1
  const messages = [] as Message[]

  for (let group = 1; group <= 5; group++) {
    for (let key = 1; key <= 5; key++) {
      for (let i = 1; i <= group + key; i++) {
        const content = generateString(1024)
        messages.push(createMessage(messageId++, `group/${group}`, `key/${key}`, content) as Message)
      }
    }
  }

  const [cache, cacheState] = mockCache(10 * 1024)

  const balancer = new MessageBalancer3({
    storage: {} as MessageStorage,
    storage3: mockStorage3(messages),
    cache,
    onPartitionAdded: () => {}
  })

  await balancer.init({perRequestMessageCountLimit: 50, initCache: true})

  expect(balancer.size()).toBe(messages.length)
  expect(cacheState.addMessageCalls.length).toBe(10)
  expect(cache.size()).toBe(10 * 1024)
})

test('storeMessage', async () => {
  const startedAt = new Date()

  const [cache, cacheState] = mockCache(Number.MAX_SAFE_INTEGER)

  let createMessageId = 0
  const storage = {
    async createMessage(message) {
      await sleep()
      return {messageId: ++createMessageId, ...message}
    }
  } as MessageStorage

  const onPartitionAddedCalls = [] as {partitionGroup: string; partitionKey: string}[]
  const balancer = new MessageBalancer3({
    storage,
    storage3: mockStorage3([]),
    cache,
    onPartitionAdded: (partitionGroup, partitionKey) => {
      onPartitionAddedCalls.push({partitionGroup, partitionKey})
    }
  })

  await balancer.init({perRequestMessageCountLimit: 10000, initCache: false})

  await balancer.storeMessage(createMessageData('group/1', 'key/1', 'message/1'))
  expect(onPartitionAddedCalls).toEqual([{partitionGroup: 'group/1', partitionKey: 'key/1'}])

  await balancer.storeMessage(createMessageData('group/1', 'key/1', 'message/2'))
  expect(onPartitionAddedCalls).toEqual([{partitionGroup: 'group/1', partitionKey: 'key/1'}])

  await balancer.storeMessage(createMessageData('group/2', 'key/1', 'message/3'))
  expect(onPartitionAddedCalls).toEqual([
    {partitionGroup: 'group/1', partitionKey: 'key/1'},
    {partitionGroup: 'group/2', partitionKey: 'key/1'}
  ])

  await balancer.storeMessage(createMessageData('group/2', 'key/2', 'message/4'))
  expect(onPartitionAddedCalls).toEqual([
    {partitionGroup: 'group/1', partitionKey: 'key/1'},
    {partitionGroup: 'group/2', partitionKey: 'key/1'},
    {partitionGroup: 'group/2', partitionKey: 'key/2'}
  ])

  await balancer.storeMessage(createMessageData('group/2', 'key/1', 'message/5'))
  expect(onPartitionAddedCalls).toEqual([
    {partitionGroup: 'group/1', partitionKey: 'key/1'},
    {partitionGroup: 'group/2', partitionKey: 'key/1'},
    {partitionGroup: 'group/2', partitionKey: 'key/2'}
  ])

  expect(
    cacheState.addMessageCalls.map(call => {
      const {receivedDate, ...rest} = call
      return rest
    })
  ).toEqual([
    createMessage(1, 'group/1', 'key/1'),
    createMessage(2, 'group/1', 'key/1'),
    createMessage(3, 'group/2', 'key/1'),
    createMessage(4, 'group/2', 'key/2'),
    createMessage(5, 'group/2', 'key/1')
  ])

  expect(cacheState.addMessageCalls.every(call => call.receivedDate > startedAt)).toBeTruthy()

  expect(createMessageId).toBe(5)
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
    storage3: mockStorage3([]),
    cache,
    onPartitionAdded: () => {}
  })

  await balancer.init({perRequestMessageCountLimit: 10000, initCache: false})

  expect(await balancer.getMessage(1)).toEqual(createMessage(1))
  expect(await balancer.getMessage(2)).toEqual(createMessage(2))
  await expect(balancer.getMessage(3)).rejects.toThrow('Unexpected messageId')
})

test('removeMessage', async () => {
  const removeMessageCalls = [] as {messageId: number}[]
  const storage = {
    async removeMessage(messageId: number) {
      removeMessageCalls.push({messageId})
      await sleep()
    }
  } as MessageStorage

  const balancer = new MessageBalancer3({
    storage,
    storage3: mockStorage3([]),
    cache: {} as MessageCache,
    onPartitionAdded: () => {}
  })

  await balancer.init({perRequestMessageCountLimit: 10000, initCache: false})

  await balancer.removeMessage(10)
  await balancer.removeMessage(100)
  await balancer.removeMessage(1)

  expect(removeMessageCalls).toEqual([{messageId: 10}, {messageId: 100}, {messageId: 1}])
})

function mockStorage3(messages: Message[]) {
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
          .slice(spec.fromRow - 1, spec.toRow)
        result.push(...partitionSlice)
      }

      result.sort((a, b) => a.messageId - b.messageId)

      await sleep()

      let contentSize = 0
      return result.map((x): FullOrPartialMessage => {
        if (contentSize + x.content.length <= spec.contentSizeLimit) {
          contentSize += x.content.length
          return {type: 'full', ...x}
        }
        return {
          type: 'partial',
          messageId: x.messageId,
          partitionGroup: x.partitionGroup,
          partitionKey: x.partitionKey
        }
      })
    }
  } as MessageStorage3
}

function mockCache(maxSize: number) {
  const addMessageCalls = [] as Message[]
  let size = 0
  const cache = {
    maxSize,
    size() {
      return size
    },
    addMessage(message: Message) {
      addMessageCalls.push(message)
      if (size + message.content.length <= maxSize) {
        size += message.content.length
        return true
      }
      return false
    }
  } as MessageCache

  return [cache, {addMessageCalls}] as const
}

function createMessage(messageId: number, partitionGroup?: string, partitionKey?: string, content?: string) {
  return {
    messageId,
    partitionGroup: partitionGroup ?? `group/${messageId}`,
    partitionKey: partitionKey ?? `key/${messageId}`,
    content: Buffer.from(content ?? `message/${messageId}`),
    properties: {}
  }
}

function createMessageData(partitionGroup: string, partitionKey: string, content: string) {
  return {
    partitionGroup,
    partitionKey,
    content: Buffer.from(content),
    properties: {} as MessageProperties
  }
}

function generateString(length: number) {
  let text = ''
  const possible = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789'
  for (let i = 0; i < length; i++) {
    text += possible.charAt(Math.floor(Math.random() * possible.length))
  }
  return text
}

async function sleep(ms: number = 0) {
  return new Promise(res => {
    setTimeout(res, ms)
  })
}
