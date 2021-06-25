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
  function msg(partitionGroup: string, partitionKey: string) {
    return {
      messageId: messageIdCounter,
      partitionGroup,
      partitionKey,
      content: Buffer.from(`message/${messageIdCounter++}`),
      properties: {}
    }
  }

  const db = [
    msg('group/1', 'key/1'),
    msg('group/1', 'key/2'),
    msg('group/1', 'key/2'),
    msg('group/2', 'key/1'),
    msg('group/2', 'key/1'),
    msg('group/2', 'key/1'),
    msg('group/3', 'key/1')
  ] as Message[]
  const storage3 = {
    async readPartitionMessagesOrderedById(spec: {
      fromRow: number
      toRow: number
      contentSizeLimit: number
    }): Promise<FullOrPartialMessage[]> {
      const map = new Map<string, Message[]>()
      for (const m of db) {
        const {partitionGroup, partitionKey} = m
        const key = JSON.stringify({partitionGroup, partitionKey})
        map.set(key, [...(map.get(key) || []), m])
      }
      const result = []
      for (const msgs of map.values()) {
        msgs.sort((a, b) => a.messageId - b.messageId)
        result.push(...msgs.slice(spec.fromRow - 1, spec.fromRow + spec.toRow - 1))
      }
      result.sort((a, b) => a.messageId - b.messageId)
      await sleep()
      return result.map(x => ({type: 'full', ...x}))
    }
  } as MessageStorage3

  const balancer = new MessageBalancer3({
    storage: {} as MessageStorage,
    storage3,
    cache: new MessageCache(),
    onPartitionAdded: () => {}
  })

  await balancer.init({perRequestMessageCountLimit: 10000, initCache: true})

  expect(balancer.size()).toBe(db.length)
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
  const balancer = new MessageBalancer3({
    storage: {} as MessageStorage,
    storage3: {} as MessageStorage3,
    cache: {} as MessageCache,
    onPartitionAdded: () => {}
  })
})

async function sleep(ms: number = 0) {
  return new Promise(res => {
    setTimeout(res, ms)
  })
}
