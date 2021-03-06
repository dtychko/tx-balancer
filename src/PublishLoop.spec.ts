import {MessageProperties, Options} from 'amqplib'
import {MessagePropertyHeaders} from 'amqplib/properties'
import {Buffer} from 'buffer'
import {Publisher} from './amqp/Publisher'
import MessageBalancer3 from './balancing/MessageBalancer3'
import PublishLoop from './PublishLoop'
import {QState} from './QState'

test('trigger: idle state', async () => {
  const loop = new PublishLoop({
    mirrorQueueName: queue => `${queue}/mirror`,
    partitionGroupHeader: 'x-partition-group',
    partitionKeyHeader: 'x-partition-key',
    onError: () => {}
  })

  expect(loop.trigger()).toEqual({started: false})
  expect(loop.trigger()).toEqual({started: false})

  expect(loop.status()).toEqual({
    state: 'InitialState',
    processingMessageCount: 0,
    processedMessageCount: 0,
    failedMessageCount: 0
  })
})

test('trigger: common scenarios', async () => {
  const [publisher, publisherState] = mockPublisher()
  const [qState, qStateState] = mockQState()
  const [messageBalancer, messageBalancerState] = mockMessageBalancer([
    {
      messageId: 1,
      partitionGroup: 'group/1',
      partitionKey: 'key/1',
      content: Buffer.from('message/1'),
      properties: {} as MessageProperties
    },
    {
      messageId: 2,
      partitionGroup: 'group/2',
      partitionKey: 'key/2',
      content: Buffer.from('message/2'),
      properties: {} as MessageProperties
    }
  ])

  const loop = new PublishLoop({
    mirrorQueueName: queue => `${queue}/mirror`,
    partitionGroupHeader: 'x-partition-group',
    partitionKeyHeader: 'x-partition-key',
    onError: () => {}
  })

  loop.connectTo({
    publisher,
    qState,
    messageBalancer
  })

  expect(loop.trigger()).toEqual({started: true})
  expect(loop.trigger()).toEqual({started: false})

  await waitFor(() => loop.status().processedMessageCount == 2)

  expect(qStateState.registerMessageCalls).toEqual([
    {partitionGroup: 'group/1', partitionKey: 'key/1'},
    {partitionGroup: 'group/2', partitionKey: 'key/2'}
  ])

  expect(publisherState.publishAsyncCalls).toEqual([
    {
      exchange: '',
      queue: 'queue/group/1/mirror',
      content: Buffer.alloc(0),
      options: {
        headers: {['x-partition-group']: 'group/1', ['x-partition-key']: 'key/1'} as MessagePropertyHeaders,
        persistent: true,
        messageId: 'publish/1'
      }
    },
    {
      exchange: '',
      queue: 'queue/group/1',
      content: Buffer.from('message/1'),
      options: {
        persistent: true,
        messageId: 'publish/1'
      }
    },
    {
      exchange: '',
      queue: 'queue/group/2/mirror',
      content: Buffer.alloc(0),
      options: {
        headers: {['x-partition-group']: 'group/2', ['x-partition-key']: 'key/2'} as MessagePropertyHeaders,
        persistent: true,
        messageId: 'publish/2'
      }
    },
    {
      exchange: '',
      queue: 'queue/group/2',
      content: Buffer.from('message/2'),
      options: {
        persistent: true,
        messageId: 'publish/2'
      }
    }
  ])

  expect(messageBalancerState.removeMessageCalls).toEqual([{messageId: 1}, {messageId: 2}])
})

test('trigger: serialize message publishing by partition group (actually, serialize all messages publishing at the moment)', async () => {
  const [publisher, publisherState] = mockPublisher()
  const [qState] = mockQState()
  const [messageBalancer] = mockMessageBalancer([
    {
      messageId: 1,
      partitionGroup: 'group/1',
      partitionKey: 'key/1',
      content: Buffer.from('message/1'),
      properties: {} as MessageProperties
    },
    {
      messageId: 2,
      partitionGroup: 'group/1',
      partitionKey: 'key/2',
      content: Buffer.from('message/2'),
      properties: {} as MessageProperties
    },
    {
      messageId: 3,
      partitionGroup: 'group/2',
      partitionKey: 'key/1',
      content: Buffer.from('message/3'),
      properties: {} as MessageProperties
    }
  ])

  const awaiters = [] as (() => void)[]
  const getMessage = messageBalancer.getMessage
  ;(messageBalancer as any).getMessage = async (messageId: number) => {
    await new Promise<void>(res => {
      awaiters.push(res)
    })
    return getMessage.call(messageBalancer, messageId)
  }

  const loop = new PublishLoop({
    mirrorQueueName: queue => `${queue}/mirror`,
    partitionGroupHeader: 'x-partition-group',
    partitionKeyHeader: 'x-partition-key',
    onError: () => {}
  })

  loop.connectTo({
    publisher,
    qState,
    messageBalancer
  })

  loop.trigger()

  await waitFor(() => awaiters.length === 3)

  awaiters[2]()
  awaiters[1]()
  awaiters[0]()

  await waitFor(() => loop.status().processedMessageCount === 3)

  expect(publisherState.publishAsyncCalls).toEqual([
    {
      exchange: '',
      queue: 'queue/group/1/mirror',
      content: Buffer.alloc(0),
      options: {
        headers: {['x-partition-group']: 'group/1', ['x-partition-key']: 'key/1'} as MessagePropertyHeaders,
        persistent: true,
        messageId: 'publish/1'
      }
    },
    {
      exchange: '',
      queue: 'queue/group/1',
      content: Buffer.from('message/1'),
      options: {
        persistent: true,
        messageId: 'publish/1'
      }
    },
    {
      exchange: '',
      queue: 'queue/group/1/mirror',
      content: Buffer.alloc(0),
      options: {
        headers: {['x-partition-group']: 'group/1', ['x-partition-key']: 'key/2'} as MessagePropertyHeaders,
        persistent: true,
        messageId: 'publish/2'
      }
    },
    {
      exchange: '',
      queue: 'queue/group/1',
      content: Buffer.from('message/2'),
      options: {
        persistent: true,
        messageId: 'publish/2'
      }
    },
    {
      exchange: '',
      queue: 'queue/group/2/mirror',
      content: Buffer.alloc(0),
      options: {
        headers: {['x-partition-group']: 'group/2', ['x-partition-key']: 'key/1'} as MessagePropertyHeaders,
        persistent: true,
        messageId: 'publish/3'
      }
    },
    {
      exchange: '',
      queue: 'queue/group/2',
      content: Buffer.from('message/3'),
      options: {
        persistent: true,
        messageId: 'publish/3'
      }
    }
  ])
})

function mockPublisher() {
  const publishAsyncCalls = [] as {exchange: string; queue: string; content: Buffer; options: Options.Publish}[]
  const publisher = {
    publishAsync: (exchange, queue, content, options) => {
      publishAsyncCalls.push({exchange, queue, content, options})
      return sleep()
    }
  } as Publisher

  return [publisher, {publishAsyncCalls}] as const
}

function mockQState() {
  const registerMessageCalls = [] as {partitionGroup: string; partitionKey: string}[]
  let publishCounter = 1
  const qState = {
    registerMessage: (partitionGroup, partitionKey) => {
      registerMessageCalls.push({partitionGroup, partitionKey})
      return {queueMessageId: `publish/${publishCounter++}`, queueName: `queue/${partitionGroup}`}
    },
    canRegister: _ => {
      return {
        canProcessPartitionGroup: () => true,
        canProcessPartitionKey: _ => true
      }
    }
  } as QState

  return [qState, {registerMessageCalls}] as const
}

type ArrayElement<ArrayType extends readonly unknown[]> = ArrayType extends readonly (infer ElementType)[]
  ? ElementType
  : never

function mockMessageBalancer(
  messages: {
    messageId: number
    partitionGroup: string
    partitionKey: string
    content: Buffer
    properties: MessageProperties
  }[]
) {
  const refs = messages.map(({content, ...ref}) => ref)
  const messagesById = new Map<number, ArrayElement<typeof messages>>(messages.map(x => [x.messageId, x]))
  const removeMessageCalls = [] as {messageId: number}[]

  const messageBalancer = {
    tryDequeueMessage: _ => {
      return refs.shift()
    },
    getMessage: async messageId => {
      await sleep()
      return messagesById.get(messageId)
    },
    removeMessage: async messageId => {
      await sleep()
      removeMessageCalls.push({messageId})
    }
  } as MessageBalancer3

  return [messageBalancer, {removeMessageCalls}] as const
}

async function sleep(ms: number = 0) {
  return new Promise(res => {
    setTimeout(res, ms)
  })
}

async function waitFor(condition: () => boolean) {
  while (!condition()) {
    await new Promise(res => {
      setTimeout(res, 10)
    })
  }
}
