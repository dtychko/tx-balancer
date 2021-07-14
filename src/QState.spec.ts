import {nanoid} from 'nanoid'
import {QState} from './QState'

test('restoreMessage', () => {
  const qState = createQState({queueCount: 3})

  qState.restoreMessage(nanoid(), 'queue/1/group/1', 'key/1', 'queue/1')
  qState.restoreMessage(nanoid(), 'queue/1/group/1', 'key/2', 'queue/1')
  qState.restoreMessage(nanoid(), 'queue/1/group/1', 'key/2', 'queue/1')

  qState.restoreMessage(nanoid(), 'queue/2/group/2', 'key/1', 'queue/2')
  qState.restoreMessage(nanoid(), 'queue/2/group/2', 'key/2', 'queue/2')
  qState.restoreMessage(nanoid(), 'queue/2/group/2', 'key/3', 'queue/2')

  qState.restoreMessage(nanoid(), 'queue/1/group/4', 'key/1', 'queue/1')

  expect(qState.diagnosticsState()).toEqual({
    queues: [
      {
        queueName: 'queue/1',
        messageCount: 4,
        partitionGroups: [
          {
            partitionGroup: 'queue/1/group/1',
            messageCount: 3,
            partitionKeys: [
              {partitionKey: 'key/1', messageCount: 1},
              {partitionKey: 'key/2', messageCount: 2}
            ]
          },
          {
            partitionGroup: 'queue/1/group/4',
            messageCount: 1,
            partitionKeys: [{partitionKey: 'key/1', messageCount: 1}]
          }
        ]
      },
      {
        queueName: 'queue/2',
        messageCount: 3,
        partitionGroups: [
          {
            partitionGroup: 'queue/2/group/2',
            messageCount: 3,
            partitionKeys: [
              {partitionKey: 'key/1', messageCount: 1},
              {partitionKey: 'key/2', messageCount: 1},
              {partitionKey: 'key/3', messageCount: 1}
            ]
          }
        ]
      },
      {
        queueName: 'queue/3',
        messageCount: 0,
        partitionGroups: []
      }
    ]
  })

  expect(qState.status()).toEqual({
    queueCount: 3,
    partitionGroupCount: 3,
    partitionKeyCount: 6,
    messageCount: 7
  })

  expect(qState.size()).toBe(7)
})

test('restoreMessage: duplicate messageId => error', () => {
  const qState = createQState({queueCount: 3})

  const messageId = nanoid()
  qState.restoreMessage(messageId, 'queue/1/group/1', 'key/1', 'queue/1')

  expect(() => {
    qState.restoreMessage(messageId, 'queue/2/group/2', 'key/2', 'queue/2')
  }).toThrow()
})

test('restoreMessage: unexpected queueName => error', () => {
  const qState = createQState({queueCount: 3})

  expect(() => {
    qState.restoreMessage(nanoid(), 'queue/1/group/1', 'key/1', 'queue/2')
  }).toThrow()
})

test('registerMessage', () => {
  const qState = createQState({queueCount: 3})

  const results = [
    qState.registerMessage('queue/1/group/1', 'key/1'),
    qState.registerMessage('queue/1/group/1', 'key/2'),
    qState.registerMessage('queue/1/group/1', 'key/2'),

    qState.registerMessage('queue/2/group/2', 'key/1'),
    qState.registerMessage('queue/2/group/2', 'key/2'),
    qState.registerMessage('queue/2/group/2', 'key/3'),

    qState.registerMessage('queue/1/group/4', 'key/1')
  ]

  expect(results.map(x => x.queueName)).toEqual([
    'queue/1',
    'queue/1',
    'queue/1',

    'queue/2',
    'queue/2',
    'queue/2',

    'queue/1'
  ])

  expect(qState.diagnosticsState()).toEqual({
    queues: [
      {
        queueName: 'queue/1',
        messageCount: 4,
        partitionGroups: [
          {
            partitionGroup: 'queue/1/group/1',
            messageCount: 3,
            partitionKeys: [
              {partitionKey: 'key/1', messageCount: 1},
              {partitionKey: 'key/2', messageCount: 2}
            ]
          },
          {
            partitionGroup: 'queue/1/group/4',
            messageCount: 1,
            partitionKeys: [{partitionKey: 'key/1', messageCount: 1}]
          }
        ]
      },
      {
        queueName: 'queue/2',
        messageCount: 3,
        partitionGroups: [
          {
            partitionGroup: 'queue/2/group/2',
            messageCount: 3,
            partitionKeys: [
              {partitionKey: 'key/1', messageCount: 1},
              {partitionKey: 'key/2', messageCount: 1},
              {partitionKey: 'key/3', messageCount: 1}
            ]
          }
        ]
      },
      {
        queueName: 'queue/3',
        messageCount: 0,
        partitionGroups: []
      }
    ]
  })

  expect(qState.status()).toEqual({
    queueCount: 3,
    partitionGroupCount: 3,
    partitionKeyCount: 6,
    messageCount: 7
  })

  expect(qState.size()).toBe(7)
})

test('registerDeliveryTag: call onMessageProcessed callback', () => {
  const processedMessages = [] as unknown[]
  const qState = createQState({
    queueCount: 3,
    onMessageProcessed(messageId: string, mirrorDeliveryTag: number, responseDeliveryTag: number) {
      processedMessages.push({messageId, mirrorDeliveryTag, responseDeliveryTag})
    }
  })

  // Registration order:
  //   1. registerMirrorDeliveryTag
  //   2. registerResponseDeliveryTag
  const {queueMessageId: queueMessageId1} = qState.registerMessage('queue/1/group/1', 'key/1')
  expect(processedMessages).toEqual([])

  expect(qState.registerMirrorDeliveryTag(queueMessageId1, 101)).toEqual({registered: true})
  expect(processedMessages).toEqual([])

  expect(qState.registerResponseDeliveryTag(queueMessageId1, 102)).toEqual({registered: true})
  expect(processedMessages).toEqual([{messageId: queueMessageId1, mirrorDeliveryTag: 101, responseDeliveryTag: 102}])

  processedMessages.shift()

  // Registration order:
  //   1. registerResponseDeliveryTag
  //   2. registerMirrorDeliveryTag
  const {queueMessageId: queueMessageId2} = qState.registerMessage('queue/2/group/2', 'key/2')
  expect(processedMessages).toEqual([])

  expect(qState.registerResponseDeliveryTag(queueMessageId2, 201)).toEqual({registered: true})
  expect(processedMessages).toEqual([])

  expect(qState.registerMirrorDeliveryTag(queueMessageId2, 202)).toEqual({registered: true})
  expect(processedMessages).toEqual([{messageId: queueMessageId2, mirrorDeliveryTag: 202, responseDeliveryTag: 201}])
})

test('registerDeliveryTag: reject tags for unknown messageIds', () => {
  const processedMessages = [] as unknown[]
  const qState = createQState({
    queueCount: 3,
    onMessageProcessed(messageId: string, mirrorDeliveryTag: number, responseDeliveryTag: number) {
      processedMessages.push({messageId, mirrorDeliveryTag, responseDeliveryTag})
    }
  })

  qState.registerMessage('queue/1/group/1', 'key/1')

  expect(qState.registerMirrorDeliveryTag('unknown/1', 101)).toEqual({registered: false})
  expect(qState.registerResponseDeliveryTag('unknown/2', 102)).toEqual({registered: false})
  expect(processedMessages).toEqual([])
})

test('canRegister', () => {
  const qState = createQState({
    queueCount: 3,
    queueSizeLimit: 10,
    singlePartitionGroupLimit: 5,
    singlePartitionKeyLimit: 2
  })

  for (let i = 0; i < 10; i++) {
    qState.registerMessage('queue/1/group/1', `key/${i + 1}`)
  }

  for (let i = 0; i < 5; i++) {
    qState.registerMessage('queue/2/group/2', `key/${i + 1}`)
  }

  for (let i = 0; i < 2; i++) {
    qState.registerMessage('queue/2/group/3', 'key/1')
  }

  expect(qState.canRegister('queue/1/group/1').canProcessPartitionGroup()).toBeFalsy()
  expect(qState.canRegister('queue/1/group/1').canProcessPartitionKey('key/999')).toBeFalsy()
  expect(qState.canRegister('queue/1/group/999').canProcessPartitionGroup()).toBeFalsy()
  expect(qState.canRegister('queue/1/group/999').canProcessPartitionKey('key/999')).toBeFalsy()

  expect(qState.canRegister('queue/2/group/2').canProcessPartitionGroup()).toBeFalsy()
  expect(qState.canRegister('queue/2/group/2').canProcessPartitionKey('key/999')).toBeFalsy()

  expect(qState.canRegister('queue/2/group/3').canProcessPartitionGroup()).toBeTruthy()
  expect(qState.canRegister('queue/2/group/3').canProcessPartitionKey('key/1')).toBeFalsy()
  expect(qState.canRegister('queue/2/group/3').canProcessPartitionKey('key/999')).toBeTruthy()

  expect(qState.canRegister('queue/2/group/999').canProcessPartitionGroup()).toBeTruthy()
  expect(qState.canRegister('queue/2/group/999').canProcessPartitionKey('key/999')).toBeTruthy()

  expect(qState.canRegister('queue/3/group/999').canProcessPartitionGroup()).toBeTruthy()
  expect(qState.canRegister('queue/3/group/999').canProcessPartitionKey('key/999')).toBeTruthy()
})

test('canRegister: incompatible guard version => error', () => {
  const qState = createQState({queueCount: 3})

  qState.registerMessage('queue/1/group/1', 'key/1')

  const guard1 = qState.canRegister('queue/1/group/1')
  const guard2 = qState.canRegister('queue/2/group/2')
  qState.registerMessage('queue/3/group/3', 'key/3')

  expect(() => guard1.canProcessPartitionGroup()).toThrow()
  expect(() => guard1.canProcessPartitionKey('key/1')).toThrow()

  expect(() => guard2.canProcessPartitionGroup()).toThrow()
  expect(() => guard2.canProcessPartitionKey('key/1')).toThrow()
})

function createQState(params: {
  queueCount: number
  queueSizeLimit?: number
  singlePartitionGroupLimit?: number
  singlePartitionKeyLimit?: number
  onMessageProcessed?: (messageId: string, mirrorDeliveryTag: number, responseDeliveryTag: number) => void
}) {
  const {
    queueCount,
    queueSizeLimit = 100,
    singlePartitionGroupLimit = 50,
    singlePartitionKeyLimit = 10,
    onMessageProcessed = () => {}
  } = params

  return new QState({
    queueCount,
    outputQueueName: index => `queue/${index}`,
    partitionGroupHash: partitionGroup => {
      return Number(partitionGroup.match(/^queue\/([0-9]*)\//i)![1]) - 1
    },
    queueSizeLimit,
    singlePartitionGroupLimit,
    singlePartitionKeyLimit,
    onMessageProcessed
  })
}
