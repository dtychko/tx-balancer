import PartitionGroupQueue3 from './PartitionGroupQueue3'

test('tryDequeue with different partition group guards', () => {
  const queue = new PartitionGroupQueue3()

  expect(queue.enqueue(1, 'group/1', 'key/1')).toEqual({partitionKeyAdded: true})
  expect(queue.enqueue(2, 'group/1', 'key/2')).toEqual({partitionKeyAdded: true})
  expect(queue.enqueue(3, 'group/1', 'key/2')).toEqual({partitionKeyAdded: false})
  expect(queue.enqueue(4, 'group/2', 'key/1')).toEqual({partitionKeyAdded: true})
  expect(queue.enqueue(5, 'group/2', 'key/1')).toEqual({partitionKeyAdded: false})
  expect(queue.enqueue(6, 'group/2', 'key/2')).toEqual({partitionKeyAdded: true})

  expect(queue.size()).toBe(6)

  const canProcessGroup3 = (pg: string) => ({
    canProcessPartitionGroup: () => pg === 'group/3',
    canProcessPartitionKey: () => true
  })
  expect(queue.tryDequeue(canProcessGroup3)).toBeUndefined()
  expect(queue.size()).toBe(6)

  const canProcessGroup2 = (pg: string) => ({
    canProcessPartitionGroup: () => pg === 'group/2',
    canProcessPartitionKey: () => true
  })
  expect(queue.tryDequeue(canProcessGroup2)).toEqual({messageId: 4, partitionGroup: 'group/2', partitionKey: 'key/1'})
  expect(queue.tryDequeue(canProcessGroup2)).toEqual({messageId: 6, partitionGroup: 'group/2', partitionKey: 'key/2'})
  expect(queue.tryDequeue(canProcessGroup2)).toEqual({messageId: 5, partitionGroup: 'group/2', partitionKey: 'key/1'})
  expect(queue.tryDequeue(canProcessGroup2)).toBeUndefined()
  expect(queue.size()).toBe(3)

  const canProcessGroup1Key2 = (pg: string) => ({
    canProcessPartitionGroup: () => pg === 'group/1',
    canProcessPartitionKey: (pk: string) => pk === 'key/2'
  })
  expect(queue.tryDequeue(canProcessGroup1Key2)).toEqual({
    messageId: 2,
    partitionGroup: 'group/1',
    partitionKey: 'key/2'
  })
  expect(queue.tryDequeue(canProcessGroup1Key2)).toEqual({
    messageId: 3,
    partitionGroup: 'group/1',
    partitionKey: 'key/2'
  })
  expect(queue.tryDequeue(canProcessGroup2)).toBeUndefined()
  expect(queue.size()).toBe(1)
})
