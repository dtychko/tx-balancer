import {LinkedListQueue} from './LinkedListQueue'

test('should dequeue last node with predicate', () => {
  const queue = new LinkedListQueue<number>()

  queue.enqueue(1)
  queue.enqueue(2)
  queue.tryDequeue(x => x === 2)
  queue.enqueue(3)
  queue.tryDequeue(() => true)

  expect(queue.isEmpty()).toBeFalsy()

  queue.tryDequeue(() => true)

  expect(queue.isEmpty()).toBeTruthy()
})
