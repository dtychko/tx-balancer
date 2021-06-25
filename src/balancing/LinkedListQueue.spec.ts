import {LinkedListQueue} from './LinkedListQueue'

test('enqueue then dequeue', () => {
  const queue = new LinkedListQueue<number>()
  for (let i = 0; i < 10; i++) {
    queue.enqueue(i)
  }

  const dequeued = []
  for (let i = 0; i < 10; i++) {
    dequeued.push(queue.dequeue())
  }

  expect(queue.isEmpty()).toBeTruthy()
  expect(dequeued.map(x => x.value)).toEqual(Array.from({length: 10}, (_, i) => i))
})

test('enqueue then tryDequeue', () => {
  const queue = new LinkedListQueue<number>()
  for (let i = 0; i < 10; i++) {
    queue.enqueue(i)
  }

  const dequeued = []
  for (let i = 0; i < 10; i++) {
    dequeued.push(queue.tryDequeue(() => true))
  }

  expect(queue.isEmpty()).toBeTruthy()
  expect(dequeued.map(x => x!.value)).toEqual(Array.from({length: 10}, (_, i) => i))
})

test('enqueue then tryDequeue reversed', () => {
  const queue = new LinkedListQueue<number>()
  for (let i = 0; i < 10; i++) {
    queue.enqueue(i)
  }

  const dequeued = []
  for (let i = 9; i >= 0; i--) {
    dequeued.push(queue.tryDequeue(x => x === i))
  }

  expect(queue.isEmpty()).toBeTruthy()
  expect(dequeued.map(x => x!.value)).toEqual(Array.from({length: 10}, (_, i) => 9 - i))
})

test('bug: should not corrupt queue state after tryDequeue the last element', () => {
  const queue = new LinkedListQueue<number>()

  queue.enqueue(1)
  queue.enqueue(2)
  // Dequeue the last element
  expect(queue.tryDequeue(x => x === 2)).toEqual({value: 2})

  queue.enqueue(3)
  expect(queue.tryDequeue(() => true)).toEqual({value: 1})
  expect(queue.tryDequeue(() => true)).toEqual({value: 3})
  expect(queue.isEmpty()).toBeTruthy()
})
