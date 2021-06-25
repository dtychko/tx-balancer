import Centrifuge3 from './Centrifuge3'

test('enqueue then tryDequeue', () => {
  const centrifuge = new Centrifuge3<number>()

  for (let i = 0; i < 5; i++) {
    for (let j = 0; j <= i; j++) {
      centrifuge.enqueue(j, `key/${i}`)
    }
  }

  expect(centrifuge.size()).toBe(15)

  const dequeued = []
  for (let i = 0; i < 15; i++) {
    dequeued.push(centrifuge.tryDequeue(() => true))
  }

  const expectedDequeued = []
  for (let i = 0; i < 5; i++) {
    for (let j = i; j < 5; j++) {
      expectedDequeued.push({value: i, partitionKey: `key/${j}`})
    }
  }

  expect(dequeued).toEqual(expectedDequeued)
  expect(centrifuge.size()).toBe(0)
})

test('tryDequeue with predicate', () => {
  const centrifuge = new Centrifuge3<number>()

  centrifuge.enqueue(1, 'key/1')
  centrifuge.enqueue(2, 'key/1')
  centrifuge.enqueue(3, 'key/2')
  centrifuge.enqueue(4, 'key/2')

  expect(centrifuge.size()).toBe(4)

  const dequeued = []
  while (true) {
    const result = centrifuge.tryDequeue(pk => pk === 'key/2')
    if (!result) {
      break
    }
    dequeued.push(result)
  }

  expect(dequeued).toEqual([
    {value: 3, partitionKey: 'key/2'},
    {value: 4, partitionKey: 'key/2'}
  ])
  expect(centrifuge.size()).toBe(2)

  expect(centrifuge.tryDequeue(pk => pk === 'key/3')).toBeUndefined()
  expect(centrifuge.size()).toBe(2)
})
