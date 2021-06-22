import {LinkedListQueue} from './Centrifuge'

const queue = new LinkedListQueue<number>()

queue.enqueue(1)
queue.enqueue(2)

queue.tryDequeue(x => x === 2)
queue.enqueue(3)
queue.tryDequeue(x => true)
console.log(queue.isEmpty()) // false
queue.tryDequeue(x => true)
console.log(queue.isEmpty()) // true

// queue.enqueue(1)
// console.log(queue.tryDequeue(x => x === 100))
// queue.enqueue(2)
// console.log(queue.tryDequeue(x => x === 2))
// console.log(queue.tryDequeue(x => x === 100))
// queue.enqueue(3)
// console.log(queue.tryDequeue(x => x === 100))
// console.log(queue.tryDequeue(x => true))
//
// console.log({size: queue.size(), isEmpty: queue.isEmpty()})
