export default class Centrifuge<TValue> {
  private readonly partitions = new Map<string, Partition<TValue>>()
  private readonly partitionQueue = new LinkedListQueue<Partition<TValue>>()
  private valueCount = 0

  public size() {
    return this.valueCount
  }

  public enqueue(value: TValue, partitionKey: string): number {
    let partition = this.partitions.get(partitionKey)
    if (!partition) {
      partition = new Partition<TValue>(partitionKey)
      this.partitions.set(partitionKey, partition)
      this.partitionQueue.enqueue(partition)
    }

    partition.enqueue(value)
    this.valueCount += 1

    return partition.size()
  }

  public tryDequeue(predicate: (partitionKey: string) => boolean): {value: TValue; partitionKey: string} | undefined {
    const dequeued = this.partitionQueue.tryDequeue(p => predicate(p.partitionKey))
    if (!dequeued) {
      return undefined
    }

    const partition = dequeued.value

    if (partition.size() > 1) {
      this.partitionQueue.enqueue(partition)
    } else {
      this.partitions.delete(partition.partitionKey)
    }

    this.valueCount -= 1

    return {
      value: partition.dequeue(),
      partitionKey: partition.partitionKey
    }
  }
}

class Partition<TValue> {
  public readonly partitionKey: string
  private readonly queue: TValue[] = []

  constructor(partitionKey: string) {
    this.partitionKey = partitionKey
  }

  public enqueue(value: TValue) {
    this.queue.push(value)
  }

  public dequeue(): TValue {
    if (!this.size()) {
      throw new Error('Queue is empty')
    }
    return this.queue.shift()!
  }

  public size(): number {
    return this.queue.length
  }
}

interface LinkedListNode<TValue> {
  value: TValue
  next: LinkedListNode<TValue> | undefined
}

export class LinkedListQueue<TValue> {
  private head: LinkedListNode<TValue> | undefined = undefined
  private tail: LinkedListNode<TValue> | undefined = undefined

  public isEmpty() {
    return this.head === undefined
  }

  public enqueue(value: TValue) {
    if (this.head === undefined) {
      this.head = this.tail = {value, next: undefined}
    } else {
      const node = {value, next: undefined}
      this.tail!.next = node
      this.tail = node
    }
  }

  public dequeue(): {value: TValue} {
    if (this.head === undefined) {
      throw new Error('Queue is empty!')
    }

    const value = this.head.value

    if (this.head === this.tail) {
      this.head = this.tail = undefined
    } else {
      this.head = this.head!.next
    }

    return {value}
  }

  public tryDequeue(predicate: (value: TValue) => boolean): {value: TValue} | undefined {
    if (this.head === undefined) {
      return undefined
    }

    if (predicate(this.head.value)) {
      return this.dequeue()
    }

    let curr = this.head.next
    let prev = this.head

    while (curr) {
      if (predicate(curr.value)) {
        if (curr.next) {
          // curr is an intermediate node, just remove it
          prev.next = curr.next
          curr.next = undefined
        } else {
          // curr is a tail node, remove it and set tail to prev
          prev.next = undefined
          this.tail = prev
        }

        return {value: curr.value}
      }

      prev = curr
      curr = curr.next
    }

    return undefined
  }
}
