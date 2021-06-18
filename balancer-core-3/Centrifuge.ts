export default class Centrifuge<TValue> {
  private readonly partitions = new Map<string, Partition<TValue>>()
  private readonly partitionQueue = new PartitionQueue<TValue>()
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
    const partition = this.partitionQueue.tryDequeue(predicate)
    if (!partition) {
      return undefined
    }

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

class PartitionQueue<TValue> {
  private readonly queue: Partition<TValue>[] = []

  public enqueue(partition: Partition<TValue>) {
    this.queue.push(partition)
  }

  public tryDequeue(predicate: (partitionKey: string) => boolean): Partition<TValue> | undefined {
    const index = this.queue.findIndex(p => predicate(p.partitionKey))
    if (index === -1) {
      return undefined
    }

    const [partition] = this.queue.splice(index, 1)
    return partition
  }
}
