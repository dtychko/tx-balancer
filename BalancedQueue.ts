export default class BalancedQueue<TValue> {
  private readonly onPartitionAdded: (partitionKey: string) => void

  private readonly partitions: Map<string, TValue[]> = new Map<string, TValue[]>()
  private readonly partitionQueue: PartitionQueue = new PartitionQueue()

  constructor(onPartitionAdded: (partitionKey: string) => void) {
    this.onPartitionAdded = onPartitionAdded
  }

  public enqueue(value: TValue, partitionKey: string) {
    let partition = this.partitions.get(partitionKey)
    if (!partition) {
      partition = []
      this.partitions.set(partitionKey, partition)
      this.partitionQueue.enqueue(partitionKey)
    }

    partition.push(value)

    if (partition.length === 1) {
      this.onPartitionAdded(partitionKey)
    }
  }

  public tryDequeue(predicate: (partitionKey: string) => boolean): {value: TValue; partitionKey: string} | undefined {
    const partitionKey = this.partitionQueue.tryDequeue(predicate)
    if (partitionKey === undefined) {
      return undefined
    }

    const partition = this.partitions.get(partitionKey)!

    if (partition.length > 1) {
      this.partitionQueue.enqueue(partitionKey)
    } else {
      this.partitions.delete(partitionKey)
    }

    return {
      value: partition.shift()!,
      partitionKey
    }
  }
}

class PartitionQueue {
  private readonly queue: string[] = []

  public enqueue(partitionKey: string) {
    this.queue.push(partitionKey)
  }

  public tryDequeue(predicate: (partitionKey: string) => boolean): string | undefined {
    const index = this.queue.findIndex(predicate)
    if (index === -1) {
      return undefined
    }

    const [partitionKey] = this.queue.splice(index, 1)
    return partitionKey
  }
}
