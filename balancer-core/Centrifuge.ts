import CircularList from './Centrifuge.CircularList'
import Partition from './Centrifuge.Partition'

interface Awaiter<TValue> {
  resolve: (val: PartitionValue<TValue>) => void
  reject: (error: Error) => void
  lockPartition: boolean
}

interface PartitionValue<TValue> {
  partitionKey: string
  value: TValue
}

type ProcessValue<TValue> = (value: PartitionValue<TValue>) => Promise<void>

export default class Centrifuge<TValue = unknown> {
  private readonly partitionList = new CircularList<Partition<TValue>>()
  private readonly partitionByKey = new Map<string, Partition<TValue>>()
  private readonly partitionLocks = new Set<string>()
  private readonly awaiters = [] as Awaiter<TValue>[]
  private totalSize = 0

  public enqueue(partitionKey: string, value: TValue): void {
    this.addToPartition(partitionKey, partition => partition.enqueue(value))
  }

  public requeue(partitionKey: string, value: TValue): void {
    this.addToPartition(partitionKey, partition => partition.requeue(value))
  }

  private addToPartition(partitionKey: string, add: (partition: Partition<TValue>) => void) {
    const partition = this.getOrAddPartitionByKey(partitionKey)
    add(partition)
    this.totalSize += 1
    this.pulseAwaiterQueue()
  }

  public waitForNext(): Promise<PartitionValue<TValue>> {
    return this.waitNextValue(false)
  }

  public async processNext(process: ProcessValue<TValue>, lockPartition?: boolean): Promise<void> {
    if (!lockPartition) {
      return process(await this.waitNextValue(false))
    }

    const nextValue = await this.waitNextValue(true)
    const {partitionKey} = nextValue

    try {
      await process(nextValue)
    } finally {
      this.partitionLocks.delete(partitionKey)
      this.pulseAwaiterQueue()
    }
  }

  public size(): number {
    return this.totalSize
  }

  public partitionKeys(): string[] {
    return [...this.partitionByKey.keys()]
  }

  public partitionCount(): number {
    return this.partitionByKey.size
  }

  public partitionSize(partitionKey: string): number {
    const partition = this.partitionByKey.get(partitionKey)
    return partition ? partition.size() : 0
  }

  public lockedPartitions(): string[] {
    return [...this.partitionLocks]
  }

  public isPartitionLocked(partitionKey: string): boolean {
    return this.partitionLocks.has(partitionKey)
  }

  private getOrAddPartitionByKey(partitionKey: string) {
    let partition = this.partitionByKey.get(partitionKey)

    if (partition === undefined) {
      partition = new Partition(partitionKey)
      this.partitionList.insertLast(partition)
      this.partitionByKey.set(partitionKey, partition)
    }

    return partition
  }

  private isPulseScheduled = false

  private pulseAwaiterQueue() {
    if (this.isPulseScheduled || !this.awaiters.length) {
      return
    }

    this.isPulseScheduled = true

    process.nextTick(() => {
      this.isPulseScheduled = false

      while (this.awaiters.length) {
        const lockPartition = this.awaiters[0].lockPartition
        const nextValue = this.getNextValue(lockPartition)
        if (nextValue === undefined) {
          return
        }

        const awaiter = this.awaiters.shift()!
        awaiter.resolve(nextValue)
      }
    })
  }

  private async waitNextValue(lockPartition: boolean): Promise<PartitionValue<TValue>> {
    return (
      this.getNextValue(lockPartition) ||
      new Promise((resolve, reject) => {
        this.awaiters.push({resolve, reject, lockPartition})
      })
    )
  }

  private getNextValue(lockPartition: boolean): PartitionValue<TValue> | undefined {
    const partition = this.partitionList.findNext(x => !this.partitionLocks.has(x.partitionKey))
    if (partition === undefined) {
      return undefined
    }

    if (lockPartition) {
      this.partitionLocks.add(partition.partitionKey)
    }

    const value = partition.dequeue()
    this.totalSize -= 1

    if (!partition.size()) {
      this.partitionList.deleteLast()
      this.partitionByKey.delete(partition.partitionKey)
    }

    return {partitionKey: partition.partitionKey, value}
  }
}
