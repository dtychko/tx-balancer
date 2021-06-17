export default class Partition<TValue> {
  public readonly partitionKey: string
  private readonly queue: TValue[] = []

  constructor(partitionKey: string) {
    this.partitionKey = partitionKey
  }

  public enqueue(value: TValue) {
    this.queue.push(value)
  }

  public requeue(value: TValue) {
    this.queue.unshift(value)
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
