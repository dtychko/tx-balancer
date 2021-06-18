import Centrifuge from './Centrifuge'

export default class PartitionGroupQueue {
  private readonly partitionGroupCentrifuges = new Map<string, Centrifuge<number>>()
  private readonly partitionGroupQueue = [] as {partitionGroup: string; centrifuge: Centrifuge<number>}[]

  public enqueue(messageId: number, partitionGroup: string, partitionKey: string): {partitionKeyAdded: boolean} {
    let centrifuge = this.partitionGroupCentrifuges.get(partitionGroup)

    if (!centrifuge) {
      centrifuge = new Centrifuge<number>()
      this.partitionGroupCentrifuges.set(partitionGroup, centrifuge)
      this.partitionGroupQueue.push({partitionGroup, centrifuge})
    }

    const partitionSize = centrifuge.enqueue(messageId, partitionKey)

    return {
      partitionKeyAdded: partitionSize === 1
    }
  }

  public tryDequeue(
    partitionGroupPredicate: (partitionGroup: string) => boolean,
    partitionKeyPredicate: (partitionKey: string) => boolean
  ): {messageId: number; partitionGroup: string; partitionKey: string} | undefined {
    for (let i = 0; i < this.partitionGroupQueue.length; i++) {
      const {partitionGroup, centrifuge} = this.partitionGroupQueue[i]

      if (!partitionGroupPredicate(partitionGroup)) {
        continue
      }

      const dequeued = centrifuge.tryDequeue(partitionKeyPredicate)

      if (dequeued) {
        this.partitionGroupQueue.splice(i, 1)

        if (centrifuge.size()) {
          this.partitionGroupQueue.push({partitionGroup, centrifuge})
        } else {
          this.partitionGroupCentrifuges.delete(partitionGroup)
        }

        return {messageId: dequeued.value, partitionGroup, partitionKey: dequeued.partitionKey}
      }
    }

    return undefined
  }
}
