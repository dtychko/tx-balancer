import Centrifuge3 from './Centrifuge3'
import {PartitionGroupGuard} from '../QState'

export default class PartitionGroupQueue3 {
  private readonly partitionGroupCentrifuges = new Map<string, Centrifuge3<number>>()
  private readonly partitionGroupQueue = [] as {partitionGroup: string; centrifuge: Centrifuge3<number>}[]

  private messageCount = 0

  public status() {
    return {
      messageCount: this.messageCount,
      partitionGroupCount: this.partitionGroupQueue.length,
      partitionKeyCount: this.partitionGroupQueue.reduce((acc, {centrifuge}) => {
        return acc + centrifuge.partitionCount()
      }, 0)
    }
  }

  public size() {
    return this.messageCount
  }

  public enqueue(messageId: number, partitionGroup: string, partitionKey: string): {partitionKeyAdded: boolean} {
    let centrifuge = this.partitionGroupCentrifuges.get(partitionGroup)

    if (!centrifuge) {
      centrifuge = new Centrifuge3<number>()
      this.partitionGroupCentrifuges.set(partitionGroup, centrifuge)
      this.partitionGroupQueue.push({partitionGroup, centrifuge})
    }

    const partitionSize = centrifuge.enqueue(messageId, partitionKey)
    this.messageCount += 1

    return {
      partitionKeyAdded: partitionSize === 1
    }
  }

  public tryDequeue(
    canProcess: (partitionGroup: string) => PartitionGroupGuard
  ): {messageId: number; partitionGroup: string; partitionKey: string} | undefined {
    for (let i = 0; i < this.partitionGroupQueue.length; i++) {
      const {partitionGroup, centrifuge} = this.partitionGroupQueue[i]
      const guard = canProcess(partitionGroup)

      if (!guard.canProcessPartitionGroup()) {
        continue
      }

      const dequeued = centrifuge.tryDequeue(partitionKey => guard.canProcessPartitionKey(partitionKey))

      if (dequeued) {
        this.partitionGroupQueue.splice(i, 1)
        this.messageCount -= 1

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
