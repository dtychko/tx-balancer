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
