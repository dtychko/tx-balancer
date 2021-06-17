interface ListNode<TValue> {
  value: TValue
  prev: ListNode<TValue>
  next: ListNode<TValue>
}

export default class CircularList<TValue> {
  private lastNode: ListNode<TValue> | undefined

  public insertLast(value: TValue): void {
    if (this.lastNode === undefined) {
      // tslint:disable-next-line:no-any
      const node: {[key: string]: any} = {
        value,
        prev: undefined,
        next: undefined
      }
      node.prev = node
      node.next = node
      this.lastNode = node as ListNode<TValue>
      return
    }

    const newLastNode = {
      value,
      prev: this.lastNode,
      next: this.lastNode.next
    }
    this.lastNode.next.prev = newLastNode
    this.lastNode.next = newLastNode
    this.lastNode = newLastNode
  }

  public findNext(predicate: (value: TValue) => boolean): TValue | undefined {
    const nextMatchingNode = this.findNextMatchingNode(predicate)
    if (nextMatchingNode === undefined) {
      return undefined
    }

    this.lastNode = nextMatchingNode
    return nextMatchingNode.value
  }

  public deleteLast(): boolean {
    return this.deleteNode(this.lastNode)
  }

  public delete(value: TValue): boolean {
    const matchingNode = this.findNextMatchingNode(x => x === value)
    return this.deleteNode(matchingNode)
  }

  public clear(): void {
    this.lastNode = undefined
  }

  public size(): number {
    if (this.lastNode === undefined) {
      return 0
    }

    let count = 1
    let currentNode = this.lastNode.next
    while (currentNode !== this.lastNode) {
      count += 1
      currentNode = currentNode.next
    }

    return count
  }

  private findNextMatchingNode(predicate: (value: TValue) => boolean): ListNode<TValue> | undefined {
    if (this.lastNode === undefined) {
      return undefined
    }

    // noinspection UnnecessaryLocalVariableJS
    let currentNode = this.lastNode
    do {
      currentNode = currentNode.next
    } while (currentNode !== this.lastNode && !predicate(currentNode.value))

    return predicate(currentNode.value) ? currentNode : undefined
  }

  private deleteNode(node: ListNode<TValue> | undefined): boolean {
    if (node === undefined) {
      return false
    }

    if (node.next === node) {
      this.lastNode = undefined
      return true
    }

    node.prev.next = node.next
    node.next.prev = node.prev
    if (node === this.lastNode) {
      this.lastNode = this.lastNode.prev
    }
    return true
  }
}
