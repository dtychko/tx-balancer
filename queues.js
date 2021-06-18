function main() {
  // const queue = ArrayQueue();
  // const queue = LinedListQueue();

  for (const queue of [ArrayQueue(), LinedListQueue(), DoubleLinedListQueue()]) {
    queue.enqueue(42);
    queue.dequeue(42);

    const startedAt = Date.now();

    for (let j = 0; j < 1000; j++) {
      queue.enqueue(j);
    }

    for (let i = 0; i < 1000000; i++) {
      // queue.enqueue(queue.dequeue());
      queue.enqueue(queue.tryDequeue(x => x > 2));
    }
    // for (let i = 0; i < 10000; i++) {
    //   for (let j = 0; j < 1000; j++) {
    //     queue.enqueue(j);
    //   }
    //   while (queue.tryDequeue(x => x % 5 === 0) !== undefined) {}
    //   while (!queue.isEmpty()) {
    //     queue.dequeue();
    //   }
    // }

    console.log(Date.now() - startedAt);

    // while (!queue.isEmpty()) {
    //   console.log(queue.dequeue());
    // }
  }
}

main();

function ArrayQueue() {
  const array = [];

  return {
    isEmpty() {
      return array.length === 0;
    },
    enqueue(value) {
      array.push(value);
    },
    dequeue() {
      return array.shift();
    },
    tryDequeue(predicate) {
      const index = array.findIndex(predicate);
      if (index === -1) {
        return undefined;
      }
      return array.splice(index, 1)[0];
    }
  };
}

function LinedListQueue() {
  let head = null;
  let tail = null;

  return {
    isEmpty() {
      return head === null;
    },
    enqueue(value) {
      if (!head) {
        head = tail = {value, next: null};
      } else {
        const node = {value, next: null};
        tail.next = node;
        tail = node;
      }
    },
    dequeue() {
      const value = head.value;
      if (head === tail) {
        head = tail = null;
      } else {
        head = head.next;
      }
      return value;
    },
    tryDequeue(predicate) {
      if (!head) {
        return undefined;
      }
      if (predicate(head.value)) {
        return this.dequeue();
      }

      let curr = head.next;
      let prev = head;

      while (curr) {
        if (predicate(curr.value)) {
          prev.next = curr.next;
          curr.next = null;
          return curr.value;
        }
        prev = curr;
        curr = curr.next;
      }

      return undefined;
    }
  };
}

function DoubleLinedListQueue() {
  let head = null;
  let tail = null;

  return {
    isEmpty() {
      return head === null;
    },
    enqueue(value) {
      if (!head) {
        head = tail = {value, next: null, prev: null};
      } else {
        const node = {value, next: null, prev: tail};
        tail.next = node;
        tail = node;
      }
    },
    dequeue() {
      const value = head.value;
      if (head === tail) {
        head = tail = null;
      } else {
        head = head.next;
        head.prev = null;
      }
      return value;
    },
    tryDequeue(predicate) {
      if (!head) {
        return undefined;
      }

      let curr = head;

      while (curr) {
        if (predicate(curr.value)) {
          if (curr.next === null && curr.prev === null) {
            head = tail = null;
          } else {
            if (curr.next) {
              curr.next.prev = curr.prev;
            }
            if (curr.prev) {
              curr.prev.next = curr.next;
            }
          }

          return curr.value;
        }

        curr = curr.next;
      }

      return undefined;
    }
  };
}
