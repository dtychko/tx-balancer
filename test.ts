const queues = new Map<string, {messageCount: number}>()
queues.set('output_1', {messageCount: 10})
queues.set('output_2', {messageCount: 2})
queues.set('output_3', {messageCount: 11})

const qs = [...queues.values()]

const startedAt = Date.now()

for (let i = 0; i < 1000000; i++) {
  const orderedQs = qs.sort((a, b) => a.messageCount - b.messageCount)
  const q = orderedQs[0]

  // let q = {value: qs[0].messageCount, index: 0}
  // for (let i = 1; i < qs.length; i++) {
  //   if (qs[i].messageCount < q.value) {
  //     q = {value: qs[i].messageCount, index: i}
  //   }
  // }
}

console.log(Date.now() - startedAt)
