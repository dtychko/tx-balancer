export function sumCharCodes(str: string) {
  let sum = 0
  for (let i = 0; i < str.length; i++) {
    sum += str.charCodeAt(i)
  }
  return sum
}

export async function waitFor(condition: () => boolean, ms: number = 100) {
  while (!condition()) {
    await sleep(ms)
  }
}

async function sleep(ms: number = 0) {
  return new Promise(res => {
    setTimeout(res, ms)
  })
}
