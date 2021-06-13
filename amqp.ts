// @ts-ignore
import * as defs from 'amqplib/lib/defs'
import {Channel, Connection} from 'amqplib'

export interface TxChannel extends Channel {
  txCommit(): Promise<void>
  tx(action: (tx: Tx) => void): Promise<void>
}

type Tx = {
  ack: Channel['ack']
  publish: Channel['publish']
}

export async function createTxChannel(conn: Connection): Promise<TxChannel> {
  const ch = (await conn.createChannel()) as TxChannel
  await txSelect(ch)

  ch.txCommit = () => txCommit(ch)
  ch.tx = (action: (tx: Tx) => void) => txCommitSchedule(ch, () => action(ch))

  return ch
}

async function txSelect(ch: Channel) {
  await (ch as any).rpc(defs.TxSelect, {nowait: false}, defs.TxSelectOk)
}

async function txCommit(ch: Channel) {
  await (ch as any).rpc(defs.TxCommit, {nowait: false}, defs.TxCommitOk)
}

const lastCommitByChannel = new Map()
const scheduledActionsByChannel = new Map()

async function txCommitSchedule(ch: Channel, action: () => void) {
  const scheduledActions = scheduledActionsByChannel.get(ch)
  if (scheduledActions) {
    scheduledActions.push(action)
    return lastCommitByChannel.get(ch)
  }

  scheduledActionsByChannel.set(ch, [action])

  const capturedLastCommit = lastCommitByChannel.get(ch) || Promise.resolve()
  const lastCommit = new Promise<void>(async res => {
    await capturedLastCommit
    const capturedScheduledActions = scheduledActionsByChannel.get(ch)
    scheduledActionsByChannel.delete(ch)
    while (capturedScheduledActions.length) {
      capturedScheduledActions.shift()()
    }
    await txCommit(ch)
    res()
  })

  lastCommitByChannel.set(ch, lastCommit)
  return lastCommit
}
