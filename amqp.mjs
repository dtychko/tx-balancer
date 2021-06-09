import * as defs from 'amqplib/lib/defs.js';

export async function txSelect(ch) {
  await ch.rpc(defs.TxSelect, {nowait: false}, defs.TxSelectOk);
}

export async function txCommit(ch) {
  await ch.rpc(defs.TxCommit, {nowait: false}, defs.TxCommitOk);
}

let lastCommit = Promise.resolve();
const scheduledActions = [];

const lastCommitByChannel = new Map();
const scheduledActionsByChannel = new Map();

export async function txCommitSchedule(ch, action) {
  const scheduledActions = scheduledActionsByChannel.get(ch);
  if (scheduledActions) {
    scheduledActions.push(action);
    return lastCommitByChannel.get(ch);
  }

  scheduledActionsByChannel.set(ch, [action]);

  const capturedLastCommit = lastCommitByChannel.get(ch) || Promise.resolve();
  const lastCommit = new Promise(async res => {
    await capturedLastCommit;
    const capturedScheduledActions = scheduledActionsByChannel.get(ch);
    scheduledActionsByChannel.delete(ch);
    while (capturedScheduledActions.length) {
      capturedScheduledActions.shift()();
    }
    await txCommit(ch);
    res();
  });

  lastCommitByChannel.set(ch, lastCommit);
  return lastCommit;
}

export function handleMessage(handler) {
  return async msg => {
    if (!msg) {
      console.error(`[CRITICAL] Consumer canceled by broker`);
      return;
    }

    try {
      await handler(msg);
    } catch (err) {
      console.error(`[CRITICAL] Unexpected error during message handling: ${err}`);
    }
  };
}