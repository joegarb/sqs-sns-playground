import assert from "node:assert/strict";
import { after, before, test } from "node:test";

import { CloudWatchLogsClient } from "@aws-sdk/client-cloudwatch-logs";
import { DynamoDBClient } from "@aws-sdk/client-dynamodb";
import { SQSClient } from "@aws-sdk/client-sqs";
import { BatchWriteCommand, DynamoDBDocumentClient, ScanCommand } from "@aws-sdk/lib-dynamodb";

import { DESIGN_CHANGE_BATCH, POISON_CHANGE_ID } from "../../src/design-change.ts";
import { drain, peek, redrive } from "../../src/scripts/dlq.ts";
import { readLogs } from "../../src/scripts/logs.ts";
import { publishDesignChanges } from "../../src/scripts/publish.ts";
import { stackOutput } from "../../src/scripts/stack.ts";
import { AUDIT_TABLE_NAME, CONSUMERS, type ConsumerName } from "../../src/topology.ts";

// End to end against the deployed stack. Only runs against an emulator (never
// real AWS), and skips if it isn't up or the stack isn't deployed.
async function skipReason(): Promise<string | false> {
  if (!process.env.AWS_ENDPOINT_URL) return "AWS_ENDPOINT_URL is not set (copy .env.example to .env)";
  try {
    await stackOutput("TopicArn");
    return false;
  } catch (error) {
    return `stack not reachable: ${(error as Error).message}`;
  }
}
const skip = await skipReason();

const sqs = new SQSClient();
const logs = new CloudWatchLogsClient();
const db = DynamoDBDocumentClient.from(new DynamoDBClient());
let startTime = 0;

async function eventually<T>(read: () => Promise<T>, done: (value: T) => boolean, timeoutMs = 90_000): Promise<T> {
  const deadline = Date.now() + timeoutMs;
  for (;;) {
    const value = await read();
    if (done(value) || Date.now() > deadline) return value;
    await new Promise((resolve) => setTimeout(resolve, 1000));
  }
}

async function loggedChangeIds(consumer: ConsumerName, message: string): Promise<string[]> {
  const entries = await readLogs(logs, consumer, startTime);
  return entries.filter(({ fields }) => fields.message === message).map(({ fields }) => String(fields.changeId));
}

async function auditedChangeIds(): Promise<string[]> {
  const { Items = [] } = await db.send(new ScanCommand({ TableName: AUDIT_TABLE_NAME }));
  return Items.map((item) => String(item.changeId)).sort();
}

const changeIds = (filter: (project: string) => boolean = () => true) =>
  DESIGN_CHANGE_BATCH.filter((event) => filter(event.project))
    .map((event) => event.changeId)
    .sort();

before(async () => {
  if (skip) return;
  for (const consumer of Object.keys(CONSUMERS) as ConsumerName[]) await drain(sqs, consumer);
  const { Items = [] } = await db.send(new ScanCommand({ TableName: AUDIT_TABLE_NAME }));
  for (let i = 0; i < Items.length; i += 25) {
    await db.send(
      new BatchWriteCommand({
        RequestItems: {
          [AUDIT_TABLE_NAME]: Items.slice(i, i + 25).map((item) => ({
            DeleteRequest: { Key: { changeId: item.changeId as string } },
          })),
        },
      }),
    );
  }
  startTime = Date.now();
});

after(() => {
  sqs.destroy();
  logs.destroy();
  db.destroy();
});

test("design changes fan out to every consumer", { skip, timeout: 300_000 }, async (t) => {
  await publishDesignChanges(DESIGN_CHANGE_BATCH);

  await t.test("audit records every change", async () => {
    const audited = await eventually(auditedChangeIds, (ids) => ids.length === DESIGN_CHANGE_BATCH.length);
    assert.deepEqual(audited, changeIds());
  });

  await t.test("checks processes every change except the poison one", async () => {
    const expected = changeIds().filter((id) => id !== POISON_CHANGE_ID);
    const processed = await eventually(
      () => loggedChangeIds("checks", "checks recomputed"),
      (ids) => ids.length >= expected.length,
    );
    assert.deepEqual(processed.sort(), expected);
  });

  await t.test("apollo only receives apollo changes", async () => {
    const expected = changeIds((project) => project === "apollo");
    const received = await eventually(
      () => loggedChangeIds("apollo", "received"),
      (ids) => ids.length >= expected.length,
    );
    assert.deepEqual(received.sort(), expected);
  });

  await t.test("audit skips every change when the batch is published again", async () => {
    await publishDesignChanges(DESIGN_CHANGE_BATCH);
    const skipped = await eventually(
      () => loggedChangeIds("audit", "skipped duplicate"),
      (ids) => ids.length >= DESIGN_CHANGE_BATCH.length,
    );
    assert.deepEqual(skipped.sort(), changeIds());
    assert.deepEqual(await auditedChangeIds(), changeIds());
  });

  await t.test("the poison change is dead-lettered on checks only", async () => {
    // Published twice, so two copies end up in the DLQ.
    const letters = await eventually(
      () => peek(sqs, "checks"),
      (found) => found.length >= 2,
    );
    assert.deepEqual(
      letters.map((letter) => (JSON.parse(letter.body) as { changeId: string }).changeId),
      [POISON_CHANGE_ID, POISON_CHANGE_ID],
    );
    assert.deepEqual(await peek(sqs, "audit"), []);
    assert.deepEqual(await peek(sqs, "apollo"), []);
  });

  await t.test("peek leaves the messages in place", async () => {
    assert.equal((await peek(sqs, "checks")).length, 2);
  });

  await t.test("redrive moves them back to the source queue, where they dead-letter again", async () => {
    assert.equal(await redrive(sqs, "checks"), 2);
    assert.equal((await peek(sqs, "checks")).length, 0);

    const letters = await eventually(
      () => peek(sqs, "checks"),
      (found) => found.length >= 2,
    );
    assert.equal(letters.length, 2);
  });

  await t.test("drain empties the dead-letter queue", async () => {
    assert.equal((await drain(sqs, "checks")).length, 2);
    assert.deepEqual(await peek(sqs, "checks"), []);
  });
});
