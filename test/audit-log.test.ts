import assert from "node:assert/strict";
import { test } from "node:test";

import { type AuditLog, auditConsumer } from "../src/audit-log.ts";
import { DESIGN_CHANGE_BATCH, type DesignChangeEvent } from "../src/design-change.ts";
import { context, sqsEvent } from "./sqs-event.ts";

// Same semantics as the DynamoDB conditional write: the first record of a changeId wins.
function memoryAuditLog(): AuditLog & { recorded: DesignChangeEvent[] } {
  const recorded: DesignChangeEvent[] = [];
  return {
    recorded,
    record(event) {
      if (recorded.some((existing) => existing.changeId === event.changeId)) return Promise.resolve("duplicate");
      recorded.push(event);
      return Promise.resolve("recorded");
    },
  };
}

test("records every change in the batch", async () => {
  const auditLog = memoryAuditLog();

  const response = await auditConsumer(auditLog)(sqsEvent(DESIGN_CHANGE_BATCH), context);

  assert.deepEqual(response.batchItemFailures, []);
  assert.deepEqual(auditLog.recorded, DESIGN_CHANGE_BATCH);
});

test("a redelivered change is recorded once and not reported as a failure", async () => {
  const auditLog = memoryAuditLog();
  const handler = auditConsumer(auditLog);

  await handler(sqsEvent(DESIGN_CHANGE_BATCH), context);
  const response = await handler(sqsEvent(DESIGN_CHANGE_BATCH, 2), context);

  assert.deepEqual(response.batchItemFailures, []);
  assert.deepEqual(auditLog.recorded, DESIGN_CHANGE_BATCH);
});

test("a change that cannot be recorded is reported back to SQS on its own", async () => {
  const auditLog = memoryAuditLog();
  const flaky: AuditLog = {
    record: (event) =>
      event.changeId === "chg-1002" ? Promise.reject(new Error("throttled")) : auditLog.record(event),
  };

  const response = await auditConsumer(flaky)(sqsEvent(DESIGN_CHANGE_BATCH), context);

  assert.deepEqual(response.batchItemFailures, [{ itemIdentifier: "chg-1002" }]);
  assert.equal(auditLog.recorded.length, DESIGN_CHANGE_BATCH.length - 1);
});
