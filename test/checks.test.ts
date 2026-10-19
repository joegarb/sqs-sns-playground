import assert from "node:assert/strict";
import { test } from "node:test";

import { checksConsumer } from "../src/checks.ts";
import { DESIGN_CHANGE_BATCH, POISON_CHANGE_ID } from "../src/design-change.ts";
import { context, sqsEvent } from "./sqs-event.ts";

test("only the poison change is reported back to SQS", async () => {
  const response = await checksConsumer(sqsEvent(DESIGN_CHANGE_BATCH), context);

  assert.deepEqual(response.batchItemFailures, [{ itemIdentifier: POISON_CHANGE_ID }]);
});

test("a batch without the poison change succeeds", async () => {
  const healthy = DESIGN_CHANGE_BATCH.filter((event) => event.changeId !== POISON_CHANGE_ID);

  const response = await checksConsumer(sqsEvent(healthy), context);

  assert.deepEqual(response.batchItemFailures, []);
});
