import { sqsConsumer } from "./consumer.ts";
import { POISON_CHANGE_ID } from "./design-change.ts";

// Recomputes downstream checks, and always fails on the poison change so it can be
// watched being retried and then dead-lettered.
export const checksConsumer = sqsConsumer("checks", (event, logger) => {
  if (event.changeId === POISON_CHANGE_ID) {
    throw new Error(`cannot compute checks for ${event.changeId}`);
  }
  logger.info("checks recomputed", { project: event.project, revision: event.revision });
});
