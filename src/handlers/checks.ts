import { sqsConsumer } from "../consumer.ts";

export const handler = sqsConsumer("checks", (event, logger) => {
  logger.info("checks recomputed", { project: event.project, revision: event.revision });
});
