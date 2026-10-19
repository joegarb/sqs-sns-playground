import { sqsConsumer } from "../consumer.ts";

export const handler = sqsConsumer("audit", async (event, logger) => {
  logger.info("recorded", { project: event.project, revision: event.revision });
});
