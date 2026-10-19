import { sqsConsumer } from "../consumer.ts";

export const handler = sqsConsumer("audit", (event, logger) => {
  logger.info("recorded", { project: event.project, revision: event.revision });
});
