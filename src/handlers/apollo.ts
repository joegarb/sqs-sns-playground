import { sqsConsumer } from "../consumer.ts";

export const handler = sqsConsumer("apollo", (event, logger) => {
  logger.info("received", { project: event.project, revision: event.revision });
});
