import { BatchProcessor, EventType, processPartialResponse } from "@aws-lambda-powertools/batch";
import { Logger } from "@aws-lambda-powertools/logger";
import type { Context, SQSBatchResponse, SQSEvent, SQSRecord } from "aws-lambda";

import { type DesignChangeEvent, parseDesignChange } from "./design-change.ts";

export type ChangeHandler = (event: DesignChangeEvent, logger: Logger) => Promise<void>;

// Wraps a per-change handler as an SQS-triggered Lambda. Each record succeeds or
// fails on its own: failures are reported as batch item failures, so only those
// messages go back to the queue (and on to the DLQ once maxReceiveCount is hit).
export function sqsConsumer(serviceName: string, handle: ChangeHandler) {
  const logger = new Logger({ serviceName });
  const processor = new BatchProcessor(EventType.SQS);

  const processRecord = async (record: SQSRecord): Promise<void> => {
    let event: DesignChangeEvent;
    try {
      event = parseDesignChange(record.body);
    } catch (error) {
      logger.error("unreadable message", { messageId: record.messageId, error });
      throw error;
    }
    const receiveCount = Number(record.attributes.ApproximateReceiveCount);
    const recordLogger = logger.createChild({ persistentKeys: { changeId: event.changeId, receiveCount } });
    try {
      await handle(event, recordLogger);
    } catch (error) {
      recordLogger.error("failed", error as Error);
      throw error;
    }
  };

  return async (event: SQSEvent, context: Context): Promise<SQSBatchResponse> =>
    processPartialResponse(event, processRecord, processor, { context });
}
