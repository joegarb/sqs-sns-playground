import type { Context, SQSEvent } from "aws-lambda";

import type { DesignChangeEvent } from "../src/design-change.ts";

// Builds the event an SQS event source mapping hands to a Lambda, one record per
// change, with the change's ID as the SQS message ID.
export function sqsEvent(events: readonly DesignChangeEvent[], receiveCount = 1): SQSEvent {
  return {
    Records: events.map((event) => ({
      messageId: event.changeId,
      receiptHandle: `receipt-${event.changeId}`,
      body: JSON.stringify(event),
      attributes: {
        ApproximateReceiveCount: String(receiveCount),
        SentTimestamp: "0",
        SenderId: "sns",
        ApproximateFirstReceiveTimestamp: "0",
      },
      messageAttributes: {},
      md5OfBody: "",
      eventSource: "aws:sqs",
      eventSourceARN: "arn:aws:sqs:us-east-1:000000000000:test",
      awsRegion: "us-east-1",
    })),
  };
}

export const context = {
  functionName: "test",
  awsRequestId: "test-request",
  getRemainingTimeInMillis: () => 5000,
} as Context;
