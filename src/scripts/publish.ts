import { PublishBatchCommand, SNSClient } from "@aws-sdk/client-sns";

import { DESIGN_CHANGE_BATCH, type DesignChangeEvent } from "../design-change.ts";
import { stackOutput } from "./stack.ts";

export async function publishDesignChanges(events: readonly DesignChangeEvent[]): Promise<void> {
  const sns = new SNSClient();
  try {
    const topicArn = await stackOutput("TopicArn");
    await sns.send(
      new PublishBatchCommand({
        TopicArn: topicArn,
        PublishBatchRequestEntries: events.map((event) => ({
          Id: event.changeId,
          Message: JSON.stringify(event),
          // Filter policies match on message attributes, not the body, so the
          // project is carried in both.
          MessageAttributes: { project: { DataType: "String", StringValue: event.project } },
        })),
      }),
    );
  } finally {
    sns.destroy();
  }
}

if (import.meta.main) {
  await publishDesignChanges(DESIGN_CHANGE_BATCH);
  for (const event of DESIGN_CHANGE_BATCH) {
    console.log(`published ${event.changeId} (project=${event.project}, revision=${event.revision})`);
  }
}
