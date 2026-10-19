import { ConditionalCheckFailedException, DynamoDBClient } from "@aws-sdk/client-dynamodb";
import { DynamoDBDocumentClient, PutCommand } from "@aws-sdk/lib-dynamodb";

import { sqsConsumer } from "./consumer.ts";
import type { DesignChangeEvent } from "./design-change.ts";

export interface AuditLog {
  record(event: DesignChangeEvent): Promise<"recorded" | "duplicate">;
}

// Delivery is at-least-once, so the same change can arrive more than once. The
// conditional write makes recording idempotent on `changeId` without needing FIFO
// deduplication, and holds across Lambda instances.
export function dynamoAuditLog(tableName: string): AuditLog {
  const db = DynamoDBDocumentClient.from(new DynamoDBClient());
  return {
    async record(event) {
      try {
        await db.send(
          new PutCommand({
            TableName: tableName,
            Item: { ...event, recordedAt: new Date().toISOString() },
            ConditionExpression: "attribute_not_exists(changeId)",
          }),
        );
        return "recorded";
      } catch (error) {
        if (error instanceof ConditionalCheckFailedException) return "duplicate";
        throw error;
      }
    },
  };
}

export function auditConsumer(auditLog: AuditLog) {
  return sqsConsumer("audit", async (event, logger) => {
    const outcome = await auditLog.record(event);
    logger.info(outcome === "recorded" ? "recorded" : "skipped duplicate", {
      project: event.project,
      revision: event.revision,
    });
  });
}
