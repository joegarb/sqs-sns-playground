import { auditConsumer, dynamoAuditLog } from "../audit-log.ts";

const tableName = process.env.AUDIT_TABLE_NAME;
if (!tableName) throw new Error("AUDIT_TABLE_NAME is not set");

export const handler = auditConsumer(dynamoAuditLog(tableName));
