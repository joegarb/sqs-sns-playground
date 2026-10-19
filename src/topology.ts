// Names shared by the CDK stack and the local scripts. Fixed physical names keep
// the scripts and logs readable; CDK would otherwise generate them.
export const STACK_NAME = "SqsSnsPlayground";
export const TOPIC_NAME = "design-changes";
export const AUDIT_TABLE_NAME = "design-change-audit";

export interface ConsumerSpec {
  queueName: string;
  functionName: string;
  // Receives before SQS moves a message to the consumer's dead-letter queue.
  maxReceiveCount: number;
}

export const CONSUMERS = {
  audit: { queueName: "audit-log", functionName: "design-changes-audit", maxReceiveCount: 10 },
  checks: { queueName: "downstream-checks", functionName: "design-changes-checks", maxReceiveCount: 3 },
  apollo: { queueName: "apollo-changes", functionName: "design-changes-apollo", maxReceiveCount: 10 },
} satisfies Record<string, ConsumerSpec>;

export type ConsumerName = keyof typeof CONSUMERS;

export const dlqName = (consumer: ConsumerSpec): string => `${consumer.queueName}-dlq`;
export const logGroupName = (consumer: ConsumerSpec): string => `/aws/lambda/${consumer.functionName}`;
