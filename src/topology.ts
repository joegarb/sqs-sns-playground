// Names shared by the CDK stack and the local scripts. Fixed physical names keep
// the scripts and logs readable; CDK would otherwise generate them.
export const STACK_NAME = "SqsSnsPlayground";
export const TOPIC_NAME = "design-changes";

export interface ConsumerSpec {
  queueName: string;
  functionName: string;
}

export const CONSUMERS = {
  audit: { queueName: "audit-log", functionName: "design-changes-audit" },
} satisfies Record<string, ConsumerSpec>;

export type ConsumerName = keyof typeof CONSUMERS;

export const dlqName = (consumer: ConsumerSpec): string => `${consumer.queueName}-dlq`;
export const logGroupName = (consumer: ConsumerSpec): string => `/aws/lambda/${consumer.functionName}`;
