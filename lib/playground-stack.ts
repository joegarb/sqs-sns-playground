import { fileURLToPath } from "node:url";

import { CfnOutput, Duration, RemovalPolicy, Stack, type StackProps } from "aws-cdk-lib";
import { AttributeType, BillingMode, Table } from "aws-cdk-lib/aws-dynamodb";
import { Runtime } from "aws-cdk-lib/aws-lambda";
import { SqsEventSource } from "aws-cdk-lib/aws-lambda-event-sources";
import { NodejsFunction } from "aws-cdk-lib/aws-lambda-nodejs";
import { LogGroup, RetentionDays } from "aws-cdk-lib/aws-logs";
import { SubscriptionFilter, Topic } from "aws-cdk-lib/aws-sns";
import { SqsSubscription } from "aws-cdk-lib/aws-sns-subscriptions";
import { Queue } from "aws-cdk-lib/aws-sqs";
import type { Construct } from "constructs";

import { AUDIT_TABLE_NAME, CONSUMERS, type ConsumerName, dlqName, logGroupName, TOPIC_NAME } from "../src/topology.ts";

const FUNCTION_TIMEOUT = Duration.seconds(5);

export class PlaygroundStack extends Stack {
  constructor(scope: Construct, id: string, props?: StackProps) {
    super(scope, id, props);

    const topic = new Topic(this, "DesignChanges", { topicName: TOPIC_NAME });
    new CfnOutput(this, "TopicArn", { value: topic.topicArn });

    const auditTable = new Table(this, "AuditLog", {
      tableName: AUDIT_TABLE_NAME,
      partitionKey: { name: "changeId", type: AttributeType.STRING },
      billingMode: BillingMode.PAY_PER_REQUEST,
      removalPolicy: RemovalPolicy.DESTROY,
    });

    const audit = this.addConsumer("audit", topic);
    audit.addEnvironment("AUDIT_TABLE_NAME", auditTable.tableName);
    auditTable.grantWriteData(audit);

    this.addConsumer("checks", topic);
    // SNS applies the filter policy, so non-apollo changes never reach the queue
    // (and never cost a Lambda invocation).
    this.addConsumer("apollo", topic, {
      project: SubscriptionFilter.stringFilter({ allowlist: ["apollo"] }),
    });
  }

  private addConsumer(
    name: ConsumerName,
    topic: Topic,
    filterPolicy?: Record<string, SubscriptionFilter>,
  ): NodejsFunction {
    const spec = CONSUMERS[name];

    const deadLetterQueue = new Queue(this, `${name}-dlq`, {
      queueName: dlqName(spec),
      retentionPeriod: Duration.days(14),
    });
    const queue = new Queue(this, `${name}-queue`, {
      queueName: spec.queueName,
      // AWS recommends at least 6x the function timeout in production; kept short
      // here so a failed message comes back (and dead-letters) quickly.
      visibilityTimeout: Duration.seconds(10),
      deadLetterQueue: { queue: deadLetterQueue, maxReceiveCount: spec.maxReceiveCount },
    });

    topic.addSubscription(new SqsSubscription(queue, { rawMessageDelivery: true, filterPolicy }));

    const fn = new NodejsFunction(this, `${name}-function`, {
      functionName: spec.functionName,
      entry: fileURLToPath(new URL(`../src/handlers/${name}.ts`, import.meta.url)),
      runtime: Runtime.NODEJS_24_X,
      timeout: FUNCTION_TIMEOUT,
      logGroup: new LogGroup(this, `${name}-logs`, {
        logGroupName: logGroupName(spec),
        retention: RetentionDays.ONE_WEEK,
        removalPolicy: RemovalPolicy.DESTROY,
      }),
    });
    fn.addEventSource(new SqsEventSource(queue, { batchSize: 10, reportBatchItemFailures: true }));
    return fn;
  }
}
