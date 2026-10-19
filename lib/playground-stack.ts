import { fileURLToPath } from "node:url";

import { CfnOutput, Duration, RemovalPolicy, Stack, type StackProps } from "aws-cdk-lib";
import { Runtime } from "aws-cdk-lib/aws-lambda";
import { SqsEventSource } from "aws-cdk-lib/aws-lambda-event-sources";
import { NodejsFunction } from "aws-cdk-lib/aws-lambda-nodejs";
import { LogGroup, RetentionDays } from "aws-cdk-lib/aws-logs";
import { Topic } from "aws-cdk-lib/aws-sns";
import { SqsSubscription } from "aws-cdk-lib/aws-sns-subscriptions";
import { Queue } from "aws-cdk-lib/aws-sqs";
import type { Construct } from "constructs";

import { CONSUMERS, type ConsumerName, dlqName, logGroupName, TOPIC_NAME } from "../src/topology.ts";

const FUNCTION_TIMEOUT = Duration.seconds(5);

export class PlaygroundStack extends Stack {
  constructor(scope: Construct, id: string, props?: StackProps) {
    super(scope, id, props);

    const topic = new Topic(this, "DesignChanges", { topicName: TOPIC_NAME });
    new CfnOutput(this, "TopicArn", { value: topic.topicArn });

    this.addConsumer("audit", topic);
  }

  private addConsumer(name: ConsumerName, topic: Topic): void {
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
      deadLetterQueue: { queue: deadLetterQueue, maxReceiveCount: 10 },
    });

    topic.addSubscription(new SqsSubscription(queue, { rawMessageDelivery: true }));

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
  }
}
