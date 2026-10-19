import {
  ChangeMessageVisibilityBatchCommand,
  DeleteMessageBatchCommand,
  GetQueueAttributesCommand,
  GetQueueUrlCommand,
  ListMessageMoveTasksCommand,
  type Message,
  ReceiveMessageCommand,
  SQSClient,
  StartMessageMoveTaskCommand,
} from "@aws-sdk/client-sqs";

import { CONSUMERS, type ConsumerName, dlqName } from "../topology.ts";

// Operates on a consumer's dead-letter queue:
//   npm run dlq -- <consumer>           peek (default): list what's there, non-destructively
//   npm run dlq -- <consumer> redrive   move everything back to the source queue
//   npm run dlq -- <consumer> drain     log each message, then delete it

export interface DeadLetter {
  messageId: string;
  body: string;
  receiveCount: number;
  sourceQueueArn: string | undefined;
  sentAt: Date;
}

const VERBS = ["peek", "redrive", "drain"] as const;
type Verb = (typeof VERBS)[number];

async function queueUrl(sqs: SQSClient, consumer: ConsumerName): Promise<string> {
  const { QueueUrl } = await sqs.send(new GetQueueUrlCommand({ QueueName: dlqName(CONSUMERS[consumer]) }));
  if (!QueueUrl) throw new Error(`No dead-letter queue for ${consumer}. Run \`npm run deploy\` first.`);
  return QueueUrl;
}

async function receiveAll(sqs: SQSClient, url: string, visibilityTimeout: number): Promise<Message[]> {
  const messages: Message[] = [];
  for (;;) {
    const { Messages = [] } = await sqs.send(
      new ReceiveMessageCommand({
        QueueUrl: url,
        MaxNumberOfMessages: 10,
        VisibilityTimeout: visibilityTimeout,
        MessageSystemAttributeNames: ["All"],
      }),
    );
    if (Messages.length === 0) return messages;
    messages.push(...Messages);
  }
}

function toDeadLetter(message: Message): DeadLetter {
  return {
    messageId: message.MessageId ?? "",
    body: message.Body ?? "",
    receiveCount: Number(message.Attributes?.ApproximateReceiveCount),
    sourceQueueArn: message.Attributes?.DeadLetterQueueSourceArn,
    sentAt: new Date(Number(message.Attributes?.SentTimestamp)),
  };
}

// SQS has no true peek. Each message is received (which hides it, so the loop
// doesn't see it twice) and then made visible again straight away. Receiving
// bumps the receive count, which is harmless on a DLQ with no redrive policy.
export async function peek(sqs: SQSClient, consumer: ConsumerName): Promise<DeadLetter[]> {
  const url = await queueUrl(sqs, consumer);
  const messages = await receiveAll(sqs, url, 30);
  for (let i = 0; i < messages.length; i += 10) {
    await sqs.send(
      new ChangeMessageVisibilityBatchCommand({
        QueueUrl: url,
        Entries: messages.slice(i, i + 10).map((message, index) => ({
          Id: String(index),
          ReceiptHandle: message.ReceiptHandle,
          VisibilityTimeout: 0,
        })),
      }),
    );
  }
  return messages.map(toDeadLetter);
}

// The native redrive: SQS moves the messages back to whichever queue each one was
// dead-lettered from. Still-poison messages will fail and dead-letter again.
export async function redrive(sqs: SQSClient, consumer: ConsumerName): Promise<number> {
  const url = await queueUrl(sqs, consumer);
  const { Attributes } = await sqs.send(new GetQueueAttributesCommand({ QueueUrl: url, AttributeNames: ["QueueArn"] }));
  const sourceArn = Attributes?.QueueArn;
  if (!sourceArn) throw new Error(`No ARN for ${url}`);

  const { TaskHandle } = await sqs.send(new StartMessageMoveTaskCommand({ SourceArn: sourceArn }));
  const deadline = Date.now() + 60_000;
  while (Date.now() < deadline) {
    const { Results = [] } = await sqs.send(new ListMessageMoveTasksCommand({ SourceArn: sourceArn }));
    const task = Results.find((result) => result.TaskHandle === TaskHandle) ?? Results[0];
    if (task?.Status === "COMPLETED") return task.ApproximateNumberOfMessagesMoved ?? 0;
    if (task?.Status === "FAILED" || task?.Status === "CANCELLED") {
      throw new Error(`Move task ${task.Status}: ${task.FailureReason ?? "no reason given"}`);
    }
    await new Promise((resolve) => setTimeout(resolve, 500));
  }
  throw new Error(`Move task ${TaskHandle ?? ""} didn't finish within a minute`);
}

// Deletes rather than PurgeQueue, so there's a record of what was thrown away.
export async function drain(sqs: SQSClient, consumer: ConsumerName): Promise<DeadLetter[]> {
  const url = await queueUrl(sqs, consumer);
  const messages = await receiveAll(sqs, url, 60);
  for (let i = 0; i < messages.length; i += 10) {
    await sqs.send(
      new DeleteMessageBatchCommand({
        QueueUrl: url,
        Entries: messages.slice(i, i + 10).map((message, index) => ({
          Id: String(index),
          ReceiptHandle: message.ReceiptHandle,
        })),
      }),
    );
  }
  return messages.map(toDeadLetter);
}

function describe(letter: DeadLetter): string {
  const source = letter.sourceQueueArn ? ` from=${letter.sourceQueueArn.split(":").pop() ?? ""}` : "";
  return `  ${letter.body}${source} receives=${letter.receiveCount} sent=${letter.sentAt.toISOString()}`;
}

async function main(): Promise<void> {
  const [consumer, verb = "peek"] = process.argv.slice(2) as [ConsumerName | undefined, Verb | undefined];
  if (!consumer || !(consumer in CONSUMERS) || !VERBS.includes(verb)) {
    console.error(`usage: npm run dlq -- <${Object.keys(CONSUMERS).join("|")}> [${VERBS.join("|")}]`);
    process.exit(1);
  }

  const sqs = new SQSClient();
  const name = dlqName(CONSUMERS[consumer]);
  switch (verb) {
    case "peek": {
      const letters = await peek(sqs, consumer);
      console.log(`${name}: ${letters.length} message(s)`);
      for (const letter of letters) console.log(describe(letter));
      break;
    }
    case "redrive": {
      const moved = await redrive(sqs, consumer);
      console.log(`${name}: moved ${moved} message(s) back to ${CONSUMERS[consumer].queueName}`);
      break;
    }
    case "drain": {
      const letters = await drain(sqs, consumer);
      for (const letter of letters) console.log(`discarded${describe(letter)}`);
      console.log(`${name}: drained ${letters.length} message(s)`);
      break;
    }
  }
  sqs.destroy();
}

if (import.meta.main) await main();
