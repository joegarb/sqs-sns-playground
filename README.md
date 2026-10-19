# sqs-sns-playground

A small, runnable demo of the AWS **SNS → SQS → Lambda** fan-out pattern, run locally on
[Floci](https://github.com/floci-io/floci) so it needs no AWS account. One event is published to an SNS
topic, and three Lambda consumers each get their own copy through their own SQS queue.

It covers fan-out, idempotent consumers, partial batch failures, dead-letter queues and redrive, and
subscription filter policies.

The example domain: a _design change_ is published once. **audit** records it, **checks** recomputes
downstream checks (and fails on a poison message), and **apollo** only cares about changes to the
`apollo` project.

## Design

```
                              ┌────────────────────────────┐
  npm run publish ──────────▶ │  SNS topic: design-changes │
                              └─────────────┬──────────────┘
                                            │ fan-out: every subscription gets its own copy
             ┌──────────────────────────────┼─────────────────────────────┐
             │                              │                             │ filter policy:
             ▼                              ▼                             ▼ project = apollo
     SQS: audit-log              SQS: downstream-checks          SQS: apollo-changes
       │      └─▶ DLQ               │      └─▶ DLQ (after 3)        │      └─▶ DLQ
       ▼                            ▼                               ▼
     Lambda: audit               Lambda: checks                  Lambda: apollo
     records each change once    fails on chg-BAD                only sees apollo changes
     (DynamoDB conditional put)
```

## How it works

- Everything is in one CDK stack ([`lib/playground-stack.ts`](lib/playground-stack.ts)): the topic, an
  SQS queue and dead-letter queue (DLQ) per consumer, the subscriptions, the Lambda functions and their
  SQS event source mappings, and the audit table. The same stack deploys to Floci with `cdklocal` or to
  a real account with `cdk deploy`.
- The subscriptions use raw message delivery, so each SQS message body is the published event itself
  rather than SNS's JSON envelope. Without it, the body would look like this, with the event as an
  escaped string inside `Message`:
  ```json
  {
    "Type": "Notification",
    "MessageId": "…",
    "TopicArn": "arn:aws:sns:us-east-1:000000000000:design-changes",
    "Message": "{\"changeId\":\"chg-1001\",\"project\":\"apollo\",\"revision\":1}",
    "MessageAttributes": { "project": { "Type": "String", "Value": "apollo" } },
    "Signature": "…"
  }
  ```
- Lambda receives up to 10 messages per batch and reports failures per message
  (`ReportBatchItemFailures`, using [Powertools' batch utility](https://docs.powertools.aws.dev/lambda/typescript/latest/features/batch/)).
  Only the failed message goes back to the queue, not the whole batch.
- A failed message becomes visible again after the queue's visibility timeout (10s) and is retried.
  Once it has been received more than `maxReceiveCount` times (3 for checks, 10 for the others), SQS
  moves it to the DLQ.
- The topic and queues are Standard, not FIFO. Delivery is at-least-once, so a consumer can see the
  same change twice. audit handles that with a DynamoDB conditional write keyed on `changeId`: the
  first write wins and later ones are logged as duplicates. FIFO would add ordering and short-window
  deduplication, at the cost of throughput limits and message group IDs, and neither is needed here.
- The apollo subscription has the filter policy `{"project": ["apollo"]}`. It matches against message
  attributes, not the body, so the publisher sets `project` in both. SNS drops non-matching messages
  before they reach the queue, so apollo's Lambda is never invoked for them.

## Requirements

- Docker
- Node 24+

No AWS account or credentials. `.env.example` points the AWS SDK at Floci with dummy credentials.

## Run it

```bash
npm install
cp .env.example .env
docker compose up -d     # Floci on localhost:4566
npm run deploy           # bootstrap CDK and deploy the stack to Floci
```

Then publish and watch each consumer's CloudWatch Logs:

```bash
npm run publish          # 5 design changes, one of them (chg-BAD) poison
npm run logs -- audit    # add --follow (or -f) to keep watching
npm run logs -- checks
npm run logs -- apollo
```

The first batch each Lambda handles takes a few seconds while Floci pulls the Node.js Lambda image.

### What to observe

- **Fan-out.** One publish, and all three consumers log it, each from its own queue.
- **Filter policy.** apollo only logs `chg-1001` and `chg-1004`, the two `apollo` changes.
- **Partial batch failure and dead-lettering.** checks handles four changes and fails `chg-BAD`.
  `receiveCount` climbs about every 10 seconds, then the message moves to `downstream-checks-dlq`.
  audit and apollo aren't affected:
  ```
  21:20:57 [checks] checks recomputed changeId=chg-1002 receiveCount=1 project=mercury revision=4
  21:20:57 [checks] failed changeId=chg-BAD receiveCount=1 error=cannot compute checks for chg-BAD
  21:20:57 [checks] checks recomputed changeId=chg-1003 receiveCount=1 project=gemini revision=2
  21:21:07 [checks] failed changeId=chg-BAD receiveCount=2 error=cannot compute checks for chg-BAD
  21:21:18 [checks] failed changeId=chg-BAD receiveCount=3 error=cannot compute checks for chg-BAD
  ```
- **Idempotency.** Run `npm run publish` again. audit logs `skipped duplicate` for all five changes,
  and its table still has one item per change.

## Dead-letter queues

```bash
npm run dlq -- checks            # peek: list what's in the DLQ (the default)
npm run dlq -- checks redrive    # move it all back to downstream-checks
npm run dlq -- checks drain      # log each message, then delete it
```

```
downstream-checks-dlq: 1 message(s)
  {"changeId":"chg-BAD","project":"mercury","revision":5} receives=5 sent=2026-10-19T21:20:56.388Z
```

- SQS has no real peek. `peek` receives the messages and immediately makes them visible again, which
  bumps their receive count. That's harmless on a DLQ, which has no redrive policy of its own.
- `redrive` uses SQS's message move task (`StartMessageMoveTask`), the same thing as the console's
  "Start DLQ redrive". Each message goes back to the queue it was dead-lettered from. `chg-BAD` is
  still poison, so it fails three more times and lands back in the DLQ.
- SQS doesn't record why a message was dead-lettered. The reason is in the consumer's logs.

## Tests

```bash
npm test                    # unit tests for the handlers; no Floci needed
npm run test:integration    # end to end against the deployed stack (about 70s)
```

The integration test publishes the batch and checks what each consumer did, the dead-lettering, and
all three DLQ commands. It only runs when `AWS_ENDPOINT_URL` is set and skips if the stack isn't
deployed, so it never runs against a real account. CI runs both, with the integration test against
Floci.

## Notes

- Floci keeps everything in memory. `docker compose down` resets it, then `npm run deploy` again.
- Some in-place stack updates don't take effect on Floci: changing a subscription's filter policy or a
  queue's `maxReceiveCount` on an existing deployment did nothing in testing. After changing the
  stack, restart Floci and deploy again.
- Floci runs each Lambda in its own container through the mounted Docker socket, and stops them when it
  stops. The ECR registry container it starts for the CDK bootstrap (`floci-aws-ecr-registry`) is left
  running; remove it with `docker rm -f floci-aws-ecr-registry`.
- The visibility timeout is kept short so the poison message dead-letters within about 30 seconds. AWS
  recommends at least six times the function timeout.
- To deploy to a real account, use `npx cdk deploy` with normal AWS credentials and without the
  `.env` values. Resource names are fixed, so there can be one copy per account and region.

## References

- [Fanout to Amazon SQS queues](https://docs.aws.amazon.com/sns/latest/dg/sns-sqs-as-subscriber.html)
- [SNS raw message delivery](https://docs.aws.amazon.com/sns/latest/dg/sns-large-payload-raw-message-delivery.html)
- [SNS message filtering](https://docs.aws.amazon.com/sns/latest/dg/sns-message-filtering.html)
- [Using Lambda with Amazon SQS](https://docs.aws.amazon.com/lambda/latest/dg/with-sqs.html) and
  [handling errors for an SQS event source](https://docs.aws.amazon.com/lambda/latest/dg/services-sqs-errorhandling.html)
- [SQS dead-letter queues](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-dead-letter-queues.html)
  and [DLQ redrive](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-configure-dead-letter-queue-redrive.html)
- [SNS FIFO topics](https://docs.aws.amazon.com/sns/latest/dg/sns-fifo-topics.html), for what this
  demo leaves out

## License

MIT
