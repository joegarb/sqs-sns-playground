import { CloudWatchLogsClient, FilterLogEventsCommand } from "@aws-sdk/client-cloudwatch-logs";

import { CONSUMERS, type ConsumerName, logGroupName } from "../topology.ts";

// Tails a consumer's CloudWatch Logs, formatting Powertools' JSON lines and
// skipping the Lambda platform's START/END/REPORT lines.
//   npm run logs -- <consumer> [--follow]

const IGNORED_KEYS = new Set([
  "level",
  "message",
  "timestamp",
  "service",
  "sampling_rate",
  "xray_trace_id",
  "cold_start",
  "function_arn",
  "function_memory_size",
  "function_name",
  "function_request_id",
]);

function format(line: string): string | undefined {
  let entry: Record<string, unknown>;
  try {
    entry = JSON.parse(line) as Record<string, unknown>;
  } catch {
    return undefined;
  }
  const text = (value: unknown): string => {
    if (typeof value === "string") return value;
    // Powertools logs errors as { name, message, stack }; the message is enough here.
    if (typeof value === "object" && value !== null && "message" in value) return text(value.message);
    return JSON.stringify(value);
  };
  const fields = Object.entries(entry)
    .filter(([key]) => !IGNORED_KEYS.has(key))
    .map(([key, value]) => `${key}=${text(value)}`);
  return `${text(entry.timestamp).slice(11, 19)} [${text(entry.service)}] ${text(entry.message)} ${fields.join(" ")}`;
}

async function main(): Promise<void> {
  const name = process.argv[2] as ConsumerName | undefined;
  if (!name || !(name in CONSUMERS)) {
    console.error(`usage: npm run logs -- <${Object.keys(CONSUMERS).join("|")}> [--follow]`);
    process.exit(1);
  }
  const follow = process.argv.includes("--follow") || process.argv.includes("-f");

  const logs = new CloudWatchLogsClient();
  let startTime = Date.now() - 15 * 60_000;
  const seen = new Set<string>();

  for (;;) {
    const { events = [] } = await logs.send(
      new FilterLogEventsCommand({ logGroupName: logGroupName(CONSUMERS[name]), startTime }),
    );
    for (const event of events) {
      if (!event.eventId || seen.has(event.eventId)) continue;
      seen.add(event.eventId);
      startTime = Math.max(startTime, event.timestamp ?? 0);
      const line = format(event.message?.trim() ?? "");
      if (line) console.log(line);
    }
    if (!follow) break;
    await new Promise((resolve) => setTimeout(resolve, 1000));
  }
  logs.destroy();
}

await main();
