import { CloudWatchLogsClient, paginateFilterLogEvents } from "@aws-sdk/client-cloudwatch-logs";

import { CONSUMERS, type ConsumerName, logGroupName } from "../topology.ts";

// Reads a consumer's CloudWatch Logs, keeping Powertools' JSON lines and skipping
// the Lambda platform's START/END/REPORT lines.
//   npm run logs -- <consumer> [--follow]

export interface LogEntry {
  eventId: string;
  timestamp: number;
  fields: Record<string, unknown>;
}

export async function readLogs(
  logs: CloudWatchLogsClient,
  consumer: ConsumerName,
  startTime: number,
): Promise<LogEntry[]> {
  const entries: LogEntry[] = [];
  const pages = paginateFilterLogEvents(
    { client: logs },
    { logGroupName: logGroupName(CONSUMERS[consumer]), startTime },
  );
  for await (const page of pages) {
    for (const event of page.events ?? []) {
      try {
        const fields = JSON.parse(event.message ?? "") as Record<string, unknown>;
        entries.push({ eventId: event.eventId ?? "", timestamp: event.timestamp ?? 0, fields });
      } catch {
        // not a Powertools line
      }
    }
  }
  return entries;
}

const HIDDEN_KEYS = new Set([
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

function text(value: unknown): string {
  if (typeof value === "string") return value;
  // Powertools logs errors as { name, message, stack }; the message is enough here.
  if (typeof value === "object" && value !== null && "message" in value) return text(value.message);
  return JSON.stringify(value);
}

function format({ fields }: LogEntry): string {
  const extra = Object.entries(fields)
    .filter(([key]) => !HIDDEN_KEYS.has(key))
    .map(([key, value]) => `${key}=${text(value)}`);
  return `${text(fields.timestamp).slice(11, 19)} [${text(fields.service)}] ${text(fields.message)} ${extra.join(" ")}`;
}

async function main(): Promise<void> {
  const consumer = process.argv[2] as ConsumerName | undefined;
  if (!consumer || !(consumer in CONSUMERS)) {
    console.error(`usage: npm run logs -- <${Object.keys(CONSUMERS).join("|")}> [--follow]`);
    process.exit(1);
  }
  const follow = process.argv.includes("--follow") || process.argv.includes("-f");

  const logs = new CloudWatchLogsClient();
  let startTime = Date.now() - 15 * 60_000;
  const seen = new Set<string>();
  for (;;) {
    for (const entry of await readLogs(logs, consumer, startTime)) {
      if (seen.has(entry.eventId)) continue;
      seen.add(entry.eventId);
      startTime = Math.max(startTime, entry.timestamp);
      console.log(format(entry));
    }
    if (!follow) break;
    await new Promise((resolve) => setTimeout(resolve, 1000));
  }
  logs.destroy();
}

if (import.meta.main) await main();
