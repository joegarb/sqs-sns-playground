// `changeId` is the stable identity of a change, and the key consumers dedupe on.
export interface DesignChangeEvent {
  changeId: string;
  project: string;
  revision: number;
}

// Subscriptions use raw message delivery, so the body is the bare event. An SNS
// envelope (raw delivery switched off) fails loudly here instead of being misread.
export function parseDesignChange(body: string): DesignChangeEvent {
  const raw: unknown = JSON.parse(body);
  if (typeof raw !== "object" || raw === null) {
    throw new Error(`Not a design change: ${body}`);
  }

  const { changeId, project, revision } = raw as Record<string, unknown>;
  if (typeof changeId !== "string" || typeof project !== "string" || typeof revision !== "number") {
    throw new Error(`Not a design change: ${body}`);
  }

  return { changeId, project, revision };
}

export const POISON_CHANGE_ID = "chg-BAD";

export const DESIGN_CHANGE_BATCH: readonly DesignChangeEvent[] = [
  { changeId: "chg-1001", project: "apollo", revision: 1 },
  { changeId: "chg-1002", project: "mercury", revision: 4 },
  { changeId: POISON_CHANGE_ID, project: "mercury", revision: 5 },
  { changeId: "chg-1003", project: "gemini", revision: 2 },
  { changeId: "chg-1004", project: "apollo", revision: 2 },
];
