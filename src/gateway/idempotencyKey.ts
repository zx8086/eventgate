// src/gateway/idempotencyKey.ts
import { createHash } from "node:crypto";

function pickString(b: Record<string, unknown>, ...keys: string[]): string | undefined {
  for (const k of keys) {
    const v = b[k];
    if (typeof v === "string" && v.length > 0) return v;
  }
  return undefined;
}

// Opportunistic content hash. The gateway is "accept everything" — this helper
// stamps a stable header on bodies that look like AutoOps events, so operators
// can spot repeated content via the Kafka header. Returns undefined for
// payloads we can't recognize; the message still ships with no header attached.
export function autoOpsIdempotencyKey(body: unknown): string | undefined {
  if (typeof body !== "object" || body === null || Array.isArray(body)) return undefined;
  const b = body as Record<string, unknown>;
  const resourceId = pickString(b, "resourceId", "deployment-id");
  const title = pickString(b, "title");
  const status = pickString(b, "status");
  const startTime = pickString(b, "startTime", "start-time");
  const endTime = pickString(b, "endTime", "end-time");
  if (!resourceId || !title || !status) return undefined;
  return createHash("sha256")
    .update([resourceId, title, status, startTime ?? "", endTime ?? ""].join("::"))
    .digest("hex");
}
