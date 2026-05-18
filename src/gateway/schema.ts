// src/gateway/schema.ts
import { z } from "zod";

const csvOrArray = z
  .union([z.array(z.string()), z.string()])
  .optional()
  .describe("Comma-separated string or string[]; AutoOps templates emit either.");

// AutoOps' default body template uses hyphenated keys (deployment-id, start-time, ...).
// We also accept the camelCase form documented in our README. Map hyphenated → camelCase
// before validation so a single schema covers both. Un-substituted "${VAR}" placeholders
// from AutoOps' Validate button are treated as missing — strings starting with "${" become
// undefined here so the optional fields validate.
const HYPHEN_TO_CAMEL: Record<string, string> = {
  "deployment-id": "resourceId",
  "deployment-name": "resourceName",
  "start-time": "startTime",
  "end-time": "endTime",
  "endpoint-type": "endpointType",
  "affected-nodes": "affectedNodes",
  "affected-indices": "affectedIndices",
  "event-link": "eventLink",
};

function stripPlaceholder(v: unknown): unknown {
  if (typeof v === "string" && v.startsWith("${") && v.endsWith("}")) return undefined;
  return v;
}

export function normalizeAutoOpsBody(raw: unknown): unknown {
  if (raw === null || typeof raw !== "object" || Array.isArray(raw)) return raw;
  const out: Record<string, unknown> = {};
  for (const [k, v] of Object.entries(raw as Record<string, unknown>)) {
    const key = HYPHEN_TO_CAMEL[k] ?? k;
    const value = stripPlaceholder(v);
    if (value !== undefined) out[key] = value;
  }
  return out;
}

export const autoOpsWebhookSchema = z
  .object({
    source: z.string().optional().describe("Optional source tag set by upstream."),
    resourceId: z.string().min(1).describe("Elastic deployment / resource id (RESOURCE_ID)."),
    resourceName: z
      .string()
      .default("")
      .describe("Human-readable resource name (RESOURCE_NAME)."),
    title: z.string().min(1).describe("Alert title (TITLE); part of alertSignature."),
    description: z.string().optional().describe("Alert description (DESCRIPTION)."),
    severity: z.string().optional().describe("High | Medium | Low (SEVERITY)."),
    status: z
      .string()
      .default("unknown")
      .describe("open | close (STATUS); opened/closed also tolerated."),
    message: z.string().optional().describe("Free-form message (MESSAGE)."),
    startTime: z.string().optional().describe("ISO-8601 start time (START_TIME)."),
    endTime: z.string().nullable().optional().describe("ISO-8601 end time or null (END_TIME)."),
    endpointType: z.string().optional().describe("Endpoint type metadata (ENDPOINT_TYPE)."),
    affectedNodes: csvOrArray.describe("Affected nodes (AFFECTED_NODES)."),
    affectedIndices: csvOrArray.describe("Affected indices (AFFECTED_INDICES)."),
    eventLink: z.string().optional().describe("Deep-link to AutoOps event (EVENT_LINK)."),
  })
  .loose()
  .describe("Elastic AutoOps webhook body. Lenient on shape — strict on required identifiers.");

export type AutoOpsWebhookInput = z.infer<typeof autoOpsWebhookSchema>;
