// src/gateway/schema.ts
import { z } from "zod";

const csvOrArray = z
  .union([z.array(z.string()), z.string()])
  .optional()
  .describe("Comma-separated string or string[]; AutoOps templates emit either.");

export const autoOpsWebhookSchema = z
  .strictObject({
    source: z.string().optional().describe("Optional source tag set by upstream."),
    resourceId: z.string().min(1).describe("Elastic deployment / resource id (RESOURCE_ID)."),
    resourceName: z.string().min(1).describe("Human-readable resource name (RESOURCE_NAME)."),
    title: z.string().min(1).describe("Alert title (TITLE); part of alertSignature."),
    description: z.string().optional().describe("Alert description (DESCRIPTION)."),
    severity: z.string().optional().describe("High | Medium | Low (SEVERITY)."),
    status: z.string().min(1).describe("open | close (STATUS); opened/closed also tolerated."),
    message: z.string().optional().describe("Free-form message (MESSAGE)."),
    startTime: z.string().optional().describe("ISO-8601 start time (START_TIME)."),
    endTime: z.string().nullable().optional().describe("ISO-8601 end time or null (END_TIME)."),
    endpointType: z.string().optional().describe("Endpoint type metadata (ENDPOINT_TYPE)."),
    affectedNodes: csvOrArray.describe("Affected nodes (AFFECTED_NODES)."),
    affectedIndices: csvOrArray.describe("Affected indices (AFFECTED_INDICES)."),
    eventLink: z.string().optional().describe("Deep-link to AutoOps event (EVENT_LINK)."),
  })
  .describe("Elastic AutoOps webhook body. Lenient on shape — strict on required identifiers.");

export type AutoOpsWebhookInput = z.infer<typeof autoOpsWebhookSchema>;
