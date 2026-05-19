import { createHash } from "node:crypto";
import type {
  ElasticAutoOpsWebhook,
  EventType,
  NormalizedEvent,
  Severity,
  SeverityRank,
} from "./types.ts";

export function normalizeSeverity(value: string | undefined): Severity {
  const v = (value ?? "").toLowerCase();
  if (v === "low" || v === "medium" || v === "high") return v;
  return "unknown";
}

export function severityRank(value: Severity): SeverityRank {
  if (value === "low") return 1;
  if (value === "medium") return 2;
  if (value === "high") return 3;
  return 0;
}

// AutoOps docs spell STATUS as "open" or "close" but operators often template
// the body with "opened"/"closed", and AutoOps also emits "RESOLVED" for close
// events. Accept all spellings case-insensitively.
export function normalizeStatus(value: string | undefined): EventType {
  const v = (value ?? "").toLowerCase();
  if (v === "open" || v === "opened") return "opened";
  if (v === "close" || v === "closed" || v === "resolved") return "closed";
  return "unknown";
}

export function asArray(value: string[] | string | undefined): string[] {
  if (!value) return [];
  if (Array.isArray(value)) return value.map((s) => s.trim()).filter(Boolean);
  return value
    .split(",")
    .map((s) => s.trim())
    .filter(Boolean);
}

export function slugify(input: string): string {
  return input
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-+|-+$/g, "")
    .slice(0, 120);
}

export function sha256(input: string): string {
  return createHash("sha256").update(input).digest("hex");
}

export function buildAlertSignature(resourceId: string, title: string): string {
  return slugify([resourceId, title].filter(Boolean).join("::"));
}

export function buildIdempotencyKey(parts: {
  resourceId: string;
  title: string;
  status: EventType;
  startTime?: string;
  endTime?: string | null;
}): string {
  return sha256(
    [
      parts.resourceId,
      parts.title,
      parts.status,
      parts.startTime ?? "",
      parts.endTime ?? "",
    ].join("::"),
  );
}

export type NormalizeContext = {
  tenant: string;
  environment: string;
  now?: () => Date;
};

export function normalizeElasticAutoOps(
  raw: ElasticAutoOpsWebhook,
  ctx: NormalizeContext,
): NormalizedEvent {
  const receivedAt = (ctx.now?.() ?? new Date()).toISOString();
  const status = normalizeStatus(raw.status);
  const severity = normalizeSeverity(raw.severity);
  const occurredAt = raw.startTime ?? receivedAt;
  const alertSignature = buildAlertSignature(raw.resourceId, raw.title);
  const idempotencyKey = buildIdempotencyKey({
    resourceId: raw.resourceId,
    title: raw.title,
    status,
    startTime: raw.startTime,
    endTime: raw.endTime,
  });

  return {
    schemaVersion: 1,
    source: "elastic-autoops",
    eventCategory: "cluster-alert",
    eventType: status,
    receivedAt,
    occurredAt,
    idempotencyKey,
    routingKey: raw.resourceId,
    tenant: ctx.tenant,
    environment: ctx.environment,
    endpointType: raw.endpointType,
    resource: {
      id: raw.resourceId,
      name: raw.resourceName,
      kind: "elastic-deployment",
    },
    alert: {
      title: raw.title,
      description: raw.description,
      severity,
      severityRank: severityRank(severity),
      status,
      message: raw.message,
      link: raw.eventLink,
      startTime: raw.startTime,
      endTime: raw.endTime ?? null,
      affectedNodes: asArray(raw.affectedNodes),
      affectedIndices: asArray(raw.affectedIndices),
      alertSignature,
      isOpen: status === "opened",
    },
    raw,
  };
}
