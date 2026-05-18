export type Severity = "low" | "medium" | "high" | "unknown";
export type SeverityRank = 0 | 1 | 2 | 3;
export type EventType = "opened" | "closed" | "unknown";

export type ElasticAutoOpsWebhook = {
  source?: string;
  resourceId: string;
  resourceName: string;
  title: string;
  description?: string;
  severity?: string;
  status: string;
  message?: string;
  startTime?: string;
  endTime?: string | null;
  endpointType?: string;
  affectedNodes?: string[] | string;
  affectedIndices?: string[] | string;
  eventLink?: string;
};

export type NormalizedAlert = {
  title: string;
  description?: string;
  severity: Severity;
  severityRank: SeverityRank;
  status: EventType;
  message?: string;
  link?: string;
  startTime?: string;
  endTime?: string | null;
  affectedNodes: string[];
  affectedIndices: string[];
  alertSignature: string;
  isOpen: boolean;
};

export type NormalizedEvent = {
  schemaVersion: 1;
  source: "elastic-autoops";
  eventCategory: "cluster-alert";
  eventType: EventType;
  receivedAt: string;
  occurredAt: string;
  idempotencyKey: string;
  routingKey: string;
  tenant: string;
  environment: string;
  endpointType?: string;
  resource: {
    id: string;
    name: string;
    kind: "elastic-deployment";
  };
  alert: NormalizedAlert;
  raw: unknown;
};

export type AutoOpsEventHistoryDoc = {
  docType: "autoopsEvent";
  resourceId: string;
  resourceName: string;
  eventType: EventType;
  severity: Severity;
  title: string;
  occurredAt: string;
  receivedAt: string;
  idempotencyKey: string;
  alertSignature: string;
  payload: NormalizedAlert;
  raw: unknown;
};

export type AutoOpsStateDoc = {
  docType: "autoopsState";
  resourceId: string;
  resourceName: string;
  alertSignature: string;
  currentStatus: EventType;
  currentSeverity: Severity;
  currentSeverityRank: SeverityRank;
  firstSeenAt: string;
  lastSeenAt: string;
  lastEventId: string;
  openCount: number;
  closeCount: number;
  isOpen: boolean;
  updatedAt: string;
};
