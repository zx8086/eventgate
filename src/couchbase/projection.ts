import type {
  AutoOpsEventHistoryDoc,
  AutoOpsStateDoc,
  NormalizedEvent,
} from "../types.ts";

export function historyDocKey(event: NormalizedEvent): string {
  return `autoops::event::${event.resource.id}::${event.occurredAt}::${event.idempotencyKey}`;
}

export function stateDocKey(event: NormalizedEvent): string {
  return `autoops::state::${event.resource.id}::${event.alert.alertSignature}`;
}

export function toHistoryDoc(event: NormalizedEvent): AutoOpsEventHistoryDoc {
  return {
    docType: "autoopsEvent",
    resourceId: event.resource.id,
    resourceName: event.resource.name,
    eventType: event.eventType,
    severity: event.alert.severity,
    title: event.alert.title,
    occurredAt: event.occurredAt,
    receivedAt: event.receivedAt,
    idempotencyKey: event.idempotencyKey,
    alertSignature: event.alert.alertSignature,
    payload: event.alert,
    raw: event.raw,
  };
}

export function evolveState(
  previous: AutoOpsStateDoc | null,
  event: NormalizedEvent,
): AutoOpsStateDoc {
  const now = event.receivedAt;
  if (!previous) {
    return {
      docType: "autoopsState",
      resourceId: event.resource.id,
      resourceName: event.resource.name,
      alertSignature: event.alert.alertSignature,
      currentStatus: event.alert.status,
      currentSeverity: event.alert.severity,
      currentSeverityRank: event.alert.severityRank,
      firstSeenAt: event.occurredAt,
      lastSeenAt: event.occurredAt,
      lastEventId: event.idempotencyKey,
      openCount: event.alert.status === "opened" ? 1 : 0,
      closeCount: event.alert.status === "closed" ? 1 : 0,
      isOpen: event.alert.isOpen,
      updatedAt: now,
    };
  }
  return {
    ...previous,
    resourceName: event.resource.name,
    currentStatus: event.alert.status,
    currentSeverity: event.alert.severity,
    currentSeverityRank: event.alert.severityRank,
    lastSeenAt: event.occurredAt,
    lastEventId: event.idempotencyKey,
    openCount: previous.openCount + (event.alert.status === "opened" ? 1 : 0),
    closeCount: previous.closeCount + (event.alert.status === "closed" ? 1 : 0),
    isOpen: event.alert.isOpen,
    updatedAt: now,
  };
}
