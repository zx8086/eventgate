// src/config/topicPolicy.ts
// Org-wide topic naming policy enforced for gateway-owned topics.
// See: docs/superpowers/specs/2026-05-19-config-driven-routes-design.md

const KAFKA_MAX_TOPIC_LENGTH = 249;

const GATEWAY_TOPIC_REGEX = /^T_PRIVATE_SOURCE_[A-Z][A-Z0-9]*_[A-Z][A-Z0-9]*(_[A-Z][A-Z0-9]*)*$/;

type Check = { ok: true } | { ok: false; message: string };

export function isGatewayTopic(topic: string): boolean {
  return checkGatewayTopic(topic).ok;
}

export function checkGatewayTopic(topic: string): Check {
  if (topic.length === 0) {
    return { ok: false, message: "topic must be non-empty" };
  }
  if (topic.length > KAFKA_MAX_TOPIC_LENGTH) {
    return {
      ok: false,
      message: `topic length ${topic.length} exceeds Kafka limit of ${KAFKA_MAX_TOPIC_LENGTH}`,
    };
  }
  if (topic.startsWith("T_PUBLIC_")) {
    return {
      ok: false,
      message: "gateway is not an MDM publisher; topic must start with T_PRIVATE_SOURCE_",
    };
  }
  if (topic.startsWith("T_PRIVATE_SINK_")) {
    return {
      ok: false,
      message: "gateway is not a sink connector; topic must start with T_PRIVATE_SOURCE_",
    };
  }
  if (topic.startsWith("DLQ_T_")) {
    return {
      ok: false,
      message: "DLQ topics are declared via dlqTopic, not topic",
    };
  }
  if (topic.startsWith("__") || topic === "_schemas" || topic.startsWith("_confluent-")) {
    return { ok: false, message: "system topic prefix; not gateway-writable" };
  }
  if (
    topic.startsWith("T_PRIVATE_") &&
    (topic.endsWith("_RICH_NOTIFICATIONS") || topic.endsWith("_EVENTS"))
  ) {
    return {
      ok: false,
      message:
        "internal event/notification streams are not gateway-owned; topic must start with T_PRIVATE_SOURCE_",
    };
  }
  if (!GATEWAY_TOPIC_REGEX.test(topic)) {
    return {
      ok: false,
      message: "topic must match T_PRIVATE_SOURCE_<SYSTEM>_<ENTITY> (uppercase, underscores)",
    };
  }
  return { ok: true };
}

export function expectedDlqTopic(topic: string): string {
  return `DLQ_${topic}`;
}
