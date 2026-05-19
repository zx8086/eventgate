// src/outbox/schemas.ts
import { z } from "zod";

export const outboxTopicSchema = z
  .enum(["raw", "events", "dlq"])
  .describe("Which configured Kafka topic family this row should publish to.");

export type OutboxTopic = z.infer<typeof outboxTopicSchema>;

export const outboxStatusSchema = z
  .enum(["pending", "dispatched", "failed"])
  .describe("Lifecycle state of an outbox row.");

export type OutboxStatus = z.infer<typeof outboxStatusSchema>;

export const outboxRowSchema = z.strictObject({
  id: z.string().min(1).describe("Row id (uuid v4)."),
  topic: outboxTopicSchema,
  message_key: z.string().describe("Kafka partition key. May be empty for dlq."),
  payload: z.string().describe("Already JSON-stringified Kafka message value."),
  headers: z
    .string()
    .nullable()
    .describe("JSON-stringified record-headers object, or null when none."),
  status: outboxStatusSchema,
  attempts: z.number().int().nonnegative(),
  next_attempt_at: z.number().int().describe("Epoch ms; eligible when <= now()."),
  created_at: z.number().int(),
  dispatched_at: z.number().int().nullable(),
  last_error: z.string().nullable(),
});

export type OutboxRow = z.infer<typeof outboxRowSchema>;
