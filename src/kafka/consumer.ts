// src/kafka/consumer.ts
import { Consumer, stringDeserializers } from "@platformatic/kafka";
import { config } from "../config/index.ts";

export function createConsumer(
  clientId: string,
  groupId: string,
): Consumer<string, string, string, string> {
  return new Consumer<string, string, string, string>({
    clientId,
    groupId,
    bootstrapBrokers: config.kafka.brokers,
    deserializers: stringDeserializers,
    autocommit: 5000,
  });
}
