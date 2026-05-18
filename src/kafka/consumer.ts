import { Kafka, type Consumer } from "kafkajs";
import { buildKafkaConfig } from "./clientConfig.ts";

export async function createConsumer(clientId: string, groupId: string): Promise<Consumer> {
  const kafka = new Kafka(buildKafkaConfig(clientId));
  const consumer = kafka.consumer({ groupId });
  await consumer.connect();
  return consumer;
}
