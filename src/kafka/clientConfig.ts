// src/kafka/clientConfig.ts
import { generateAuthToken } from "aws-msk-iam-sasl-signer-js";
import { type KafkaConfig, logLevel } from "kafkajs";
import { config } from "../config/index.ts";

export function buildKafkaConfig(clientId: string): KafkaConfig {
  const base: KafkaConfig = {
    clientId,
    brokers: config.kafka.brokers,
    logLevel: logLevel.INFO,
  };

  if (config.kafka.auth === "iam") {
    const region = config.kafka.region;
    if (!region) throw new Error("kafka.region is required when auth=iam");
    return {
      ...base,
      ssl: true,
      sasl: {
        mechanism: "oauthbearer",
        oauthBearerProvider: async () => {
          const { token } = await generateAuthToken({ region });
          return { value: token };
        },
      },
    };
  }

  return base;
}
