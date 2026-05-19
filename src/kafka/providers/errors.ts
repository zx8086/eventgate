// src/kafka/providers/errors.ts
export type ProviderErrorCode =
  | "PROVIDER_NOT_FOUND"
  | "PROVIDER_CONFIG_INVALID"
  | "PROVIDER_CONNECTION_FAILED"
  | "PROVIDER_AUTH_FAILED"
  | "PROVIDER_TIMEOUT";

export class KafkaProviderError extends Error {
  public readonly code: ProviderErrorCode;
  public readonly provider: string;
  public override readonly cause?: unknown;

  constructor(message: string, code: ProviderErrorCode, provider: string, cause?: unknown) {
    super(message);
    this.name = "KafkaProviderError";
    this.code = code;
    this.provider = provider;
    this.cause = cause;
  }
}
