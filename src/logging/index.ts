// src/logging/index.ts
import pino, { type Logger as PinoLogger } from "pino";
import ecsFormat from "@elastic/ecs-pino-format";
import { config } from "../config/index.ts";

export type LogBindings = Record<string, unknown>;

export type ILogger = {
  trace(obj: LogBindings | string, msg?: string): void;
  debug(obj: LogBindings | string, msg?: string): void;
  info(obj: LogBindings | string, msg?: string): void;
  warn(obj: LogBindings | string, msg?: string): void;
  error(obj: LogBindings | string, msg?: string): void;
  fatal(obj: LogBindings | string, msg?: string): void;
  child(bindings: LogBindings): ILogger;
  flush(): void;
};

let rootLogger: PinoLogger | undefined;

function buildRoot(): PinoLogger {
  // Synchronous destination — pino.transport() uses worker threads
  // which are incompatible with Bun. Per bun-logging-guide.md.
  const destination = { write: (s: string) => process.stdout.write(s) };

  return pino(
    {
      ...ecsFormat({ apmIntegration: false, convertErr: true }),
      level: config.observability.logLevel,
      base: {
        "service.name": config.app.name,
        "service.version": config.app.version,
        "service.environment": config.app.environment,
      },
    },
    destination,
  );
}

function getRoot(): PinoLogger {
  if (!rootLogger) rootLogger = buildRoot();
  return rootLogger;
}

function wrap(p: PinoLogger): ILogger {
  return {
    trace: (obj, msg) => p.trace(obj as object, msg as string),
    debug: (obj, msg) => p.debug(obj as object, msg as string),
    info: (obj, msg) => p.info(obj as object, msg as string),
    warn: (obj, msg) => p.warn(obj as object, msg as string),
    error: (obj, msg) => p.error(obj as object, msg as string),
    fatal: (obj, msg) => p.fatal(obj as object, msg as string),
    child: (bindings) => wrap(p.child(bindings)),
    flush: () => p.flush(),
  };
}

export function getLogger(component: string, bindings: LogBindings = {}): ILogger {
  return wrap(getRoot().child({ component, ...bindings }));
}

export function resetLoggerForTests(): void {
  rootLogger = undefined;
}
