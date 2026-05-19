# syntax=docker/dockerfile:1
# Pattern: 4-stage Alpine -> Alpine, BuildKit cache mounts, non-root 65532.
# Bun runs .ts natively, so there is no `bun run build` step — the builder
# stage just stages the source tree and prunes non-runtime artefacts.
# Single image, two entrypoints: default CMD is the gateway; the writer is
# launched via task-def command override (CMD ["bun", "src/writer/index.ts"]).

# -----------------------------------------------------------------------------
# Stage 1: deps-base — system deps shared by every later stage.
# -----------------------------------------------------------------------------
FROM oven/bun:1.3.11-alpine AS deps-base
WORKDIR /app
RUN --mount=type=cache,target=/var/cache/apk,sharing=locked \
    --mount=type=cache,target=/var/lib/apk,sharing=locked \
    apk update && apk upgrade && \
    apk add --no-cache ca-certificates dumb-init

# -----------------------------------------------------------------------------
# Stage 2: deps-prod — production node_modules only.
# -----------------------------------------------------------------------------
FROM deps-base AS deps-prod
COPY package.json bun.lock* ./
RUN --mount=type=cache,target=/root/.bun/install/cache,sharing=locked \
    bun install --frozen-lockfile --production

# -----------------------------------------------------------------------------
# Stage 3: builder — stage source, prune non-runtime files.
# Bun executes .ts directly so there is no compile/bundle step.
# -----------------------------------------------------------------------------
FROM deps-base AS builder
COPY package.json bun.lock* tsconfig.json ./
COPY src ./src
RUN rm -rf .git .github node_modules/.cache test/ tests/ \
           *.test.* *.spec.* *.md docs/ coverage/ .vscode .idea *.log

# -----------------------------------------------------------------------------
# Stage 4: production — minimal runtime, non-root, healthchecked.
# -----------------------------------------------------------------------------
FROM oven/bun:1.3.11-alpine AS production
WORKDIR /app

RUN apk add --no-cache dumb-init ca-certificates && \
    addgroup -g 65532 -S nonroot && \
    adduser  -u 65532 -S -G nonroot -h /app nonroot

COPY --from=deps-prod --chown=65532:65532 /app/node_modules ./node_modules
COPY --from=deps-prod --chown=65532:65532 /app/package.json ./package.json
COPY --from=builder  --chown=65532:65532 /app/src           ./src
COPY --from=builder  --chown=65532:65532 /app/tsconfig.json ./tsconfig.json

USER 65532:65532

ENV NODE_ENV=production \
    PORT=3000 \
    HOST=0.0.0.0

EXPOSE 3000

# Healthcheck targets the gateway's /healthz. Writer task defs disable this
# (HEALTHCHECK NONE in the task definition) since the writer has no HTTP server.
HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
  CMD ["/usr/local/bin/bun", "--eval", \
       "fetch('http://localhost:3000/healthz').then(r=>r.ok?process.exit(0):process.exit(1)).catch(()=>process.exit(1))"]

ENTRYPOINT ["/usr/bin/dumb-init", "--"]
CMD ["/usr/local/bin/bun", "src/gateway/index.ts"]

ARG BUILD_DATE
ARG VCS_REF
ARG SERVICE_NAME
ARG SERVICE_VERSION
ARG SERVICE_DESCRIPTION
ARG SERVICE_AUTHOR
ARG SERVICE_LICENSE
LABEL org.opencontainers.image.title="${SERVICE_NAME}" \
      org.opencontainers.image.description="${SERVICE_DESCRIPTION}" \
      org.opencontainers.image.vendor="${SERVICE_AUTHOR}" \
      org.opencontainers.image.version="${SERVICE_VERSION}" \
      org.opencontainers.image.created="${BUILD_DATE}" \
      org.opencontainers.image.revision="${VCS_REF}" \
      org.opencontainers.image.licenses="${SERVICE_LICENSE}" \
      org.opencontainers.image.base.name="oven/bun:1.3.11-alpine"
