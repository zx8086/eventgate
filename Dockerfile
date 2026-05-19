# syntax=docker/dockerfile:1
# Pattern: 6-stage. Alpine deps -> distroless production (Tier 2) with an
# Alpine 'release' fallback (Tier 1). Swap DHI_IMAGE to dhi.io/static:<tag>
# at build-arg level to upgrade to real Tier 3 once a DHI subscription is
# in place; no Dockerfile change required.
#
# Bun runs .ts natively, so there is no `bun run build` step — the builder
# stage stages the source tree and prunes non-runtime artefacts.
# Single image, two entrypoints: default CMD is the gateway; the writer is
# launched via task-def command override (CMD ["bun", "src/writer/index.ts"]).

ARG BUN_VERSION=1.3.11
ARG DHI_IMAGE=gcr.io/distroless/static-debian12:nonroot

# -----------------------------------------------------------------------------
# Stage 1: deps-base — Alpine with Bun, dumb-init, ca-certificates. Source of
# truth for the musl libs copied into the distroless production stage.
# -----------------------------------------------------------------------------
FROM oven/bun:${BUN_VERSION}-alpine AS deps-base
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
# Stage 3: builder — stage source, prune non-runtime files. No compile step.
# -----------------------------------------------------------------------------
FROM deps-base AS builder
COPY package.json bun.lock* tsconfig.json ./
COPY src ./src
RUN rm -rf .git .github node_modules/.cache test/ tests/ \
           *.test.* *.spec.* *.md docs/ coverage/ .vscode .idea *.log

# -----------------------------------------------------------------------------
# Stage 4: production (Tier 2) — distroless runtime. No shell, no package
# manager. Bun is musl-linked so we copy the dynamic linker + libgcc + libstdc++
# from Alpine; distroless uses UID 65532 by default.
# -----------------------------------------------------------------------------
FROM ${DHI_IMAGE} AS production
WORKDIR /app

COPY --from=deps-base /usr/local/bin/bun /usr/local/bin/bun
COPY --from=deps-base /usr/bin/dumb-init /usr/bin/dumb-init
COPY --from=deps-base /lib/ld-musl-*.so.1 /lib/
COPY --from=deps-base /usr/lib/libgcc_s.so.1 /usr/lib/
COPY --from=deps-base /usr/lib/libstdc++.so.6 /usr/lib/
COPY --from=deps-base /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/

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
ARG DHI_IMAGE
LABEL org.opencontainers.image.title="${SERVICE_NAME}" \
      org.opencontainers.image.description="${SERVICE_DESCRIPTION}" \
      org.opencontainers.image.vendor="${SERVICE_AUTHOR}" \
      org.opencontainers.image.version="${SERVICE_VERSION}" \
      org.opencontainers.image.created="${BUILD_DATE}" \
      org.opencontainers.image.revision="${VCS_REF}" \
      org.opencontainers.image.licenses="${SERVICE_LICENSE}" \
      org.opencontainers.image.base.name="${DHI_IMAGE}"

# -----------------------------------------------------------------------------
# Stage 5: release (Tier 1 fallback) — Alpine runtime. Same behaviour, larger
# attack surface (shell + apk present). Built when the distroless stage is
# blocked (e.g. Bun upgrade breaks musl-libs copy, DHI subscription lapses).
# Tagged separately so the public-facing path always defaults to Tier 2.
# -----------------------------------------------------------------------------
FROM oven/bun:${BUN_VERSION}-alpine AS release
ARG BUN_VERSION
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
      org.opencontainers.image.base.name="oven/bun:${BUN_VERSION}-alpine"
