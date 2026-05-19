# Container Image

> **Targets:** Bun 1.3.11+ | TypeScript 5.x | Docker
> **Last updated:** 2026-05-19
> **Conventions:** See [../../guides/documentation-guide.md](../../guides/documentation-guide.md)

eventgate ships as a single multi-platform Docker image with two runtime stages: a distroless **Tier 2** production stage (the default) and an Alpine **Tier 1** release fallback. Bun runs `.ts` directly, so there is no `bun run build` step — the builder stage stages the source tree and the runtime stages copy it as-is. This document describes the stages, the build arguments, and the per-process healthcheck semantics.

## Tiered Hardening Model

| Tier | Stage | Base image | Why use it |
|------|-------|------------|------------|
| Tier 1 | `release` | `oven/bun:${BUN_VERSION}-alpine` | Fallback when the distroless stage is blocked (a Bun upgrade breaks the musl-libs copy, a DHI subscription lapses, an emergency debug requires a shell). Larger attack surface — apk and ash are present. |
| Tier 2 | `production` | `${DHI_IMAGE}` (default `gcr.io/distroless/static-debian12:nonroot`) | The default build target. No shell, no package manager, UID 65532. Swap `DHI_IMAGE` to `dhi.io/static:<tag>` at build-arg level to upgrade to a real Tier 3 base once a DHI subscription is provisioned — no Dockerfile change required. |

The CD workflow (`.github/workflows/cd.yml`) builds `target: production` for `linux/amd64` and `linux/arm64` and pushes to `docker.io/zx8086/eventgate`. The `release` stage is built on demand by overriding `--target release`.

## Stages

The Dockerfile has six stages. Stages 1-3 are shared deps and source staging; stages 4-5 are the two runtime tiers.

| Stage | Purpose |
|-------|---------|
| `deps-base` | Alpine with Bun, `dumb-init`, `ca-certificates`. Source of truth for the musl libs copied into the distroless production stage. |
| `deps-prod` | Production `node_modules` only — `bun install --frozen-lockfile --production`. |
| `builder` | Stages `src/`, `package.json`, `bun.lock`, `tsconfig.json` and prunes test, doc, and tooling files. No compile. |
| `production` (Tier 2) | Distroless runtime. Copies Bun, `dumb-init`, the musl dynamic linker, `libgcc_s`, `libstdc++`, CA bundle, then `node_modules` + `src/` + `tsconfig.json`. Runs as UID 65532. |
| `release` (Tier 1) | Alpine runtime. Creates UID 65532, installs `dumb-init` + `ca-certificates`, copies the same payload. |

## Entrypoint and CMD

Both runtime stages use the same entrypoint and default command:

```dockerfile
ENTRYPOINT ["/usr/bin/dumb-init", "--"]
CMD ["/usr/local/bin/bun", "src/gateway/index.ts"]
```

- `dumb-init` reaps zombies and forwards signals — required when the only process in the container is a single Bun runtime.
- The default `CMD` launches the **gateway**. The writer is launched by overriding `command` at the ECS task-definition level (see [aws-ecs.md](aws-ecs.md)). The Dockerfile keeps the default explicit so `docker run` without flags still produces a usable HTTP server.

## Build Arguments

| Argument | Default | Purpose |
|----------|---------|---------|
| `BUN_VERSION` | `1.3.11` | Bun version to install in the Alpine stages |
| `DHI_IMAGE` | `gcr.io/distroless/static-debian12:nonroot` | Production base image. Swap to `dhi.io/static:<tag>` for Docker Hardened Images. |
| `BUILD_DATE` | (set by CD) | OCI `image.created` label, sourced from `github.event.repository.updated_at` |
| `VCS_REF` | (set by CD) | OCI `image.revision` label, sourced from `github.sha` |
| `SERVICE_NAME` | `eventgate` | OCI `image.title` |
| `SERVICE_VERSION` | (set by CD from ref) | OCI `image.version` |
| `SERVICE_DESCRIPTION` | (set by CD) | OCI `image.description` |
| `SERVICE_AUTHOR` | `Siobytes` | OCI `image.vendor` |
| `SERVICE_LICENSE` | `UNLICENSED` | OCI `image.licenses` |

The CD workflow passes every argument explicitly. Local builds inherit the defaults.

## Healthcheck Semantics

The Dockerfile declares a `HEALTHCHECK` that probes `http://localhost:3000/healthz`:

```dockerfile
HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
  CMD ["/usr/local/bin/bun", "--eval", \
       "fetch('http://localhost:3000/healthz').then(r=>r.ok?process.exit(0):process.exit(1)).catch(()=>process.exit(1))"]
```

| Process | Healthcheck behaviour |
|---------|----------------------|
| gateway | Honoured — `/healthz` returns `200` once the Kafka producer is connected, `503` otherwise |
| writer | Disabled at the task-definition layer by setting `HEALTHCHECK NONE` (the writer has no HTTP server) |

Disabling the writer's healthcheck inside the task definition is correct — the alternative (every writer task perpetually unhealthy) would prevent ECS from ever marking the service stable.

## Image Contents

The production image contains only:

- `/usr/local/bin/bun` and `/usr/bin/dumb-init`
- Musl runtime libraries (`ld-musl-*.so.1`, `libgcc_s.so.1`, `libstdc++.so.6`) — Bun is musl-linked, distroless is glibc, so these are explicitly copied
- `/etc/ssl/certs/ca-certificates.crt`
- `/app/node_modules` (production dependencies only)
- `/app/src/` (TypeScript sources — Bun runs them directly)
- `/app/package.json`, `/app/tsconfig.json`
- No shell, no package manager, no test files, no `.git`, no docs

The release image contains the same payload plus `ash`, `apk`, and the standard Alpine baseline.

## Build Commands

Default production build (Tier 2):

```bash
docker build -t eventgate:local .
```

Tier 1 fallback build:

```bash
docker build --target release -t eventgate:local-tier1 .
```

Multi-platform build matching CD:

```bash
docker buildx build \
  --platform linux/amd64,linux/arm64 \
  --target production \
  -t docker.io/zx8086/eventgate:local \
  --build-arg SERVICE_VERSION=local \
  --push .
```

## Image Verification

After a build, verify the image runs as a non-root user with the expected entrypoint:

```bash
docker inspect eventgate:local --format '{{.Config.User}} {{json .Config.Entrypoint}} {{json .Config.Cmd}}'
```

Expected: `65532:65532 ["/usr/bin/dumb-init","--"] ["/usr/local/bin/bun","src/gateway/index.ts"]`.

Smoke-test the gateway directly:

```bash
docker run --rm -p 3000:3000 -e KAFKA_BROKERS=invalid:9092 eventgate:local &
curl -i http://localhost:3000/healthz
```

A failing producer connection will return `503` from `/healthz` — that is expected when the container is run in isolation without a Kafka broker.

## See Also

- [aws-ecs.md](aws-ecs.md) — how the image is launched twice with different `command` overrides.
- [../security/container-scanning.md](../security/container-scanning.md) — Trivy scan in CD and the daily security audit.
- [../../guides/bun-docker-security-guide.md](../../guides/bun-docker-security-guide.md) — project-agnostic Bun container hardening reference.

## Changelog

| Date | Change |
|------|--------|
| 2026-05-19 | Initial container image doc created |
