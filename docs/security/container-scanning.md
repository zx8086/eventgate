# Container Scanning

> **Targets:** Bun 1.3.11+ | TypeScript 5.x | Trivy
> **Last updated:** 2026-05-19
> **Conventions:** See [../../guides/documentation-guide.md](../../guides/documentation-guide.md)

eventgate runs Trivy against every image it publishes and again every 24 hours on the latest image. Both scans upload SARIF to GitHub Code Scanning so findings appear in the Security tab and can gate future PRs. This document describes the two scan jobs, the severity policy, and how to interpret findings against the tiered Dockerfile.

## Scan Jobs

| Job | Workflow | Trigger | Image | Severity | Behaviour on findings |
|-----|----------|---------|-------|----------|----------------------|
| Build-time scan | `.github/workflows/cd.yml`, `scan` job | Every push to default branch and version tags (after build-and-push completes) | `docker.io/zx8086/eventgate:latest` | `HIGH,CRITICAL` | SARIF uploaded under category `trivy`. `ignore-unfixed: true` — vulnerabilities without an upstream fix do not block. |
| Daily audit | `.github/workflows/security-audit.yml`, `audit` job | Cron `0 6 * * *` and manual dispatch | `docker.io/zx8086/eventgate:latest` | `HIGH,CRITICAL` | SARIF uploaded under category `trivy-scheduled`. Does **not** set `ignore-unfixed` — surfaces every CVE so unfixed ones stay visible until upstream patches land. |

Both jobs authenticate to Docker Hub before scanning. Anonymous pulls from GitHub Actions runners hit rate-limit or 401 errors — the explicit `docker/login-action` step (`cd.yml:100-104` and `security-audit.yml:37-41`) is what makes the scan reliable.

## Daily Audit Scope

The daily workflow does more than just scan the image:

| Step | What it checks |
|------|---------------|
| `bun audit --severity high` | Dependency vulnerabilities in `bun.lock` |
| Trivy container scan | OS and language-package CVEs in the pushed image |
| Gitleaks secret scan | Committed secrets across the full git history (`fetch-depth: 0`) |

Each step is independent — a failure in one does not skip the others. SARIF and Gitleaks output appear in the GitHub Security tab.

## Tier Coverage

The CD workflow always builds and pushes the **Tier 2** distroless production stage (`target: production` in `cd.yml:69`). Trivy scans the result. The Tier 1 Alpine `release` stage is not built or scanned in CI — it exists as an on-demand fallback (see [../deployment/container-image.md](../deployment/container-image.md)).

| Tier | Scanned in CI | Scanned daily | Notes |
|------|--------------|---------------|-------|
| Tier 2 production | Yes | Yes | Distroless base, minimal attack surface. Most findings are in Bun, OpenSSL libs copied from Alpine, or the musl dynamic linker. |
| Tier 1 release | No (not built in CI) | No | If you need to publish the Alpine fallback, build with `--target release` and run Trivy locally before pushing. Findings will include the full Alpine package set. |

## Severity Policy

| Severity | Action |
|----------|--------|
| `CRITICAL` | Investigate same day. If the vulnerable package is reachable from `src/`, upgrade or replace before the next release. |
| `HIGH` | Triage within the week. Document an exception (with an expiry date) if you cannot fix and the CVE is unreachable. |
| `MEDIUM` and below | Not currently surfaced by either scan. Lower the `severity` filter on a scan-by-scan basis when investigating. |

`ignore-unfixed: true` on the build-time scan is deliberate — a noisy CD scan that fails on CVEs with no upstream patch leads to the wrong reflex (disable the scan). The daily audit keeps unfixed findings visible without blocking deploys.

## Interpreting Common Findings

| Finding location | Likely root cause | Mitigation |
|------------------|-------------------|------------|
| `/usr/local/bin/bun` | Bun version itself | Bump `BUN_VERSION` in the Dockerfile and rebuild |
| `/lib/ld-musl-*.so.1`, `/usr/lib/libgcc_s.so.1`, `/usr/lib/libstdc++.so.6` | Alpine packages copied into the distroless stage | Bump the `oven/bun:${BUN_VERSION}-alpine` base for `deps-base`; the copies pick up the new libs on rebuild |
| `node_modules/<pkg>` | Bun production dependency | Update `package.json`, run `bun install`, commit `bun.lock` |
| `/etc/ssl/certs/ca-certificates.crt` | CA bundle freshness | Same as above — bump the Alpine base in `deps-base` |

## Local Reproduction

To reproduce the CD scan locally before pushing:

```bash
# Build the production stage matching CD
docker build --target production -t eventgate:scan .

# Run the same Trivy filter
docker run --rm \
  -v /var/run/docker.sock:/var/run/docker.sock \
  aquasec/trivy:latest image \
  --severity HIGH,CRITICAL --ignore-unfixed eventgate:scan
```

To reproduce the daily audit's stricter scan (no `--ignore-unfixed`):

```bash
docker run --rm \
  -v /var/run/docker.sock:/var/run/docker.sock \
  aquasec/trivy:latest image \
  --severity HIGH,CRITICAL eventgate:scan
```

## Upgrading the Base Image

The Dockerfile takes the production base as a build argument:

```dockerfile
ARG DHI_IMAGE=gcr.io/distroless/static-debian12:nonroot
```

To move to Docker Hardened Images (DHI) once a subscription is provisioned, pass `--build-arg DHI_IMAGE=dhi.io/static:<tag>` in `.github/workflows/cd.yml`. No Dockerfile change is required. The Trivy scan will pick up the smaller findings surface automatically on the next CD run.

## See Also

- [../deployment/container-image.md](../deployment/container-image.md) — what is actually in the image being scanned.
- [../../guides/bun-docker-security-guide.md](../../guides/bun-docker-security-guide.md) — project-agnostic Bun container hardening reference.
- [../../guides/ci-cd-guide.md](../../guides/ci-cd-guide.md) — project-agnostic CD pipeline patterns.

## Changelog

| Date | Change |
|------|--------|
| 2026-05-19 | Initial container scanning doc created |
