# Broken Mermaid Fixture

This file is intentionally broken. The sequence diagram below reproduces the
SIO-804 admin-reload parse failure: semicolons in a message body are treated
as statement separators by Mermaid's sequence-diagram parser. Used by the
`scripts/validate-mermaid.ts` CI step to prove the validator actually catches
broken Mermaid blocks rather than silently passing.

Do not "fix" this diagram — its job is to fail.

```mermaid
sequenceDiagram
    autonumber
    participant Op as Operator
    participant Idx as gateway/index.ts

    Op->>Idx: PUT /admin/routes
    Idx-->>Op: 500 { error: "reload failed",<br/>message: "routes persisted; restart will apply" }
```
