# Final Railway Smoke Test Evidence

## Status

**Not run — waiting for the user to commit, push, configure Railway, and deploy
the exact green-CI application SHA.**

No live production result is claimed in this document.

## Deployment identity

| Field | Current value |
|---|---|
| Application SHA | Pending user commit and push |
| Green CI SHA | Pending GitHub Actions |
| Railway deployment SHA | Pending Railway deployment |
| Railway deployment ID | Pending Railway deployment |
| Public domain | Pending Railway domain generation |
| UTC / IST smoke window | Pending live run |
| Migration pre-deploy result | Pending live run |

## Required checks

| Check | Status |
|---|---|
| `/health` returns 200 | Pending live deployment |
| `/ready` returns 200 with private PostgreSQL and Redis | Pending live deployment |
| Invalid key returns standard 401 and trace ID | Pending live deployment |
| Non-stream request returns normalized 200 | Pending live deployment |
| Stream emits deltas and exactly one `[DONE]` | Pending live deployment |
| `/stats/me` reflects the demonstrated calls | Pending live deployment |
| Selected trace reconciles request, attempts, ledger, and cost | Pending live deployment |
| Langfuse/log sample contains no secret or raw PII | Pending live deployment |
| PostgreSQL and Redis have no public exposure | Pending dashboard review |
| Restart/redeploy restores readiness | Pending live deployment |
| Migration failure blocks activation | Pending deployment-policy review |

## SHA invariant

The final smoke is valid only when:

```text
tested application SHA
  == pushed application SHA
  == green CI SHA
  == Railway deployment SHA used for the smoke run
```

An evidence-only documentation commit may be a later descendant. Record both
SHAs without rewriting history.

## Execution source

Follow [`docs/deployment/railway-checklist.md`](../deployment/railway-checklist.md).
Populate this file only from the live results; do not infer a pass from local
Docker validation.

## Residual limitations

- Railway plan limits, provider quotas, and public-network behavior cannot be
  established by local Compose.
- A single live smoke validates one deployment, not a production SLO.
