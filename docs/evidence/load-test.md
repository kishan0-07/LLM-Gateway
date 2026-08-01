# Load-Test Evidence

## Status

**Harness implemented and unit-tested; capacity stages intentionally not run.**

The current worktree is uncommitted at the user's request and no isolated load
environment has been deployed. The production service's current Railway
ceiling is known (2 vCPU / 1 GB), but provider spend/quota, elevated test-only
rate limits, connection ceilings, and an abort owner have not been approved.
Throughput, safe-concurrency, or capacity numbers are therefore not claimed.

## Harness verification

| Field | Value |
|---|---|
| Repository base commit | `713f18b` |
| Harness source state | uncommitted worktree |
| Locust version | `2.46.2` |
| Focused test result | 30 passed |
| Traffic mix | 80% non-stream / 20% stream |
| Output cap per request | 100 tokens |
| Stream primary metric | client E2E through `[DONE]` |
| Separate stream metric | first non-empty delta TTFT |
| Trace IDs | unique full UUID suffix |
| Failure output | HTTP status or allowlisted public SSE code only |

The tests cover:

- normalized non-stream schema validation;
- finite, non-negative token/cost fields;
- successful deltas followed by `[DONE]`;
- public SSE error-code filtering;
- malformed/non-object SSE data and missing terminal events;
- TTFT on the first non-empty delta;
- overriding Locust's header-only stream timing with body-complete E2E;
- credential-free root-origin validation;
- uniqueness of trace IDs;
- absence of prompts, responses, and API keys from helper output.

## Why capacity stages are blocked

Before a positive-capacity run, the operator must declare:

- isolated environment and exact application SHA;
- temporary tenant/API-key limits high enough not to turn expected policy
  rejections into false capacity failures;
- maximum provider requests and dollar spend;
- tenant budget hard stop;
- provider request/token quota;
- PostgreSQL and Redis connection ceilings;
- Railway CPU/memory plan;
- named abort owner.

Only the current Railway CPU/memory ceiling has been confirmed. The remaining
controls are absent, and the live public service is not an isolated load
environment, so even the five-user stage would not be publishable evidence yet.

## Planned bounded stages

Run only after the preconditions above are recorded:

```powershell
$env:GATEWAY_BASE_URL = "https://YOUR-ISOLATED-LOAD-DOMAIN"
$env:GATEWAY_API_KEY = Read-Host "Paste the isolated load-test API key"
$env:GATEWAY_MODEL = "openai/gpt-oss-20b"
New-Item -ItemType Directory -Force artifacts/load | Out-Null

uv run locust -f load/locustfile.py --headless `
  --users 5 --spawn-rate 1 --run-time 2m `
  --csv artifacts/load/5-users

uv run locust -f load/locustfile.py --headless `
  --users 25 --spawn-rate 5 --run-time 5m `
  --csv artifacts/load/25-users

# Run only if both earlier stages remain inside every declared limit.
uv run locust -f load/locustfile.py --headless `
  --users 100 --spawn-rate 10 --run-time 5m `
  --csv artifacts/load/100-users
```

For the isolated capacity environment only:

```dotenv
RATE_LIMIT_TENANT_REQUESTS=15000
RATE_LIMIT_API_KEY_REQUESTS=15000
RATE_LIMIT_REDIS_FAILURE_MODE=fail_closed
```

Restore production policy immediately afterward.

## Stop conditions

Abort the current stage and do not scale further after:

- an unexpected 5xx or unhandled exception;
- a stream without `[DONE]` or an unexpected protocol error;
- PostgreSQL/Redis pool exhaustion;
- reconciliation backlog that does not drain;
- held budget remaining after terminal states;
- unexpected rate-limit distribution;
- declared provider spend/quota reached;
- Railway resource limits dominating the result.

## Measurements required from a future run

Publish these separately and with exact sources:

- achieved requests/second and concurrent users;
- client E2E p50/p95/p99;
- stream TTFT and stream E2E percentiles;
- PostgreSQL-derived non-stream gateway-overhead percentiles;
- status/error distribution;
- provider mix and fallback count;
- reconciliation count and held-budget drain time;
- provider, Railway plan, duration, limits, and exact SHA.

Never label client latency as gateway overhead.

## Explicit 1,000-concurrency boundary

The priority chaos matrix and bounded protocol tests pass locally. A
1,000-concurrency result is intentionally not claimed because provider quota,
paid spend, connection ceilings, and hosting-plan limits were not controlled.

Raw CSV/HTML output belongs under ignored `artifacts/load/` and must not be
committed.
