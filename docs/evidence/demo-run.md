# Demo Run Evidence

## Evidence status

**Historical local rehearsal passed; release video recording remains pending.**

At the time of this rehearsal, the executable demo and probe changes were
intentionally uncommitted at the user's request. They were later committed in
`53f8f78` and are present in current green-CI main `7eed536`. The measurements
below remain local rehearsal evidence; the later commit does not retroactively
turn them into hosted Railway measurements.

- **Repository base commit:** `713f18b`
- **Demo tooling state at rehearsal:** uncommitted worktree; later committed in `53f8f78`
- **Date:** 2026-07-30 (UTC and IST calendar date)
- **Stack:** local development Compose, direct app port
- **Base URL class:** loopback HTTP
- **Provider response content:** not recorded or printed
- **API key:** disposable, captured in-process, never printed by the demo

## Selected safe trace IDs

- `demo-concurrent-0-3bec082f779b48eab8213db6c3018eb8`
- `demo-concurrent-1-83b9c279d4914b8db22e3ebbd0f8e715`
- `demo-concurrent-2-279684ad8a574a75a44e139c2a355596`
- `demo-concurrent-3-a2747e9089a64a4e824952d59a7d1727`
- `demo-concurrent-4-0b27378c93b748f8b587f3a56ca7ecf1`

## Commands and results

| Demonstration | Expected | Actual | Result |
|---|---|---|---|
| `/health` and `/ready` | dependencies healthy | `ok` / `ready` | Passed |
| Five concurrent requests | five successes and five unique full trace IDs | 5/5 successful; 5/5 unique | Passed |
| Stats delta | durable rows account for every started attempt | +5 requests, +5 settled, +8 attempts, +8 ledger rows | Passed |
| Budget drain | no active reservation or held balance after completion | 0 active reservations; 0 held micros | Passed |
| Accounted usage | safe aggregate only | 419 input, 960 output, `$0.001856` | Passed |
| Request-state inspection | exact request, attempt parity, cost parity | 2 attempts, 2 ledger rows, `accounting_complete=true`, `cost_matches_reservation=true` | Passed |
| Real fallback | failed/invalid first attempt plus successful fallback are both accounted | Groq invalid output followed by OpenAI success on the selected trace | Passed |
| Authorized output limit | deterministic failure, no terminal success, actual usage preserved | covered by the reviewed WP4 scenario at commit `99c27c2`; not rerun in this concurrent rehearsal | Referenced |
| Langfuse safety | allowlisted sanitized metadata | reviewed separately in `docs/evidence/langfuse-review.md` | Referenced |

## Safe selected-trace reconciliation

For `demo-concurrent-0-3bec082f779b48eab8213db6c3018eb8`:

- request status: `completed`
- reservation status/final status: `settled` / `completed`
- held micros: `0`
- consumed micros: `591`
- attempts: Groq `invalid_output`, then OpenAI `success`
- ledger rows: `2`
- accounting complete: `true`
- ledger cost equals reservation consumption: `true`
- reconciliation classification: `needs_reconciliation /
  actual_cost_exceeded_hold`

The reconciliation flag is a settled provider-bill review state, not active
recovery work; the reservation hold is zero.

## Quick-start validation performed alongside the rehearsal

The separate named project `gateway-quickstart-1785421055` validated:

- health-aware dependency startup;
- Alembic upgrade on a fresh volume;
- the complete 260-test suite;
- one-shot API-key creation;
- Docker migration before app activation;
- public health/readiness;
- authenticated `/whoami`;
- invalid-key `401` with a trace ID;
- normalized non-stream completion;
- incremental SSE with eight deltas and one `[DONE]`;
- two settled requests and zero held micros.

The project was stopped without `-v`; its PostgreSQL volume was intentionally
preserved for review.

## Production-shaped lifecycle rehearsal

At rehearsal time, the then-uncommitted worktree image `llm-gateway:ship` was
also exercised behind local Nginx with private PostgreSQL/Redis container ports:

- fresh-volume migration completed before app activation;
- `/ready` returned `ready` through Nginx;
- rerunning `alembic upgrade head` on the current volume succeeded;
- restarting the app restored readiness;
- PostgreSQL and Redis health checks remained healthy;
- the stack was stopped without deleting its volumes.

This validates the local packaging contract, not Railway networking or a
deployed-SHA invariant.

## Remaining release-video rerun

The tooling is now committed, current main has green CI, and the project has
been redeployed. Before publishing the release video:

1. record the replacement Railway deployment ID/SHA;
2. use the rotated demo key without exposing it on screen;
3. rerun the concurrent demo and safe state probe against that deployment;
4. record only safe aggregate output and selected trace IDs;
5. add only the final hosted video link to the README.
