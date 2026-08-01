# Final Railway Smoke Test Evidence

## Status

**Core public-path and durable-accounting smoke passed on August 1, 2026. The
deployed SHA, migration revision, private dependency endpoints, sanitized
application logs, live Langfuse traces, and private-only PostgreSQL/Redis
networking were verified. Controlled restart, key rotation, and bounded load
remain open.**

This document separates observed results from checks that still require an
authenticated Railway or Langfuse operator session. It does not treat the
public smoke as an SLA, capacity test, or complete production certification.

## Deployment identity

| Field | Current value |
|---|---|
| Tested application SHA | `53f8f784b64578b91745849269f186223df2f45b` |
| Pushed `origin/main` SHA | `53f8f784b64578b91745849269f186223df2f45b` |
| Green CI SHA | `53f8f784b64578b91745849269f186223df2f45b` |
| Green CI run | [GitHub Actions run 30557100025](https://github.com/kishan0-07/LLM-Gateway/actions/runs/30557100025) |
| Railway deployment SHA | `53f8f784b64578b91745849269f186223df2f45b` |
| Railway deployment ID | `8fc4e474-ef4a-4feb-83fc-87021def77be` |
| Public domain | <https://llm-gateway-7.up.railway.app> |
| Smoke window | 2026-08-01 06:29–06:32 UTC / 11:59–12:02 IST |
| Railway region / replicas | US West (`sfo`) / 1 replica |
| Railway resource ceiling | 2 vCPU / 1 GB RAM |
| Migration pre-deploy result | `alembic upgrade head` configured; deployed database is at `e7f4a2c91b60 (head)` |

The public OpenAPI document exposes the expected shipping routes and expanded
stats contract. Railway shows the deployment as active and sourced from the
same commit that passed GitHub Actions.

## Public smoke matrix

| Check | Observed result | Status |
|---|---|---|
| `GET /health` | HTTP 200, `{"status":"ok"}` | PASS |
| `GET /ready` | HTTP 200, `{"status":"ready"}` | PASS |
| Invalid API key | HTTP 401, `authentication_failed`, caller trace ID returned unchanged | PASS |
| Non-stream completion | HTTP 200, normalized Groq response and six-decimal cost | PASS |
| Streaming completion | HTTP 200, deltas followed by terminal `[DONE]` | PASS |
| `/stats/me` isolated delta | 2 requests, 2 settled reservations, 2 attempts, 2 ledger rows | PASS |
| Post-smoke active holds | 0 active reservations, 0 held micros | PASS |
| Selected traces in PostgreSQL | Both requests completed; reservations settled; zero hold; attempt/ledger parity and cost equality true | PASS |
| Application-log review | Sampled deployment logs contain trace/usage metadata and no prompt, response, key, or raw provider exception | PASS |
| Langfuse live traces | Both trace IDs match PostgreSQL/log metadata and contain only sanitized excerpts plus allowlisted metadata | PASS |
| Gateway dependency URLs | Runtime hosts are `postgres.railway.internal` and `redis.railway.internal` | PASS |
| PostgreSQL and Redis public exposure | Both public TCP proxies removed; `/health` and `/ready` remained HTTP 200 after each change | PASS |
| Restart/redeploy readiness recovery | Requires a controlled Railway restart/redeploy | PENDING |
| Migration failure blocks activation | Earlier malformed `DATABASE_URL` stopped deployment during pre-deploy | PASS |

## Safe request evidence

No prompt, response, header, credential, or raw provider error is stored here.

| Type | Trace ID | Request ID | Provider/model | HTTP / terminal | Client E2E | Stream TTFT |
|---|---|---:|---|---|---:|---:|
| Non-stream | `dogfood-railway-sync-625f7aef184b4ff798df642fb03f4d7b` | 26 | Groq / `openai/gpt-oss-20b` | 200 / success | 1899.62 ms | n/a |
| Stream | `dogfood-railway-stream-e8d5091849bf43959393a883fac14243` | 27 | Groq / `openai/gpt-oss-20b` | 200 / `[DONE]` | 821.90 ms | 718.83 ms |

The non-stream request reported 79 input tokens, 81 output tokens, and
`$0.000030`. The stream reported 80 input tokens, 106 output tokens, and
`$0.000038`.

## PostgreSQL state-probe evidence

| Check | Non-stream request 26 | Stream request 27 |
|---|---:|---:|
| Request status | `completed` | `completed` |
| Reservation status / final status | `settled` / `completed` | `settled` / `completed` |
| Held micros after finalization | 0 | 0 |
| Consumed micros | 30 | 38 |
| Reconciliation state | `none` | `none` |
| Provider attempt | 40 / `success` | 41 / `success` |
| Provider latency | 659 ms | 221 ms |
| Ledger usage / billing | `actual` / `known` | `actual` / `known` |
| Attempt-to-ledger accounting complete | true | true |
| Ledger cost equals reservation cost | true | true |

These probes were executed inside the deployed container and queried the live
PostgreSQL database. They print only allowlisted metadata.

## Authoritative stats delta

| Metric | Delta / final value |
|---|---:|
| Total requests | +2 |
| Settled reservations | +2 |
| Provider attempts | +2 |
| Usage-ledger entries | +2 |
| Input tokens | +159 |
| Output tokens | +187 |
| Accounted cost | +$0.000068 |
| Failovers | +0 |
| Active reservations after smoke | 0 |
| Active held micros after smoke | 0 |

`/stats/me` also reported 25 reservations requiring reconciliation across the
key's earlier demo history. A grouped PostgreSQL query classified all 25 as
`settled` / `completed` with reason `actual_cost_exceeded_hold`. They have no
active hold. They are historical reconciliation debt, not failures from the two
smoke requests above, whose reconciliation state is `none`.

Operator disposition: retain these 25 rows as provider-invoice review flags.
They are already settled and fully accounted, while the implemented automatic
reconciler intentionally processes only uncertain reservations that are still
`reserved`. Do not rewrite them to `reconciled` merely to make the counter zero;
close them only through a future audited invoice-review operation.

## SHA invariant

Confirmed:

```text
tested local application SHA
  == pushed origin/main SHA
  == green GitHub CI SHA
  == Railway deployment SHA
  == 53f8f784b64578b91745849269f186223df2f45b
```

The evidence changes in the current worktree are a later, uncommitted
documentation state. They must not replace the tested application SHA above.

## Secret handling

- The smoke key was read only into a short-lived process environment.
- The key is absent from the redacted JSONL result and this document.
- Raw smoke artifacts remain under ignored `artifacts/dogfood/`.
- Because the key was shared in a chat, rotate it after the remaining operator
  checks and do not reuse it as a long-lived production credential.

## Remaining operator checks

1. Perform one controlled restart/redeploy, then repeat `/ready` and one
   authenticated request.
2. Rotate the exposed smoke key and verify that the old key returns 401.
3. Run only the bounded load stages whose spend, provider quota, test-only rate
   limits, Railway resource ceiling, and abort owner have been declared.

## Residual limitations

- The Railway ceiling is known (2 vCPU / 1 GB), but provider quotas and
  controlled capacity have not been measured.
- The bounded Locust stages remain blocked on a dedicated environment, declared
  spend/quota limits, elevated test-only rate limits, and an abort owner.
- A single live smoke validates one deployment path, not a production SLO.
