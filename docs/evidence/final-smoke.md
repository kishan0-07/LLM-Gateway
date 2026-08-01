# Final Railway Smoke Test Evidence

## Status

**Core public-path and durable-accounting smoke passed on August 1, 2026. The
temporary exposed key was subsequently rotated and the green-CI project was
redeployed. Post-redeploy `/health`, `/ready`, and `/openapi.json` checks passed.
Authenticated revocation evidence, in-flight stream drain evidence, and bounded
load remain open.**

This document separates observed results from checks that still require an
authenticated Railway or Langfuse operator session. It does not treat the
public smoke as an SLA, capacity test, or complete production certification.

## Deployment identity

| Field | Current value |
|---|---|
| Authenticated-smoke application SHA | `53f8f784b64578b91745849269f186223df2f45b` |
| Authenticated-smoke CI run | [GitHub Actions run 30557100025](https://github.com/kishan0-07/LLM-Gateway/actions/runs/30557100025) |
| Authenticated-smoke Railway deployment ID | `8fc4e474-ef4a-4feb-83fc-87021def77be` |
| Current pushed `origin/main` SHA | `7eed53633aea47bf80d3076d1748602e0764d51d` |
| Current green CI run | [GitHub Actions run 30690883651](https://github.com/kishan0-07/LLM-Gateway/actions/runs/30690883651) |
| Current Railway redeploy | Current main redeployed by the operator; replacement deployment ID was not captured here |
| Public domain | <https://llm-gateway-7.up.railway.app> |
| Smoke window | 2026-08-01 06:29–06:32 UTC / 11:59–12:02 IST |
| Railway region / replicas | US West (`sfo`) / 1 replica |
| Railway resource ceiling | 2 vCPU / 1 GB RAM |
| Migration pre-deploy result | Original smoke verified database head `e7f4a2c91b60`; current `railway.json` still configures `alembic upgrade head` |

The public OpenAPI document exposes the expected shipping routes and expanded
stats contract. The current main commit passed GitHub Actions, the operator
reported redeploying it, and the replacement public service recovered health,
readiness, and OpenAPI access. The replacement Railway deployment ID/SHA was not
independently read from the dashboard during this documentation update.

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
| Post-rotation redeploy readiness recovery | Operator completed redeploy; `/health`, `/ready`, and `/openapi.json` returned HTTP 200 at 2026-08-01 17:33 UTC | PASS |
| In-flight stream drain during termination | Not observed during this redeploy | PENDING |
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
authenticated smoke application SHA
  == smoke-time origin/main SHA
  == smoke-time green GitHub CI SHA
  == smoke-time Railway deployment SHA
  == 53f8f784b64578b91745849269f186223df2f45b
```

Current `origin/main` is `7eed53633aea47bf80d3076d1748602e0764d51d`
with green CI. The diff from the authenticated-smoke SHA contains documentation
only, so the application implementation is unchanged. The later redeploy's
public health/readiness/OpenAPI checks do not replace the earlier authenticated
request and PostgreSQL accounting evidence.

## Secret handling

- The smoke key was read only into a short-lived process environment.
- The key is absent from the redacted JSONL result and this document.
- Raw smoke artifacts remain under ignored `artifacts/dogfood/`.
- Because the key was shared in a chat, the operator rotated it on August 1,
  2026. Neither the old nor the new raw key was used or recorded during this
  documentation update.

## Remaining operator checks

1. Capture an old-key `401`, successful new-key authentication, and exactly one
   active key without storing either raw credential.
2. Capture an in-flight stream finalizing within the configured Uvicorn/Railway
   termination window.
3. Run only the bounded load stages whose spend, provider quota, test-only rate
   limits, Railway resource ceiling, and abort owner have been declared.

## Residual limitations

- The Railway ceiling is known (2 vCPU / 1 GB), but provider quotas and
  controlled capacity have not been measured.
- The bounded Locust stages remain blocked on a dedicated environment, declared
  spend/quota limits, elevated test-only rate limits, and an abort owner.
- A single live smoke validates one deployment path, not a production SLO.
