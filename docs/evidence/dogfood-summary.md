# Dogfood Summary — GatewayLLM

- Application SHA: `56e9852f7971b5ea9b5a213b6cbe5fc585b0adc3`

## Overview

- Total cases: 50
- Successful / failed: 50 / 0
- Streaming cases: 22
- Reconciled: 50
- Not found: 0
- Settled reservations: 50
- Active reservations after grace: 0
- Active held micros after grace: 0
- Reconciliation-needed: 38
  - active recovery work: 0
  - settled provider-bill review: 38
- Provider attempts: 65
- Usage-ledger entries: 65
- Attempt/ledger/cost mismatches: 0
- Failover requests: 15
- Tokens: 4692 input / 22240 output
- Accounted cost: $0.028858
- Fake-PII sentinel cases executed: 4

## Stats Before/After Reconciliation

| Metric | Before | After | Delta | Reconciled result |
|---|---:|---:|---:|---|
| Total requests | 0 | 50 | +50 | 50 unique traces |
| Settled reservations | 0 | 50 | +50 | 50 settled |
| Provider attempts | 0 | 65 | +65 | 65 attempt rows |
| Usage-ledger entries | 0 | 65 | +65 | 65 ledger rows |
| Active reservations | 0 | 0 | 0 | No active recovery work |
| Active held micros | 0 | 0 | 0 | All holds released |
| Reconciliation-needed | 0 | 38 | +38 | 38 settled provider-bill review flags |
| Failover requests | 0 | 15 | +15 | 15 reconciled failovers |
| Accounted cost | $0.000000 | $0.028858 | +$0.028858 | Ledger cost equals consumed micros |

The before/after API snapshots, per-trace reconciliation, and generated summary
agree on request, settlement, attempt, ledger, hold, reconciliation, failover,
token, and cost totals.

## Non-Stream Gateway Overhead

| Metric | Value |
|---|---:|
| p50 | 32.5 ms |
| p95 | 54.3 ms |
| p99 | 57.9 ms |

## Client Latency

| Metric | Value |
|---|---:|
| p50 | 1022.7 ms |
| p95 | 5687.7 ms |
| p99 | 7162.3 ms |

## Stream TTFT

| Metric | Value |
|---|---:|
| p50 | 930.6 ms |
| p95 | 3970.5 ms |
| p99 | 6678.3 ms |

## Stream End-to-End

| Metric | Value |
|---|---:|
| p50 | 1172.1 ms |
| p95 | 6666.9 ms |
| p99 | 7402.1 ms |

> Dataset note: prompts and responses are not published. This summary contains
> only aggregate metrics. The four PII sentinel categories were separately
> verified as redacted in both structured logs and Langfuse; see
> `docs/evidence/langfuse-review.md`.
