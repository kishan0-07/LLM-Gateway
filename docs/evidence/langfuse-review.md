# Langfuse Review

## Railway deployment review — August 1, 2026

The deployed application reports `LANGFUSE_ENABLED=true` and exports to the JP
Langfuse region. An authenticated Chrome review found both live smoke traces:

| Trace ID | Event | Request / attempt | Provider/model | Cost | Result |
|---|---|---:|---|---:|---|
| `dogfood-railway-sync-625f7aef184b4ff798df642fb03f4d7b` | `request_completed` | 26 / 40 | Groq / `openai/gpt-oss-20b` | `$0.000030` | `success`, reconciliation `none` |
| `dogfood-railway-stream-e8d5091849bf43959393a883fac14243` | `stream_completed` | 27 / 41 | Groq / `openai/gpt-oss-20b` | `$0.000038` | `success`, reconciliation `none` |

The request IDs, attempt IDs, provider/model, outcome, usage source, costs, and
reconciliation states match the PostgreSQL probes and Railway logs. Langfuse
contains only `prompt_excerpt`, `response_excerpt`, and the documented metadata
fields. The two intentionally harmless smoke prompts contain no PII. No gateway
API key, provider secret, database URL, Redis URL, authorization header, or raw
provider error is present. Langfuse's own public ingestion-key and OpenTelemetry
resource attributes are platform metadata, not gateway secrets.

- Reviewed implementation commit SHA: `99c27c247eb21bea65d8ac0a84d9eca2c5e5e96e`
- Evidence-collection base SHA: `56e9852f7971b5ea9b5a213b6cbe5fc585b0adc3`
- Collection state: WP4-remediated worktree subsequently finalized in the
  reviewed implementation commit
- Session timestamp UTC: `2026-07-30T08:00:59.699949+00:00`
- Session timestamp IST: `2026-07-30T13:30:59.699949+05:30`
- Sample count: 10
- Negative-control trace: `langfuse-negative-7ad3b48f20c741f2a0a7643308625dad`

| Trace ID | Type | Event name | DB request / attempt | Langfuse matched | PII clean | Notes |
|---|---|---|---|---|---|---|
| `dogfood-wp4-pii-003-529cf75fb66740ccb6496f121f2a53f2` | non-stream success | `request_completed` | `336` / `250` | Yes | Yes ([SSN]) | Application metadata allowlisted; PostgreSQL fields matched. |
| `dogfood-wp4-pii-004-aac7164e842347878ff55323e156d9dc` | non-stream success | `request_completed` | `337` / `251` | Yes | Yes ([EMAIL], [PHONE], [SSN]) | Application metadata allowlisted; PostgreSQL fields matched. |
| `dogfood-wp4-code-010-122f270a4b2644bda81b258acba2c014` | non-stream success | `request_completed` | `332` / `245` | Yes | N/A | Application metadata allowlisted; PostgreSQL fields matched. |
| `dogfood-wp4-code-012-15ca2f38021543a2819d9a298b3528ac` | non-stream success | `request_completed` | `333` / `247` | Yes | N/A | Application metadata allowlisted; PostgreSQL fields matched. |
| `dogfood-wp4-pii-001-ea72545318794361a91586d378d9c5a0` | stream success | `stream_completed` | `334` / `248` | Yes | Yes ([EMAIL]) | Application metadata allowlisted; PostgreSQL fields matched. |
| `dogfood-wp4-pii-002-6503d3a9812944b09f91d18cd7857188` | stream success | `stream_completed` | `335` / `249` | Yes | Yes ([PHONE]) | Application metadata allowlisted; PostgreSQL fields matched. |
| `dogfood-wp4-code-001-59616d0abc564a5db53f15ee659ea430` | stream success | `stream_completed` | `331` / `243` | Yes | N/A | Application metadata allowlisted; PostgreSQL fields matched. |
| `wp4-failover-d133df62e9ab448e83ee3ec3672fe69e` | failover | `request_completed` | `328` / `239` | Yes | N/A | Application metadata allowlisted; PostgreSQL fields matched. |
| `wp4-output-limit-d133df62e9ab448e83ee3ec3672fe69e` | authorized output limit | `stream_output_limit_exceeded` | `329` / `240` | Yes | N/A | Application metadata allowlisted; PostgreSQL fields matched. |
| `wp4-terminal-failure-d133df62e9ab448e83ee3ec3672fe69e` | terminal provider failure | `request_failed` | `330` / `242` | Yes | N/A | Application metadata allowlisted; PostgreSQL fields matched. |

## Allowlist result

- Application-controlled metadata was restricted to the documented 11-key allowlist.
- Langfuse added its own `resourceAttributes.*` and `scope.*` transport metadata.
  Those platform-owned keys were reviewed separately and were not supplied by
  GatewayLLM's event dictionary.
- Observation input/output contained only `prompt_excerpt` and
  `response_excerpt`.
- The Langfuse sink now applies defensive redaction to both excerpts even when
  an upstream caller supplies unsanitized text.
- No gateway API key, provider secret, database URL, Redis URL, authorization
  header, provider exception body, or raw fake PII sentinel appeared.
- The same four PII review traces were checked against persisted structured
  application logs: `4/4` contained only the expected redaction labels and no
  raw sentinel values.

## Negative control

- HTTP result: `422 validation_error`
- Structured log present: `true`
- Langfuse generation absent: `true`

## Failure isolation

`tests/test_langfuse_sink.py` proves sink exceptions are swallowed and cannot
change the request or PostgreSQL settlement path. PostgreSQL values were used as
the authority for every comparison in this review.

## Conclusion

PostgreSQL remains billing truth. Langfuse status: **healthy** for the reviewed
sample. Runtime traces identify their collection base, while the reviewed
implementation commit identifies the finalized and fully tested source tree.
