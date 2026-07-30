# GatewayLLM Architecture Decisions

This record describes decisions implemented in the current gateway. Evidence
links point to source, tests, or sanitized run summaries; observability data is
never treated as financial proof.

## D-001 — Keep a Modular Monolith with Hexagonal Boundaries

**Context:** The gateway coordinates authentication, policy, provider calls,
accounting, and recovery. Coupling those rules directly to FastAPI, SQLAlchemy,
Redis, or provider SDKs would make failure behavior difficult to test.

**Decision:** Keep one deployable FastAPI process, but place lifecycle rules in
application use cases, external contracts in ports, and SDK/database/cache
details in infrastructure adapters.

**Consequences:** The project remains operationally simple while providers and
state backends can be replaced in tests. The application layer still needs
careful orchestration tests because one use case owns a large lifecycle.

**Evidence:** [`app/application`](app/application),
[`app/infrastructure`](app/infrastructure),
[`tests/test_execute_completion.py`](tests/test_execute_completion.py), and
[`tests/test_stream_completion.py`](tests/test_stream_completion.py).

## D-002 — PostgreSQL Is the Financial Authority

**Context:** A financial decision must remain auditable across process crashes,
Redis restarts, retries, and concurrent requests.

**Decision:** Authorize, reserve, consume, release, and reconcile budget in
PostgreSQL. Lock the tenant/month budget-period row when changing available
capacity. Redis does not cache or own the balance.

**Consequences:** Financial writes pay database transaction and lock cost, but
one durable system owns the invariant. Very high concurrency for one tenant may
eventually require sharded accounts or a dedicated ledger service.

**Evidence:**
[`postgres_budget_store.py`](app/infrastructure/db/postgres_budget_store.py),
[`d4c8b1a7e2f0_postgres_budget_authority.py`](alembic/versions/d4c8b1a7e2f0_postgres_budget_authority.py),
[`tests/test_budget.py`](tests/test_budget.py), and
[`tests/test_usage_ledger.py`](tests/test_usage_ledger.py).

## D-003 — Reserve Worst-Case Exposure Before Provider Spend

**Context:** A successful “check remaining budget” followed by a later charge
contains a race: another request can spend the same capacity in between.

**Decision:** Estimate input plus authorized output, create the durable gateway
request, and reserve the worst-case cost before starting any provider attempt.

**Consequences:** A request may temporarily hold more than it ultimately uses.
Every terminal path must therefore finalize exactly once and release the
unused hold.

**Evidence:** [`budget_authorizer.py`](app/application/services/budget_authorizer.py),
[`execute_completion.py`](app/application/use_cases/execute_completion.py),
[`stream_completion.py`](app/application/use_cases/stream_completion.py), and
the provider-not-called outage cases in
[`tests/test_priority_smoke.py`](tests/test_priority_smoke.py).

## D-004 — Account Per Provider Attempt

**Context:** A failed, timed-out, or invalid provider response can still create
a provider bill before the gateway falls back.

**Decision:** Create a durable `provider_attempts` row for every started call,
disable provider-SDK auto-retries, and write at most one `usage_ledger` row for
each attempt. GatewayLLM owns fallback so request cost is the sum of all
explicit attempt ledger rows, not merely the successful response.

**Consequences:** Fallback cost remains visible and idempotency constraints
become part of correctness. When actual usage is unavailable, the ledger stores
a conservative value and an explicit source/status.

**Evidence:** [`models.py`](app/infrastructure/db/models.py),
[`postgres_budget_store.py`](app/infrastructure/db/postgres_budget_store.py),
[`tests/test_usage_ledger.py`](tests/test_usage_ledger.py), and the 65-attempt /
65-ledger reconciliation in
[`dogfood-summary.md`](docs/evidence/dogfood-summary.md).

## D-005 — Use Redis Only for Coordination

**Context:** Sliding-window rate limits and circuit state are short-lived,
high-frequency coordination concerns. They are not financial records.

**Decision:** Use one Redis Lua script to atomically enforce tenant and API-key
rate limits and Redis keys for circuit-breaker state. Keep production
rate-limiter failure policy explicit and fail closed.

**Consequences:** Losing Redis may temporarily remove coordination state or
make production requests unavailable, but it cannot erase settled spend or
change the budget ledger.

**Evidence:** [`rate_limiter.py`](app/infrastructure/redis/rate_limiter.py),
[`circuit_breaker.py`](app/infrastructure/redis/circuit_breaker.py),
[`tests/test_rate_limiter.py`](tests/test_rate_limiter.py), and
[`tests/test_routing_engine.py`](tests/test_routing_engine.py).

## D-006 — Normalize Providers Behind One Contract

**Context:** “OpenAI-compatible” APIs still differ in request parameters,
stream events, error shapes, model identifiers, and usage reporting.

**Decision:** Keep Groq, OpenAI, and the deterministic mock behind the
`ProviderClient` contract. Normalize provider results before application policy
uses them; keep adapter-specific parameter translation inside each adapter.

**Consequences:** Application code avoids SDK branching, but every adapter
requires contract tests for request parameters, malformed output, usage, and
stream failure.

**Evidence:** [`provider_client.py`](app/application/ports/provider_client.py),
[`groq.py`](app/infrastructure/providers/groq.py),
[`openai.py`](app/infrastructure/providers/openai.py),
[`test_groq_provider.py`](tests/contract/test_groq_provider.py), and
[`test_openai_provider.py`](tests/contract/test_openai_provider.py).

## D-007 — Streaming Finalizes Deterministically and Enforces the Authorized Limit

**Context:** A provider may stall, disconnect, ignore `max_tokens`, or report
hidden usage after the visible stream.

**Decision:** Apply independent total and idle timeouts, cap locally visible
output at the authorized limit, close iterators best-effort, preserve actual
reported usage, and finalize the reservation exactly once. Output-limit
protocol failures do not count as provider-health failures.

**Consequences:** The client cannot receive visible output beyond what was
authorized. Billing uncertainty remains explicit through reconciliation rather
than being silently discarded.

**Evidence:** [`stream_completion.py`](app/application/use_cases/stream_completion.py),
[`tests/test_stream_completion.py`](tests/test_stream_completion.py),
[`tests/test_error_contract.py`](tests/test_error_contract.py), and the output
limit scenario in [`langfuse-review.md`](docs/evidence/langfuse-review.md).

## D-008 — Separate Content Quality from Provider Health

**Context:** A provider can return empty or unusable content while its
infrastructure remains reachable and otherwise healthy.

**Decision:** Permit failover for unusable output without recording a circuit
failure. Only transport/provider-health categories affect the circuit breaker.

**Consequences:** One prompt-specific bad output does not take a healthy
provider offline, while the caller can still receive a useful fallback.

**Evidence:**
[`response_validator.py`](app/application/services/response_validator.py),
[`routing_engine.py`](app/application/services/routing_engine.py),
[`tests/test_execute_completion.py`](tests/test_execute_completion.py), and
[`tests/test_routing_engine.py`](tests/test_routing_engine.py).

## D-009 — Logs and Langfuse Are Non-Billing Projections

**Context:** Observability must not expose credentials or raw personal data,
and it can be delayed, sampled, or unavailable.

**Decision:** Keep Structlog metadata-only by dropping prompt and response
fields at the log sink. When Langfuse is explicitly enabled, export only
allowlisted metadata plus redacted/truncated excerpts. Treat both systems as
derived observability; use PostgreSQL for every billing comparison.

**Consequences:** Telemetry failure cannot change request settlement. The
redactor is a practical safeguard, not a compliance-grade data-loss-prevention
system.

**Evidence:** [`event_logger.py`](app/infrastructure/observability/event_logger.py),
[`sanitizer.py`](app/application/services/sanitizer.py),
[`langfuse_sink.py`](app/infrastructure/observability/langfuse_sink.py),
[`tests/test_event_logger.py`](tests/test_event_logger.py),
[`tests/test_sanitizer.py`](tests/test_sanitizer.py),
[`tests/test_langfuse_sink.py`](tests/test_langfuse_sink.py), and
[`langfuse-review.md`](docs/evidence/langfuse-review.md).

## D-010 — Railway Is the Production Edge; Nginx Validates the Local Shape

**Context:** Railway provides public TLS/domain routing, while local testing
still needs a reverse proxy that can prove SSE buffering behavior.

**Decision:** Deploy only the gateway application plus private managed
PostgreSQL and Redis on Railway. Keep Nginx in the local production-shaped
Compose stack and disable response buffering for SSE.

**Consequences:** The hosted topology has fewer moving pieces. Local Nginx
evidence does not replace a live Railway smoke test.

**Evidence:** [`railway.json`](railway.json),
[`docker-compose.production.yml`](docker-compose.production.yml),
[`nginx.conf`](nginx/nginx.conf), and
[`railway-checklist.md`](docs/deployment/railway-checklist.md).

## D-011 — Measurements Keep Their Meaning

**Context:** Gateway policy overhead, provider latency, client end-to-end
latency, and stream time-to-first-token measure different parts of the path.

**Decision:** Derive non-stream gateway-overhead percentiles from PostgreSQL.
Measure client E2E and stream TTFT/E2E in clients. Label each metric by its
source and never rename client latency as gateway overhead.

**Consequences:** The public evidence can contain “not measured” values and
limitations instead of unsupported performance claims.

**Evidence:** [`stats_reader.py`](app/infrastructure/db/stats_reader.py),
[`scripts/dogfood/summarize.py`](scripts/dogfood/summarize.py),
[`load/harness.py`](load/harness.py), and measured values in
[`dogfood-summary.md`](docs/evidence/dogfood-summary.md).

## D-012 — High-Concurrency Claims Require Controlled Conditions

**Context:** Provider quotas, application rate limits, database/Redis
connections, hosting resources, and paid token spend can dominate a load test.

**Decision:** Keep a bounded, SSE-aware Locust harness and protocol tests, but
publish capacity results only from an isolated environment with declared
limits, spend, quotas, hosting plan, duration, abort owner, and exact SHA.

**Consequences:** A 1,000-concurrency result is deliberately not claimed when
those controls are absent. This is an evidence boundary, not an invitation to
weaken accounting or turn expected `429` responses into “success.”

**Evidence:** [`locustfile.py`](load/locustfile.py),
[`harness.py`](load/harness.py),
[`tests/test_load_harness.py`](tests/test_load_harness.py), and the explicit
status in [`load-test.md`](docs/evidence/load-test.md).
