# 🚀 THE GATEWAY LLM — DETAILED EXECUTION PLAN (V5.3 — FULL-FEATURE SCHEDULE + PER-DAY HOUR AUDITED)
### Kishan Prajapati | BTech CSE — DY Patil
### July 2 → July 31, 2026 | Phase A: Sprint (Jul 2-14) + Phase B: College Mode (Jul 15-31)

> **This document is your single source of truth for the Gateway LLM build.**
> **V5 changes:** Keeps the same stack, scope, and July schedule, but fixes the long-term architecture before implementation hardens: use-case ownership, ledger-first budgeting, provider capability adapters, safer Redis failure policy, streaming finalization, and a clearer folder structure.
> **V5.1 changes:** Keeps the full V5 feature set. No features are removed or downgraded. This pass fixes day-level sequencing so the schedule matches the V5 architecture, folder structure, and feature dependencies.
> **V5.2 changes:** Rechecks the hour math against the actual calendar. The 229-hour figure is valid only if Sprint Day 1 was completed on Jul 2. If starting from Jul 3, remaining build capacity is 219.5 hours.
> **V5.3 changes:** Rechecks whether each day's task load fits the available hours. Full feature set remains locked; overloaded days are corrected by moving implementation detail to the right day, not by deleting scope.

---

## TABLE OF CONTENTS

1. [What The Gateway IS (and What It Is NOT)](#what-the-gateway-is)
2. [Pre-Conditions](#pre-conditions)
3. [Architecture Deep Dive](#architecture-deep-dive)
4. [Feature List (Locked)](#feature-list-locked)
5. [Folder Structure](#folder-structure)
6. [Nine Design Decisions](#nine-design-decisions)
7. [V5.1 Schedule Audit Findings](#v51-schedule-audit-findings)
8. [V5.3 Per-Day Hour Fit Audit](#v53-per-day-hour-fit-audit)
9. [Schedule Templates (3 Modes)](#schedule-templates)
10. [Phase A — Sprint Mode (Jul 2-14)](#phase-a--sprint-mode-jul-214)
11. [Phase B — College Mode (Jul 15-31)](#phase-b--college-mode-jul-1531)
12. [Math Refresher Integration](#math-refresher-integration)
13. [Gates and Checkpoints Summary](#gates-and-checkpoints-summary)
14. [Risk Register and Mitigation](#risk-register-and-mitigation)
15. [Dogfooding Protocol](#dogfooding-protocol)
16. [Security Considerations](#security-considerations)
17. [Horizontal Scaling Notes](#horizontal-scaling-notes)
18. [Blog Strategy](#blog-strategy)
19. [Changelog](#changelog)

---

## WHAT THE GATEWAY IS

The Gateway is **Layer 1 of the Adaptive LLM Inference Platform**. It is the routing, budgeting, and serving infrastructure that Layers 2 (Augmentation), 3 (Sampling), and 4 (Evaluation) plug into from August onward.

```
INCOMING QUERY
      |
      v
+----------------------------------+
|  LAYER 1: ROUTING (THE GATEWAY)  |  <-- YOU ARE HERE (July)
|  Multi-provider routing          |
|  Atomic token budget (Redis Lua) |
|  Usage ledger + budget settlement|
|  Circuit breaking                |
|  Streaming (tiktoken)            |
|  Rate limiting + Auth            |
|  PII sanitization on logs        |
|  Content quality checks          |
|  Structured logging (structlog)  |
|  Langfuse observability          |
+----------------------------------+
```

### What This Is NOT — Honest Positioning

> [!IMPORTANT]
> **Do not position this project as "better than LiteLLM."** LiteLLM supports 100+ providers, is battle-tested at scale, and has a massive community. Your Layer 1 Gateway rebuilds what LiteLLM already does — but that's the point.

**The correct framing (use this in interviews and README):**

> *"I built Layer 1 from scratch to learn production infrastructure patterns — Redis Lua atomicity, circuit breaking, async streaming, token economics. The real value of this project is in Layers 2-4 and Reflexion, which no existing open-source gateway provides: hybrid RAG, test-time compute scaling, evaluation pipelines, and a self-improving feedback loop."*

**Why this framing is stronger:**
- It's honest — interviewers respect honesty
- It shifts focus to your actual differentiators (Layers 2-4)
- It preempts the "why not just use LiteLLM?" question
- It proves you understand the competitive landscape

**Resume Line:**
> *"Built the routing layer of an adaptive LLM inference platform in FastAPI: multi-provider routing, circuit breaking, ledger-backed token budgeting with Redis Lua reservations, provider-attempt tracking, real-time streaming with deterministic finalization, PII-safe request logging, GitHub Actions CI, and Langfuse observability. Then added 3 novel layers on top (RAG, sampling, evaluation) that no existing gateway provides."*

---

## PRE-CONDITIONS

> [!CAUTION]
> **Do NOT start the Gateway sprint unless ALL of these are true by July 2.**

| # | Pre-Condition | How to Verify |
|---|---|---|
| 1 | Docker Compose fluency | Rebuild the Postgres+Redis compose from scratch without notes |
| 2 | Async Python understanding | Explain `gather()` vs `create_task()` without lookup |
| 3 | Async generators working | Build and run the `token_stream` + budget consumer example |
| 4 | FastAPI theory complete | Read entire official tutorial. Can explain `Depends()` clearly |
| 5 | FastAPI CRUD built (Week 5) | CRUD API with middleware + auth + streaming works |
| 6 | Redis fundamentals (Week 6) | Sliding window rate limiter + Lua budget script run successfully |
| 7 | SQLAlchemy async (Week 7) | `create_async_engine` with pool config + Alembic migrations run |
| 8 | Integration project (Jul 2) | Full FastAPI+SQLAlchemy+Redis+Docker pipeline in 6 hours |
| 9 | SQL depth | `EXPLAIN ANALYZE` run on 3 queries, indexes added |
| 10 | `pyproject.toml` habit | Every project from this point has `pyproject.toml` |

**Hard Rule:** Phase 0B must finish by July 1. Gateway starts July 2.

---

## ARCHITECTURE DEEP DIVE

### Request Flow (memorize this)

```
Client
  | X-API-Key + optional X-Trace-ID
RequestContextMiddleware -> trace_id only, no auth logic
  |
FastAPI Auth Dependency  -> hash key, lookup ApiKey + Tenant in DB, return Principal
  |
ExecuteCompletion Use Case owns the full lifecycle:
  |
  1. create GatewayRequest row
  2. Rate Limiter -> Redis Lua, tenant/API-key scoped
  3. Semantic Cache -> future, tenant-scoped, after auth/rate-limit
  4. Token + Cost Estimate -> ModelCatalog + TokenEstimator
  5. Budget Reservation -> Redis Lua fast path + Postgres BudgetReservation
  6. RoutingEngine -> ordered provider/model candidates
  7. CircuitBreaker -> provider+model health, not global provider only
  8. Provider Adapter -> ProviderResult or ProviderStreamEvent
  9. ResponseValidator -> output usable? if not, try next candidate
 10. UsageLedger -> settle actual tokens/cost/status
 11. Event Sink -> sanitized logs, Langfuse, metrics, cache write
  |
Client response
```

### Ownership Contract (do not violate this)

The Gateway must stay a **modular monolith**. One backend, one deployment, one database, one Redis. Do not split into microservices.

But inside the app, ownership is strict:

| Layer | Owns | Must NOT own |
|---|---|---|
| `api/` | HTTP parsing, dependencies, response/error mapping | Budget math, provider retry rules, DB models |
| `application/use_cases/` | Request lifecycle orchestration | Raw SDK calls, SQLAlchemy models, Redis commands |
| `domain/` | Gateway concepts and invariants | FastAPI, Redis, SQLAlchemy, provider SDKs |
| `infrastructure/` | Postgres, Redis, provider SDKs, Langfuse | Business decisions |
| `workers/` | Async drains, reconciliation, retryable background work | Request-time policy decisions |

**Golden rule:** `/v1/completions` and `/v1/chat/completions` are thin HTTP wrappers. The use case owns the lifecycle. If a future change touches budget + routing + provider + logging, it belongs in the use case, not scattered across random services.

### Latency Budget (Honest — Not Misleading)

> [!WARNING]
> **Never claim "p99 < 200ms" for the full request.** 95% of latency is the LLM provider. Measure and report **gateway overhead** separately.

```
Auth dependency:        ~1-2ms
Rate limiter (Lua):     ~1-3ms
tiktoken count:         ~2-5ms
Budget Lua (Redis):     ~1-3ms
Ledger insert/update:   ~2-6ms
Circuit breaker check:  ~1-2ms
Provider routing:       ~0.5ms
Content quality check:  ~1ms
PII sanitization:       ~1ms
Event emit:             ~1-2ms
-----------------------------------
GATEWAY OVERHEAD:       ~15-30ms   <-- THIS is what you measure and report
Provider API call:      ~200-2000ms  <-- This is the provider, not you
-----------------------------------
TOTAL:                  ~215-2030ms
```

**In the README and interviews, say:** *"Gateway adds ~15-30ms p99 overhead on top of provider latency, while keeping durable usage accounting."* That is honest and stronger than a lower number that hides accounting work.

---

## FEATURE LIST (LOCKED)

> [!IMPORTANT]
> **No scope creep.** If it's not on this list, it does not go into the Gateway in July.

| # | Feature | V5 Day | Priority |
|---|---|---|---|
| 1 | Use-case-owned request lifecycle (`ExecuteCompletion`) | Sprint Days 2-7 | P0 |
| 2 | Multi-provider routing (Groq + OpenAI) | Sprint Day 3 (Jul 4) | P0 |
| 3 | API key authentication (hashed key + tenant) | Sprint Day 2 (Jul 3) | P0 |
| 4 | Ledger-backed token budgeting (reservation + settlement) | Sprint Day 4 (Jul 5) | P0 |
| 5 | Circuit breaker (provider+model scoped) | Sprint Day 6 (Jul 7) | P0 |
| 6 | Streaming with finalizer and rolling estimate | Sprint Days 8-9 (Jul 9-10) | P0 |
| 7 | Mid-stream budget enforcement (with settlement guard) | Sprint Days 8-9 (Jul 9-10) | P0 |
| 8 | Durable usage ledger + provider attempt tracking | Sprint Days 2, 4, 7 | P0 |
| 9 | Sanitized request/event logging (not billing source of truth) | Sprint Day 7 (Jul 8) | P0 |
| 10 | Health + Stats endpoints from usage ledger | Sprint Day 10 (Jul 11) | P1 |
| 11 | Per-user/per-tenant rate limiting (Lua script — atomic) | Sprint Day 10 (Jul 11) | P0 |
| 12 | Explicit Redis failure policy | Sprint Days 4, 6, 10 | P0 |
| 13 | Explicit connection pool config | Sprint Day 1 (Jul 2) | P0 |
| 14 | Request IDs (trace_id) on every request | Sprint Day 1 (Jul 2) | P0 |
| 15 | **Content quality check post-response** | Sprint Day 6 (Jul 7) | P0 |
| 16 | **PII sanitization on persisted/logged text** | Sprint Day 7 (Jul 8) | P0 |
| 17 | **Structured logging (structlog)** | Sprint Day 1 (Jul 2) | P0 |
| 18 | **Graceful shutdown / worker drain handler** | Sprint Day 11 (Jul 12) | P1 |
| 19 | GitHub Actions CI pipeline | Phase B, Week B1 (Jul 17) | P1 |
| 20 | Langfuse observability | Phase B, Week B1 (Jul 18) | P1 |
| 21 | Nginx reverse proxy | Phase B, Week B1 (Jul 18) | P1 |
| 22 | Locust load testing (gateway overhead p99) | Phase B, Week B1 priority if time; final fallback Jul 29 | P1 |
| 23 | **decisions.md log** | Sprint Day 1 (Jul 2, ongoing) | P1 |
| 24 | **Pre-flight script** | Sprint Day 1 (Jul 2) | P1 |

Features 1, 4, 8, 9, and 12 are **V5 architecture fixes**. They are not scope creep; they prevent expensive rewrites later.

---

## FOLDER STRUCTURE

```
llm-gateway/
+-- pyproject.toml
+-- decisions.md                <-- NEW: technical decision log (Day 1, ongoing)
+-- app/
|   +-- __init__.py
|   +-- main.py                 <-- FastAPI app factory, router registration, lifespan
|   +-- core/
|   |   +-- __init__.py
|   |   +-- config.py           <-- pydantic-settings, env loading
|   |   +-- errors.py           <-- domain/app error classes + error codes
|   |   +-- logging.py          <-- structlog config
|   |   +-- ids.py              <-- trace_id/request_id helpers
|   +-- api/
|   |   +-- __init__.py
|   |   +-- deps.py             <-- auth dependency, DB/Redis/use-case dependencies
|   |   +-- middleware.py       <-- TraceIDMiddleware ONLY
|   |   +-- schemas/
|   |   |   +-- completion.py
|   |   |   +-- usage.py
|   |   +-- routes/
|   |       +-- completions.py   <-- thin HTTP wrapper around use case
|   |       +-- health.py
|   |       +-- usage.py
|   +-- domain/
|   |   +-- __init__.py
|   |   +-- auth.py             <-- Principal, Tenant, ApiKey concepts
|   |   +-- budget.py           <-- Reservation, Settlement, BudgetDecision
|   |   +-- provider.py         <-- ProviderResult, ProviderStreamEvent, ProviderError
|   |   +-- routing.py          <-- RouteCandidate, RoutingDecision
|   |   +-- usage.py            <-- UsageRecord, ProviderAttempt
|   +-- application/
|   |   +-- __init__.py
|   |   +-- use_cases/
|   |   |   +-- execute_completion.py  <-- owns non-stream lifecycle
|   |   |   +-- stream_completion.py   <-- owns stream lifecycle/finalizer
|   |   +-- services/
|   |   |   +-- budget_authorizer.py    <-- reserve/settle orchestration
|   |   |   +-- routing_engine.py       <-- provider/model selection
|   |   |   +-- model_catalog.py        <-- allowed model IDs, pricing, tokenizer hints
|   |   |   +-- token_estimator.py
|   |   |   +-- response_validator.py   <-- output validity, not provider health
|   |   |   +-- sanitizer.py
|   |   +-- ports/
|   |       +-- api_key_store.py
|   |       +-- budget_store.py
|   |       +-- provider_client.py
|   |       +-- rate_limiter.py
|   |       +-- usage_ledger.py
|   |       +-- event_sink.py
|   +-- infrastructure/
|   |   +-- __init__.py
|   |   +-- db/
|   |   |   +-- models.py        <-- SQLAlchemy models
|   |   |   +-- session.py       <-- async engine, session factory, pool config
|   |   |   +-- repositories.py  <-- DB-backed port implementations
|   |   +-- redis/
|   |   |   +-- client.py
|   |   |   +-- budget_store.py  <-- Redis Lua + Postgres-backed reservation calls
|   |   |   +-- rate_limiter.py
|   |   |   +-- circuit_breaker.py
|   |   |   +-- event_stream.py  <-- Redis Streams for non-billing events
|   |   +-- providers/
|   |   |   +-- base.py          <-- ProviderClient protocol + metadata
|   |   |   +-- groq.py
|   |   |   +-- openai.py
|   |   |   +-- registry.py
|   |   +-- observability/
|   |       +-- langfuse.py
|   |       +-- metrics.py
|   +-- workers/
|       +-- __init__.py
|       +-- event_drain.py       <-- drains Redis Streams to logs/Langfuse if enabled
|       +-- reservation_reconciler.py
+-- tests/
|   +-- __init__.py
|   +-- conftest.py
|   +-- test_auth.py
|   +-- test_budget.py          <-- reservation + settlement + race tests
|   +-- test_routing_engine.py  <-- failover + provider/model selection
|   +-- test_execute_completion.py
|   +-- test_stream_completion.py
|   +-- test_usage_ledger.py
|   +-- test_sanitizer.py
|   +-- test_rate_limiter.py
|   +-- contract/
|       +-- test_groq_provider.py
|       +-- test_openai_provider.py
+-- scripts/
|   +-- preflight.py            <-- NEW: environment check (Day 1)
|   +-- seed_user.py
|   +-- locustfile.py
+-- nginx/
|   +-- nginx.conf
+-- .github/workflows/ci.yml
+-- alembic/
+-- .env.example                <-- NEW: template for forkers (no secrets)
+-- .env
+-- Dockerfile
+-- docker-compose.yml
+-- README.md
```

> [!IMPORTANT]
> This is not "enterprise architecture." It is one app with clear ownership. The extra folders prevent spaghetti once streaming, budget settlement, provider failover, and observability all touch the same request.

---

## NINE DESIGN DECISIONS

> [!IMPORTANT]
> Understand these BEFORE writing any code. Each prevents a class of bugs.
> **Log every decision you make in `decisions.md`.** By July 31 this becomes your interview prep.

### Decision 1: The use case owns the request lifecycle

| | |
|---|---|
| **Naive approach** | Put auth in middleware, budget in one service, routing in another, logging in another, and glue it in the route |
| **Why it fails** | Nobody owns the full transaction. Provider called but budget not settled. Stream cancelled but log missing. Cache hit not counted. |
| **Production approach** | `ExecuteCompletion` and `StreamCompletion` own the lifecycle from request row -> provider attempts -> usage settlement -> response |

### Decision 2: UsageLedger is the source of truth; logs are not billing

| | |
|---|---|
| **Naive approach** | Treat `RequestLog` / Redis log buffer as billing history |
| **Why it fails** | Logs can be dropped, redacted, sampled, delayed, duplicated, or changed for observability needs |
| **Production approach** | Write durable `UsageLedger` and `ProviderAttempt` records. Logs/events are derived observability, not financial truth |

### Decision 3: Redis is coordination, not the financial database

| | |
|---|---|
| **Naive approach** | Keep only Redis counters for budget usage |
| **Why it fails** | Redis loss/restart destroys the only accounting record. You cannot audit or reconcile spend |
| **Production approach** | Redis Lua handles fast atomic reserve/check. Postgres stores reservations and settled usage. Reconciler repairs stale reservations |

### Decision 4: Redis failure policy is explicit, not always fail-open

| | |
|---|---|
| **Naive approach** | Crash the request |
| **Also naive** | Always allow request when Redis is down |
| **Why it fails** | For a paid gateway, Redis down can mean unlimited provider spend and no rate limiting |
| **Production approach** | Local/dev: fail-open and flag. Demo: fail-open with emergency cap. Paid/prod: fail-closed for budget/rate checks unless request is cache-only or tenant is explicitly trusted |

### Decision 5: Token counts are estimates until provider usage settles them

| | |
|---|---|
| **Naive approach** | Assume `tiktoken` equals every provider tokenizer |
| **Why it fails** | Groq/OpenAI/model tokenizers can differ; chunk-level BPE can overcount |
| **Production approach** | Use token estimates for reservation and mid-stream protection. Use provider usage when available. If unavailable, settle with final full-text estimate and mark `usage_source="estimated"` |

### Decision 6: Provider adapters expose capabilities, not just methods

| | |
|---|---|
| **Naive approach** | `complete()`, `stream()`, `count_tokens()`, `calculate_cost()` and call it done |
| **Why it fails** | Providers differ on streaming usage, JSON mode, tool calls, rate-limit headers, context windows, errors, and pricing |
| **Production approach** | Each provider exposes `ProviderMetadata` plus normalized `ProviderResult` / `ProviderStreamEvent` / `ProviderError` |

### Decision 7: Content validation is not the same as provider health

| | |
|---|---|
| **Naive approach** | Empty/garbage response -> immediately record circuit breaker failure |
| **Why it fails** | Bad output can be prompt-specific or validator-specific, not provider outage |
| **Production approach** | Record provider health failures for transport/timeouts/5xx/rate-limit classes. Record output validation separately. Fail over for user experience, but don't poison provider health blindly |

### Decision 8: Streaming must always finalize

| | |
|---|---|
| **Naive approach** | Put budget reconcile and logging in a route-local generator `finally` block |
| **Why it fails** | Client disconnects, provider timeouts, partial output, and cancellation paths become inconsistent |
| **Production approach** | `StreamCompletion` uses a finalizer that always settles reservation, records partial usage, closes provider attempt, emits events, and releases stale reservations through a reconciler |

### Decision 9: PII never reaches persistent storage unsanitized

| | |
|---|---|
| **Naive approach** | Log raw prompts and responses |
| **Why it fails** | Users send emails, phone numbers, addresses. Logging PII is a compliance liability |
| **Production approach** | Sanitize before storing request/response excerpts or emitting logs/events. Store minimal raw content by default; prefer metadata + redacted excerpts |

Minimal sanitizer lives at `app/application/services/sanitizer.py`. It is not compliance-grade, but it prevents the obvious Day 1 mistake.

---

## V5.1 SCHEDULE AUDIT FINDINGS

This pass preserves the full V5 feature set. It only corrects execution order and missing work.

| Finding | Severity | Correction |
|---|---|---|
| Day 7 pipeline referenced rate limiting before the Lua rate limiter exists | P0 | Day 7 now wires a `RateLimiter` port with a permissive implementation; Day 10 replaces it with Redis Lua. |
| CI expected Docker build before Dockerfile was explicitly created | P0 | Day 1 now creates a minimal Dockerfile and app service in Docker Compose; Jul 18 refines it into production multi-stage Docker. |
| Provider adapter day lacked a deterministic test provider | P1 | Day 3 now adds `MockProvider` for failover/stream/error tests without spending real API calls. |
| Budget day did not explicitly create token/cost estimation boundary | P1 | Day 4 now adds `TokenEstimator` and `ModelCatalog` integration before reservation. |
| Streaming day had no preflight gate for fake stream before real provider streaming | P1 | Days 8-9 now start with `MockProvider` streaming before real provider SSE. |
| Jul 18 overloaded Langfuse + Docker + Nginx in one day | P1 | Langfuse instrumentation starts Jul 16 night; Jul 18 focuses on Docker/Nginx plus Langfuse verification. |
| Railway deploy depended on environment secrets without a pre-check | P2 | Jul 24 now collects deployment env vars and Railway readiness before the Jul 26 deploy day. |

---

## V5.3 PER-DAY HOUR FIT AUDIT

This audit checks each day against actual available build time, not total calendar time.

**Capacity rules used for this audit:**
- Sprint day: 9.5 build hours + 1 math hour.
- College weekday: plan against 5.5 build hours, not the best-case 6.5. Reading room productivity is not guaranteed.
- Weekend: 8.5 build hours + 1.5 math hours.
- Math slides only when the Gateway gate is not met. It is not counted as build capacity.

### Sprint Phase Fit

| Day | Capacity | Original Load Verdict | V5.3 Correction |
|---|---:|---|---|
| Day 1 | 9.5h | Fits if environment setup is smooth; fails if Docker/Python setup breaks. | Keep as written. If starting Jul 3 and Day 1 is unfinished, complete Day 1 before touching Day 2. |
| Day 2 | 9.5h | Tight but valid only with minimal schema columns. | No admin CRUD, no dashboard queries, no relationship perfection. Build only the columns needed by auth, reservations, attempts, and ledger. |
| Day 3 | 9.5h | Overloaded if real streaming is implemented for both providers here. | Day 3 owns non-streaming provider adapters, contract shape, and `MockProvider`. Real provider streaming moves to Days 8-9. |
| Day 4 | 9.5h | Overloaded if full reconciler worker is built here. | Day 4 owns reservation, settlement, ledger, and one-shot stale-expiry function. Background worker wiring moves to Day 11. |
| Day 5 | 9.5h | Fits only as review/fix day. | Protected catch-up. Do not add new feature scope. |
| Day 6 | 9.5h | Fits, but tight. | Keep scope limited to simple routing order and provider:model circuit state. No weighted routing yet. |
| Day 7 | 9.5h | High risk because it wires the whole request path. | Event sink must be minimal: sanitized structured log or Redis Stream emit only. Event drain worker remains Day 11. |
| Days 8-9 | 19h | Fits only if MockProvider stream is proven before real SDK streaming. | Keep the mock-first order. Do not debug two provider SDK streams before the finalizer passes on fake chunks. |
| Day 10 | 9.5h | Fits. | Keep rate limiter + stats only. No analytics dashboard. |
| Day 11 | 9.5h | Overloaded if all 16 scenarios plus graceful shutdown are completed in one day. | Day 11 builds the test harness, error format, lifespan wiring, and priority smoke matrix. Full scenario completion finishes across Days 12-13. |
| Day 12 | 9.5h | Fits in Case A; becomes catch-up in Case B. | If Jul 2 was missed, P0 catch-up beats pytest expansion. |
| Day 13 | 9.5h | Fits only as buffer/finalization day. | No new features. This day exists to close streaming/test/finalizer debt. |

### College Phase Fit

| Day | Capacity | Original Load Verdict | V5.3 Correction |
|---|---:|---|---|
| Jul 15 | 5.5h | Fits. | Keep `test_stream_completion.py` and sanitizer tests only. |
| Jul 16 | 5.5h | Tight. Provider contracts + Lua rate tests + Langfuse can exceed the day. | Langfuse is no-op wrapper only at night. Full trace verification stays Jul 18. |
| Jul 17 | 5.5h | Fits if CI issues are normal; fails if dependency/build config is broken. | CI green is target. Docker build failures can continue into Jul 18 Session 1. |
| Jul 18 | 8.5h | Tight but valid after Jul 16 Langfuse prep. | Docker/Nginx first, Langfuse verification last. |
| Jul 19 | 8.5h | Overloaded if every chaos and Locust test is run plus fixed. | Run priority chaos first. Locust final p99 run can move to Jul 29 if failures consume the day. |
| Jul 20 | 5.5h | Fits. | README sections 1-5 only. |
| Jul 21 | 5.5h | Fits. | README sections 6-11 only. |
| Jul 22 | 5.5h | Fits. | Diagram + quick start in reading room; 10 dogfood queries at night. |
| Jul 23 | 5.5h | Fits because dogfood uses real study questions. | Keep as written. |
| Jul 24 | 5.5h | Tight but valid. | Railway env/secrets checklist is mandatory; do not push analysis into deploy day. |
| Jul 25 | 8.5h | Tight but valid if the live system is already stable. | Record demo before making polish changes. |
| Jul 26 | 8.5h | Fits only because env/secrets were prepared Jul 24. | If deployment breaks, skip extra writing and fix deploy. |
| Jul 27-31 | 5.5h/day | Fits. | These are buffer/polish days, not Layer 2 days. |

**Per-day verdict:** The total-hour math works, but the original daily load did not fully work. V5.3 fixes the bad assumptions by moving real provider streaming to Days 8-9, moving background worker wiring to Day 11, splitting integration scenarios across Days 11-13, and making Jul 19 priority-chaos-first instead of all-chaos-or-fail.

---

## SCHEDULE TEMPLATES

> [!CAUTION]
> **Two distinct operating modes. Do NOT mix them.** Phase A is a controlled burnout sprint — sustainable for 13 days, not 29. Phase B is the sustainable pace that carries you through college.

### Mode 1: SPRINT (Jul 2–14, full-time)

```
 8:30 AM   Wake + breakfast
 9:00 AM   DSA warmup (20 min — keep the muscle, don't grind hard problems)
 9:20 AM   BUILD SESSION 1 (4.5 hours — deep focus, hardest work here)
 2:00 PM   Lunch + break
 3:00 PM   BUILD SESSION 2 (3 hours)
 6:00 PM   GYM (1.5 hours)
 7:30 PM   Dinner
 8:30 PM   BUILD SESSION 3 (2 hours — evening push)
10:30 PM   MATH (1 hour — compressed from 1.5 during sprint)
11:30 PM   Journal + stop.
```

**Sprint Hour Count:**
```
Build:    4.5 + 3 + 2                  = 9.5 hrs/day
DSA:      20 min                        = 0.33 hr
Gym:      1.5h                          = non-negotiable
Math:     1h (compressed from 1.5)      = 1.0 hr
────────────────────────────────────────────────────
Total productive:  10.83 hrs/day
Sprint total:      9.5 × 13 = 123.5 hrs of build
```

> [!WARNING]
> **This pace is NOT sustainable for 29 days.** It works for 13 because you have college forced-rest starting Jul 15. Do not extend sprint mode past Jul 14.

### Mode 2: COLLEGE WEEKDAY (Jul 15–31, Mon-Fri)

```
 9:00 AM   College starts
           (use 3-5 hrs in reading room for BUILD — modular tasks only)
 6:30 PM   College ends
 7:00 PM   GYM (1.5 hours)
 8:30 PM   Dinner
 9:00 PM   BUILD SESSION (1.5 hours — night push)
10:30 PM   MATH (1 hour)
11:30 PM   Stop.
```

**College Weekday Hour Count:**
```
Build:    3-5h (reading room) + 1.5h (night)  = 4.5-6.5 hrs
Math:     1h (night)                            = 1.0 hr
Gym:      1.5h                                  = non-negotiable
────────────────────────────────────────────────────────
Total productive:  5.5-7.5 hrs/day
Weekday total (13 days × 5.5 avg):  ~72 hrs build
```

> [!TIP]
> **Reading room tasks must be MODULAR.** Writing tests, CI config, README sections, chaos test scripts — things you can pick up and put down in 1-2 hour chunks. Do NOT attempt deep architecture work (like streaming or Lua scripts) in the reading room.

### Mode 3: WEEKEND (Jul 18-19, Jul 25-26)

```
 9:00 AM   Wake + breakfast
 9:30 AM   BUILD SESSION 1 (4.5 hours)
 2:00 PM   Lunch + break
 3:00 PM   DSA (1 hour — full session on weekends)
 4:00 PM   BUILD SESSION 2 (2 hours)
 6:00 PM   GYM (1.5 hours)
 7:30 PM   Dinner
 8:30 PM   BUILD SESSION 3 (2 hours)
10:30 PM   MATH (1.5 hours — full session on weekends)
12:00 AM   Stop.
```

**Weekend Hour Count:**
```
Build:    4.5 + 2 + 2              = 8.5 hrs/day
DSA:      1h                        = catch up from weekdays
Math:     1.5h (full session)       = catch up deficit
Gym:      1.5h                      = non-negotiable
────────────────────────────────────────────────────
Total productive:  11 hrs/day
Weekend total (4 days × 8.5):  34 hrs build
```

### Total Build Hours — Honest Math

#### Case A: Sprint Day 1 was completed on Jul 2

```
Phase A (Sprint):     13 days × 9.5 hrs/day  = 123.5 hrs
Phase B (Weekdays):   13 days × 5.5 hrs/day  =  71.5 hrs
Phase B (Weekends):    4 days × 8.5 hrs/day  =  34 hrs
──────────────────────────────────────────────────────
TOTAL BUILD:                                  = 229 hrs

Original plan estimated:                      = 203 hrs
Difference:                                   = +26 hrs (buffer from sprint intensity)
```

> [!IMPORTANT]
> **Use the 229-hour number only if Sprint Day 1 was actually completed on Jul 2.** If Day 1 was not completed, this number is not your remaining capacity.

#### Case B: Execution starts from Jul 3

```
Phase A remaining:     12 days × 9.5 hrs/day  = 114.0 hrs
Phase B (Weekdays):    13 days × 5.5 hrs/day  =  71.5 hrs
Phase B (Weekends):     4 days × 8.5 hrs/day  =  34.0 hrs
---------------------------------------------------------
TOTAL REMAINING BUILD:                         = 219.5 hrs

Capacity lost from missing Jul 2:              = -9.5 hrs
Original plan estimated:                       = 203 hrs
Remaining difference vs original:              = +16.5 hrs
```

> [!CAUTION]
> **If Jul 2 was not completed, you do not have a free 13-day sprint anymore.** You still have enough calendar capacity on paper, but Day 5 and Day 13 stop being comfortable buffers. They become recovery days for whatever Day 1 work was missed.

**Feasibility verdict:** The hour math works only under the correct starting assumption. If Day 1 is complete, follow the plan as written. If Day 1 is not complete, keep the feature set but treat Sprint Day 5 and Sprint Day 13 as protected catch-up capacity, not optional polish.

**Daily Standup Journal (in decisions.md):**
```
## Day X — [Date]
**Target:** [What I planned]
**Actual:** [What I actually did]
**Decision:** [Any technical choice made today and why]
**Tomorrow:** [Single target]
```

---

## PHASE A — SPRINT MODE (Jul 2–14)

> [!CAUTION]
> **Goal: Complete ALL P0 features in 13 days.** Every feature that requires deep focus, long uninterrupted sessions, or complex debugging goes here. After Jul 14, you will never again have 9.5 build hours in a single day until January.

### WHY THIS WORKS

The original plan had 14 feature-days in Weeks 1-2, plus 2 buffer days. With 9.5 hrs/day instead of 6 hrs/day, 13 sprint days give you:

```
Original Weeks 1-2:    14 days × 6 hrs  = 84 hrs
Sprint Phase A:        13 days × 9.5 hrs = 123.5 hrs  (+47% more capacity)
```

If starting from Jul 3, the sprint calculation becomes:

```
Original Weeks 1-2:    14 days × 6 hrs  = 84 hrs
Sprint Phase A left:   12 days × 9.5 hrs = 114.0 hrs  (+36% more capacity)
```

This still gives enough raw build time, but it reduces recovery margin. Therefore the compression works only if the early foundation days do not spill into college mode.

### SPRINT DAY 1 (Jul 2): Environment + Preflight + trace_id + Modular Skeleton

**Session 1 (9:20-14:00):** Feel the API — call Groq directly, observe streaming, compare a cheap/fast model vs a larger/slower model, calculate real token costs. Verify live model IDs before committing them to `ModelCatalog`; do not trust old notes for model names.

**Session 2 + 3 (15:00-18:00, 20:30-22:30):**
1. Run `scripts/preflight.py` — verify Python 3.11+, Docker, Git all present
2. Docker Compose up (Postgres + Redis)
3. Project structure + `pyproject.toml`
4. Create the V5 folders (`api/`, `application/`, `domain/`, `infrastructure/`, `workers/`)
5. Connection pool config in `app/infrastructure/db/session.py`
6. `TraceIDMiddleware` in `app/api/middleware.py` — trace_id only, no auth
7. `structlog` setup in `app/core/logging.py` — JSON structured logs from Day 1
8. Create `decisions.md` — first entry: "Why use-case ownership over service spaghetti"
9. Create `.env.example` (no real secrets, just key names)
10. Create minimal `Dockerfile` for the FastAPI app
11. Add `app`, `postgres`, and `redis` services to `docker-compose.yml`

```python
# app/core/logging.py — structured logging from Day 1
import structlog

structlog.configure(
    processors=[
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.add_log_level,
        structlog.processors.JSONRenderer()
    ],
    wrapper_class=structlog.make_filtering_bound_logger(logging.INFO),
)

logger = structlog.get_logger()

# Usage everywhere:
logger.info("request_completed",
    trace_id=trace_id, provider="groq", model="openai/gpt-oss-20b",
    input_tokens=150, output_tokens=320, cost_usd=0.000024,
    latency_ms=234, gateway_overhead_ms=14
)
```

**Gate:**
- [ ] Preflight script passes
- [ ] Docker runs clean. DB connects. Redis pings.
- [ ] `docker compose up` starts app + Postgres + Redis
- [ ] Every request returns `X-Trace-ID` header
- [ ] All log output is structured JSON (not print statements)
- [ ] V5 folder structure exists
- [ ] `decisions.md` has first entry
- [ ] `.env.example` exists

**Math (22:30-23:30):** 3B1B LA ch.1-3 + MML 2.1-2.2 skim (60 min)

---

### SPRINT DAY 2 (Jul 3): Database Foundation + Auth Dependency

> Combined from original Days 2-3. With 9.5 hrs, both fit in one day.

**Session 1 (9:20-14:00):** Minimal production-shaped DB models. Alembic init + first migration + upgrade.

Build these tables now, with only necessary columns:
- `tenants`
- `api_keys` (`prefix`, `key_hash`, `tenant_id`, `status`, `created_at`, `last_used_at`)
- `budget_accounts`
- `budget_reservations`
- `gateway_requests`
- `provider_attempts`
- `usage_ledger`

**Session 2 (15:00-18:00):** Auth as FastAPI dependency, not auth middleware. Hash API key, lookup `ApiKey + Tenant`, return `Principal`. Skip auth for `/health`.

**Session 3 (20:30-22:30):** Seed one tenant, one active API key, one budget account. Test: valid key -> 200. Invalid -> 401. Missing -> 401. Health works without auth.

**Gate:** Alembic migration runs clean. All seven tables exist. Seed user works. Auth dependency returns `Principal(tenant_id, api_key_id)`. No route reads `request.state.user`.

**Math:** 3B1B LA ch.4-6 + MML 2.4-2.5 (60 min)

---

### SPRINT DAY 3 (Jul 4): Both Provider Adapters (Groq + OpenAI)

> Combined from original Days 4-5. Same pattern, second adapter is faster.

**Session 1 (9:20-14:00):** Provider contract with metadata:
- `ProviderClient.complete()`
- `ProviderClient.stream()` method signature and normalized event contract only
- `ProviderMetadata` (name, models, supports_streaming_usage, tokenizer_hint, pricing)
- `ProviderResult`
- `ProviderStreamEvent`
- `ProviderError`

Implement `GroqProvider.complete()`.

**Session 2 (15:00-18:00):** Implement `OpenAIProvider.complete()` with the same normalized result shape.

**Session 3 (20:30-22:30):** Add `MockProvider` with forced success, forced timeout, forced error, empty-output, and streaming-delta modes. Integration test — both real adapters and `MockProvider` return identical `ProviderResult` shapes. `MockProvider` proves the `ProviderStreamEvent` shape. Verify model IDs and pricing in `ModelCatalog`.

**Gate:** Both real providers return normalized non-streaming results. `MockProvider` can simulate success/failure/empty/streaming. Streaming event shape is proven with `MockProvider`; real provider streaming is explicitly Days 8-9. Provider-specific SDK objects do NOT leak outside `app/infrastructure/providers/`.

**Math:** MML 2.3 Gaussian elimination + `gaussian_elimination.py` (60 min)

---

### SPRINT DAY 4 (Jul 5): Budget Reservation + Usage Ledger

> Full day — Lua atomicity needs deep, uninterrupted focus.

**Build:** Atomic Redis Lua script for fast reserve, plus durable Postgres reservation/settlement.

Before the Lua script is called, wire the estimation boundary:
- `ModelCatalog` returns model pricing, context limits, and tokenizer hint
- `TokenEstimator` estimates input tokens and max output reservation
- `BudgetAuthorizer` converts estimate -> reservation request
- Provider actual usage always wins during settlement when available

**Mental model:**
```
Estimate request cost
  -> reserve estimated tokens/cost
  -> provider call happens
  -> settle actual usage in usage_ledger
  -> release unused reservation or record shortfall by policy
```

```lua
-- Atomic check + reserve. Redis fast path only; Postgres ledger remains truth.
local used = tonumber(redis.call('GET', KEYS[1]) or "0")
local limit = tonumber(ARGV[2])
local requested = tonumber(ARGV[1])
if (limit - used) >= requested then
    redis.call('INCRBY', KEYS[1], requested)
    redis.call('EXPIRE', KEYS[1], ARGV[3])
    return 1
else return 0 end
```

**Settlement guard:**
```python
async def settle_reservation(reservation_id: str, actual_tokens: int, status: str):
    """One request must settle exactly once: success, error, timeout, or cancelled."""
    reservation = await budget_store.get_for_update(reservation_id)
    if reservation.status != "reserved":
        return  # idempotency guard
    await usage_ledger.record_from_reservation(reservation, actual_tokens, status)
    await budget_store.mark_settled(reservation_id)
```

**Gate:** Lua runs via redis-cli. 100 concurrent reservations -> zero double-spending. Postgres has `budget_reservations` and `usage_ledger`. Settlement is idempotent. A one-shot `expire_stale_once()` function can mark expired reservations. Background reconciler lifecycle wiring is Day 11.

**Math:** 3B1B LA ch.7-9 + MML 2.6 Basis/Rank (60 min)

---

### SPRINT DAY 5 (Jul 6): Review + Fix Days 1-4

> **Do NOT skip this.** 4 days of compressed work accumulates bugs faster than normal pace.

**Build:** Fix everything broken Days 1-4. Then create the empty orchestration shell:
- `ExecuteCompletion` use case with dependencies injected
- `StreamCompletion` placeholder with finalizer method stub
- `BudgetAuthorizer` wrapper around reservation/settlement
- `UsageLedger` port and DB implementation wired
- Import smoke test for `api`, `application`, `domain`, `infrastructure`, and `workers`

Do not call providers from the route directly after this day.

**Gate:** All 10 curl tests pass. No broken imports or circular imports. trace_id visible in structured logs. `ExecuteCompletion` can create a `gateway_requests` row and return a stubbed response in a unit test.

**Math:** 3B1B LA ch.10-15 eigenvectors (60 min)

---

### SPRINT DAY 6 (Jul 7): Circuit Breaker + Routing Engine + Response Validator

> Combined from original Days 8-9. Circuit breaker is ~4 hrs, router with quality check is ~4 hrs.

**Session 1 (9:20-14:00):** Three-state FSM (CLOSED -> OPEN -> HALF_OPEN). Redis-backed state per `provider:model`. Graceful policy is explicit:
- Circuit state missing -> assume CLOSED
- Redis down in local/demo -> assume CLOSED and flag `circuit_state_unavailable`
- Redis down in production -> policy-controlled

**Session 2 (15:00-18:00):** `RoutingEngine` returns ordered `RouteCandidate`s from `ModelCatalog` and provider health. Start simple: Groq cheap model first, OpenAI fallback second.

**Response validation integrated into the use case, not into provider health blindly:**
```python
# app/application/use_cases/execute_completion.py
async def execute(self, request: CompletionRequest, principal: Principal) -> CompletionResponse:
    candidates = await self.routing_engine.plan(request, principal)
    for candidate in candidates:
        if not await self.circuit.is_available(candidate.provider, candidate.model):
            continue
        try:
            attempt = await self.usage_ledger.start_provider_attempt(candidate)
            response = await candidate.provider.complete(request, candidate.model)

            if not self.response_validator.is_valid(response.content):
                await self.usage_ledger.finish_provider_attempt(attempt, status="invalid_output")
                continue  # try next candidate, but do not poison provider health

            await self.circuit.record_success(candidate.provider, candidate.model)
            return response
        except Exception as e:
            await self.circuit.record_failure(candidate.provider, candidate.model, error=e)
            continue
    raise AllProvidersFailedError("All providers unavailable or returned invalid responses")
```

**Session 3 (20:30-22:30):** Test circuit breaker: break Groq model -> trips -> OpenAI takes over -> re-enable -> resets. Empty 200 -> failover without circuit poisoning.

**Gate:** Circuit trips per provider:model. Failover works. Empty 200 triggers failover but is recorded as `invalid_output`, not provider outage. Redis down behavior matches configured policy.

**Math:** MML 3.1-3.4 cosine similarity (60 min)

---

### SPRINT DAY 7 (Jul 8): Completions Endpoint + Usage Settlement + PII-Safe Events

> Full day — wiring everything together is where hidden bugs appear.

**Pipeline (full):**
```
HTTP Route -> Auth Dependency -> ExecuteCompletion Use Case
  -> create GatewayRequest
  -> RateLimiter port (permissive implementation today; Redis Lua on Day 10)
  -> estimate tokens/cost
  -> Budget Reservation
  -> RoutingEngine + Provider Attempt(s)
  -> ResponseValidator
  -> UsageLedger Settlement
  -> PII-Safe Event Emit
  -> Return
```

**Thin route:**
```python
@router.post("/v1/chat/completions")
async def complete(
    request: CompletionRequest,
    principal: Principal = Depends(get_current_principal),
    use_case: ExecuteCompletion = Depends(get_execute_completion),
):
    return await use_case.execute(request, principal)
```

**PII-safe event emit after ledger settlement:**
```python
event = {
    "event": "request_completed",
    "trace_id": trace_id,
    "tenant_id": principal.tenant_id,
    "request_id": gateway_request.id,
    "provider": result.provider,
    "model": result.model,
    "input_tokens": usage.input_tokens,
    "output_tokens": usage.output_tokens,
    "cost_usd": str(usage.cost_usd),
    "prompt_excerpt": sanitize(request.preview_text()),
    "response_excerpt": sanitize(result.content[:500]),
}
await event_sink.emit(event)  # Redis Streams or structured log; not billing truth
```

**Event sink rule:** Day 7 event sink is intentionally minimal: sanitized structured log or Redis Stream emit. Do not build the full background drain today. If event emit fails, the request can still succeed because `usage_ledger` already settled. If non-streaming ledger settlement fails before the response is sent, return an error. If streaming has already started, mark the reservation `needs_reconciliation`, emit an error event if possible, and let `reservation_reconciler.py` repair it. Never pretend an unsettled request is cleanly complete.

**Gate:** Happy path works. Budget exceeded -> 429. Provider failure -> failover. `gateway_requests`, `provider_attempts`, and `usage_ledger` rows are written. PII is stripped from events/logs. Event sink failure does not lose billing truth. `RateLimiter` port is wired, but real Redis Lua enforcement is still explicitly Day 10.

**Math:** MML 3.3, 3.8 projections + `cosine_similarity.py` (60 min)

---

### SPRINT DAYS 8-9 (Jul 9-10): Streaming Endpoint

> [!WARNING]
> **Still 2 full days. Still the highest risk.** Sprint pace doesn't change this — streaming is inherently complex. Don't rush it.

**Build:** Rolling estimate during stream. Mid-stream budget enforcement. 30s timeout. Finalizer that settles reservation exactly once. Proper error handling + SSE headers.

Execution order:
1. First implement streaming against `MockProvider` using deterministic chunks.
2. Prove finalizer behavior for success, timeout, exception, budget cutoff, and client disconnect.
3. Only then wire real Groq/OpenAI streaming.
4. Only after real streaming works, enable mid-stream budget cutoff.

**Stream with V5 finalization:**
```python
async def generate():
    accumulated_text = ""       # accumulate full text, not token count
    approx_output_tokens = 0    # rough count for mid-stream budget check only
    final_status = "cancelled"
    try:
        async for event in stream_use_case.stream(request, principal):
            if event.type == "delta":
                accumulated_text += event.content or ""
                approx_output_tokens += len(encoder.encode(event.content or ""))

            # Mid-stream budget check (approximate — for early termination only)
            if approx_output_tokens % 100 == 0:
                remaining = await budget_authorizer.remaining(principal.tenant_id)
                if remaining <= 0:
                    final_status = "budget_exceeded"
                    yield "data: [BUDGET_EXCEEDED]\n\n"
                    return
            yield f"data: {event.model_dump_json()}\n\n"

        final_status = "success"
    except httpx.ReadTimeout:
        final_status = "timeout"
        yield 'data: {"error": "stream_timeout", "trace_id": "' + trace_id + '"}\n\n'
    except Exception as e:
        final_status = "error"
        yield 'data: {"error": "stream_failed"}\n\n'
    finally:
        await stream_use_case.finalize(
            status=final_status,
            accumulated_text=accumulated_text,
            usage_source="provider_if_available_else_estimated",
        )

    yield "data: [DONE]\n\n"
```

**SSE headers:**
```python
return StreamingResponse(
    generate(),
    media_type="text/event-stream",
    headers={
        "Cache-Control": "no-cache",
        "Connection": "keep-alive",
        "X-Accel-Buffering": "no",
        "X-Trace-ID": trace_id,
    }
)
```

**Gate:** MockProvider stream passes all finalizer cases. Real provider SSE streams correctly. Final full-string estimate is used only when provider usage is unavailable. Budget stops mid-stream. Reservation settles exactly once for success, timeout, error, disconnect, and budget cutoff. SSE headers correct. Client disconnect test does not leave stale reservation unhandled.

**Math (both days):** MML 4.1-4.2 eigenvalues (60 min) | MML 4.4 eigendecomposition (60 min)

---

### SPRINT DAY 10 (Jul 11): Rate Limiting (Lua) + Ledger Stats + Gateway Overhead

> Combined from original Days 12-13. Rate limiter is ~4 hrs, stats endpoints are ~3 hrs.

**Atomic Lua rate limiter:**
```lua
local key = KEYS[1]
local now = tonumber(ARGV[1])
local window = tonumber(ARGV[2])
local limit = tonumber(ARGV[3])
redis.call('ZREMRANGEBYSCORE', key, 0, now - window)
local count = redis.call('ZCARD', key)
if count < limit then
    redis.call('ZADD', key, now, now .. ':' .. math.random(1000000))
    redis.call('EXPIRE', key, window)
    return 1
end
return 0
```

**Gateway overhead metric from use case timing:**
```python
@router.post("/v1/chat/completions")
async def complete(
    request: CompletionRequest,
    principal: Principal = Depends(get_current_principal),
):
    total_start = time.perf_counter()
    response = await execute_completion.execute(request, principal)
    total_ms = (time.perf_counter() - total_start) * 1000
    gateway_overhead_ms = total_ms - response.provider_latency_ms
```

**Stats source:** `/stats` and `/stats/me` read from `usage_ledger` and `provider_attempts`, not from logs.

**Dependency wiring:** Replace the Day 7 permissive `RateLimiter` implementation with the Redis Lua implementation in the app dependency container. Do not leave the permissive limiter reachable from `/v1/chat/completions`.

**Gate:** Rate limiter is Lua-atomic. 100 concurrent requests -> no TOCTOU race. No route is still using the permissive limiter. /stats includes `gateway_overhead_ms`, total requests, total tokens, total cost, failover count. /stats/me scoped to tenant/API key.

**Math:** MML 4.5 SVD + MML 5.1-5.2 (60 min)

---

### SPRINT DAY 11 (Jul 12): Integration Testing + Error Handling + Graceful Shutdown

> Combined from original Days 14-15. The last "deep work" day.

**Error handling:** No raw exceptions reach the client. Standard JSON error format everywhere.

**Graceful shutdown handler:**
```python
from contextlib import asynccontextmanager

@asynccontextmanager
async def lifespan(app: FastAPI):
    event_task = asyncio.create_task(event_worker.drain())
    reconcile_task = asyncio.create_task(reservation_reconciler.run())
    yield
    logger.info("shutdown_initiated", message="Draining event worker...")
    event_task.cancel()
    reconcile_task.cancel()
    await event_worker.force_drain()
    logger.info("shutdown_complete", message="Event worker drained.")

app = FastAPI(lifespan=lifespan)
```

**Create the integration test harness and run the priority smoke matrix first.** The full matrix must be green by the Phase A final gate, but Day 11 itself cannot honestly absorb every scenario plus graceful shutdown wiring.

| Scenario | Expected |
|---|---|
| Happy path (non-streaming) | 200 |
| Happy path (streaming) | SSE completes |
| Budget exhaustion | 429 |
| Budget exhaustion mid-stream | Stream stops cleanly |
| Provider failure (Groq) | Failover to OpenAI |
| Provider returns empty 200 | Failover triggered; not counted as circuit outage |
| All providers down | 503 |
| Rate limit exceeded | 429 + Retry-After |
| Invalid API key | 401 |
| Missing API key | 401 |
| Postgres down before provider call | Request fails before provider spend |

**Remaining matrix for Days 12-13:**

| Scenario | Expected |
|---|---|
| Settlement when over-estimated | Reservation settled and unused amount released |
| Settlement when under-estimated | Ledger records actual usage and policy handles shortfall |
| Stale reservation reconciler | Expired reservation released/marked |
| Redis down (budget, demo policy) | Allowed only within emergency cap, flagged |
| Redis down (rate limit, demo policy) | Allowed, logged, flagged |
| PII in query | Stripped from logs |

**Gate:** Priority smoke matrix passes. Error format is consistent. Graceful shutdown drains event worker. One-shot stale reservation expiry exists. Full remaining matrix is queued for Days 12-13.

**Math:** MML 5.3 + `numerical_vs_analytical_deriv.py` (60 min)

---

### SPRINT DAY 12 (Jul 13): Pytest Start + Fix Everything

> **Case A:** If Sprint Day 1 was completed on Jul 2, this is the extra day created by starting early. Use it to get a head start on tests so less P1 bleeds into college.
>
> **Case B:** If execution started on Jul 3, this is no longer an extra day. Use it first to close any unfinished P0 foundation work, then start pytest only after the request path is stable.

**Session 1 (9:20-14:00):** Fix any failing priority smoke tests from Day 11. Then run the remaining matrix: over-estimate settlement, under-estimate settlement, stale reconciler, Redis down budget/rate behavior, and PII stripping. Fix accumulated tech debt.

**Session 2 (15:00-18:00):** Start writing pytest files:
- `test_auth.py` — valid key, invalid key, missing key, health skips auth
- `test_budget.py` — Lua atomicity, 100 concurrent reservations, settlement idempotency, stale reservation expiry
- `test_usage_ledger.py` — successful request, failed request, cache/future placeholder, provider usage vs estimated usage

**Session 3 (20:30-22:30):** Clean up code — imports, unused variables, type hints. Run full curl suite.

**Gate:**
- [ ] Day 11 priority smoke matrix still passes
- [ ] Remaining integration matrix passes or each failure is assigned to Day 13
- [ ] `test_auth.py` passes
- [ ] `test_budget.py` passes (including concurrent race + settlement tests)
- [ ] `test_usage_ledger.py` passes
- [ ] No broken imports, no unused variables

**Math:** MML 5.5-5.6 Backpropagation (60 min)

---

### SPRINT DAY 13 (Jul 14): Sprint Buffer + Retrospective

> **Last full-time day before college.** Fix everything. This is your safety net.

**Priority order:**
1. Fix any failing tests from Days 11-12
2. Finish any remaining integration matrix failures
3. Write `test_routing_engine.py` and `test_execute_completion.py`
4. Clean up code one more time
5. Run the full curl test suite + pytest suite
6. Write sprint retrospective in decisions.md

> [!IMPORTANT]
> **Sprint Phase A ends here.** By end of Jul 14, ALL P0 features must work. If streaming is still broken, this day is for fixing it — not for new features.

**Gate (Phase A Final):**
- [ ] Auth works end-to-end
- [ ] Both providers return normalized `ProviderResult`
- [ ] Budget Lua is atomic (concurrent reservation test passes)
- [ ] Budget settlement is idempotent and writes `usage_ledger`
- [ ] Circuit breaker trips and recovers
- [ ] Content quality check catches empty responses without poisoning circuit health
- [ ] Non-streaming completions work E2E
- [ ] Streaming completions work with finalizer on success/error/timeout
- [ ] Rate limiter is Lua-atomic
- [ ] PII is stripped from all logs
- [ ] Event drain failure does not lose usage ledger truth
- [ ] Health/stats endpoints work
- [ ] Graceful shutdown drains event worker
- [ ] Gateway overhead is measured and logged
- [ ] All integration test scenarios pass
- [ ] `test_auth.py`, `test_budget.py`, `test_usage_ledger.py`, `test_routing_engine.py`, `test_execute_completion.py` pass

**Math:** MML 5.7 Higher-order derivatives (60 min)

---

## PHASE B — COLLEGE MODE (Jul 15–31)

> [!IMPORTANT]
> **All P0 features are DONE.** Phase B is exclusively P1 work: tests, CI, observability, deployment, documentation, and dogfooding. These tasks are modular — they work in 1-3 hour reading room chunks.

### WHAT GOES WHERE

| Task | Where To Do It | Why |
|---|---|---|
| Writing pytest test files | Reading room (laptop) | Modular, each use case or adapter test is independent |
| GitHub Actions CI config | Reading room (laptop) | YAML config, no provider calls needed |
| Langfuse integration | Night session (home) | Needs running Docker stack |
| Docker production build | Weekend (home) | Needs Docker, long builds |
| Nginx config | Weekend (home) | Needs Docker stack |
| Chaos testing | Weekend (home) | Needs full running system |
| README writing | Reading room (laptop) | Pure writing, perfect for reading room |
| Demo recording | Weekend (home) | Needs full stack + screen recorder |
| Dogfooding | Both | Route study questions anytime |
| Railway deploy | Weekend (home) | Needs CLI + secrets |

---

### WEEK B1: Jul 15-19 (Wed-Fri + Weekend)

**Weekdays (Jul 15-17) — Reading Room: Remaining Pytest + CI**

> [!TIP]
> `test_auth.py`, `test_budget.py`, `test_usage_ledger.py`, `test_routing_engine.py`, and `test_execute_completion.py` are already done from Sprint Days 12-13. Focus on the remaining test files.

| Day | Reading Room (3-5 hrs) | Night Session (1.5 hrs) |
|---|---|---|
| Jul 15 (Wed) | `test_stream_completion.py` + `test_sanitizer.py` | Fix any test issues |
| Jul 16 (Thu) | Provider contract tests + `test_rate_limiter.py` (Lua atomicity) | Start Langfuse integration behind config: no-op when disabled, real trace when enabled |
| Jul 17 (Fri) | GitHub Actions CI: lint -> type check -> pytest -> Docker build | Debug CI if red |

**Math (all weeknights):** MML 6.2-6.3 (distributions, Bayes) + StatQuest (60 min/night)

**Weekend (Jul 18-19) — Full Days: Langfuse + Docker + Nginx**

| Day | Session 1 (9:30-14:00) | Session 2 (16:00-18:00) | Session 3 (20:30-22:30) |
|---|---|---|---|
| Jul 18 (Sat) | Docker multi-stage build + production compose | Nginx config + `proxy_buffering off` for SSE | Verify Langfuse traces for completions, streaming, failover, and budget rejection |
| Jul 19 (Sun) | Run priority chaos tests first | Fix failures from priority chaos | Buffer or optional Locust p99 run |

**Priority chaos tests for Jul 19:**

| Test | Expected Result |
|---|---|
| Redis down mid-request | Behavior matches configured demo policy and is flagged |
| Provider returns empty 200 OK | Failover triggered; circuit health not poisoned |
| SSE through Nginx | Stream works correctly |
| Postgres down before request | Request fails before provider spend |
| Client disconnect during stream | Reservation finalized or reconciler releases it |

**Secondary chaos tests:** Run these on Jul 19 only if priority chaos passes early. Otherwise run them on Jul 29 final-review day.

| Test | Expected Result |
|---|---|
| 1000 concurrent requests (Locust) | Pool holds, gateway overhead p99 < 35ms |
| Kill app mid-event-drain | Usage ledger already contains truth; event worker drains/retries |

**Math (weekend):** Entropy + Cross-Entropy (StatQuest) (90 min/day — full sessions on weekends)

---

### WEEK B2: Jul 20-26 (Mon-Fri + Weekend)

**Weekdays (Jul 20-24) — Reading Room: README + Dogfood Start**

| Day | Reading Room (3-5 hrs) | Night Session (1.5 hrs) |
|---|---|---|
| Jul 20 (Mon) | README draft — sections 1-5 (what, why, architecture, how to run, API reference) | Review + polish |
| Jul 21 (Tue) | README draft — sections 6-11 (decisions, chaos results, security, "what I'd change") | Review + polish |
| Jul 22 (Wed) | Architecture diagram (Excalidraw). Quick Start for Forkers. | Dogfood: route 10 study questions through Gateway |
| Jul 23 (Thu) | Dogfood: route 15 study questions through Gateway during reading room | Review Langfuse traces |
| Jul 24 (Fri) | Dogfood: route 15 study questions. Collect metrics. Prepare Railway env/secrets checklist. | Analyze dogfood data + dry-run deploy configuration |

**Math (weeknights):** MML 8.1-8.3 (ERM, MLE/MAP) + StatQuest MLE (60 min/night)

**Weekend (Jul 25-26) — Full Days: Demo + Deploy**

| Day | Session 1 (9:30-14:00) | Session 2 (16:00-18:00) | Session 3 (20:30-22:30) |
|---|---|---|---|
| Jul 25 (Sat) | Demo recording: 5 concurrent requests, failover, mid-stream budget cutoff | Update README with dogfood metrics | Fix any remaining issues |
| Jul 26 (Sun) | Deploy to Railway using pre-checked env vars/secrets. Verify health, auth failure, success completion, streaming, and stats from live URL. | Write `decisions.md` summary for the full sprint | **REDUCED LOAD** — rest, review, prepare for college week |

**Math (weekend):** KL divergence + `entropy_crossentropy_kl.py` (90 min/day)

---

### WEEK B3: Jul 27-31 (Mon-Fri, final stretch)

**Remaining weekdays — wrap up:**

| Day | Reading Room (3-5 hrs) | Night Session (1.5 hrs) |
|---|---|---|
| Jul 27 (Mon) | Final dogfooding: 10 more queries, edge cases | Update README metrics section |
| Jul 28 (Tue) | Polish README. Verify Railway deployment is live. | Blog post #3 outline (Redis race condition — can write from July experience) |
| Jul 29 (Wed) | Final review: all tests pass, CI green, Langfuse traces look right. Run any deferred secondary chaos tests. | Buffer |
| Jul 30 (Thu) | Buffer day — catch up on anything behind | Buffer |
| Jul 31 (Fri) | **SHIP DAY.** Final smoke test. Share link. Done. | 🎉 |

> [!TIP]
> Do NOT start Layer 2 during buffer days. Do NOT skip dogfooding. Real data beats synthetic data, and showing up rested beats grinding half-asleep.

---

<!-- Older full-time Week 1-4 content removed in V4. V5 keeps the Phase A/Phase B schedule and corrects architecture inside it. -->

---

## MATH REFRESHER INTEGRATION

The math plan runs **July 2 to August 2** at adapted intensity.

### Phase-Specific Rules

1. **Phase A (Sprint, Jul 2-14):** Math compressed to 1 hr/day (from 1.5). Done at 22:30-23:30. Gateway ALWAYS takes priority — if build runs over, math slides.
2. **Phase B Weekdays (Jul 15-31):** Math at 1 hr/day (22:30-23:30). Build in reading room and night sessions takes priority.
3. **Phase B Weekends:** Math at 1.5 hrs/day (full session). Catch up any deficit from weekdays.
4. **Hard rule:** No build goal met = no math that day. This hasn't changed.
5. **V5.2 schedule rule:** Gap tests on Sundays only (Jul 13, Jul 19, Jul 26). Jul 13 is a lighter pytest day only in Case A. In Case B, Jul 13 may become P0 catch-up, so math slides behind the Gateway.

### Math Topics Mapped to Sprint Days

| Sprint Day | Math Topic | Time | Connection |
|---|---|---|---|
| Day 1 (Jul 2) | 3B1B LA ch.1-3, MML 2.1-2.2 | 60 min | Vectors = embeddings |
| Day 4 (Jul 5) | MML 2.6, 3B1B ch.7-9 | 60 min | Basis/Rank |
| Day 6 (Jul 7) | MML 3.1-3.4 cosine similarity | 60 min | **Direct Layer 2 math** |
| Day 10 (Jul 11) | MML 4.5 SVD + 5.1-5.2 | 60 min | **SVD -> LoRA** |
| Day 13 (Jul 14) | MML 5.7 Higher-order derivatives | 60 min | **September foundation** |
| Week B1 | Distributions, Bayes, Entropy, CE | 60-90 min | **CE = Layer 3 loss** |
| Week B2 | ERM, MLE/MAP, KL divergence | 60-90 min | **KL -> RLHF penalty** |

---

## GATES AND CHECKPOINTS SUMMARY

| Checkpoint | Date | Must Be True |
|---|---|---|
| Sprint Midpoint | Jul 8 | Auth works. Both providers work. Budget reservation is atomic. `usage_ledger` can settle. Circuit breaker works. Content validation catches empty responses without poisoning circuit health. trace_id flows. Structured logging works. |
| Sprint End (Phase A) | Jul 14 | ALL P0 features work E2E. Integration scenarios pass. Streaming finalizer works. Rate limiter is Lua-atomic. PII stripped. Usage ledger is durable. Event drain is non-billing. Graceful shutdown works. Gateway overhead measured. Core pytest files pass. |
| College Week 1 | Jul 19 | Pytest suite passes. CI green. Langfuse traces visible with gateway_overhead_ms/request_id/provider_attempt_id. Docker + Nginx work. Priority chaos tests documented. Secondary chaos/Locust may defer to Jul 29. |
| College Week 2 | Jul 26 | README complete (honest positioning + security). Demo recorded. Dogfood data collected (50+ real queries). Deployed on Railway. |
| Ship | Jul 31 | Everything polished. Railway live. Final smoke test passes. |

---

## RISK REGISTER AND MITIGATION

| Risk | Prob | Impact | Mitigation |
|---|---|---|---|
| Streaming (Days 8-9) takes 3+ days | 40% | Medium | Sprint Days 12-13 absorb overflow. If still broken, Jul 18-19 weekend becomes emergency fix. |
| Groq API rate limits during dev | 20% | Low | Use mock providers for most testing. |
| Docker issues on Windows | 30% | Medium | Use WSL2. Test Docker early (Day 1). |
| Redis connection pool exhaustion | 15% | Medium | Set explicit max_connections. |
| Postgres ledger write failure | 15% | High | Create request/reservation before provider spend. If ledger unavailable, fail before provider call. |
| Stale budget reservations | 25% | High | Build `reservation_reconciler.py` and test timeout/cancel paths. |
| PII regex catches too much / too little | 20% | Low | Start conservative. Test with known PII patterns. |
| Content quality check false positives | 15% | Low | Only reject truly empty/whitespace responses. Record as invalid output, not provider outage. |
| Architecture overbuild slows sprint | 20% | Medium | Implement minimal tables/ports only. No cache, no admin UI, no advanced policy engine in July. |
| pytest-asyncio version conflicts | 30% | Low | Pin versions in pyproject.toml. |
| Math plan falls behind | 50% | Low | Math slides. Gateway never slides for math. |
| **No time for dogfooding** | 30% | **High** | Even 1 day of real queries is better than zero. Prioritize over polish. |
| **College schedule unpredictable** | 40% | Medium | Reading room tasks are modular — any can be done in any order. Weekend catches overflow. |
| **Reading room has no internet** | 20% | Medium | Write tests offline. Git push from home. Keep provider mocks local. |
| **Sprint Phase A P0 not complete by Jul 14** | 25% | **Critical** | Jul 18-19 weekend becomes emergency P0 completion. Delay P1 by 1 week. |

---

## DOGFOODING PROTOCOL

> [!IMPORTANT]
> **Real data from 2 days of self-usage is worth more than all synthetic benchmarks combined.**

### What To Do (Jul 22-28, during college)

1. Route your AI-assisted study questions through the Gateway during college reading room sessions and at night
2. Ask at least 50 diverse queries — coding questions, math explanations, concept comparisons
3. Include some deliberately hard queries and some trivially simple ones
4. Include queries with emails/phone numbers embedded — verify PII sanitization works
5. **Advantage of college mode:** your real study questions ARE the real traffic. You don't need to simulate usage — you ARE a user.

### What To Measure and Report

| Metric | How To Get It |
|---|---|
| Total requests processed | GET /stats |
| Gateway overhead p99 | GET /stats |
| Usage ledger rows | GET /stats or direct SQL count |
| Provider attempt count | GET /stats/providers |
| Cache hit rate (future, January) | N/A for July |
| Provider failover events | Langfuse traces |
| Budget consumption pattern | GET /stats/me |
| PII sanitization events | grep "REDACTED" in logs |
| Errors / failures | Langfuse error traces |

### What To Write In The README

```
## Real Usage Metrics (2-day dogfooding session)

- 73 real queries processed
- 73 usage ledger rows settled
- 76 provider attempts recorded (3 failovers)
- Gateway overhead: 14ms p50 / 19ms p95 / 23ms p99
- Provider failover triggered: 2 times (Groq timeout)
- Content quality check caught: 1 empty response from Groq
- PII sanitization: 4 emails redacted from logged queries
- Zero unhandled exceptions
```

Even these modest numbers are **infinitely more credible** than "simulated 40% cache hit rate on synthetic data."

---

## SECURITY CONSIDERATIONS

> [!IMPORTANT]
> You don't need a full security layer. But you need to be able to **talk about security** in interviews.

### What July Implements

| Feature | July Target | Where |
|---|---|---|
| API key hashing + prefix lookup | Build in July | Sprint Day 2 (Jul 3) |
| Tenant-scoped API keys | Build in July | Sprint Day 2 (Jul 3) |
| Durable usage ledger | Build in July | Sprint Days 2, 4, 7 |
| PII sanitization before logging | Build in July | Sprint Day 7 (Jul 8) |
| Explicit Redis failure policy | Build in July | Sprint Days 4, 6, 10 |
| Structured audit logging | Build in July | Sprint Day 1 (Jul 2) |
| Event drain separate from billing truth | Build in July | Sprint Day 7 (Jul 8) |

### What You'd Add In Production (Know This For Interviews)

| Feature | Why | Interview Answer |
|---|---|---|
| API key rotation | Compromised keys need instant revocation | "I'd add a POST /keys/rotate endpoint that generates a new key, returns it once, and revokes the old one after a grace period" |
| Request/response encryption at rest | Compliance (GDPR, SOC2) | "Logs are already PII-sanitized. For full compliance, I'd encrypt the Postgres volume and use Doppler for secrets" |
| Rate limiting by IP (not just API key) | Prevents key-sharing abuse | "I'd add IP-based rate limiting as a second layer alongside the per-user Lua limiter" |
| Prompt injection detection | Prevent malicious inputs | "I'd add a lightweight heuristic check before the provider call — known injection patterns — with Guardrails AI for production" |
| Secret/key pepper rotation | Defense if DB leaks | "API keys are hashed; production would add a server-side pepper and rotation plan" |

---

## HORIZONTAL SCALING NOTES

> You don't need to implement this in July. But you need to **explain it in interviews.**

### How Would You Scale to 10K req/sec?

```
                     Load Balancer (Nginx / AWS ALB)
                    /          |          \
            Gateway-1    Gateway-2    Gateway-3
                 \          |          /
                  Redis Cluster (6 nodes)
                       |
                 Postgres (with read replicas)
```

**What changes:**
1. **Multiple Gateway instances** behind a load balancer — request-time state is externalized
2. **Postgres primary** remains source of truth for tenants, API keys, reservations, usage ledger, and provider attempts
3. **Redis Cluster** handles rate limits, circuit state, reservation counters, and event streams
4. **Shard Redis keys by tenant/API key** so Lua scripts remain single-shard atomic
5. **Postgres read replicas** serve /stats queries later; writes still go to primary
6. **Connection pool per instance** — calculate total connections across all app workers
7. **Workers scale separately** for event drains and stale reservation reconciliation

**Interview answer:** *"The Gateway is horizontally scalable because request-time coordination is in Redis and durable truth is in Postgres. Redis Lua remains atomic when keys are shard-local by tenant/API key. The app instances stay stateless with respect to in-flight policy decisions, while Postgres keeps the audit and usage ledger."*

---

## BLOG STRATEGY

> [!WARNING]
> **This is a 12-month content plan, not a July deliverable.** Each post has a target month — the month its prerequisite feature is built. Do NOT feel pressure to write any of these during the July Gateway sprint. They are tracked here because the Gateway plan feeds into them, not because they ship with it.

| Priority | Post | Target Month | Prerequisite | Target Audience |
|---|---|---|---|---|
| **#1 (must-ship)** | "How semantic caching cut my LLM API bill by 40%" | **January** (after semantic cache is built) | Semantic cache feature (January Week 1) | Every developer paying LLM bills |
| **#2 (must-ship)** | "I built Reflexion from scratch: LLMs that improve without fine-tuning" | **January** (after Reflexion sprint) | Reflexion loop (December Week 3) | AI engineers with eval pipelines |
| **#3 (must-ship)** | "Why Redis GET + Python check + INCRBY is a race condition" | **August** (Gateway is shipped, write from experience) | Gateway Layer 1 (July) | Backend engineers using Redis |
| #4 (optional) | "I added BM25 to my vector search — here's what changed" | **November** (after Layer 2) | Layer 2 hybrid search (October) | RAG builders |

**Rule:** Post #2 (Reflexion) must be live **before** the first outreach email is sent. The email references the quality curve — it must be at a real URL.

> [!NOTE]
> Only Post #3 (Redis race condition) can be written from July experience. The rest require features that don't exist yet. Don't confuse "must-ship" priority with "ship in July."

---

## CHANGELOG

### What V5.3 Fixed (per-day hour-fit audit pass)

| # | Issue | Severity | What Changed |
|---|---|---|---|
| 55 | **Total-hour math existed, but daily task-hour fit was not audited** | P0 | Added a per-day fit table for sprint days, college weekdays, and weekends using conservative capacity: 9.5h sprint, 5.5h college weekday, 8.5h weekend. |
| 56 | **Day 3 overloaded provider adapters with real streaming too early** | P0 | Day 3 now builds real non-streaming adapters, provider contract shape, and `MockProvider`. Real provider streaming is explicitly Days 8-9. |
| 57 | **Day 4 overloaded budgeting with full background reconciliation** | P0 | Day 4 now builds reservation/settlement/ledger and a one-shot stale expiry function. Background worker lifecycle wiring moves to Day 11. |
| 58 | **Day 11 overloaded all integration scenarios plus graceful shutdown** | P0 | Day 11 now runs priority smoke tests and infrastructure wiring; the remaining integration matrix finishes across Days 12-13. |
| 59 | **Jul 19 overloaded all chaos tests plus Locust and fixes** | P1 | Jul 19 now runs priority chaos first. Secondary chaos and Locust can defer to Jul 29 without cutting the feature. |

### What V5.2 Fixed (hour-math audit pass)

| # | Issue | Severity | What Changed |
|---|---|---|---|
| 52 | **229 build hours were treated as still available even after Jul 2** | P0 | Added two capacity cases: 229 hrs if Sprint Day 1 was completed on Jul 2, and 219.5 hrs if execution starts from Jul 3. |
| 53 | **Sprint feasibility claim ignored the lost-start-day case** | P0 | Added the corrected Jul 3 sprint math: 12 sprint days × 9.5 hrs = 114 hrs, still +36% over the original 84-hour early-plan budget but with less buffer. |
| 54 | **Buffers were not reclassified after losing a sprint day** | P1 | Clarified that Sprint Day 5 and Sprint Day 13 become protected catch-up capacity if Day 1 was not completed. |

### What V5.1 Fixed (full-feature schedule audit pass)

| # | Issue | Severity | What Changed |
|---|---|---|---|
| 46 | **Day 7 referenced rate limiting before Lua limiter existed** | P0 | Day 7 now wires a permissive `RateLimiter` port only; Day 10 explicitly replaces it with Redis Lua and verifies no route still uses the permissive limiter. |
| 47 | **CI expected Docker build before Dockerfile was scheduled** | P0 | Day 1 now creates a minimal Dockerfile and app service in Compose; Jul 18 upgrades this into production Docker and Nginx. |
| 48 | **Provider and streaming tests lacked deterministic provider behavior** | P0 | Day 3 now creates `MockProvider` with success, timeout, error, empty-output, and streaming-delta modes. Days 8-9 prove stream finalization on `MockProvider` before real SDK streaming. |
| 49 | **Budgeting lacked an explicit token-estimation boundary** | P1 | Day 4 now adds `ModelCatalog`, `TokenEstimator`, and `BudgetAuthorizer` before Redis reservation logic. Provider actual usage wins at settlement when available. |
| 50 | **Jul 18 overloaded Langfuse, Docker, and Nginx from scratch** | P1 | Langfuse starts on Jul 16 night behind config; Jul 18 focuses on Docker/Nginx plus trace verification. |
| 51 | **Railway deployment prep happened on deploy day** | P2 | Jul 24 now prepares env vars/secrets and dry-runs deploy configuration before Jul 26 deployment. |

### What V5 Fixed (architecture correction pass)

| # | Issue | Severity | What Changed |
|---|---|---|---|
| 36 | **Technical-folder architecture caused unclear ownership** | P0 | Added strict ownership boundaries: `api`, `application/use_cases`, `domain`, `infrastructure`, `workers`. Routes are thin; use cases own lifecycle. |
| 37 | **Request logging was carrying billing responsibility** | P0 | Added durable `usage_ledger`, `provider_attempts`, and `budget_reservations`. Logs/events are observability only. |
| 38 | **Redis was treated as financial truth** | P0 | Redis remains fast coordination; Postgres ledger/reservations become durable truth. |
| 39 | **Redis failure policy was always fail-open** | P0 | Replaced with explicit environment policy: local/demo can fail-open with cap; paid/prod should fail-closed for budget/rate controls. |
| 40 | **Provider interface was too thin** | P1 | Provider adapters now expose metadata, normalized results, normalized stream events, and error taxonomy. |
| 41 | **Content validation poisoned circuit breaker state** | P1 | Empty/invalid output triggers failover but is recorded separately from provider outage. |
| 42 | **Streaming cleanup lived inside generator logic** | P0 | Added `StreamCompletion` finalizer and stale reservation reconciler requirement. |
| 43 | **Stats/logs were not tied to durable records** | P1 | `/stats` now reads from `usage_ledger` and `provider_attempts`. |
| 44 | **Day 2 schema was too small for production growth** | P0 | Replaced `User/Budget/RequestLog` with tenant/API key/reservation/request/attempt/ledger foundation. |
| 45 | **Phase B tests used old module boundaries** | P2 | Updated tests to use-case, ledger, provider contract, stream finalization, and routing-engine tests. |

### What V2 Fixed (from the honest review)

| # | Issue | What Changed |
|---|---|---|
| 1 | **LiteLLM positioning** | Reframed from "competing with LiteLLM" to "built Layer 1 to learn, Layers 2-4 are the differentiator" |
| 2 | **No real traffic data** | Added Dogfooding Protocol (Days 26-27) — 50+ real queries, report real metrics |
| 3 | **Rate limiter TOCTOU race** | Replaced pipeline-based rate limiter with atomic Lua script |
| 4 | **Stream error handling missing** | Added try/except with proper error SSE events and trace_id logging |
| 5 | **Budget reconciliation guard missing** | V2 added post-stream reconciliation; V5 replaces this with reservation settlement + idempotent finalizer |
| 6 | **Log drain idempotency** | V2 added a DLQ idea; V5 replaces billing logs with durable `usage_ledger` and non-billing event drain |
| 7 | **SSE headers incomplete** | Added `Cache-Control`, `Connection`, `X-Accel-Buffering` headers |
| 8 | **No PII sanitization** | Added `sanitizer.py` with email/phone/SSN regex stripping |
| 9 | **No content quality check** | Added `quality_check.py` — catches "200 OK but garbage" responses |
| 10 | **print() instead of structured logging** | Added structlog from Day 1 with JSON output |
| 11 | **No graceful shutdown** | Added lifespan handler; V5 drains event worker and stops reconciler cleanly |
| 12 | **Misleading latency claims** | Replaced "p99 < 200ms" with "gateway overhead p99" — honest metric |
| 13 | **No decisions log** | Added `decisions.md` from Day 1 as ongoing practice |
| 14 | **No preflight script** | Added `scripts/preflight.py` for environment verification |
| 15 | **No security section** | Added Security Considerations with interview answers |
| 16 | **No scaling design** | Added Horizontal Scaling Notes with interview-ready architecture |
| 17 | **Wrong blog order** | Semantic caching moved to #1, BM25 moved to optional #4 |
| 18 | **Math gap test conflict** | Moved gap tests to Sundays to avoid build-day conflicts |
| 19 | **4 chaos tests** | Expanded to 6 chaos tests (added shutdown drain + Postgres-down DLQ) |
| 20 | **12 integration test scenarios** | Expanded scenarios; V5 further adds ledger, provider-attempt, and reservation-finalizer checks |

### What V3 Fixed (from red-team review)

| # | Issue | Severity | What Changed |
|---|---|---|---|
| 21 | **Schedule contradicted locked routine** | P0 | Rewrote daily template: 9:30 wake (not 8:00), DSA at 15:00-16:00 (1 hr, not 30 min), gym at 18:00-19:30 (was absent entirely), math at 20:30-22:00 (was 17:30-19:00, overlapping gym). Build drops from fictional 7h to honest 6h. Total productive hours recalculated from 9.5h to 8.75h. |
| 22 | **Zero rest days across 29 days** | P0 | Day 25 is now an explicit reduced-load recovery day — DSA only, no build sessions. Absorbs illness/burnout without cascading slips. |
| 23 | **Token reconciliation drift bug** | P1 | Replaced running per-chunk `tiktoken.encode()` sum with single full-string encoding at stream end. Per-chunk encoding overcounts because BPE merges across chunk boundaries can't happen when fragments are encoded independently. Provider tokenizer (Llama) ≠ cl100k_base adds further systematic bias. Gate tightened from ±5% to ±2%. |
| 24 | **Blog posts had no target months** | P2 | Added target month + prerequisite feature to each post. Clarified this is a 12-month content plan, not a July deliverable. Only Post #3 (Redis race condition) can be written from July experience. |
| 25 | **Arithmetic error in schedule** | P0 | Previous template claimed 9.5h productive but actually summed to 11h when all stated blocks were added. V3 arithmetic is verified: 6 + 1 + 1.5 + 0.25 = 8.75h. |

### What V4 Fixed (college schedule adaptation)

| # | Issue | What Changed |
|---|---|---|
| 26 | **29-day full-time assumption was wrong** | Split into Phase A (13-day sprint, Jul 2-14) + Phase B (17-day college mode, Jul 15-31) |
| 27 | **Schedule didn't account for college 9-6:30** | Added 3 schedule modes: Sprint, College Weekday, Weekend |
| 28 | **P0 features scattered across 21 days** | All P0 features front-loaded into 13 sprint days. P1 polish moved to college mode |
| 29 | **No reading room task guidance** | Added "What Goes Where" table mapping tasks to reading room vs home vs weekend |
| 30 | **Original 15 plan-days compressed to 13** | Merged simple consecutive days (DB+Auth, Groq+OpenAI, RateLimiter+Stats) using 9.5 hrs/day. Extra day (13th) used for early pytest |
| 31 | **Dogfooding was standalone** | Dogfooding now uses real study questions during college — natural traffic, not simulated |
| 32 | **Math compressed during sprint** | 1 hr/day during sprint (from 1.5), full 1.5 hr on weekends to catch up |
| 33 | **Total build hours verified** | 123.5 (sprint) + 71.5 (weekdays) + 34 (weekends) = 229 hrs if Jul 2 is completed. V5.2 adds the Jul 3-start correction: 219.5 remaining hrs. |
| 34 | **Old V3 Week 1-4 content was duplicate** | Removed 550 lines of superseded schedule. Phase A/Phase B is now the single source of truth |
| 35 | **Phase B weekday day-of-week labels wrong** | Fixed: Jul 15 = Wed (not Tue), weekends = Jul 18-19 & Jul 25-26 (Sat-Sun) |

---

*Generated from Master Blueprint V12, Math Refresher Plan v3, Honest Project Review, and Red-team Review.*
*V2: All review findings integrated as executable plan changes.*
*V3: Red-team findings fixed — schedule synced to locked routine, rest day added, token drift bug killed, blog scope labeled.*
*V4: Restructured for college start Jul 15. Two-phase plan: Sprint (Jul 2-14, 13 days) + College Mode (Jul 15-31). 229 total planned build hours if Jul 2 is completed.*
*V5: Architecture correction pass — same stack and schedule, fixed ownership boundaries, ledger-first budgeting, provider metadata, stream finalization, and Redis failure policy.*
*V5.2: Hour-math audit — 229 planned hours if Day 1 was completed on Jul 2; 219.5 remaining hours if execution starts on Jul 3.*
*V5.3: Per-day hour-fit audit — corrected overloaded Day 3, Day 4, Day 11, and Jul 19 without cutting features.*
*Last updated: July 3, 2026*

---

> **Execute.** 🚀
