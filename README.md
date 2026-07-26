# LLM-Gateway

## Current capabilities

- Hexagonal architecture with multi-provider routing and fallback
- PostgreSQL-authoritative budget periods, reservations, and usage ledger
- Attempt-scoped accounting for successful, failed, invalid, and uncertain calls
- Atomic tenant and API-key rate limiting with Redis Lua
- Redis-backed, best-effort circuit breaker
- Deterministic streaming with idle/total timeouts and durable finalization
- Reservation reconciliation and graceful shutdown
- Structured logging, trace IDs, API-key authentication, and usage stats

Redis remains coordination infrastructure for rate limiting and provider health.
It is not a financial source of truth.
