# ⚡ Sprint 2: Transaction Mode & Performance
**Week 4 | Focus: Multiplexing & Hardening**

## 🎯 Objectives
Transform the basic proxy into a space-efficient multiplexer. This sprint implements the "PgBouncer magic" where connections are shared efficiently across transactions.

## 📝 Task List

### 1. Transaction Boundary Detection
- [ ] Peek into `Query` messages to check for `BEGIN`, `COMMIT`, `ROLLBACK`.
- [ ] Track session state: `InTransaction` vs `Idle`.
- [ ] Handle multi-statement queries if applicable.

### 2. Transaction Mode Implementation
- [ ] Update `Release()` logic: If NOT in a transaction, return the backend connection to the pool immediately after the query result is sent.
- [ ] Implement a wait queue: If all connections are busy, incoming queries should block until one is released.

### 3. Health & Reliability
- [ ] **Health Checker:** Implement a background goroutine that pings (`SELECT 1`) idle connections every 30 seconds.
- [ ] Auto-reconnect: If a backend connection drops, transparently replace it in the pool.
- [ ] Configuration: Allow toggling between `session` and `transaction` mode via `config.yaml`.

### 4. Benchmarking & Documentation
- [ ] Use `k6` or a custom Go benchmark test to compare raw PG vs Proxy.
- [ ] Measure p99 latency overhead.
- [ ] Document the "Pool Sizing Formula" in the project README.

## 🧪 Definition of Done
- 100 concurrent `psql` clients can run queries against a pool of only 5 backend connections.
- `go test -race ./...` passes under load.
- `BENCHMARKS.md` populated with improvement data.

---

## 🔥 Interview Prep Quick-Fire
- **Why use Transaction Mode?** Because most HTTP requests only need a DB connection for a few milliseconds of a 100ms request. Sharing it increases density by 10x-100x.
- **When is Transaction Mode bad?** When the app uses `SET` commands, prepared statements, or temporary tables that rely on session-level persistence.
