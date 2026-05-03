# 🚀 TCP Connection Pool — Sprint Plan
## PgBouncer from Scratch (pgwire Protocol)

**Duration:** 2 Weeks (Weeks 3–4)  
**Main Goal:** Build a high-performance TCP proxy that can multiplex 10,000 application connections over a small pool of PostgreSQL database connections using the `pgwire` protocol.

---

## 🏗️ Architecture Overview

| Component | Responsibility | Status |
|---|---|---|
| **TCP Listener** | Accepts connections from client applications on port 5433 | ⏳ Pending |
| **pgwire Parser** | Decodes PostgreSQL wire protocol messages (Startup, Query, Sync) | ⏳ Pending |
| **Connection Pool** | Maintains a heartbeat-guarded pool of N real DB connections | ⏳ Pending |
| **Session Router** | Handles multiplexing logic (Transaction vs Session mode) | ⏳ Pending |
| **Health Checker** | Periodically pings idle backend connections | ⏳ Pending |

---

## 📅 Sprint Breakdown

### [Sprint 1: Core Proxy & Protocol Foundations](./Sprint_01_Core_Proxy.md)
*Focus: Getting the data flowing between the app and the DB.*
- TCP Listener setup.
- Basic `pgwire` startup handshake.
- Simple session-mode pooling (one client = one backend connection).
- Basic query forwarding.

### [Sprint 2: Transaction Mode & Performance](./Sprint_02_Transaction_Mode.md)
*Focus: Efficiency, multiplexing, and benchmarking.*
- Transaction boundary detection (`BEGIN`/`COMMIT`).
- Full Transaction Mode (returning connections on commit).
- Health checking and auto-recovery.
- Benchmarking (10k clients vs 9 DB connections).

---

## 🚦 Phase Definitions

### Phase 1: The Handshake
Parsing the `StartupMessage` is the first hurdle. We need to handle authentication (likely bypassed for local dev) and send back the correct response frames to make the client think they are talking to a real PostgreSQL instance.

### Phase 2: The Pool
Building the `Acquire()` and `Release()` logic. Use channels or a mutex-guarded slice to manage backend connections. Implement backpressure: if the pool is full, callers should block until a connection is available or a timeout occurs.

### Phase 3: Transaction Boundaries
The "Magic" of PgBouncer. We must scan the `Query` message contents for tokens like `BEGIN`, `COMMIT`, and `ROLLBACK` to know when a connection can be safely returned to the pool without breaking session state.

---

## 📈 Success Metrics
- **Stability:** Pass `go test -race ./...`.
- **Performance:** Handle 1,000 concurrent clients with only 10 backend connections.
- **Latency:** Overhead added by the proxy should be < 1ms for p99.
