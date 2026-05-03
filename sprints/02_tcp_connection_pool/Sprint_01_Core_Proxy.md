# 🛠️ Sprint 1: Core Proxy & Protocol Foundations
**Week 3 | Focus: Connectivity**

## 🎯 Objectives
Build the foundation of the proxy. By the end of this week, a standard PostgreSQL client (like `psql` or `pgx`) should be able to connect to our proxy and execute simple `SELECT 1` queries.

## 📝 Task List

### 1. Project Infrastructure
- [ ] Initialize Go module `github.com/yourusername/tcp-conn-pool`.
- [ ] Define internal packages: `pkg/pool`, `pkg/pgwire`, `pkg/proxy`.
- [ ] Implement a basic TCP listener on `localhost:5433`.

### 2. Startup Handshake
- [ ] Read and parse `StartupMessage`.
- [ ] Implement authentication bypass (respond with `AuthenticationOk`).
- [ ] Forward backend parameter statuses (e.g., `server_version`) from the real DB to the client.

### 3. Session Mode Pooling (MVP)
- [ ] Implement `BackendConn` struct to hold a `net.Conn` to the real PostgreSQL server.
- [ ] Create a `Pool` that manages a fixed number of `BackendConn` instances.
- [ ] **Session Mode:** When a client connects, assign them a dedicated backend connection for the duration of their session.

### 4. Basic Query Forwarding
- [ ] Parse the `Query` message type ('Q').
- [ ] Transparently forward all data from Client -> Backend and Backend -> Client.
- [ ] Ensure the client receives `ReadyForQuery` ('Z') after each interaction.

## 🧪 Definition of Done
- `psql -h localhost -p 5433 -U postgres` can connect and run `SELECT 1;`.
- Multiple concurrent `psql` sessions work (up to the pool size).
- Basic error handling for lost backend connections.

---

## 💡 Implementation Tip
PostgreSQL messages follow a `Type (1 byte) | Length (4 bytes) | Payload` format. Always read the length first to know how many more bytes to pull from the buffer.
