# Biweekly Project 2 — TCP Connection Pool
## PgBouncer from Scratch (pgwire Protocol)

**Timeline:** Weeks 3–4  
**Language:** Go  
**What it mirrors:** PgBouncer · HikariCP · pgx connection pool internals  

---

## 1. What This Teaches

Why connection pools exist, how they work at the TCP level, and why PgBouncer's transaction mode is the correct choice for HTTP APIs. Most engineers configure PgBouncer without understanding what it actually does. After this project, you understand exactly why 10,000 application connections can be multiplexed over 9 database connections.

---

## 2. The Problem It Solves

PostgreSQL spawns one OS process per connection. At 10,000 connections, the database is spending more CPU on process scheduling than on queries. A connection pool sits between the application and PostgreSQL: applications think they have their own connection, but the pool multiplexes them over a small number of real backend connections.

---

## 3. What You Build

### 3.1 Components

| Component | Responsibility |
|---|---|
| TCP Listener | Accepts connections from client applications on a local port |
| pgwire Parser | Reads the PostgreSQL wire protocol — startup message, query messages, response frames |
| Connection Pool | Maintains N real connections to PostgreSQL backend |
| Session Router | In transaction mode: assigns a backend connection for the duration of one transaction, returns it on COMMIT/ROLLBACK |
| Health Checker | Periodically pings idle backend connections, removes dead ones |

### 3.2 pgwire Protocol (Simplified)

```
Client → Pool:
  StartupMessage { user, database, ... }
  Query { "BEGIN" }
  Query { "SELECT * FROM orders WHERE id = $1" }
  Query { "COMMIT" }

Pool → PostgreSQL (on one backend connection):
  [forwards all of the above transparently]

PostgreSQL → Pool → Client:
  AuthenticationOk
  ReadyForQuery
  DataRow { ... }
  CommandComplete
  ReadyForQuery
```

### 3.3 Transaction Mode Logic

```
On client Query("BEGIN"):
  - Acquire a backend connection from the pool (block if none available)
  - Assign connection to this client session
  - Forward BEGIN to PostgreSQL

On client Query("COMMIT") or Query("ROLLBACK"):
  - Forward to PostgreSQL
  - Release backend connection back to pool
  - Client session no longer holds a backend connection

Between transactions:
  - Client holds NO backend connection
  - Backend connection is available for other clients
```

### 3.4 Pool Sizing Formula

```
pool_size = (num_CPUs × 2) + effective_spindle_count

For a 4-CPU database server:
  pool_size = (4 × 2) + 1 = 9

Result: 10,000 app connections → 9 PostgreSQL connections
```

---

## 4. Key Concepts Demonstrated

- **pgwire protocol parsing** — PostgreSQL's binary wire protocol. Every message has a type byte + 4-byte length. Parsing it teaches you what `libpq` does under the hood.
- **Transaction mode vs session mode** — transaction mode: connection returned after each transaction. Session mode: connection held for the entire client session. Transaction mode is 100x more efficient for HTTP APIs.
- **Backpressure** — if all backend connections are busy, new client queries block (with configurable timeout). This is the correct behaviour.
- **Connection lifecycle** — idle connections consume PostgreSQL memory. The health checker detects and removes dead connections before clients hit errors.

---

## 5. Implementation Checklist

- [ ] TCP listener accepting client connections on `localhost:5433`
- [ ] pgwire startup message parsing (authentication bypass for local connections)
- [ ] Query message parser: reads type byte + length prefix + query string
- [ ] Backend connection pool: `Acquire() (*pgConn, error)` and `Release(*pgConn)`
- [ ] Transaction boundary detection: scan for `BEGIN` / `COMMIT` / `ROLLBACK` tokens
- [ ] Session mode vs transaction mode switchable via config
- [ ] Health checker goroutine: ping every idle connection every 30s
- [ ] Benchmark: raw PostgreSQL vs pool at 100/1000/10000 concurrent clients
- [ ] `go test -race ./...` passes

---

## 6. Benchmarks to Document

| Metric | Raw PostgreSQL | With Pool | Improvement |
|---|---|---|---|
| 100 concurrent clients, simple SELECT | Baseline | Measure | Document |
| 1000 concurrent clients | Likely errors (too many connections) | Stable | Document |
| Connection acquisition latency p99 | N/A | < 1ms | Document |
| Memory: PostgreSQL process count | 100 processes | 9 processes | ~11x |

---

## 7. Interview Value

- **Zomato / Swiggy:** *"Your API has 5000 concurrent requests hitting PostgreSQL. It's falling over. What do you do?"* → PgBouncer transaction mode. You can explain exactly why.
- **Uber:** *"How does PgBouncer work internally?"* → pgwire parsing, transaction boundary detection, pool sizing formula.
- **DoorDash:** *"Why is `max_connections = 9` in your PgBouncer config when you have 10,000 app instances?"* → Connection pool sizing formula from first principles.

---

## 8. ADR to Write

**"PgBouncer transaction mode vs session mode"**  
Decision: transaction mode for all HTTP API services.  
Exception: session mode for long-running reporting jobs (they use `SET` commands and prepared statements that require session persistence).  
Formula: `pool_size = (CPUs × 2) + spindles` — document with actual numbers from your RDS instance.
