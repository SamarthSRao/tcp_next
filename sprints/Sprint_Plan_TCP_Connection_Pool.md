# Project: TCP Connection Pool — Personal Sprint Plan

**Owner: Samarth**
**Timeline: Weeks 3–4 (Biweekly 02)**

> **Project Name: TCP-Pool-Master** (PgBouncer from Scratch)
> 
> This sprint is about understanding the core of database scalability. You are building a TCP proxy that understands the `pgwire` protocol, allowing 10,000 application instances to share just 9 real database connections. You'll implement Transaction Mode—the industry-standard way to handle high-concurrency HTTP APIs—mirroring the internals of PgBouncer and pgx.

---

## The Goal

Build a high-performance, protocol-aware TCP proxy in Go. It must multiplex client connections onto a fixed number of backend PostgreSQL connections. The proxy must be smart enough to detect transaction boundaries (`BEGIN`/`COMMIT`) to safely return connections to the pool, ensuring maximum throughput with minimum database overhead.

**Success Standard:**
- **Density:** 1,000 concurrent `psql` clients running simple queries against a pool of only 10 backend connections.
- **Protocol Integrity:** A standard driver like `pgx` or `libpq` should not know it's talking to a proxy.
- **Backpressure:** When the pool is exhausted and the queue is full, new clients should be rejected with a clear error or block until a timeout.
- **Latency:** Overhead added by the proxy should be < 1ms for p99.

---

## Where You Are Starting

- You have a PostgreSQL instance running locally.
- You have the `pgwire` protocol documentation.
- You understand raw TCP socket programming in Go (`net.Listen`, `net.Dial`).
- You have the project spec for Biweekly Project 2.

---

## What You Ship This Sprint

| Ticket | What | Priority | Days |
|--------|------|----------|------|
| TCP-201 | **TCP Proxy Foundations** — Listener, backend connection, and transparent pipe | P0 | 1-2 |
| TCP-202 | **pgwire Startup Handshake** — Parsing `StartupMessage` and Auth bypass | P0 | 3-4 |
| TCP-203 | **Simple Connection Pool** — Session mode pooling (1 client : 1 connection) | P0 | 5 |
| TCP-204 | **Transaction Mode** — Boundary detection (`BEGIN`/`COMMIT`) and multiplexing | P0 | 6-7 |
| TCP-205 | **Health Checker** — Background `ping` goroutine and auto-reconnects | P1 | 8 |
| TCP-206 | **Benchmarking & Sizing** — Raw PG vs Proxy load tests + Formula validation | P1 | 9 |
| TCP-207 | **Mode Configuration** — YAML-based config for Session/Transaction toggle | P1 | 10 |

---

### TCP-201: TCP Proxy Foundations
**Priority: P0 | Days 1-2**

Implement the plumbing. Accept a connection on one port and tunnel it to another.

**Done when:**
- A TCP listener accepts connections on `:5433`.
- The proxy opens a backend TCP connection to `:5432`.
- Bytes are piped between the two connections (`io.Copy` style but protocol-aware later).
- `nc localhost 5433` successfully connects to a listening server on the other end.

---

### TCP-202: pgwire Startup Handshake
**Priority: P0 | Days 3-4**

The "Handshake" is where most proxies fail. You must decode the `StartupMessage` and spoof a success response.

**Done when:**
- Parser correctly reads the 4-byte length and extract key-value params (user, db).
- Proxy sends back `AuthenticationOk` ('R' message) to the client.
- Proxy sends back essential `ParameterStatus` ('S') messages (server_version, client_encoding).
- `psql` reaches the prompt (even if it hangs on the first query).

---

### TCP-203: Simple Connection Pool (Session Mode)
**Priority: P0 | Day 5**

Create the "Backend Pool" manager. Before implementing multiplexing, prove the pool can manage connections.

**Done when:**
- Pool initializes with `MaxConnections` real DB connections.
- `Acquire()` gives a connection to a client; `Release()` returns it.
- If no connections are available, the client blocks until one is returned.
- A client holds a single backend connection for its entire session.

---

### TCP-204: Transaction Mode (The "Magic")
**Priority: P0 | Days 6-7**

Multiplex connections. A client only holds a backend connection while a transaction is active.

**Done when:**
- Parser identifies 'Q' (Simple Query) messages.
- Logic detects `BEGIN` to start a transaction and `COMMIT`/`ROLLBACK` to end it.
- Outside of a transaction, the backend connection is returned to the pool immediately after the final response frame is sent.
- Multiple clients share the same backend connection over time.

---

### TCP-205: Health Checker
**Priority: P1 | Day 8**

Ensure the pool doesn't fill with "ghost" connections.

**Done when:**
- A background goroutine iterates through idle connections every 30s.
- `SELECT 1` is sent to verify the connection is still alive.
- Dead connections are closed and transparently replaced in the pool.

---

### TCP-206: Benchmarking & Sizing
**Priority: P1 | Day 9**

Prove the value Proposition.

**Done when:**
- `k6` or `go-pg-bench` shows that 1,000 clients can run queries against 10 backend connections.
- Memory usage of PostgreSQL is significantly lower with the proxy.
- ADR written documenting the "Pool Sizing Formula": `pool_size = (CPUs * 2) + spindles`.

---

### TCP-207: Mode Configuration
**Priority: P1 | Day 10**

Make the proxy flexible.

**Done when:**
- Configuration file `proxy.yaml` defines `mode: transaction` or `mode: session`.
- All pool parameters (size, timeouts, logs) are configurable via the file.

---

## Environment & Setup

**Tech Stack:** Go (Standard Library), `bufio`, `net`, `os`.

**Workflow:**
- Use `tcpdump` or `Wireshark` to see how a real `psql` client talks to PG.
- Treat every `net.Conn` as a `bufio.Reader`/`Writer`.
- Use a local Docker PostgreSQL for testing.

---

## Why This Matters — For Your Portfolio

1. **Protocol Mastery:** You didn't just use a library; you implemented a binary wire protocol from the specification.
2. **Concurrency Expertise:** You managed a complex pool of shared resources with thread-safe `Acquire/Release` logic.
3. **Database Performance Insights:** You can explain exactly why databases fail under high connection counts and how pooling solves it.
4. **Multiplexing Logic:** You implemented the "Transaction Mode" logic that powers PgBouncer and large-scale cloud DB platforms.
