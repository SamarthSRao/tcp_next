# Data Flow — TCP Connection Pool Proxy

> **Current behavior is the [README](../README.md).** The sections below were written when the proxy was a 1:1 byte tunnel: parse startup, then `io.Copy` for the rest of the session. `main.go` now keeps a fixed pool of backend sockets and returns a connection to the pool when `ReadyForQuery` reports idle (`I`).

This document describes how data moves through the proxy **as implemented through TCP-202** (transparent tunnel + pgwire startup handshake).

---

## 1. Big picture

```
┌─────────┐         ┌──────────────────────┐         ┌──────────┐
│ Client  │  TCP    │   Your Go process    │  TCP    │ Postgres │
│ (psql)  │◄───────►│   :5433              │◄───────►│  :5432   │
└─────────┘         │                      │         └──────────┘
                    │  clientConn          │
                    │       ↕              │
                    │  handleConnection    │
                    │       ↕              │
                    │  backendConn         │
                    └──────────────────────┘
```

- **Client never opens a socket to 5432.** It only talks to the proxy.
- **The proxy** opens a separate socket to Postgres.
- The proxy is a **man-in-the-middle** that understands startup, then becomes a dumb byte pipe.

---

## 2. Process-level flow (control plane)

```
main()
  │
  ├─ Listen(:5433)          // one listening socket for all clients
  │
  └─ loop forever:
        Accept()            // new clientConn
        go handleConnection(clientConn)
```

| Step | What moves | Who owns it |
|------|------------|-------------|
| Listen | OS binds port 5433 | `listener` |
| Accept | New TCP connection from client | `clientConn` |
| `go` | Same connection handled concurrently | one goroutine per client |

Many clients ⇒ many `handleConnection` goroutines. **Today each still opens its own `backendConn`** (no pool yet).

---

## 3. One session: phased data flow

Each client session has **four phases**.

### Phase A — Client connects (TCP only)

```
Client  ──SYN/ACK──►  :5433
Accept  ───────────►  clientConn is live
                      (no Postgres yet)
```

No Postgres traffic yet. Only a socket to the proxy process.

---

### Phase B — Startup phase (protocol-aware)

This is **not** a transparent pipe yet. The proxy **parses and speaks** pgwire.

#### B1. Client → proxy (startup)

```
Client ──► proxy

  Option 1: SSLRequest
    [len=8][code=80877103]
    proxy ──► client : single byte 'N'   (no TLS)
    then client continues...

  Option 2: StartupMessage
    [len][protocol version][user\0 ... database\0 ... \0]
```

**Inside the proxy:**

- Read 4-byte length → read body
- If SSL → write `'N'`, wait again
- Else parse params → build `StartupMessage` (including `Raw` bytes)
- Log user/database

| Data | Fate |
|------|------|
| Meaning of the message | Parsed into a struct (map of params) |
| Exact bytes | Kept as `startup.Raw` to forward to Postgres |

```
Client bytes ──► ReadStartupPhase ──► struct { Params, Raw, ... }
                     │
                     └── nothing to Postgres yet
```

#### B2. Proxy → Postgres (open backend + same startup)

```
proxy ──Dial──► localhost:5432     → backendConn
proxy ──Write(startup.Raw)──► Postgres
```

Postgres sees a **normal frontend** connecting with that user/db.

#### B3. Postgres → proxy (backend handshake; **not** forwarded live)

```
Postgres ──► proxy only (consumed)

  'R' Authentication*   → maybe proxy sends 'p' password back
  'S' ParameterStatus   → saved in hs.ParameterStatuses
  'K' BackendKeyData    → saved in hs.BackendKeyData
  'Z' ReadyForQuery     → backend is ready; loop ends
```

| Direction | What happens to the bytes |
|-----------|---------------------------|
| Backend → proxy | **Read and interpreted** |
| Proxy → client (during this) | **Nothing yet** |
| Why | Client will get a **forged** success handshake next |

If auth needs a password:

```
Postgres ──'R' (cleartext/MD5)──► proxy
proxy ──'p' PasswordMessage──► Postgres   (from PGPASSWORD / MD5)
Postgres ──'R' AuthOk + 'S'* + 'K' + 'Z'──► proxy
```

#### B4. Proxy → client (spoofed “you’re logged in”)

```
proxy ──► client

  'R' AuthenticationOk      (always success; client did not auth to proxy)
  'S' ParameterStatus...    (replay from real Postgres)
  'K' BackendKeyData        (replay or synthetic)
  'Z' ReadyForQuery 'I'     (idle — client may send SQL)
```

**Client’s view:** “I connected to a Postgres server and auth succeeded.”  
**Reality:** Auth was between **proxy and Postgres**; the client was **told** OK.

```
                    ┌─────────────────┐
 Client ◄──spoof─── │ WriteClientOK   │
                    │  uses saved hs  │
                    └────────▲────────┘
                             │ collected during
                    ┌────────┴────────┐
 Postgres ──auth───►│ CompleteBackend │
                    └─────────────────┘
```

After B4, **both sides are “ready for queries”**, but only the proxy has the real DB session.

---

### Phase C — Query phase (transparent byte tunnel)

Handshake is done. The proxy no longer parses (for now). Two concurrent pumps:

```
        clientConn                    backendConn
Client ───────────► proxy ───────────► Postgres
       io.Copy(backend, client)

Client ◄─────────── proxy ◄─────────── Postgres
       io.Copy(client, backend)
```

| Goroutine | Source | Destination | Typical content |
|-----------|--------|-------------|-----------------|
| Client → backend | `clientConn` | `backendConn` | `'Q'` queries, `'X'` terminate, extended protocol, … |
| Backend → client | `backendConn` | `clientConn` | `'T'` row desc, `'D'` rows, `'C'` complete, `'Z'` ready, errors, … |

**Data flow property:** bytes are copied as a stream. Messages are not reassembled in this phase.

**Sync:**

```
done channel (buffer 2)
  copy1 finishes → done
  copy2 finishes → done
  handleConnection waits for both → returns → defers Close
```

If one side closes:

```
e.g. client disconnects
  → client→backend Copy ends
  → backendConn.Close()
  → backend→client Copy unblocks / ends
  → both signal done
  → session over
```

---

### Phase D — Teardown

```
defer clientConn.Close()
defer backendConn.Close()
```

OS closes both TCP connections. Postgres drops that backend session. Client is gone.

---

## 4. Full sequence (one successful `psql` session)

```
 time
  │
  │  [TCP] Client ──connect──► :5433
  │  [TCP] Proxy  ──Dial─────► :5432
  │
  │  [MSG] Client ──SSLRequest?──► Proxy ──'N'──► Client
  │  [MSG] Client ──StartupMessage──► Proxy
  │  [MSG] Proxy  ──StartupMessage──► Postgres
  │  [MSG] Postgres ──Auth / Params / Key / Ready──► Proxy   (eaten)
  │  [MSG] Proxy  ──AuthOk / Params / Key / Ready──► Client  (spoof)
  │
  │  [BYTES] Client ◄════════════ pipe ════════════► Postgres
  │           (SELECT 1, results, more queries...)
  │
  │  [TCP] either side closes → both copies stop → sockets closed
  ▼
```

---

## 5. What flows **where** (cheat sheet)

| Data | Client → Proxy | Proxy → Postgres | Postgres → Proxy | Proxy → Client |
|------|----------------|------------------|------------------|----------------|
| TCP connect | yes | yes (separate) | — | — |
| SSLRequest | yes | no | no | `'N'` only |
| StartupMessage | yes (parsed) | yes (`Raw`) | no | no |
| Password (real) | **no** (bypass) | yes if asked | challenges | **no** |
| AuthOk to app | — | — | to proxy only | **spoofed** yes |
| ParameterStatus | — | — | saved | **replayed** |
| Queries / results | after ready | after ready | after ready | after ready |

---

## 6. Two important invariants

### 1. Two TCP connections, one logical session (today)

```
1 clientConn  ↔  1 handleConnection  ↔  1 backendConn
```

No sharing. Three clients ⇒ three Postgres connections.

### 2. Handshake is asymmetric; query phase is symmetric

| Phase | Proxy role |
|-------|------------|
| Startup | **Protocol endpoint** for client; **real client** to Postgres |
| Queries | **Byte forwarder** both ways |

Startup is special: Postgres auth messages must not be blindly copied to the client (wrong peer, double auth, or client would see the proxy’s password exchange).

---

## 7. One-sentence mental model

**Accept the client, finish a private login to Postgres, tell the client “you’re authenticated,” then shuttle every later byte between the two sockets until someone hangs up.**

---

## 8. What will change later

| Ticket | Flow change |
|--------|-------------|
| **TCP-203 pool** | `Dial` moves to pool init; session does `Acquire` / `Release` instead of always new `Dial` |
| **TCP-204 transaction mode** | Client may stay connected while **backendConn is returned to pool** between transactions — pipe is no longer “for the whole client lifetime” |
| **Health check** | Idle `backendConn`s get periodic traffic **without** a client |

Right now the data flow is still **session-sticky 1:1** after a **parsed + spoofed** handshake.

---

## Related code

| File | Role |
|------|------|
| `main.go` | Listen/accept, wire handshake + tunnel |
| `pkg/pgwire/messages.go` | Startup parse, backend auth, client spoof, framing |

## Related docs

- [PostgreSQL Frontend/Backend Protocol](https://www.postgresql.org/docs/current/protocol.html)
- [Message Formats](https://www.postgresql.org/docs/current/protocol-message-formats.html)
- Project sprint: `sprints/Sprint_Plan_TCP_Connection_Pool.md` (TCP-201, TCP-202)
- Spec: `Biweekly_02_TCP_Connection_Pool.md`
