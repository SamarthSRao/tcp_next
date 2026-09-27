# tcp-conn-pool

A PostgreSQL wire-protocol (pgwire) proxy written in Go. It accepts client TCP connections, speaks the startup handshake, and forwards **simple-query** messages to a small fixed pool of backend connections.

This is a learning implementation of the ideas behind [PgBouncer](https://www.pgbouncer.org/) transaction pooling. It is not a drop-in PgBouncer replacement. The sections below describe what the code in this repository actually does.

## What it does

`main` listens on **`:5433`** (all interfaces) and pre-dials **10** TCP connections to **`localhost:5432`**. For each client:

1. Read the startup phase. An `SSLRequest` is answered with a single byte `N` (TLS is not supported). A cancel request is rejected. A `StartupMessage` is parsed into user, database, and the other parameters.
2. Check out a backend connection. If that socket has never completed startup, send this client's startup bytes to PostgreSQL and finish authentication with the backend. The password, if the backend asks for one, comes from the **`PGPASSWORD`** environment variable (cleartext or MD5). SCRAM/SASL is rejected.
3. Tell the client authentication succeeded. The client is not asked for a password. The proxy sends `AuthenticationOk`, replays the backend's `ParameterStatus` and `BackendKeyData`, then `ReadyForQuery` (idle).
4. Return the backend connection to the pool.
5. For each later client message, check out a backend (starting it first if it is still a raw TCP socket), forward that one message, and copy backend messages back to the client until `ReadyForQuery` (`Z`).
   - Status `I` (idle): return the backend to the pool.
   - Any other status, including `T` (in a transaction) and `E` (failed transaction): keep that backend checked out to this client and repeat.

A backend socket is started **at most once**. Later clients that are handed the same socket do not send `StartupMessage` again; they are shown the saved handshake. Queries from those clients run on the session that was opened by whichever client started that connection.

`pkg/proxy` is an earlier byte-for-byte TCP tunnel (`io.Copy` in both directions). `main` does not use it.

## How connections are handled

```
 client                         this process                         PostgreSQL
   |                                  |                                    |
   |  TCP :5433                       |                                    |
   |--------------------------------->|                                    |
   |  SSLRequest?  -------- 'N'       |                                    |
   |  StartupMessage                  |                                    |
   |                                  |  checkout pooled TCP conn          |
   |                                  |  StartupMessage (first use only)   |
   |                                  |----------------------------------->|
   |                                  |  auth challenge / password         |
   |                                  |<---------------------------------->|
   |                                  |  ParameterStatus, KeyData, Ready   |
   |                                  |<-----------------------------------|
   |  AuthenticationOk + replay + Z   |                                    |
   |<---------------------------------|  return conn to pool                |
   |                                  |                                    |
   |  'Q' simple query                |  checkout (block if none idle)     |
   |--------------------------------->|----------------------------------->|
   |  rows … ReadyForQuery            |                                    |
   |<---------------------------------|<-----------------------------------|
   |                                  |  if status == 'I': return to pool  |
   |                                  |  else: keep conn until a later 'I' |
```

The pool is an idle slice of `*pool.PooledConn`, filled at startup by `net.Dial` and guarded by a mutex. `Get` blocks when the slice is empty. `Put` appends the connection and wakes one waiter. Checkout is first-in, first-out. There is no checkout timeout and no background health check.

Transaction boundaries come from the backend's `ReadyForQuery` status byte, not from scanning SQL text. `pgwire.ClassifyQuery` can label a string as `BEGIN` / `COMMIT` / `ROLLBACK` by prefix, but the session loop does not call it. One simple query can contain several statements; PostgreSQL sends a single `ReadyForQuery` when it is done, and that status is what decides whether the connection stays checked out.

## Build and run

Requires Go 1.24 (see `go.mod`) and a PostgreSQL server already listening on `localhost:5432`.

The backend must accept the user from the client's startup message with **trust**, **password** (cleartext), or **md5** authentication. The proxy cannot answer SCRAM. For md5 or cleartext, set `PGPASSWORD` to the backend password. That password is used for the backend only; clients are not authenticated.

```bash
go build -o tcp-conn-pool .
PGPASSWORD=secret ./tcp-conn-pool
```

The process logs each client startup and then prints that it is tunneling queries. It runs until it is killed. On a listen error it exits with status 1.

### Tests

Tests use in-process TCP peers. They do not need PostgreSQL.

```bash
go test -race ./...
go vet ./...
```

## Example

With PostgreSQL on port 5432 and a local trust or md5 rule for user `postgres`:

```bash
PGPASSWORD=secret ./tcp-conn-pool
```

In another shell:

```bash
psql "host=127.0.0.1 port=5433 user=postgres dbname=postgres sslmode=disable"
```

```sql
SELECT 1;
BEGIN;
SELECT 1;
COMMIT;
```

`SELECT 1` outside a transaction ends in `ReadyForQuery` status `I`, and the backend socket goes back to the pool. `BEGIN` ends in status `T`, so that same client keeps the backend until a later `ReadyForQuery` reports `I` (for example after `COMMIT`, or after a rollback of a failed transaction).

A second `psql` session can complete startup without a second `StartupMessage` on a backend that is already live. Its queries then share that backend whenever the socket is idle.

## Layout

| Path | Role |
| --- | --- |
| `main.go` | Listen on `:5433`, pool of 10, per-client session loop |
| `pkg/pool` | Fixed-size pre-dialed TCP pool (`Get` / `Put` / `Discard` / `Close`) |
| `pkg/pgwire` | Startup parsing, backend auth, client auth spoof, message framing |
| `pkg/proxy` | Raw TCP byte tunnel; not used by `main` |
| `docs/DATA_FLOW.md` | Earlier notes from when the proxy was a 1:1 byte pipe |
| `sprints/`, `Biweekly_02_TCP_Connection_Pool.md` | Original project plan. Several items there are not implemented |

## Limitations

These are properties of the current code, not a roadmap with measured results. Nothing in this repository is a benchmark.

- **Addresses and pool size are fixed.** Listen `:5433`, backend `localhost:5432`, size 10. There is no config file, no session-vs-transaction switch, and no pool-sizing formula applied at runtime.
- **Clients are not authenticated.** Every client is sent `AuthenticationOk`. The proxy never reads a client password. Backend auth uses `PGPASSWORD` and the startup message of the client that first started that backend socket.
- **One backend session is shared.** The first successful startup on a pooled connection decides the user and database. Later clients reuse that session and the cached `ParameterStatus` / `BackendKeyData`. `SET`, prepared statements, temporary tables, advisory locks, and `search_path` leak across clients that share the socket. Cancel keys are whatever the backend sent at startup; the proxy does not handle cancel requests.
- **Simple query only.** After each client message the proxy reads backend messages until `ReadyForQuery`. That matches the simple-query protocol (`Q`), where the backend does not wait for another client message before `Z`. The extended protocol (`Parse` / `Bind` / `Execute` / `Sync`, and COPY) does not: the proxy will sit in the inner read while the client waits to send the next message. A client `Terminate` (`X`) often makes the backend close instead of sending `Z`; the proxy treats that as a backend read error and discards the socket.
- **`Get` waits without a timeout.** If every live connection is checked out, the client goroutine waits until one is returned or the pool is closed. If every pre-dial failed, the idle set is empty and `Get` waits until `Close`. A failed `Discard` redial leaves the pool one connection smaller. There is no queue limit and no statement timeout in the proxy.
- **No health checks.** Idle sockets are not pinged. A dead server is noticed on the next read or write. `Discard` closes a bad socket and dials a replacement; if that dial fails, the pool shrinks.
- **`Close` only closes idle connections.** A connection checked out to a client stays open until that session `Put`s or `Discard`s it. `Close` can be called more than once. A `Put` or `Discard` that arrives after `Close` closes that socket and returns an error.
- **Startup failures and broken reads drop the socket.** The failed handshake path used to leak the checkout. It now closes that TCP connection and dials another one. The in-memory pool still will not retry a client whose handshake already failed.
- **TLS is refused** (`N`). SCRAM is refused. Listen is on all interfaces, with no client authentication.
- **`pkg/proxy` ignores the contents of the bytes.** Cancelling its `Start` context closes the listener and stops `Accept`. Connections already accepted are not shut down by that cancel. When either copy direction finishes, both sockets are closed so the other direction cannot block forever. `main` does not call this type.

`docs/ADR_001_Durability_Vs_Performance.md` is about a different exercise (WAL-Kv). Its numbers are not measurements of this proxy.
