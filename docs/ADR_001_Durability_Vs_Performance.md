# ADR 001: Durability vs. Performance in WAL-Kv

## Status
Proposed/Accepted

## Context
In a Write-Ahead Log (WAL) system, every write operation must be recorded on disk before it is acknowledged as successful. This ensures the "Durability" (D in ACID) property. However, writing to disk is significantly slower than writing to memory.

We need to decide on the default sync strategy for `Wal-Kv` and understand the tradeoffs between:
1. **SyncModeFull (`fsync`)**: Force data AND metadata to physical disk.
2. **SyncModeNone (Buffered)**: Write to OS page cache, letting the OS decide when to flush.

## Benchmarks
Conducted on: Intel(R) Core(TM) i7-8650U CPU @ 1.90GHz (Windows 11)

| Sync Mode | Latency per Op | Ops/Sec (approx) | Durability Guarantee |
| :--- | :--- | :--- | :--- |
| **SyncModeFull** | 1,260,585 ns (1.26ms) | ~800 | 100% - Survivor of Power Loss |
| **SyncModeNone** | 8,746 ns (0.008ms) | ~114,000 | 0% - Data lost if OS crashes |

## Decision
We will use **SyncModeFull** as the default for the production API to guarantee no data loss, despite the 144x performance penalty.

For high-throughput, non-critical tenants, we will consider exposing an optional `fdatasync` (SyncModeData) mode if supported by the OS, or allowing buffered writes if the tenant explicitly accepts the risk of data loss on power failure.

## Consequences
- **Positive**: The system is "Crash-Safe." A `kill -9` or power failure will not result in lost commits.
- **Negative**: Write throughput is limited by disk seek/write latency (~800-1000 requests per second on SSD).
- **Mitigation**: To scale beyond this, we would need to implement "Group Commit" (batching multiple logical writes into a single physical disk sync).
