# Benchmarks

Methodology and results for the benchmarks in SPEC §14. Numbers are medians across runs. Absolute values depend on the machine (see Environment). Comparisons between queues on the same machine are the meaningful part.

## Environment

| | |
|---|---|
| CPU | AMD Ryzen 9 7845HX (Zen 4), 12 cores / 24 threads |
| Caches | L1d 32 KiB per core, L2 1 MiB per core, L3 32 MiB (as reported in WSL2); 64-byte cache lines |
| OS | WSL2 on Windows, Ubuntu 26.04.1 LTS |
| Kernel | `6.6.87.2-microsoft-standard-WSL2` |
| Compilers | gcc 15.2.0, clang 21.1.6 |
| Build | `release` preset (`-O3 -DNDEBUG`, CMake Release defaults) |
| CPU governor | Not controllable from WSL2 (host power plan applies) |
| Thread pinning | Recorded per result (`TSTREAM_BENCH_CPUS`); WSL2 vCPUs are scheduled by the Windows host, so pinning is best-effort |

WSL2 caveat (SPEC §11.1): the VM can't control frequency scaling, and the host scheduler moves vCPUs. Treat absolute latencies as environment-specific. Relative comparisons (ring vs mutex, padded vs unpadded) remain valid.

## How to run

```bash
cmake --preset release && cmake --build --preset release

# Unpinned
./build/release/bench/bench_spsc --benchmark_repetitions=5 --benchmark_report_aggregates_only=true

# Pinned to two physical cores, or to two SMT siblings of one core
TSTREAM_BENCH_CPUS=2,4 ./build/release/bench/bench_spsc --benchmark_repetitions=5 --benchmark_report_aggregates_only=true
TSTREAM_BENCH_CPUS=2,3 ./build/release/bench/bench_spsc --benchmark_repetitions=5 --benchmark_report_aggregates_only=true
```

On this machine SMT siblings are adjacent pairs (`/sys/devices/system/cpu/cpu2/topology/thread_siblings_list` → `2-3`), so `2,3` shares one physical core (and its L1/L2) while `2,4` uses two cores that communicate through L3.

## SPSC ring (SPEC §14.1)

**What is measured** (`bench/bench_spsc.cpp`):
- `BM_Throughput<Q>`: a producer thread streams `uint64_t` values to a consumer thread. Each iteration moves 65,536 items and waits until all are consumed. Reported as items/s.
- `BM_PingPong<Q>`: one value bounces between two queues. Time per iteration is one round trip; one-way handoff latency is about half.
- Queues (capacity 1024 each): `SpscRing`, `MutexRingQueue` (mutex + fixed array), `MutexDequeQueue` (mutex + bounded `std::deque`). The mutex-ring baseline isolates the cost of locking from the cost of `std::deque`'s block allocation.
- Both sides spin with `pause` on full/empty (no `yield`), so scheduler syscalls don't pollute the numbers.

**Results** (2026-10-12, gcc 15.2 `release` preset, medians of 5 repetitions; reproduce with the commands above):

| Queue | Throughput, M items/s (unpinned / `2,4` / `2,3`) | Round trip, ns (unpinned / `2,4` / `2,3`) |
|---|---|---|
| `SpscRing` | **135.7 / 135.5 / 130.8** | **335 / 357 / 370** |
| `MutexRingQueue` | 7.1 / 6.6 / 6.8 | 2135 / 2413 / 2285 |
| `MutexDequeQueue` | 7.7 / 7.3 / 7.0 | 2384 / 2388 / 2400 |

On separate cores (`2,4`), `SpscRing` moves **20.6×** the items of the mutex+ring baseline (18.5× the mutex+deque), and a round trip takes **6.8× less time** (357 vs 2413 ns, so about 180 ns one way). Both mutex queues land in the same place, so the gap comes from locking itself, not from `std::deque`'s allocation. Every push and pop takes the lock, which bounces the mutex's cache line between cores on every operation; the ring's two sides each write their own line and read the other's only when they look full or empty.

**Variants** (one knob changed from the default):

| Variant | Throughput, M items/s (unpinned / `2,4` / `2,3`) | Round trip, ns (unpinned / `2,4` / `2,3`) |
|---|---|---|
| Default: cached indices, sides 128 B apart | **135.7 / 135.5 / 130.8** | **335 / 357 / 370** |
| Cached indices off | 65.4 / 64.1 / 66.7 | 352 / 374 / 394 |
| Sides 64 B apart | 83.8 / 80.9 / 92.3 | 340 / 343 / 354 |
| No padding (sides share a line) | 151.6 / 85.7 / 148.5 † | 465 / 436 / 419 |

- **Cached indices about double throughput.** Without them, every push reads the consumer's line and every pop reads the producer's, so the lines move between cores constantly. Latency barely changes, because ping-pong has at most one item in flight, so the cache is stale on almost every call anyway.
- **128 B beats 64 B by ~50% in throughput,** consistently across runs (CV under 2% in a 10-repetition run) and in all three placements, with no latency cost. This changed the default (ADR-010). The mechanism isn't proven. AMD's adjacent-line prefetcher (which fetches cache lines in 128-byte pairs) is the usual explanation, but 64 B is slow even on SMT siblings, so confirming it needs hardware performance counters on bare metal.
- **No padding: latency is consistently 13–39% worse than the default,** the expected false-sharing cost. † Its throughput is bimodal: about 86 or about 150 M/s from run to run, even when pinned (40% CV in one 10-repetition run). Under WSL2 the Windows host decides where vCPUs physically run, and the fast mode matches two hardware threads sharing an L1, where a shared line costs almost nothing. **WSL2 can't measure false-sharing throughput reliably;** repeat this on bare-metal Linux before quoting it.

## Encode (SPEC §14.2) and end-to-end (SPEC §14.3)

Added in M6.
