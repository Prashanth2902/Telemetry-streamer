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

**Results:** pending the `SpscRing` implementation (M1).

| Queue | Throughput (M items/s) | Round trip (ns) | Pinning |
|---|---|---|---|
| `SpscRing` | | | |
| `MutexRingQueue` | | | |
| `MutexDequeQueue` | | | |

**Variants:** pending. These need `SpscRing` to expose the knobs.

| Variant | Throughput (M items/s) | Round trip (ns) |
|---|---|---|
| Cached indices on (default) | | |
| Cached indices off | | |
| `head_`/`tail_` padded to 64 B (default) | | |
| No padding (false sharing) | | |
| Padded to 128 B | | |

## Encode (SPEC §14.2) and end-to-end (SPEC §14.3)

Added in M6.
