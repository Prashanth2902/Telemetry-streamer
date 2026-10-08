# Telemetry Streamer — Project Specification

Working name: `tstream`
Language: C++20 · Platform: Linux (x86_64 dev machine; no hardware board required)
Purpose: portfolio project for embedded, automotive, and general C++ systems roles

---

## 1. Summary

`tstream` is a real-time telemetry pipeline. It ingests high-rate data from multiple vehicle-style sources (IMU, GPS, battery, and later CAN bus). It moves the data through lock-free queues, serializes it with Protobuf, and streams it over the network to a receiver. The receiver decodes it, checks it for integrity, and measures end-to-end latency.

All data sources are simulated or emulated, so the project runs on any Linux machine. The design keeps hardware sources pluggable. A Linux virtual CAN (`vcan`) source arrives in a later milestone, and that is the bridge to the companion CAN/UDS diagnostics project.

The project exists to show, with measured evidence:

- Concurrency done correctly (lock-free SPSC queues, correct memory ordering, verified with ThreadSanitizer)
- Performance discipline (no heap allocation on the hot path in steady state, benchmarked latency percentiles)
- Robust systems behavior (backpressure, drop policies, sequence-gap detection, health monitoring, fault injection)
- Professional engineering (CMake, unit/integration/stress tests, sanitizers, CI, clear design documentation)

## 2. Goals and non-goals

### Goals
1. Ingest at least 3 concurrent simulated sources at different rates (1 Hz to 1 kHz+).
2. Pass samples from producer threads to the pipeline through a custom lock-free SPSC ring buffer.
3. Serialize samples in batches with Protobuf and stream them over TCP with length-prefixed framing.
4. Handle slow consumers with configurable per-source backpressure policies, and count every drop.
5. Detect stale sources, rate deviations, and sequence gaps, and emit health events into the stream.
6. Measure and publish throughput and end-to-end latency (p50 / p99 / p99.9).
7. Keep the steady-state hot path free of heap allocations, and verify this with a test.
8. Add a SocketCAN source that reads from `vcan0` (later milestone).

### Non-goals
- No GUI or dashboard (a CLI receiver that prints stats is enough).
- No cloud backend, database, or MQTT broker.
- No security or TLS in v1. Mention it in the README as future work.
- No real hardware drivers in v1.

## 3. Architecture

```
 ┌──────────────┐   ┌──────────────┐   ┌──────────────┐   ┌──────────────┐
 │ IMU source   │   │ GPS source   │   │ Battery src  │   │ CAN source   │ (M7)
 │ thread 1kHz  │   │ thread 10Hz  │   │ thread 1Hz   │   │ vcan0 reader │
 └──────┬───────┘   └──────┬───────┘   └──────┬───────┘   └──────┬───────┘
        │ SPSC ring        │ SPSC ring        │ SPSC ring        │ SPSC ring
        ▼                  ▼                  ▼                  ▼
 ┌─────────────────────────────────────────────────────────────────────────┐
 │ Pipeline thread                                                         │
 │  • polls all rings (round-robin, bounded per-ring drain)                │
 │  • health monitors (stale / rate / seq-gap) → HealthEvent samples      │
 │  • batcher (flush on size OR max-delay timer)                           │
 │  • encoder: reused Protobuf Batch → preallocated byte buffer            │
 │  • transport: non-blocking TCP, length-prefixed frames, epoll           │
 └───────────────────────────────┬─────────────────────────────────────────┘
                                 │ TCP (loopback or LAN)
                                 ▼
 ┌─────────────────────────────────────────────────────────────────────────┐
 │ Receiver (separate process: tstream-recv)                               │
 │  • frame reassembly, decode, validation                                 │
 │  • per-source seq-gap tracking, latency histogram, throughput           │
 │  • periodic stats print; optional CSV/binary recording                  │
 └─────────────────────────────────────────────────────────────────────────┘

 Cross-cutting: Config loader · Stats registry (atomics) · Logger · Fault injector
```

### 3.1 Components

| Component | Responsibility | Key design points |
|---|---|---|
| `Source` (interface) | Produces `Sample`s into its ring | `start()`, `stop()`, `id()`. One thread per source. Fixed-rate sources use `sleep_until` on `steady_clock`. |
| `SimImuSource`, `SimGpsSource`, `SimBatterySource` | Simulated sensors | Deterministic seeded noise models. Fault-injection hooks. |
| `ReplaySource` | Replays a recorded file | Supports deterministic tests and benchmarks. |
| `CanSource` (M7) | Reads raw CAN frames from SocketCAN | `socket(PF_CAN, SOCK_RAW, CAN_RAW)` bound to `vcan0`. |
| `SpscRing<T, N>` | Lock-free handoff between threads | See §5. |
| `Pipeline` | Drains rings, monitors, batches, encodes, sends | Single thread. No locks on the hot path. |
| `HealthMonitor` (interface) + monitors | Stale, rate-deviation, and seq-gap detection | Config-driven thresholds. Emits `HealthEvent` with severity. |
| `Encoder` | Protobuf batch serialization | Reuses one message object and one output buffer. |
| `TcpTransport` | Framed, non-blocking send | `TCP_NODELAY`. Handles partial writes and reconnects with backoff. |
| `Receiver` | Decode, validate, measure | Separate executable. |
| `Stats` | Counters and histograms | Relaxed atomics, snapshot on read. |
| `Config` | Loads a JSON config | Validates at startup. Fails fast on bad config. |

### 3.2 Threading model

- N producer threads (one per source), 1 pipeline thread, and the main thread (signals, periodic stats).
- Each queue has exactly one producer and one consumer. That invariant is what makes the SPSC ring valid. Document it and enforce it in debug builds (record the thread id on the first push/pop and assert on later calls).
- Shutdown: on SIGINT/SIGTERM, set an atomic stop flag. Sources stop producing, the pipeline drains the remaining samples and flushes a final batch, and the transport closes cleanly. There must be no deadlocks or lost final batch, and an integration test checks this.

## 4. Data model

### 4.1 In-memory sample (hot path)

The hot path uses a trivially copyable struct, not a Protobuf message:

```cpp
enum class SampleKind : uint8_t { Imu, Gps, Battery, Can, Health };

struct ImuData     { float ax, ay, az, gx, gy, gz; };
struct GpsData     { double lat, lon; float alt_m, speed_mps; uint8_t fix; };
struct BatteryData { float voltage_v, current_a, soc_pct, temp_c; };
struct CanData     { uint32_t can_id; uint8_t dlc; uint8_t data[8]; };
struct HealthData  { uint16_t source_id; uint8_t monitor; uint8_t severity; uint32_t detail; };

struct Sample {
  uint16_t   source_id;
  SampleKind kind;
  uint32_t   seq;           // per-source, monotonically increasing
  int64_t    t_capture_ns;  // steady_clock at creation
  union { ImuData imu; GpsData gps; BatteryData bat; CanData can; HealthData health; };
};
static_assert(std::is_trivially_copyable_v<Sample>);
```

Identity and sequencing rules:
- **`seq` is assigned by the source when the sample is created, before `try_push`.** A sample rejected by a full ring still consumes its `seq`, so every ring drop shows up downstream as a sequence gap. Compare `seq` values with unsigned wraparound arithmetic.
- **Source id 0 is reserved for the pipeline.** Health samples carry `source_id = 0`, `kind = Health`, a `seq` from a pipeline-owned counter, and `t_capture_ns` = the time the monitor raised the event. The affected source goes in `HealthData::source_id`. The receiver tracks gaps for source 0 like any other source.

### 4.2 Wire schema (`proto/telemetry.proto`)

```proto
syntax = "proto3";
package tstream.v1;

message Imu     { float ax = 1; float ay = 2; float az = 3; float gx = 4; float gy = 5; float gz = 6; }
message Gps     { double lat = 1; double lon = 2; float alt_m = 3; float speed_mps = 4; uint32 fix = 5; }
message Battery { float voltage_v = 1; float current_a = 2; float soc_pct = 3; float temp_c = 4; }
message Can     { uint32 can_id = 1; bytes data = 2; }
message Health  { uint32 source_id = 1; uint32 monitor = 2; uint32 severity = 3; uint32 detail = 4; }

message Sample {
  uint32 source_id    = 1;
  uint32 seq          = 2;
  int64  t_capture_ns = 3;
  oneof payload { Imu imu = 10; Gps gps = 11; Battery battery = 12; Can can = 13; Health health = 14; }
}

message Batch {
  uint64 batch_seq   = 1;
  int64  t_send_ns   = 2;
  repeated Sample samples = 3;
}
```

### 4.3 Framing

Each TCP frame looks like this:

```
| magic u16 = 0x7473 | version u8 = 1 | flags u8 | length u32 | Batch bytes |
```

- The header is exactly 8 bytes. All multi-byte header fields are big-endian (network byte order), so the magic appears on the wire as the bytes `0x74 0x73` ("ts").
- `length` is the size of the Batch payload only, excluding the 8-byte header. It must be greater than 0 and at most `transport.max_frame_bytes` (default 1 MiB).
- `flags` is reserved in v1: the sender writes 0 and the receiver rejects nonzero values. New meanings come with a version bump.

The receiver rejects frames with a bad magic, version, flags, or length, and counts each rejection by reason. A bad header means the byte stream can't be resynchronized, so the receiver closes the connection and the sender reconnects. Write and fuzz the framing code as its own unit.

### 4.4 Timestamps and latency

- Use `std::chrono::steady_clock` everywhere. On Linux it is `CLOCK_MONOTONIC`, which is shared across processes on the same host, so the receiver can compute `now - t_capture_ns` directly when sender and receiver run on the same machine.
- Report end-to-end latency (capture to receive) and pipeline latency (capture to send).
- When running across two machines, mark latency as unavailable instead of reporting misleading numbers.

## 5. Lock-free SPSC ring buffer

This is the centerpiece. Write it yourself and be ready to explain every line in an interview.

Requirements:
- `template <typename T, size_t Capacity> class SpscRing`, where `Capacity` is a power of two and `T` is trivially copyable.
- Storage is a fixed array with no allocation after construction.
- `head_` (consumer index) and `tail_` (producer index) are `std::atomic<size_t>`, each on its own cache line via a project constant `kCacheLine = 64` (`alignas(kCacheLine)`). Don't use `std::hardware_destructive_interference_size`: its value can differ between compilers and flags, and GCC warns (`-Winterference-size`) when it is used in a header, which fails the build under `-Werror`. Benchmark 64 vs 128 (adjacent-line prefetch) and record the result.
- Producer: write the slot, then `tail_.store(..., memory_order_release)`. Consumer: `tail_.load(memory_order_acquire)`, then read the slot. The reverse direction mirrors this for `head_`.
- Optimization: each side keeps a non-atomic cached copy of the other side's index and refreshes it only when the ring appears full or empty. Benchmark the effect.
- API: `bool try_push(const T&)`, `bool try_pop(T&)`, `size_t try_pop_bulk(T* out, size_t max)`, `size_t size_approx() const`.
- Index arithmetic uses masking (`idx & (Capacity - 1)`) with monotonically increasing counters, so there is no wraparound ambiguity.

Verification:
- Unit tests: empty, full, wraparound, and bulk pop.
- Stress test: producer pushes 100M sequenced values while the consumer verifies strict ordering with no loss or duplication. Runs under TSan in CI with a smaller count.
- Benchmark against a `std::mutex` + `std::deque` queue and a `std::mutex` + ring queue. Report ops/sec and per-op latency.

## 6. Backpressure and drop policies

When a source's ring is full, apply the policy configured for that source:

| Policy | Behavior | Use for |
|---|---|---|
| `drop_newest` | Reject the new sample and increment `dropped_newest` | High-rate sensor streams, where losing an occasional sample is acceptable |
| `block_with_timeout` | Spin, then yield, until space frees or the timeout expires, then drop and increment `dropped_timeout` | Low-rate critical data (battery, GPS) |

**No `drop_oldest`.** Evicting the oldest entry would mean the producer advancing `head_`, which the consumer owns. That breaks the single-writer-per-index invariant the SPSC ring depends on, and would need a CAS loop or a different queue. The rejected alternatives (MPMC queue, overwrite ring with per-slot sequence numbers) go in `docs/design-decisions.md`. Health samples never pass through a ring: the pipeline thread creates them and appends them directly to the current batch.

On the transport side, if the socket send buffer is full (`EAGAIN`), keep at most `max_pending_batches` (K) encoded batches. Beyond that, drop whole batches, count them, and count the samples they contained per source, so receiver-side gaps can be reconciled with sender-side drops (§13). The pending batches live in K fixed buffers allocated at startup, each sized to the worst-case encoded batch (see §9), plus a write offset to handle partial writes. Nothing is allocated when a batch is queued.

All drops show up in stats and in the final report. A system that silently loses data counts as a bug.

## 7. Health monitoring

The interface follows a pluggable-monitor pattern:

```cpp
struct HealthMonitor {
  virtual ~HealthMonitor() = default;
  virtual void on_sample(const Sample&, int64_t now_ns) = 0;
  virtual void on_tick(int64_t now_ns) = 0;            // for time-based checks
  virtual std::optional<HealthData> poll_event() = 0;  // no allocation
};
```

Monitors:
- **Stale source**: no sample from source X for more than its stale threshold.
- **Rate deviation**: the measured rate over a sliding window is outside ±`rate_tolerance_pct` of the expected rate.

Thresholds are per source, because sources differ in rate by 1000×. A single global `stale_ms` of 50 would flag a 1 Hz source as stale all the time. Defaults are derived from each source's period (`1000 / rate_hz` ms), and a source can override them (see §10):
- stale threshold = `max(stale_min_ms, stale_periods × period)`. The floor keeps scheduler jitter from raising false alarms on fast sources.
- rate window = `max(rate_window_ms, rate_min_samples × period)`, so a 1 Hz source is judged over at least `rate_min_samples` samples, not one
- **Sequence gap**: `seq` jumped, so samples were dropped upstream.
- **Frozen value** (optional): the payload is bit-identical for more than `frozen_count` consecutive samples.

Health events are injected into the stream as `Health` samples, so the receiver sees them in order with the data.

## 8. Fault injection

Sources accept runtime faults (from the config or the `--fault` CLI flag) so health monitors and drop policies can be shown working:

- `stall:<source>:<ms>` — the source stops producing for a period.
- `burst:<source>:<n>` — the source emits n samples instantly.
- `rate:<source>:<factor>` — the source's rate is multiplied by a factor.
- `freeze:<source>` — the payload stops changing.
- `slow_consumer:<ms>` — the receiver sleeps between reads to force backpressure.

Each fault has an integration test asserting that the expected health event or drop counter appears.

## 9. Memory discipline

- The steady-state hot path (source push, pipeline drain, monitor, encode, send) must not allocate.
- **Reusing a heap-allocated `Batch` is not enough.** `Clear()` keeps the cleared `Sample` elements of a repeated field, but `Sample` has a `oneof`. Clearing a oneof, or switching its case (the slot that held an `Imu` last batch holds a `Gps` this batch), deletes the old submessage and allocates a new one. Batches mix kinds, so this allocates on every batch.
- **Decision:** build each `Batch` on a `google::protobuf::Arena` whose initial block is a buffer owned by the encoder and allocated at startup (`ArenaOptions::initial_block`). After the batch is serialized, `Reset()` the arena. The arena keeps the user-supplied initial block, so steady-state batches allocate nothing as long as they fit in it. Size the block from `max_samples` and verify the size with the allocation test.
- **Fallback:** if the allocation test still finds allocations (for example, arena-internal bookkeeping in the installed Protobuf version), encode directly with `google::protobuf::io::CodedOutputStream` over a preallocated array. The `.proto` stays the schema and the receiver keeps using generated parsing. Record which path was taken in `docs/design-decisions.md`.
- Serialize into a preallocated output buffer sized to the worst-case encoded batch: `max_samples` × the largest encoded `Sample`, plus `Batch` overhead and the 8-byte frame header. Config validation fails if this exceeds `transport.max_frame_bytes`. The same bound sizes the transport's pending buffers (§6).
- Verify the zero-allocation property with the encoder unit test as soon as the encoder exists (M3), not only in M6.
- **Verification test:** install a global `operator new` / `operator delete` override in a test binary that counts allocations. Run the pipeline through a warmup period, reset the counter, run N batches, and assert zero allocations.

## 10. Configuration

Use a JSON file loaded with nlohmann/json. Example:

```json
{
  "sources": [
    { "id": 1, "type": "sim_imu",     "rate_hz": 1000, "ring_capacity": 4096, "policy": "drop_newest" },
    { "id": 2, "type": "sim_gps",     "rate_hz": 10,   "ring_capacity": 256,  "policy": "block_with_timeout", "timeout_us": 500 },
    { "id": 3, "type": "sim_battery", "rate_hz": 1,    "ring_capacity": 64,   "policy": "block_with_timeout", "timeout_us": 500,
      "health": { "stale_ms": 5000 } }
  ],
  "batch": { "max_samples": 256, "max_delay_us": 2000 },
  "transport": { "host": "127.0.0.1", "port": 9000, "max_pending_batches": 64, "max_frame_bytes": 1048576 },
  "health": { "stale_periods": 3, "stale_min_ms": 20, "rate_tolerance_pct": 20, "rate_window_ms": 1000, "rate_min_samples": 10 },
  "seed": 42
}
```

The top-level `health` block holds defaults expressed relative to each source's period (§7). A source's optional `health` block overrides them with absolute values (`stale_ms`, `rate_window_ms`, `rate_tolerance_pct`). With the example above, the effective stale thresholds are 20 ms (IMU, the floor), 300 ms (GPS), and 5000 ms (battery, overridden). The effective rate windows are 1 s (IMU, GPS) and 10 s (battery).

Configs shipped in `configs/`:
- `default.json`: the example above. Tuned for zero drops, not for minimum latency.
- `bench.json`: same sources with `max_delay_us` ≤ 500, used for the latency targets in §14.
- `stress.json`, `faults.json`: see §13 and §8.

Validate at startup:
- Ring capacity is a power of two, and rates are positive.
- Ports are in range.
- Source ids are unique and in 1–65535 (0 is reserved for the pipeline, §4.1).
- `block_with_timeout` has a `timeout_us`.
- The worst-case encoded batch fits in `max_frame_bytes` (§9).

Invalid config means a clear error and a nonzero exit code.

## 11. Build, tooling, and dependencies

- C++20, CMake ≥ 3.20 with `CMakePresets.json` presets: `debug`, `release`, `asan` (ASan + UBSan), and `tsan`.
- Dependencies come from Ubuntu system packages (apt) only, no vcpkg: Protobuf (+ `protoc`), GoogleTest/GMock, Google Benchmark, nlohmann/json, spdlog (or fmt). Find them with CMake `find_package`.
- Warnings: `-Wall -Wextra -Wpedantic -Werror` in CI.
- `.clang-format` and `.clang-tidy` (modernize-*, performance-*, bugprone-*, concurrency-*).
- GitHub Actions: gcc and clang × {debug, asan, tsan}. Run unit tests in all configurations, integration tests in debug, and a short benchmark smoke run in release.
- CI runner: use an `ubuntu-26.04` image to match the dev environment if GitHub offers one. Otherwise use `ubuntu-24.04` (older gcc/clang/gtest) and keep the code within what those compilers support. Install dependencies with the same apt package list as §11.1.
- TSan on recent Ubuntu kernels: high ASLR entropy makes TSan binaries abort at startup ("unexpected memory mapping"). Run `sudo sysctl vm.mmap_rnd_bits=28` in the CI job before the tests, and locally if TSan fails the same way.
- `vcan` tests (M7): GitHub-hosted runners may lack the `vcan` kernel module. Make these tests skip automatically when `vcan0` isn't available, and document local setup:
  ```bash
  sudo modprobe vcan
  sudo ip link add dev vcan0 type vcan
  sudo ip link set up vcan0
  ```

(Bazel is a reasonable alternative, but CMake is what most embedded/automotive teams and reviewers expect to see.)

### 11.1 Development environment: Windows host

The project targets Linux: it uses epoll, POSIX signals, and SocketCAN, and Linux is what embedded/automotive teams use. On Windows, develop inside **WSL2 (Ubuntu 26.04)** rather than porting to MSVC.

Setup:
```powershell
# PowerShell (admin). `wsl --list --online` shows the exact distro name.
wsl --install -d Ubuntu-26.04
```
```bash
# inside Ubuntu
sudo apt update
sudo apt install -y build-essential clang clang-tidy clang-format cmake ninja-build gdb \
  libprotobuf-dev protobuf-compiler libgtest-dev libgmock-dev libbenchmark-dev \
  nlohmann-json3-dev libspdlog-dev can-utils git
```

Rules:
- **Keep the repo in the Linux filesystem** (`~/src/tstream`), not under `/mnt/c/...`. Builds across the Windows filesystem boundary are much slower.
- Edit with VS Code using the WSL extension (or CLion with a WSL toolchain). Builds, tests, and debugging all run inside Linux.
- Run Claude Code from the WSL terminal inside the repo, so every command it runs (cmake, ctest, sanitizers) executes in Linux.
- Sanitizers (ASan, UBSan, TSan) work in WSL2 with gcc and clang.

Known WSL2 limitations:
- **vcan (M7):** confirmed unavailable on the dev machine (2026-10-07). The stock kernel `6.6.87.2-microsoft-standard-WSL2` has `CONFIG_CAN=m` and `CONFIG_CAN_RAW=m` but `# CONFIG_CAN_VCAN is not set` (check with `zcat /proc/config.gz | grep CONFIG_CAN`). Before M7, either build a custom WSL2 kernel with `CONFIG_CAN_VCAN=m` added, or run M7 in a full Ubuntu VM (Hyper-V or VirtualBox). Record the choice in `docs/design-decisions.md`. Everything before M7 works in plain WSL2.
- **Memory:** the WSL2 VM has much less RAM than CPU cores (7 GB vs 24 cores on the dev machine), so cap build parallelism (`cmake --build <dir> -j 8`) or raise `memory=` in `%UserProfile%\.wslconfig`. Sanitizer builds need the most memory.
- **Benchmarks:** WSL2 is a lightweight VM, so you can't control the CPU governor, and thread pinning is less meaningful. Benchmarks are still valid for relative comparisons (ring vs mutex, padding vs none). Note "WSL2 on Windows, <CPU model>" in `docs/benchmarks.md`, and treat absolute latency numbers as environment-specific.
- **Networking:** sender and receiver both run inside WSL2 over loopback, which avoids Windows firewall and port-forwarding issues.

## 12. Repository layout

```
tstream/
├── CMakeLists.txt
├── CMakePresets.json
├── README.md                 # the "portfolio page": design, results, trade-offs
├── docs/
│   ├── SPEC.md               # this document
│   ├── design-decisions.md   # ADR-style log of choices and why
│   └── benchmarks.md         # methodology + results + machine specs
├── proto/telemetry.proto
├── include/tstream/
│   ├── spsc_ring.hpp
│   ├── sample.hpp
│   ├── source.hpp
│   ├── health_monitor.hpp
│   ├── encoder.hpp
│   ├── framing.hpp
│   ├── transport.hpp
│   ├── stats.hpp
│   └── config.hpp
├── src/
│   ├── sources/  (sim_imu.cpp, sim_gps.cpp, sim_battery.cpp, replay.cpp, can_source.cpp)
│   ├── monitors/ (stale.cpp, rate.cpp, seq_gap.cpp, frozen.cpp)
│   ├── pipeline.cpp
│   ├── encoder.cpp
│   ├── framing.cpp
│   ├── tcp_transport.cpp
│   ├── stats.cpp
│   ├── config.cpp
│   └── main.cpp              # tstream (sender)
├── tools/receiver/main.cpp   # tstream-recv
├── tests/
│   ├── unit/
│   ├── stress/
│   ├── integration/
│   └── alloc/                # zero-allocation test binary
├── bench/
│   ├── bench_spsc.cpp
│   ├── bench_encode.cpp
│   └── bench_e2e.cpp
├── configs/  (default.json, bench.json, stress.json, faults.json)
└── .github/workflows/ci.yml
```

## 13. Testing strategy

| Layer | What | Examples |
|---|---|---|
| Unit | Each class in isolation | Ring semantics, framing encode/decode, config validation, each monitor's logic driven with synthetic timestamps |
| Mock-based | Pipeline with fake sources and transport (GMock) | Batch flushes on size and on timeout; health events are injected in order |
| Stress | Concurrency correctness | 100M-item SPSC ordering test; all sources at max rate for 60 s with no crash or leak |
| Integration | Real processes over loopback TCP | Sender + receiver run 10 s with zero seq gaps under normal load; graceful shutdown flushes the final batch; receiver restart triggers reconnect; under forced drops, the gaps the receiver sees for each source equal the sender's drop counters for that source (drop accounting reconciles; the test removes the forced backpressure before shutdown so trailing drops are followed by a delivered sample) |
| Fault injection | Each fault in §8 | `stall` → stale event; `slow_consumer` → drop counters increase and the sender stays alive |
| Allocation | §9 test | Zero allocations in steady state |
| Fuzz (optional) | Frame decoder | libFuzzer target on `framing::decode` |

Rule: every milestone ends with green tests under ASan and TSan.

## 14. Benchmarks and performance targets

Methodology goes in `docs/benchmarks.md`: CPU model, core count, kernel, compiler and flags, CPU governor, whether threads are pinned, run count, and warmup. Report medians across runs.

Benchmarks:
1. **SPSC ring:** throughput (ops/sec) and per-op latency vs mutex queues; with and without cached indices; with and without cache-line padding (to demonstrate false sharing).
2. **Encode:** batch encode time vs batch size (16 / 64 / 256 / 1024).
3. **End-to-end:** capture-to-receive latency p50 / p99 / p99.9 at aggregate rates of 1k, 10k, and 100k samples/sec; maximum sustainable throughput before drops begin.

Indicative targets (validate and adjust once measured — publish the real numbers, not these):
- SPSC throughput clearly above both mutex baselines, with the gap explained.
- Loopback end-to-end p99 under 1 ms at 10k samples/sec with `configs/bench.json` (`max_delay_us` ≤ 500). The default config's `max_delay_us` of 2000 alone exceeds this target.
- Zero drops with `configs/default.json` for a 10-minute run.

## 15. Milestones

Each milestone is a separate, reviewable chunk of work with acceptance criteria. Don't start the next one until the current one is green.

**M0 — Skeleton and CI**
- CMake project, presets, dependency setup, clang-format/tidy, empty test target, CI workflow running all configurations.
- Local WSL2 environment set up per §11.1. The vcan check is already done (unavailable, §11.1).
- Create `docs/design-decisions.md` with the first ADRs: apt-only dependencies, vcan unavailable on WSL2 (M7 needs a custom kernel or VM), the `kCacheLine` constant instead of `hardware_destructive_interference_size`, no `drop_oldest`, and the arena-based encoder plan.
- CI workflow includes the TSan `vm.mmap_rnd_bits` workaround (§11).
- ✅ CI is green on gcc and clang across debug, asan, and tsan.

**M1 — SPSC ring buffer**
- `SpscRing` with tests (unit and stress) and `bench_spsc` including mutex baselines.
- ✅ Stress test passes under TSan; benchmark results are recorded in `docs/benchmarks.md`.

**M2 — Simulated sources and the pipeline core**
- `Sample`, the `Source` interface, three simulated sources, the pipeline drain loop, the batcher, and a stdout sink.
- ✅ Running `tstream --config configs/default.json` prints batches; graceful shutdown works.

**M3 — Protobuf encoding, framing, TCP transport, receiver**
- Proto schema, encoder, framing, non-blocking TCP sender, and `tstream-recv` with seq-gap and latency stats.
- ✅ Integration test passes: 10 s run with zero gaps; receiver prints p50/p99 latency.

**M4 — Backpressure, drop policies, stats**
- Per-source policies, transport pending-batch limit, stats registry, and a periodic stats line.
- ✅ The `slow_consumer` test shows correct drop accounting; the sender never blocks indefinitely.

**M5 — Health monitors and fault injection**
- Stale, rate, and seq-gap monitors (frozen is optional), fault injector, and `configs/faults.json`.
- ✅ Each fault type has a passing integration test asserting the correct health event.

**M6 — Performance pass and write-up**
- Zero-allocation test, encode and end-to-end benchmarks, any optimizations they motivate, and README results.
- ✅ The allocation test passes; README contains an architecture diagram, results table, and design trade-offs.

**M7 — SocketCAN source (bridge to the CAN/UDS project)**
- `CanSource` reading `vcan0`, plus a test helper that writes frames with `cansend` (can-utils) or a small C++ writer.
- ✅ CAN frames flow end to end; the test skips cleanly when `vcan0` is absent.

**M8 — Stretch (pick at most one)**
- Shared-memory transport for same-host consumers, benchmarked against TCP.
- A source compiled for Zephyr's `native_sim` or QEMU Cortex-M, streaming over a socket or UART to `tstream`. This is the strongest firmware signal.
- A record/replay file format with an index for seeking.

## 16. README and resume output

The README is what reviewers actually read. It should include: a one-paragraph pitch, the architecture diagram, a "Design decisions" section (why SPSC rather than MPMC, why batch, why length-prefixed framing, the drop-policy trade-offs), a benchmark results table with machine specs, how to build and run, and a "Future work" section.

Resume bullet templates — fill in real numbers after M6 and don't invent any:
- Built a real-time C++20 telemetry pipeline streaming [N] concurrent sensor sources over TCP with Protobuf, sustaining [X] samples/sec at [Y] µs p99 end-to-end latency
- Designed a lock-free SPSC ring buffer with cache-line-isolated indices, achieving [Z]× the throughput of a mutex-based queue; verified ordering and race-freedom with stress tests under ThreadSanitizer
- Engineered an allocation-free steady-state hot path with configurable backpressure and drop policies, plus stale/rate/sequence-gap health monitoring validated through fault-injection tests
- Integrated a Linux SocketCAN source for emulated vehicle bus data, with CI across GCC/Clang and ASan/UBSan/TSan configurations

## 17. Interview talking points to be ready for

- Why `acquire`/`release` and not `seq_cst` or `relaxed`, and what would break with `relaxed`.
- False sharing: what it is, how you measured it, and the padding fix.
- Why SPSC per source instead of one MPMC queue.
- Batching trade-off: throughput vs latency, and how `max_delay_us` bounds latency.
- Nagle's algorithm and `TCP_NODELAY`; handling partial writes; framing over a byte stream.
- Where the hot path could still allocate, and how you proved it doesn't.
- What changes on a real microcontroller: no threads or an RTOS, no heap, ISR-to-task handoff (the same SPSC idea), DMA, and a constrained serializer such as nanopb.

## 18. Working agreement for Claude Code

- Work one milestone at a time. At the start of each, restate the acceptance criteria, then implement, then run the full test suite under the `asan` and `tsan` presets before calling it done.
- Write tests alongside the code. Don't mark anything complete with failing or skipped tests (except the documented `vcan` skip).
- Don't add dependencies beyond §11 without asking.
- Keep the hot path free of locks and allocations. Flag any change that might violate this.
- Record notable decisions in `docs/design-decisions.md` (context, decision, alternatives, consequences).
- Prefer clear code over clever code. This repo will be read by interviewers.
- **The author writes `SpscRing` (M1) and the health monitor interface personally.** Claude Code may review, test, and benchmark them, but should not write the first implementation. The author must be able to explain them from memory in interviews.

## 19. Assumptions and open questions

- Host machine is Windows. All development happens in WSL2 Ubuntu (see §11.1). M7 may need a custom WSL2 kernel or a Linux VM for `vcan`.
- No physical board. Everything is simulated or emulated by design.
- Timeline and weekly hours aren't decided yet, so milestones are ordered but not scheduled.
- Decided: JSON for config (familiarity; nlohmann/json is already a dependency).
- Open: whether M8 goes toward shared memory (systems roles) or Zephyr/QEMU (firmware roles).
