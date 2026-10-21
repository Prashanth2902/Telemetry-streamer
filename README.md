# telemetry-streamer
Real-time C++20 telemetry pipeline. Work in progress

## Build

Requires Linux (developed on WSL2 Ubuntu 26.04). Install the toolchain and dependencies:

```bash
sudo apt install -y build-essential clang clang-tidy clang-format cmake ninja-build gdb \
  libprotobuf-dev protobuf-compiler libgtest-dev libgmock-dev libbenchmark-dev \
  nlohmann-json3-dev libspdlog-dev can-utils
```

Then:

```bash
cmake --preset debug          # configure (gcc); clang variants: clang-debug, clang-asan, ...
cmake --build --preset debug  # build
ctest --preset debug          # test
```

| Preset | Compiler | Build type | Notes |
|---|---|---|---|
| `debug` / `clang-debug` | gcc / clang | Debug | |
| `release` / `clang-release` | gcc / clang | Release | also builds benchmarks |
| `asan` / `clang-asan` | gcc / clang | Debug | ASan + UBSan |
| `tsan` / `clang-tsan` | gcc / clang | RelWithDebInfo | ThreadSanitizer |

Add `-DTSTREAM_WARNINGS_AS_ERRORS=ON` at configure time to build with `-Werror`, as CI does.

If TSan binaries abort at startup with "unexpected memory mapping", run `sudo sysctl vm.mmap_rnd_bits=28`.

If a sanitizer report or gdb hangs for minutes, it's fetching debug info from `debuginfod.ubuntu.com`. The test presets already disable that. When running binaries by hand, use `DEBUGINFOD_URLS= ./build/tsan/tests/<test>`.

## Run

Start the receiver, then the sender (it connects to `transport.host:port` from the config, and retries until the receiver is up):

```bash
cmake --preset release && cmake --build --preset release
./build/release/tools/tstream-recv --port 9000                          # terminal 1, until Ctrl-C
./build/release/src/tstream --config configs/default.json --duration 10 # terminal 2
```

The receiver prints a stats line per second and a summary on exit (latencies in µs; reported only when the sender is on the same host):

```
stats t=1.0s samples=942 rate=942/s batches=311 gaps=0 out_of_order=0 frame_errors=0 decode_errors=0 e2e_us p50=1376.3 p99=2490.4 ...
recv summary samples=3043 batches=1006 gaps=0 out_of_order=0 batch_seq_gaps=0 frame_errors=0 ...
recv health events=0 stale=0 rate=0 seq_gap=0 frozen=0 unknown=0
recv source=1 samples=3008 gaps=0 out_of_order=0
```

The sender prints a stats line every second (`--stats-interval`, 0 turns it off), then a per-source summary and its transport counters; logs go to stderr:

```
stats t=1.0s generated=1017 rate=1014/s ring_dropped=0 delivered=1016 batches=335 health=0 tx_sent=1016 tx_dropped=0 queued=0 queue_high_water=0 connects=1
summary source=1 type=sim_imu generated=3009 dropped_newest=0 dropped_timeout=0 blocked=0 delivered=3009 tx_sent=3009 tx_dropped=0 lost=0 overruns=0
summary total generated=3044 dropped=0 delivered=3044 tx_sent=3044 tx_dropped=0 lost=0 batches=1005 health_events=0
summary transport batches_sent=1005 samples_sent=3044 bytes_sent=157775 batches_dropped=0 samples_dropped=0 queue_high_water=0 connects=1 disconnects=0
```

Every sample is accounted for: per source, `generated` = ring drops (`dropped_newest` + `dropped_timeout`, by the source's `policy`) + `tx_dropped` (the transport's queue of `max_pending_batches` was full) + `tx_sent`. `lost` is whatever is left, and must be 0.

### Forcing backpressure

`tstream-recv --slow-consumer-ms 50 --slow-for 3` reads 1 KiB per 50 ms for its first 3 s, then catches up. With a small `transport.socket_send_buffer_bytes` (e.g. 8192) in the sender's config, its queue fills within a second and it starts dropping whole batches. Each dropped sample then shows up at the receiver as exactly one gap for that source: compare the sender's `summary source=N ... tx_dropped=` with the receiver's `recv source=N ... gaps=`.

### Health monitoring

The pipeline thread runs four monitors over every sample it batches. Each raises an event once when a problem starts, and a `cleared` event when it ends. Events travel in the stream as samples from source 0, right behind the data that caused them, and `tstream-recv` prints each one as it arrives:

```
health t=2.519s source=2 monitor=stale severity=critical detail=300
health t=3.317s source=2 monitor=stale severity=cleared detail=1099
```

| Monitor | Raised when | `detail` |
|---|---|---|
| `stale` (critical) | no sample for longer than the source's stale threshold | ms since the last sample; on `cleared`, the whole outage |
| `rate` (warning) | the rate over the last rate window is outside ±`rate_tolerance_pct` | measured rate in mHz |
| `seq_gap` (warning) | seqs went missing upstream (ring drops); reported at most every 100 ms per source | samples missing since the last report |
| `frozen` (warning) | `health.frozen_count` (default 20) identical payloads in a row | run length |

Thresholds are per source: stale = max(`stale_min_ms`, `stale_periods` × period), and the rate window = max(`rate_window_ms`, `rate_min_samples` × period). A source's `health` block can override `stale_ms`, `rate_window_ms`, and `rate_tolerance_pct`. With the default config, the IMU goes stale after 20 ms, the GPS after 300 ms, and the battery after 5 s.

### Fault injection

Faults make the monitors and drop policies fire on demand. Pass them with `--fault` (repeatable), or list them in the config's `"faults"` array; both use the same syntax. Times count from when the sources start:

| Fault | Effect | Shows up as |
|---|---|---|
| `stall:<id>:<ms>[@start]` | the source produces nothing for `ms` (no seqs consumed) | `stale` raised and cleared; `rate` |
| `burst:<id>:<n>[@start]` | `n` (≤ 100,000) extra samples hit the ring at once | `dropped_newest`, `seq_gap`, receiver gaps; `rate` |
| `rate:<id>:<factor>[@start[+dur]]` | the source's rate × `factor` | `rate` |
| `freeze:<id>[@start[+dur]]` | the payload repeats (new seqs) | `frozen` raised and cleared |
| `slow_consumer:<ms>` | `tstream-recv --fault slow_consumer:<ms>`: read 1 KiB per `ms` | transport drops (`tx_dropped`) |

`configs/faults.json` schedules one of each sender fault over ten seconds:

```bash
./build/release/tools/tstream-recv --port 9000
./build/release/src/tstream --config configs/faults.json --duration 10
```

A burst's payloads are generated before it's pushed, so the source pauses for that long first. In debug builds, a 20,000-sample burst can take long enough to raise and clear a `stale` event on the IMU.

Without a receiver, `tstream --config configs/default.json --sink stdout` prints one line per batch instead of sending.

Exit codes: `tstream`: `0` clean, `1` usage or startup error, `2` invalid config, `3` samples unaccounted for (`lost` > 0). `tstream-recv`: `0` clean, `1` usage error, `2` cannot listen.
