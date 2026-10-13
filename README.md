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
recv source=1 samples=3008 gaps=0 out_of_order=0
```

The sender prints a per-source summary and its transport counters; logs go to stderr:

```
summary source=1 type=sim_imu generated=3008 dropped=0 delivered=3008 lost=0 overruns=0
summary total generated=3043 dropped=0 delivered=3043 lost=0 batches=1006
summary transport batches_sent=1006 samples_sent=3043 bytes_sent=157747 batches_dropped=0 samples_dropped=0 connects=1 disconnects=0
```

Without a receiver, `tstream --config configs/default.json --sink stdout` prints one line per batch instead of sending.

Exit codes: `tstream`: `0` clean, `1` usage or startup error, `2` invalid config, `3` samples unaccounted for (`lost` > 0). `tstream-recv`: `0` clean, `1` usage error, `2` cannot listen.
