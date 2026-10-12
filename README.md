# telemetry-streamer
Real-time C++20 telemetry pipeline. Work in progress

## Build

Requires Linux (developed on WSL2 Ubuntu 26.04) and the packages in `docs/SPEC.md` §11.1.

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

```bash
cmake --preset release && cmake --build --preset release
./build/release/src/tstream --config configs/default.json              # until Ctrl-C
./build/release/src/tstream --config configs/default.json --duration 5 # for 5 seconds
```

stdout gets one line per batch, then a per-source summary; logs go to stderr:

```
batch 0: 4 samples (imu 2, gps 1, battery 1, can 0, health 0) max_age_us=2103
...
summary source=1 type=sim_imu generated=2007 dropped=0 delivered=2007 lost=0 overruns=0
summary total generated=2031 dropped=0 delivered=2031 lost=0 batches=671
```

Exit codes: `0` clean, `1` usage error, `2` invalid config, `3` samples unaccounted for (`lost` > 0).
