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
