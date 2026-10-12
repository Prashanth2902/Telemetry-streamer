# Design decisions

ADR-style log. Each entry records the context, the decision, the alternatives considered, and the consequences. New entries go at the end; superseded entries stay, marked as such.

---

## ADR-001: Dependencies from Ubuntu packages only

**Status:** Accepted (M0, 2026-10-12)

**Context.** SPEC §11 allowed either system packages or vcpkg. The dev environment is WSL2 Ubuntu 26.04, which ships every dependency the spec lists: Protobuf 3.21.12, GoogleTest/GMock 1.17.0, Google Benchmark 1.9.1, nlohmann/json 3.11.3, spdlog 1.15.3. The GitHub `ubuntu-26.04` runner installs the same packages.

**Decision.** Use apt packages only, found with CMake `find_package`. No vcpkg, no FetchContent.

**Alternatives.**
- vcpkg: pins exact versions and works on other distros, but adds a bootstrap step, a manifest, and long first builds in CI, all for versions apt already provides.
- FetchContent: same build-time cost, and every dependency gets compiled with our warning and sanitizer flags.

**Consequences.** Versions are whatever Ubuntu 26.04 ships, and CI uses the same package list (§11.1), so local and CI match. The project doesn't build on other distros without extra work, which is acceptable for a Linux-only project. GoogleTest from apt isn't sanitizer-instrumented; that's fine for ASan and TSan because the code under test is instrumented.

---

## ADR-002: vcan is unavailable on the WSL2 kernel

**Status:** Accepted (M0, 2026-10-12). The M7 environment choice is deferred.

**Context.** M7 needs the `vcan` kernel module. The stock WSL2 kernel on the dev machine is `6.6.87.2-microsoft-standard-WSL2`. Its config (`zcat /proc/config.gz`) has `CONFIG_CAN=m` and `CONFIG_CAN_RAW=m`, but `# CONFIG_CAN_VCAN is not set`, so `modprobe vcan` cannot succeed.

**Decision.** M0–M6 run in plain WSL2. Before M7 starts, pick one of the alternatives below and record it here as a new ADR.

**Alternatives (for M7).**
- Custom WSL2 kernel: build Microsoft's WSL2 kernel source with `CONFIG_CAN_VCAN=m` and point `.wslconfig` at it. Keeps one environment, but the kernel has to be rebuilt after WSL updates.
- Full Ubuntu VM (Hyper-V or VirtualBox): stock Ubuntu kernels include vcan. A second environment to keep in sync.

**Consequences.** `vcan` tests must skip cleanly when `vcan0` is absent (SPEC §11). They will skip both locally and on GitHub-hosted runners until M7's environment exists.

---

## ADR-003: Fixed `kCacheLine = 64` instead of `std::hardware_destructive_interference_size`

**Status:** Accepted (M0, 2026-10-12)

**Context.** `SpscRing` keeps `head_` and `tail_` on separate cache lines to avoid false sharing. The standard constant seems like the natural choice, but GCC treats it as ABI-unstable: it emits `-Winterference-size` when the constant is used in a header, and that fails the build under `-Werror`. Its value can also differ between compilers and `-mtune` settings.

**Decision.** Define a project constant `kCacheLine = 64` and use `alignas(kCacheLine)`.

**Alternatives.**
- The standard constant with `-Wno-interference-size`: silences a warning that exists for a real reason (layout can change with flags).
- 128: covers adjacent-line prefetching on some Intel CPUs, at the cost of more padding. M1 benchmarks 64 vs 128.

**Consequences.** The layout is the same on every compiler and flag set. If M1 benchmarks show 128 is meaningfully faster, change the constant in one place and record a new ADR.

---

## ADR-004: No `drop_oldest` backpressure policy

**Status:** Accepted (M0, 2026-10-12)

**Context.** SPEC §6 originally listed `drop_oldest`. Evicting the oldest entry from a full ring means the producer advances `head_`, but the consumer owns `head_`. An SPSC ring is correct because each index has exactly one writer.

**Decision.** Support only `drop_newest` and `block_with_timeout`.

**Alternatives.**
- Producer CASes `head_`: makes both sides contend on `head_`, so it's no longer SPSC, and it's easy to get subtly wrong.
- An overwrite ring with per-slot sequence numbers (the consumer detects overwritten slots): a different data structure with different correctness arguments. Out of scope for the centerpiece queue.
- An MPMC queue: heavier, and gives up the per-source ordering and simplicity that motivated SPSC.

**Consequences.** The ring stays simple enough to explain line by line. Fresh-data-preferred sources use `drop_newest` with a large enough ring. Every drop is still counted, and shows downstream as a sequence gap (SPEC §4.1).

---

## ADR-005: Encode on a Protobuf arena with a preallocated initial block

**Status:** Proposed (M0, 2026-10-12). Validate in M3 with the allocation test.

**Context.** The hot path must not allocate (SPEC §9). Reusing a heap-allocated `Batch` doesn't achieve this: `Sample` has a `oneof`, and clearing it or switching its case deletes and reallocates the submessage. Batches mix kinds, so this happens on every batch.

**Decision.** Build each `Batch` on a `google::protobuf::Arena` whose initial block (`ArenaOptions::initial_block`) is an encoder-owned buffer allocated at startup. Call `Reset()` after each serialization. Serialize into a preallocated buffer sized to the worst-case batch.

**Alternatives.**
- Hand-written encoding with `CodedOutputStream`: guaranteed allocation-free and the fastest option, but duplicates the schema in code. This is the fallback if the arena path still allocates.
- Heap `Batch` with `Clear()`: allocates whenever a oneof case changes. Rejected.

**Consequences.** The initial block must be large enough for `max_samples`. If a batch overflows it, the arena allocates, and the allocation test will catch that. Record the outcome here after M3.

---

## ADR-006: Presets per compiler

**Status:** Accepted (M0, 2026-10-12)

**Context.** SPEC §11 names presets `debug`, `release`, `asan`, `tsan`, and CI runs both gcc and clang. Choosing the compiler through `CXX` would make both compilers share `build/<preset>`, and build or test presets couldn't tell them apart.

**Decision.** The spec's preset names use gcc (`g++`). Clang variants have a `clang-` prefix: `clang-debug`, `clang-release`, `clang-asan`, `clang-tsan`. Each preset builds in `build/<preset>`. Build presets use `jobs: 8`, because the WSL2 VM has 7 GB of RAM for 24 cores.

**Alternatives.**
- Compiler-neutral presets plus `-B` overrides: works for configure, but build and test presets are tied to the configure preset's directory.
- A toolchain file per compiler: more files for what is a one-variable difference.

**Consequences.** There are 8 configure/build/test presets, generated from small hidden mixins (`gcc`/`clang` × `cfg-*`). The presets use unversioned `g++` and `clang++`, which resolve to gcc 15 and clang 21 on Ubuntu 26.04.

---

## ADR-007: CI on the `ubuntu-26.04` runner

**Status:** Accepted (M0, 2026-10-12)

**Context.** The dev environment is Ubuntu 26.04. GitHub's `ubuntu-26.04` image is a public preview (image 20260927) with gcc 15.2 and clang 21.1.8. `ubuntu-24.04` has older compilers (default gcc 13) and older library packages.

**Decision.** Run all CI jobs on `ubuntu-26.04` and install dependencies with the §11.1 apt list.

**Alternatives.**
- `ubuntu-24.04`: GA and stable, but its toolchain and packages differ from local, so "works locally, fails in CI" becomes more likely.
- A container (`ubuntu:26.04`) on `ubuntu-latest`: exact match, but the TSan `sysctl vm.mmap_rnd_bits` workaround needs host privileges.

**Consequences.** Preview images can change or break. If that blocks CI, switch to `ubuntu-24.04` and keep the code within gcc 13 / clang 18 support. This is one line per job in `.github/workflows/ci.yml`.

---

## ADR-008: How sanitizers and warnings are applied

**Status:** Accepted (M0, 2026-10-12)

**Context.** Sanitizer instrumentation has to be consistent across everything we compile, while `-Werror` should only cover our own code.

**Decision.**
- Sanitizers are global (`add_compile_options`/`add_link_options` from `TSTREAM_SANITIZER=none|address|thread`), so every target in the build is instrumented the same way.
- `asan` combines ASan and UBSan (`-fno-sanitize-recover=undefined`) and builds in Debug.
- `tsan` builds in RelWithDebInfo, because TSan slows code down 5–15× and the 100M-item stress test (M1) needs optimization.
- Warnings are per target (`tstream_set_warnings`): the spec's `-Wall -Wextra -Wpedantic`, plus shadow, conversion, old-style-cast, and similar. `-Werror` is behind `TSTREAM_WARNINGS_AS_ERRORS`, which is on in CI and off by default locally.
- Test presets set `ASAN_OPTIONS`, `UBSAN_OPTIONS`, and `TSAN_OPTIONS` so failures abort with stack traces.
- Test presets set `DEBUGINFOD_URLS=""`. Ubuntu sets it to `https://debuginfod.ubuntu.com` by default, and then `llvm-symbolizer` tries to download debug info while printing a sanitizer report. Measured on the dev machine: one clang TSan race report took 181 s with it, and 0.9 s with it empty, with the same `file:line` stack traces, since our binaries carry their own debug info.

**Alternatives.** Warnings as global flags would also hit generated Protobuf code (M3) and fail `-Werror` on code we don't control.

**Consequences.** Every new target must call `tstream_set_warnings`. The `tstream_add_test` and `tstream_add_bench` helpers do this already. Generated Protobuf sources (M3) will get their own target without the project warnings.
