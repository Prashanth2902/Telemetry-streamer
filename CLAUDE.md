cd ~/src/telemetry-streamer
cat > CLAUDE.md <<'EOF'
# telemetry-streamer

Full specification: docs/SPEC.md. Read it before starting any work.

- Work one milestone at a time (M0, M1, ...). State the acceptance criteria before starting.
- Environment: WSL2 Ubuntu, C++20, CMake + Ninja, gcc and clang.
- A milestone is done only when all tests pass under the asan and tsan presets.
- Keep the hot path free of locks and heap allocations; flag anything that might break this.
- Do not add dependencies beyond SPEC.md §11 without asking.
- I write SpscRing (M1) and the HealthMonitor interface myself. Review and test them, but don't write the first version.
- Use `cmake --build <dir> -j 8` (WSL memory is limited).
- Record design decisions in docs/design-decisions.md.
EOF
