// libFuzzer target for the frame decoder (SPEC §4.3, §13 "Fuzz").
//
// Build: cmake --preset clang-asan -DTSTREAM_BUILD_FUZZ=ON && cmake --build --preset clang-asan
// Run:   ./build/clang-asan/fuzz/framing_fuzz -max_total_time=60
//
// The first input byte picks a chunk size, so the fuzzer also explores how the
// stream is split across feed() calls. ASan/UBSan catch memory errors; the
// asserts check the decoder's own invariants.

#include <algorithm>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <span>

#include "tstream/framing.hpp"

// Called by libFuzzer once per generated input.
extern "C" int LLVMFuzzerTestOneInput(const std::uint8_t* data, std::size_t size) {
  if (size == 0) {
    return 0;
  }
  constexpr std::size_t kMaxPayload = 1 << 16;
  const std::size_t chunk = 1 + data[0] % 32;
  const auto bytes = std::as_bytes(std::span(data + 1, size - 1));

  tstream::framing::FrameDecoder decoder(kMaxPayload);
  bool failed = false;
  for (std::size_t pos = 0; pos < bytes.size(); pos += chunk) {
    decoder.feed(bytes.subspan(pos, std::min(chunk, bytes.size() - pos)));
    for (auto r = decoder.next(); r.status != tstream::framing::FrameDecoder::Status::NeedMore;
         r = decoder.next()) {
      if (r.status == tstream::framing::FrameDecoder::Status::Error) {
        failed = true;
        break;
      }
      assert(!failed);
      assert(!r.payload.empty() && r.payload.size() <= kMaxPayload);
    }
    assert(decoder.failed() == failed);  // once failed, stays failed
    if (failed) {
      break;
    }
  }
  return 0;
}
