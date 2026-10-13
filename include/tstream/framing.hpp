// Length-prefixed framing over a TCP byte stream (SPEC §4.3).
//
//   | magic u16 = 0x7473 | version u8 = 1 | flags u8 = 0 | length u32 | payload |
//
// All header fields are big-endian. `length` counts the payload only. A frame
// with a bad header can't be skipped, because the stream can't be
// resynchronized: the decoder stops, and the receiver closes the connection.

#pragma once

#include <cstddef>
#include <cstdint>
#include <span>
#include <string_view>
#include <vector>

namespace tstream::framing {

inline constexpr std::size_t kHeaderBytes = 8;
inline constexpr std::uint16_t kMagic = 0x7473;  // "ts" on the wire
inline constexpr std::uint8_t kVersion = 1;

enum class FrameError : std::uint8_t { BadMagic, BadVersion, BadFlags, BadLength };
inline constexpr std::size_t kFrameErrorCount = 4;

// Short name for logs and stats, e.g. "bad_magic".
std::string_view frame_error_name(FrameError error);

// Writes the header for a `length`-byte payload into `out`.
void write_header(std::span<std::byte, kHeaderBytes> out, std::uint32_t length);

// Result of checking one header: either an error, or the payload length.
struct HeaderCheck {
  bool ok = false;
  FrameError error = FrameError::BadMagic;  // meaningful only if !ok
  std::uint32_t length = 0;                 // meaningful only if ok
};

// Validates magic, version, flags, and 0 < length <= max_payload_bytes.
HeaderCheck parse_header(std::span<const std::byte, kHeaderBytes> header,
                         std::size_t max_payload_bytes);

// Reassembles frames from arbitrarily split chunks of a byte stream.
//
// feed() appends bytes; next() returns complete frames one at a time. A
// returned payload points into the decoder's buffer and stays valid until the
// next feed() or reset(). After an error the decoder stays failed until reset(),
// so a broken stream is never parsed past the bad header.
class FrameDecoder {
 public:
  enum class Status : std::uint8_t { NeedMore, Frame, Error };

  struct Next {
    Status status = Status::NeedMore;
    FrameError error = FrameError::BadMagic;  // meaningful only for Error
    std::span<const std::byte> payload{};     // meaningful only for Frame
  };

  // Frames whose payload exceeds max_payload_bytes are rejected as BadLength.
  explicit FrameDecoder(std::size_t max_payload_bytes);

  // Appends received bytes. Invalidates payloads returned earlier.
  void feed(std::span<const std::byte> bytes);
  // The next complete frame, NeedMore if a frame is still partial, or Error.
  [[nodiscard]] Next next();
  // Discards buffered bytes and any error, e.g. for a new connection.
  void reset();

  // Bytes received but not yet returned as frames.
  [[nodiscard]] std::size_t buffered() const { return buffer_.size() - begin_; }
  // True once a bad header has been seen (until reset()).
  [[nodiscard]] bool failed() const { return failed_; }

 private:
  std::size_t max_payload_bytes_;
  std::vector<std::byte> buffer_;
  std::size_t begin_ = 0;  // start of the first unconsumed byte in buffer_
  bool failed_ = false;
  FrameError error_ = FrameError::BadMagic;
};

}  // namespace tstream::framing
