#include "tstream/framing.hpp"

namespace tstream::framing {

namespace {

// Once this many consumed bytes pile up at the front, move the rest down so
// the buffer doesn't grow without bound on a long-lived connection.
constexpr std::size_t kCompactThreshold = 64 * 1024;

// Reads a big-endian u16 from two bytes.
std::uint16_t load_be16(const std::byte* p) {
  return static_cast<std::uint16_t>((std::to_integer<std::uint16_t>(p[0]) << 8) |
                                    std::to_integer<std::uint16_t>(p[1]));
}

// Reads a big-endian u32 from four bytes.
std::uint32_t load_be32(const std::byte* p) {
  return (std::to_integer<std::uint32_t>(p[0]) << 24) |
         (std::to_integer<std::uint32_t>(p[1]) << 16) |
         (std::to_integer<std::uint32_t>(p[2]) << 8) | std::to_integer<std::uint32_t>(p[3]);
}

}  // namespace

// Matches the names used in receiver stats.
std::string_view frame_error_name(FrameError error) {
  switch (error) {
    case FrameError::BadMagic:
      return "bad_magic";
    case FrameError::BadVersion:
      return "bad_version";
    case FrameError::BadFlags:
      return "bad_flags";
    case FrameError::BadLength:
      return "bad_length";
  }
  return "unknown";
}

// Big-endian, field by field, so the layout doesn't depend on host byte order.
void write_header(std::span<std::byte, kHeaderBytes> out, std::uint32_t length) {
  out[0] = static_cast<std::byte>(kMagic >> 8);
  out[1] = static_cast<std::byte>(kMagic & 0xFF);
  out[2] = static_cast<std::byte>(kVersion);
  out[3] = std::byte{0};  // flags: reserved in v1
  out[4] = static_cast<std::byte>(length >> 24);
  out[5] = static_cast<std::byte>((length >> 16) & 0xFF);
  out[6] = static_cast<std::byte>((length >> 8) & 0xFF);
  out[7] = static_cast<std::byte>(length & 0xFF);
}

// Checks fields in wire order, so the first problem is the one reported.
HeaderCheck parse_header(std::span<const std::byte, kHeaderBytes> header,
                         std::size_t max_payload_bytes) {
  if (load_be16(header.data()) != kMagic) {
    return {.ok = false, .error = FrameError::BadMagic};
  }
  if (std::to_integer<std::uint8_t>(header[2]) != kVersion) {
    return {.ok = false, .error = FrameError::BadVersion};
  }
  if (std::to_integer<std::uint8_t>(header[3]) != 0) {
    return {.ok = false, .error = FrameError::BadFlags};
  }
  const std::uint32_t length = load_be32(header.data() + 4);
  if (length == 0 || length > max_payload_bytes) {
    return {.ok = false, .error = FrameError::BadLength};
  }
  return {.ok = true, .length = length};
}

// Reserves room for one maximal frame up front, so steady-state feeds rarely grow it.
FrameDecoder::FrameDecoder(std::size_t max_payload_bytes) : max_payload_bytes_(max_payload_bytes) {
  buffer_.reserve(kHeaderBytes + max_payload_bytes + kCompactThreshold);
}

// Compacts first (which is why earlier payloads become invalid), then appends.
void FrameDecoder::feed(std::span<const std::byte> bytes) {
  if (begin_ == buffer_.size()) {
    buffer_.clear();
    begin_ = 0;
  } else if (begin_ >= kCompactThreshold) {
    buffer_.erase(buffer_.begin(), buffer_.begin() + static_cast<std::ptrdiff_t>(begin_));
    begin_ = 0;
  }
  buffer_.insert(buffer_.end(), bytes.begin(), bytes.end());
}

// Validates the header as soon as all 8 bytes are in, before waiting for the payload.
FrameDecoder::Next FrameDecoder::next() {
  if (failed_) {
    return {.status = Status::Error, .error = error_};
  }
  if (buffered() < kHeaderBytes) {
    return {};
  }
  const std::span<const std::byte, kHeaderBytes> header(buffer_.data() + begin_, kHeaderBytes);
  const HeaderCheck check = parse_header(header, max_payload_bytes_);
  if (!check.ok) {
    failed_ = true;
    error_ = check.error;
    return {.status = Status::Error, .error = error_};
  }
  if (buffered() < kHeaderBytes + check.length) {
    return {};
  }
  const std::span<const std::byte> payload(buffer_.data() + begin_ + kHeaderBytes, check.length);
  begin_ += kHeaderBytes + check.length;
  return {.status = Status::Frame, .payload = payload};
}

// Keeps the reserved capacity for the next connection.
void FrameDecoder::reset() {
  buffer_.clear();
  begin_ = 0;
  failed_ = false;
}

}  // namespace tstream::framing
