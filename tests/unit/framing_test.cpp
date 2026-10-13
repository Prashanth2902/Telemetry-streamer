// Frame header encoding and stream reassembly (SPEC §4.3).

#include "tstream/framing.hpp"

#include <array>
#include <cstddef>
#include <cstdint>
#include <random>
#include <span>
#include <vector>

#include <gtest/gtest.h>

namespace {

using tstream::framing::FrameDecoder;
using tstream::framing::FrameError;
using tstream::framing::kHeaderBytes;
using Status = FrameDecoder::Status;

constexpr std::size_t kMaxPayload = 4096;

// A complete frame (header + payload) whose payload bytes are 0, 1, 2, ...
// offset by `fill`, so payloads from different frames are distinguishable.
std::vector<std::byte> make_frame(std::size_t payload_size, std::uint8_t fill = 0) {
  std::vector<std::byte> frame(kHeaderBytes + payload_size);
  tstream::framing::write_header(std::span<std::byte, kHeaderBytes>(frame.data(), kHeaderBytes),
                                 static_cast<std::uint32_t>(payload_size));
  for (std::size_t i = 0; i < payload_size; ++i) {
    frame[kHeaderBytes + i] = static_cast<std::byte>((i + fill) & 0xFF);
  }
  return frame;
}

// True if `payload` is what make_frame(payload.size(), fill) put in.
bool payload_matches(std::span<const std::byte> payload, std::uint8_t fill) {
  for (std::size_t i = 0; i < payload.size(); ++i) {
    if (payload[i] != static_cast<std::byte>((i + fill) & 0xFF)) {
      return false;
    }
  }
  return true;
}

// The 8 header bytes of `frame`, for parse_header.
std::span<const std::byte, kHeaderBytes> header_of(const std::vector<std::byte>& frame) {
  return std::span<const std::byte, kHeaderBytes>(frame.data(), kHeaderBytes);
}

TEST(Framing, HeaderIsBigEndianWithMagicVersionAndZeroFlags) {
  const auto frame = make_frame(0x0102);
  const std::array<std::byte, kHeaderBytes> expected{
      std::byte{0x74}, std::byte{0x73}, std::byte{1},    std::byte{0},
      std::byte{0x00}, std::byte{0x00}, std::byte{0x01}, std::byte{0x02}};
  for (std::size_t i = 0; i < kHeaderBytes; ++i) {
    EXPECT_EQ(frame[i], expected[i]) << "byte " << i;
  }
}

TEST(Framing, ParseHeaderRoundTripsLength) {
  for (const std::size_t length :
       {std::size_t{1}, std::size_t{255}, std::size_t{256}, kMaxPayload}) {
    const auto check = tstream::framing::parse_header(header_of(make_frame(length)), kMaxPayload);
    ASSERT_TRUE(check.ok) << length;
    EXPECT_EQ(check.length, length);
  }
}

TEST(Framing, ParseHeaderRejectsEachBadField) {
  auto frame = make_frame(10);
  frame[0] = std::byte{0x00};
  EXPECT_EQ(tstream::framing::parse_header(header_of(frame), kMaxPayload).error,
            FrameError::BadMagic);

  frame = make_frame(10);
  frame[2] = std::byte{2};
  EXPECT_EQ(tstream::framing::parse_header(header_of(frame), kMaxPayload).error,
            FrameError::BadVersion);

  frame = make_frame(10);
  frame[3] = std::byte{1};
  EXPECT_EQ(tstream::framing::parse_header(header_of(frame), kMaxPayload).error,
            FrameError::BadFlags);

  const auto empty = make_frame(0);
  EXPECT_EQ(tstream::framing::parse_header(header_of(empty), kMaxPayload).error,
            FrameError::BadLength);
  const auto too_big = make_frame(kMaxPayload + 1);
  EXPECT_EQ(tstream::framing::parse_header(header_of(too_big), kMaxPayload).error,
            FrameError::BadLength);
}

// The first bad field in wire order is the one reported.
TEST(Framing, ParseHeaderReportsFirstBadField) {
  auto frame = make_frame(0);  // bad length
  frame[0] = std::byte{0};     // and bad magic
  frame[3] = std::byte{9};     // and bad flags
  EXPECT_EQ(tstream::framing::parse_header(header_of(frame), kMaxPayload).error,
            FrameError::BadMagic);
}

TEST(Framing, ErrorNamesAreDistinct) {
  EXPECT_EQ(tstream::framing::frame_error_name(FrameError::BadMagic), "bad_magic");
  EXPECT_EQ(tstream::framing::frame_error_name(FrameError::BadVersion), "bad_version");
  EXPECT_EQ(tstream::framing::frame_error_name(FrameError::BadFlags), "bad_flags");
  EXPECT_EQ(tstream::framing::frame_error_name(FrameError::BadLength), "bad_length");
}

TEST(FrameDecoder, NeedsMoreUntilFrameIsComplete) {
  FrameDecoder decoder(kMaxPayload);
  EXPECT_EQ(decoder.next().status, Status::NeedMore);
  const auto frame = make_frame(100);
  decoder.feed(std::span(frame).first(kHeaderBytes + 99));
  EXPECT_EQ(decoder.next().status, Status::NeedMore);
  decoder.feed(std::span(frame).last(1));
  const auto result = decoder.next();
  ASSERT_EQ(result.status, Status::Frame);
  EXPECT_EQ(result.payload.size(), 100U);
  EXPECT_TRUE(payload_matches(result.payload, 0));
  EXPECT_EQ(decoder.next().status, Status::NeedMore);
  EXPECT_EQ(decoder.buffered(), 0U);
}

TEST(FrameDecoder, ReturnsSeveralFramesFromOneFeed) {
  std::vector<std::byte> stream;
  for (std::uint8_t i = 0; i < 5; ++i) {
    const auto frame = make_frame(10U + i, i);
    stream.insert(stream.end(), frame.begin(), frame.end());
  }
  FrameDecoder decoder(kMaxPayload);
  decoder.feed(stream);
  for (std::uint8_t i = 0; i < 5; ++i) {
    const auto result = decoder.next();
    ASSERT_EQ(result.status, Status::Frame) << int{i};
    EXPECT_EQ(result.payload.size(), 10U + i);
    EXPECT_TRUE(payload_matches(result.payload, i));
  }
  EXPECT_EQ(decoder.next().status, Status::NeedMore);
}

// TCP may split the stream anywhere: try every split point of two frames.
TEST(FrameDecoder, HandlesEverySplitPoint) {
  std::vector<std::byte> stream = make_frame(20, 1);
  const auto second = make_frame(7, 2);
  stream.insert(stream.end(), second.begin(), second.end());

  for (std::size_t split = 0; split <= stream.size(); ++split) {
    FrameDecoder decoder(kMaxPayload);
    std::vector<std::size_t> sizes;
    decoder.feed(std::span(stream).first(split));
    for (auto r = decoder.next(); r.status == Status::Frame; r = decoder.next()) {
      sizes.push_back(r.payload.size());
    }
    decoder.feed(std::span(stream).subspan(split));
    for (auto r = decoder.next(); r.status == Status::Frame; r = decoder.next()) {
      sizes.push_back(r.payload.size());
    }
    EXPECT_EQ(sizes, (std::vector<std::size_t>{20, 7})) << "split at " << split;
  }
}

TEST(FrameDecoder, StaysFailedAfterBadHeaderUntilReset) {
  FrameDecoder decoder(kMaxPayload);
  auto bad = make_frame(10);
  bad[2] = std::byte{7};  // bad version
  decoder.feed(bad);
  const auto first = decoder.next();
  ASSERT_EQ(first.status, Status::Error);
  EXPECT_EQ(first.error, FrameError::BadVersion);
  EXPECT_TRUE(decoder.failed());

  // A valid frame after a bad header is never parsed on the same stream.
  decoder.feed(make_frame(5));
  EXPECT_EQ(decoder.next().status, Status::Error);

  decoder.reset();
  EXPECT_FALSE(decoder.failed());
  decoder.feed(make_frame(5));
  EXPECT_EQ(decoder.next().status, Status::Frame);
}

TEST(FrameDecoder, RejectsOversizedLengthBeforePayloadArrives) {
  FrameDecoder decoder(kMaxPayload);
  const auto frame = make_frame(kMaxPayload + 1);
  decoder.feed(std::span(frame).first(kHeaderBytes));  // header only
  const auto result = decoder.next();
  ASSERT_EQ(result.status, Status::Error);
  EXPECT_EQ(result.error, FrameError::BadLength);
}

// Many frames through one decoder: consumed bytes get compacted away, so the
// buffer doesn't grow with the total amount of data seen.
TEST(FrameDecoder, LongStreamDoesNotGrowBuffer) {
  FrameDecoder decoder(kMaxPayload);
  const auto frame = make_frame(1000, 3);
  for (int i = 0; i < 2000; ++i) {  // ~2 MB total
    decoder.feed(frame);
    const auto result = decoder.next();
    ASSERT_EQ(result.status, Status::Frame);
    ASSERT_TRUE(payload_matches(result.payload, 3));
  }
  EXPECT_EQ(decoder.buffered(), 0U);
}

// Randomized: valid streams chunked arbitrarily decode exactly; corrupted
// streams never crash and report at most one error.
TEST(FrameDecoder, RandomizedChunkingAndCorruption) {
  std::mt19937 rng(1234);
  for (int round = 0; round < 200; ++round) {
    std::vector<std::byte> stream;
    std::vector<std::size_t> expected;
    const int frames = 1 + static_cast<int>(rng() % 8);
    for (int f = 0; f < frames; ++f) {
      const std::size_t size = 1 + rng() % 300;
      const auto frame = make_frame(size, static_cast<std::uint8_t>(f));
      stream.insert(stream.end(), frame.begin(), frame.end());
      expected.push_back(size);
    }
    const bool corrupt = round % 4 == 3;
    if (corrupt) {
      stream[rng() % stream.size()] ^= std::byte{0xFF};
    }

    FrameDecoder decoder(kMaxPayload);
    std::vector<std::size_t> got;
    int errors = 0;
    for (std::size_t pos = 0; pos < stream.size();) {
      const std::size_t chunk = std::min<std::size_t>(1 + rng() % 64, stream.size() - pos);
      decoder.feed(std::span(stream).subspan(pos, chunk));
      pos += chunk;
      for (auto r = decoder.next(); r.status != Status::NeedMore; r = decoder.next()) {
        if (r.status == Status::Error) {
          ++errors;
          break;
        }
        got.push_back(r.payload.size());
      }
      if (errors > 0) {
        break;
      }
    }
    if (!corrupt) {
      EXPECT_EQ(got, expected) << "round " << round;
      EXPECT_EQ(errors, 0);
    } else {
      EXPECT_LE(errors, 1);
      EXPECT_LE(got.size(), expected.size());
    }
  }
}

}  // namespace
