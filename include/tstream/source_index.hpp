// Maps a source id to a small dense index, for per-source arrays on hot paths.
//
// Built once from the configured ids: a 65,536-entry table, so a lookup is one
// array read and never allocates. Index 0 is "other", for ids that weren't
// configured (e.g. the pipeline's own health samples); configured ids get
// 1..N in the order given.

#pragma once

#include <cstddef>
#include <cstdint>
#include <limits>
#include <span>
#include <vector>

namespace tstream {

class SourceIndex {
 public:
  // Index 0 is reserved for "other", so N ids give size() == N + 1.
  explicit SourceIndex(std::span<const std::uint16_t> ids)
      : table_(std::size_t{std::numeric_limits<std::uint16_t>::max()} + 1, 0),
        size_(ids.size() + 1) {
    for (std::size_t i = 0; i < ids.size(); ++i) {
      table_[ids[i]] = static_cast<std::uint16_t>(i + 1);
    }
  }

  // Number of slots a per-source array needs, including "other".
  [[nodiscard]] std::size_t size() const { return size_; }

  // Slot for `source_id`; 0 if it wasn't configured.
  [[nodiscard]] std::size_t operator()(std::uint16_t source_id) const { return table_[source_id]; }

 private:
  std::vector<std::uint16_t> table_;
  std::size_t size_;
};

}  // namespace tstream
