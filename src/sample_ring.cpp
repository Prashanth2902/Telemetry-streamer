#include "tstream/sample_ring.hpp"

#include <stdexcept>
#include <string>

#include "tstream/config.hpp"
#include "tstream/spsc_ring.hpp"

namespace tstream {

namespace {

template <std::size_t Capacity>
class SampleRingImpl final : public SampleRing {
 public:
  bool try_push(const Sample& sample) override { return ring_.try_push(sample); }
  std::size_t try_pop_bulk(Sample* out, std::size_t max) override {
    return ring_.try_pop_bulk(out, max);
  }
  [[nodiscard]] std::size_t size_approx() const override { return ring_.size_approx(); }
  [[nodiscard]] std::size_t capacity() const override { return Capacity; }

 private:
  SpscRing<Sample, Capacity> ring_;
};

template <std::size_t Capacity>
std::unique_ptr<SampleRing> make() {
  return std::make_unique<SampleRingImpl<Capacity>>();
}

// The switch below must cover exactly the range Config accepts.
static_assert(kMinRingCapacity == 16 && kMaxRingCapacity == 65536);

}  // namespace

std::unique_ptr<SampleRing> make_sample_ring(std::size_t capacity) {
  switch (capacity) {
    case 16:
      return make<16>();
    case 32:
      return make<32>();
    case 64:
      return make<64>();
    case 128:
      return make<128>();
    case 256:
      return make<256>();
    case 512:
      return make<512>();
    case 1024:
      return make<1024>();
    case 2048:
      return make<2048>();
    case 4096:
      return make<4096>();
    case 8192:
      return make<8192>();
    case 16384:
      return make<16384>();
    case 32768:
      return make<32768>();
    case 65536:
      return make<65536>();
    default:
      throw std::invalid_argument("unsupported ring capacity " + std::to_string(capacity) +
                                  " (must be a power of two in [" +
                                  std::to_string(kMinRingCapacity) + ", " +
                                  std::to_string(kMaxRingCapacity) + "])");
  }
}

}  // namespace tstream
