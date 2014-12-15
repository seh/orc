#include "RLEv2.hh"
#include "Compression.hh"

#include <cstddef>

namespace orc {

namespace {

enum SubEncoding {
  // Shift these at definition time to avoid needing to
  // shift the header bits later.
  SHORT_REPEAT = 0 << 6,
  DIRECT       = 1 << 6,
  PATCHED_BASE = 2 << 6,
  DELTA        = 3 << 6
};

SubEncoding encodingOf(const unsigned char header) {
  // The high two bits indicate one of four sub-encodings.
  return static_cast<SubEncoding>(header & 0xC0);
}

struct ShortRepeatState {
  ptrdiff_t width_;
  size_t remaining_;
};

// TODO(seharris): Provide a factory function that inspects the first byte
//                 drawn from a stream, and possibly reads a few more bytes
//                 to establish the initial state for one of the four
//                 sub-encodings.

}  // namespace

void RleDecoderV2::seek(PositionProvider& provider) {
  // TODO(seharris): Implement this.
}

void RleDecoderV2::skip(size_t numValues) {
  // TODO(seharris): Implement this.
}

void RleDecoderV2::next(long* const data, size_t numValues, const char* const notNull) {
  // TODO(seharris): Implement this.
}

}  // namespace orc
