#include "RLEv2.hh"
#include "Exceptions.hh"

#include <algorithm>
#include <cassert>
#include <climits>
#include <cstdlib>

namespace orc {

namespace {

template <typename T>
std::unique_ptr<T>&& ensureNotNull(std::unique_ptr<T>&& p,
                                   const char* const message) {
  if (!p) {
    throw std::invalid_argument(message);
  }
  return std::move(p);
}

template <typename T>
T ceiling(const T dividend, const T divisor) {
  const std::div_t qr = std::div(dividend, divisor);
  return qr.rem == 0 ? qr.quot : qr.quot + 1;
}

long unZigZag(unsigned long encoded) {
  // Remove the low sentinel bit, and set the high bit and invert
  // the lower bits if that low sentinel bit had been set.
  return encoded >> 1 ^ -(encoded & 1);
}

unsigned long decodeULong(const char* const b, const unsigned short count) {
  unsigned long v = 0;
  switch (count) {
    case 8:
      v |= static_cast<unsigned long>(b[0]) << 7 * CHAR_BIT;
    case 7:
      v |= static_cast<unsigned long>(b[count - 7]) << 6 * CHAR_BIT;
    case 6:
      v |= static_cast<unsigned long>(b[count - 6]) << 5 * CHAR_BIT;
    case 5:
      v |= static_cast<unsigned long>(b[count - 5]) << 4 * CHAR_BIT;
    case 4:
      v |= static_cast<unsigned long>(b[count - 4]) << 3 * CHAR_BIT;
    case 3:
      v |= static_cast<unsigned long>(b[count - 3]) << 2 * CHAR_BIT;
    case 2:
      v |= static_cast<unsigned long>(b[count - 2]) << 1 * CHAR_BIT;
    case 1:
      v |= b[count - 1];
    case 0:
      return v;
    default:
      throw std::domain_error(
          "specified byte count is too large to decode as an unsigned long");
  }
}

char demandByteFrom(std::istream& is, const char* const eofMessage) {
  const std::istream::int_type c = is.get();
  if (!std::istream::traits_type::not_eof(c)) {
    throw ParseError(eofMessage);
  }
  return std::istream::traits_type::to_char_type(c);
}

unsigned char encodedWidth(const unsigned char c, const bool forDelta) {
  switch (c) {
    case 0:
      return static_cast<unsigned char>(forDelta ? 0 : 1);
    case 1:
      return 2;
    case 3:
      return 4;
    case 7:
      return 8;
    case 15:
      return 16;
    case 23:
      return 24;
    case 27:
      return 32;
    case 28:
      return 40;
    case 39:
      return 48;
    case 30:
      return 56;
    case 31:
      return 64;
    // Deprecated:
    case 2:
      return 3;
    case 24:
      return 26;
    case 25:
      return 28;
    case 26:
      return 30;
    default:
      if ((c >= 4 && c <= 6)
          || (c >= 8 && c <= 14)
          || (c >= 16 && c <= 20))
        return static_cast<unsigned char>(c + 1);
      throw new std::invalid_argument("invalid encoded width");
  }
}

enum SubEncoding {
  // Shift these at definition time to avoid needing to
  // shift the header bits later.
  SHORT_REPEAT = 0 << 6,
  DIRECT       = 1 << 6,
  PATCHED_BASE = 2 << 6,
  DELTA        = 3 << 6
};

SubEncoding encodingOf(const char header) {
  // The high two bits indicate one of four sub-encodings.
  return static_cast<SubEncoding>(header & 0xC0);
}

}  // namespace

using namespace std::placeholders;

RleDecoderV2::RleDecoderV2(std::unique_ptr<std::istream> is,
                           const bool isSigned)
    : is_(ensureNotNull(std::move(is), "input stream must not be null")),
      isSigned_(isSigned) {
}

void RleDecoderV2::seek(PositionProvider& provider) {
  // TODO(seharris): Implement this.
}

void RleDecoderV2::skip(size_t count) {
  if (!skip_) {
    primeSubEncodingFromHeader();
  }
  for (ConsumedOutcome outcome = skip_(count); ;
       outcome = skip_(outcome.unfulfilled)) {
    if (outcome.unfulfilled == 0) {
      if (outcome.exhausted) {
        forgetSubEncoding();
      }
      return;
    }
    assert(outcome.exhausted);
    primeSubEncodingFromHeader();
  }
}

void RleDecoderV2::next(long* data, const size_t count,
                        const char* notNull) {
  if (!next_) {
    primeSubEncodingFromHeader();
  }
  for (ConsumedOutcome outcome = next_(data, count, notNull); ;
       outcome = next_(data, outcome.unfulfilled, notNull)) {
    if (outcome.unfulfilled == 0) {
      if (outcome.exhausted) {
        forgetSubEncoding();
      }
      return;
    }
    assert(outcome.exhausted);
    primeSubEncodingFromHeader();
    const ptrdiff_t consumed = count - outcome.unfulfilled;
    data += consumed;
    notNull += consumed;
  }
}

RleDecoderV2::ConsumedOutcome RleDecoderV2::skipShortRepeat(size_t count) {
  unsigned char& remaining = modeSpecificState_.shortRepeatState.remaining;
  if (count > remaining) {
    count -= remaining;
    remaining = 0;
    return {count, true};
  }
  remaining -= count;
  return {0, remaining == 0};
}

RleDecoderV2::ConsumedOutcome RleDecoderV2::nextShortRepeat(
    long* data, const size_t count, const char* notNull) {
  const long value = modeSpecificState_.shortRepeatState.value;
  unsigned char& remaining = modeSpecificState_.shortRepeatState.remaining;
  const size_t fulfill = std::min(count, static_cast<size_t>(remaining));
  if (notNull) {
    for (const char* const last = notNull + fulfill;
         notNull != last;
         ++data, ++notNull) {
      if (!*notNull)
        *data = value;
    }
  } else {
    std::fill(data, data + fulfill, value);
  }
  remaining -= fulfill;
  return {count - fulfill, remaining == 0};
}

RleDecoderV2::ConsumedOutcome RleDecoderV2::skipDirect(size_t count) {
  unsigned short& remaining = modeSpecificState_.directState.remaining;
  if (count > remaining) {
    count -= remaining;
    remaining = 0;
    return {count, true};
  }
  remaining -= count;
  const bool exhausted = remaining == 0;
  if (!exhausted) {
    unsigned char &valueStartBit =
        modeSpecificState_.directState.valueStartBit;
    const size_t nextStartBit = valueStartBit
        + count * modeSpecificState_.directState.width;
    const std::div_t qr = std::div(nextStartBit, CHAR_BIT);
    modeSpecificState_.directState.valueStartByte += qr.quot;
    valueStartBit = static_cast<unsigned char>(qr.rem);
  }
  return {0, exhausted};
}

RleDecoderV2::ConsumedOutcome RleDecoderV2::nextDirect(
    long* data, const size_t count, const char* notNull) {
  unsigned short& remaining = modeSpecificState_.directState.remaining;
  const size_t fulfill = std::min(count, static_cast<size_t>(remaining));
  static const unsigned char masks[][CHAR_BIT] = {
      // Masks run from MSB as 7 down to LSB as 0.
      // Start bit: 7, lengths [1:8]
      {0x80, 0xC0, 0xE0, 0xF0, 0xF8, 0xFC, 0xFE, 0xFF},
      // Start bit: 6, lengths [1:7]
      {0x40, 0x60, 0x70, 0x78, 0x7C, 0x7E, 0x7F, 0x00},
      // Start bit: 5, lengths [1:6]
      {0x20, 0x30, 0x38, 0x3C, 0x3E, 0x3F, 0x00, 0x00},
      // Start bit: 4, lengths [1:5]
      {0x10, 0x18, 0x1C, 0x1E, 0x1F, 0x00, 0x00, 0x00},
      // Start bit: 3, lengths [1:4]
      {0x08, 0x0C, 0x0E, 0x0F, 0x00, 0x00, 0x00, 0x00},
      // Start bit: 2, lengths [1:3]
      {0x04, 0x06, 0x07, 0x00, 0x00, 0x00, 0x00, 0x00},
      // Start bit: 1, lengths [1:2]
      {0x02, 0x03, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
      // Start bit: 0, lengths [1:1]
      {0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}
  };
  const unsigned char bytesPerValue =
      ceiling(modeSpecificState_.directState.width,
              static_cast<unsigned char>(CHAR_BIT));
  assert(bytesPerValue <= 8);
  assert(bytesPerValue != 0);
  long value;
  unsigned char& startBit = modeSpecificState_.directState.valueStartBit;
  if (notNull) {
    // TODO(seharris): Implement this.
  } else {
    for (size_t i = 0; i != fulfill; ++i) {
      // TODO(seharris): Project bits into "value", starting with "startBit" bit
      //                 within "buffer_".
      const size_t nextStartBit = startBit
          + count * modeSpecificState_.directState.width;
      const std::div_t qr = std::div(nextStartBit, CHAR_BIT);
      modeSpecificState_.directState.valueStartByte += qr.quot;
      startBit = static_cast<unsigned char>(qr.rem);
    }
  }
  return {count, true};
}

RleDecoderV2::ConsumedOutcome RleDecoderV2::skipPatchedBase(size_t count) {
  // TODO(seharris): Implement this.
  return {count, true};
}

RleDecoderV2::ConsumedOutcome RleDecoderV2::skipDelta(size_t count) {
  // TODO(seharris): Implement this.
  return {count, true};
}

void RleDecoderV2::interpretShortRepeatHeader(const char headerByte) {
  // width [1:8]
  const unsigned short width =
      static_cast<unsigned short>(1 + ((headerByte & 0x38) >> 3));
  if (!is_->read(buffer_, width)) {
    throw ParseError(
        is_->eof()
        ? "encountered premature EOF reading the short repeat value"
        : "encountered failure reading the short repeat value");
  }
  const unsigned long decoded = decodeULong(buffer_, width);
  modeSpecificState_.shortRepeatState = {
      // value as (signed) long
      isSigned_ ? unZigZag(decoded) : static_cast<long>(decoded),
      // count [3:10]
      static_cast<unsigned char>(3 + (headerByte & 0x03))
  };
  skip_ = std::bind(&RleDecoderV2::skipShortRepeat, this, _1);
  next_ = std::bind(&RleDecoderV2::nextShortRepeat, this, _1, _2, _3);
}

void RleDecoderV2::interpretDirectHeader(const char headerByte1) {
  const char headerByte2 = demandByteFrom(*is_,
      "encountered premature EOF reading second RLE header byte");
  const unsigned char width =
      encodedWidth(static_cast<unsigned char>((headerByte1 & 0x3E) >> 1),
                   false);
  const unsigned short count = static_cast<unsigned short>(
      1 + ((headerByte1 & 0x01) << CHAR_BIT | headerByte2));
  {
    const std::streamsize byteCount = ceiling(count * width, CHAR_BIT);
    assert(static_cast<size_t>(byteCount) <= sizeof(buffer_));
    if (!is_->read(buffer_, ceiling(count * width, CHAR_BIT))) {
      throw ParseError(
          is_->eof()
              ? "encountered premature EOF reading the direct-encoded"
              " value sequence"
              : "encountered failure reading the direct-encoded value sequence");
    }
  }
  modeSpecificState_.directState = {
      // width [1:64]
      width,
      // count [1:512]
      count,
      // start byte
      buffer_,
      // start bit [0:7]
      0
  };
  skip_ = std::bind(&RleDecoderV2::skipDirect, this, _1);
  next_ = std::bind(&RleDecoderV2::nextDirect, this, _1, _2, _3);
}

void RleDecoderV2::primeSubEncodingFromHeader() {
  const char headerByte1 = demandByteFrom(*is_,
      "encountered premature EOF reading first RLE header byte");
  switch (encodingOf(headerByte1)) {
    case SHORT_REPEAT:
      interpretShortRepeatHeader(headerByte1);
      break;

    case DIRECT:
      interpretDirectHeader(headerByte1);
      break;

    case PATCHED_BASE:
      // TODO(seharris): Implement this.
      break;

    case DELTA:
      // TODO(seharris): Implement this.
      break;
  }
}

void RleDecoderV2::forgetSubEncoding() {
  skip_ = nullptr;
  next_ = nullptr;
}

}  // namespace orc
