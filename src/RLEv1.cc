/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "RLEv1.hh"
#include "Compression.hh"
#include "Exceptions.hh"

#include <algorithm>

namespace orc {

const unsigned long MINIMUM_REPEAT = 3;
const unsigned long BASE_128_MASK = 0x7f;

inline long unZigZag(unsigned long value) {
  return value >> 1 ^ -(value & 1);
}

signed char RleDecoderV1::readByte() {
  if (bufferStart == bufferEnd) {
    int bufferLength;
    const void* bufferPointer;
    if (!inputStream->Next(&bufferPointer, &bufferLength)) {
      throw ParseError("bad read in readByte");
    }
    bufferStart = static_cast<const char*>(bufferPointer);
    bufferEnd = bufferStart + bufferLength;
  }
  return *(bufferStart++);
}

unsigned long RleDecoderV1::readLong() {
  unsigned long result = 0;
  int offset = 0;
  signed char ch = readByte();
  if (ch >= 0) {
    result = static_cast<unsigned long>(ch);
  } else {
    result = static_cast<unsigned long>(ch) & BASE_128_MASK;
    while ((ch = readByte()) < 0) {
      offset += 7;
      result |= (static_cast<unsigned long>(ch) & BASE_128_MASK) << offset;
    }
    result |= static_cast<unsigned long>(ch) << (offset + 7);
  }
  return result;
}

void RleDecoderV1::skipLongs(unsigned long numValues) {
  while (numValues > 0) {
    if (readByte() >= 0) {
      --numValues;
    }
  }
}

void RleDecoderV1::readHeader() {
  signed char ch = readByte();
  if (ch < 0) {
    remainingValues = static_cast<unsigned long>(-ch);
    repeating = false;
  } else {
    remainingValues = static_cast<unsigned long>(ch) + MINIMUM_REPEAT;
    repeating = true;
    delta = readByte();
    value = isSigned
        ? unZigZag(readLong())
        : static_cast<long>(readLong());
  }
}

RleDecoderV1::RleDecoderV1(std::unique_ptr<SeekableInputStream> input,
                           bool hasSigned)
    : inputStream(std::move(input)),
      isSigned(hasSigned),
      remainingValues(0),
      bufferStart(nullptr),
      bufferEnd(bufferStart) {
}

void RleDecoderV1::seek(PositionProvider& location) {
  // move the input stream
  inputStream->seek(location);
  // force a re-read from the stream
  bufferEnd = bufferStart;
  // read a new header
  readHeader();
  // skip ahead the given number of records
  skip(location.next());
}

void RleDecoderV1::skip(unsigned long numValues) {
  while (numValues > 0) {
    if (remainingValues == 0) {
      readHeader();
    }
    unsigned long count = std::min(numValues, remainingValues);
    remainingValues -= count;
    numValues -= count;
    if (repeating) {
      value += delta * static_cast<long>(count);
    } else {
      skipLongs(count);
    }
  }
}

void RleDecoderV1::next(long* const data,
                        const unsigned long numValues,
                        const char* const notNull) {
  unsigned long position = 0;
  const auto skipNulls = [&position, numValues, notNull] {
    if (notNull) {
      // Skip over null values.
      while (position < numValues && !notNull[position]) {
        ++position;
      }
    }
  };
  skipNulls();
  while (position < numValues) {
    // If we are out of values, read more.
    if (remainingValues == 0) {
      readHeader();
    }
    // How many do we read out of this block?
    unsigned long count = std::min(numValues - position, remainingValues);
    unsigned long consumed = 0;
    if (repeating) {
      if (notNull) {
        for (unsigned long i = 0; i < count; ++i) {
          if (notNull[position + i]) {
            data[position + i] = value + static_cast<long>(consumed) * delta;
            consumed += 1;
          }
        }
      } else {
        for (unsigned long i = 0; i < count; ++i) {
          data[position + i] = value + static_cast<long>(i) * delta;
        }
        consumed = count;
      }
      value += static_cast<long>(consumed) * delta;
    } else {
      if (notNull) {
        for (unsigned long i = 0 ; i < count; ++i) {
          if (notNull[i]) {
            data[position + i] = isSigned
                ? unZigZag(readLong())
                : static_cast<long>(readLong());
            ++consumed;
          }
        }
      } else {
        if (isSigned) {
          for (unsigned long i = 0; i < count; ++i) {
            data[position + i] = unZigZag(readLong());
          }
        } else {
          for (unsigned long i = 0; i < count; ++i) {
            data[position + i] = static_cast<long>(readLong());
          }
        }
        consumed = count;
      }
    }
    remainingValues -= consumed;
    position += count;
    skipNulls();
  }
}

}  // namespace orc
