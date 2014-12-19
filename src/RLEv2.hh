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

#ifndef ORC_RLEV2_HH
#define ORC_RLEV2_HH

#include "RLE.hh"

#include <cstddef>
#include <functional>
#include <istream>
#include <memory>

namespace orc {

class PositionProvider;

class RleDecoderV2 : public RleDecoder {
public:
  RleDecoderV2(std::unique_ptr<std::istream> is,
               bool isSigned);

  void seek(PositionProvider& provider) override;

  void skip(size_t count) override;

  void next(long* data, size_t count, const char* notNull) override;

private:
  struct ConsumedOutcome {
    size_t unfulfilled;
    // Assumed to be true when "unfulfilled" is positive.
    bool exhausted;
  };
  ConsumedOutcome skipShortRepeat(size_t);
  ConsumedOutcome nextShortRepeat(long*, size_t, const char*);
  ConsumedOutcome skipDirect(size_t);
  ConsumedOutcome nextDirect(long*, size_t, const char*);
  ConsumedOutcome skipPatchedBase(size_t);
  ConsumedOutcome skipDelta(size_t);

  void interpretShortRepeatHeader(const char);
  void interpretDirectHeader(const char);
  void primeSubEncodingFromHeader();

  const std::unique_ptr<std::istream> is_;
  const bool isSigned_;
  // Maximum encoded sizes of sequences by sub-encoding:
  // Short Repeat: 1 + 8                          = 9
  // Direct:       2 + (8 * 512)                  = 4098
  // Patched Base: 4 + 8 + (8 * 512) + 31*(1 + 8) = 4387
  // Delta:        2 + 10 + 10 + 8*(512 - 2)      = 4102
  // Note that these sizes are the largest that could possibly
  // be threatened by the header values. In practice, by
  // virtue of selecting a sub-encoding that yields the most
  // compact encoded data, these values wouldn't occur for the
  // patched base and delta sub-encodings, as the direct
  // sub-encoding would be more compact.
  // Since we read the header bytes separately, don't include
  // them in the buffer capacity.
  char buffer_[/*4 + */8 + (8 * 512) + 31*(1 + 8)];
  std::function<ConsumedOutcome(size_t)> skip_;
  std::function<ConsumedOutcome(long*, size_t, const char*)> next_;
  union {
    struct {
      long value;
      unsigned char remaining;
    } shortRepeatState;
    struct {
      unsigned char width;
      unsigned short remaining;
      const char* valueStartByte;
      // Indexed backwards with MSB as 0 up to LSB as 7.
      unsigned char valueStartBit;
    } directState;
    // TODO(seharris): patchedBaseState_, deltaState_.
  } modeSpecificState_;

  void forgetSubEncoding();
};

}  // namespace orc

#endif  // ORC_RLEV2_HH
