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

#ifndef ORC_RLEV1_HH
#define ORC_RLEV1_HH

#include "RLE.hh"
#include "wrap/zero-copy-stream-wrapper.h"

#include <memory>

namespace orc {

class RleDecoderV1 : public RleDecoder {
public:
    typedef google::protobuf::io::ZeroCopyInputStream StreamT;

    RleDecoderV1(std::unique_ptr<StreamT> input,
                 bool isSigned);

    /**
    * Seek over a given number of values.
    */
    void skip(unsigned long numValues) override;

    /**
    * Read a number of values into the batch.
    */
    void next(long* data, unsigned long numValues, const char* notNull) override;

private:
    inline signed char readByte();

    inline void readHeader();

    inline unsigned long readLong();

    inline void skipLongs(unsigned long numValues);

    const std::unique_ptr<StreamT> inputStream;
    const bool isSigned;
    unsigned long remainingValues;
    long value;
    const char *bufferStart;
    const char *bufferEnd;
    int delta;
    bool repeating;
};

}  // namespace orc

#endif  // ORC_RLEV1_HH
