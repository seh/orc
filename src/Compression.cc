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

#include "Compression.hh"
#include "Exceptions.hh"

#include <algorithm>
#include <iomanip>
#include <iostream>
#include <sstream>

namespace orc {

  void printBuffer(const char *buffer,
                   unsigned long length) {
    const unsigned long width = 24;
    std::cout << std::hex;
    for(unsigned long line = 0; line < (length + width - 1) / width; ++line) {
      std::cout << std::setfill('0') << std::setw(7) << (line * width);
      for(unsigned long byte = 0; 
          byte < width && line * width + byte < length; ++byte) {
        std::cout << " " << std::setfill('0') << std::setw(2) 
                  << static_cast<unsigned int>(0xff & buffer[line * width + byte]);
      }
      std::cout << "\n";
    }
    std::cout << std::dec;
  }

  PositionProvider::PositionProvider(const std::list<unsigned long>& posns) {
    position = posns.cbegin();
  }

  unsigned long PositionProvider::next() {
    unsigned long result = *position;
    ++position;
    return result;
  }

  SeekableInputStream::~SeekableInputStream() {
    // PASS
  }

  SeekableArrayInputStream::~SeekableArrayInputStream() {
    // PASS
  }

  SeekableArrayInputStream::SeekableArrayInputStream
     (std::initializer_list<unsigned char> values,
      long blkSize) {
    length = values.size();
    data = std::unique_ptr<char[]>(new char[length]);
    char *ptr = data.get();
    for(unsigned char ch: values) {
      *(ptr++) = static_cast<char>(ch);
    }
    position = 0;
    blockSize = blkSize == -1 ? length : static_cast<unsigned long>(blkSize);
  }

  SeekableArrayInputStream::SeekableArrayInputStream(char* values, 
                                                     unsigned long size, 
                                                     long blkSize) {
    length = size;
    data = std::unique_ptr<char[]>(new char[length]);
    memcpy(data.get(), values, length);
    position = 0;
    blockSize = blkSize == -1 ? length : static_cast<unsigned long>(blkSize);
  }

  bool SeekableArrayInputStream::Next(const void** buffer, int*size) {
    unsigned long currentSize = std::min(length - position, blockSize);
    if (currentSize > 0) {
      *buffer = data.get() + position;
      *size = static_cast<int>(currentSize);
      position += currentSize;
      return true;
    }
    return false;
  }

  void SeekableArrayInputStream::BackUp(int count) {
    if (count >= 0) {
      unsigned long unsignedCount = static_cast<unsigned long>(count);
      if (unsignedCount <= blockSize && unsignedCount <= position) {
        position -= unsignedCount;
      }
    }
  }

  bool SeekableArrayInputStream::Skip(int count) {
    if (count >= 0) {
      unsigned long unsignedCount = static_cast<unsigned long>(count);
      if (unsignedCount + position <= length) {
        position += unsignedCount;
        return true;
      }
    }
    return false;
  }

  google::protobuf::int64 SeekableArrayInputStream::ByteCount() const {
    return static_cast<google::protobuf::int64>(position);
  }

  void SeekableArrayInputStream::seek(PositionProvider& seekPosition) {
    position = seekPosition.next();
  }

  std::string SeekableArrayInputStream::getName() const {
    std::ostringstream result;
    result << "memory from " << std::hex << data.get() << std::dec 
           << " for " << length;
    return result.str();
  }

  SeekableFileInputStream::SeekableFileInputStream(InputStream* _input,
                                                   unsigned long _offset,
                                                   unsigned long _length,
                                                   long _blockSize) {
    input = _input;
    offset = _offset;
    length = _length;
    position = 0;
    blockSize = std::min(length,
                         static_cast<unsigned long>(_blockSize < 0 ? 
                                                    256 * 1024 : _blockSize));
    buffer.reset(new char[blockSize]);
    remainder = 0;
  }

  SeekableFileInputStream::~SeekableFileInputStream() {
    // PASS
  }

  bool SeekableFileInputStream::Next(const void** data, int*size) {
    unsigned long bytesRead = std::min(length - position, blockSize);
    if (bytesRead > 0) {
      *data = buffer.get();
      // read from the file, skipping over the remainder
      input->read(buffer.get() + remainder, offset + position + remainder, 
                  bytesRead - remainder);
      position += bytesRead;
      *size = static_cast<int>(bytesRead);
      remainder = 0;
    }
    return bytesRead != 0;
  }

  void SeekableFileInputStream::BackUp(int count) {
    if (position == 0 || remainder > 0) {
      throw std::logic_error("can't backup unless we just called Next");
    }
    if (remainder > blockSize) {
      throw std::logic_error("can't backup that far");
    }
    remainder = static_cast<unsigned long>(count);
    memmove(buffer.get(), 
            buffer.get() + blockSize - static_cast<size_t>(count), 
            static_cast<size_t>(count));
    position -= static_cast<unsigned long>(count);
  }

  bool SeekableFileInputStream::Skip(int _count) {
    unsigned long count = static_cast<unsigned long>(_count);
    position += count;
    if (position > length) {
      position = length;
      return false;
    }
    if (remainder > count) {
      remainder -= count;
    } else {
      remainder = 0;
    }
    return true;
  }
  
  google::protobuf::int64 SeekableFileInputStream::ByteCount() const {
    return static_cast<google::protobuf::int64>(position);
  }

  void SeekableFileInputStream::seek(PositionProvider& location) {
    position = location.next();
    if (position > length) {
      throw std::logic_error("seek too far");
    }
    remainder = 0;
  }

  std::string SeekableFileInputStream::getName() const {
    std::ostringstream result;
    result << input->getName() << " from " << offset << " for "
           << length;
    return result.str();
  }

  std::unique_ptr<SeekableInputStream> 
     createCodec(CompressionKind kind,
                 std::unique_ptr<SeekableInputStream> input,
                 unsigned long) {
    switch (kind) {
    case CompressionKind_NONE:
      return std::move(input);
    case CompressionKind_LZO:
    case CompressionKind_SNAPPY:
    case CompressionKind_ZLIB:
      throw NotImplementedYet("compression codec");
    }
  }
}
