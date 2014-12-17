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

  void printBuffer(std::ostream& out,
                   const char *buffer,
                   unsigned long length) {
    const unsigned long width = 24;
    out << std::hex;
    for(unsigned long line = 0; line < (length + width - 1) / width; ++line) {
      out << std::setfill('0') << std::setw(7) << (line * width);
      for(unsigned long byte = 0;
          byte < width && line * width + byte < length; ++byte) {
        out << " " << std::setfill('0') << std::setw(2)
                  << static_cast<unsigned int>(0xff & buffer[line * width +
                                                             byte]);
      }
      out << "\n";
    }
    out << std::dec;
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

  SeekableArrayInputStream::SeekableArrayInputStream(
      std::initializer_list<unsigned char> values,
      long blkSize)
      : ownedData(values.size()),
        data(nullptr),
        length(values.size()),
        blockSize(blkSize < 0 ? length : static_cast<size_t>(blkSize)),
        position(0) {
    char *ptr = ownedData.data();
    for(const unsigned char ch : values) {
      *(ptr++) = static_cast<char>(ch);
    }
  }

  SeekableArrayInputStream::SeekableArrayInputStream(
      const char* const values,
      size_t size,
      long blkSize)
      : data(values),
        length(size),
        blockSize(blkSize < 0 ? length : static_cast<size_t>(blkSize)),
        position(0) {
  }

  bool SeekableArrayInputStream::Next(const void** buffer, int* size) {
    const size_t currentSize = std::min(length - position, blockSize);
    if (currentSize > 0) {
      *buffer = reinterpret_cast<const void*>(
          (data ? data : ownedData.data()) + position);
      *size = static_cast<int>(currentSize);
      position += currentSize;
      return true;
    }
    *size = 0;
    return false;
  }

  void SeekableArrayInputStream::BackUp(int count) {
    if (count >= 0) {
      if (static_cast<size_t>(count) <= blockSize
          && static_cast<size_t>(count) <= position) {
        position -= static_cast<size_t>(count);
      } else {
        throw std::logic_error("Can't backup that much!");
      }
    }
  }

  bool SeekableArrayInputStream::Skip(int count) {
    if (count >= 0) {
      if (static_cast<size_t>(count) + position <= length) {
        position += static_cast<size_t>(count);
        return true;
      } else {
        position = length;
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
    result << "memory from " << std::hex << (data ? data : ownedData.data())
           << std::dec << " for " << length;
    return result.str();
  }

  SeekableFileInputStream::SeekableFileInputStream(InputStream* _input,
                                                   std::streamoff _offset,
                                                   std::streamsize _length,
                                                   std::streamsize _blockSize)
      : input(_input),
        length(std::max(_length, std::streamsize(0))),
        blockSize(static_cast<size_t>(
                      std::min(length,
                               _blockSize < 0 ? 256 * 1024 : _blockSize))),
        buffer(new char[blockSize]),
        offset(_offset),
        position(0),
        remainder(0) {
  }

  SeekableFileInputStream::~SeekableFileInputStream() {
    // PASS
  }

  bool SeekableFileInputStream::Next(const void** data, int* size) {
    const std::streamsize bytesRead =
        std::min(length - position,
                 static_cast<std::streamsize>(blockSize));
    if (bytesRead > 0) {
      *data = buffer.get();
      // read from the file, skipping over the remainder
      input->read(buffer.get() + remainder,
                  static_cast<size_t>(offset)
                  + static_cast<size_t>(position)
                  + remainder,
                  static_cast<size_t>(bytesRead) - remainder);
      position += bytesRead;
      remainder = 0;
    }
    *size = static_cast<int>(bytesRead);
    return bytesRead != 0;
  }

  void SeekableFileInputStream::BackUp(int count) {
    if (position == 0 || remainder > 0) {
      throw std::logic_error("can't back up unless we just called Next");
    }
    if (static_cast<unsigned long>(count) > blockSize) {
      throw std::logic_error("can't back up that far");
    }
    remainder = static_cast<unsigned long>(count);
    position -= remainder;
    memmove(buffer.get(), 
            buffer.get() + blockSize - static_cast<size_t>(count), 
            static_cast<size_t>(count));
  }

  bool SeekableFileInputStream::Skip(int _count) {
    if (_count < 0) {
      return false;
    }
    const std::streamoff count = _count;
    position += count;
    if (position > length) {
      position = length;
      remainder = 0;
      return false;
    }
    if (remainder > static_cast<size_t>(count)) {
      remainder -= static_cast<size_t>(count);
      memmove(buffer.get(), buffer.get() + count, remainder);
    } else {
      remainder = 0;
    }
    return true;
  }
  
  google::protobuf::int64 SeekableFileInputStream::ByteCount() const {
    return static_cast<google::protobuf::int64>(position);
  }

  void SeekableFileInputStream::seek(PositionProvider& location) {
    position = static_cast<std::streamoff>(location.next());
    if (position > length) {
      position = length;
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
    case CompressionKind_ZLIB: {
      // PASS
    }
    }
    throw NotImplementedYet("compression codec");
  }
}
