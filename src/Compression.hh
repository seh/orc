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

#ifndef ORC_COMPRESSION_HH
#define ORC_COMPRESSION_HH

#include "orc/OrcFile.hh"
#include "wrap/zero-copy-stream-wrapper.h"

#include <initializer_list>
#include <list>
#include <memory>

namespace orc {

  void printBuffer(std::ostream& out,
                   const char *buffer,
                   unsigned long length);

  class PositionProvider {
  private:
    std::list<unsigned long>::const_iterator position;
  public:
    PositionProvider(const std::list<unsigned long>& positions);
    unsigned long next();
  };

  /**
   * A subclass of Google's ZeroCopyInputStream that supports seek.
   * By extending Google's class, we get the ability to pass it directly
   * to the protobuf readers.
   */
  class SeekableInputStream : public google::protobuf::io::ZeroCopyInputStream {
  public:
    virtual ~SeekableInputStream();
    virtual void seek(PositionProvider& position) = 0;
    virtual std::string getName() const = 0;
  };

  /**
   * Create a seekable input stream based on a memory range.
   */
  class SeekableArrayInputStream : public SeekableInputStream {
  private:
    std::vector<char> ownedData;
    const char* const data;
    const size_t length;
    const size_t blockSize;
    size_t position;

  public:
    SeekableArrayInputStream(std::initializer_list<unsigned char> list,
                             long block_size = -1);
    SeekableArrayInputStream(const char* list,
                             size_t length,
                             long block_size = -1);
    virtual ~SeekableArrayInputStream();

    bool Next(const void** data, int* size) override;
    void BackUp(int count) override;
    bool Skip(int count) override;
    google::protobuf::int64 ByteCount() const override;
    void seek(PositionProvider& position) override;
    std::string getName() const override;
  };

  /**
   * Create a seekable input stream based on an input stream.
   */
  class SeekableFileInputStream : public SeekableInputStream {
  private:
    InputStream* const input;
    const std::streamsize length;
    const size_t blockSize;
    const std::unique_ptr<char[]> buffer;
    const std::streamoff offset;
    std::streamoff position;
    size_t remainder;

  public:
    SeekableFileInputStream(InputStream* input,
                            std::streamoff offset,
                            std::streamsize length,
                            std::streamsize blockSize = -1);
    virtual ~SeekableFileInputStream();

    bool Next(const void** data, int* size) override;
    void BackUp(int count) override;
    bool Skip(int count) override;
    google::protobuf::int64 ByteCount() const override;
    void seek(PositionProvider& position) override;
    std::string getName() const override;
  };

  /**
   * Create a codec for the given compression kind.
   * @param kind the compression type to implement
   * @param input the input stream that is the underlying source
   * @param bufferSize the maximum size of the buffer
   */
  std::unique_ptr<SeekableInputStream> 
     createCodec(CompressionKind kind,
                 std::unique_ptr<SeekableInputStream> input,
                 unsigned long bufferSize);

}  // namespace orc

#endif
