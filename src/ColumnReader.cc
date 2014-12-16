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

#include "ByteRLE.hh"
#include "ColumnReader.hh"
#include "Exceptions.hh"
#include "RLEs.hh"

namespace orc {

  StripeStreams::~StripeStreams() {
    // PASS
  }

  ColumnReader::ColumnReader(const Type& type,
                             StripeStreams& stripe
                             ): columnId(type.getColumnId()) {
    std::unique_ptr<SeekableInputStream> stream =
      stripe.getStream(columnId, proto::Stream_Kind_PRESENT);
    if (stream.get()) {
      notNullDecoder = createBooleanRleDecoder(std::move(stream));
    }
  }

  ColumnReader::~ColumnReader() {
    // PASS
  }

  unsigned long ColumnReader::skip(unsigned long numValues) {
    ByteRleDecoder* decoder = notNullDecoder.get();
    if (decoder) {
      // page through the values that we want to skip
      // and count how many are non-null
      const size_t bufferSize = std::min(32768UL, numValues);
      std::unique_ptr<char[]> buffer(new char[bufferSize]);
      for(size_t remaining = numValues; remaining > 0;) {
        const size_t chunkSize = std::min(remaining, bufferSize);
        decoder->next(buffer.get(), chunkSize, nullptr);
        remaining -= chunkSize;
        for(unsigned long i=0; i < chunkSize; ++i) {
          if (!buffer[i]) {
            numValues -= 1;
          }
        }
      }
    }
    return numValues;
  }

  void ColumnReader::next(ColumnVectorBatch& rowBatch, 
                          unsigned long numValues,
                          char* incomingMask) {
    rowBatch.numElements = numValues;
    ByteRleDecoder* decoder = notNullDecoder.get();
    if (decoder) {
      char* notNullArray = rowBatch.notNull.get();
      decoder->next(notNullArray, numValues, incomingMask);
      // check to see if there are nulls in this batch
      for(unsigned long i=0; i < numValues; ++i) {
        if (!notNullArray[i]) {
          rowBatch.hasNulls = true;
          return;
        }
      }
    }
    rowBatch.hasNulls = false;
  }

  class IntegerColumnReader: public ColumnReader {
  private:
    std::unique_ptr<orc::RleDecoder> rle;

  public:
    IntegerColumnReader(const Type& type, StripeStreams& stipe);
    ~IntegerColumnReader();

    unsigned long skip(unsigned long numValues) override;

    void next(ColumnVectorBatch& rowBatch, 
              unsigned long numValues,
              char* notNull) override;
  };

  IntegerColumnReader::IntegerColumnReader(const Type& type,
                                           StripeStreams& stripe)
      : ColumnReader(type, stripe) {
    switch (stripe.getEncoding(columnId).kind()) {
    case proto::ColumnEncoding_Kind_DIRECT:
      rle = createRleDecoder(stripe.getStream(columnId,
                                              proto::Stream_Kind_DATA), 
                             true, RleVersion_1);
      break;
    case proto::ColumnEncoding_Kind_DIRECT_V2:
      rle = createRleDecoder(stripe.getStream(columnId,
                                              proto::Stream_Kind_DATA), 
                             true, RleVersion_2);
      break;
    case proto::ColumnEncoding_Kind_DICTIONARY:
    case proto::ColumnEncoding_Kind_DICTIONARY_V2:
      throw ParseError("Unknown encoding for IntegerColumnReader");
    }
  }

  IntegerColumnReader::~IntegerColumnReader() {
    // PASS
  }

  unsigned long IntegerColumnReader::skip(unsigned long numValues) {
    numValues = ColumnReader::skip(numValues);
    rle->skip(numValues);
    return numValues;
  }

  void IntegerColumnReader::next(ColumnVectorBatch& rowBatch, 
                                 unsigned long numValues,
                                 char *notNull) {
    ColumnReader::next(rowBatch, numValues, notNull);
    rle->next(dynamic_cast<LongVectorBatch&>(rowBatch).data.get(),
              numValues, rowBatch.hasNulls ? rowBatch.notNull.get() : nullptr);
  }

  class StringDictionaryColumnReader: public ColumnReader {
  private:
    std::unique_ptr<char[]> dictionaryBlob;
    std::unique_ptr<long[]> dictionaryOffset;
    std::unique_ptr<RleDecoder> rle;
    unsigned int dictionaryCount;
    
  public:
    StringDictionaryColumnReader(const Type& type, StripeStreams& stipe);
    ~StringDictionaryColumnReader();

    unsigned long skip(unsigned long numValues) override;

    void next(ColumnVectorBatch& rowBatch, 
              unsigned long numValues,
              char *notNull) override;
  };

  void readFully(char* buffer, long bufferSize, SeekableInputStream* stream) {
    long posn = 0;
    while (posn < bufferSize) {
      const void* chunk;
      int length;
      if (!stream->Next(&chunk, &length)) {
        throw ParseError("bad read in readFully");
      }
      memcpy(buffer + posn, chunk, static_cast<size_t>(length));
      posn += length;
    }
  }

  StringDictionaryColumnReader::StringDictionaryColumnReader
      (const Type& type,
       StripeStreams& stripe
       ): ColumnReader(type, stripe) {
    RleVersion rleVersion;
    switch (stripe.getEncoding(columnId).kind()) {
    case proto::ColumnEncoding_Kind_DICTIONARY:
      rleVersion = RleVersion_1;
      break;
    case proto::ColumnEncoding_Kind_DICTIONARY_V2:
      rleVersion = RleVersion_2;
      break;
    case proto::ColumnEncoding_Kind_DIRECT:
    case proto::ColumnEncoding_Kind_DIRECT_V2:
      throw ParseError("Unknown encoding for StringDictionaryColumnReader");
    }
    dictionaryCount = stripe.getEncoding(columnId).dictionarysize();
    rle = createRleDecoder(stripe.getStream(columnId,
                                            proto::Stream_Kind_DATA), 
                           false, rleVersion);
    std::unique_ptr<RleDecoder> lengthDecoder = 
      createRleDecoder(stripe.getStream(columnId,
                                        proto::Stream_Kind_LENGTH),
                       false, rleVersion);
    dictionaryOffset = std::unique_ptr<long[]>(new long[dictionaryCount+1]);
    long* lengthArray = dictionaryOffset.get();
    lengthDecoder->next(lengthArray + 1, dictionaryCount, nullptr);
    lengthArray[0] = 0;
    for(unsigned int i=1; i < dictionaryCount + 1; ++i) {
      lengthArray[i] += lengthArray[i-1];
    }
    long blobSize = lengthArray[dictionaryCount];
    dictionaryBlob = std::unique_ptr<char[]>(new char[blobSize]);
    std::unique_ptr<SeekableInputStream> blobStream =
      stripe.getStream(columnId, proto::Stream_Kind_DICTIONARY_DATA);
    readFully(dictionaryBlob.get(), blobSize, blobStream.get());
  }

  StringDictionaryColumnReader::~StringDictionaryColumnReader() {
    // PASS
  }

  unsigned long StringDictionaryColumnReader::skip(unsigned long numValues) {
    numValues = ColumnReader::skip(numValues);
    rle->skip(numValues);
    return numValues;
  }

  void StringDictionaryColumnReader::next(ColumnVectorBatch& rowBatch, 
                                          unsigned long numValues,
                                          char *notNull) {
    ColumnReader::next(rowBatch, numValues, notNull);
    // update the notNull from the parent class
    notNull = rowBatch.hasNulls ? rowBatch.notNull.get() : 0;
    StringVectorBatch& byteBatch = dynamic_cast<StringVectorBatch&>(rowBatch);
    char *blob = dictionaryBlob.get();
    long *dictionaryOffsets = dictionaryOffset.get();
    char **outputStarts = byteBatch.data.get();
    long *outputLengths = byteBatch.length.get();
    rle->next(outputLengths, numValues, notNull);
    if (notNull) {
      for(unsigned int i=0; i < numValues; ++i) {
        if (notNull[i]) {
          long entry = outputLengths[i];
          outputStarts[i] = blob + dictionaryOffsets[entry];
          outputLengths[i] = dictionaryOffsets[entry+1] - 
            dictionaryOffsets[entry];
        }
      }
    } else {
      for(unsigned int i=0; i < numValues; ++i) {
        long entry = outputLengths[i];
        outputStarts[i] = blob + dictionaryOffsets[entry];
        outputLengths[i] = dictionaryOffsets[entry+1] - 
          dictionaryOffsets[entry];
      }
    }
  }

  class StructColumnReader: public ColumnReader {
  private:
    std::unique_ptr<std::unique_ptr<ColumnReader>[]> children;
    unsigned int subtypeCount;

  public:
    StructColumnReader(const Type& type,
                       StripeStreams& stipe);
    ~StructColumnReader();

    unsigned long skip(unsigned long numValues) override;

    void next(ColumnVectorBatch& rowBatch, 
              unsigned long numValues,
              char *notNull) override;
  };

  StructColumnReader::StructColumnReader(const Type& type, 
                                         StripeStreams& stripe
                                         ): ColumnReader(type, stripe) {
    // count the number of selected sub-columns
    const bool *selectedColumns = stripe.getSelectedColumns();
    subtypeCount = type.getSubtypeCount();
    for(unsigned int i=0; i < type.getSubtypeCount(); ++i) {
      if (!selectedColumns[type.getSubtype(i).getColumnId()]) {
        subtypeCount -= 1;
      }
    }
    children.reset(new std::unique_ptr<ColumnReader>[subtypeCount]);
    switch (stripe.getEncoding(columnId).kind()) {
    case proto::ColumnEncoding_Kind_DIRECT:
      for(unsigned int i=0, posn=0; i < type.getSubtypeCount(); ++i) {
        const Type& child = type.getSubtype(i);
        if (selectedColumns[child.getColumnId()]) {
          children.get()[posn++].reset(buildReader(child, stripe).release());
        }
      }
      break;
    case proto::ColumnEncoding_Kind_DIRECT_V2:
    case proto::ColumnEncoding_Kind_DICTIONARY:
    case proto::ColumnEncoding_Kind_DICTIONARY_V2:
      throw ParseError("Unknown encoding for StructColumnReader");
    }
  }

  StructColumnReader::~StructColumnReader() {
    // PASS
  }

  unsigned long StructColumnReader::skip(unsigned long numValues) {
    numValues = ColumnReader::skip(numValues);
    for(unsigned int i=0; i < subtypeCount; ++i) {
      children.get()[i].get()->skip(numValues);
    }
    return numValues;
  }

  void StructColumnReader::next(ColumnVectorBatch& rowBatch, 
                                unsigned long numValues,
                                char *notNull) {
    ColumnReader::next(rowBatch, numValues, notNull);
    std::unique_ptr<ColumnVectorBatch> *childBatch = 
      dynamic_cast<StructVectorBatch&>(rowBatch).fields.get();
    for(unsigned int i=0; i < subtypeCount; ++i) {
      children.get()[i].get()->next(*(childBatch[i]), numValues,
                                    rowBatch.hasNulls
                                    ? rowBatch.notNull.get()
                                    : nullptr);
    }
  }

  /**
   * Create a reader for the given stripe.
   */
  std::unique_ptr<ColumnReader> buildReader(const Type& type,
                                            StripeStreams& stripe) {
    switch (type.getKind()) {
    case BYTE:
    case SHORT:
    case INT:
    case LONG:
      return std::unique_ptr<ColumnReader>(new IntegerColumnReader(type,
                                                                   stripe));
    case STRING:
      switch (stripe.getEncoding(type.getColumnId()).kind()) {
      case proto::ColumnEncoding_Kind_DICTIONARY:
      case proto::ColumnEncoding_Kind_DICTIONARY_V2:
        return std::unique_ptr<ColumnReader>(new StringDictionaryColumnReader
                                             (type, stripe));
      case proto::ColumnEncoding_Kind_DIRECT:
      case proto::ColumnEncoding_Kind_DIRECT_V2:
        throw NotImplementedYet("buildReader unhandled string encoding");
      }
    case STRUCT:
      return std::unique_ptr<ColumnReader>(new StructColumnReader(type,
                                                                  stripe));
    case FLOAT:
    case DOUBLE:
    case BINARY:
    case BOOLEAN:
    case TIMESTAMP:
    case LIST:
    case MAP:
    case UNION:
    case DECIMAL:
    case DATE:
    case VARCHAR:
    case CHAR: {
      // PASS
    }
    }
    throw NotImplementedYet("buildReader unhandled type");
  }

}
