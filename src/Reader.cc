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

#include "orc/Reader.hh"
#include "orc/OrcFile.hh"
#include "ColumnReader.hh"
#include "Exceptions.hh"
#include "RLE.hh"
#include "TypeImpl.hh"

#include <google/protobuf/text_format.h>

#include <algorithm>
#include <iostream>
#include <limits>
#include <memory>
#include <sstream>
#include <string>
#include <vector>

namespace orc {

  std::string printProtobufMessage(const google::protobuf::Message& message) {
    std::string result;
    google::protobuf::TextFormat::PrintToString(message, &result);
    return result;
  }

  struct ReaderOptionsPrivate {
    std::list<int> includedColumns;
    unsigned long dataStart;
    unsigned long dataLength;
    unsigned long tailLocation;
    ReaderOptionsPrivate() {
      includedColumns.push_back(0);
      dataStart = 0;
      dataLength = std::numeric_limits<unsigned long>::max();
      tailLocation = std::numeric_limits<unsigned long>::max();
    }
  };

  ReaderOptions::ReaderOptions(): 
    privateBits(std::unique_ptr<ReaderOptionsPrivate>
                  (new ReaderOptionsPrivate())) {
    // PASS
  }

  ReaderOptions::ReaderOptions(const ReaderOptions& rhs): 
    privateBits(std::unique_ptr<ReaderOptionsPrivate>
                (new ReaderOptionsPrivate(*(rhs.privateBits.get())))) {
    // PASS
  }

  ReaderOptions::ReaderOptions(ReaderOptions&& rhs) {
    privateBits.swap(rhs.privateBits);
  }
  
  ReaderOptions& ReaderOptions::operator=(const ReaderOptions& rhs) {
    if (this != &rhs) {
      privateBits.reset(new ReaderOptionsPrivate(*(rhs.privateBits.get())));
    }
    return *this;
  }
  
  ReaderOptions::~ReaderOptions() {
    // PASS
  }

  ReaderOptions& ReaderOptions::include(const std::list<int>& include) {
    privateBits->includedColumns.clear();
    for(int columnId: include) {
      privateBits->includedColumns.push_back(columnId);
    }
    return *this;
  }

  ReaderOptions& ReaderOptions::include(std::initializer_list<int> include) {
    privateBits->includedColumns.clear();
    for(int columnId: include) {
      privateBits->includedColumns.push_back(columnId);
    }
    return *this;
  }

  ReaderOptions& ReaderOptions::range(unsigned long offset, 
                                      unsigned long length) {
    privateBits->dataStart = offset;
    privateBits->dataLength = length;
    return *this;
  }

  ReaderOptions& ReaderOptions::setTailLocation(unsigned long offset) {
    privateBits->tailLocation = offset;
    return *this;
  }

  const std::list<int>& ReaderOptions::getInclude() const {
    return privateBits->includedColumns;
  }

  unsigned long ReaderOptions::getOffset() const {
    return privateBits->dataStart;
  }

  unsigned long ReaderOptions::getLength() const {
    return privateBits->dataLength;
  }

  unsigned long ReaderOptions::getTailLocation() const {
    return privateBits->tailLocation;
  }

  Reader::~Reader() {
    // PASS
  }

  static const std::streamsize DIRECTORY_SIZE_GUESS = 16 * 1024;

  class ReaderImpl : public Reader {
  private:
    // inputs
    std::unique_ptr<InputStream> stream;
    ReaderOptions options;
    std::unique_ptr<bool[]> selectedColumns;

    // postscript
    proto::PostScript postscript;
    unsigned long blockSize;
    CompressionKind compression;
    unsigned long postscriptLength;

    // footer
    proto::Footer footer;
    std::unique_ptr<unsigned long[]> firstRowOfStripe;
//    std::vector<unsigned long> firstRowOfStripe;
    unsigned long numberOfStripes;
    std::unique_ptr<Type> schema;

    // metadata
    bool isMetadataLoaded;
    proto::Metadata metadata;

    // reading state
    unsigned long previousRow;
    unsigned long currentStripe;
    unsigned long currentRowInStripe;
    unsigned long rowsInCurrentStripe;
    proto::StripeInformation currentStripeInfo;
    proto::StripeFooter currentStripeFooter;
    std::unique_ptr<ColumnReader> reader;

    // internal methods
    void readPostscript(char * buffer, unsigned long length);
    void readFooter(char *buffer, unsigned long length,
                    unsigned long fileLength);
    proto::StripeFooter getStripeFooter(const proto::StripeInformation& info);
    void startNextStripe();
    void ensureOrcFooter(char* buffer, unsigned long length);
    void checkOrcVersion();
    void selectTypeParent(int columnId);
    void selectTypeChildren(int columnId);
    std::unique_ptr<ColumnVectorBatch> createRowBatch(const Type& type, 
                                                      unsigned long capacity
                                                      ) const;

  public:
    /**
     * Constructor that lets the user specify additional options.
     * @param stream the stream to read from
     * @param options options for reading
     */
    ReaderImpl(std::unique_ptr<InputStream> stream, 
               const ReaderOptions& options);

    CompressionKind getCompression() const override;

    unsigned long getNumberOfRows() const override;

    unsigned long getRowIndexStride() const override;

    const std::string& getStreamName() const override;
    
    std::list<std::string> getMetadataKeys() const override;

    std::string getMetadataValue(const std::string& key) const override;

    bool hasMetadataValue(const std::string& key) const override;

    unsigned long getCompressionSize() const override;

    unsigned long getNumberOfStripes() const override;

    std::unique_ptr<StripeInformation> getStripe(unsigned long
                                                 ) const override;

    unsigned long getContentLength() const override;

    std::list<ColumnStatistics*> getStatistics() const override;

    const Type& getType() const override;

    const bool* getSelectedColumns() const override;

    std::unique_ptr<ColumnVectorBatch> createRowBatch(unsigned long size
                                                      ) const override;

    bool next(ColumnVectorBatch& data) override;

    unsigned long getRowNumber() const override;

    void seekToRow(unsigned long rowNumber) override;
  };

  InputStream::~InputStream() {
    // PASS
  };

  ReaderImpl::ReaderImpl(std::unique_ptr<InputStream> input,
                         const ReaderOptions& opts
                         ): stream(std::move(input)), options(opts) {
    isMetadataLoaded = false;
    // figure out the size of the file using the option or filesystem
    const std::streamsize size =
        std::min(options.getTailLocation(),
                 static_cast<unsigned long>(stream->getLength()));

    // read last bytes into buffer to get PostScript
    {
      const std::streamsize readSize = std::min(size, DIRECTORY_SIZE_GUESS);
      std::unique_ptr<char[]> buffer(new char[readSize]);
      stream->read(buffer.get(),
                   size - readSize,
                   readSize);
      readPostscript(buffer.get(), readSize);
      readFooter(buffer.get(), readSize, size);
    }

    currentStripe = 0;
    currentRowInStripe = 0;
    unsigned long rowTotal = 0;
    firstRowOfStripe.reset(new unsigned long[footer.stripes_size()]);
    for(int i=0; i < footer.stripes_size(); ++i) {
      firstRowOfStripe.get()[i] = rowTotal;
      rowTotal += footer.stripes(i).numberofrows();
    }
    selectedColumns.reset(new bool[footer.types_size()]);
    memset(selectedColumns.get(), 0, 
           static_cast<std::size_t>(footer.types_size()));
    for(int columnId: options.getInclude()) {
      selectTypeParent(columnId);
      selectTypeChildren(columnId);
    }
    schema = convertType(footer.types(0), footer);
    schema->assignIds(0);
    previousRow = std::numeric_limits<unsigned long>::max();
  }
                         
  CompressionKind ReaderImpl::getCompression() const { 
    return compression;
  }

  unsigned long ReaderImpl::getCompressionSize() const {
    return blockSize;
  }

  unsigned long ReaderImpl::getNumberOfStripes() const {
    return numberOfStripes;
  }

  std::unique_ptr<StripeInformation> 
      ReaderImpl::getStripe(unsigned long) const {
    // TODO
    return std::unique_ptr<StripeInformation>();
  }

  unsigned long ReaderImpl::getNumberOfRows() const { 
    return footer.numberofrows();
  }

  unsigned long ReaderImpl::getContentLength() const {
    return footer.contentlength();
  }

  unsigned long ReaderImpl::getRowIndexStride() const {
    return footer.rowindexstride();
  }

  const std::string& ReaderImpl::getStreamName() const {
    return stream->getName();
  }

  std::list<std::string> ReaderImpl::getMetadataKeys() const {
    std::list<std::string> result;
    for(int i=0; i < footer.metadata_size(); ++i) {
      result.push_back(footer.metadata(i).name());
    }
    return result;
  }

  std::string ReaderImpl::getMetadataValue(const std::string& key) const {
    for(int i=0; i < footer.metadata_size(); ++i) {
      if (footer.metadata(i).name() == key) {
        return footer.metadata(i).value();
      }
    }
    throw std::range_error("key not found");
  }

  bool ReaderImpl::hasMetadataValue(const std::string& key) const {
    for(int i=0; i < footer.metadata_size(); ++i) {
      if (footer.metadata(i).name() == key) {
        return true;
      }
    }
    return false;
  }

  void ReaderImpl::selectTypeParent(int columnId) {
    bool* selectedColumnArray = selectedColumns.get();
    for(int parent=0; parent < columnId; ++parent) {
      for(unsigned int child: footer.types(parent).subtypes()) {
        if (static_cast<int>(child) == columnId) {
          if (!selectedColumnArray[parent]) {
            selectedColumnArray[parent] = true;
            selectTypeParent(parent);
            return;
          }
        }
      }
    }
  }

  void ReaderImpl::selectTypeChildren(int columnId) {
    bool* selectedColumnArray = selectedColumns.get();
    if (!selectedColumnArray[columnId]) {
      selectedColumnArray[columnId] = true;
      for(unsigned int child: footer.types(columnId).subtypes()) {
        selectTypeChildren(static_cast<int>(child));
      }
    }
  }

  void ReaderImpl::ensureOrcFooter(char*, unsigned long) {
    // TODO fix me
  }

  const bool* ReaderImpl::getSelectedColumns() const {
    return selectedColumns.get();
  }

  const Type& ReaderImpl::getType() const {
    return *(schema.get());
  }

  unsigned long ReaderImpl::getRowNumber() const {
    return previousRow;
  }

  std::list<ColumnStatistics*> ReaderImpl::getStatistics() const {
    throw NotImplementedYet("getStatistics");
  }

  void ReaderImpl::seekToRow(unsigned long) {
    throw NotImplementedYet("seekToRow");
  }

  void ReaderImpl::readPostscript(char *buffer, unsigned long readSize) {

    //get length of PostScript
    postscriptLength = buffer[readSize - 1] & 0xff;

    ensureOrcFooter(buffer, readSize);

    if (!postscript.ParseFromArray(buffer+readSize-1-postscriptLength, 
                                   static_cast<int>(postscriptLength))) {
      throw ParseError("bad postscript parse");
    }
    if (postscript.has_compressionblocksize()) {
      blockSize = postscript.compressionblocksize();
    } else {
      blockSize = 256 * 1024;
    }

    checkOrcVersion();

    //check compression codec
    compression = static_cast<CompressionKind>(postscript.compression());
  }

  void ReaderImpl::readFooter(char* buffer, unsigned long readSize,
                              unsigned long) {
    unsigned long footerSize = postscript.footerlength();
    //check if extra bytes need to be read
    unsigned long tailSize = 1 + postscriptLength + footerSize;
    if (tailSize > readSize) {
      throw NotImplementedYet("need more footer data.");
    }
    std::unique_ptr<SeekableInputStream> pbStream =createCodec(compression,
                          std::unique_ptr<SeekableInputStream>
                          (new SeekableArrayInputStream(buffer +
                                                        (readSize - tailSize),
                                                        footerSize)),
                          blockSize);
    // TODO: do not SeekableArrayInputStream, rather use an array
//    if (!footer.ParseFromArray(buffer+readSize-tailSize, footerSize)) {
    if (!footer.ParseFromZeroCopyStream(pbStream.get())) {
      throw ParseError("bad footer parse");
    }
    numberOfStripes = static_cast<unsigned long>(footer.stripes_size());
  }

  proto::StripeFooter ReaderImpl::getStripeFooter
                        (const proto::StripeInformation& info) {
    unsigned long footerStart = info.offset() + info.indexlength() +
      info.datalength();
    unsigned long footerLength = info.footerlength();
    std::unique_ptr<SeekableInputStream> pbStream = 
      createCodec(compression,
                  std::unique_ptr<SeekableInputStream>
                  (new SeekableFileInputStream(stream.get(), footerStart,
                                               footerLength, 
                                               static_cast<long>(blockSize)
                                               )),
                  blockSize);
    proto::StripeFooter result;
    if (!result.ParseFromZeroCopyStream(pbStream.get())) {
      throw ParseError(std::string("bad StripeFooter from ") + 
                       pbStream->getName());
    }
    return result;
  }

  class StripeStreamsImpl: public StripeStreams {
  private:
    const ReaderImpl& reader;
    const proto::StripeFooter& footer;
    const unsigned long stripeStart;
    InputStream& input;

  public:
    StripeStreamsImpl(const ReaderImpl& reader,
                      const proto::StripeFooter& footer,
                      unsigned long stripeStart,
                      InputStream& input);

    virtual ~StripeStreamsImpl();

    virtual const bool* getSelectedColumns() const override;

    virtual proto::ColumnEncoding getEncoding(int columnId) const override;

    virtual std::unique_ptr<SeekableInputStream> 
                    getStream(int columnId,
                              proto::Stream_Kind kind) const override;
  };

  StripeStreamsImpl::StripeStreamsImpl(const ReaderImpl& _reader,
                                       const proto::StripeFooter& _footer,
                                       unsigned long _stripeStart,
                                       InputStream& _input
                                       ): reader(_reader), 
                                          footer(_footer),
                                          stripeStart(_stripeStart),
                                          input(_input) {
    // PASS
  }

  StripeStreamsImpl::~StripeStreamsImpl() {
    // PASS
  }

  const bool* StripeStreamsImpl::getSelectedColumns() const {
    return reader.getSelectedColumns();
  }

  proto::ColumnEncoding StripeStreamsImpl::getEncoding(int columnId) const {
    return footer.columns(columnId);
  }

  std::unique_ptr<SeekableInputStream> 
        StripeStreamsImpl::getStream(int columnId,
                                     proto::Stream_Kind kind) const {
    unsigned long offset = stripeStart;
    for(int i = 0; i < footer.streams_size(); ++i) {
      const proto::Stream& stream = footer.streams(i);
      if (stream.kind() == kind && 
          stream.column() == static_cast<unsigned int>(columnId)) {
        return createCodec(reader.getCompression(),
                           std::unique_ptr<SeekableInputStream>
                           (new SeekableFileInputStream
                            (&input,
                             offset,
                             stream.length(),
                             static_cast<long>(reader.getCompressionSize()))),
                           reader.getCompressionSize());
      }
      offset += stream.length();
    }
    return std::unique_ptr<SeekableInputStream>();
  }

  void ReaderImpl::startNextStripe() {
    currentStripeInfo = footer.stripes(static_cast<int>(currentStripe));
    currentStripeFooter = getStripeFooter(currentStripeInfo);
    rowsInCurrentStripe = currentStripeInfo.numberofrows();
    StripeStreamsImpl stripeStreams(*this, currentStripeFooter, 
                                    currentStripeInfo.offset(),
                                    *(stream.get()));
    reader = buildReader(*(schema.get()), stripeStreams);
  }

  void ReaderImpl::checkOrcVersion() {
    // TODO
  }

  bool ReaderImpl::next(ColumnVectorBatch& data) {
    if (currentStripe >= numberOfStripes) {
      data.numElements = 0;
      return false;
    }
    if (currentRowInStripe == 0) {
      startNextStripe();
    }
    unsigned long rowsToRead = 
      std::min(data.capacity, rowsInCurrentStripe - currentRowInStripe);
    data.numElements = rowsToRead;
    reader->next(data, rowsToRead, 0);
    // update row number
    previousRow = firstRowOfStripe.get()[currentStripe] + currentRowInStripe;
    currentRowInStripe += rowsToRead;
    if (currentRowInStripe >= rowsInCurrentStripe) {
      currentStripe += 1;
      currentRowInStripe = 0;
    }
    return rowsToRead != 0;
  }

  std::unique_ptr<ColumnVectorBatch> ReaderImpl::createRowBatch
       (const Type& type, unsigned long capacity) const {
    switch (type.getKind()) {
    case BOOLEAN:
    case BYTE:
    case SHORT:
    case INT:
    case LONG:
    case TIMESTAMP:
    case DATE:
      return std::unique_ptr<ColumnVectorBatch>(new LongVectorBatch(capacity));

    case FLOAT:
    case DOUBLE:
      return std::unique_ptr<ColumnVectorBatch>
        (new DoubleVectorBatch(capacity));

    case STRING:
    case BINARY:
    case CHAR:
    case VARCHAR:
      return std::unique_ptr<StringVectorBatch>
        (new StringVectorBatch(capacity));

    case STRUCT: {
      std::unique_ptr<ColumnVectorBatch> result =
        std::unique_ptr<ColumnVectorBatch>(new StructVectorBatch(capacity));

      StructVectorBatch* structPtr = 
        dynamic_cast<StructVectorBatch*>(result.get());

      structPtr->numFields = 0;
      bool* selected = selectedColumns.get();
      for(unsigned int i=0; i < type.getSubtypeCount(); ++i) {
        const Type& child = type.getSubtype(i);
        if (selected[child.getColumnId()]) {
          structPtr->numFields += 1;
        }
      }
      structPtr->fields = std::unique_ptr<std::unique_ptr<ColumnVectorBatch>[]>
        (new std::unique_ptr<ColumnVectorBatch>[structPtr->numFields]);
      unsigned long next = 0;
      for(unsigned int i=0; i < type.getSubtypeCount(); ++i) {
        const Type& child = type.getSubtype(i);
        if (selected[child.getColumnId()]) {
          structPtr->fields[next++] = createRowBatch(child, capacity);
        }
      }
      return result;
    }
    case LIST:
    case MAP:
    case UNION:
    case DECIMAL: {
      // PASS
    }
    }
    throw NotImplementedYet("not supported yet");
  }

  std::unique_ptr<ColumnVectorBatch> ReaderImpl::createRowBatch
       (unsigned long capacity) const {
    return createRowBatch(*(schema.get()), capacity);
  }

  std::unique_ptr<Reader> createReader(std::unique_ptr<InputStream> stream, 
                                       const ReaderOptions& options) {
    return std::unique_ptr<Reader>(new ReaderImpl(std::move(stream), options));
  }
}
