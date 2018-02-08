#pragma once
#include <stdio.h>
#include <stdlib.h>

#include <sstream>
#include <fstream>
#include <string>
#include <vector>
#include <memory>
#include <utility>

namespace recordio {

constexpr size_t kDefaultMaxChunkSize = 32 * 1024 * 1024;
constexpr uint32_t kMagicNumber = 0x01020304;

enum class Compressor {
  kNoCompress = 0,
    kSnappy = 1,
    kGzip = 2,
};

// template<bool less, size_t I, typename... ARGS>
// struct LittleEndian;

// template<false, size_t I, typename... ARGS>
// struct LittleEndian {
// };


class Header {
public:
  Header() = default;
  Header(uint32_t num, uint32_t sum, Compressor ct, uint32_t cs):
    num_records_(num), checksum_(sum), compressor_(ct), compress_size_(cs) {}
  // compatible with Go
  void Write(std::ostream& os);

  // native interface in c++
  friend std::ostream& operator << (std::ostream& os, const Header& h);

  void Parse(std::istream& iss);

  uint32_t NumRecords() const {return num_records_;}
  uint32_t Checksum() const { return checksum_;}
  Compressor CompressType() const { return compressor_;}
  uint32_t CompressSize() const { return compress_size_;}
  bool operator==(const Header& rhs) const {
    return num_records_ == rhs.NumRecords() &&
      checksum_ == rhs.Checksum() &&
      compressor_ == rhs.CompressType() &&
      compress_size_ == rhs.CompressSize();
  }
private:
  uint32_t num_records_;
  uint32_t checksum_;
  Compressor compressor_;
  uint32_t compress_size_;
};

// Chunk
// a chunk contains the Header and optionally compressed records.
class Chunk {
public:
  Chunk() = default;
  void Add(const char* record, size_t length);
  void Add(const std::string&);

  bool Dump(std::ostream& os, Compressor ct);
  void Parse(std::istream& iss, int64_t offset);
  const std::string Record(int i) { return records_[i];}

private:
  std::vector<std::string> records_;
  size_t num_bytes_;
};

size_t CompressData(const std::stringstream& ss, Compressor ct, char* buffer);

uint32_t DeflateData(char* buffer, uint32_t size, Compressor c);

// Scanner

class Index {
public:
  int NumRecords() { return num_records_;}

  // Locate returns the index of chunk that contains the given record,
  // and the record index within the chunk.  It returns (-1, -1) if the
  // record is out of range.
  void Locate(int record_idx, std::pair<int, int>* out) {
    size_t sum = 0;
    for(size_t i=0; i < chunk_lens_.size(); ++i) {
      sum += chunk_lens_[i];
      if (static_cast<size_t>(record_idx) < sum) {
        out->first = i;
        out->second = record_idx - sum + chunk_lens_[i];
        return ;
      }
    }
    // out->swap(std::make_pair<int,int>(-1, -1));
    out->first = -1;
    out->second = -1;
  }
private:
  std::vector<int64_t> chunk_offsets_;
  std::vector<uint32_t> chunk_lens_;
  int num_records_;
  std::vector<int> chunk_records_;
};

// RangeScanner
// creates a scanner that sequencially reads records in the
// range [start, start+len).  If start < 0, it scans from the
// beginning.  If len < 0, it scans till the end of file.
class RangeScanner {
public:
  RangeScanner(std::istream is, Index idx, int start, int end);
  bool Scan();
  const std::string Record();
  
private:
  std::istream stream_;
  Index index_;
  int start_, end_, cur_;
  int chunk_index_;
  std::unique_ptr<Chunk> chunk_;
};

class Scanner {
public:
  Scanner(const char* paths);
  const std::string Record();
  bool Scan();
  void Close();

private:
  bool NextFile();
  int Err() { return err_;}

private:
  std::vector<std::string> paths_;
  FILE* cur_file_;
  RangeScanner* cur_scanner_;
  int path_idx_;
  bool end_;
  int err_;
};


class Writer {
public:
  Writer(std::ostream& os, int maxChunkSize, int compressor);
  Writer(std::ostream& os);

  // compatible with Go
  size_t Write(const char* buf, size_t length);
  size_t Write(const std::string& buf);

  template<typename T>
  Writer& operator << (const T& val) {
    stream_ << val;
    return *this;
  }

  void Close();
private:
  std::ostream stream_;
  std::shared_ptr<Chunk> chunk_;
  int max_chunk_size_;
  int compressor_;

private:
  Writer(const Writer&) = delete;
  Writer& operator=(const Writer&) = delete;
};

// NewWriter creates a RecordIO file writer.  Each chunk is compressed
// using the deflate algorithm given compression level.  Note that
// level 0 means no compression and -1 means default compression.
// Writer& NewWriter(std::ostream& os, int maxChunkSize, int compressor);

} // namespace recordio
