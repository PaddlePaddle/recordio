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

enum class CompressType {
  kNoCompress = 0,
    kSnappy = 1,
    kGzip = 2,
};

class Header {
public:
  Header(size_t n, size_t sum, CompressType c, size_t cs):
    num_records_(n), checksum_(sum), compress_type_(c), compress_size_(cs) {}
  // compatible with Go
  void Write(std::ostream& os);

  // native interface in c++
  friend std::ostream& operator << (std::ostream& os, const Header& h);
  void Parse(const Header& h);
private:
  size_t num_records_;
  size_t checksum_;
  CompressType compress_type_;
  size_t compress_size_;
};

// Chunk
// a chunk contains the Header and optionally compressed records.
class Chunk {
public:
  void Add(const char* record, size_t length);
  void Add(const std::string&);

  bool Dump(std::ostream& os, CompressType ct);
  void Parse();
  const std::string Record(int i) { return records_[i];}
private:
  std::vector<const std::string> records_;
  size_t num_bytes_;
};

size_t CompressData(const std::stringstream& ss, CompressType ct);


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
      if (record_idx < sum) {
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
  std::vector<unsigned> chunk_lens_;
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

  // recommend interface
  template<typename T>
  Writer& operator << (const T& val) {
    stream_ << val;
    return *this;
  }

  void Close();
private:
  std::ostream stream_;
  Chunk* chunk_;
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
