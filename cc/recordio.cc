#include <glob.h>
#include <cstring>

#include "recordio.h"


namespace recordio {


void Header::Parse(const Header& h) {
  *this = std::move(h);
}

void Header::Write(std::ostream& os) {
  os << *this;
}

std::ostream& operator << (std::ostream& os, const Header& h) {
  os << h.num_records_
     << h.checksum_
     << static_cast<int>(h.compress_type_)
     << h.compress_size_;
  return os;
}

size_t CompressData(const std::string& os, CompressType ct, char* buffer) {
  size_t compress_size = 0;

  // std::unique_ptr<char[]> buffer(new char[kDefaultMaxChunkSize]);
  // std::string compressed;
  compress_size = os.size();
  memcpy(buffer, os.c_str(), compress_size);
  return compress_size;
}

void Chunk::Add(const std::string& s) {
  num_bytes_ += s.size();
  records_.emplace_back(std::move(s));
}

void Chunk::Add(const char* record, size_t length) {
  Add(std::string(record, length));
}

bool Chunk::Dump(std::ostream& os, CompressType ct) {
  if (records_.size() == 0) return false;

  // TODO(dzhwinter):
  // we pack the string with same size buffer,
  // then compress with another buffer.
  // Here can be optimized if it is the bottle-neck.
  std::ostringstream oss;
  for(auto& record : records_) {
    unsigned len = record.size();
    oss << len;
    oss << record;
    // os.write(std::to_string(len).c_str(), sizeof(unsigned));
    // os.write(record.c_str(), record.size());
  }
  char* buffer = new char[kDefaultMaxChunkSize];
  size_t compressed = CompressData(oss.str(), ct, buffer);


  // TODO(dzhwinter): crc32 checksum

  Header hdr();
  delete[] buffer;
  return true;
}

Scanner::Scanner(const char* paths) : cur_file_(nullptr), path_idx_(0), end_(false) {
  glob_t glob_result;
  glob(paths,GLOB_TILDE,NULL,&glob_result);

  for(size_t i=0; i < glob_result.gl_pathc; ++i) {
    paths_.emplace_back(std::string(glob_result.gl_pathv[i]));
  }
  globfree(&glob_result);
}

bool Scanner::Scan() {
  if (err_ == -1 || end_ == true) {
    return false;
  }
  if (cur_scanner_ == nullptr) {
    if(!NextFile()) {
      end_ = true;
      return false;
    }
    if (err_ == -1) { return false;}
  }
  if (!cur_scanner_->Scan()) {
    if (err_ == -1) {
      return false;
    }
  }

  return true;
}

RangeScanner::RangeScanner(std::istream is, Index idx, int start, int len)
  : stream_(is.rdbuf()), index_(idx) {
    if (start < 0) {
      start = 0;
    }
    if (len < 0 || start + len >= idx.NumRecords()) {
      len = idx.NumRecords() - start;
    }

    start_ = start;
    end_ = start + len;
    cur_ = start - 1;
    chunk_index_ = -1;
    chunk_->reset(new Chunk());
}

bool RangeScanner::Scan() {
  
}

const std::string RangeScanner::Record() {
  int i = index_.Locate(cur_);
  return chunk_->Record(i);
}

Writer::Writer(std::ostream& os, int maxChunkSize, int compressor)
  :stream_(os.rdbuf()), chunk_(nullptr), max_chunk_size_(maxChunkSize), compressor_(compressor){
  // clear rdstate
  stream_.clear();
}

Writer::Writer(std::ostream& os) : Writer(os, -1, -1) {}

void Writer::Write(const std::string& buf) {
  stream_ << buf;
}

void Writer::Write(const char* buf, size_t length) {
  // std::string s(buf, length);
  Write(std::string(buf, length));
}

void Writer::Close() {
  stream_.flush();
  stream_.setstate(std::ios::eofbit);
}


// Writer* NewWriter(std::ostream& os, int maxChunkSize, int compressor) {
// }

} // namespace recordio
