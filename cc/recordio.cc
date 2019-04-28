#include <glob.h>
#include <cstring>
#include "snappy.h"
#include <iostream>

#include "recordio.h"

namespace recordio {

void Header::Parse(std::istream& iss) {
  iss.read(reinterpret_cast<char*>(&num_records_), sizeof(uint32_t));
  iss.read(reinterpret_cast<char*>(&checksum_), sizeof(uint32_t));
  iss.read(reinterpret_cast<char*>(&compressor_), sizeof(uint32_t));
  iss.read(reinterpret_cast<char*>(&compress_size_), sizeof(uint32_t));
}

void Header::Write(std::ostream& os) {
  os.write(reinterpret_cast<char*>(&num_records_), sizeof(uint32_t));
  os.write(reinterpret_cast<char*>(&checksum_), sizeof(uint32_t));
  os.write(reinterpret_cast<char*>(&compressor_), sizeof(uint32_t));
  os.write(reinterpret_cast<char*>(&compress_size_), sizeof(uint32_t));
}

std::ostream& operator << (std::ostream& os, const Header& h) {
  os << h.num_records_
     << h.checksum_
     << static_cast<uint32_t>(h.compressor_)
     << h.compress_size_;
  return os;
}

size_t CompressData(const std::string& os, Compressor ct, char* buffer) {
  size_t compress_size = 0;

  // std::unique_ptr<char[]> buffer(new char[kDefaultMaxChunkSize]);
  // std::string compressed;
  compress_size =os.size();
  memcpy(buffer, os.c_str(), compress_size);
  return compress_size;
}

void Chunk::Add(const std::string& s) {
  num_bytes_ += s.size() * sizeof(char);
  records_.emplace_back(std::move(s));
  // records_.resize(records_.size()+1);
  // records_[records_.size()-1] = s;
}

void Chunk::Add(const char* record, size_t length) {
  Add(std::string(record, length));
}

bool Chunk::Dump(std::ostream& os, Compressor ct) {
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
  std::unique_ptr<char[]> buffer(new char[kDefaultMaxChunkSize]);
  size_t compressed = CompressData(oss.str(), ct, buffer.get());


  // TODO(dzhwinter): crc32 checksum
  size_t checksum = compressed;

  Header hdr(records_.size(), checksum, ct, compressed);

  return true;
}

void Chunk::Parse(std::istream& iss, int64_t offset) {
  iss.seekg(offset, iss.beg);
  Header hdr;
  hdr.Parse(iss);

  std::unique_ptr<char[]> buffer(new char[kDefaultMaxChunkSize]);
  iss.read(buffer.get(), static_cast<size_t>(hdr.CompressSize()));
  // TODO(dzhwinter): checksum
  uint32_t deflated_size = DeflateData(buffer.get(), hdr.CompressSize(), hdr.CompressType());
  std::istringstream deflated(std::string(buffer.get(), deflated_size));
  for(size_t i=0; i < hdr.NumRecords(); ++i) {
    uint32_t rs;
    deflated >> rs;
    std::string record(rs, '\0');
    deflated.read(&record[0], rs);
    records_.emplace_back(record);
    num_bytes_ += record.size();
  }
}

uint32_t DeflateData(char* buffer, uint32_t size, Compressor c) { 
  uint32_t deflated_size = 0;
  std::string uncompressed;
  switch(c) {
  case Compressor::kNoCompress:
    deflated_size = size;
    break;
  case Compressor::kSnappy:
    // snappy::Uncompress(buffer, size, &uncompressed);
    // deflated_size = uncompressed.size();
    // memcpy(buffer, uncompressed.data(), uncompressed.size() * sizeof(char));
    break;
  }
  return deflated_size;
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

bool Scanner::NextFile() {
  
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
    // chunk_->reset(new Chunk());
}

bool RangeScanner::Scan() {
  
}

const std::string RangeScanner::Record() {
  // int i = index_.Locate(cur_);
  // return chunk_->Record(i);
}

Writer::Writer(std::ostream& os, int maxChunkSize, int compressor)
  :stream_(os.rdbuf()), max_chunk_size_(maxChunkSize), compressor_(compressor){
  // clear rdstate
  stream_.clear();
  chunk_.reset(new Chunk);
}

Writer::Writer(std::ostream& os) : Writer(os, -1, -1) {}

size_t Writer::Write(const std::string& buf) {
}

size_t Writer::Write(const char* buf, size_t length) {
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
