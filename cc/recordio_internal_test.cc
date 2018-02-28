#include "gtest/gtest.h"

#include "recordio.h"
#include <iostream>

using namespace recordio;

TEST(Recordio, ChunkHead) {
  Header hdr(0, 1, static_cast<Compressor>(2), 3);
  std::ostringstream oss;
  hdr.Write(oss);

  std::istringstream iss(oss.str());
  Header hdr2;
  hdr2.Parse(iss);

  std::ostringstream oss2;
  hdr2.Write(oss2);
  EXPECT_STREQ(oss2.str().c_str(), oss.str().c_str());
}

TEST(Recordio, WriteAndRead) {
  std::vector<std::string> data = {
		"12345",
		"1234",
		"12",
  };
  std::ostringstream os;
  //create a small chunk size Writer
  Writer w(os, 10, kNoCompress);

  w.Write()

  int count = -1;

  // not exceed chunk size
  w.Write(data[0]);
}

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
