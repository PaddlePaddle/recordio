#include "gtest/gtest.h"

#include "recordio.h"

TEST(Recordio, ChunkHead) {
  using namespace recordio;
}

TEST(Recordio, WriteAndRead) {
  using namespace recordio;
  std::vector<std::string> data = {
		"12345",
		"1234",
		"12"};
  
  std::ostringstream os;
  Writer w(os, 10, kNoCompress);

  int count = -1;

  // not exceed chunk size
  w.Write(data[0]);
}

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
