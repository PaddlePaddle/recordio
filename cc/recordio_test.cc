#include "gtest/gtest.h"

#include <cstring>

#include "recordio.h"

TEST(Recordio, Write) {
  using namespace recordio;
  std::ostringstream os;
  Writer w(os, -1, -1);
  w.Write("Hello");

  const char* buf = {"World!"};
  w.Write(buf, sizeof(buf));

  std::string sbuf = {"Hello"};
  w.Write(sbuf);

  w.Close();
}

// TEST(Recordio, Scan) {
//   using namespace recordio;

//   {
//     std::ofstream ofs("/tmp/example_recordio_0");
//     ASSERT_EQ(ofs.is_open(), true);

//     Writer w(ofs, -1, -1);
//     w.Write("Hello");
//     w.Close();
//     ofs.close();
//   }

//   {
//     std::ofstream ofs("/tmp/example_recordio_1");
//     ASSERT_EQ(ofs.is_open(), true);

//     Writer w(ofs, -1, -1);
//     w.Write("World");
//     w.Close();
//     ofs.close();
//   }

//   Scanner s("/tmp/example_recordio_*");
//   ASSERT_NE(s, nullptr);

//   std::ostringstream out;
//   while(s->Scan()) {
//     out << s->Record();
//   }
//   ASSERT_STREQ(out.str(), "HelloWorld");
// }

// TEST(Recordio, RangeScanner) {
//   using namespace recordio;

//   const char * path = {"/tmp/example_recordio_0"};
//   {
//     std::ofstream ofs(path);
//     ASSERT_EQ(ofs.is_open(), true, "Failed created file");

//     Writer* w = NewWriter(ofs, -1, -1);
//     w.Write("Hello");
//     w.Write("World");
//     w.Close();
//     ofs.close();
//   }

// }

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
