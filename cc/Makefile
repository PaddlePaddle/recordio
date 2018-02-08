GTEST_PATH=/Users/dzh/code/gtest
SNAPPY_PATH=/Users/dzh/github/snappy

CFLAGS := -g -Werror -std=c++11
CFLAGS += -I$(GTEST_PATH)/ -I$(SNAPPY_PATH)/
# CFLAGS += $(SNAPPY_PATH)/libsnappy.a

LDFLAGS := $(SNAPPY_PATH)/libsnappy.a
LDFLAGS += -L$(GTEST_PATH)/ -L$(SNAPPY_PATH)/ -lgtest -lsnappy -W

OBJ=$(patsubst %.cc, %.o, $(wildcard *.cc))

all: librecordio.a recordio_test.bin recordio_internal_test.bin
	echo "Done."

%.o:%.cc
	g++ $(CFLAGS) -c $< -o $@ $(LDFLAGS)

recordio.o: recordio.cc
	g++ $(CFLAGS) -c recordio.cc

librecordio.a: recordio.o
	ar rcs librecordio.a recordio.o $(SNAPPY_PATH)/libsnappy.a

recordio_test.bin:librecordio.a recordio_test.o $(SNAPPY_PATH)/libsnappy.a
	g++ $(CFLAGS) $^ -o $@ $(LDFLAGS) 

recordio_internal_test.bin:librecordio.a recordio_internal_test.o
	g++ $(CFLAGS) $^ -o $@ $(LDFLAGS)


clean:
	rm -f *.a *.o *.bin
	
