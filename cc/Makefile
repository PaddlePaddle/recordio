GTEST_PATH=/Users/dzh/code/gtest

CFLAGS := -g -Werror -std=c++11
CFLAGS += -I$(GTEST_PATH)/

LDFLAGS := -L$(GTEST_PATH)/ -lgtest -W

OBJ=$(patsubst %.cc, %.o, $(wildcard *.cc))

all: recordio_test.bin
	echo "Done."

%.o:%.cc
	g++ $(CFLAGS) -c $< -o $@ $(LDFLAGS)

recordio.o: recordio.cc
	g++ $(CFLAGS) -c recordio.cc 

librecordio.a: recordio.o 
	ar rcs librecordio.a recordio.o

recordio_test.bin:librecordio.a recordio_test.o
	g++ $(CFLAGS) $^ -o $@ $(LDFLAGS)


clean:
	rm -f *.a *.o *.bin
	
