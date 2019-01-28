# -*- Makefile -*-

CXXFLAGS=-g -std=c++17 -Wall -Wextra
LD=g++
LIB=-lrocksdb
OBJ=$(SRC:.cpp=.o)
PLPATHS=-p library=prolog -p foreign="$(PACKSODIR)"
SOBJ=$(PACKSODIR)/rocksdb.$(SOEXT)
SRC=$(wildcard cpp/*.cpp)

.PHONY: all check clean distclean install

all: $(SOBJ)

$(SOBJ): $(OBJ)
	mkdir -p $(PACKSODIR)
	$(LD) $(ARCH) $(LDSOFLAGS) -o $@ $^ $(LIB) $(SWISOLIB)

cpp/%.o: cpp/%.cpp
	$(CXX) $(ARCH) $(CFLAGS) $(CXXFLAGS) -c -o $@ $<

check::
	$(SWIPL) $(PLPATHS) -g run,halt -t 'halt(1)' test/test_rocksdb.pl

clean:
distclean:
	$(RM) $(SOBJ) $(OBJ)

install::
