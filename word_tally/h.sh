#!/usr/bin/zsh

gcc -o test.o -g hashTest.c -lrt -lqlibc -pthread
./test
