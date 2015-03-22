#!/usr/bin/zsh

#simple script for automating compiling and running

gcc -o test.o -g hashTest.c -lrt -lqlibc -pthread
./test.o
