#!/usr/bin/zsh

#simple script for automating compiling and running

gcc -o test.o -g hash_test.c -lrt -lqlibc -pthread
./test.o
