#!/usr/bin/zsh

gcc -o test -g hashTest.c -lrt -lqlibc -pthread
./test
gcc -o test -g hashTest.c -lrt -lqlibc -pthread
