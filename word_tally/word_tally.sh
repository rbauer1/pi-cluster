#!/usr/bin/zsh

#simple script to automate cleaning up, compiling, distributing, and running
rm -r OPS
mpicc -o run.o distributed_epub2text_wordtally.c -lrt -lqlibc -pthread
/home/pi/cluster/cluster_scripts/send_file.py /home/pi/cluster/word_tally/run.o
mpiexec -f /home/pi/chris_machinefile -n 4 /home/pi/cluster/word_tally/run.o sherlock_holmes_ebook.epub
