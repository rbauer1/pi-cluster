#!/usr/bin/zsh

rm -r OPS
mpicc -o run distributed_epub2text.c -lrt -lqlibc -pthread
/home/pi/cluster_scripts/send_file.py /home/pi/test/run
mpiexec -f /home/pi/chris_machinefile -n 4 /home/pi/test/run sherlock_holmes_ebook.epub
