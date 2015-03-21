#!/usr/bin/zsh

rm -r OPS
gcc -o run distributed_epub2text.c
./run sherlock_holmes_ebook.epub

