#!/bin/bash
# Trivial script to compile
mpicc -O3 -g -std=gnu99 src/*.c -I src/ plugins/md-dummy.c plugins/md-posix.c  -I . -lrt -o md-workbench 

