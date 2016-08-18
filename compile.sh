#/bin/bash

CC=mpicc
CFLAGS="-g -O3 -Wall -std=gnu99 -I."
SOURCE="option.c md-real-io.c"

$CC $CFLAGS $SOURCE -o md-real-io 
