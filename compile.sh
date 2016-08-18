#/bin/bash

CC=mpicc
CFLAGS="-g -O3 -Wall -std=gnu99 -I."
SOURCE="option.c md-real-io.c"

VERSION=$(git log -1 --format="%H %aD")
if [[ $(git diff|head -n 10) != "" ]] ; then
	VERSION="${VERSION} modified!"
fi 

$CC $CFLAGS $SOURCE -DVERSION="\"$VERSION\"" -o md-real-io 
