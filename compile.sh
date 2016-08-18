#/bin/bash


if [[ "$CC" == "" ]]; then
	CC=mpicc
fi
if [[ "$CFLAGS" == "" ]]; then
	CFLAGS="-g -O3 -Wall -std=gnu99"
fi

SOURCE="option.c md-real-io.c"

VERSION=$(git log -1 --format="%H %aD")
if [[ $(git diff|head -n 10) != "" ]] ; then
	VERSION="${VERSION} modified!"
fi 

$CC $CFLAGS $SOURCE -DVERSION="\"$VERSION\"" -o md-real-io 
