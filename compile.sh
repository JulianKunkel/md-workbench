#/bin/bash


if [[ "$CC" == "" ]]; then
	CC=mpicc
fi
if [[ "$CFLAGS" == "" ]]; then
	CFLAGS="-g -O3 -Wall -std=gnu99"
fi

CFLAGS="$CFLAGS -I. -DMD_PLUGIN_POSIX -DMD_PLUGIN_POSTGRES -DMD_PLUGIN_MONGO -I/usr/local/include/libmongoc-1.0 -I/usr/local/include/libbson-1.0"

SOURCE="option.c md-real-io.c plugins/md-posix.c plugins/md-postgres.c plugins/md-mongo.c"

VERSION=$(git log -1 --format="%H %aD")
if [[ $(git diff|head -n 10) != "" ]] ; then
	VERSION="${VERSION} modified!"
fi

$CC $CFLAGS $SOURCE -DVERSION="\"$VERSION\"" -lpq -lmongoc-1.0 -lbson-1.0 -o md-real-io
