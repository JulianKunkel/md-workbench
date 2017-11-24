#!/usr/bin/bash -e

ARGS="-w=600 -W -O=1 -I=2000 -D=1 -P=10000 -R=3 --process-reports -L=/tmp/1   -S=3901 --latency-all"
PLUGIN_ARGS="-- -D=md-testdir"

./src/md-real-io $ARGS -1 $PLUGIN_ARGS
./src/md-real-io $ARGS -2 $PLUGIN_ARGS
./src/md-real-io $ARGS -2 $PLUGIN_ARGS
./src/md-real-io $ARGS -3 $PLUGIN_ARGS


./src/md-real-io $ARGS -1 $PLUGIN_ARGS
./src/md-real-io $ARGS -2 --read-only $PLUGIN_ARGS
./src/md-real-io $ARGS -2 $PLUGIN_ARGS
./src/md-real-io $ARGS -3 $PLUGIN_ARGS
