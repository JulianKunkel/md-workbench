#!/bin/bash -e

# This little system test shows how to separate md-real execution into multiple phases
# It does set various useful options like stonewalling, recording of traces

ARGS="-w=10 -W -O=1 -I=2000 -D=2 -P=10000 -R=3 --process-reports -L=/tmp/1 -S=3901 --latency-all --run-info-file=mdreal-status"

PLUGIN_ARGS="-- -D=md-testdir"
RUN="mpiexec -n 2 ../build/src/md-real-io"

$RUN $ARGS -1 $PLUGIN_ARGS
$RUN $ARGS -2 $PLUGIN_ARGS
$RUN $ARGS -2 $PLUGIN_ARGS
$RUN $ARGS -3 $PLUGIN_ARGS


$RUN $ARGS -1 $PLUGIN_ARGS
$RUN $ARGS -2 --read-only $PLUGIN_ARGS
$RUN $ARGS -2 $PLUGIN_ARGS
$RUN $ARGS -3 $PLUGIN_ARGS
