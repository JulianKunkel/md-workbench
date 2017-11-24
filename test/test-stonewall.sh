#!/bin/bash -e

# This little system test tests stonewalling
ARGS="-O=1 -I=10 -D=2 -P=20 -R=3 --process-reports -L=/tmp/1 -S=3901 --latency-all --run-info-file=mdreal-status -i=dummy"
PLUGIN_ARGS="-- -s=30000"
RUN="mpiexec -n 2 ../build/src/md-real-io"
$RUN -w=1  $ARGS  $PLUGIN_ARGS

$RUN -w=1 -W  $ARGS  $PLUGIN_ARGS
