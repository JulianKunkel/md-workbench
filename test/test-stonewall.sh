#!/bin/bash -e

# This little system test tests stonewalling
ARGS="-O=1 -I=10 -D=2 -P=20 -R=3 --process-reports -L=/tmp/1 -S=3901 --latency-all --run-info-file=mdreal-status"
PLUGIN_ARGS="-i=dummy -- -s=30000"
RUN="mpiexec -n 2 ../build/src/md-workbench"
$RUN -w=1  $ARGS  $PLUGIN_ARGS

$RUN -w=1 -W  $ARGS  $PLUGIN_ARGS


ARGS="-O=1 -I=10000 -D=10 -P=20000 -R=3 -S=3901 --run-info-file=mdreal-status"
PLUGIN_ARGS=""
RUN="mpiexec -n 2 ../build/src/md-workbench"
$RUN -w=1 -W $ARGS  $PLUGIN_ARGS

