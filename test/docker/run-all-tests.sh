#!/bin/bash

ARGS=" /data/test/docker/run-machine-test.sh $@"
OPT="-it --rm -v $PWD/../../:/data/"
docker run $OPT -h ubuntu14.04 kunkel/md-real-io:ubuntu14.04 $ARGS
docker run $OPT -h ubuntu16.04 kunkel/md-real-io:ubuntu16.04 $ARGS
docker run $OPT -h ubuntu17.04 kunkel/md-real-io:ubuntu17.04 $ARGS

docker run $OPT -h centos6 kunkel/md-real-io:centos6 $ARGS

# To finalize:
# docker run $OPT -h centos7 kunkel/md-real-io:centos7 $ARGS

