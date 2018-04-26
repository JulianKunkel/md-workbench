#!/bin/bash

mkdir -p ../../build-docker

ARGS=" /data/test/docker/run-machine-test.sh $@"
UD="-u $(id -u):$(id -g)"
OPT="-it --rm -v $PWD/../../:/data/"
ERROR=0
docker run $OPT -h ubuntu14.04 kunkel/md-workbench:ubuntu14.04 $ARGS
ERROR=$(($ERROR+$?))
docker run $OPT -h ubuntu16.04 kunkel/md-workbench:ubuntu16.04 $ARGS
ERROR=$(($ERROR+$?))
docker run $OPT -h ubuntu17.04 kunkel/md-workbench:ubuntu17.04 $ARGS
ERROR=$(($ERROR+$?))
docker run $OPT -h centos6 kunkel/md-workbench:centos6 $ARGS
ERROR=$(($ERROR+$?))
docker run $OPT -h centos7 kunkel/md-workbench:centos7 $ARGS
ERROR=$(($ERROR+$?))

if [[ $ERROR != 0 ]] ; then
	echo "Errors occured: $ERROR"
else
	echo "OK all tests passed!"
fi
