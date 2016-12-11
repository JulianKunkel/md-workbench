#!/bin/bash

echo "Running test for $HOSTNAME"
SCRIPT=/data/test/docker/machine-tests/$HOSTNAME
if [ -e $SCRIPT ] ; then
  BUILD=/data/build-docker/$HOSTNAME/
  mkdir -p $BUILD
  $SCRIPT $BUILD "$@"
  if [[ $? != 0 ]]  ; then
    echo "Errors occured, see: "
    find $BUILD/ -name LastTest.log | sed "s#/data/#../../#"
    exit 1
  fi
  echo ""
  exit 0
else
  echo "Unknown machine type"
  exit 1
fi
