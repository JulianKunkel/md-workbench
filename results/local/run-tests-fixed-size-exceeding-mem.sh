#!/bin/bash

export PATH=$PATH:/usr/lib64/mpich/bin/
BENCH=/home/kunkel/benchmarks/md-real-io/build/src/md-real-io

if [[ $HOSTNAME != "sandy9" ]] ; then
	echo "This test is run on the host sandy9!"
	exit 1
fi

function runBenchmark(){
	NAME=$1
	DEVICE=$2
	OPTIONS=$3

	for fs in ext4 xfs btrfs ; do
		if [[ "$fs" == "ext4" ]] ; then
		mkfs.$fs $DEVICE || exit 1
		else
		mkfs.$fs -f $DEVICE || exit 1
		fi
		mount $DEVICE $OPTIONS /mnt/test || exit 1
		for M in 1000 ; do
			for N in 1 2 3 4 5 6 7 8 10 12 15 ; do
				FILE=/home/kunkel/benchmarks/md-real-io/$NAME-$fs-${M}bigger--$N.txt
				if [[ ! -e $FILE ]] ; then
					mpiexec -n $N $BENCH -P=$((10000/$N)) -D=50 -I=10000 -i=posix -m=$M -R=5 --process-reports -- -D=/mnt/test/out > $FILE 2>&1
				fi
			done
		done
		umount /mnt/test || exit 1
	done
}

mkdir /mnt/test || exit 1

runBenchmark "HDD" "/dev/sdc1" "-o relatime"
runBenchmark "SSD" "/dev/sdd1" "-o discard,relatime"
