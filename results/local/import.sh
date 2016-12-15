#!/bin/bash

for dev in HDD SSD ; do
for fs in ext4 xfs btrfs ; do
for N in $(seq 1 12); do

../../results-in-db.py $dev-$fs-$N sandy9/$dev-$fs-*-$N.txt

done
done
done
