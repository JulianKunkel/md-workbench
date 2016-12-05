#!/bin/bash

# Ubuntu16.04: apt-get install mongodb-clients mongodb-server 

killall -9 mongod

rm -rf /tmp/mongo
mkdir /tmp/mongo
mongod --dbpath /tmp/mongo &

sleep 1

echo '
use mdbench
db.createUser(
  {
    user: "mduser",
    pwd: "mduser",
    roles: [
       { role: "readWrite", db: "mdbench" }
    ]
  }
) ' | mongo
