FROM ubuntu:14.04

WORKDIR /data
RUN apt-get update
RUN apt-get install -y libopenmpi-dev openmpi-bin mpich git cmake pkg-config gcc-4.7 nano wget
RUN wget https://cmake.org/files/v3.0/cmake-3.0.0-1-src.tar.bz2
RUN tar -xf cmake-3.0.0-1-src.tar.bz2
RUN bash -c "./cmake-3.0.0-1.sh all ; cd cmake-3.0.0/.build && make install"
