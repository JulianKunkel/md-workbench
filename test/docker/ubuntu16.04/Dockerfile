FROM ubuntu:16.04

WORKDIR /data
RUN apt-get update
RUN apt-get install -y libopenmpi-dev openmpi-bin mpich git cmake pkg-config gcc-5 gcc-4.8 nano wget

# POSTGRES TESTS
RUN apt-get install -y postgresql libpq-dev
# Sleep is neccesary as the DB starts in the background
RUN /etc/init.d/postgresql start; sleep 5; /etc/init.d/postgresql status ; su - postgres -c "createuser root -d" ; su - postgres -c "echo CREATE DATABASE ROOT OWNER ROOT | psql"

# MONGODB TESTS
RUN apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv EA312927
RUN bash -c 'echo "deb http://repo.mongodb.org/apt/ubuntu xenial/mongodb-org/3.2 multiverse" > /etc/apt/sources.list.d/mongodb-org-3.2.list'
RUN apt-get update
RUN apt-get install -y mongodb-org libsasl2-dev
RUN wget https://github.com/mongodb/mongo-c-driver/releases/download/1.5.0/mongo-c-driver-1.5.0.tar.gz
RUN tar xzf mongo-c-driver-1.5.0.tar.gz
RUN cd mongo-c-driver-1.5.0 ; ./configure ; make -j 4 install

# LIBS3 TESTS
RUN apt-get install -y libs3-dev

