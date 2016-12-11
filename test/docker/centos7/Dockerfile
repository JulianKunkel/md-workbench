FROM centos:7

WORKDIR /data
RUN yum install -y mpich openmpi git pkg-config nano gcc wget bzip2 patch gcc-c++ make mpich-devel openmpi-devel openssl-devel cyrus-sasl-devel
RUN wget https://cmake.org/files/v3.0/cmake-3.0.0-1-src.tar.bz2
RUN tar -xf cmake-3.0.0-1-src.tar.bz2
RUN bash -c "./cmake-3.0.0-1.sh all ; cd cmake-3.0.0/.build && make install"

# FOR POSTGRES
RUN yum install -y postgresql-server postgresql-devel

# FOR MONGODB https://docs.mongodb.com/v3.2/tutorial/install-mongodb-on-red-hat/
RUN echo -e "[mongodb-org-2.6]\nname=MongoDB 2.6 Repository\nbaseurl=http://downloads-distro.mongodb.org/repo/redhat/os/x86_64/\ngpgcheck=0\nenabled=1" > /etc/yum.repos.d/mongodb-org-3.2.repo
RUN yum install -y mongodb-org
RUN wget https://github.com/mongodb/mongo-c-driver/releases/download/1.5.0/mongo-c-driver-1.5.0.tar.gz
RUN tar xzf mongo-c-driver-1.5.0.tar.gz
RUN cd mongo-c-driver-1.5.0 ; ./configure ; make -j 4 install
