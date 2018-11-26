#
# Docker file to build a Bro image pre loeaded with Bro and Bro-Kafka plugin.
# This is used in the Bro/Logisland tutorial: Indexing Bro events.
#

FROM ubuntu:16.04
MAINTAINER Hurence version: 0.1

# Update apt sources
RUN apt-get update

USER root

#
# Define where things will be installed
#

# Base dir for work is the user directory 
ARG HOME_DIR=/root
# Where to put sources to compile
ARG SRC_DIR=${HOME_DIR}/sources
# Where to put .pcap files
ARG PCAP_DIR=${HOME_DIR}/pcap_files
# Base install dir for binaries
ARG INSTALL_DIR=/usr/local
# Installed Bro base dir
ARG BRO_BASE_DIR=${INSTALL_DIR}/bro

#
# Create needed base directories
#

RUN mkdir -p ${SRC_DIR}
RUN mkdir -p ${PCAP_DIR}
RUN mkdir -p ${BRO_BASE_DIR}

#
# Install dev tools
#

# Note: apt-get install -y avoids prompting for confirmation

# Git
RUN apt-get install -y git
# make, gcc...
RUN apt-get install -y build-essential
# Special additional components needed for building bro workspace
RUN apt-get install -y python-dev flex bison libpcap-dev libssl-dev zlib1g-dev flex bison swig
# CMake
RUN apt-get install -y cmake

#
# Install sources, build and install Bro, librdkafka and the Bro-Kafka plugin
#
    
# Download, build and install Bro 
RUN cd ${SRC_DIR}; \
    git clone --recursive https://github.com/bro/bro.git; \
    cd bro;  \
    ./configure --prefix=${BRO_BASE_DIR}; \
    make; \
    make install;
    
# Download, build and install librdkafka 
RUN cd ${SRC_DIR}; \
    git clone https://github.com/edenhill/librdkafka.git; \
    cd librdkafka;  \
    ./configure --prefix=${INSTALL_DIR}; \
    make; \
    make install;
    
# Download, build and install Bro-Kafka plugin 
RUN cd ${SRC_DIR}; \
    git clone https://github.com/apache/metron-bro-plugin-kafka.git; \
    cd metron-bro-plugin-kafka; \
    ./configure --install-root=${BRO_BASE_DIR}/lib/bro/plugins --bro-dist=${SRC_DIR}/bro --with-librdkafka=${INSTALL_DIR}/lib; \
    make; \
    make install;
    
#
# Install additional tools needed for the bro tutorial
#

# Install curl for accessing Logisland ElasticSearch
RUN apt-get install -y curl
# Install vi to be able to edit bro config files
RUN apt-get install -y vim
# Install ifconfig to be able to check container ip
RUN apt-get install -y net-tools
# Install ping requests to easily generate a DNS query
RUN apt-get install -y iputils-ping

#
# Define environment variables
#

# Bro
ENV BRO_HOME ${BRO_BASE_DIR}
# Update path for bro binary
ENV PATH="${BRO_HOME}/bin:${PATH}"
# Pcap files
ENV PCAP_HOME ${PCAP_DIR}

#
# Copy .pcap files
#

# ssh.pcap comes from https://www.bro.org/bro-workshop-2011/solutions/notices/
# It is used to be able to easily generate a Bro notice (password guessing)
COPY ssh.pcap ${PCAP_DIR}

WORKDIR /root
