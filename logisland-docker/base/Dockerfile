FROM centos:6
MAINTAINER Hurence


USER root

# install dev tools
RUN yum clean all; \
    rpm --rebuilddb; \
    yum install -y curl which tar sudo openssh-server openssh-clients rsync wget git svn telnet nano vim;
RUN yum groupinstall -y 'Development Tools';


# Java 8
RUN wget --no-cookies --no-check-certificate --header "Cookie: gpw_e24=http%3A%2F%2Fwww.oracle.com%2F; oraclelicense=accept-securebackup-cookie" "http://download.oracle.com/otn-pub/java/jdk/8u131-b11/d54c1d3a095b4ff2b6607d096fa80163/jre-8u131-linux-x64.rpm" && yum -y localinstall jre-8u131-linux-x64.rpm && rm jre-8u131-linux-x64.rpm
ENV JAVA_HOME /usr


# Install Nginx.
RUN rm -rf /var/cache/yum/x86_64/6/extras/packages && rm -rf /tmp && yum -y install epel-release && yum -y install nginx
COPY default.conf /etc/nginx/conf.d/default.conf
EXPOSE 80 443


# Build and install kafkacat util
RUN cd /usr/local; \
    git clone https://github.com/edenhill/librdkafka.git; \
    cd librdkafka/;  \
    ./configure; \
    make; \
    make install;
RUN cd /usr/local; \
    git clone https://github.com/edenhill/kafkacat.git; \
    cd kafkacat/;  \
    ./configure; \
    make; \
    make install;
ENV LD_LIBRARY_PATH $LD_LIBRARY_PATH:/usr/local/lib

# Install python
RUN sudo yum install -y zlib-devel  bzip2-devel openssl-devel ncurses-devel sqlite-devel python-devel libpcap-devel
RUN cd /opt; \
    wget --no-check-certificate https://www.python.org/ftp/python/2.7.6/Python-2.7.6.tar.xz; \
    tar xf Python-2.7.6.tar.xz; \
    cd Python-2.7.6; \
    ./configure --prefix=/usr/local; \
    make && make altinstall;
RUN curl https://bootstrap.pypa.io/get-pip.py | python2.7 -
ENV PATH /usr/local/bin:$PATH

# Install pycapa (python for pcap)
RUN mkdir /tmp; \
    cd /tmp; \
    git clone https://github.com/apache/incubator-metron.git
RUN cd /tmp/incubator-metron/metron-sensors/pycapa; \
    pip install six; \
    /usr/local/bin/pip install -r requirements.txt; \
    python2.7 setup.py install; \
    rm -rf /tmp/incubator-metron

# update boot script
COPY bootstrap.sh /etc/bootstrap.sh
RUN chown root.root /etc/bootstrap.sh
RUN chmod 700 /etc/bootstrap.sh

ENTRYPOINT ["/etc/bootstrap.sh"]
