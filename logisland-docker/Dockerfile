FROM centos:6.7
MAINTAINER Hurence


USER root

# install dev tools
RUN yum clean all; \
    rpm --rebuilddb; \
    yum install -y curl which tar sudo openssh-server openssh-clients rsync wget git svn telnet nano;
RUN yum groupinstall -y 'Development Tools';


# Java 8
RUN wget --no-cookies --no-check-certificate --header "Cookie: gpw_e24=http%3A%2F%2Fwww.oracle.com%2F; oraclelicense=accept-securebackup-cookie" "http://download.oracle.com/otn-pub/java/jdk/8u60-b27/jre-8u60-linux-x64.rpm" && yum -y localinstall jre-8u60-linux-x64.rpm && rm jre-8u60-linux-x64.rpm
ENV JAVA_HOME /usr


# Install Nginx.
RUN rm -rf /var/cache/yum/x86_64/6/extras/packages && rm -rf /tmp && yum -y install epel-release && yum -y install nginx
COPY default.conf /etc/nginx/conf.d/default.conf
EXPOSE 80 443


# Kafka
RUN curl -s http://apache.crihan.fr/dist/kafka/0.9.0.1/kafka_2.10-0.9.0.1.tgz | tar -xz -C /usr/local/
RUN cd /usr/local && ln -s kafka_2.10-0.9.0.1 kafka
ENV KAFKA_HOME /usr/local/kafka
EXPOSE 2181 9092
COPY server.properties $KAFKA_HOME/config/server.properties

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


# Spark
RUN curl -s http://d3kbcqa49mib13.cloudfront.net/spark-1.6.2-bin-hadoop2.6.tgz | tar -xz -C /usr/local/
RUN cd /usr/local && ln -s spark-1.6.2-bin-hadoop2.6 spark
ENV SPARK_HOME /usr/local/spark
ENV PATH $PATH:$SPARK_HOME/bin
EXPOSE 4040


# Kibana
RUN curl -s https://download.elastic.co/kibana/kibana/kibana-4.5.1-linux-x64.tar.gz | tar -xz -C /usr/local/
RUN cd /usr/local && ln -s kibana-4.5.1-linux-x64 kibana
ENV KIBANA_HOME /usr/local/kibana
EXPOSE 5601


# Elasticsearch
RUN curl -s https://download.elastic.co/elasticsearch/release/org/elasticsearch/distribution/tar/elasticsearch/2.3.3/elasticsearch-2.3.3.tar.gz | tar -xz -C /usr/local/
RUN cd /usr/local && ln -s elasticsearch-2.3.3 elasticsearch
ENV ES_HOME /usr/local/elasticsearch
COPY elasticsearch.yml /usr/local/elasticsearch/config/elasticsearch.yml
RUN useradd -ms /bin/bash elastic
RUN chown -R elastic /usr/local/elasticsearch/
RUN mkdir /tmp; chmod 777 /tmp
USER elastic
RUN mkdir /usr/local/elasticsearch/plugins; /usr/local/elasticsearch/bin/plugin install lmenezes/elasticsearch-kopf/2.3.3
RUN /usr/local/elasticsearch/bin/plugin install mobz/elasticsearch-head
USER root
EXPOSE 9200 9300 9201 9301

# Grafana
RUN yum install -y https://grafanarel.s3.amazonaws.com/builds/grafana-3.1.1-1470047149.x86_64.rpm

RUN curl -s http://www-eu.apache.org/dist/nifi/1.1.1/nifi-1.1.1-bin.tar.gz | tar -xz -C /usr/local/
RUN cd /usr/local && ln -s nifi-1.1.1 nifi
ENV NIFI_HOME /usr/local/nifi
EXPOSE 8080
EXPOSE 3000

# Log-Island
COPY logisland-0.9.7-SNAPSHOT-bin.tar.gz /usr/local/logisland-0.9.7-SNAPSHOT-bin.tar.gz
RUN cd /usr/local; \
    tar -xzf logisland-0.9.7-SNAPSHOT-bin.tar.gz; \
    ln -s /usr/local/logisland-0.9.7-SNAPSHOT /usr/local/logisland; \
    mkdir /usr/local/logisland/log; \
    rm -f /usr/local/logisland-0.9.7-SNAPSHOT-bin.tar.gz
ENV LOGISLAND_HOME /usr/local/logisland


# Kafka manager
#COPY docker/kafka-manager-1.3.0.4.zip /usr/local/kafka-manager-1.3.0.4.zip
#RUN cd /usr/local; \
#    unzip kafka-manager-1.3.0.4.zip; \
#    ln -s /usr/local/kafka-manager-1.3.0.4 /usr/local/kafka-manager; \
#    rm -f /usr/local/kafka-manager-1.3.0.4.zip
#ENV KAFKA_MGR_HOME /usr/local/kafka-manager
#ENV ZK_HOSTS sandbox:2181
#EXPOSE 9000



# update boot script
COPY bootstrap.sh /etc/bootstrap.sh
RUN chown root.root /etc/bootstrap.sh
RUN chmod 700 /etc/bootstrap.sh

ENTRYPOINT ["/etc/bootstrap.sh"]
