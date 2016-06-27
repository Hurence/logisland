FROM centos:6.7
MAINTAINER Hurence


USER root

# install dev tools
RUN yum clean all; \
    rpm --rebuilddb; \
    yum install -y curl which tar sudo openssh-server openssh-clients rsync wget git svn;
RUN yum groupinstall -y 'Development Tools';


# Java 8
RUN wget --no-cookies --no-check-certificate --header "Cookie: gpw_e24=http%3A%2F%2Fwww.oracle.com%2F; oraclelicense=accept-securebackup-cookie" "http://download.oracle.com/otn-pub/java/jdk/8u60-b27/jre-8u60-linux-x64.rpm" && yum -y localinstall jre-8u60-linux-x64.rpm && rm jre-8u60-linux-x64.rpm
ENV JAVA_HOME /usr


# Install Nginx.
RUN rm -rf /var/cache/yum/x86_64/6/extras/packages && rm -rf /tmp && yum -y install epel-release && yum -y install nginx
COPY docker/default.conf /etc/nginx/conf.d/default.conf
EXPOSE 80 443


# Flume
RUN curl -s http://apache.crihan.fr/dist/flume/1.6.0/apache-flume-1.6.0-bin.tar.gz | tar -xz -C /usr/local/
RUN cd /usr/local && ln -s apache-flume-1.6.0-bin flume
ENV FLUME_HOME /usr/local/flume
ENV PATH $PATH:$FLUME_HOME/bin


# Kafka
RUN curl -s http://apache.mirrors.ovh.net/ftp.apache.org/dist/kafka/0.8.2.2/kafka_2.10-0.8.2.2.tgz | tar -xz -C /usr/local/
RUN cd /usr/local && ln -s kafka_2.10-0.8.2.2 kafka
ENV KAFKA_HOME /usr/local/kafka
EXPOSE 2181 9092


# Spark
RUN curl -s http://d3kbcqa49mib13.cloudfront.net/spark-1.6.0-bin-hadoop2.6.tgz | tar -xz -C /usr/local/
RUN cd /usr/local && ln -s spark-1.6.0-bin-hadoop2.6 spark
ENV SPARK_HOME /usr/local/spark
ENV PATH $PATH:$SPARK_HOME/bin
EXPOSE 4040


# Logstash
RUN curl -s https://download.elastic.co/logstash/logstash/logstash-2.1.1.tar.gz | tar -xz -C /usr/local/
RUN cd /usr/local && ln -s logstash-2.1.1 logstash
ENV LOGSTASH_HOME /usr/local/logstash
ENV PATH $PATH:$LOGSTASH_HOME/bin


# Kibana
RUN curl -s https://download.elastic.co/kibana/kibana/kibana-4.3.1-linux-x64.tar.gz | tar -xz -C /usr/local/
RUN cd /usr/local && ln -s kibana-4.3.1-linux-x64 kibana
ENV KIBANA_HOME /usr/local/kibana
EXPOSE 5601


# Elasticsearch
RUN curl -s https://download.elasticsearch.org/elasticsearch/release/org/elasticsearch/distribution/tar/elasticsearch/2.1.1/elasticsearch-2.1.1.tar.gz | tar -xz -C /usr/local/
RUN cd /usr/local && ln -s elasticsearch-2.1.1 elasticsearch
ENV ES_HOME /usr/local/elasticsearch
COPY docker/elasticsearch.yml /usr/local/elasticsearch/config/elasticsearch.yml
RUN useradd -ms /bin/bash elastic
RUN chown -R elastic /usr/local/elasticsearch/

RUN mkdir /tmp; chmod 777 /tmp
USER elastic
RUN mkdir /usr/local/elasticsearch/plugins; /usr/local/elasticsearch/bin/plugin install lmenezes/elasticsearch-kopf/2.1.1
RUN /usr/local/elasticsearch/bin/plugin install mobz/elasticsearch-head
USER root


EXPOSE 9200 9300 9201 9301


USER root

RUN  yum install -y telnet nano
# Kafka manager
COPY docker/kafka-manager-1.3.0.4.zip /usr/local/kafka-manager-1.3.0.4.zip
RUN cd /usr/local; \
    unzip kafka-manager-1.3.0.4.zip; \
    ln -s /usr/local/kafka-manager-1.3.0.4 /usr/local/kafka-manager; \
    rm -f /usr/local/kafka-manager-1.3.0.4.zip
ENV KAFKA_MGR_HOME /usr/local/kafka-manager
ENV ZK_HOSTS sandbox:2181
EXPOSE 9000

# Log Island
COPY docker/log-island-0.9.1.tgz /usr/local/log-island-0.9.1.tgz
RUN cd /usr/local; \
    tar -xzf log-island-0.9.1.tgz; \
    ln -s /usr/local/log-island-0.9.1 /usr/local/log-island; \
    mkdir /usr/local/log-island/log; \
    rm -f /usr/local/log-island-0.9.1.tgz
ENV LOGISLAND_HOME /usr/local/log-island


COPY docker/server.properties $KAFKA_HOME/config/server.properties

# update boot script
COPY docker/bootstrap.sh /etc/bootstrap.sh
RUN chown root.root /etc/bootstrap.sh
RUN chmod 700 /etc/bootstrap.sh

ENTRYPOINT ["/etc/bootstrap.sh"]
