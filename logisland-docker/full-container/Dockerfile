FROM hurence/base
MAINTAINER Hurence


USER root


# Kafka
RUN curl -s http://apache.crihan.fr/dist/kafka/0.10.0.0/kafka_2.11-0.10.0.0.tgz | tar -xz -C /usr/local/
RUN cd /usr/local && ln -s kafka_2.11-0.10.0.0 kafka
ENV KAFKA_HOME /usr/local/kafka
EXPOSE 2181 9092
COPY server.properties $KAFKA_HOME/config/server.properties



# Spark
RUN curl -s http://d3kbcqa49mib13.cloudfront.net/spark-2.1.0-bin-hadoop2.7.tgz | tar -xz -C /usr/local/
RUN cd /usr/local && ln -s spark-2.1.0-bin-hadoop2.7 spark
ENV SPARK_HOME /usr/local/spark
ENV PATH $PATH:$SPARK_HOME/bin
EXPOSE 4040

# Grafana
RUN yum install -y https://grafanarel.s3.amazonaws.com/builds/grafana-3.1.1-1470047149.x86_64.rpm

# Kibana
RUN curl -s https://artifacts.elastic.co/downloads/kibana/kibana-5.4.3-linux-x86_64.tar.gz | tar -xz -C /usr/local/
RUN cd /usr/local && ln -s kibana-5.4.3-linux-x86_64 kibana
ENV KIBANA_HOME /usr/local/kibana
RUN echo 'server.host: 0.0.0.0' >> /usr/local/kibana-5.4.3-linux-x86_64/config/kibana.yml
EXPOSE 5601

# Elasticsearch
RUN curl -s https://artifacts.elastic.co/downloads/elasticsearch/elasticsearch-5.4.3.tar.gz | tar -xz -C /usr/local/
RUN cd /usr/local && ln -s elasticsearch-5.4.3 elasticsearch
ENV ES_HOME /usr/local/elasticsearch
COPY elasticsearch.yml /usr/local/elasticsearch/config/elasticsearch.yml
RUN useradd -ms /bin/bash elastic
RUN chown -R elastic /usr/local/elasticsearch/
RUN mkdir /tmp; chmod 777 /tmp
RUN echo 'elastic  -  nofile  65536' >> /etc/security/limits.conf; echo 'elastic  -  nproc  2048' >> /etc/security/limits.conf;
EXPOSE 9200 9300



# Logisland
COPY logisland-*.tar.gz /usr/local/
RUN ls /usr/local/
RUN cd /usr/local; \
    tar -xzf logisland-*.tar.gz; \
    ln -s /usr/local/logisland-0.11.0-SNAPSHOT /usr/local/logisland; \
    mkdir /usr/local/logisland/log; \
    rm -f /usr/local/*.gz
ENV LOGISLAND_HOME /usr/local/logisland
COPY log4j.properties /usr/local/spark/conf





# Nifi
#RUN curl -s http://www-eu.apache.org/dist/nifi/1.1.1/nifi-1.1.1-bin.tar.gz | tar -xz -C /usr/local/
#RUN cd /usr/local && ln -s nifi-1.1.1 nifi
#ENV NIFI_HOME /usr/local/nifi
#EXPOSE 8080
#EXPOSE 3000




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
