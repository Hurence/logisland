FROM openjdk:8-jre
MAINTAINER Hurence


USER root

# Elasticsearch
RUN curl -s https://download.elastic.co/elasticsearch/release/org/elasticsearch/distribution/tar/elasticsearch/2.4.5/elasticsearch-2.4.5.tar.gz | tar -xz -C /usr/local/
RUN cd /usr/local && ln -s elasticsearch-2.4.5 elasticsearch
ENV ES_HOME /usr/local/elasticsearch
COPY elasticsearch.yml /usr/local/elasticsearch/config/elasticsearch.yml
RUN useradd -mUs /bin/bash elastic
RUN chown -R elastic:elastic /usr/local/elasticsearch/
EXPOSE 9200 9300

ENTRYPOINT runuser -l elastic -c '/usr/local/elasticsearch/bin/elasticsearch'
