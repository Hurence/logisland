FROM anapsix/alpine-java:8_jdk_nashorn



RUN apk add --update unzip wget curl docker jq coreutils procps vim
LABEL maintainer="support@hurence.com"

ADD ./logisland-*.tar.gz /tmp/
RUN cd /tmp; \
        mv logisland-* /opt
RUN cd /opt && ln -s $(eval ls | grep logisland) logisland
ENV LOGISLAND_HOME /opt/logisland
ENV PATH $PATH:$LOGISLAND_HOME/bin
WORKDIR $LOGISLAND_HOME/

ENTRYPOINT ["tail", "-f", "bin/logisland.sh"]
