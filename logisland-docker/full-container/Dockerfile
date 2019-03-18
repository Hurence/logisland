FROM hurence/spark:2.4.0

LABEL maintainer="support@hurence.com"

VOLUME ["/logisland"]

COPY logisland-assembly/target/logisland-*.tar.gz /tmp/
RUN cd /tmp; \
        tar -xzf logisland-*.tar.gz; \
        rm -f /tmp/*.gz; \
        mv logisland-* /opt
RUN cd /opt && ln -s $(eval ls | grep logisland) logisland
ENV LOGISLAND_HOME /opt/logisland
ENV PATH $PATH:$LOGISLAND_HOME/bin
WORKDIR $LOGISLAND_HOME/
