FROM hurence/spark:2.3.3

LABEL maintainer="support@hurence.com"

COPY ./opencv4/* /usr/local/share/java/opencv4/

COPY ./logisland-*.tar.gz /tmp/
RUN cd /tmp; \
        tar -xzf logisland-*.tar.gz; \
        rm -f /tmp/*.gz; \
        mv logisland-* /opt
RUN cd /opt && ln -s $(eval ls | grep logisland) logisland
ENV LOGISLAND_HOME /opt/logisland
ENV PATH $PATH:$LOGISLAND_HOME/bin
WORKDIR $LOGISLAND_HOME/

# If you set this entry point integration test can not execute properly (never ending...)
#ENTRYPOINT ["tail", "-f", "bin/logisland.sh"]