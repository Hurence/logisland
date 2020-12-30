FROM hurence/spark:2.4.0

LABEL maintainer="support@hurence.com"

COPY ./opencv4/* /usr/local/share/java/opencv4/

ADD ./logisland-*.tar.gz /tmp/
RUN cd /tmp; \
        mv logisland-* /opt
RUN cd /opt && ln -s $(eval ls | grep logisland) logisland
ENV LOGISLAND_HOME /opt/logisland
ENV PATH $PATH:$LOGISLAND_HOME/bin
WORKDIR $LOGISLAND_HOME/

# If you set this entry point integration test can not execute properly (never ending...)
#ENTRYPOINT ["tail", "-f", "bin/logisland.sh"]
