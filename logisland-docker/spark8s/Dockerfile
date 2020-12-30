FROM gcr.io/spark-operator/spark:v2.4.0

LABEL maintainer="support@hurence.com"

COPY ./opencv4/* /usr/local/share/java/opencv4/

COPY classpath_update.sh /tmp/
RUN chmod a+x /tmp/classpath_update.sh

ADD ./logisland-*.tar.gz /opt/

RUN cd /opt && ln -s $(eval ls | grep logisland) logisland
ENV LOGISLAND_HOME /opt/logisland
ENV PATH $PATH:$LOGISLAND_HOME/bin
WORKDIR $LOGISLAND_HOME/

COPY log4j* $LOGISLAND_HOME/conf/

RUN jars="$(/tmp/classpath_update.sh /opt/logisland/lib)" && echo $jars > $LOGISLAND_HOME/logisland_jars.txt

