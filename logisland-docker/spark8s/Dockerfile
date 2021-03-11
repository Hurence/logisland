FROM gcr.io/spark-operator/spark:v2.4.0

LABEL maintainer="support@hurence.com"

COPY logisland-docker/spark8s/opencv4/* /usr/local/share/java/opencv4/

ADD logisland-assembly/target/logisland-*-full-*.tar.gz /opt/

RUN cd /opt && ln -s $(eval ls | grep logisland) logisland
ENV LOGISLAND_HOME /opt/logisland
ENV PATH $PATH:$LOGISLAND_HOME/bin
WORKDIR $LOGISLAND_HOME/

COPY logisland-docker/spark8s/log4j* $LOGISLAND_HOME/conf/
