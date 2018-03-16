FROM hurence/kafka-spark:latest
MAINTAINER support@hurence.com

USER root

# Logisland
COPY logisland-*.tar.gz /usr/local/
RUN cd /usr/local; \
    tar -xzf logisland-*.tar.gz; \
    ln -s /usr/local/logisland-0.12.2 /usr/local/logisland; \
    mkdir /usr/local/logisland/log; \
    rm -f /usr/local/*.gz
ENV LOGISLAND_HOME /usr/local/logisland
RUN mv /usr/local/logisland/conf/log4j.properties /usr/local/spark/conf


# update boot script
COPY bootstrap.sh /etc/bootstrap.sh
RUN chown root:root /etc/bootstrap.sh
RUN chmod 700 /etc/bootstrap.sh

ENTRYPOINT ["/etc/bootstrap.sh"]
