FROM vertx/vertx3

ENV HISTORIAN_HOME /opt/historian/
EXPOSE 8080

COPY ./logisland-gateway-historian-1.3.0-fat.jar $HISTORIAN_HOME
COPY ./config.json $HISTORIAN_HOME/conf/
COPY ./log4j.properties $HISTORIAN_HOME/

WORKDIR $HISTORIAN_HOME

ENTRYPOINT ["java", "-Dlog4j.configuration=file:log4j.properties", "-jar", "./logisland-gateway-historian-1.3.0-fat.jar", "-conf" , "./conf/config.json"]