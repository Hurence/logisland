#Build historian gateway

run :
```shell script
mvn clean install
```

require some other logisland module, if it fails try this :

```shell script
mvn -pl :logisland-gateway-historian -am clean install -DskipTests
```

#Run server on local

run :
```shell script
java -jar target/logisland-gateway-historian-1.2.0-fat.jar -conf target/classes/config.json
```

#Run server with docker-compose

in logisland-quickstart project you can run a pipeline that injects data into solr.
You have access to solr UI, a grafana with a predefined historian dashboard.

in root folder of projct **logisland-quickstart** run :

```shell script
docker-compose -f docker-compose.yml -f docker-compose.historian.yml up -d
```

then follow the timeseries tutorial to inject timeseries : https://logisland.github.io/docs/guides/timeseries-guide

Go to grafana at http://localhost:3000. 
* user: admin
* mdp: admin.

Go to the historian dashboard and see your data ! We added three variables so that you can specify
the sampling algorithm to use, the bucket size or filter on a tag. Currently tags are just the name of the metric but we could
imagine tagging several different metric names with a same tag. For exemple 'temp' for metrics 'temp_a' and 'temp_b'.

By default no sampling is used if there is not too many point to draw. Otherwise we calculate the bucket size depending on
the total number of points that is being queried with the average algorithm. At the moment only basic algorithms are available.

#Run server on cluster

TODO
```shell script
java -jar <jar_path> -cluster -conf <conf_path>
```

#RUN TEST
mark the folder ./src/integration-test/java as source test code in your IDE.
mark the folder ./src/integration-test/resources as resources test in your IDE.

Then run :
```shell script
mvn clean install -Pbuild-integration-tests
``` 

to build integration tests source class ! Then you can run the test in your IDE.


#TROUBLESHOOT

When code generator fails. It may be because of hidden char ? This is really irritating.
I fought with javadoc... Sometimes I could not succeed into making it working.