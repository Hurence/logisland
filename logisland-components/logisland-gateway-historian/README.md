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