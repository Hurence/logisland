Historian docker files
======================

Build your own
--------------

use appropriate version of jar !

Build historian-gateway

```shell script
mvn clean package
```

copy paste jar and conf file

```shell script
cd ./docker
cp ../target/logisland-gateway-historian-1.2.0-fat.jar .
cp ../target/classes/config.json .
```  
  
Building the image, modify version of jar in ENTRYPOINT if needed

```shell script
docker build --rm -t hurence/historian .
docker tag hurence/historian:latest hurence/historian:1.2.0
```

Deploy the image to Docker hub
------------------------------

tag the image as latest

verify image build :

```shell script
docker images
docker tag <IMAGE_ID> latest
```

then login and push the latest image

```shell script
docker login
docker push hurence/historian
````


