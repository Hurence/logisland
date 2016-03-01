---
layout: page
title: Build
permalink: /build/
---



### Building Docker image



```sh
# build log-island
git clone https://github.com/Hurence/log-island.git
cd log-island
sbt universal:packageZipTarball 
cp target/universal/log-island-*.tgz docker/


# build kafka-manager
git clone https://github.com/yahoo/kafka-manager.git
cd kafka-manager
sbt clean dist
```

The archive is generated under dist directory, 
you have to copy this file into your Dockerfile directory you can now issue

```
docker build --rm -t hurence/log-island:0.9.1 .
```