---
layout: page
title: Build
permalink: /build/
---

In the following section you'll find all the commands needed to build the source code and the Docker image.

### Build source code
Log-Island is written in Java and Scala, we then use [Scala Build Tool](http://www.scala-sbt.org) to build it

```sh
# to build only the source code
sbt package

# to build all dependencies into one single jar
sbt assemblyPackageDependency

# to publish jar to local ivy cache to develop your own plugins
sbt publishLocal
    
# to build API documentation
sbt doc

# to build user documentation
cd docs
jekyll build
```
    
### Build Docker image
The build the docker image, build log-island.tgz and kafka-manager tool

```sh
# build a tgz archive with full standalone dependencies
sbt universal:packageZipTarball 
cp target/universal/log-island-*.tgz docker/
    
# build kafka-manager
git clone https://github.com/yahoo/kafka-manager.git
cd kafka-manager
sbt clean dist
    
# build docker
docker build --rm -t hurence/log-island:0.9.1 -f docker/Dockerfile .
```