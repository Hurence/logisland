---
layout: page
title: Deployement
permalink: /deploy/
---

How to deploy Log-Island ?

Basicaly you have 3 options

### 1. Single node Docker container
The easy way : you start a small Docker container with all you need inside (Elasticsearch, Kibana, Kafka, Spark, LogIsland + some usefull tools)

[Docker](https://www.docker.com) is becoming an unavoidable tool to isolate a complex service component. It's easy to manage, deploy and maintain. That's why you can start right away to play with LogIsland through the Docker image provided from [Docker HUB](https://hub.docker.com/r/hurence/log-island/)

```sh

# Get the LogIsland image
docker pull hurence/log-island:latest

# Run the container
docker run \
      -it \
      -p 80:80 \
      -p 9200-9300:9200-9300 \
      -p 5601:5601 \
      -p 2181:2181 \
      -p 9092:9092 \
      -p 9000:9000 \
      -p 4050-4060:4050-4060 \
      --name log-island \
      -h sandbox \
      hurence/log-island:0.9.1 bash

# Connect a shell to your LogIsland container
docker exec -ti log-island bash

# Start you parsers and processors
...

```


### 2. Production mode in an Hadoop cluster
When it comes to scale, you'll need a cluster. Log-Island is just a framework that runs sparks jobs over Kafka topics so if you already have a cluster you just have to get the latest log-island binaries and unzip them to a edge node of your hadoop cluster.

For now Log-Island is compatible with HDP 2.3.2.


### 3. Elastic scale with Mesos/Marathon
// TO BE DONE