



Create namespace

.. code-block:: sh

    kubectl create namespace logisland


setup elasticsearch

.. code-block:: sh

    kubectl create -f elasticsearch-deployment.yml
    kubectl create -f elasticsearch-service.yml


.. code-block:: sh

    kubectl get pods --namespace logisland



Configmaps
----------

.. code-block:: sh

    kubectl create -f config-maps.yml
    kubectl describe configmaps special-config --namespace logisland


    Name:         special-config
    Namespace:    logisland
    Labels:       <none>
    Annotations:  <none>

    Data
    ====
    LOGGEN_NUM:
    ----
    0
    LOGGEN_SLEEP:
    ----
    0.2
    Events:  <none>


Use the minikube docker registry instead of your local docker
-------------------------------------------------------------
https://kubernetes.io/docs/tutorials/stateless-application/hello-minikube/#create-a-docker-container-image

Set docker to point to minikube

.. code-block:: sh

    eval $(minikube docker-env)

Push to minikube docker

.. code-block:: sh

    docker build -t hello-node:v1 .

Set your deployment to not pull IfNotPresent
K8S default is set to "Always" Change to "IfNotPresent"

imagePullPolicy: IfNotPresent



Working with Kafka
==================
If you have deployed the kafka-test-client pod from the configuration above, the following commands should get you started with some basic operations:


**Create Topic**

.. code-block:: sh

    kubectl -n logisland exec kafka-test-client -- \
    /usr/bin/kafka-topics --zookeeper kafka-zookeeper:2181 \
    --topic logisland_raw --create --partitions 3 --replication-factor 1

**List Topics**

.. code-block:: sh

    kubectl -n logisland exec kafka-test-client -- \
    /usr/bin/kafka-topics --zookeeper kafka-zookeeper:2181 --list

**Listen on a Topic**

.. code-block:: sh

    kubectl -n logisland exec -ti kafka-test-client -- \
    /usr/bin/kafka-console-consumer --bootstrap-server kafka:9092 \
    --topic logisland_raw --from-beginning


.. code-block:: sh

    kubectl -n logisland exec kafka-test-client -- /usr/bin/kafka-topics --zookeeper kafka-zookeeper-headless:2181 --list

    __confluent.support.metrics
