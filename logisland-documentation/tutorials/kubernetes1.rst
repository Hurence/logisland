================================================
Run Logisland stream within Kubernetes : stage 1
================================================
This is the begining of a multiple part series of blog posts going through setting up an Apache log indexation to Elasticsearch in kubernetes.
This guide will bring you to a fully functionnal Kubernetes logisland setup.

Part 1 - Setting up Elasticsearch
Part 2 - Setting up Kibana
Part 3 - Setting up Zookeeper
Part 4 - Setting up Kafka
Part 5 - Setting up Logisland


This guide set up an elasticsearch/kibana stack, a three-node Kafka cluster and a three-node Zookeeper cluster required by Kafka.
And a logisland instance
Kafka and Zookeeper can be manually scaled up at any time by altering and re-applying configuration.
Kubernetes also provides features for autoscaling, read more about auto scaling Kubernetes Pods should that be a requirement.


0 - Config maps & persistent volumes
------------------------------------

Namespace
"""""""""
In this guide, I use the fictional namespace `logisland`. You can create this namespace in your cluster or use your own.

Create the file `namespace.yml`:

.. code-block:: yml

    apiVersion: v1
    kind: Namespace
    metadata:
      name: logisland

Apply the configuration:

.. code-block:: sh

    kubectl create -f ./namespace.yml

If you wish to use your own namespace for this Kafka installation, be sure to replace `logisland` in the configurations below.

Persistent volumes
""""""""""""""""""
In Kubernetes, managing storage is a distinct problem from managing compute. The PersistentVolume subsystem provides an API for users and administrators that abstracts details of how storage is provided from how it is consumed. To do this we introduce two new API resources: PersistentVolume and PersistentVolumeClaim.

A PersistentVolume (PV) is a piece of storage in the cluster that has been provisioned by an administrator. It is a resource in the cluster just like a node is a cluster resource. PVs are volume plugins like Volumes, but have a lifecycle independent of any individual pod that uses the PV. This API object captures the details of the implementation of the storage, be that NFS, iSCSI, or a cloud-provider-specific storage system.

A PersistentVolumeClaim (PVC) is a request for storage by a user. It is similar to a pod. Pods consume node resources and PVCs consume PV resources. Pods can request specific levels of resources (CPU and Memory). Claims can request specific size and access modes (e.g., can be mounted once read/write or many times read-only).

Create th local folders where you want to store your files (change this to wherever you want to store dat on your nodes) :

.. code-block:: sh

    mkdir /tmp/data

Create the file `pv-volume.yml`

.. code-block:: yml

    kind: PersistentVolume
    apiVersion: v1
    metadata:
      name: datadir
      labels:
        app: kafka
        type: local
      namespace: logisland
    spec:
      storageClassName: manual
      capacity:
        storage: 10Gi
      accessModes:
        - ReadWriteOnce
      hostPath:
        path: "/tmp/data"

Apply the configuration:

.. code-block:: sh

    kubectl create -f ./pv-volume.yml

1 - Setting up Elasticsearch cluster on Kubernetes
--------------------------------------------------

The main aim of this series of blog posts will be make notes for myself as I try to learn kubernetes and for anyone in the same position.

In this blog post, I will just concentrate on useful Kubernetes getting started resources, commands, and also with an aim of creating a single node Elasticsearch cluster.

Getting Started
"""""""""""""""
I found the most helpful resource for me was the Kubernetes official website for starting to learn kubernetes. Head over to the Interactive Tutorials section of the website and spend time going through all 6 modules to cover the basics. It should only take you 1 - 2 hours. You won’t have to install anything to try it out.

The next step I took was followed the Hello Minikube tutorial. This helped me to get minikube and kubectl commands installed. (Minikube is the local development Kubernetes environment and kubectl is the command line interface used to interact with Kubernetest cluster).

Shaving the Yak!
""""""""""""""""
One or two commands that used in this post will be mac specific. Reference this guide to get more up to date and OS specific commands. Once you’ve got the tools all installed, you can now follow along these steps to create a single node Elasticsearch cluster. If you are using Minikube, make sure that its started properly by running this command (for mac):

.. code-block:: sh

    minikube start --vm-driver=hyperkit

Now set the Minikube context. The context is what determines which cluster kubectl is interacting with.

.. code-block:: sh

    kubectl config use-context minikube

Verify that kubectl is configured to communicate with your cluster:

.. code-block:: sh

    kubectl cluster-info

To view the nodes in the cluster, run

.. code-block:: sh

    kubectl get nodes

Kubernetes Dashboard
""""""""""""""""""""
Minikube includes the kubernetes dashboard as an addon which you can enable.

.. code-block:: sh

    minikube addons list

returns

.. code-block:: sh

    - default-storageclass: enabled
    - coredns: disabled
    - kube-dns: enabled
    - ingress: disabled
    - registry: disabled
    - registry-creds: disabled
    - addon-manager: enabled
    - dashboard: enabled
    - storage-provisioner: enabled
    - heapster: disabled
    - efk: disabled

You can enable an addon using:

.. code-block:: sh

    minikube addons enable dashboard

You can then open the dashboard with command

.. code-block:: sh

    minikube addons open dashboard

Single Node Elasticsearch Cluster
"""""""""""""""""""""""""""""""""
Create the file `elasticsearch-service.yml`:

.. code-block:: yml

    apiVersion: v1
    kind: Service
    metadata:
      name: elasticsearch
      namespace: logisland
      labels:
        component: elasticsearch
    spec:
      type: ClusterIP
      selector:
        component: elasticsearch
      ports:
        - name: http
          port: 9200
          protocol: TCP

Apply the configuration:

.. code-block:: sh

    kubectl create -f ./elasticsearch-service.yml


Create the file `elasticsearch-deployment.yml`:

.. code-block:: yml

    apiVersion: apps/v1beta2
    kind: Deployment
    metadata:
      name: elasticsearch
      namespace: logisland
    spec:
      selector:
        matchLabels:
          component: elasticsearch
      template:
        metadata:
          labels:
            component: elasticsearch
        spec:
          containers:
            - name: elasticsearch
              image: docker.elastic.co/elasticsearch/elasticsearch:6.2.1
              env:
                - name: discovery.type
                  value: single-node
                - name: ES_HEAP_SIZE
                  value: "256m"
              ports:
                - containerPort: 9200
                  name: http
                  protocol: TCP


Apply the configuration:

.. code-block:: sh

    kubectl create -f ./elasticsearch-deployment.yml

Expose the cluster
""""""""""""""""""
We can verify that the cluster is running by looking at the logs. But, let’s check if elasticsearch api is responding first.

In a seperate shell window, excute the following to start a proxy into Kubernetest cluster.

.. code-block:: sh

    kubectl proxy

Outputs:

.. code-block:: sh

Starting to serve on 127.0.0.1:8001
Now, back in the other window, lets execute a curl command to get the response from the pod via the proxy.

.. code-block:: sh

    curl http://localhost:8001/api/v1/namespaces/default/pods/$POD_NAME/proxy/

Outputs:

.. code-block:: json

    {
      "name" : "DdWnre5",
      "cluster_name" : "docker-cluster",
      "cluster_uuid" : "P2xSeKPeTTSnBSpNyiZQtA",
      "version" : {
        "number" : "6.2.1",
        "build_hash" : "7299dc3",
        "build_date" : "2018-02-07T19:34:26.990113Z",
        "build_snapshot" : false,
        "lucene_version" : "7.2.1",
        "minimum_wire_compatibility_version" : "5.6.0",
        "minimum_index_compatibility_version" : "5.0.0"
      },
      "tagline" : "You Know, for Search"
    }

Great, everything is working.

Now, lets expose this deployment to outside of Kubernetes network:

.. code-block:: sh

    kubectl expose deployment elasticsearch --type=LoadBalancer

Pro tip Use MiniKube to open the service in your default browser.

.. code-block:: sh

    minikube service elasticsearch

In my case, the port that was assigned to this pod was 31389. But, we have elasticsearch cluster now running in Kubernetes!


2 - Setup Kibana
----------------
Let’s try to setup kibana pointing to our elasticsearch single node cluster.

Create the file `kibana-service.yml`:

.. code-block:: yml

    apiVersion: v1
    kind: Service
    metadata:
      name: kibana
      namespace: logisland
      labels:
        component: kibana
    spec:
      type: ClusterIP
      selector:
        run: kibana
      ports:
        - name: http
          port: 5601
          protocol: TCP

Apply the configuration:

.. code-block:: sh

    kubectl create -f ./kibana-service.yml


Create the file `kibana-deployment.yml`:

.. code-block:: yml

    apiVersion: apps/v1
    kind: Deployment
    metadata:
      name: kibana
      namespace: logisland
    spec:
      selector:
        matchLabels:
          component: kibana
      template:
        metadata:
          labels:
            component: kibana
        spec:
          containers:
            - name: kibana
              image: docker.elastic.co/kibana/kibana:6.2.1
              env:
                - name: ELASTICSEARCH_URL
                  value: http://elasticsearch:9200
                - name: XPACK_SECURITY_ENABLED
                  value: "true"
              ports:
                - containerPort: 5601
                  name: http
                  protocol: TCP

Apply the configuration:

.. code-block:: sh

    kubectl create -f ./kibana-deployment.yml

Screenshot of kibana dashboard



3 - Setting up Zookeeper
------------------------
Kafka requires Zookeeper for maintaining configuration information, naming, providing distributed synchronization, and providing group services to coordinate its nodes.

Zookeeper Headless Service
""""""""""""""""""""""""""
A Kubernetes Headless Service does not resolve to a single IP; instead, Headless Services returns the IP addresses of any Pods found by their selector, in this case, Pods labeled app: kafka-zookeeper.

Once Pods labeled app: kafka-zookeeper are running, this Headless Service returns the results of an in-cluster DNS lookup similar to the following:

.. code-block:: sh

    # nslookup kafka-zookeeper
    Server:        10.96.0.10
    Address:    10.96.0.10#53

    Name:    kafka-zookeeper-headless.the-project.svc.cluster.local
    Address: 192.168.108.150
    Name:    kafka-zookeeper-headless.the-project.svc.cluster.local
    Address: 192.168.108.181
    Name:    kafka-zookeeper-headless.the-project.svc.cluster.local
    Address: 192.168.108.132

In the example above, the Kubernetes Service kafka-zookeeper-headless returned the internal IP addresses of three individual Pods.

At this point, no Pod IPs can be returned until the Pods are configured in the StatefulSet further down.

Create the file `zookeeper-service-headless.yml`:

.. code-block:: yml

    apiVersion: v1
    kind: Service
    metadata:
      name: kafka-zookeeper
      namespace: the-project
    spec:
      clusterIP: None
      ports:
      - name: client
        port: 2181
        protocol: TCP
        targetPort: 2181
      - name: election
        port: 3888
        protocol: TCP
        targetPort: 3888
      - name: server
        port: 2888
        protocol: TCP
        targetPort: 2888
      selector:
        app: kafka-zookeeper
      sessionAffinity: None
      type: ClusterIP

Apply the configuration:

.. code-block:: sh

    kubectl create -f ./zookeeper-service-headless.yml

Zookeeper StatefulSet
"""""""""""""""""""""
Kubernetes StatefulSets offer stable and unique network identifiers, persistent storage, ordered deployments, scaling, deletion, termination, and automated rolling updates.

Unique network identifiers and persistent storage are essential for stateful cluster nodes in systems like Zookeeper and Kafka. While it seems strange to have a coordinator like Zookeeper running inside a Kubernetes cluster sitting on its own coordinator Etcd, it makes sense since these systems are built to run independently. Kubernettes supports running services like Zookeeper and Kafka with features like headless services and stateful sets which demonstrates the flexibility of Kubernetes as both a microservices platform and a type of virtual infrastructure.

The following configuration creates three kafka-zookeeper Pods, kafka-zookeeper-0, kafka-zookeeper-1, kafka-zookeeper-2 and can be scaled to as many as desired. Ensure that the number of specified replicas matches the environment variable ZK_REPLICAS specified in the container spec.

Pods in this StatefulSet run the Zookeeper Docker image gcr.io/google_samples/k8szk:v3, which is a sample image provided by Google for testing GKE, it is recommended to use custom and maintained Zookeeper image once you are familiar with this setup.

Create the file `zookeeper-statefulset.yml`:

.. code-block:: yml

    apiVersion: apps/v1
    kind: StatefulSet
    metadata:
      name: kafka-zookeeper
      namespace: the-project
    spec:
      podManagementPolicy: OrderedReady
      replicas: 3
      revisionHistoryLimit: 1
      selector:
        matchLabels:
          app: kafka-zookeeper
      serviceName: kafka-zookeeper-headless
      template:
        metadata:
          labels:
            app: kafka-zookeeper
        spec:
          containers:
          - command:
            - /bin/bash
            - -xec
            - zkGenConfig.sh && exec zkServer.sh start-foreground
            env:
            - name: ZK_REPLICAS
              value: "3"
            - name: JMXAUTH
              value: "false"
            - name: JMXDISABLE
              value: "false"
            - name: JMXPORT
              value: "1099"
            - name: JMXSSL
              value: "false"
            - name: ZK_CLIENT_PORT
              value: "2181"
            - name: ZK_ELECTION_PORT
              value: "3888"
            - name: ZK_HEAP_SIZE
              value: 1G
            - name: ZK_INIT_LIMIT
              value: "5"
            - name: ZK_LOG_LEVEL
              value: INFO
            - name: ZK_MAX_CLIENT_CNXNS
              value: "60"
            - name: ZK_MAX_SESSION_TIMEOUT
              value: "40000"
            - name: ZK_MIN_SESSION_TIMEOUT
              value: "4000"
            - name: ZK_PURGE_INTERVAL
              value: "0"
            - name: ZK_SERVER_PORT
              value: "2888"
            - name: ZK_SNAP_RETAIN_COUNT
              value: "3"
            - name: ZK_SYNC_LIMIT
              value: "10"
            - name: ZK_TICK_TIME
              value: "2000"
            image: gcr.io/google_samples/k8szk:v3
            imagePullPolicy: IfNotPresent
            livenessProbe:
              exec:
                command:
                - zkOk.sh
              failureThreshold: 3
              initialDelaySeconds: 20
              periodSeconds: 10
              successThreshold: 1
              timeoutSeconds: 1
            name: zookeeper
            ports:
            - containerPort: 2181
              name: client
              protocol: TCP
            - containerPort: 3888
              name: election
              protocol: TCP
            - containerPort: 2888
              name: server
              protocol: TCP
            readinessProbe:
              exec:
                command:
                - zkOk.sh
              failureThreshold: 3
              initialDelaySeconds: 20
              periodSeconds: 10
              successThreshold: 1
              timeoutSeconds: 1
            resources: {}
            terminationMessagePath: /dev/termination-log
            terminationMessagePolicy: File
            volumeMounts:
            - mountPath: /var/lib/zookeeper
              name: data
          dnsPolicy: ClusterFirst
          restartPolicy: Always
          schedulerName: default-scheduler
          securityContext:
            fsGroup: 1000
            runAsUser: 1000
          terminationGracePeriodSeconds: 30
          volumes:
          - emptyDir: {}
            name: data
      updateStrategy:
        type: OnDelete

Apply the configuration:

.. code-block:: sh

    kubectl create -f ./zookeeper-statefulset.yml

Zookeeper PodDisruptionBudget
"""""""""""""""""""""""""""""
PodDisruptionBudget can help keep the Zookeeper service stable during Kubernetes administrative events such as draining a node or updating Pods.

From the official documentation for PDB (PodDisruptionBudget):

A PDB specifies the number of replicas that an application can tolerate having, relative to how many it is intended to have. For example, a Deployment which has a .spec.replicas: 5 is supposed to have 5 pods at any given time. If its PDB allows for there to be 4 at a time, then the Eviction API will allow voluntary disruption of one, but not two pods, at a time.

The configuration below tells Kubernetes that we can only tolerate one of our Zookeeper Pods down at any given time. maxUnavailable may be set to a higher number if we increase the number of Zookeeper Pods in the StatefulSet.

Create the file `zookeeper-disruptionbudget.yml`:

.. code-block:: yml

    apiVersion: policy/v1beta1
    kind: PodDisruptionBudget
    metadata:
      labels:
        app: kafka-zookeeper
      name: kafka-zookeeper
      namespace: the-project
    spec:
      maxUnavailable: 1
      selector:
        matchLabels:
          app: kafka-zookeeper

Apply the configuration:

.. code-block:: sh

    kubectl create -f ./zookeeper-disruptionbudget.yml


4 - Setting up Kafka
--------------------
Once Zookeeper is up and running we have satisfied the requirements for Kafka. Kafka is set up in a similar configuration to Zookeeper, utilizing a Service, Headless Service and a StatefulSet.

Kafka Service
"""""""""""""
The following Service provides a persistent internal Cluster IP address that proxies and load balance requests to Kafka Pods found with the label app: kafka and exposing the port 9092.

Create the file `kafka-service.yml`:

    apiVersion: v1
    kind: Service
    metadata:
      name: kafka
      namespace: the-project
    spec:
      ports:
      - name: broker
        port: 9092
        protocol: TCP
        targetPort: kafka
      selector:
        app: kafka
      sessionAffinity: None
      type: ClusterIP

Apply the configuration:

.. code-block:: sh

    kubectl create -f ./kafka-service.yml

Kafka Headless Service
""""""""""""""""""""""
The following Headless Service provides a list of Pods and their internal IPs found with the label app: kafka and exposing the port 9092. The previously created Service: kafka always returns a persistent IP assigned at the creation time of the Service. The following kafka-headless services return the domain names and IP address of individual Pods and are liable to change as Pods are added, removed or updated.

Create the file `kafka-service-headless.yml`:

    apiVersion: v1
    kind: Service
    metadata:
      name: kafka-headless
      namespace: the-project
    spec:
      clusterIP: None
      ports:
      - name: broker
        port: 9092
        protocol: TCP
        targetPort: 9092
      selector:
        app: kafka
      sessionAffinity: None
      type: ClusterIP

Apply the configuration:

.. code-block:: sh

    kubectl create -f ./kafka-service-headless.yml

Kafka StatefulSet
"""""""""""""""""
The following StatefulSet deploys Pods running the confluentinc/cp-kafka:4.1.2-2 Docker image from Confluent.

Each pod is assigned 1Gi of storage using the rook-block storage class. See Rook.io for more information on file, block, and object storage services for cloud-native environments.

Create the file `kafka-statefulset.yml`:

    apiVersion: apps/v1
    kind: StatefulSet
    metadata:
      labels:
        app: kafka
      name: kafka
      namespace: the-project
    spec:
      podManagementPolicy: OrderedReady
      replicas: 3
      revisionHistoryLimit: 1
      selector:
        matchLabels:
          app: kafka
      serviceName: kafka-headless
      template:
        metadata:
          labels:
            app: kafka
        spec:
          containers:
          - command:
            - sh
            - -exc
            - |
              unset KAFKA_PORT && \
              export KAFKA_BROKER_ID=${HOSTNAME##*-} && \
              export KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://${POD_IP}:9092 && \
              exec /etc/confluent/docker/run
            env:
            - name: POD_IP
              valueFrom:
                fieldRef:
                  apiVersion: v1
                  fieldPath: status.podIP
            - name: KAFKA_HEAP_OPTS
              value: -Xmx1G -Xms1G
            - name: KAFKA_ZOOKEEPER_CONNECT
              value: kafka-zookeeper:2181
            - name: KAFKA_LOG_DIRS
              value: /opt/kafka/data/logs
            - name: KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR
              value: "3"
            - name: KAFKA_JMX_PORT
              value: "5555"
            image: confluentinc/cp-kafka:4.1.2-2
            imagePullPolicy: IfNotPresent
            livenessProbe:
              exec:
                command:
                - sh
                - -ec
                - /usr/bin/jps | /bin/grep -q SupportedKafka
              failureThreshold: 3
              initialDelaySeconds: 30
              periodSeconds: 10
              successThreshold: 1
              timeoutSeconds: 5
            name: kafka-broker
            ports:
            - containerPort: 9092
              name: kafka
              protocol: TCP
            readinessProbe:
              failureThreshold: 3
              initialDelaySeconds: 30
              periodSeconds: 10
              successThreshold: 1
              tcpSocket:
                port: kafka
              timeoutSeconds: 5
            resources: {}
            terminationMessagePath: /dev/termination-log
            terminationMessagePolicy: File
            volumeMounts:
            - mountPath: /opt/kafka/data
              name: datadir
          dnsPolicy: ClusterFirst
          restartPolicy: Always
          schedulerName: default-scheduler
          securityContext: {}
          terminationGracePeriodSeconds: 60
      updateStrategy:
        type: OnDelete
      volumeClaimTemplates:
      - metadata:
          name: datadir
        spec:
          accessModes:
          - ReadWriteOnce
          resources:
            requests:
              storage: 1Gi
          storageClassName: rook-block

Apply the configuration:

.. code-block:: sh

    kubectl create -f ./kafka-statefulset.yml

Kafka Test Pod
""""""""""""""
Add a test Pod to help explore and debug your new Kafka cluster. The Confluent Docker image confluentinc/cp-kafka:4.1.2-2 used for the test Pod is the same as our nodes from the StatefulSet and contain useful command in the /usr/bin/ folder.

Create the file 400-pod-test.yml:

    apiVersion: v1
    kind: Pod
    metadata:
      name: kafka-test-client
      namespace: the-project
    spec:
      containers:
      - command:
        - sh
        - -c
        - exec tail -f /dev/null
        image: confluentinc/cp-kafka:4.1.2-2
        imagePullPolicy: IfNotPresent
        name: kafka
        resources: {}
        terminationMessagePath: /dev/termination-log
        terminationMessagePolicy: File

Apply the configuration:

.. code-block:: sh

    kubectl create -f ./pod-test.yml

5 - Working with Kafka
----------------------
If you have deployed the kafka-test-client pod from the configuration above, the following commands should get you started with some basic operations:

Create Topic
""""""""""""
.. code-block:: sh

    kubectl -n the-project exec kafka-test-client -- \
    /usr/bin/kafka-topics --zookeeper kafka-zookeeper:2181 \
    --topic logisland_raw --create --partitions 3 --replication-factor 1

List Topics
"""""""""""
.. code-block:: sh

    kubectl -n the-project exec kafka-test-client -- \
/usr/bin/kafka-topics --zookeeper kafka-zookeeper:2181 --list

Sending logs to Kafka
"""""""""""""""""""""
This script generates a boatload of fake apache logs very quickly.
Its useful for generating fake workloads for data ingest and/or analytics applications.
It can write log lines to console, to log files or directly to gzip files. Or to kafka ...
It utilizes the excellent Faker library to generate realistic ip's, URI's etc.

Create the file `loggen-deployment.yml`:

    apiVersion: v1
    kind: Pod
    metadata:
      name: loggen-job
      namespace: logisland
    spec:
      containers:
        - name: loggen
          image: hurence/loggen
          imagePullPolicy: IfNotPresent
          env:
            - name: LOGGEN_SLEEP
              valueFrom:
                configMapKeyRef:
                  name: special-config
                  key: loggen.sleep
            - name: LOGGEN_NUM
              valueFrom:
                configMapKeyRef:
                  name: special-config
                  key: loggen.num
            - name: LOGGEN_KAFKA
              valueFrom:
                configMapKeyRef:
                  name: logisland-config
                  key: kafka.brokers
            - name: LOGGEN_KAFKA_TOPIC
              valueFrom:
                configMapKeyRef:
                  name: special-config
                  key: loggen.topic

Apply the configuration:

.. code-block:: sh

    kubectl create -f ./loggen-deployment.yml


Listen on a Topic
"""""""""""""""""
make sure some fake apache logs are flowing through kafka topic

.. code-block:: sh

    kubectl -n the-project exec -ti kafka-test-client -- \
    /usr/bin/kafka-console-consumer --bootstrap-server kafka:9092 \
    --topic logisland_raw --from-beginning

6 - Setup logisland
-------------------
Create the file `logisland-deployment.yml`:

    apiVersion: v1
    kind: Pod
    metadata:
      name: logisland-job
      namespace: logisland
    spec:
      containers:
        - name: loggen
          image: hurence/logisland
          env:
            - name: SPECIAL_LEVEL_KEY
              valueFrom:
                configMapKeyRef:
                  name: special-config
                  key: special.how
            - name: LOG_LEVEL
              valueFrom:
                configMapKeyRef:
                  name: env-config
                  key: log_level

.. code-block:: sh

    kubectl create -f ./logisland-deployment.yml