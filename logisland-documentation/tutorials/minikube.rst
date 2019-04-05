=======================================
Run Logisland stream within Kubernetes
=======================================

This is the begining of a multiple part series of blog posts going through setting up an Apache log indexation to Elasticsearch in kubernetes.

Part 1 - Setting up Single Node Elasticsearch
Part 2 - Setting up Kibana Service
Part 3 - Setting up Logisland Service
Part 4 - Kubernetes Configuration Files


Part 1 - Setting up Elasticsearch cluster on Kubernetes
-------------------------------------------------------

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
Let’s start off by creating a single node elasticsearch cluster. According to the elasticsearch documentation, the current version at the time of this writing is 6.1.1. And there are three flavours of docker images. We will just use the basic image which has xpack and free license.

Run the following command to deploy elasticsearch container into our kubernetes environment exposing just the port 9200. There is no way to expose multiple ports using the kubectl command line currently. We will probably revisit this in a later post.

.. code-block:: sh

    kubectl run elasticsearch --image=docker.elastic.co/elasticsearch/elasticsearch:6.2.1 --env="discovery.type=single-node" --port=9200

To list your deployments use:

.. code-block:: sh

    kubectl get deployments

To list all pods and watch the container getting created:

.. code-block:: sh

    kubectl get pods --watch

returns

.. code-block:: sh

    NAME                             READY   STATUS              RESTARTS   AGE
    elasticsearch-57969556b5-xbtjp   0/1     ContainerCreating   0          6s
    kibana-595fbcf599-r5lmk          0/1     ContainerCreating   0          6s
    elasticsearch-57969556b5-xbtjp   1/1     Running             0          2m7s
    kibana-595fbcf599-r5lmk          1/1     Running             0          5m20s

Save the pod name as a variable for use in later commands.

.. code-block:: sh

    export POD_NAME=$(kubectl get pods -o go-template --template '{{range .items}}{{.metadata.name}}{{"\n"}}{{end}}')
    echo Name of the Pod: $POD_NAME

.. note::

    The above command will not work if you have multiple pods.

Now, you can take a look at the logs using the command:

.. code-block:: sh

    kubectl logs $POD_NAME

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


Part 2 - Setup Kibana
---------------------
Let’s try to setup kibana pointing to our elasticsearch single node cluster.

.. code-block:: sh

    kubectl run kibana --image=docker.elastic.co/kibana/kibana:6.2.1 --env="ELASTICSEARCH_URL=http://elasticsearch:9200" --env="XPACK_SECURITY_ENABLED=true" --port=5601

Notice that we have set the ELASTICSEARCH_URL to http://elasticsearch which is the name of our kubernetes pod. And the environment variable XPACK_SECURITY_ENABLED is set to true. When I tried to run without security enabled, kibana was stuck on Optimizing and caching bundles for graph, monitoring, ml, apm, kibana, stateSessionStorageRedirect, timelion, dashboardViewer and statuspage. 🤦🏽‍♂️. You can find more configuration options from their website.

You can see both elasticsearch deployment and kibana with the following command:

.. code-block:: sh

    kubectl get deployments

Outputs:

.. code-block:: sh

    NAME            DESIRED   CURRENT   UP-TO-DATE   AVAILABLE   AGE
    elasticsearch   1         1         1            1           22m
    kibana          1         1         1            0           2m

Pro tip : You can keep watching the progress of your pod creation using the command:

.. code-block:: sh

    kubectl get pods -w -l run=kibana

If the creation of the pod takes too long, might be to do with the network connection. For me, I had to wait 52 minutes for the image to be pulled down.

.. code-block:: sh

    kubectl describe pod kibana-595fbcf599-r5lmk

    Type    Reason     Age   From               Message
    ----    ------     ----  ----               -------
    Normal  Scheduled  51m   default-scheduler  Successfully assigned default/kibana-595fbcf599-r5lmk to minikube
    Normal  Pulling    51m   kubelet, minikube  Pulling image "docker.elastic.co/kibana/kibana:6.2.1"
    Normal  Pulled     45m   kubelet, minikube  Successfully pulled image "docker.elastic.co/kibana/kibana:6.2.1"
    Normal  Created    45m   kubelet, minikube  Created container kibana
    Normal  Started    45m   kubelet, minikube  Started container kibana

Let’s expose the kibana deployment as a service:

.. code-block:: sh

    kubectl expose deployment kibana --type=LoadBalancer

Open kibana using

.. code-block:: sh

    minikube service kibana

Screenshot of kibana dashboard



Part 3 - Setting up a logisland stream  on Kubernetes
-----------------------------------------------------


Zookeeper in a Kube
"""""""""""""""""""
Let’s try to setup zookeeper in order to runner Kafka against it.

.. code-block:: sh

    kubectl run zookeeper --image=wurstmeister/zookeeper --port=2181
    kubectl expose deployment zookeeper --type=LoadBalancer


Kafka in a Kube
"""""""""""""""
Let’s try to setup kafka in order to runner Kafka against it.

.. code-block:: sh

    kubectl run kafka --image=wurstmeister/kafka --port=9092 --env="KAFKA_ADVERTISED_HOST_NAME=192.168.99.100" --env="KAFKA_ADVERTISED_PORT=30092" --env="KAFKA_BROKER_ID=1" --env="KAFKA_ZOOKEEPER_CONNECT=zookeeper:2181"  --env="KAFKA_CREATE_TOPICS=test-topic:1:1"

    kubectl expose deployment zookeeper --type=LoadBalancer





Part 4 - Configuration File
---------------------------
Now that we have a single node elasticsearch service, kibana to monitor the cluster and Logisland to feed logs in, lets try to capture the work so far into kubernetes configuration file.

Kubernetes Configuration File
"""""""""""""""""""""""""""""
Our current elasticsearch deployment configuration looks like:

.. code-block:: yml

    apiVersion: apps/v1beta2
    kind: Deployment
    metadata:
      name: elasticsearch
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
              ports:
                - containerPort: 9200
                  name: http
                  protocol: TCP

Our elasticsearch service configuration looks like:

.. code-block:: yml

    apiVersion: v1
    kind: Service
    metadata:
      name: elasticsearch
      labels:
        component: elasticsearch
    spec:
      type: LoadBalancer
      selector:
        component: elasticsearch
      ports:
        - name: http
          port: 9200
          protocol: TCP

Let’s save these two configurations into a file called elasticsearch_deployment.yaml and elasticsearch_service.yaml respectively into a folder called elasticsearch-k8s.

.. code-block:: sh

    elasticsearch-k8s
    ├── elasticsearch_deployment.yaml
    └── elasticsearch_service.yaml

Let’s delete the elasticsearch deployment and service that was created before:

.. code-block:: sh

    kubectl delete deployment elasticsearch
    kubectl delete service elasticsearch

To create the resources from the configuration files, run the command from the elasticsearch-k8s folder:

.. code-block:: sh

    kubectl create -f .

Outputs:

.. code-block:: sh

    deployment "elasticsearch" created
    service "elasticsearch" created

Let’s do the same for kibana.

Kibana deployment:

.. code-block:: yml

    apiVersion: apps/v1beta2
    kind: Deployment
    metadata:
      name: kibana
    spec:
      selector:
        matchLabels:
          run: kibana
      template:
        metadata:
          labels:
            run: kibana
        spec:
          containers:
            - name: kibana
              image: docker.elastic.co/kibana/kibana:6.2.1
              env:
                - name: ELASTICSEARCH_URL
                  value: http://elasticsearch:9200
                - name: XPACK_SECURITY_ENABLED
                  value: true
              ports:
                - containerPort: 5601
                  name: http
                  protocol: TCP

Kibana service:

.. code-block:: yml

    apiVersion: v1
    kind: Service
    metadata:
      name: kibana
      labels:
        run: kibana
    spec:
      type: LoadBalancer
      selector:
        run: kibana
      ports:
        - name: http
          port: 5601
          protocol: TCP


zookeeper-deployment.yml


.. code-block:: yml

    apiVersion: extensions/v1beta1
    kind: Deployment
    metadata:
      labels:
        app: zookeeper
      name: zookeeper
    spec:
      replicas: 1
      template:
        metadata:
          labels:
            app: zookeeper
        spec:
          containers:
          - image: wurstmeister/zookeeper
            imagePullPolicy: IfNotPresent
            name: zookeeper
            ports:
            - containerPort: 2181

zookeeper-service.yml

.. code-block:: yml

    apiVersion: v1
    kind: Service
    metadata:
      labels:
        app: zookeeper-service
      name: zookeeper-service
    spec:
      type: NodePort
      ports:
      - name: zookeeper-port
        port: 2181
        nodePort: 30181
        targetPort: 2181
      selector:
        app: zookeeper

Now you can easily delete and recreate these resource using the commands `kubectl delete -f .` and `kubectl create -f` . respectively.

