# Fake Apache Log Generator

This script generates a boatload of fake apache logs very quickly. Its useful for generating fake workloads for data ingest and/or analytics applications.

It can write log lines to console, to log files or directly to gzip files.

It utilizes the excellent Faker library to generate realistic ip's, URI's etc.

Basic Usage
-----------
Generate a single log line to STDOUT

    $ python apache-fake-log-gen.py  
    
Generate 100 log lines into a .log file

    $ python apache-fake-log-gen.py -n 100 -o LOG 

Generate 100 log lines into a .gz file at intervals of 10 seconds

    $ python apache-fake-log-gen.py -n 100 -o GZ -s 10

Infinite log file generation (useful for testing File Tail Readers)

    $ python apache-fake-log-gen.py -n 0 -o LOG 

Prefix the output filename

    $ python apache-fake-log-gen.py -n 100 -o LOG -p WEB1

Detailed help

    $ python apache-fake-log-gen.py -h
    usage: apache-fake-log-gen.py [-h] [--output {LOG,GZ,CONSOLE}]
                              [--num NUM_LINES] [--prefix FILE_PREFIX]
                              [--sleep SLEEP]

    Fake Apache Log Generator
    
    optional arguments:
      -h, --help            show this help message and exit
      --output {LOG,GZ,CONSOLE}, -o {LOG,GZ,CONSOLE}
                            Write to a Log file, a gzip file or to STDOUT
      --num NUM_LINES, -n NUM_LINES
                            Number of lines to generate (0 for infinite)
      --prefix FILE_PREFIX, -p FILE_PREFIX
                            Prefix the output file name
      --sleep SLEEP, -s SLEEP
                            Sleep this long between lines (in seconds)


Build the docker container
--------------------------
Build

    $ docker build --rm -t hurence/loggen .

Run

    $ docker run -it --name loggen  hurence/loggen


Run in kubernetes
-----------------
Create a Pod with the following config `loggen-pod.yml` file

    apiVersion: v1
    kind: ConfigMap
    metadata:
      name: special-config
      namespace: logisland
    data:
      loggen.sleep: '0.2'
      loggen.num: '0'
      loggen.topic: logisland_raw
    ---
    apiVersion: v1
    kind: ConfigMap
    metadata:
      name: logisland-config
      namespace: logisland
    data:
      kafka.brokers: kafka:9092
    ---
    apiVersion: v1
    kind: Pod
    metadata:
      name: loggen-pod
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
                  
deploy the Pod with

    kubectl create -f loggen-pod.yml

The Base Alpine Linux Image
---------------------------
Alpine Linux is a tiny Linux distribution designed for power users who appreciate security, simplicity and resource efficiency.

As claimed by Alpine:

Small. Simple. Secure. Alpine Linux is a security-oriented, lightweight Linux distribution based on musl libc and busybox.
The Alpine image is surprisingly tiny with a size of no more than 8MB for containers. With minimal packages installed to reduce the attack surface on the underlying container. This makes Alpine an image of choice for our data science container.

Python Data Science Packages
----------------------------
Our Python data science container makes use of the following super cool python packages:

- **NumPy** : NumPy or Numeric Python supports large, multi-dimensional arrays and matrices. It provides fast precompiled functions for mathematical and numerical routines. In addition, NumPy optimizes Python programming with powerful data structures for efficient computation of multi-dimensional arrays and matrices.
- **SciPy**: SciPy provides useful functions for regression, minimization, Fourier-transformation, and many more. Based on NumPy, SciPy extends its capabilities. SciPy’s main data structure is again a multidimensional array, implemented by Numpy. The package contains tools that help with solving linear algebra, probability theory, integral calculus, and many more tasks.
- **Pandas**: Pandas offer versatile and powerful tools for manipulating data structures and performing extensive data analysis. It works well with incomplete, unstructured, and unordered real-world data — and comes with tools for shaping, aggregating, analyzing, and visualizing datasets.
- **SciKit-Learn**: Scikit-learn is a Python module integrating a wide range of state-of-the-art machine learning algorithms for medium-scale supervised and unsupervised problems. It is one of the best-known machine-learning libraries for python. The Scikit-learn package focuses on bringing machine learning to non-specialists using a general-purpose high-level language. The primary emphasis is upon ease of use, performance, documentation, and API consistency. With minimal dependencies and easy distribution under the simplified BSD license, SciKit-Learn is widely used in academic and commercial settings. Scikit-learn exposes a concise and consistent interface to the common machine learning algorithms, making it simple to bring ML into production systems.
- **Matplotlib**: Matplotlib is a Python 2D plotting library, capable of producing publication quality figures in a wide variety of hardcopy formats and interactive environments across platforms. Matplotlib can be used in Python scripts, the Python and IPython shell, the Jupyter notebook, web application servers, and four graphical user interface toolkits.
- **NLTK**: NLTK is a leading platform for building Python programs to work with human language data. It provides easy-to-use interfaces to over 50 corpora and lexical resources such as WordNet, along with a suite of text processing libraries for classification, tokenization, stemming, tagging, parsing, and semantic reasoning.
