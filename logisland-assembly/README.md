Logisland Assembly
==================

Build assemblies for logisland
------------------------------

Run this command at **logisland ROOT project**, not in this sub module otherwise assemblies will miss jars :

.. code-block:: sh

    mvn clean package -DskipTests
    
We would expect that when running below command in root pom. 
The assemblies are correctly built but unfortunately this not the case.
Results seems to depend on maven command used.
With this command core library dependencies are not included...

.. code-block:: sh

    mvn clean install -Pintegration-tests

the lib/core folder should contains (currently):

```
./lib/cores/asm-4.0.jar
./lib/cores/avro-1.8.2.jar
./lib/cores/cglib-nodep-3.2.10.jar
./lib/cores/commons-cli-1.2.jar
./lib/cores/commons-codec-1.10.jar
./lib/cores/commons-collections-3.2.1.jar
./lib/cores/commons-compress-1.8.1.jar
./lib/cores/commons-compress-1.14.jar
./lib/cores/commons-io-2.4.jar
./lib/cores/commons-lang3-3.8.1.jar
./lib/cores/hamcrest-core-1.3.jar
./lib/cores/ivy-2.2.0.jar
./lib/cores/jackson-annotations-2.4.4.jar
./lib/cores/jackson-core-2.4.4.jar
./lib/cores/jackson-core-asl-1.9.13.jar
./lib/cores/jackson-databind-2.4.4.jar
./lib/cores/jackson-dataformat-yaml-2.4.4.jar
./lib/cores/jackson-mapper-asl-1.9.13.jar
./lib/cores/jackson-module-jsonSchema-2.4.4.jar
./lib/cores/jline-0.9.94.jar
./lib/cores/joda-time-2.8.2.jar
./lib/cores/json-20090211.jar
./lib/cores/json-simple-1.1.jar
./lib/cores/junit-4.12.jar
./lib/cores/kryo-2.21.jar
./lib/cores/log4j-api-2.10.0.jar
./lib/cores/log4j-to-slf4j-2.10.0.jar
./lib/cores/logisland-api-1.1.2.jar
./lib/cores/logisland-bootstrap-1.1.2.jar
./lib/cores/logisland-plugin-support-1.1.2.jar
./lib/cores/logisland-scripting-base-1.1.2.jar
./lib/cores/logisland-scripting-mvel-1.1.2.jar
./lib/cores/logisland-utils-1.1.2.jar
./lib/cores/minlog-1.2.jar
./lib/cores/mvel2-2.3.1.Final.jar
./lib/cores/netty-3.7.0.Final.jar
./lib/cores/objenesis-1.2.jar
./lib/cores/objenesis-2.6.jar
./lib/cores/paranamer-2.7.jar
./lib/cores/protobuf-java-2.5.0.jar
./lib/cores/reflectasm-1.07-shaded.jar
./lib/cores/slf4j-api-1.7.16.jar
./lib/cores/snakeyaml-1.12.jar
./lib/cores/snappy-java-1.1.1.3.jar
./lib/cores/spring-boot-loader-2.0.0.RELEASE.jar
./lib/cores/spring-boot-loader-tools-2.0.0.RELEASE.jar
./lib/cores/spring-core-5.0.4.RELEASE.jar
./lib/cores/spring-jcl-5.0.4.RELEASE.jar
./lib/cores/xz-1.5.jar
./lib/cores/zkclient-0.8.jar
./lib/cores/zookeeper-3.4.6.jar
```
 

Different Type of assemblies
----------------------------

The descriptors are located in src/assembly.

At the moment we build three differents version of assembly for logisland

Bare metal
++++++++++

This is built as :
* logisland-1.1.2-bare-metal-bin
* logisland-1.1.2-bare-metal-bin.tar.gz

This contain only framework dependencies and engines.
There is no services or processors included so you will have to install them manually with bin/component.sh script


Light
+++++

This is built as :
* logisland-1.1.2-light-bin
* logisland-1.1.2-light-bin.tar.gz

This contain only framework dependencies and engines.
There is also a few processors from logisland-processor-commons module.
For other services or processors you will have to install them manually with bin/component.sh script

full
++++

This is built as :
* logisland-1.1.2-full-bin
* logisland-1.1.2-full-bin.tar.gz

This contain all built-in dependencies. With this packaging every logisland services and processors are already installed.
You can install third party components with bin/component.sh script if necessary.
