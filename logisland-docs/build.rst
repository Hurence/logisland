Build and deploy logisland
====

In the following section you'll find all the commands needed to build the source code and the Docker image.

Build source code
----
Log-Island is written in Java (and a few lines of Scala), we then use maven to build it

to build from the source just clone and package

.. code-block::

    git clone git@github.com:Hurence/logisland.git
    cd logisland
    mvn package
    
to deploy artifacts (if you're allowed to), follow this guide `release to OSS Sonatype with maven <http://central.sonatype.org/pages/apache-maven.html>`_

.. code-block::

    mvn versions:set -DnewVersion=0.9.5
    mvn clean deploy
    mvn versions:commit

follow the staging procedure in `oss.sonatype.org <https://oss.sonatype.org/#stagingRepositories>`_ or read `Sonatype book <http://books.sonatype.com/nexus-book/reference/staging-deployment.html#staging-maven>`_
    

    
Build Docker image
----
The build the docker image, build log-island.tgz and kafka-manager tool

you have to copy this file into your Dockerfile directory you can now issue

.. code-block::

    cd logisland-docker
    docker build --rm -t hurence/log-island:0.9.5 .