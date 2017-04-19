Release manual
==============


This guide will help you releasing logisland


Release to maven repositories
-----------------------------
to release artifacts (if you're allowed to), follow this guide `release to OSS Sonatype with maven <http://central.sonatype.org/pages/apache-maven.html>`_

.. code-block::

    mvn versions:set -DnewVersion=0.10.0-SNAPSHOT
    mvn license:format
    mvn test
    mvn -DperformRelease=true clean deploy
    mvn versions:commit

    git tag -a v0.10.0-SNAPSHOT -m "new logisland release 0.10.0-SNAPSHOT"
    git push origin v0.10.0-SNAPSHOT

follow the staging procedure in `oss.sonatype.org <https://oss.sonatype.org/#stagingRepositories>`_ or read `Sonatype book <http://books.sonatype.com/nexus-book/reference/staging-deployment.html#staging-maven>`_

go to `oss.sonatype.org <https://oss.sonatype.org/#stagingRepositories>`_ to release manually the artefact

Publish Docker image
--------------------
Building the image

.. code-block::sh

    # build logisland
    mvn clean install -Dhdp=2.4
    cp logisland-assembly/target/logisland-0.10.0-SNAPSHOT-bin.tar.gz logisland-docker

The archive is generated under dist directory,
you have to copy this file into your Dockerfile directory you can now issue

.. code-block::sh

    docker build --rm -t hurence/logisland:0.10.0-SNAPSHOT .


To tag the latest docker image find its id

.. code-block::sh

    docker images
    docker tag 7d9495d03763 hurence/logisland:latest

then login and push the latest image

.. code-block::sh

    docker login
    docker push hurence/logisland


Publish artefact to github
--------------------------

Tag the release + upload latest tgz