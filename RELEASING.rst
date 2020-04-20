Releasing guide
===============

This guide will help you to perform the full release process for Logisland framework.

Logisland sources follow the `GitFlow <https://datasift.github.io/gitflow/IntroducingGitFlow.html>`_ process.
So be sure that any changes from the latest available release have been back-ported to the master and develop branch.

Start a new branch and bump the version
---------------------------------------

    git hf release start v1.3.0

    # update the version with the bump_version.sh <old_version> <new_version> script:
    # you should double escape dots so it is correctly parsed, otherwise it will be considered as the any character
    ./bump_version.sh 1\\.3\\.0 1.4.0

Build the code, run the unit tests as well as integration tests
---------------------------------------------------------------

The following commands must be run from the top-level directory:

.. code-block:: sh

    mvn -Pintegration-tests clean verify

.. note::
   When doing a new release, all available unit tests and integration tests should be run and pass.
   If you know what your are doing, you can however use those additional information:

To only build and run unit tests without integration tests:

.. code-block:: sh

    mvn clean package

If you wish to skip the unit tests in any of those commands, you can do this by adding `-DskipTests` to the command line.

Release to maven repositories
-----------------------------
to release artifacts (if you're allowed to), follow this guide `release to OSS Sonatype with maven <http://central.sonatype.org/pages/apache-maven.html>`_

.. code-block:: sh

    mvn license:format
    mvn -DperformRelease=true clean deploy
    mvn versions:commit


follow the staging procedure in `oss.sonatype.org <https://oss.sonatype.org/#stagingRepositories>`_ or read `Sonatype book <http://books.sonatype.com/nexus-book/reference/staging-deployment.html#staging-maven>`_

go to `oss.sonatype.org <https://oss.sonatype.org/#stagingRepositories>`_ to release manually the artifact



Publish release assets to github
--------------------------------

please refer to `https://developer.github.com/v3/repos/releases <https://developer.github.com/v3/repos/releases>`_

curl -XPOST https://uploads.github.com/repos/Hurence/logisland/releases/v1.3.0/assets?name=logisland-1.3.0-bin-hdp2.5.tar.gz -v  --data-binary  @logisland-assembly/target/logisland-0.10.3-bin-hdp2.5.tar.gz --user oalam -H 'Content-Type: application/gzip'



Publish Docker image
--------------------
Building the image

.. code-block:: sh

    # build logisland
    mvn clean install -DskipTests -Pdocker -Dhdp2.5

    # verify image build
    docker images


then login and push the latest image

.. code-block:: sh

    docker login
    docker push hurence/logisland


Publish artifact to github
--------------------------

Tag the release + upload latest tgz
