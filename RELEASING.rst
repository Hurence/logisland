Releasing guide
===============

This guide will help you to perform the full release process for Logisland framework.

Logisland sources follow the `GitFlow <https://datasift.github.io/gitflow/IntroducingGitFlow.html>`_ process.
So be sure that any changes from the latest available release have been back-ported to the master and develop branch.

The develop branch must also include all of your new features that you want to release compared to the current existing release.

Start a new branch and bump the version
---------------------------------------

Be sure `HubFlow git extension <https://github.com/datasift/gitflow>`_ is installed on your system. Then:

.. code-block:: sh

    git hf release start v1.4.0

Update the version with the bump_version.sh script. Usage: bump_version.sh <old_version> <new_version>. You should double escape dots (in the old version only) so it is correctly parsed, otherwise it will be considered as the any character

.. code-block:: sh

    bump_version.sh 1\.2\.0 1.4.0

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

Build logisland docker image
----------------------------

WARNING: you must build the logisland archive with the following command at the root directory of the workspace:

.. code-block:: sh

    mvn -DskipTests clean package

You must not build it with the integration tests run command, as the final packaging will not be spring compliant and
you will have some errors like this one once you run the docker based quickstarts:

.. code-block:: sh

    Exception in thread "main" java.lang.NoClassDefFoundError: org/springframework/boot/loader/archive/Archive

Once the workspace is built, you must locally create with it the docker image that you will test.
To do that, follow instructions in logisland-docker/full-container/README.rst, in the 'Build your own' section.

Run manual sanity checking using the QuickStarts
------------------------------------------------

Run some of the `QuickStarts <https://github.com/Hurence/logisland-quickstarts>`_.
Those tests use the logisland docker image.

You must at least successfully run the `Getting Started Guide <https://logisland.github.io/docs/guides/getting-started-guide>`_
and the `Datastore Elasticsearch Guide <https://logisland.github.io/docs/guides/datastore-elasticsearch-guide>`_

Update the release notes
------------------------

When all the automatic and manual tests are ok, update the release notes with what's new in the new version in logisland-documentation/changes.rst.

Release to maven repositories
-----------------------------
to release artifacts (if you're allowed to), follow this guide `release to OSS Sonatype with maven <http://central.sonatype.org/pages/apache-maven.html>`_

.. code-block:: sh

    mvn license:format
    mvn -DperformRelease=true clean deploy
    mvn versions:commit

Follow the staging procedure in `oss.sonatype.org <https://oss.sonatype.org/#stagingRepositories>`_ or read `Sonatype book <http://books.sonatype.com/nexus-book/reference/staging-deployment.html#staging-maven>`_

go to `oss.sonatype.org <https://oss.sonatype.org/#stagingRepositories>`_ to release manually the artifact

Publish release assets to github
--------------------------------

please refer to `https://developer.github.com/v3/repos/releases <https://developer.github.com/v3/repos/releases>`_

curl -XPOST https://uploads.github.com/repos/Hurence/logisland/releases/v1.4.0/assets?name=logisland-full-bin.tar.gz -v  --data-binary  @logisland-assembly/target/logisland-full-bin.tar.gz --user oalam -H 'Content-Type: application/gzip'

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

Merge back changes from the release into master and close release branch
------------------------------------------------------------------------

.. code-block:: sh

    git hf release finish v1.4.0

TBD: does this also generate the release tag?