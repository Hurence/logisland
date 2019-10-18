.. _developer:

Developer Guide
===============

This document summarizes information relevant to logisland committers and contributors. 
It includes information about the development processes and policies as well as the tools we use to facilitate those.



Workflows
---------

This section explains how to perform common activities such as reporting a bug or merging a pull request.


Internal dev (aka logisland team)
+++++++++++++++++++++++++++++++++

We're using GitFlow for github so read carefully the docs :
`<https://datasift.github.io/gitflow/GitFlowForGitHub.html>`_


Coding Guidelines
+++++++++++++++++

Basic
_____

1. Avoid cryptic abbreviations. Single letter variable names are fine in very short methods with few variables, otherwise make them informative.
2. Clear code is preferable to comments. When possible make your naming so good you don't need comments. When that isn't possible comments should be thought of as mandatory, write them to be read.
3. Logging, configuration, and public APIs are our "UI". Make them pretty, consistent, and usable.
4. Maximum line length is 130.
5. Don't leave TODOs in the code or FIXMEs if you can help it. Don't leave println statements in the code. TODOs should be filed as github issues.
6. User documentation should be considered a part of any user-facing the feature, just like unit tests. Example REST apis should've accompanying documentation.
7. Tests should never rely on timing in order to pass.
8. Every unit test should leave no side effects, i.e., any test dependencies should be set during setup and clean during tear down.

Java
____

1. Apache license headers. Make sure you have Apache License headers in your files. 
2. Tabs vs. spaces. We are using 4 spaces for indentation, not tabs. 
3. Blocks. All statements after if, for, while, do, … must always be encapsulated in a block with curly braces (even if the block contains one statement):

    .. code-block:: java

         for (...) {
             ...
         }

4. No wildcard imports. 
5. No unused imports. Remove all unused imports.
6. No raw types. Do not use raw generic types, unless strictly necessary (sometime necessary for signature matches, arrays).
7. Suppress warnings. Add annotations to suppress warnings, if they cannot be avoided (such as “unchecked”, or “serial”).
8. Comments.  Add JavaDocs to public methods or inherit them by not adding any comments to the methods. 
9. logger instance should be upper case LOG.
10. When in doubt refer to existing code or `Java Coding Style <http://google.github.io/styleguide/javaguide.html>`_ except line breaking, which is described above.
  

Logging
_______

1. Please take the time to assess the logs when making a change to ensure that the important things are getting logged and there is no junk there.

2. There are six levels of logging TRACE, DEBUG, INFO, WARN, ERROR, and FATAL, they should be used as follows.

    2.1. INFO is the level you should assume the software will be run in.
     INFO messages are things which are not bad but which the user will definitely want to know about
     every time they occur.

    2.2 TRACE and DEBUG are both things you turn on when something is wrong and you want to
     figure out what is going on. DEBUG should not be so fine grained that it will seriously effect the performance
     of the server. TRACE can be anything. Both DEBUG and TRACE statements should be
     wrapped in an if(logger.isDebugEnabled) if an expensive computation in the argument list of log method call.

    2.3. WARN and ERROR indicate something that is bad.
     Use WARN if you aren't totally sure it is bad, and ERROR if you are.

    2.4. Use FATAL only right before calling System.exit().

3. Logging statements should be complete sentences with proper capitalization that are written to be read by a person not necessarily familiar with the source code. 

4. String appending using StringBuilders should not be used for building log messages.
    Formatting should be used. For ex:
    LOG.debug("Loaded class [{}] from jar [{}]", className, jarFile);

5. In Logisland class implementing ConfigurableComponent use **getLogger** method to log. Most of components in Logisland are ConfigurableComponent.

TimeZone in Tests
_________________

Your environment jdk can be different than travis ones. Be aware that there is changes on TimeZone objects between different
version of jdk... Even between 8.x.x versions.
For example TimeZone "America/Cancun" may not give the same date in your environment than in travis one...


Contribute code
+++++++++++++++

Create a pull request
_____________________

Pull requests should be done against the read-only git repository at
`https://github.com/hurence/logisland <https://github.com/hurence/logisland>`_.

Take a look at `Creating a pull request <https://help.github.com/articles/creating-a-pull-request>`_.  In a nutshell you
need to:

1. `Fork <https://help.github.com/articles/fork-a-repo>`_ the Logisland GitHub repository at
   `https://github.com/hurence/logisland <https://github.com/hurence/logisland>`_ to your personal GitHub
   account.  See `Fork a repo <https://help.github.com/articles/fork-a-repo>`_ for detailed instructions.
2. Commit any changes to your fork.
3. Send a `pull request <https://help.github.com/articles/creating-a-pull-request>`_ to the Logisland GitHub repository
   that you forked in step 1.  If your pull request is related to an existing IoTaS github issue ticket -- for instance, because
   you reported a bug report via github issue earlier -- then prefix the title of your pull request with the corresponding github issue
   ticket number (e.g. `IOT-123: ...`).

You may want to read `Syncing a fork <https://help.github.com/articles/syncing-a-fork>`_ for instructions on how to keep
your fork up to date with the latest changes of the upstream  `Streams` repository.

We are using gitflow to have standard way of starting features, hotfixes and releases.
You can check documentation about `gitflow here <https://datasift.github.io/gitflow/GitFlowForGitHub.html>`_.

Git Commit Messages Format
__________________________

The Git commit messages must be standardized as follows:

LOGISLAND-XXX: Title matching exactly the github issue Summary (title)


    - An optional, bulleted (+, -, ., \*), summary of the contents of
    - the patch. The goal is not to describe the contents of every file,
    - but rather give a quick overview of the main functional areas
    - addressed by the patch.


The text immediately following the github issue number (LOGISLAND-XXX: ) must be an exact transcription of the github issue summary (title), not the a summary of the contents of the patch.

If the github issue summary does not accurately describe what the patch is addressing, the github issue summary must be modified, and then copied to the Git commit message.

A summary with the contents of the patch is optional but strongly encouraged if the patch is large and/or the github issue title is not expressive enough to describe what the patch is doing. This text must be bulleted using one of the following bullet points (+, -, ., ). There must be at last a 1 space indent before the bullet char, and exactly one space between bullet char and the first letter of the text. Bullets are not optional, but *required**.

Develop components
__________________

You can find help on these topics here :

- :ref:`dev-processors`
- :ref:`dev-services`
- :ref:`dev-connectors`
- :ref:`dev-streams`
- :ref:`dev-engines`

Build the code and run the tests
--------------------------------

Prerequisites
-------------
First of all you need to make sure you are using maven 3.2.5 or higher and JDK 1.8 or higher.

Building
--------

The following commands must be run from the top-level directory.

To build a light version of logisland with only common processors installed, run the unit tests and integration tests:

.. code-block:: sh

    mvn -Pintegration-tests install

To build a heavy version of logisland with all logisland plugins installed, run the unit tests and integration tests:


.. code-block:: sh

    mvn -P full,integration-tests install

If you wish to run unit tests and integration tests, no more, use the maven 'verify' phase instead of 'install'

If you wish to skip the unit as well as integration tests you can do this by adding `-DskipTests` to the command line.

If you wish to skip the integration tests, remove the 'integration-tests' profile.

If you wish to add all the plugins to the build, add the 'full' profile.

Unit tests and integration tests guidelines
-------------------------------------------

While unit tests do only use logisland libraries, integration tests are tests that make interactions between logisland
and some external systems. For instance, all tests effectively pushing records to external databases from a logisland
datastore service are integration tests. They require for instance a docker container running for the database being
automatically launched and stopped for the integration tests.

Do your unit tests in the traditional maven test directory of your component. Integration tests must however be placed
in a separate folder named integration-test, at the same level of the unit tests test folder. The integration test
classes should respect the *IT.java naming pattern.

The failsafe maven plugin responsible for launching the integration tests (at the integration-test maven phase) is
already defined in the root pom.xml. You must however add an integration-tests profile in the pom.xml of your component
to add the logic for launching and stopping your external system.

For instance to launch a docker container at the pre-integration-test maven phase and to stop it at the
post-integration-test maven phase, use the io.fabric8/docker-maven-plugin maven plugin in your integration-tests profile.
For a complete exemple of integration-tests profile declaration, please see and copy what has been done in the influxdb service.

Release to maven repositories
-----------------------------
to release artifacts (if you're allowed to), follow this guide `release to OSS Sonatype with maven <http://central.sonatype.org/pages/apache-maven.html>`_

.. code-block:: sh

   ./update-version.sh -o 1\\.1\\.2 -n 14.4
    mvn license:format
    mvn test
    mvn -DperformRelease=true clean deploy -Pfull
    mvn versions:commit


follow the staging procedure in `oss.sonatype.org <https://oss.sonatype.org/#stagingRepositories>`_ or read `Sonatype book <http://books.sonatype.com/nexus-book/reference/staging-deployment.html#staging-maven>`_

go to `oss.sonatype.org <https://oss.sonatype.org/#stagingRepositories>`_ to release manually the artifact



Publish release assets to github
--------------------------------

please refer to `https://developer.github.com/v3/repos/releases <https://developer.github.com/v3/repos/releases>`_

curl -XPOST https://uploads.github.com/repos/Hurence/logisland/releases/v1.2.0/assets?name=logisland-1.1.2-bin.tar.gz -v  --data-binary  @logisland-assembly/target/logisland-1.1.2-bin.tar.gz --user oalam -H 'Content-Type: application/gzip'



Publish Docker image
--------------------
Building the image

.. code-block:: sh

    # build logisland
    mvn install -DskipTests -Pdocker -Pfull

    # verify image build
    docker images

then login and push the latest image

.. code-block:: sh

    docker login
    docker push hurence/logisland


Publish artifact to github
--------------------------

Tag the release + upload latest tgz
