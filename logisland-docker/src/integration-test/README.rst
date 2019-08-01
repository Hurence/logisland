LogIsland integration tests
===========================

These tests are used to be sure we do not break already implemented engine and streams.

We could add integration test of all current tutorial latters.

Lanching tests
--------------

You need to have docker and docker-compose installed. Those tests use the current version of logisland in your local machine,
it build a logisland image based on logisland-assembly/target/logisland-*.tar.gz file. So you must have install logsialnd with this command before running the test

.. code-block:: sh

    mvn clean package

Now just run the script with sudo (for using docker).

.. code-block:: sh

    sudo ./run-all-test.sh





