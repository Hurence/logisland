LogIsland integration tests
===========================

We suppose you are in root project directory

Lanching tests
--------------

Building current logisland docker

.. code-block:: sh

    # build logisland
    mvn clean install -Pfull
    cp logisland-assembly/target/logisland-1.0.0-RC1-bin.tar.gz logisland-docker/container/logisland-1.0.0-RC1-bin.tar.gz
    # go in our working dir
    cd logisland-docker/src/integration-test
    # run all integration tests
    # First time should be longer if you have not needed images.
    sudo ./run-all-test.sh





