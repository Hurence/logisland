Docker base container file

used to build all other containers on it with latest jre 8 and some dev tools enabled




build with th following command

.. code-block:: sh

    docker build --rm -t hurence/base:1.0.1 .
    docker run   -it  hurence/base:1.0.1  bash


tag the image as latest

.. code-block:: sh

    # verify image build
    docker images
    docker tag <IMAGE_ID> latest


then login and push the latest image

.. code-block:: sh

    docker login
    docker push hurence/base
