Deployement
===========

Here you'll find all the information needed to deploy logisland on an Hadoop cluster


Launching the Agent
-------------------

.. code-block::

    export SPARK_HOME=/opt/spark-2.1.0-bin-hadoop2.7
    export HADOOP_CONF_DIR=/opt/spark-2.1.0-bin-hadoop2.7

Launching the Web ui
--------------------

Install a web server like Apache httpd or nginx and make it serve static files under $LOGISLAND_HOME/www

.. code-block::

    sudo yum install nginx
    sudo systemctl start nginx


Install NPM tools & build the agent

.. code-block::

    sudo yum install npm
    cd $LOGISLAND_HOME/www
    npm install

