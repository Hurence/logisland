
Architecture
===============

Is there something clever out there ?

Most of the systems in this data world can be observables through their **events**. 
You just have to look at the [event sourcing pattern]() to get an idea of how we could define any system state as a sequence of temporal events. The main source of events are the **logs** files, application logs, transaction logs, sensor data, etc.

Large and complex systems, made of number of heterogeneous components are not easy to monitor, especially when have to deal with distributed computing. Most of the time of IT resources is spent in maintenance tasks, so there's a real need for tools to help achieving them.

.. note::
    Basicaly LogIsland will help us to handle system events from log files.

Data driven architecture
------------------------

.. image:: /_static/data-driven-computing.png


Technical design
----------------

LogIsland is an event processing framework based on Kafka and Spark. The main goal of this Open Source platform is to
abstract the level of complexity of complex event processing at scale. Of course many people start with an ELK stack, 
which is really great but not enough to elaborate a really complete system monitoring tool. 
So with LogIsland, you'll move the log processing burden to a powerfull distributed stack.

Kafka acts a the distributed message queue middleware while Spark is the core of the distributed processing. 
LogIsland glue those technologies to simplify log complex event processing at scale.
 
Just write a custom LogParser and EventMapper class, deploy them as a new jar and launch

.. image:: /_static/logisland-architecture.png
