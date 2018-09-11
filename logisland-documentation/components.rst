Components
==========
You'll find here the list of all usable Processors, Engines, Services and other components that can be usable out of the box in your analytics streams


----------

.. _com.hurence.logisland.service.hbase.HBase_1_1_2_ClientService: 

HBase_1_1_2_ClientService
-------------------------
Implementation of HBaseClientService for HBase 1.1.2. This service can be configured by providing a comma-separated list of configuration files, or by specifying values for the other properties. If configuration files are provided, they will be loaded first, and the values of the additional properties will override the values from the configuration files. In addition, any user defined properties on the processor will also be passed to the HBase configuration.

Class
_____
com.hurence.logisland.service.hbase.HBase_1_1_2_ClientService

Tags
____
hbase, client

Properties
__________
In the list below, the names of required properties appear in **bold**. Any other properties (not in bold) are considered optional. The table also indicates any default values
, and whether a property supports the  `Expression Language <expression-language.html>`_ .

.. csv-table:: allowable-values
   :header: "Name","Description","Allowable Values","Default Value","Sensitive","EL"
   :widths: 20,60,30,20,10,10

   "hadoop.configuration.files", "Comma-separated list of Hadoop Configuration files, such as hbase-site.xml and core-site.xml for kerberos, including full paths to the files.", "", "null", "", ""
   "zookeeper.quorum", "Comma-separated list of ZooKeeper hosts for HBase. Required if Hadoop Configuration Files are not provided.", "", "null", "", ""
   "zookeeper.client.port", "The port on which ZooKeeper is accepting client connections. Required if Hadoop Configuration Files are not provided.", "", "null", "", ""
   "zookeeper.znode.parent", "The ZooKeeper ZNode Parent value for HBase (example: /hbase). Required if Hadoop Configuration Files are not provided.", "", "null", "", ""
   "hbase.client.retries", "The number of times the HBase client will retry connecting. Required if Hadoop Configuration Files are not provided.", "", "3", "", ""
   "phoenix.client.jar.location", "The full path to the Phoenix client JAR. Required if Phoenix is installed on top of HBase.", "", "null", "", "**true**"

Dynamic Properties
__________________
Dynamic Properties allow the user to specify both the name and value of a property.

.. csv-table:: dynamic-properties
   :header: "Name","Value","Description","EL"
   :widths: 20,20,40,10

   "The name of an HBase configuration property.", "The value of the given HBase configuration property.", "These properties will be set on the HBase configuration after loading any provided configuration files.", ""
