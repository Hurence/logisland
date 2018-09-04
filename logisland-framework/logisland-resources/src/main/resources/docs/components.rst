Components
==========
You'll find here the list of all usable Processors, Engines, Services and other components that can be usable out of the box in your analytics streams


----------

.. _com.hurence.logisland.processor.hbase.FetchHBaseRow: 

FetchHBaseRow
-------------
Fetches a row from an HBase table. The Destination property controls whether the cells are added as flow file attributes, or the row is written to the flow file content as JSON. This processor may be used to fetch a fixed row on a interval by specifying the table and row id directly in the processor, or it may be used to dynamically fetch rows by referencing the table and row id from incoming flow files.

Class
_____
com.hurence.logisland.processor.hbase.FetchHBaseRow

Tags
____
hbase, scan, fetch, get, enrich

Properties
__________
In the list below, the names of required properties appear in **bold**. Any other properties (not in bold) are considered optional. The table also indicates any default values
, and whether a property supports the  `Expression Language <expression-language.html>`_ .

.. csv-table:: allowable-values
   :header: "Name","Description","Allowable Values","Default Value","Sensitive","EL"
   :widths: 20,60,30,20,10,10

   "**hbase.client.service**", "The instance of the Controller Service to use for accessing HBase.", "", "null", "", ""
   "**table.name.field**", "The field containing the name of the HBase Table to fetch from.", "", "null", "", "**true**"
   "**row.identifier.field**", "The field containing the  identifier of the row to fetch.", "", "null", "", "**true**"
   "columns.field", "The field containing an optional comma-separated list of "<colFamily>:<colQualifier>" pairs to fetch. To return all columns for a given family, leave off the qualifier such as "<colFamily1>,<colFamily2>".", "", "null", "", "**true**"
   "record.serializer", "the serializer needed to i/o the record in the HBase row", "kryo serialization (serialize events as json blocs), json serialization (serialize events as json blocs), avro serialization (serialize events as avro blocs), no serialization (send events as bytes)", "com.hurence.logisland.serializer.KryoSerializer", "", ""
   "record.schema", "the avro schema definition for the Avro serialization", "", "null", "", ""
   "table.name.default", "The table table to use if table name field is not set", "", "null", "", ""

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

----------

.. _com.hurence.logisland.processor.hbase.PutHBaseCell: 

PutHBaseCell
------------
Adds the Contents of a Record to HBase as the value of a single cell

Class
_____
com.hurence.logisland.processor.hbase.PutHBaseCell

Tags
____
hadoop, hbase

Properties
__________
In the list below, the names of required properties appear in **bold**. Any other properties (not in bold) are considered optional. The table also indicates any default values
, and whether a property supports the  `Expression Language <expression-language.html>`_ .

.. csv-table:: allowable-values
   :header: "Name","Description","Allowable Values","Default Value","Sensitive","EL"
   :widths: 20,60,30,20,10,10

   "**hbase.client.service**", "The instance of the Controller Service to use for accessing HBase.", "", "null", "", ""
   "**table.name.field**", "The field containing the name of the HBase Table to put data into", "", "null", "", "**true**"
   "row.identifier.field", "Specifies  field containing the Row ID to use when inserting data into HBase", "", "null", "", "**true**"
   "row.identifier.encoding.strategy", "Specifies the data type of Row ID used when inserting data into HBase. The default behavior is to convert the row id to a UTF-8 byte array. Choosing Binary will convert a binary formatted string to the correct byte[] representation. The Binary option should be used if you are using Binary row keys in HBase", "String (Stores the value of row id as a UTF-8 String.), Binary (Stores the value of the rows id as a binary byte array. It expects that the row id is a binary formatted string.)", "String", "", ""
   "**column.family.field**", "The field containing the  Column Family to use when inserting data into HBase", "", "null", "", "**true**"
   "**column.qualifier.field**", "The field containing the  Column Qualifier to use when inserting data into HBase", "", "null", "", "**true**"
   "**batch.size**", "The maximum number of Records to process in a single execution. The Records will be grouped by table, and a single Put per table will be performed.", "", "25", "", ""
   "record.schema", "the avro schema definition for the Avro serialization", "", "null", "", ""
   "record.serializer", "the serializer needed to i/o the record in the HBase row", "kryo serialization (serialize events as json blocs), json serialization (serialize events as json blocs), avro serialization (serialize events as avro blocs), no serialization (send events as bytes)", "com.hurence.logisland.serializer.KryoSerializer", "", ""
   "table.name.default", "The table table to use if table name field is not set", "", "null", "", ""
   "column.family.default", "The column family to use if column family field is not set", "", "null", "", ""
   "column.qualifier.default", "The column qualifier to use if column qualifier field is not set", "", "null", "", ""
