Components
==========
You'll find here the list of all usable Processors, Engines, Services and other components that can be usable out of the box in your analytics streams


----------

.. _com.hurence.logisland.service.mongodb.AbstractMongoDBControllerService: 

AbstractMongoDBControllerService
--------------------------------
No description provided.

Class
_____
com.hurence.logisland.service.mongodb.AbstractMongoDBControllerService

Tags
____
None.

Properties
__________
In the list below, the names of required properties appear in **bold**. Any other properties (not in bold) are considered optional. The table also indicates any default values
, and whether a property supports the  `Expression Language <expression-language.html>`_ .

.. csv-table:: allowable-values
   :header: "Name","Description","Allowable Values","Default Value","Sensitive","EL"
   :widths: 20,60,30,20,10,10

   "**Mongo URI**", "MongoURI, typically of the form: mongodb://host1[:port1][,host2[:port2],...]", "", "null", "", "**true**"
   "**Mongo Database Name**", "The name of the database to use", "", "null", "", "**true**"
   "**Mongo Collection Name**", "The name of the collection to use", "", "null", "", "**true**"

----------

.. _com.hurence.logisland.processor.AddFields: 

AddFields
---------
Add one or more field with a default value
...

Class
_____
com.hurence.logisland.processor.AddFields

Tags
____
record, fields, Add

Properties
__________
In the list below, the names of required properties appear in **bold**. Any other properties (not in bold) are considered optional. The table also indicates any default values
.

.. csv-table:: allowable-values
   :header: "Name","Description","Allowable Values","Default Value","Sensitive","EL"
   :widths: 20,60,30,20,10,10

   "conflict.resolution.policy", "What to do when a field with the same name already exists ?", "overwrite existing field (if field already exist), keep only old field value (keep only old field)", "keep_only_old_field", "", ""

Dynamic Properties
__________________
Dynamic Properties allow the user to specify both the name and value of a property.

.. csv-table:: dynamic-properties
   :header: "Name","Value","Description","EL"
   :widths: 20,20,40,10

   "field to add", "a default value", "Add a field to the record with the default value", ""

----------

.. _com.hurence.logisland.processor.ApplyRegexp: 

ApplyRegexp
-----------
This processor is used to create a new set of fields from one field (using regexp).

Class
_____
com.hurence.logisland.processor.ApplyRegexp

Tags
____
parser, regex, log, record

Properties
__________
In the list below, the names of required properties appear in **bold**. Any other properties (not in bold) are considered optional. The table also indicates any default values
.

.. csv-table:: allowable-values
   :header: "Name","Description","Allowable Values","Default Value","Sensitive","EL"
   :widths: 20,60,30,20,10,10

   "conflict.resolution.policy", "What to do when a field with the same name already exists ?", "overwrite existing field (if field already exist), keep only old field (keep only old field)", "keep_only_old_field", "", ""

Dynamic Properties
__________________
Dynamic Properties allow the user to specify both the name and value of a property.

.. csv-table:: dynamic-properties
   :header: "Name","Value","Description","EL"
   :widths: 20,20,40,10

   "alternative regex & mapping", "another regex that could match", "This processor is used to create a new set of fields from one field (using regexp).", **true**

See Also:
_________
`com.hurence.logisland.processor.ApplyRegexp`_ 

----------

.. _com.hurence.logisland.processor.elasticsearch.BulkAddElasticsearch: 

BulkAddElasticsearch
--------------------
Indexes the content of a Record in Elasticsearch using elasticsearch's bulk processor

Class
_____
com.hurence.logisland.processor.elasticsearch.BulkAddElasticsearch

Tags
____
elasticsearch

Properties
__________
In the list below, the names of required properties appear in **bold**. Any other properties (not in bold) are considered optional. The table also indicates any default values
, and whether a property supports the  `Expression Language <expression-language.html>`_ .

.. csv-table:: allowable-values
   :header: "Name","Description","Allowable Values","Default Value","Sensitive","EL"
   :widths: 20,60,30,20,10,10

   "**elasticsearch.client.service**", "The instance of the Controller Service to use for accessing Elasticsearch.", "", "null", "", ""
   "**default.index**", "The name of the index to insert into", "", "null", "", "**true**"
   "**default.type**", "The type of this document (used by Elasticsearch for indexing and searching)", "", "null", "", "**true**"
   "**timebased.index**", "do we add a date suffix", "No date (no date added to default index), Today's date (today's date added to default index), yesterday's date (yesterday's date added to default index)", "no", "", ""
   "es.index.field", "the name of the event field containing es index name => will override index value if set", "", "null", "", ""
   "es.type.field", "the name of the event field containing es doc type => will override type value if set", "", "null", "", ""

----------

.. _com.hurence.logisland.processor.datastore.BulkPut: 

BulkPut
-------
Indexes the content of a Record in a Datastore using bulk processor

Class
_____
com.hurence.logisland.processor.datastore.BulkPut

Tags
____
datastore, record, put, bulk

Properties
__________
In the list below, the names of required properties appear in **bold**. Any other properties (not in bold) are considered optional. The table also indicates any default values
, and whether a property supports the  `Expression Language <expression-language.html>`_ .

.. csv-table:: allowable-values
   :header: "Name","Description","Allowable Values","Default Value","Sensitive","EL"
   :widths: 20,60,30,20,10,10

   "**datastore.client.service**", "The instance of the Controller Service to use for accessing datastore.", "", "null", "", ""
   "**default.collection**", "The name of the collection/index/table to insert into", "", "null", "", "**true**"
   "**timebased.collection**", "do we add a date suffix", "No date (no date added to default index), Today's date (today's date added to default index), yesterday's date (yesterday's date added to default index)", "no", "", ""
   "date.format", "simple date format for date suffix. default : yyyy.MM.dd", "", "yyyy.MM.dd", "", ""
   "collection.field", "the name of the event field containing es index name => will override index value if set", "", "null", "", "**true**"

----------

.. _com.hurence.logisland.service.cassandra.CassandraControllerService: 

CassandraControllerService
--------------------------
Provides a controller service that wraps most of the functionality of the Cassandra driver.

Class
_____
com.hurence.logisland.service.cassandra.CassandraControllerService

Tags
____
cassandra, service

Properties
__________
In the list below, the names of required properties appear in **bold**. Any other properties (not in bold) are considered optional. The table also indicates any default values
.

.. csv-table:: allowable-values
   :header: "Name","Description","Allowable Values","Default Value","Sensitive","EL"
   :widths: 20,60,30,20,10,10

   "**Cassandra hosts**", "Cassandra cluster hosts as a comma separated value list", "", "null", "", ""
   "**Cassandra port**", "Cassandra cluster port", "", "null", "", ""
   "**Cassandra keyspace name**", "The name of the keyspace to use", "", "null", "", ""
   "**Cassandra table name**", "The name of the table to use in the keyspace", "", "null", "", ""
   "**Cassandra table fields and types**", "Tne names of the table fields and their cassandra types. For a bulkput, each field must be an existing incoming logisland record field. The format of this property is: <record_field1>:<cassandra_type1>[,<record_fieldN>:<cassandra_typeN>]. Example: record_id:uuid,record_time:timestamp,intValue,textValue.", "", "null", "", ""
   "**Cassandra table primary key**", "Tne ordered names of the fields forming the primary key in the table to create. The format of this property is: primaryKeyField[,<nextPrimaryKeyFieldN>]. Example: record_id,intValue", "", "null", "", ""
   "Create or not the cassandra schema if it does not exist.", "If this property is true, then if they do not exist, the keyspace and the table with its defined fields and primary key will be created at initialization time.Otherwise, all these elements are expected to already exist in the cassandra cluster", "", "true", "", ""
   "Use SSL.", "If this property is true, use SSL. Default is no SSL (false).", "", "false", "", ""
   "Use credentials.", "If this property is true, use credentials. Default is no credentials (false).", "", "false", "", ""
   "User name.", "The user name to use for authentication. cassandra.with-credentials must be true for that property to be used.", "", "null", "", ""
   "User password.", "The user password to use for authentication. cassandra.with-credentials must be true for that property to be used.", "", "null", "", ""
   "batch.size", "The preferred number of Records to setField to the database in a single transaction", "", "1000", "", ""
   "bulk.size", "bulk size in MB", "", "5", "", ""
   "flush.interval", "flush interval in ms", "", "500", "", ""

----------

.. _com.hurence.logisland.processor.alerting.CheckAlerts: 

CheckAlerts
-----------
Add one or more field with a default value

Class
_____
com.hurence.logisland.processor.alerting.CheckAlerts

Tags
____
record, alerting, thresholds, opc, tag

Properties
__________
In the list below, the names of required properties appear in **bold**. Any other properties (not in bold) are considered optional. The table also indicates any default values
.

.. csv-table:: allowable-values
   :header: "Name","Description","Allowable Values","Default Value","Sensitive","EL"
   :widths: 20,60,30,20,10,10

   "max.cpu.time", "maximum CPU time in milliseconds allowed for script execution.", "", "100", "", ""
   "max.memory", "maximum memory in Bytes which JS executor thread can allocate", "", "51200", "", ""
   "allow.no.brace", "Force, to check if all blocks are enclosed with curly braces "{}".
<p>
  Explanation: all loops (for, do-while, while, and if-else, and functions
  should use braces, because poison_pill() function will be inserted after
  each open brace "{", to ensure interruption checking. Otherwise simple
  code like:
  <pre>
    while(true) while(true) {
      // do nothing
    }
  </pre>
  or even:
  <pre>
    while(true)
  </pre>
  cause unbreakable loop, which force this sandbox to use {@link Thread#stop()}
  which make JVM unstable.
</p>
<p>
  Properly writen code (even in bad intention) like:
  <pre>
    while(true) { while(true) {
      // do nothing
    }}
  </pre>
  will be changed into:
  <pre>
    while(true) {poison_pill(); 
      while(true) {poison_pill();
        // do nothing
      }
    }
  </pre>
  which finish nicely when interrupted.
<p>
  For legacy code, this check can be turned off, but with no guarantee, the
  JS thread will gracefully finish when interrupted.
</p>", "", "false", "", ""
   "max.prepared.statements", "The size of prepared statements LRU cache. Default 0 (disabled).
<p>
  Each statements when {@link #setMaxCPUTime(long)} is set is prepared to
  quit itself when time exceeded. To execute only once this procedure per
  statement set this value.
</p>
<p>
  When {@link #setMaxCPUTime(long)} is set 0, this value is ignored.
</p>", "", "30", "", ""
   "**datastore.client.service**", "The instance of the Controller Service to use for accessing datastore.", "", "null", "", ""
   "datastore.cache.collection", "The collection where to find cached objects", "", "test", "", ""
   "js.cache.service", "The cache service to be used to store already sanitized JS expressions. If not specified a in-memory unlimited hash map will be used.", "", "null", "", ""
   "output.record.type", "the type of the output record", "", "event", "", ""
   "profile.activation.condition", "A javascript expression that activates this alerting profile when true", "", "0==0", "", ""
   "alert.criticity", "from 0 to ...", "", "0", "", ""

Dynamic Properties
__________________
Dynamic Properties allow the user to specify both the name and value of a property.

.. csv-table:: dynamic-properties
   :header: "Name","Value","Description","EL"
   :widths: 20,20,40,10

   "field to add", "a default value", "Add a field to the record with the default value", ""

----------

.. _com.hurence.logisland.processor.alerting.CheckThresholds: 

CheckThresholds
---------------
Compute threshold cross from given formulas.
            each dynamic property will return a new record according to the formula definition
            the record name will be set to the property name
            the record time will be set to the current timestamp

Class
_____
com.hurence.logisland.processor.alerting.CheckThresholds

Tags
____
record, threshold, tag, alerting

Properties
__________
In the list below, the names of required properties appear in **bold**. Any other properties (not in bold) are considered optional. The table also indicates any default values
.

.. csv-table:: allowable-values
   :header: "Name","Description","Allowable Values","Default Value","Sensitive","EL"
   :widths: 20,60,30,20,10,10

   "max.cpu.time", "maximum CPU time in milliseconds allowed for script execution.", "", "100", "", ""
   "max.memory", "maximum memory in Bytes which JS executor thread can allocate", "", "51200", "", ""
   "allow.no.brace", "Force, to check if all blocks are enclosed with curly braces "{}".
<p>
  Explanation: all loops (for, do-while, while, and if-else, and functions
  should use braces, because poison_pill() function will be inserted after
  each open brace "{", to ensure interruption checking. Otherwise simple
  code like:
  <pre>
    while(true) while(true) {
      // do nothing
    }
  </pre>
  or even:
  <pre>
    while(true)
  </pre>
  cause unbreakable loop, which force this sandbox to use {@link Thread#stop()}
  which make JVM unstable.
</p>
<p>
  Properly writen code (even in bad intention) like:
  <pre>
    while(true) { while(true) {
      // do nothing
    }}
  </pre>
  will be changed into:
  <pre>
    while(true) {poison_pill(); 
      while(true) {poison_pill();
        // do nothing
      }
    }
  </pre>
  which finish nicely when interrupted.
<p>
  For legacy code, this check can be turned off, but with no guarantee, the
  JS thread will gracefully finish when interrupted.
</p>", "", "false", "", ""
   "max.prepared.statements", "The size of prepared statements LRU cache. Default 0 (disabled).
<p>
  Each statements when {@link #setMaxCPUTime(long)} is set is prepared to
  quit itself when time exceeded. To execute only once this procedure per
  statement set this value.
</p>
<p>
  When {@link #setMaxCPUTime(long)} is set 0, this value is ignored.
</p>", "", "30", "", ""
   "**datastore.client.service**", "The instance of the Controller Service to use for accessing datastore.", "", "null", "", ""
   "datastore.cache.collection", "The collection where to find cached objects", "", "test", "", ""
   "js.cache.service", "The cache service to be used to store already sanitized JS expressions. If not specified a in-memory unlimited hash map will be used.", "", "null", "", ""
   "output.record.type", "the type of the output record", "", "event", "", ""
   "record.ttl", "How long (in ms) do the record will remain in cache", "", "30000", "", ""
   "min.update.time.ms", "The minimum amount of time (in ms) that we expect between two consecutive update of the same threshold record", "", "200", "", ""

Dynamic Properties
__________________
Dynamic Properties allow the user to specify both the name and value of a property.

.. csv-table:: dynamic-properties
   :header: "Name","Value","Description","EL"
   :widths: 20,20,40,10

   "field to add", "a default value", "Add a field to the record with the default value", ""

----------

.. _com.hurence.logisland.processor.alerting.ComputeTags: 

ComputeTags
-----------
Compute tag cross from given formulas.
- each dynamic property will return a new record according to the formula definition
- the record name will be set to the property name
- the record time will be set to the current timestamp

a threshold_cross has the following properties : count, sum, avg, time, duration, value

Class
_____
com.hurence.logisland.processor.alerting.ComputeTags

Tags
____
record, fields, Add

Properties
__________
In the list below, the names of required properties appear in **bold**. Any other properties (not in bold) are considered optional. The table also indicates any default values
.

.. csv-table:: allowable-values
   :header: "Name","Description","Allowable Values","Default Value","Sensitive","EL"
   :widths: 20,60,30,20,10,10

   "max.cpu.time", "maximum CPU time in milliseconds allowed for script execution.", "", "100", "", ""
   "max.memory", "maximum memory in Bytes which JS executor thread can allocate", "", "51200", "", ""
   "allow.no.brace", "Force, to check if all blocks are enclosed with curly braces "{}".
<p>
  Explanation: all loops (for, do-while, while, and if-else, and functions
  should use braces, because poison_pill() function will be inserted after
  each open brace "{", to ensure interruption checking. Otherwise simple
  code like:
  <pre>
    while(true) while(true) {
      // do nothing
    }
  </pre>
  or even:
  <pre>
    while(true)
  </pre>
  cause unbreakable loop, which force this sandbox to use {@link Thread#stop()}
  which make JVM unstable.
</p>
<p>
  Properly writen code (even in bad intention) like:
  <pre>
    while(true) { while(true) {
      // do nothing
    }}
  </pre>
  will be changed into:
  <pre>
    while(true) {poison_pill(); 
      while(true) {poison_pill();
        // do nothing
      }
    }
  </pre>
  which finish nicely when interrupted.
<p>
  For legacy code, this check can be turned off, but with no guarantee, the
  JS thread will gracefully finish when interrupted.
</p>", "", "false", "", ""
   "max.prepared.statements", "The size of prepared statements LRU cache. Default 0 (disabled).
<p>
  Each statements when {@link #setMaxCPUTime(long)} is set is prepared to
  quit itself when time exceeded. To execute only once this procedure per
  statement set this value.
</p>
<p>
  When {@link #setMaxCPUTime(long)} is set 0, this value is ignored.
</p>", "", "30", "", ""
   "**datastore.client.service**", "The instance of the Controller Service to use for accessing datastore.", "", "null", "", ""
   "datastore.cache.collection", "The collection where to find cached objects", "", "test", "", ""
   "js.cache.service", "The cache service to be used to store already sanitized JS expressions. If not specified a in-memory unlimited hash map will be used.", "", "null", "", ""
   "output.record.type", "the type of the output record", "", "event", "", ""

Dynamic Properties
__________________
Dynamic Properties allow the user to specify both the name and value of a property.

.. csv-table:: dynamic-properties
   :header: "Name","Value","Description","EL"
   :widths: 20,20,40,10

   "field to add", "a default value", "Add a field to the record with the default value", ""

----------

.. _com.hurence.logisland.processor.ConvertFieldsType: 

ConvertFieldsType
-----------------
Converts a field value into the given type. does nothing if conversion is not possible

Class
_____
com.hurence.logisland.processor.ConvertFieldsType

Tags
____
type, fields, update, convert

Properties
__________
This component has no required or optional properties.

Dynamic Properties
__________________
Dynamic Properties allow the user to specify both the name and value of a property.

.. csv-table:: dynamic-properties
   :header: "Name","Value","Description","EL"
   :widths: 20,20,40,10

   "field", "the new type", "convert field value into new type", **true**

----------

.. _com.hurence.logisland.processor.DebugStream: 

DebugStream
-----------
This is a processor that logs incoming records

Class
_____
com.hurence.logisland.processor.DebugStream

Tags
____
record, debug

Properties
__________
In the list below, the names of required properties appear in **bold**. Any other properties (not in bold) are considered optional. The table also indicates any default values
.

.. csv-table:: allowable-values
   :header: "Name","Description","Allowable Values","Default Value","Sensitive","EL"
   :widths: 20,60,30,20,10,10

   "**event.serializer**", "the way to serialize event", "Json serialization (serialize events as json blocs), String serialization (serialize events as toString() blocs)", "json", "", ""
   "record.types", "comma separated list of record to include. all if empty", "", "", "", ""

----------

.. _com.hurence.logisland.service.elasticsearch.Elasticsearch_2_4_0_ClientService: 

Elasticsearch_2_4_0_ClientService
---------------------------------
Implementation of ElasticsearchClientService for Elasticsearch 2.4.0.

Class
_____
com.hurence.logisland.service.elasticsearch.Elasticsearch_2_4_0_ClientService

Tags
____
elasticsearch, client

Properties
__________
In the list below, the names of required properties appear in **bold**. Any other properties (not in bold) are considered optional. The table also indicates any default values
, and whether a property is considered "sensitive", meaning that its value will be encrypted. Before entering a value in a sensitive property, ensure that the **logisland.properties** file has an entry for the property **logisland.sensitive.props.key**.

.. csv-table:: allowable-values
   :header: "Name","Description","Allowable Values","Default Value","Sensitive","EL"
   :widths: 20,60,30,20,10,10

   "**backoff.policy**", "strategy for retrying to execute requests in bulkRequest", "No retry policy (when a request fail there won't be any retry.), wait a fixed amount of time between retries (wait a fixed amount of time between retries, using user put retry number and throttling delay), custom exponential policy (time waited between retries grow exponentially, using user put retry number and throttling delay), es default exponential policy (time waited between retries grow exponentially, using es default parameters)", "defaultExponentialBackoff", "", ""
   "**throttling.delay**", "number of time we should wait between each retry (in milliseconds)", "", "500", "", ""
   "**num.retry**", "number of time we should try to inject a bulk into es", "", "3", "", ""
   "batch.size", "The preferred number of Records to setField to the database in a single transaction", "", "1000", "", ""
   "bulk.size", "bulk size in MB", "", "5", "", ""
   "flush.interval", "flush interval in sec", "", "5", "", ""
   "concurrent.requests", "setConcurrentRequests", "", "2", "", ""
   "**cluster.name**", "Name of the ES cluster (for example, elasticsearch_brew). Defaults to 'elasticsearch'", "", "elasticsearch", "", ""
   "**ping.timeout**", "The ping timeout used to determine when a node is unreachable. For example, 5s (5 seconds). If non-local recommended is 30s", "", "5s", "", ""
   "**sampler.interval**", "How often to sample / ping the nodes listed and connected. For example, 5s (5 seconds). If non-local recommended is 30s.", "", "5s", "", ""
   "username", "Username to access the Elasticsearch cluster", "", "null", "", ""
   "password", "Password to access the Elasticsearch cluster", "", "null", "**true**", ""
   "shield.location", "Specifies the path to the JAR for the Elasticsearch Shield plugin. If the Elasticsearch cluster has been secured with the Shield plugin, then the Shield plugin JAR must also be available to this processor. Note: Do NOT place the Shield JAR into NiFi's lib/ directory, doing so will prevent the Shield plugin from being loaded.", "", "null", "", ""
   "**hosts**", "ElasticSearch Hosts, which should be comma separated and colon for hostname/port host1:port,host2:port,....  For example testcluster:9300.", "", "null", "", ""
   "ssl.context.service", "The SSL Context Service used to provide client certificate information for TLS/SSL connections. This service only applies if the Shield plugin is available.", "", "null", "", ""
   "**charset**", "Specifies the character set of the document data.", "", "UTF-8", "", ""

----------

.. _com.hurence.logisland.service.elasticsearch.Elasticsearch_5_4_0_ClientService: 

Elasticsearch_5_4_0_ClientService
---------------------------------
Implementation of ElasticsearchClientService for Elasticsearch 5.4.0.

Class
_____
com.hurence.logisland.service.elasticsearch.Elasticsearch_5_4_0_ClientService

Tags
____
elasticsearch, client

Properties
__________
In the list below, the names of required properties appear in **bold**. Any other properties (not in bold) are considered optional. The table also indicates any default values
, and whether a property is considered "sensitive", meaning that its value will be encrypted. Before entering a value in a sensitive property, ensure that the **logisland.properties** file has an entry for the property **logisland.sensitive.props.key**.

.. csv-table:: allowable-values
   :header: "Name","Description","Allowable Values","Default Value","Sensitive","EL"
   :widths: 20,60,30,20,10,10

   "**backoff.policy**", "strategy for retrying to execute requests in bulkRequest", "No retry policy (when a request fail there won't be any retry.), wait a fixed amount of time between retries (wait a fixed amount of time between retries, using user put retry number and throttling delay), custom exponential policy (time waited between retries grow exponentially, using user put retry number and throttling delay), es default exponential policy (time waited between retries grow exponentially, using es default parameters)", "defaultExponentialBackoff", "", ""
   "**throttling.delay**", "number of time we should wait between each retry (in milliseconds)", "", "500", "", ""
   "**num.retry**", "number of time we should try to inject a bulk into es", "", "3", "", ""
   "batch.size", "The preferred number of Records to setField to the database in a single transaction", "", "1000", "", ""
   "bulk.size", "bulk size in MB", "", "5", "", ""
   "flush.interval", "flush interval in sec", "", "5", "", ""
   "concurrent.requests", "setConcurrentRequests", "", "2", "", ""
   "**cluster.name**", "Name of the ES cluster (for example, elasticsearch_brew). Defaults to 'elasticsearch'", "", "elasticsearch", "", ""
   "**ping.timeout**", "The ping timeout used to determine when a node is unreachable. For example, 5s (5 seconds). If non-local recommended is 30s", "", "5s", "", ""
   "**sampler.interval**", "How often to sample / ping the nodes listed and connected. For example, 5s (5 seconds). If non-local recommended is 30s.", "", "5s", "", ""
   "username", "Username to access the Elasticsearch cluster", "", "null", "", ""
   "password", "Password to access the Elasticsearch cluster", "", "null", "**true**", ""
   "shield.location", "Specifies the path to the JAR for the Elasticsearch Shield plugin. If the Elasticsearch cluster has been secured with the Shield plugin, then the Shield plugin JAR must also be available to this processor. Note: Do NOT place the Shield JAR into NiFi's lib/ directory, doing so will prevent the Shield plugin from being loaded.", "", "null", "", ""
   "**hosts**", "ElasticSearch Hosts, which should be comma separated and colon for hostname/port host1:port,host2:port,....  For example testcluster:9300.", "", "null", "", ""
   "ssl.context.service", "The SSL Context Service used to provide client certificate information for TLS/SSL connections. This service only applies if the Shield plugin is available.", "", "null", "", ""
   "**charset**", "Specifies the character set of the document data.", "", "UTF-8", "", ""

----------

.. _com.hurence.logisland.processor.datastore.EnrichRecords: 

EnrichRecords
-------------
Enrich input records with content indexed in datastore using multiget queries.
Each incoming record must be possibly enriched with information stored in datastore. 
The plugin properties are :
- es.index (String)            : Name of the datastore index on which the multiget query will be performed. This field is mandatory and should not be empty, otherwise an error output record is sent for this specific incoming record.
- record.key (String)          : Name of the field in the input record containing the id to lookup document in elastic search. This field is mandatory.
- es.key (String)              : Name of the datastore key on which the multiget query will be performed. This field is mandatory.
- includes (ArrayList<String>) : List of patterns to filter in (include) fields to retrieve. Supports wildcards. This field is not mandatory.
- excludes (ArrayList<String>) : List of patterns to filter out (exclude) fields to retrieve. Supports wildcards. This field is not mandatory.

Each outcoming record holds at least the input record plus potentially one or more fields coming from of one datastore document.

Class
_____
com.hurence.logisland.processor.datastore.EnrichRecords

Tags
____
datastore, enricher

Properties
__________
In the list below, the names of required properties appear in **bold**. Any other properties (not in bold) are considered optional. The table also indicates any default values
, and whether a property supports the  `Expression Language <expression-language.html>`_ .

.. csv-table:: allowable-values
   :header: "Name","Description","Allowable Values","Default Value","Sensitive","EL"
   :widths: 20,60,30,20,10,10

   "**datastore.client.service**", "The instance of the Controller Service to use for accessing datastore.", "", "null", "", ""
   "record.key", "The name of field in the input record containing the document id to use in ES multiget query", "", "null", "", "**true**"
   "includes.field", "The name of the ES fields to include in the record.", "", "*", "", "**true**"
   "excludes.field", "The name of the ES fields to exclude.", "", "N/A", "", ""
   "type.name", "The typle of record to look for", "", "null", "", "**true**"
   "collection.name", "The name of the collection to look for", "", "null", "", "**true**"

----------

.. _com.hurence.logisland.processor.elasticsearch.EnrichRecordsElasticsearch: 

EnrichRecordsElasticsearch
--------------------------
Enrich input records with content indexed in elasticsearch using multiget queries.
Each incoming record must be possibly enriched with information stored in elasticsearch. 
The plugin properties are :
- es.index (String)            : Name of the elasticsearch index on which the multiget query will be performed. This field is mandatory and should not be empty, otherwise an error output record is sent for this specific incoming record.
- record.key (String)          : Name of the field in the input record containing the id to lookup document in elastic search. This field is mandatory.
- es.key (String)              : Name of the elasticsearch key on which the multiget query will be performed. This field is mandatory.
- includes (ArrayList<String>) : List of patterns to filter in (include) fields to retrieve. Supports wildcards. This field is not mandatory.
- excludes (ArrayList<String>) : List of patterns to filter out (exclude) fields to retrieve. Supports wildcards. This field is not mandatory.

Each outcoming record holds at least the input record plus potentially one or more fields coming from of one elasticsearch document.

Class
_____
com.hurence.logisland.processor.elasticsearch.EnrichRecordsElasticsearch

Tags
____
elasticsearch

Properties
__________
In the list below, the names of required properties appear in **bold**. Any other properties (not in bold) are considered optional. The table also indicates any default values
, and whether a property supports the  `Expression Language <expression-language.html>`_ .

.. csv-table:: allowable-values
   :header: "Name","Description","Allowable Values","Default Value","Sensitive","EL"
   :widths: 20,60,30,20,10,10

   "**elasticsearch.client.service**", "The instance of the Controller Service to use for accessing Elasticsearch.", "", "null", "", ""
   "**record.key**", "The name of field in the input record containing the document id to use in ES multiget query", "", "null", "", "**true**"
   "**es.index**", "The name of the ES index to use in multiget query. ", "", "null", "", "**true**"
   "es.type", "The name of the ES type to use in multiget query.", "", "default", "", "**true**"
   "es.includes.field", "The name of the ES fields to include in the record.", "", "*", "", "**true**"
   "es.excludes.field", "The name of the ES fields to exclude.", "", "N/A", "", ""

----------

.. _com.hurence.logisland.processor.EvaluateJsonPath: 

EvaluateJsonPath
----------------
Evaluates one or more JsonPath expressions against the content of a FlowFile. The results of those expressions are assigned to Records Fields depending on configuration of the Processor. JsonPaths are entered by adding user-defined properties; the name of the property maps to the Field Name into which the result will be placed. The value of the property must be a valid JsonPath expression. A Return Type of 'auto-detect' will make a determination based off the configured destination. If the JsonPath evaluates to a JSON array or JSON object and the Return Type is set to 'scalar' the Record will be routed to error. A Return Type of JSON can return scalar values if the provided JsonPath evaluates to the specified value. If the expression matches nothing, Fields will be created with empty strings as the value 

Class
_____
com.hurence.logisland.processor.EvaluateJsonPath

Tags
____
JSON, evaluate, JsonPath

Properties
__________
This component has no required or optional properties.

Dynamic Properties
__________________
Dynamic Properties allow the user to specify both the name and value of a property.

.. csv-table:: dynamic-properties
   :header: "Name","Value","Description","EL"
   :widths: 20,20,40,10

   "A Record field", "A JsonPath expression", "will be set to any JSON objects that match the JsonPath. ", ""

----------

.. _com.hurence.logisland.processor.FilterRecords: 

FilterRecords
-------------
Keep only records based on a given field value

Class
_____
com.hurence.logisland.processor.FilterRecords

Tags
____
record, fields, remove, delete

Properties
__________
In the list below, the names of required properties appear in **bold**. Any other properties (not in bold) are considered optional. The table also indicates any default values
.

.. csv-table:: allowable-values
   :header: "Name","Description","Allowable Values","Default Value","Sensitive","EL"
   :widths: 20,60,30,20,10,10

   "**field.name**", "the field name", "", "record_id", "", ""
   "**field.value**", "the field value to keep", "", "null", "", ""

----------

.. _com.hurence.logisland.processor.FlatMap: 

FlatMap
-------
Converts each field records into a single flatten record
...

Class
_____
com.hurence.logisland.processor.FlatMap

Tags
____
record, fields, flatmap, flatten

Properties
__________
In the list below, the names of required properties appear in **bold**. Any other properties (not in bold) are considered optional. The table also indicates any default values
.

.. csv-table:: allowable-values
   :header: "Name","Description","Allowable Values","Default Value","Sensitive","EL"
   :widths: 20,60,30,20,10,10

   "keep.root.record", "do we add the original record in", "", "true", "", ""
   "copy.root.record.fields", "do we copy the original record fields into the flattened records", "", "true", "", ""
   "leaf.record.type", "the new type for the flattened records if present", "", "", "", ""
   "concat.fields", "comma separated list of fields to apply concatenation ex : $rootField/$leaffield", "", "null", "", ""
   "concat.separator", "returns $rootField/$leaf/field", "", "/", "", ""
   "include.position", "do we add the original record position in", "", "true", "", ""

----------

.. _com.hurence.logisland.processor.GenerateRandomRecord: 

GenerateRandomRecord
--------------------
This is a processor that make random records given an Avro schema

Class
_____
com.hurence.logisland.processor.GenerateRandomRecord

Tags
____
record, avro, generator

Properties
__________
In the list below, the names of required properties appear in **bold**. Any other properties (not in bold) are considered optional. The table also indicates any default values
.

.. csv-table:: allowable-values
   :header: "Name","Description","Allowable Values","Default Value","Sensitive","EL"
   :widths: 20,60,30,20,10,10

   "**avro.output.schema**", "the avro schema definition for the output serialization", "", "null", "", ""
   "**min.events.count**", "the minimum number of generated events each run", "", "10", "", ""
   "**max.events.count**", "the maximum number of generated events each run", "", "200", "", ""

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

.. _com.hurence.logisland.processor.ModifyId: 

ModifyId
--------
modify id of records or generate it following defined rules

Class
_____
com.hurence.logisland.processor.ModifyId

Tags
____
record, id, idempotent, generate, modify

Properties
__________
In the list below, the names of required properties appear in **bold**. Any other properties (not in bold) are considered optional. The table also indicates any default values
.

.. csv-table:: allowable-values
   :header: "Name","Description","Allowable Values","Default Value","Sensitive","EL"
   :widths: 20,60,30,20,10,10

   "**id.generation.strategy**", "the strategy to generate new Id", "generate a random uid (generate a randomUid using java library), generate a hash from fields (generate a hash from fields), generate a string from java pattern and fields (generate a string from java pattern and fields), generate a concatenation of type, time and a hash from fields (generate a concatenation of type, time and a hash from fields (as for generate_hash strategy))", "randomUuid", "", ""
   "**fields.to.hash**", "the comma separated list of field names (e.g. : 'policyid,date_raw'", "", "record_value", "", ""
   "**hash.charset**", "the charset to use to hash id string (e.g. 'UTF-8')", "", "UTF-8", "", ""
   "**hash.algorithm**", "the algorithme to use to hash id string (e.g. 'SHA-256'", "SHA-384, SHA-224, SHA-256, MD2, SHA, SHA-512, MD5", "SHA-256", "", ""
   "java.formatter.string", "the format to use to build id string (e.g. '%4$2s %3$2s %2$2s %1$2s' (see java Formatter)", "", "null", "", ""
   "**language.tag**", "the language to use to format numbers in string", "aa, ab, ae, af, ak, am, an, ar, as, av, ay, az, ba, be, bg, bh, bi, bm, bn, bo, br, bs, ca, ce, ch, co, cr, cs, cu, cv, cy, da, de, dv, dz, ee, el, en, eo, es, et, eu, fa, ff, fi, fj, fo, fr, fy, ga, gd, gl, gn, gu, gv, ha, he, hi, ho, hr, ht, hu, hy, hz, ia, id, ie, ig, ii, ik, in, io, is, it, iu, iw, ja, ji, jv, ka, kg, ki, kj, kk, kl, km, kn, ko, kr, ks, ku, kv, kw, ky, la, lb, lg, li, ln, lo, lt, lu, lv, mg, mh, mi, mk, ml, mn, mo, mr, ms, mt, my, na, nb, nd, ne, ng, nl, nn, no, nr, nv, ny, oc, oj, om, or, os, pa, pi, pl, ps, pt, qu, rm, rn, ro, ru, rw, sa, sc, sd, se, sg, si, sk, sl, sm, sn, so, sq, sr, ss, st, su, sv, sw, ta, te, tg, th, ti, tk, tl, tn, to, tr, ts, tt, tw, ty, ug, uk, ur, uz, ve, vi, vo, wa, wo, xh, yi, yo, za, zh, zu", "en", "", ""

----------

.. _com.hurence.logisland.service.mongodb.MongoDBControllerService: 

MongoDBControllerService
------------------------
Provides a controller service that wraps most of the functionality of the MongoDB driver.

Class
_____
com.hurence.logisland.service.mongodb.MongoDBControllerService

Tags
____
mongo, mongodb, service

Properties
__________
In the list below, the names of required properties appear in **bold**. Any other properties (not in bold) are considered optional. The table also indicates any default values
, and whether a property supports the  `Expression Language <expression-language.html>`_ .

.. csv-table:: allowable-values
   :header: "Name","Description","Allowable Values","Default Value","Sensitive","EL"
   :widths: 20,60,30,20,10,10

   "**Mongo URI**", "MongoURI, typically of the form: mongodb://host1[:port1][,host2[:port2],...]", "", "null", "", "**true**"
   "**Mongo Database Name**", "The name of the database to use", "", "null", "", "**true**"
   "**Mongo Collection Name**", "The name of the collection to use", "", "null", "", "**true**"
   "batch.size", "The preferred number of Records to setField to the database in a single transaction", "", "1000", "", ""
   "bulk.size", "bulk size in MB", "", "5", "", ""
   "bulk.mode", "Bulk mode (insert or upsert)", "Insert (Insert records whose key must be unique), Insert or Update (Insert records if not already existing or update the record if already existing)", "insert", "", ""
   "flush.interval", "flush interval in ms", "", "500", "", ""
   "**Write Concern**", "The write concern to use", "ACKNOWLEDGED, UNACKNOWLEDGED, FSYNCED, JOURNALED, REPLICA_ACKNOWLEDGED, MAJORITY", "ACKNOWLEDGED", "", ""

----------

.. _com.hurence.logisland.processor.datastore.MultiGet: 

MultiGet
--------
Retrieves a content from datastore using datastore multiget queries.
Each incoming record contains information regarding the datastore multiget query that will be performed. This information is stored in record fields whose names are configured in the plugin properties (see below) :
- collection (String) : name of the datastore collection on which the multiget query will be performed. This field is mandatory and should not be empty, otherwise an error output record is sent for this specific incoming record.
- type (String) : name of the datastore type on which the multiget query will be performed. This field is not mandatory.
- ids (String) : comma separated list of document ids to fetch. This field is mandatory and should not be empty, otherwise an error output record is sent for this specific incoming record.
- includes (String) : comma separated list of patterns to filter in (include) fields to retrieve. Supports wildcards. This field is not mandatory.
- excludes (String) : comma separated list of patterns to filter out (exclude) fields to retrieve. Supports wildcards. This field is not mandatory.

Each outcoming record holds data of one datastore retrieved document. This data is stored in these fields :
- collection (same field name as the incoming record) : name of the datastore collection.
- type (same field name as the incoming record) : name of the datastore type.
- id (same field name as the incoming record) : retrieved document id.
- a list of String fields containing :
   * field name : the retrieved field name
   * field value : the retrieved field value

Class
_____
com.hurence.logisland.processor.datastore.MultiGet

Tags
____
datastore, get, multiget

Properties
__________
In the list below, the names of required properties appear in **bold**. Any other properties (not in bold) are considered optional. The table also indicates any default values
.

.. csv-table:: allowable-values
   :header: "Name","Description","Allowable Values","Default Value","Sensitive","EL"
   :widths: 20,60,30,20,10,10

   "**datastore.client.service**", "The instance of the Controller Service to use for accessing datastore.", "", "null", "", ""
   "**collection.field**", "the name of the incoming records field containing es collection name to use in multiget query. ", "", "null", "", ""
   "**type.field**", "the name of the incoming records field containing es type name to use in multiget query", "", "null", "", ""
   "**ids.field**", "the name of the incoming records field containing es document Ids to use in multiget query", "", "null", "", ""
   "**includes.field**", "the name of the incoming records field containing es includes to use in multiget query", "", "null", "", ""
   "**excludes.field**", "the name of the incoming records field containing es excludes to use in multiget query", "", "null", "", ""

----------

.. _com.hurence.logisland.processor.elasticsearch.MultiGetElasticsearch: 

MultiGetElasticsearch
---------------------
Retrieves a content indexed in elasticsearch using elasticsearch multiget queries.
Each incoming record contains information regarding the elasticsearch multiget query that will be performed. This information is stored in record fields whose names are configured in the plugin properties (see below) :
- index (String) : name of the elasticsearch index on which the multiget query will be performed. This field is mandatory and should not be empty, otherwise an error output record is sent for this specific incoming record.
- type (String) : name of the elasticsearch type on which the multiget query will be performed. This field is not mandatory.
- ids (String) : comma separated list of document ids to fetch. This field is mandatory and should not be empty, otherwise an error output record is sent for this specific incoming record.
- includes (String) : comma separated list of patterns to filter in (include) fields to retrieve. Supports wildcards. This field is not mandatory.
- excludes (String) : comma separated list of patterns to filter out (exclude) fields to retrieve. Supports wildcards. This field is not mandatory.

Each outcoming record holds data of one elasticsearch retrieved document. This data is stored in these fields :
- index (same field name as the incoming record) : name of the elasticsearch index.
- type (same field name as the incoming record) : name of the elasticsearch type.
- id (same field name as the incoming record) : retrieved document id.
- a list of String fields containing :
   * field name : the retrieved field name
   * field value : the retrieved field value

Class
_____
com.hurence.logisland.processor.elasticsearch.MultiGetElasticsearch

Tags
____
elasticsearch

Properties
__________
In the list below, the names of required properties appear in **bold**. Any other properties (not in bold) are considered optional. The table also indicates any default values
.

.. csv-table:: allowable-values
   :header: "Name","Description","Allowable Values","Default Value","Sensitive","EL"
   :widths: 20,60,30,20,10,10

   "**elasticsearch.client.service**", "The instance of the Controller Service to use for accessing Elasticsearch.", "", "null", "", ""
   "**es.index.field**", "the name of the incoming records field containing es index name to use in multiget query. ", "", "null", "", ""
   "**es.type.field**", "the name of the incoming records field containing es type name to use in multiget query", "", "null", "", ""
   "**es.ids.field**", "the name of the incoming records field containing es document Ids to use in multiget query", "", "null", "", ""
   "**es.includes.field**", "the name of the incoming records field containing es includes to use in multiget query", "", "null", "", ""
   "**es.excludes.field**", "the name of the incoming records field containing es excludes to use in multiget query", "", "null", "", ""

----------

.. _com.hurence.logisland.processor.NormalizeFields: 

NormalizeFields
---------------
Changes the name of a field according to a provided name mapping
...

Class
_____
com.hurence.logisland.processor.NormalizeFields

Tags
____
record, fields, normalizer

Properties
__________
In the list below, the names of required properties appear in **bold**. Any other properties (not in bold) are considered optional. The table also indicates any default values
.

.. csv-table:: allowable-values
   :header: "Name","Description","Allowable Values","Default Value","Sensitive","EL"
   :widths: 20,60,30,20,10,10

   "**conflict.resolution.policy**", "what to do when a field with the same name already exists ?", "nothing to do (leave record as it was), overwrite existing field (if field already exist), keep only old field and delete the other (keep only old field and delete the other), keep old field and new one (creates an alias for the new field)", "do_nothing", "", ""

Dynamic Properties
__________________
Dynamic Properties allow the user to specify both the name and value of a property.

.. csv-table:: dynamic-properties
   :header: "Name","Value","Description","EL"
   :widths: 20,20,40,10

   "alternative mapping", "a comma separated list of possible field name", "when a field has a name contained in the list it will be renamed with this property field name", **true**

----------

.. _com.hurence.logisland.processor.commonlogs.gitlab.ParseGitlabLog: 

ParseGitlabLog
--------------
The Gitlab logs processor is the Logisland entry point to get and process `Gitlab <https://www.gitlab.com>`_ logs. This allows for instance to monitor activities in your Gitlab server. The expected input of this processor are records from the production_json.log log file of Gitlab which contains JSON records. You can for instance use the `kafkacat <https://github.com/edenhill/kafkacat>`_ command to inject those logs into kafka and thus Logisland.

Class
_____
com.hurence.logisland.processor.commonlogs.gitlab.ParseGitlabLog

Tags
____
logs, gitlab

Properties
__________
In the list below, the names of required properties appear in **bold**. Any other properties (not in bold) are considered optional. The table also indicates any default values
.

.. csv-table:: allowable-values
   :header: "Name","Description","Allowable Values","Default Value","Sensitive","EL"
   :widths: 20,60,30,20,10,10

   "debug", "Enable debug. If enabled, the original JSON string is embedded in the record_value field of the record.", "", "false", "", ""

----------

.. _com.hurence.logisland.processor.ParseProperties: 

ParseProperties
---------------
Parse a field made of key=value fields separated by spaces
a string like "a=1 b=2 c=3" will add a,b & c fields, respectively with values 1,2 & 3 to the current Record

Class
_____
com.hurence.logisland.processor.ParseProperties

Tags
____
record, properties, parser

Properties
__________
In the list below, the names of required properties appear in **bold**. Any other properties (not in bold) are considered optional. The table also indicates any default values
.

.. csv-table:: allowable-values
   :header: "Name","Description","Allowable Values","Default Value","Sensitive","EL"
   :widths: 20,60,30,20,10,10

   "**properties.field**", "the field containing the properties to split and treat", "", "null", "", ""

----------

.. _com.hurence.logisland.redis.service.RedisKeyValueCacheService: 

RedisKeyValueCacheService
-------------------------
A controller service for caching records by key value pair with LRU (last recently used) strategy. using LinkedHashMap

Class
_____
com.hurence.logisland.redis.service.RedisKeyValueCacheService

Tags
____
cache, service, key, value, pair, redis

Properties
__________
In the list below, the names of required properties appear in **bold**. Any other properties (not in bold) are considered optional. The table also indicates any default values
, and whether a property is considered "sensitive", meaning that its value will be encrypted. Before entering a value in a sensitive property, ensure that the **logisland.properties** file has an entry for the property **logisland.sensitive.props.key**.

.. csv-table:: allowable-values
   :header: "Name","Description","Allowable Values","Default Value","Sensitive","EL"
   :widths: 20,60,30,20,10,10

   "**Redis Mode**", "The type of Redis being communicated with - standalone, sentinel, or clustered.", "standalone (A single standalone Redis instance.), sentinel (Redis Sentinel which provides high-availability. Described further at https://redis.io/topics/sentinel), cluster (Clustered Redis which provides sharding and replication. Described further at https://redis.io/topics/cluster-spec)", "standalone", "", ""
   "**Connection String**", "The connection string for Redis. In a standalone instance this value will be of the form hostname:port. In a sentinel instance this value will be the comma-separated list of sentinels, such as host1:port1,host2:port2,host3:port3. In a clustered instance this value will be the comma-separated list of cluster masters, such as host1:port,host2:port,host3:port.", "", "null", "", ""
   "**Database Index**", "The database index to be used by connections created from this connection pool. See the databases property in redis.conf, by default databases 0-15 will be available.", "", "0", "", ""
   "**Communication Timeout**", "The timeout to use when attempting to communicate with Redis.", "", "10 seconds", "", ""
   "**Cluster Max Redirects**", "The maximum number of redirects that can be performed when clustered.", "", "5", "", ""
   "Sentinel Master", "The name of the sentinel master, require when Mode is set to Sentinel", "", "null", "", ""
   "Password", "The password used to authenticate to the Redis server. See the requirepass property in redis.conf.", "", "null", "**true**", ""
   "**Pool - Max Total**", "The maximum number of connections that can be allocated by the pool (checked out to clients, or idle awaiting checkout). A negative value indicates that there is no limit.", "", "8", "", ""
   "**Pool - Max Idle**", "The maximum number of idle connections that can be held in the pool, or a negative value if there is no limit.", "", "8", "", ""
   "**Pool - Min Idle**", "The target for the minimum number of idle connections to maintain in the pool. If the configured value of Min Idle is greater than the configured value for Max Idle, then the value of Max Idle will be used instead.", "", "0", "", ""
   "**Pool - Block When Exhausted**", "Whether or not clients should block and wait when trying to obtain a connection from the pool when the pool has no available connections. Setting this to false means an error will occur immediately when a client requests a connection and none are available.", "true, false", "true", "", ""
   "**Pool - Max Wait Time**", "The amount of time to wait for an available connection when Block When Exhausted is set to true.", "", "10 seconds", "", ""
   "**Pool - Min Evictable Idle Time**", "The minimum amount of time an object may sit idle in the pool before it is eligible for eviction.", "", "60 seconds", "", ""
   "**Pool - Time Between Eviction Runs**", "The amount of time between attempting to evict idle connections from the pool.", "", "30 seconds", "", ""
   "**Pool - Num Tests Per Eviction Run**", "The number of connections to tests per eviction attempt. A negative value indicates to test all connections.", "", "-1", "", ""
   "**Pool - Test On Create**", "Whether or not connections should be tested upon creation.", "true, false", "false", "", ""
   "**Pool - Test On Borrow**", "Whether or not connections should be tested upon borrowing from the pool.", "true, false", "false", "", ""
   "**Pool - Test On Return**", "Whether or not connections should be tested upon returning to the pool.", "true, false", "false", "", ""
   "**Pool - Test While Idle**", "Whether or not connections should be tested while idle.", "true, false", "true", "", ""
   "**record.recordSerializer**", "the way to serialize/deserialize the record", "kryo serialization (serialize events as json blocs), avro serialization (serialize events as json blocs), avro serialization (serialize events as avro blocs), byte array serialization (serialize events as byte arrays), Kura Protobuf serialization (serialize events as Kura protocol buffer), no serialization (send events as bytes)", "com.hurence.logisland.serializer.JsonSerializer", "", ""

----------

.. _com.hurence.logisland.processor.RemoveFields: 

RemoveFields
------------
Removes a list of fields defined by a comma separated list of field names

Class
_____
com.hurence.logisland.processor.RemoveFields

Tags
____
record, fields, remove, delete

Properties
__________
In the list below, the names of required properties appear in **bold**. Any other properties (not in bold) are considered optional. The table also indicates any default values
.

.. csv-table:: allowable-values
   :header: "Name","Description","Allowable Values","Default Value","Sensitive","EL"
   :widths: 20,60,30,20,10,10

   "**fields.to.remove**", "the comma separated list of field names (e.g. 'policyid,date_raw'", "", "null", "", ""

----------

.. _com.hurence.logisland.processor.SelectDistinctRecords: 

SelectDistinctRecords
---------------------
Keep only distinct records based on a given field

Class
_____
com.hurence.logisland.processor.SelectDistinctRecords

Tags
____
record, fields, remove, delete

Properties
__________
In the list below, the names of required properties appear in **bold**. Any other properties (not in bold) are considered optional. The table also indicates any default values
.

.. csv-table:: allowable-values
   :header: "Name","Description","Allowable Values","Default Value","Sensitive","EL"
   :widths: 20,60,30,20,10,10

   "**field.name**", "the field to distinct records", "", "record_id", "", ""

----------

.. _com.hurence.logisland.processor.SendMail: 

SendMail
--------
The SendMail processor is aimed at sending an email (like for instance an alert email) from an incoming record. There are three ways an incoming record can generate an email according to the special fields it must embed. Here is a list of the record fields that generate a mail and how they work:

- **mail_text**: this is the simplest way for generating a mail. If present, this field means to use its content (value) as the payload of the mail to send. The mail is sent in text format if there is only this special field in the record. Otherwise, used with either mail_html or mail_use_template, the content of mail_text is the aletrnative text to the HTML mail that is generated.

- **mail_html**: this field specifies that the mail should be sent as HTML and the value of the field is mail payload. If mail_text is also present, its value is used as the alternative text for the mail. mail_html cannot be used with mail_use_template: only one of those two fields should be present in the record.

- **mail_use_template**: If present, this field specifies that the mail should be sent as HTML and the HTML content is to be generated from the template in the processor configuration key **html.template**. The template can contain parameters which must also be present in the record as fields. See documentation of html.template for further explanations. mail_use_template cannot be used with mail_html: only one of those two fields should be present in the record.

 If **allow_overwrite** configuration key is true, any mail.* (dot format) configuration key may be overwritten with a matching field in the record of the form mail_* (underscore format). For instance if allow_overwrite is true and mail.to is set to config_address@domain.com, a record generating a mail with a mail_to field set to record_address@domain.com will send a mail to record_address@domain.com.

 Apart from error records (when he is unable to process the incoming record or to send the mail), this processor is not expected to produce any output records.

Class
_____
com.hurence.logisland.processor.SendMail

Tags
____
smtp, email, e-mail, mail, mailer, sendmail, message, alert, html

Properties
__________
In the list below, the names of required properties appear in **bold**. Any other properties (not in bold) are considered optional. The table also indicates any default values
.

.. csv-table:: allowable-values
   :header: "Name","Description","Allowable Values","Default Value","Sensitive","EL"
   :widths: 20,60,30,20,10,10

   "debug", "Enable debug. If enabled, debug information are written to stdout.", "", "false", "", ""
   "**smtp.server**", "FQDN, hostname or IP address of the SMTP server to use.", "", "null", "", ""
   "smtp.port", "TCP port number of the SMTP server to use.", "", "25", "", ""
   "smtp.security.username", "SMTP username.", "", "null", "", ""
   "smtp.security.password", "SMTP password.", "", "null", "", ""
   "smtp.security.ssl", "Use SSL under SMTP or not (SMTPS). Default is false.", "", "false", "", ""
   "**mail.from.address**", "Valid mail sender email address.", "", "null", "", ""
   "mail.from.name", "Mail sender name.", "", "null", "", ""
   "**mail.bounce.address**", "Valid bounce email address (where error mail is sent if the mail is refused by the recipient server).", "", "null", "", ""
   "mail.replyto.address", "Reply to email address.", "", "null", "", ""
   "mail.subject", "Mail subject.", "", "[LOGISLAND] Automatic email", "", ""
   "mail.to", "Comma separated list of email recipients. If not set, the record must have a mail_to field and allow_overwrite configuration key should be true.", "", "null", "", ""
   "allow_overwrite", "If true, allows to overwrite processor configuration with special record fields (mail_to, mail_from_address, mail_from_name, mail_bounce_address, mail_replyto_address, mail_subject). If false, special record fields are ignored and only processor configuration keys are used.", "", "true", "", ""
   "html.template", "HTML template to use. It is used when the incoming record contains a mail_use_template field. The template may contain some parameters. The parameter format in the template is of the form ${xxx}. For instance ${param_user} in the template means that a field named param_user must be present in the record and its value will replace the ${param_user} string in the HTML template when the mail will be sent. If some parameters are declared in the template, everyone of them must be present in the record as fields, otherwise the record will generate an error record. If an incoming record contains a mail_use_template field, a template must be present in the configuration and the HTML mail format will be used. If the record also contains a mail_text field, its content will be used as an alternative text message to be used in the mail reader program of the recipient if it does not supports HTML.", "", "null", "", ""

----------

.. _com.hurence.logisland.processor.SplitField: 

SplitField
----------
This processor is used to create a new set of fields from one field (using split).

Class
_____
com.hurence.logisland.processor.SplitField

Tags
____
parser, split, log, record

Properties
__________
In the list below, the names of required properties appear in **bold**. Any other properties (not in bold) are considered optional. The table also indicates any default values
.

.. csv-table:: allowable-values
   :header: "Name","Description","Allowable Values","Default Value","Sensitive","EL"
   :widths: 20,60,30,20,10,10

   "conflict.resolution.policy", "What to do when a field with the same name already exists ?", "overwrite existing field (if field already exist), keep only old field (keep only old field)", "keep_only_old_field", "", ""
   "split.limit", "Specify the maximum number of split to allow", "", "10", "", ""
   "split.counter.enable", "Enable the counter of items returned by the split", "", "false", "", ""
   "split.counter.suffix", "Enable the counter of items returned by the split", "", "Counter", "", ""

Dynamic Properties
__________________
Dynamic Properties allow the user to specify both the name and value of a property.

.. csv-table:: dynamic-properties
   :header: "Name","Value","Description","EL"
   :widths: 20,20,40,10

   "alternative split field", "another split that could match", "This processor is used to create a new set of fields from one field (using split).", **true**

See Also:
_________
`com.hurence.logisland.processor.SplitField`_ 

----------

.. _com.hurence.logisland.processor.SplitText: 

SplitText
---------
This is a processor that is used to split a String into fields according to a given Record mapping

Class
_____
com.hurence.logisland.processor.SplitText

Tags
____
parser, regex, log, record

Properties
__________
In the list below, the names of required properties appear in **bold**. Any other properties (not in bold) are considered optional. The table also indicates any default values
.

.. csv-table:: allowable-values
   :header: "Name","Description","Allowable Values","Default Value","Sensitive","EL"
   :widths: 20,60,30,20,10,10

   "**value.regex**", "the regex to match for the message value", "", "null", "", ""
   "**value.fields**", "a comma separated list of fields corresponding to matching groups for the message value", "", "null", "", ""
   "key.regex", "the regex to match for the message key", "", ".*", "", ""
   "key.fields", "a comma separated list of fields corresponding to matching groups for the message key", "", "record_key", "", ""
   "record.type", "default type of record", "", "record", "", ""
   "keep.raw.content", "do we add the initial raw content ?", "", "true", "", ""
   "timezone.record.time", "what is the time zone of the string formatted date for 'record_time' field.", "", "UTC", "", ""

Dynamic Properties
__________________
Dynamic Properties allow the user to specify both the name and value of a property.

.. csv-table:: dynamic-properties
   :header: "Name","Value","Description","EL"
   :widths: 20,20,40,10

   "alternative regex & mapping", "another regex that could match", "this regex will be tried if the main one has not matched. It must be in the form alt.value.regex.1 and alt.value.fields.1", **true**

See Also:
_________
`com.hurence.logisland.processor.SplitTextMultiline`_ 

----------

.. _com.hurence.logisland.processor.SplitTextMultiline: 

SplitTextMultiline
------------------
No description provided.

Class
_____
com.hurence.logisland.processor.SplitTextMultiline

Tags
____
None.

Properties
__________
In the list below, the names of required properties appear in **bold**. Any other properties (not in bold) are considered optional. The table also indicates any default values
.

.. csv-table:: allowable-values
   :header: "Name","Description","Allowable Values","Default Value","Sensitive","EL"
   :widths: 20,60,30,20,10,10

   "**regex**", "the regex to match", "", "null", "", ""
   "**fields**", "a comma separated list of fields corresponding to matching groups", "", "null", "", ""
   "**event.type**", "the type of event", "", "null", "", ""

----------

.. _com.hurence.logisland.processor.SplitTextWithProperties: 

SplitTextWithProperties
-----------------------
This is a processor that is used to split a String into fields according to a given Record mapping

Class
_____
com.hurence.logisland.processor.SplitTextWithProperties

Tags
____
parser, regex, log, record

Properties
__________
In the list below, the names of required properties appear in **bold**. Any other properties (not in bold) are considered optional. The table also indicates any default values
.

.. csv-table:: allowable-values
   :header: "Name","Description","Allowable Values","Default Value","Sensitive","EL"
   :widths: 20,60,30,20,10,10

   "**value.regex**", "the regex to match for the message value", "", "null", "", ""
   "**value.fields**", "a comma separated list of fields corresponding to matching groups for the message value", "", "null", "", ""
   "key.regex", "the regex to match for the message key", "", ".*", "", ""
   "key.fields", "a comma separated list of fields corresponding to matching groups for the message key", "", "record_key", "", ""
   "record.type", "default type of record", "", "record", "", ""
   "keep.raw.content", "do we add the initial raw content ?", "", "true", "", ""
   "**properties.field**", "the field containing the properties to split and treat", "", "properties", "", ""

Dynamic Properties
__________________
Dynamic Properties allow the user to specify both the name and value of a property.

.. csv-table:: dynamic-properties
   :header: "Name","Value","Description","EL"
   :widths: 20,20,40,10

   "alternative regex & mapping", "another regex that could match", "this regex will be tried if the main one has not matched. It must be in the form alt.value.regex.1 and alt.value.fields.1", **true**

See Also:
_________
`com.hurence.logisland.processor.SplitTextMultiline`_ 
