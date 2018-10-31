Components
==========
You'll find here the list of all usable Processors, Engines, Services and other components that can be usable out of the box in your analytics streams


----------

.. _com.hurence.logisland.processor.AddFields: 

AddFields
---------
No description provided.

Module
______
com.hurence.logisland:logisland-processor-common:1.0.0-RC1

Class
_____
com.hurence.logisland.processor.AddFields

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

   "conflict.resolution.policy", "What to do when a field with the same name already exists ?", "overwrite existing field (if field already exist), keep only old field value (keep only old field)", "keep_only_old_field", "", ""

----------

.. _com.hurence.logisland.processor.ApplyRegexp: 

ApplyRegexp
-----------
No description provided.

Module
______
com.hurence.logisland:logisland-processor-common:1.0.0-RC1

Class
_____
com.hurence.logisland.processor.ApplyRegexp

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

   "conflict.resolution.policy", "What to do when a field with the same name already exists ?", "overwrite existing field (if field already exist), keep only old field (keep only old field)", "keep_only_old_field", "", ""

----------

.. _com.hurence.logisland.processor.elasticsearch.BulkAddElasticsearch: 

BulkAddElasticsearch
--------------------
No description provided.

Module
______
com.hurence.logisland:logisland-processor-elasticsearch:1.0.0-RC1

Class
_____
com.hurence.logisland.processor.elasticsearch.BulkAddElasticsearch

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
No description provided.

Module
______
com.hurence.logisland:logisland-processor-common:1.0.0-RC1

Class
_____
com.hurence.logisland.processor.datastore.BulkPut

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

   "**datastore.client.service**", "The instance of the Controller Service to use for accessing datastore.", "", "null", "", ""
   "**default.collection**", "The name of the collection/index/table to insert into", "", "null", "", "**true**"
   "**timebased.collection**", "do we add a date suffix", "No date (no date added to default index), Today's date (today's date added to default index), yesterday's date (yesterday's date added to default index)", "no", "", ""
   "date.format", "simple date format for date suffix. default : yyyy.MM.dd", "", "yyyy.MM.dd", "", ""
   "collection.field", "the name of the event field containing es index name => will override index value if set", "", "null", "", "**true**"

----------

.. _com.hurence.logisland.service.cassandra.CassandraControllerService: 

CassandraControllerService
--------------------------
No description provided.

Module
______
com.hurence.logisland:logisland-service-cassandra-client:1.0.0-RC1

Class
_____
com.hurence.logisland.service.cassandra.CassandraControllerService

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

   "**Cassandra hosts**", "Cassandra cluster hosts as a comma separated value list", "", "null", "", ""
   "**Cassandra port**", "Cassandra cluster port", "", "null", "", ""
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
No description provided.

Module
______
com.hurence.logisland:logisland-processor-common:1.0.0-RC1

Class
_____
com.hurence.logisland.processor.alerting.CheckAlerts

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

----------

.. _com.hurence.logisland.processor.alerting.CheckThresholds: 

CheckThresholds
---------------
No description provided.

Module
______
com.hurence.logisland:logisland-processor-common:1.0.0-RC1

Class
_____
com.hurence.logisland.processor.alerting.CheckThresholds

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

----------

.. _com.hurence.logisland.processor.alerting.ComputeTags: 

ComputeTags
-----------
No description provided.

Module
______
com.hurence.logisland:logisland-processor-common:1.0.0-RC1

Class
_____
com.hurence.logisland.processor.alerting.ComputeTags

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

----------

.. _com.hurence.logisland.processor.webAnalytics.ConsolidateSession: 

ConsolidateSession
------------------
No description provided.

Module
______
com.hurence.logisland:logisland-processor-web-analytics:1.0.0-RC1

Class
_____
com.hurence.logisland.processor.webAnalytics.ConsolidateSession

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

   "debug", "Enable debug. If enabled, the original JSON string is embedded in the record_value field of the record.", "", "null", "", ""
   "session.timeout", "session timeout in sec", "", "1800", "", ""
   "sessionid.field", "the name of the field containing the session id => will override default value if set", "", "sessionId", "", ""
   "timestamp.field", "the name of the field containing the timestamp => will override default value if set", "", "h2kTimestamp", "", ""
   "visitedpage.field", "the name of the field containing the visited page => will override default value if set", "", "location", "", ""
   "userid.field", "the name of the field containing the userId => will override default value if set", "", "userId", "", ""
   "fields.to.return", "the list of fields to return", "", "null", "", ""
   "firstVisitedPage.out.field", "the name of the field containing the first visited page => will override default value if set", "", "firstVisitedPage", "", ""
   "lastVisitedPage.out.field", "the name of the field containing the last visited page => will override default value if set", "", "lastVisitedPage", "", ""
   "isSessionActive.out.field", "the name of the field stating whether the session is active or not => will override default value if set", "", "is_sessionActive", "", ""
   "sessionDuration.out.field", "the name of the field containing the session duration => will override default value if set", "", "sessionDuration", "", ""
   "eventsCounter.out.field", "the name of the field containing the session duration => will override default value if set", "", "eventsCounter", "", ""
   "firstEventDateTime.out.field", "the name of the field containing the date of the first event => will override default value if set", "", "firstEventDateTime", "", ""
   "lastEventDateTime.out.field", "the name of the field containing the date of the last event => will override default value if set", "", "lastEventDateTime", "", ""
   "sessionInactivityDuration.out.field", "the name of the field containing the session inactivity duration => will override default value if set", "", "sessionInactivityDuration", "", ""

----------

.. _com.hurence.logisland.processor.ConvertFieldsType: 

ConvertFieldsType
-----------------
No description provided.

Module
______
com.hurence.logisland:logisland-processor-common:1.0.0-RC1

Class
_____
com.hurence.logisland.processor.ConvertFieldsType

Tags
____
None.

Properties
__________
This component has no required or optional properties.

----------

.. _com.hurence.logisland.processor.DebugStream: 

DebugStream
-----------
No description provided.

Module
______
com.hurence.logisland:logisland-processor-common:1.0.0-RC1

Class
_____
com.hurence.logisland.processor.DebugStream

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

   "**event.serializer**", "the way to serialize event", "Json serialization (serialize events as json blocs), String serialization (serialize events as toString() blocs)", "json", "", ""
   "record.types", "comma separated list of record to include. all if empty", "", "", "", ""

----------

.. _com.hurence.logisland.processor.DetectOutliers: 

DetectOutliers
--------------
No description provided.

Module
______
com.hurence.logisland:logisland-processor-outlier-detection:1.0.0-RC1

Class
_____
com.hurence.logisland.processor.DetectOutliers

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

   "**value.field**", "the numeric field to get the value", "", "record_value", "", ""
   "**time.field**", "the numeric field to get the value", "", "record_time", "", ""
   "output.record.type", "the output type of the record", "", "alert_match", "", ""
   "**rotation.policy.type**", "...", "by_amount, by_time, never", "by_amount", "", ""
   "**rotation.policy.amount**", "...", "", "100", "", ""
   "**rotation.policy.unit**", "...", "milliseconds, seconds, hours, days, months, years, points", "points", "", ""
   "**chunking.policy.type**", "...", "by_amount, by_time, never", "by_amount", "", ""
   "**chunking.policy.amount**", "...", "", "100", "", ""
   "**chunking.policy.unit**", "...", "milliseconds, seconds, hours, days, months, years, points", "points", "", ""
   "sketchy.outlier.algorithm", "...", "SKETCHY_MOVING_MAD", "SKETCHY_MOVING_MAD", "", ""
   "batch.outlier.algorithm", "...", "RAD", "RAD", "", ""
   "global.statistics.min", "minimum value", "", "null", "", ""
   "global.statistics.max", "maximum value", "", "null", "", ""
   "global.statistics.mean", "mean value", "", "null", "", ""
   "global.statistics.stddev", "standard deviation value", "", "null", "", ""
   "**zscore.cutoffs.normal**", "zscoreCutoffs level for normal outlier", "", "0.000000000000001", "", ""
   "**zscore.cutoffs.moderate**", "zscoreCutoffs level for moderate outlier", "", "1.5", "", ""
   "**zscore.cutoffs.severe**", "zscoreCutoffs level for severe outlier", "", "10.0", "", ""
   "zscore.cutoffs.notEnoughData", "zscoreCutoffs level for notEnoughData outlier", "", "100", "", ""
   "smooth", "do smoothing ?", "", "false", "", ""
   "decay", "the decay", "", "0.1", "", ""
   "**min.amount.to.predict**", "minAmountToPredict", "", "100", "", ""
   "min_zscore_percentile", "minZscorePercentile", "", "50.0", "", ""
   "reservoir_size", "the size of points reservoir", "", "100", "", ""
   "rpca.force.diff", "No Description Provided.", "", "null", "", ""
   "rpca.lpenalty", "No Description Provided.", "", "null", "", ""
   "rpca.min.records", "No Description Provided.", "", "null", "", ""
   "rpca.spenalty", "No Description Provided.", "", "null", "", ""
   "rpca.threshold", "No Description Provided.", "", "null", "", ""

----------

.. _com.hurence.logisland.service.elasticsearch.Elasticsearch_2_4_0_ClientService: 

Elasticsearch_2_4_0_ClientService
---------------------------------
No description provided.

Module
______
com.hurence.logisland:logisland-service-elasticsearch_2_4_0-client:1.0.0-RC1

Class
_____
com.hurence.logisland.service.elasticsearch.Elasticsearch_2_4_0_ClientService

Tags
____
None.

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
No description provided.

Module
______
com.hurence.logisland:logisland-service-elasticsearch_5_4_0-client:1.0.0-RC1

Class
_____
com.hurence.logisland.service.elasticsearch.Elasticsearch_5_4_0_ClientService

Tags
____
None.

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
No description provided.

Module
______
com.hurence.logisland:logisland-processor-common:1.0.0-RC1

Class
_____
com.hurence.logisland.processor.datastore.EnrichRecords

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
No description provided.

Module
______
com.hurence.logisland:logisland-processor-elasticsearch:1.0.0-RC1

Class
_____
com.hurence.logisland.processor.elasticsearch.EnrichRecordsElasticsearch

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
No description provided.

Module
______
com.hurence.logisland:logisland-processor-common:1.0.0-RC1

Class
_____
com.hurence.logisland.processor.EvaluateJsonPath

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

   "**return.type**", "Indicates the desired return type of the JSON Path expressions.  Selecting 'auto-detect' will set the return type to 'json'  or 'scalar' ", "json, scalar", "scalar", "", ""
   "**path.not.found.behavior**", "Indicates how to handle missing JSON path expressions. Selecting 'warn' will generate a warning when a JSON path expression is not found.", "warn, ignore", "ignore", "", ""
   "**Null Value Representation**", "Indicates the desired representation of JSON Path expressions resulting in a null value.", "empty string, the string 'null'", "empty string", "", ""
   "**json.input.field.name**", "the name of the field containing the json string", "", "record_value", "", ""

----------

.. _com.hurence.logisland.processor.excel.ExcelExtract: 

ExcelExtract
------------
No description provided.

Module
______
com.hurence.logisland:logisland-processor-excel:1.0.0-RC1

Class
_____
com.hurence.logisland.processor.excel.ExcelExtract

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

   "Sheets to Extract", "Comma separated list of Excel document sheet names that should be extracted from the excel document. If this property is left blank then all of the sheets will be extracted from the Excel document. You can specify regular expressions. Any sheets not specified in this value will be ignored.", "", "", "", ""
   "Columns To Skip", "Comma delimited list of column numbers to skip. Use the columns number and not the letter designation. Use this to skip over columns anywhere in your worksheet that you don't want extracted as part of the record.", "", "", "", ""
   "Field names mapping", "The comma separated list representing the names of columns of extracted cells. Order matters! You should use either field.names either field.row.header but not both together.", "", "null", "", ""
   "Number of Rows to Skip", "The row number of the first row to start processing.Use this to skip over rows of data at the top of your worksheet that are not part of the dataset.Empty rows of data anywhere in the spreadsheet will always be skipped, no matter what this value is set to.", "", "0", "", ""
   "record.type", "Default type of record", "", "excel_record", "", ""
   "Use a row header as field names mapping", "If set, field names mapping will be extracted from the specified row number. You should use either field.names either field.row.header but not both together.", "", "null", "", ""

----------

.. _com.hurence.logisland.processor.hbase.FetchHBaseRow: 

FetchHBaseRow
-------------
No description provided.

Module
______
com.hurence.logisland:logisland-processor-hbase:1.0.0-RC1

Class
_____
com.hurence.logisland.processor.hbase.FetchHBaseRow

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

   "**hbase.client.service**", "The instance of the Controller Service to use for accessing HBase.", "", "null", "", ""
   "**table.name.field**", "The field containing the name of the HBase Table to fetch from.", "", "null", "", "**true**"
   "**row.identifier.field**", "The field containing the  identifier of the row to fetch.", "", "null", "", "**true**"
   "columns.field", "The field containing an optional comma-separated list of "<colFamily>:<colQualifier>" pairs to fetch. To return all columns for a given family, leave off the qualifier such as "<colFamily1>,<colFamily2>".", "", "null", "", "**true**"
   "record.serializer", "the serializer needed to i/o the record in the HBase row", "kryo serialization (serialize events as json blocs), json serialization (serialize events as json blocs), avro serialization (serialize events as avro blocs), no serialization (send events as bytes)", "com.hurence.logisland.serializer.KryoSerializer", "", ""
   "record.schema", "the avro schema definition for the Avro serialization", "", "null", "", ""
   "table.name.default", "The table table to use if table name field is not set", "", "null", "", ""

----------

.. _com.hurence.logisland.processor.FilterRecords: 

FilterRecords
-------------
No description provided.

Module
______
com.hurence.logisland:logisland-processor-common:1.0.0-RC1

Class
_____
com.hurence.logisland.processor.FilterRecords

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

   "**field.name**", "the field name", "", "record_id", "", ""
   "**field.value**", "the field value to keep", "", "null", "", ""

----------

.. _com.hurence.logisland.processor.FlatMap: 

FlatMap
-------
No description provided.

Module
______
com.hurence.logisland:logisland-processor-common:1.0.0-RC1

Class
_____
com.hurence.logisland.processor.FlatMap

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
No description provided.

Module
______
com.hurence.logisland:logisland-processor-common:1.0.0-RC1

Class
_____
com.hurence.logisland.processor.GenerateRandomRecord

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

   "**avro.output.schema**", "the avro schema definition for the output serialization", "", "null", "", ""
   "**min.events.count**", "the minimum number of generated events each run", "", "10", "", ""
   "**max.events.count**", "the maximum number of generated events each run", "", "200", "", ""

----------

.. _com.hurence.logisland.service.hbase.HBase_1_1_2_ClientService: 

HBase_1_1_2_ClientService
-------------------------
No description provided.

Module
______
com.hurence.logisland:logisland-service-hbase_1_1_2-client:1.0.0-RC1

Class
_____
com.hurence.logisland.service.hbase.HBase_1_1_2_ClientService

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

   "hadoop.configuration.files", "Comma-separated list of Hadoop Configuration files, such as hbase-site.xml and core-site.xml for kerberos, including full paths to the files.", "", "null", "", ""
   "zookeeper.quorum", "Comma-separated list of ZooKeeper hosts for HBase. Required if Hadoop Configuration Files are not provided.", "", "null", "", ""
   "zookeeper.client.port", "The port on which ZooKeeper is accepting client connections. Required if Hadoop Configuration Files are not provided.", "", "null", "", ""
   "zookeeper.znode.parent", "The ZooKeeper ZNode Parent value for HBase (example: /hbase). Required if Hadoop Configuration Files are not provided.", "", "null", "", ""
   "hbase.client.retries", "The number of times the HBase client will retry connecting. Required if Hadoop Configuration Files are not provided.", "", "3", "", ""
   "phoenix.client.jar.location", "The full path to the Phoenix client JAR. Required if Phoenix is installed on top of HBase.", "", "null", "", "**true**"

----------

.. _com.hurence.logisland.processor.enrichment.IpToFqdn: 

IpToFqdn
--------
No description provided.

Module
______
com.hurence.logisland:logisland-processor-enrichment:1.0.0-RC1

Class
_____
com.hurence.logisland.processor.enrichment.IpToFqdn

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

   "**ip.address.field**", "The name of the field containing the ip address to use.", "", "null", "", ""
   "**fqdn.field**", "The field that will contain the full qualified domain name corresponding to the ip address.", "", "null", "", ""
   "overwrite.fqdn.field", "If the field should be overwritten when it already exists.", "", "false", "", ""
   "**cache.service**", "The name of the cache service to use.", "", "null", "", ""
   "cache.max.time", "The amount of time, in seconds, for which a cached FQDN value is valid in the cache service. After this delay, the next new request to translate the same IP into FQDN will trigger a new reverse DNS request and the result will overwrite the entry in the cache. This allows two things: if the IP was not resolved into a FQDN, this will get a chance to obtain a FQDN if the DNS system has been updated, if the IP is resolved into a FQDN, this will allow to be more accurate if the DNS system has been updated.  A value of 0 seconds disables this expiration mechanism. The default value is 84600 seconds, which corresponds to new requests triggered every day if a record with the same IP passes every day in the processor.", "", "84600", "", ""
   "resolution.timeout", "The amount of time, in milliseconds, to wait at most for the resolution to occur. This avoids to block the stream for too much time. Default value is 1000ms. If the delay expires and no resolution could occur before, the FQDN field is not created. A special value of 0 disables the logisland timeout and the resolution request may last for many seconds if the IP cannot be translated into a FQDN by the underlying operating system. In any case, whether the timeout occurs in logisland of in the operating system, the fact that a timeout occurs is kept in the cache system so that a resolution request for the same IP will not occur before the cache entry expires.", "", "1000", "", ""
   "debug", "If true, some additional debug fields are added. If the FQDN field is named X, a debug field named X_os_resolution_time_ms contains the resolution time in ms (using the operating system, not the cache). This field is added whether the resolution occurs or time is out. A debug field named  X_os_resolution_timeout contains a boolean value to indicate if the timeout occurred. Finally, a debug field named X_from_cache contains a boolean value to indicate the origin of the FQDN field. The default value for this property is false (debug is disabled.", "", "false", "", ""

----------

.. _com.hurence.logisland.processor.enrichment.IpToGeo: 

IpToGeo
-------
No description provided.

Module
______
com.hurence.logisland:logisland-processor-enrichment:1.0.0-RC1

Class
_____
com.hurence.logisland.processor.enrichment.IpToGeo

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

   "**ip.address.field**", "The name of the field containing the ip address to use.", "", "null", "", ""
   "**iptogeo.service**", "The reference to the IP to Geo service to use.", "", "null", "", ""
   "geo.fields", "Comma separated list of geo information fields to add to the record. Defaults to '*', which means to include all available fields. If a list of fields is specified and the data is not available, the geo field is not created. The geo fields are dependant on the underlying defined Ip to Geo service. The currently only supported type of Ip to Geo service is the Maxmind Ip to Geo service. This means that the currently supported list of geo fields is the following:**continent**: the identified continent for this IP address. **continent_code**: the identified continent code for this IP address. **city**: the identified city for this IP address. **latitude**: the identified latitude for this IP address. **longitude**: the identified longitude for this IP address. **location**: the identified location for this IP address, defined as Geo-point expressed as a string with the format: 'latitude,longitude'. **accuracy_radius**: the approximate accuracy radius, in kilometers, around the latitude and longitude for the location. **time_zone**: the identified time zone for this IP address. **subdivision_N**: the identified subdivision for this IP address. N is a one-up number at the end of the attribute name, starting with 0. **subdivision_isocode_N**: the iso code matching the identified subdivision_N. **country**: the identified country for this IP address. **country_isocode**: the iso code for the identified country for this IP address. **postalcode**: the identified postal code for this IP address. **lookup_micros**: the number of microseconds that the geo lookup took. The Ip to Geo service must have the lookup_micros property enabled in order to have this field available.", "", "*", "", ""
   "geo.hierarchical", "Should the additional geo information fields be added under a hierarchical father field or not.", "", "true", "", ""
   "geo.hierarchical.suffix", "Suffix to use for the field holding geo information. If geo.hierarchical is true, then use this suffix appended to the IP field name to define the father field name. This may be used for instance to distinguish between geo fields with various locales using many Ip to Geo service instances.", "", "_geo", "", ""
   "geo.flat.suffix", "Suffix to use for geo information fields when they are flat. If geo.hierarchical is false, then use this suffix appended to the IP field name but before the geo field name. This may be used for instance to distinguish between geo fields with various locales using many Ip to Geo service instances.", "", "_geo_", "", ""
   "**cache.service**", "The name of the cache service to use.", "", "null", "", ""
   "debug", "If true, an additional debug field is added. If the geo info fields prefix is X, a debug field named X_from_cache contains a boolean value to indicate the origin of the geo fields. The default value for this property is false (debug is disabled).", "", "false", "", ""

----------

.. _com.hurence.logisland.processor.MatchIP: 

MatchIP
-------
No description provided.

Module
______
com.hurence.logisland:logisland-processor-querymatcher:1.0.0-RC1

Class
_____
com.hurence.logisland.processor.MatchIP

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

   "numeric.fields", "a comma separated string of numeric field to be matched", "", "null", "", ""
   "output.record.type", "the output type of the record", "", "alert_match", "", ""
   "record.type.updatePolicy", "Record type update policy", "", "overwrite", "", ""
   "policy.onmatch", "the policy applied to match events: 'first' (default value) match events are tagged with the name and value of the first query that matched;'all' match events are tagged with all names and values of the queries that matched.", "", "first", "", ""
   "policy.onmiss", "the policy applied to miss events: 'discard' (default value) drop events that did not match any query;'forward' include also events that did not match any query.", "", "discard", "", ""
   "include.input.records", "if set to true all the input records are copied to output", "", "true", "", ""

----------

.. _com.hurence.logisland.processor.MatchQuery: 

MatchQuery
----------
No description provided.

Module
______
com.hurence.logisland:logisland-processor-querymatcher:1.0.0-RC1

Class
_____
com.hurence.logisland.processor.MatchQuery

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

   "numeric.fields", "a comma separated string of numeric field to be matched", "", "null", "", ""
   "output.record.type", "the output type of the record", "", "alert_match", "", ""
   "record.type.updatePolicy", "Record type update policy", "", "overwrite", "", ""
   "policy.onmatch", "the policy applied to match events: 'first' (default value) match events are tagged with the name and value of the first query that matched;'all' match events are tagged with all names and values of the queries that matched.", "", "first", "", ""
   "policy.onmiss", "the policy applied to miss events: 'discard' (default value) drop events that did not match any query;'forward' include also events that did not match any query.", "", "discard", "", ""
   "include.input.records", "if set to true all the input records are copied to output", "", "true", "", ""

----------

.. _com.hurence.logisland.processor.ModifyId: 

ModifyId
--------
No description provided.

Module
______
com.hurence.logisland:logisland-processor-common:1.0.0-RC1

Class
_____
com.hurence.logisland.processor.ModifyId

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
No description provided.

Module
______
com.hurence.logisland:logisland-service-mongodb-client:1.0.0-RC1

Class
_____
com.hurence.logisland.service.mongodb.MongoDBControllerService

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
   "batch.size", "The preferred number of Records to setField to the database in a single transaction", "", "1000", "", ""
   "bulk.size", "bulk size in MB", "", "5", "", ""
   "bulk.mode", "Bulk mode (insert or upsert)", "Insert (Insert records whose key must be unique), Insert or Update (Insert records if not already existing or update the record if already existing)", "insert", "", ""
   "flush.interval", "flush interval in ms", "", "500", "", ""
   "**Write Concern**", "The write concern to use", "ACKNOWLEDGED, UNACKNOWLEDGED, FSYNCED, JOURNALED, REPLICA_ACKNOWLEDGED, MAJORITY", "ACKNOWLEDGED", "", ""

----------

.. _com.hurence.logisland.processor.datastore.MultiGet: 

MultiGet
--------
No description provided.

Module
______
com.hurence.logisland:logisland-processor-common:1.0.0-RC1

Class
_____
com.hurence.logisland.processor.datastore.MultiGet

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
No description provided.

Module
______
com.hurence.logisland:logisland-processor-elasticsearch:1.0.0-RC1

Class
_____
com.hurence.logisland.processor.elasticsearch.MultiGetElasticsearch

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
No description provided.

Module
______
com.hurence.logisland:logisland-processor-common:1.0.0-RC1

Class
_____
com.hurence.logisland.processor.NormalizeFields

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

   "**conflict.resolution.policy**", "what to do when a field with the same name already exists ?", "nothing to do (leave record as it was), overwrite existing field (if field already exist), keep only old field and delete the other (keep only old field and delete the other), keep old field and new one (creates an alias for the new field)", "do_nothing", "", ""

----------

.. _com.hurence.logisland.processor.bro.ParseBroEvent: 

ParseBroEvent
-------------
No description provided.

Module
______
com.hurence.logisland:logisland-processor-cyber-security:1.0.0-RC1

Class
_____
com.hurence.logisland.processor.bro.ParseBroEvent

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

   "debug", "Enable debug. If enabled, the original JSON string is embedded in the record_value field of the record.", "", "false", "", ""

----------

.. _com.hurence.logisland.processor.commonlogs.gitlab.ParseGitlabLog: 

ParseGitlabLog
--------------
No description provided.

Module
______
com.hurence.logisland:logisland-processor-common-logs:1.0.0-RC1

Class
_____
com.hurence.logisland.processor.commonlogs.gitlab.ParseGitlabLog

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

   "debug", "Enable debug. If enabled, the original JSON string is embedded in the record_value field of the record.", "", "false", "", ""

----------

.. _com.hurence.logisland.processor.netflow.ParseNetflowEvent: 

ParseNetflowEvent
-----------------
No description provided.

Module
______
com.hurence.logisland:logisland-processor-cyber-security:1.0.0-RC1

Class
_____
com.hurence.logisland.processor.netflow.ParseNetflowEvent

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

   "debug", "Enable debug. If enabled, the original JSON string is embedded in the record_value field of the record.", "", "false", "", ""
   "output.record.type", "the output type of the record", "", "netflowevent", "", ""
   "enrich.record", "Enrich data. If enabledthe netflow record is enriched with inferred data", "", "false", "", ""

----------

.. _com.hurence.logisland.processor.networkpacket.ParseNetworkPacket: 

ParseNetworkPacket
------------------
No description provided.

Module
______
com.hurence.logisland:logisland-processor-cyber-security:1.0.0-RC1

Class
_____
com.hurence.logisland.processor.networkpacket.ParseNetworkPacket

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

   "debug", "Enable debug.", "", "false", "", ""
   "**flow.mode**", "Flow Mode. Indicate whether packets are provided in batch mode (via pcap files) or in stream mode (without headers). Allowed values are batch and stream.", "batch, stream", "null", "", ""

----------

.. _com.hurence.logisland.processor.ParseProperties: 

ParseProperties
---------------
No description provided.

Module
______
com.hurence.logisland:logisland-processor-common:1.0.0-RC1

Class
_____
com.hurence.logisland.processor.ParseProperties

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

   "**properties.field**", "the field containing the properties to split and treat", "", "null", "", ""

----------

.. _com.hurence.logisland.processor.useragent.ParseUserAgent: 

ParseUserAgent
--------------
No description provided.

Module
______
com.hurence.logisland:logisland-processor-useragent:1.0.0-RC1

Class
_____
com.hurence.logisland.processor.useragent.ParseUserAgent

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

   "debug", "Enable debug.", "", "false", "", ""
   "cache.enabled", "Enable caching. Caching to avoid to redo the same computation for many identical User-Agent strings.", "", "true", "", ""
   "cache.size", "Set the size of the cache.", "", "1000", "", ""
   "**useragent.field**", "Must contain the name of the field that contains the User-Agent value in the incoming record.", "", "null", "", ""
   "useragent.keep", "Defines if the field that contained the User-Agent must be kept or not in the resulting records.", "", "true", "", ""
   "confidence.enabled", "Enable confidence reporting. Each field will report a confidence attribute with a value comprised between 0 and 10000.", "", "false", "", ""
   "ambiguity.enabled", "Enable ambiguity reporting. Reports a count of ambiguities.", "", "false", "", ""
   "fields", "Defines the fields to be returned.", "", "DeviceClass, DeviceName, DeviceBrand, DeviceCpu, DeviceFirmwareVersion, DeviceVersion, OperatingSystemClass, OperatingSystemName, OperatingSystemVersion, OperatingSystemNameVersion, OperatingSystemVersionBuild, LayoutEngineClass, LayoutEngineName, LayoutEngineVersion, LayoutEngineVersionMajor, LayoutEngineNameVersion, LayoutEngineNameVersionMajor, LayoutEngineBuild, AgentClass, AgentName, AgentVersion, AgentVersionMajor, AgentNameVersion, AgentNameVersionMajor, AgentBuild, AgentLanguage, AgentLanguageCode, AgentInformationEmail, AgentInformationUrl, AgentSecurity, AgentUuid, FacebookCarrier, FacebookDeviceClass, FacebookDeviceName, FacebookDeviceVersion, FacebookFBOP, FacebookFBSS, FacebookOperatingSystemName, FacebookOperatingSystemVersion, Anonymized, HackerAttackVector, HackerToolkit, KoboAffiliate, KoboPlatformId, IECompatibilityVersion, IECompatibilityVersionMajor, IECompatibilityNameVersion, IECompatibilityNameVersionMajor, __SyntaxError__, Carrier, GSAInstallationID, WebviewAppName, WebviewAppNameVersionMajor, WebviewAppVersion, WebviewAppVersionMajor", "", ""

----------

.. _com.hurence.logisland.processor.hbase.PutHBaseCell: 

PutHBaseCell
------------
No description provided.

Module
______
com.hurence.logisland:logisland-processor-hbase:1.0.0-RC1

Class
_____
com.hurence.logisland.processor.hbase.PutHBaseCell

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

----------

.. _com.hurence.logisland.redis.service.RedisKeyValueCacheService: 

RedisKeyValueCacheService
-------------------------
No description provided.

Module
______
com.hurence.logisland:logisland-service-redis:1.0.0-RC1

Class
_____
com.hurence.logisland.redis.service.RedisKeyValueCacheService

Tags
____
None.

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
No description provided.

Module
______
com.hurence.logisland:logisland-processor-common:1.0.0-RC1

Class
_____
com.hurence.logisland.processor.RemoveFields

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

   "fields.to.remove", "A comma separated list of field names to remove (e.g. 'policyid,date_raw'). Usage of this property is mutually exclusive with the fields.to.keep property. In any case the technical logisland fields record_id, record_time and record_type are not removed even if specified in the list to remove.", "", "null", "", ""
   "fields.to.keep", "A comma separated list of field names to keep (e.g. 'policyid,date_raw'. All other fields will be removed. Usage of this property is mutually exclusive with the PropertyDescriptor[fields.to.remove] property. In any case the technical logisland fields record_id, record_time and record_type are not removed even if not specified in the list to keep.", "", "null", "", ""

----------

.. _com.hurence.logisland.processor.scripting.python.RunPython: 

RunPython
---------
No description provided.

Module
______
com.hurence.logisland:logisland-processor-scripting:1.0.0-RC1

Class
_____
com.hurence.logisland.processor.scripting.python.RunPython

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

   "script.code.imports", "For inline mode only. This is the python code that should hold the import statements if required.", "", "null", "", ""
   "script.code.init", "The python code to be called when the processor is initialized. This is the python equivalent of the init method code for a java processor. This is not mandatory but can only be used if **script.code.process** is defined (inline mode).", "", "null", "", ""
   "script.code.process", "The python code to be called to process the records. This is the pyhton equivalent of the process method code for a java processor. For inline mode, this is the only minimum required configuration property. Using this property, you may also optionally define the **script.code.init** and **script.code.imports** properties.", "", "null", "", ""
   "script.path", "The path to the user's python processor script. Use this property for file mode. Your python code must be in a python file with the following constraints: let's say your pyhton script is named MyProcessor.py. Then MyProcessor.py is a module file that must contain a class named MyProcessor which must inherits from the Logisland delivered class named AbstractProcessor. You can then define your code in the process method and in the other traditional methods (init...) as you would do in java in a class inheriting from the AbstractProcessor java class.", "", "null", "", ""
   "dependencies.path", "The path to the additional dependencies for the user's python code, whether using inline or file mode. This is optional as your code may not have additional dependencies. If you defined **script.path** (so using file mode) and if **dependencies.path** is not defined, Logisland will scan a potential directory named **dependencies** in the same directory where the script file resides and if it exists, any python code located there will be loaded as dependency as needed.", "", "null", "", ""
   "logisland.dependencies.path", "The path to the directory containing the python dependencies shipped with logisland. You should not have to tune this parameter.", "", "null", "", ""

----------

.. _com.hurence.logisland.processor.SampleRecords: 

SampleRecords
-------------
No description provided.

Module
______
com.hurence.logisland:logisland-processor-sampling:1.0.0-RC1

Class
_____
com.hurence.logisland.processor.SampleRecords

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

   "record.value.field", "the name of the numeric field to sample", "", "record_value", "", ""
   "record.time.field", "the name of the time field to sample", "", "record_time", "", ""
   "**sampling.algorithm**", "the implementation of the algorithm", "none, lttb, average, first_item, min_max, mode_median", "null", "", ""
   "**sampling.parameter**", "the parmater of the algorithm", "", "null", "", ""

----------

.. _com.hurence.logisland.processor.SelectDistinctRecords: 

SelectDistinctRecords
---------------------
No description provided.

Module
______
com.hurence.logisland:logisland-processor-common:1.0.0-RC1

Class
_____
com.hurence.logisland.processor.SelectDistinctRecords

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

   "**field.name**", "the field to distinct records", "", "record_id", "", ""

----------

.. _com.hurence.logisland.processor.SendMail: 

SendMail
--------
No description provided.

Module
______
com.hurence.logisland:logisland-processor-common:1.0.0-RC1

Class
_____
com.hurence.logisland.processor.SendMail

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

.. _com.hurence.logisland.processor.SetJsonAsFields: 

SetJsonAsFields
---------------
No description provided.

Module
______
com.hurence.logisland:logisland-processor-common:1.0.0-RC1

Class
_____
com.hurence.logisland.processor.SetJsonAsFields

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

   "debug", "Enable debug. If enabled, debug information are written to stdout.", "", "false", "", ""
   "**json.field**", "Field name of the string field that contains the json document to parse.", "", "record_value", "", ""
   "**keep.json.field**", "Keep the original json field or not. Default is false so default is to remove the json field.", "", "false", "", ""
   "**overwrite.existing.field**", "Overwrite an existing record field or not. Default is true so default is to remove the conflicting field.", "", "true", "", ""
   "**omit.null.attributes**", "Omit json attributes with null values. Default is false so to set them as null record fields", "", "false", "", ""
   "**omit.empty.string.attributes**", "Omit json attributes with empty string values. Default is false so to set them as empty string record fields", "", "false", "", ""

----------

.. _com.hurence.logisland.service.solr.Solr_5_5_5_ClientService: 

Solr_5_5_5_ClientService
------------------------
No description provided.

Module
______
com.hurence.logisland:logisland-service-solr_5_5_5-client:1.0.0-RC1

Class
_____
com.hurence.logisland.service.solr.Solr_5_5_5_ClientService

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

   "batch.size", "The preferred number of Records to setField to the database in a single transaction", "", "1000", "", ""
   "bulk.size", "bulk size in MB", "", "5", "", ""
   "**solr.cloud**", "is slor cloud enabled", "", "false", "", ""
   "**solr.collection**", "name of the collection to use", "", "null", "", ""
   "**solr.connection.string**", "zookeeper quorum host1:2181,host2:2181 for solr cloud or http address of a solr core ", "", "localhost:8983/solr", "", ""
   "solr.concurrent.requests", "setConcurrentRequests", "", "2", "", ""
   "flush.interval", "flush interval in ms", "", "500", "", ""
   "schema.update_timeout", "Schema update timeout interval in s", "", "15", "", ""

----------

.. _com.hurence.logisland.service.solr.Solr_6_4_2_ChronixClientService: 

Solr_6_4_2_ChronixClientService
-------------------------------
No description provided.

Module
______
com.hurence.logisland:logisland-service-solr_chronix_6.4.2-client:1.0.0-RC1

Class
_____
com.hurence.logisland.service.solr.Solr_6_4_2_ChronixClientService

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

   "batch.size", "The preferred number of Records to setField to the database in a single transaction", "", "1000", "", ""
   "**solr.cloud**", "is slor cloud enabled", "", "false", "", ""
   "**solr.collection**", "name of the collection to use", "", "null", "", ""
   "**solr.connection.string**", "zookeeper quorum host1:2181,host2:2181 for solr cloud or http address of a solr core ", "", "localhost:8983/solr", "", ""
   "flush.interval", "flush interval in ms", "", "500", "", ""
   "group.by", "The field the chunk should be grouped by", "", "", "", ""

----------

.. _com.hurence.logisland.service.solr.Solr_6_6_2_ClientService: 

Solr_6_6_2_ClientService
------------------------
No description provided.

Module
______
com.hurence.logisland:logisland-service-solr_6_6_2-client:1.0.0-RC1

Class
_____
com.hurence.logisland.service.solr.Solr_6_6_2_ClientService

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

   "batch.size", "The preferred number of Records to setField to the database in a single transaction", "", "1000", "", ""
   "bulk.size", "bulk size in MB", "", "5", "", ""
   "**solr.cloud**", "is slor cloud enabled", "", "false", "", ""
   "**solr.collection**", "name of the collection to use", "", "null", "", ""
   "**solr.connection.string**", "zookeeper quorum host1:2181,host2:2181 for solr cloud or http address of a solr core ", "", "localhost:8983/solr", "", ""
   "solr.concurrent.requests", "setConcurrentRequests", "", "2", "", ""
   "flush.interval", "flush interval in ms", "", "500", "", ""
   "schema.update_timeout", "Schema update timeout interval in s", "", "15", "", ""

----------

.. _com.hurence.logisland.processor.SplitField: 

SplitField
----------
No description provided.

Module
______
com.hurence.logisland:logisland-processor-common:1.0.0-RC1

Class
_____
com.hurence.logisland.processor.SplitField

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

   "conflict.resolution.policy", "What to do when a field with the same name already exists ?", "overwrite existing field (if field already exist), keep only old field (keep only old field)", "keep_only_old_field", "", ""
   "split.limit", "Specify the maximum number of split to allow", "", "10", "", ""
   "split.counter.enable", "Enable the counter of items returned by the split", "", "false", "", ""
   "split.counter.suffix", "Enable the counter of items returned by the split", "", "Counter", "", ""

----------

.. _com.hurence.logisland.processor.SplitText: 

SplitText
---------
No description provided.

Module
______
com.hurence.logisland:logisland-processor-common:1.0.0-RC1

Class
_____
com.hurence.logisland.processor.SplitText

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

   "**value.regex**", "the regex to match for the message value", "", "null", "", ""
   "**value.fields**", "a comma separated list of fields corresponding to matching groups for the message value", "", "null", "", ""
   "key.regex", "the regex to match for the message key", "", ".*", "", ""
   "key.fields", "a comma separated list of fields corresponding to matching groups for the message key", "", "record_key", "", ""
   "record.type", "default type of record", "", "record", "", ""
   "keep.raw.content", "do we add the initial raw content ?", "", "true", "", ""
   "timezone.record.time", "what is the time zone of the string formatted date for 'record_time' field.", "", "UTC", "", ""

----------

.. _com.hurence.logisland.processor.SplitTextMultiline: 

SplitTextMultiline
------------------
No description provided.

Module
______
com.hurence.logisland:logisland-processor-common:1.0.0-RC1

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
No description provided.

Module
______
com.hurence.logisland:logisland-processor-common:1.0.0-RC1

Class
_____
com.hurence.logisland.processor.SplitTextWithProperties

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

   "**value.regex**", "the regex to match for the message value", "", "null", "", ""
   "**value.fields**", "a comma separated list of fields corresponding to matching groups for the message value", "", "null", "", ""
   "key.regex", "the regex to match for the message key", "", ".*", "", ""
   "key.fields", "a comma separated list of fields corresponding to matching groups for the message key", "", "record_key", "", ""
   "record.type", "default type of record", "", "record", "", ""
   "keep.raw.content", "do we add the initial raw content ?", "", "true", "", ""
   "**properties.field**", "the field containing the properties to split and treat", "", "properties", "", ""

----------

.. _com.hurence.logisland.processor.webAnalytics.setSourceOfTraffic: 

setSourceOfTraffic
------------------
No description provided.

Module
______
com.hurence.logisland:logisland-processor-web-analytics:1.0.0-RC1

Class
_____
com.hurence.logisland.processor.webAnalytics.setSourceOfTraffic

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

   "referer.field", "Name of the field containing the referer value in the session", "", "referer", "", ""
   "first.visited.page.field", "Name of the field containing the first visited page in the session", "", "firstVisitedPage", "", ""
   "utm_source.field", "Name of the field containing the utm_source value in the session", "", "utm_source", "", ""
   "utm_medium.field", "Name of the field containing the utm_medium value in the session", "", "utm_medium", "", ""
   "utm_campaign.field", "Name of the field containing the utm_campaign value in the session", "", "utm_campaign", "", ""
   "utm_content.field", "Name of the field containing the utm_content value in the session", "", "utm_content", "", ""
   "utm_term.field", "Name of the field containing the utm_term value in the session", "", "utm_term", "", ""
   "source_of_traffic.suffix", "Suffix for the source of the traffic related fields", "", "source_of_traffic", "", ""
   "source_of_traffic.hierarchical", "Should the additional source of trafic information fields be added under a hierarchical father field or not.", "", "false", "", ""
   "**elasticsearch.client.service**", "The instance of the Controller Service to use for accessing Elasticsearch.", "", "null", "", ""
   "**cache.service**", "Name of the cache service to use.", "", "null", "", ""
   "cache.validity.timeout", "Timeout validity (in seconds) of an entry in the cache.", "", "0", "", ""
   "debug", "If true, an additional debug field is added. If the source info fields prefix is X, a debug field named X_from_cache contains a boolean value to indicate the origin of the source fields. The default value for this property is false (debug is disabled).", "", "false", "", ""
   "**es.index**", "Name of the ES index containing the list of search engines and social network. ", "", "null", "", ""
   "es.type", "Name of the ES type to use.", "", "default", "", ""
   "es.search_engine.field", "Name of the ES field used to specify that the domain is a search engine.", "", "search_engine", "", ""
   "es.social_network.field", "Name of the ES field used to specify that the domain is a social network.", "", "social_network", "", ""
