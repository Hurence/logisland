Components
==========
You'll find here the list of all usable Processors, Engines, Services and other components that can be usable out of the box in your analytics streams


----------

.. _com.hurence.logisland.processor.AddFields: 

AddFields
---------
Add one or more field with a default value
...

Module
______
com.hurence.logisland:logisland-processor-common:1.1.2

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

Module
______
com.hurence.logisland:logisland-processor-common:1.1.2

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

Module
______
com.hurence.logisland:logisland-processor-elasticsearch:1.1.2

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

Module
______
com.hurence.logisland:logisland-processor-common:1.1.2

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
Provides a controller service that for the moment only allows to bulkput records into cassandra.

Module
______
com.hurence.logisland:logisland-service-cassandra-client:1.1.2

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

   "**cassandra.hosts**", "Cassandra cluster hosts as a comma separated value list", "", "null", "", ""
   "**cassandra.port**", "Cassandra cluster port", "", "null", "", ""
   "cassandra.with-ssl", "If this property is true, use SSL. Default is no SSL (false).", "", "false", "", ""
   "cassandra.with-credentials", "If this property is true, use credentials. Default is no credentials (false).", "", "false", "", ""
   "cassandra.credentials.user", "The user name to use for authentication. cassandra.with-credentials must be true for that property to be used.", "", "null", "", ""
   "cassandra.credentials.password", "The user password to use for authentication. cassandra.with-credentials must be true for that property to be used.", "", "null", "", ""
   "batch.size", "The preferred number of Records to setField to the database in a single transaction", "", "1000", "", ""
   "bulk.size", "bulk size in MB", "", "5", "", ""
   "flush.interval", "flush interval in ms", "", "500", "", ""

----------

.. _com.hurence.logisland.processor.alerting.CheckAlerts: 

CheckAlerts
-----------
Add one or more field with a default value

Module
______
com.hurence.logisland:logisland-processor-common:1.1.2

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
   "allow.no.brace", "Force, to check if all blocks are enclosed with curly braces ""{}"".

   .. raw:: html

     <p>

     Explanation: all loops (for, do-while, while, and if-else, and functions

     should use braces, because poison_pill() function will be inserted after

     each open brace ""{"", to ensure interruption checking. Otherwise simple

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

   .. raw:: html

     <p>

     Each statements when setMaxCPUTime(long) is set is prepared to

     quit itself when time exceeded. To execute only once this procedure per

     statement set this value.

     </p>

     <p>

     When setMaxCPUTime(long) is set 0, this value is ignored.

     </p>

   ", "", "30", "", ""
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
- each dynamic property will return a new record according to the formula definition
- the record name will be set to the property name
- the record time will be set to the current timestamp


Module
______
com.hurence.logisland:logisland-processor-common:1.1.2

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
   "allow.no.brace", "Force, to check if all blocks are enclosed with curly braces ""{}"".

   .. raw:: html

     <p>

     Explanation: all loops (for, do-while, while, and if-else, and functions

     should use braces, because poison_pill() function will be inserted after

     each open brace ""{"", to ensure interruption checking. Otherwise simple

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

   .. raw:: html

     <p>

     Each statements when setMaxCPUTime(long) is set is prepared to

     quit itself when time exceeded. To execute only once this procedure per

     statement set this value.

     </p>

     <p>

     When setMaxCPUTime(long) is set 0, this value is ignored.

     </p>

   ", "", "30", "", ""
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

Module
______
com.hurence.logisland:logisland-processor-common:1.1.2

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
   "allow.no.brace", "Force, to check if all blocks are enclosed with curly braces ""{}"".

   .. raw:: html

     <p>

     Explanation: all loops (for, do-while, while, and if-else, and functions

     should use braces, because poison_pill() function will be inserted after

     each open brace ""{"", to ensure interruption checking. Otherwise simple

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

   .. raw:: html

     <p>

     Each statements when setMaxCPUTime(long) is set is prepared to

     quit itself when time exceeded. To execute only once this procedure per

     statement set this value.

     </p>

     <p>

     When setMaxCPUTime(long) is set 0, this value is ignored.

     </p>

   ", "", "30", "", ""
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

.. _com.hurence.logisland.processor.webAnalytics.ConsolidateSession: 

ConsolidateSession
------------------
The ConsolidateSession processor is the Logisland entry point to get and process events from the Web Analytics.As an example here is an incoming event from the Web Analytics:

"fields": [{ "name": "timestamp",              "type": "long" },{ "name": "remoteHost",             "type": "string"},{ "name": "record_type",            "type": ["null", "string"], "default": null },{ "name": "record_id",              "type": ["null", "string"], "default": null },{ "name": "location",               "type": ["null", "string"], "default": null },{ "name": "hitType",                "type": ["null", "string"], "default": null },{ "name": "eventCategory",          "type": ["null", "string"], "default": null },{ "name": "eventAction",            "type": ["null", "string"], "default": null },{ "name": "eventLabel",             "type": ["null", "string"], "default": null },{ "name": "localPath",              "type": ["null", "string"], "default": null },{ "name": "q",                      "type": ["null", "string"], "default": null },{ "name": "n",                      "type": ["null", "int"],    "default": null },{ "name": "referer",                "type": ["null", "string"], "default": null },{ "name": "viewportPixelWidth",     "type": ["null", "int"],    "default": null },{ "name": "viewportPixelHeight",    "type": ["null", "int"],    "default": null },{ "name": "screenPixelWidth",       "type": ["null", "int"],    "default": null },{ "name": "screenPixelHeight",      "type": ["null", "int"],    "default": null },{ "name": "partyId",                "type": ["null", "string"], "default": null },{ "name": "sessionId",              "type": ["null", "string"], "default": null },{ "name": "pageViewId",             "type": ["null", "string"], "default": null },{ "name": "is_newSession",          "type": ["null", "boolean"],"default": null },{ "name": "userAgentString",        "type": ["null", "string"], "default": null },{ "name": "pageType",               "type": ["null", "string"], "default": null },{ "name": "UserId",                 "type": ["null", "string"], "default": null },{ "name": "B2Bunit",                "type": ["null", "string"], "default": null },{ "name": "pointOfService",         "type": ["null", "string"], "default": null },{ "name": "companyID",              "type": ["null", "string"], "default": null },{ "name": "GroupCode",              "type": ["null", "string"], "default": null },{ "name": "userRoles",              "type": ["null", "string"], "default": null },{ "name": "is_PunchOut",            "type": ["null", "string"], "default": null }]The ConsolidateSession processor groups the records by sessions and compute the duration between now and the last received event. If the distance from the last event is beyond a given threshold (by default 30mn), then the session is considered closed.The ConsolidateSession is building an aggregated session object for each active session.This aggregated object includes: - The actual session duration. - A boolean representing wether the session is considered active or closed.   Note: it is possible to ressurect a session if for instance an event arrives after a session has been marked closed. - User related infos: userId, B2Bunit code, groupCode, userRoles, companyId - First visited page: URL - Last visited page: URL The properties to configure the processor are: - sessionid.field:          Property name containing the session identifier (default: sessionId). - timestamp.field:          Property name containing the timestamp of the event (default: timestamp). - session.timeout:          Timeframe of inactivity (in seconds) after which a session is considered closed (default: 30mn). - visitedpage.field:        Property name containing the page visited by the customer (default: location). - fields.to.return:         List of fields to return in the aggregated object. (default: N/A)

Module
______
com.hurence.logisland:logisland-processor-web-analytics:1.1.2

Class
_____
com.hurence.logisland.processor.webAnalytics.ConsolidateSession

Tags
____
analytics, web, session

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
Converts a field value into the given type. does nothing if conversion is not possible

Module
______
com.hurence.logisland:logisland-processor-common:1.1.2

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

.. _com.hurence.logisland.processor.ConvertSimpleDateFormatFields: 

ConvertSimpleDateFormatFields
-----------------------------
Convert one or more field representing a date into a Unix Epoch Time (time in milliseconds since &st January 1970, 00:00:00 GMT)
...

Module
______
com.hurence.logisland:logisland-processor-common:1.1.2

Class
_____
com.hurence.logisland.processor.ConvertSimpleDateFormatFields

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
   "**input.date.format**", "Simple date format representation of the input field to convert", "", "null", "", ""
   "**timezone**", "Specify the timezone (default is CET)", "", "CET", "", ""

Dynamic Properties
__________________
Dynamic Properties allow the user to specify both the name and value of a property.

.. csv-table:: dynamic-properties
   :header: "Name","Value","Description","EL"
   :widths: 20,20,40,10

   "field to convert", "a default value", "Add a field to the record with the default value", ""

----------

.. _com.hurence.logisland.processor.DebugStream: 

DebugStream
-----------
This is a processor that logs incoming records

Module
______
com.hurence.logisland:logisland-processor-common:1.1.2

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

----------

.. _com.hurence.logisland.processor.DetectOutliers: 

DetectOutliers
--------------
Outlier Analysis: A Hybrid Approach

In order to function at scale, a two-phase approach is taken

For every data point

- Detect outlier candidates using a robust estimator of variability (e.g. median absolute deviation) that uses distributional sketching (e.g. Q-trees)
- Gather a biased sample (biased by recency)
- Extremely deterministic in space and cheap in computation

For every outlier candidate

- Use traditional, more computationally complex approaches to outlier analysis (e.g. Robust PCA) on the biased sample
- Expensive computationally, but run infrequently

This becomes a data filter which can be attached to a timeseries data stream within a distributed computational framework (i.e. Storm, Spark, Flink, NiFi) to detect outliers.

Module
______
com.hurence.logisland:logisland-processor-outlier-detection:1.1.2

Class
_____
com.hurence.logisland.processor.DetectOutliers

Tags
____
analytic, outlier, record, iot, timeseries

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
Implementation of ElasticsearchClientService for Elasticsearch 2.4.0.

Module
______
com.hurence.logisland:logisland-service-elasticsearch_2_4_0-client:1.1.2

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

Module
______
com.hurence.logisland:logisland-service-elasticsearch_5_4_0-client:1.1.2

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

Module
______
com.hurence.logisland:logisland-processor-common:1.1.2

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
Each outcoming record holds at least the input record plus potentially one or more fields coming from of one elasticsearch document.

Module
______
com.hurence.logisland:logisland-processor-elasticsearch:1.1.2

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

Module
______
com.hurence.logisland:logisland-processor-common:1.1.2

Class
_____
com.hurence.logisland.processor.EvaluateJsonPath

Tags
____
JSON, evaluate, JsonPath

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

Dynamic Properties
__________________
Dynamic Properties allow the user to specify both the name and value of a property.

.. csv-table:: dynamic-properties
   :header: "Name","Value","Description","EL"
   :widths: 20,20,40,10

   "A Record field", "A JsonPath expression", "will be set to any JSON objects that match the JsonPath. ", ""

----------

.. _com.hurence.logisland.processor.xml.EvaluateXPath: 

EvaluateXPath
-------------
Evaluates one or more XPaths against the content of a record. The results of those XPaths are assigned to new attributes in the records, depending on configuration of the Processor. XPaths are entered by adding user-defined properties; the name of the property maps to the Attribute Name into which the result will be placed. The value of the property must be a valid XPath expression. If the expression matches nothing, no attributes is added. 

Module
______
com.hurence.logisland:logisland-processor-xml:1.1.2

Class
_____
com.hurence.logisland.processor.xml.EvaluateXPath

Tags
____
XML, evaluate, XPath

Properties
__________
In the list below, the names of required properties appear in **bold**. Any other properties (not in bold) are considered optional. The table also indicates any default values
.

.. csv-table:: allowable-values
   :header: "Name","Description","Allowable Values","Default Value","Sensitive","EL"
   :widths: 20,60,30,20,10,10

   "**source**", "Indicates the attribute containing the xml data to evaluate xpath against.", "", "null", "", ""
   "**validate_dtd**", "Specifies whether or not the XML content should be validated against the DTD.", "true, false", "true", "", ""
   "conflict.resolution.policy", "What to do when a field with the same name already exists ?", "overwrite existing field (if field already exist), keep only old field value (keep only old field)", "keep_only_old_field", "", ""

Dynamic Properties
__________________
Dynamic Properties allow the user to specify both the name and value of a property.

.. csv-table:: dynamic-properties
   :header: "Name","Value","Description","EL"
   :widths: 20,20,40,10

   "An attribute", "An XPath expression", " the attribute is set to the result of the XPath Expression.", ""

----------

.. _com.hurence.logisland.processor.excel.ExcelExtract: 

ExcelExtract
------------
Consumes a Microsoft Excel document and converts each worksheet's line to a structured record. The processor is assuming to receive raw excel file as input record.

Module
______
com.hurence.logisland:logisland-processor-excel:1.1.2

Class
_____
com.hurence.logisland.processor.excel.ExcelExtract

Tags
____
excel, processor, poi

Properties
__________
In the list below, the names of required properties appear in **bold**. Any other properties (not in bold) are considered optional. The table also indicates any default values
.

.. csv-table:: allowable-values
   :header: "Name","Description","Allowable Values","Default Value","Sensitive","EL"
   :widths: 20,60,30,20,10,10

   "sheets", "Comma separated list of Excel document sheet names that should be extracted from the excel document. If this property is left blank then all of the sheets will be extracted from the Excel document. You can specify regular expressions. Any sheets not specified in this value will be ignored.", "", "", "", ""
   "skip.columns", "Comma delimited list of column numbers to skip. Use the columns number and not the letter designation. Use this to skip over columns anywhere in your worksheet that you don't want extracted as part of the record.", "", "", "", ""
   "field.names", "The comma separated list representing the names of columns of extracted cells. Order matters! You should use either field.names either field.row.header but not both together.", "", "null", "", ""
   "skip.rows", "The row number of the first row to start processing.Use this to skip over rows of data at the top of your worksheet that are not part of the dataset.Empty rows of data anywhere in the spreadsheet will always be skipped, no matter what this value is set to.", "", "0", "", ""
   "record.type", "Default type of record", "", "excel_record", "", ""
   "field.row.header", "If set, field names mapping will be extracted from the specified row number. You should use either field.names either field.row.header but not both together.", "", "null", "", ""

----------

.. _com.hurence.logisland.processor.ExpandMapFields: 

ExpandMapFields
---------------
Expands the content of a MAP field to the root.

Module
______
com.hurence.logisland:logisland-processor-common:1.1.2

Class
_____
com.hurence.logisland.processor.ExpandMapFields

Tags
____
record, fields, Expand, Map

Properties
__________
In the list below, the names of required properties appear in **bold**. Any other properties (not in bold) are considered optional. The table also indicates any default values
.

.. csv-table:: allowable-values
   :header: "Name","Description","Allowable Values","Default Value","Sensitive","EL"
   :widths: 20,60,30,20,10,10

   "**fields.to.expand**", "Comma separated list of fields of type map that will be expanded to the root", "", "null", "", ""
   "conflict.resolution.policy", "What to do when a field with the same name already exists ?", "overwrite existing field (if field already exist), keep only old field value (keep only old field)", "keep_only_old_field", "", ""

----------

.. _com.hurence.logisland.processor.hbase.FetchHBaseRow: 

FetchHBaseRow
-------------
Fetches a row from an HBase table. The Destination property controls whether the cells are added as flow file attributes, or the row is written to the flow file content as JSON. This processor may be used to fetch a fixed row on a interval by specifying the table and row id directly in the processor, or it may be used to dynamically fetch rows by referencing the table and row id from incoming flow files.

Module
______
com.hurence.logisland:logisland-processor-hbase:1.1.2

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
   "**row.identifier.field**", "The field containing the identifier of the row to fetch.", "", "null", "", "**true**"
   "columns.field", "The field containing an optional comma-separated list of ""<colFamily>:<colQualifier>"" pairs to fetch. To return all columns for a given family, leave off the qualifier such as ""<colFamily1>,<colFamily2>"".", "", "null", "", "**true**"
   "record.serializer", "the serializer needed to i/o the record in the HBase row", "kryo serialization (serialize events as json blocs), json serialization (serialize events as json blocs), avro serialization (serialize events as avro blocs), no serialization (send events as bytes)", "com.hurence.logisland.serializer.KryoSerializer", "", ""
   "record.schema", "the avro schema definition for the Avro serialization", "", "null", "", ""
   "table.name.default", "The table to use if table name field is not set", "", "null", "", ""

----------

.. _com.hurence.logisland.processor.FilterRecords: 

FilterRecords
-------------
Keep only records based on a given field value

Module
______
com.hurence.logisland:logisland-processor-common:1.1.2

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

Module
______
com.hurence.logisland:logisland-processor-common:1.1.2

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

Module
______
com.hurence.logisland:logisland-processor-common:1.1.2

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

Module
______
com.hurence.logisland:logisland-service-hbase_1_1_2-client:1.1.2

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

.. _com.hurence.logisland.processor.webAnalytics.IncrementalWebSession: 

IncrementalWebSession
---------------------
This processor creates and updates web-sessions based on incoming web-events. Note that both web-sessions and web-events are stored in elasticsearch.
 Firstly, web-events are grouped by their session identifier and processed in chronological order.
 Then each web-session associated to each group is retrieved from elasticsearch.
 In case none exists yet then a new web session is created based on the first web event.
 The following fields of the newly created web session are set based on the associated web event: session identifier, first timestamp, first visited page. Secondly, once created, or retrieved, the web session is updated by the remaining web-events.
 Updates have impacts on fields of the web session such as event counter, last visited page,  session duration, ...
 Before updates are actually applied, checks are performed to detect rules that would trigger the creation of a new session:	the duration between the web session and the web event must not exceed the specified time-out,
	the web session and the web event must have timestamps within the same day (at midnight a new web session is created),
	source of traffic (campaign, ...) must be the same on the web session and the web event.
 When a breaking rule is detected, a new web session is created with a new session identifier where as remaining web-events still have the original session identifier. The new session identifier is the original session suffixed with the character '#' followed with an incremented counter. This new session identifier is also set on the remaining web-events.
 Finally when all web events were applied, all web events -potentially modified with a new session identifier- are save in elasticsearch. And web sessions are passed to the next processor.

WebSession information are:
- first and last visited page
- first and last timestamp of processed event 
- total number of processed events
- the userId
- a boolean denoting if the web-session is still active or not
- an integer denoting the duration of the web-sessions
- optional fields that may be retrieved from the processed events



Module
______
com.hurence.logisland:logisland-processor-web-analytics:1.1.2

Class
_____
com.hurence.logisland.processor.webAnalytics.IncrementalWebSession

Tags
____
analytics, web, session

Properties
__________
In the list below, the names of required properties appear in **bold**. Any other properties (not in bold) are considered optional. The table also indicates any default values
.

.. csv-table:: allowable-values
   :header: "Name","Description","Allowable Values","Default Value","Sensitive","EL"
   :widths: 20,60,30,20,10,10

   "debug", "Enable debug. If enabled, debug information are logged.", "", "false", "", ""
   "**es.session.index.field**", "Name of the field in the record defining the ES index containing the web session documents.", "", "null", "", ""
   "**es.session.type.name**", "Name of the ES type of web session documents.", "", "null", "", ""
   "**es.event.index.prefix**", "Prefix of the index containing the web event documents.", "", "null", "", ""
   "**es.event.type.name**", "Name of the ES type of web event documents.", "", "null", "", ""
   "**es.mapping.event.to.session.index.name**", "Name of the ES index containing the mapping of web session documents.", "", "null", "", ""
   "sessionid.field", "the name of the field containing the session id => will override default value if set", "", "sessionId", "", ""
   "timestamp.field", "the name of the field containing the timestamp => will override default value if set", "", "h2kTimestamp", "", ""
   "visitedpage.field", "the name of the field containing the visited page => will override default value if set", "", "location", "", ""
   "userid.field", "the name of the field containing the userId => will override default value if set", "", "userId", "", ""
   "fields.to.return", "the list of fields to return", "", "null", "", ""
   "firstVisitedPage.out.field", "the name of the field containing the first visited page => will override default value if set", "", "firstVisitedPage", "", ""
   "lastVisitedPage.out.field", "the name of the field containing the last visited page => will override default value if set", "", "lastVisitedPage", "", ""
   "isSessionActive.out.field", "the name of the field stating whether the session is active or not => will override default value if set", "", "is_sessionActive", "", ""
   "sessionDuration.out.field", "the name of the field containing the session duration => will override default value if set", "", "sessionDuration", "", ""
   "sessionInactivityDuration.out.field", "the name of the field containing the session inactivity duration => will override default value if set", "", "sessionInactivityDuration", "", ""
   "session.timeout", "session timeout in sec", "", "1800", "", ""
   "eventsCounter.out.field", "the name of the field containing the session duration => will override default value if set", "", "eventsCounter", "", ""
   "firstEventDateTime.out.field", "the name of the field containing the date of the first event => will override default value if set", "", "firstEventDateTime", "", ""
   "lastEventDateTime.out.field", "the name of the field containing the date of the last event => will override default value if set", "", "lastEventDateTime", "", ""
   "newSessionReason.out.field", "the name of the field containing the reason why a new session was created => will override default value if set", "", "reasonForNewSession", "", ""
   "transactionIds.out.field", "the name of the field containing all transactionIds => will override default value if set", "", "transactionIds", "", ""
   "source_of_traffic.suffix", "Prefix for the source of the traffic related fields", "", "source_of_traffic", "", ""
   "**elasticsearch.client.service**", "The instance of the Controller Service to use for accessing Elasticsearch.", "", "null", "", ""

----------

.. _com.hurence.logisland.processor.enrichment.IpToFqdn: 

IpToFqdn
--------
Translates an IP address into a FQDN (Fully Qualified Domain Name). An input field from the record has the IP as value. An new field is created and its value is the FQDN matching the IP address. The resolution mechanism is based on the underlying operating system. The resolution request may take some time, specially if the IP address cannot be translated into a FQDN. For these reasons this processor relies on the logisland cache service so that once a resolution occurs or not, the result is put into the cache. That way, the real request for the same IP is not re-triggered during a certain period of time, until the cache entry expires. This timeout is configurable but by default a request for the same IP is not triggered before 24 hours to let the time to the underlying DNS system to be potentially updated.

Module
______
com.hurence.logisland:logisland-processor-enrichment:1.1.2

Class
_____
com.hurence.logisland.processor.enrichment.IpToFqdn

Tags
____
dns, ip, fqdn, domain, address, fqhn, reverse, resolution, enrich

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
Looks up geolocation information for an IP address. The attribute that contains the IP address to lookup must be provided in the **ip.address.field** property. By default, the geo information are put in a hierarchical structure. That is, if the name of the IP field is 'X', then the the geo attributes added by enrichment are added under a father field named X_geo. "_geo" is the default hierarchical suffix that may be changed with the **geo.hierarchical.suffix** property. If one wants to put the geo fields at the same level as the IP field, then the **geo.hierarchical** property should be set to false and then the geo attributes are  created at the same level as him with the naming pattern X_geo_<geo_field>. "_geo_" is the default flat suffix but this may be changed with the **geo.flat.suffix** property. The IpToGeo processor requires a reference to an Ip to Geo service. This must be defined in the **iptogeo.service** property. The added geo fields are dependant on the underlying Ip to Geo service. The **geo.fields** property must contain the list of geo fields that should be created if data is available for  the IP to resolve. This property defaults to "*" which means to add every available fields. If one only wants a subset of the fields,  one must define a comma separated list of fields as a value for the **geo.fields** property. The list of the available geo fields is in the description of the **geo.fields** property.

Module
______
com.hurence.logisland:logisland-processor-enrichment:1.1.2

Class
_____
com.hurence.logisland.processor.enrichment.IpToGeo

Tags
____
geo, enrich, ip

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
IP address Query matching (using `Luwak <http://www.confluent.io/blog/real-time-full-text-search-with-luwak-and-samza/>)`_

You can use this processor to handle custom events matching IP address (CIDR)
The record sent from a matching an IP address record is tagged appropriately.

A query is expressed as a lucene query against a field like for example: 

.. code::

	message:'bad exception'
	error_count:[10 TO *]
	bytes_out:5000
	user_name:tom*

Please read the `Lucene syntax guide <https://lucene.apache.org/core/5_5_0/queryparser/org/apache/lucene/queryparser/classic/package-summary.html#package_description>`_ for supported operations

.. warning::

	don't forget to set numeric fields property to handle correctly numeric ranges queries

Module
______
com.hurence.logisland:logisland-processor-querymatcher:1.1.2

Class
_____
com.hurence.logisland.processor.MatchIP

Tags
____
analytic, percolator, record, record, query, lucene

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

Dynamic Properties
__________________
Dynamic Properties allow the user to specify both the name and value of a property.

.. csv-table:: dynamic-properties
   :header: "Name","Value","Description","EL"
   :widths: 20,20,40,10

   "query", "some Lucene query", "generate a new record when this query is matched", **true**

----------

.. _com.hurence.logisland.processor.MatchQuery: 

MatchQuery
----------
Query matching based on `Luwak <http://www.confluent.io/blog/real-time-full-text-search-with-luwak-and-samza/>`_

you can use this processor to handle custom events defined by lucene queries
a new record is added to output each time a registered query is matched

A query is expressed as a lucene query against a field like for example: 

.. code::

	message:'bad exception'
	error_count:[10 TO *]
	bytes_out:5000
	user_name:tom*

Please read the `Lucene syntax guide <https://lucene.apache.org/core/5_5_0/queryparser/org/apache/lucene/queryparser/classic/package-summary.html#package_description>`_ for supported operations

.. warning::

	don't forget to set numeric fields property to handle correctly numeric ranges queries

Module
______
com.hurence.logisland:logisland-processor-querymatcher:1.1.2

Class
_____
com.hurence.logisland.processor.MatchQuery

Tags
____
analytic, percolator, record, record, query, lucene

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

Dynamic Properties
__________________
Dynamic Properties allow the user to specify both the name and value of a property.

.. csv-table:: dynamic-properties
   :header: "Name","Value","Description","EL"
   :widths: 20,20,40,10

   "query", "some Lucene query", "generate a new record when this query is matched", **true**

----------

.. _com.hurence.logisland.processor.ModifyId: 

ModifyId
--------
modify id of records or generate it following defined rules

Module
______
com.hurence.logisland:logisland-processor-common:1.1.2

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

Module
______
com.hurence.logisland:logisland-service-mongodb-client:1.1.2

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

   "**mongo.uri**", "MongoURI, typically of the form: mongodb://host1[:port1][,host2[:port2],...]", "", "null", "", "**true**"
   "**mongo.db.name**", "The name of the database to use", "", "null", "", "**true**"
   "**mongo.collection.name**", "The name of the collection to use", "", "null", "", "**true**"
   "batch.size", "The preferred number of Records to setField to the database in a single transaction", "", "1000", "", ""
   "bulk.size", "bulk size in MB", "", "5", "", ""
   "mongo.bulk.mode", "Bulk mode (insert or upsert)", "Insert (Insert records whose key must be unique), Insert or Update (Insert records if not already existing or update the record if already existing)", "insert", "", ""
   "flush.interval", "flush interval in ms", "", "500", "", ""
   "**mongo.write.concern**", "The write concern to use", "ACKNOWLEDGED, UNACKNOWLEDGED, FSYNCED, JOURNALED, REPLICA_ACKNOWLEDGED, MAJORITY", "ACKNOWLEDGED", "", ""
   "mongo.bulk.upsert.condition", "A custom condition for the bulk upsert (Filter for the bulkwrite). If not specified the standard condition is to match same id ('_id': data._id)", "", "${'{ "_id" :"' + record_id + '"}'}", "", "**true**"

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

Module
______
com.hurence.logisland:logisland-processor-common:1.1.2

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

Module
______
com.hurence.logisland:logisland-processor-elasticsearch:1.1.2

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

Module
______
com.hurence.logisland:logisland-processor-common:1.1.2

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

.. _com.hurence.logisland.processor.bro.ParseBroEvent: 

ParseBroEvent
-------------
The ParseBroEvent processor is the Logisland entry point to get and process `Bro <https://www.bro.org>`_ events. The `Bro-Kafka plugin <https://github.com/bro/bro-plugins/tree/master/kafka>`_ should be used and configured in order to have Bro events sent to Kafka. See the `Bro/Logisland tutorial <http://logisland.readthedocs.io/en/latest/tutorials/indexing-bro-events.html>`_ for an example of usage for this processor. The ParseBroEvent processor does some minor pre-processing on incoming Bro events from the Bro-Kafka plugin to adapt them to Logisland.

Basically the events coming from the Bro-Kafka plugin are JSON documents with a first level field indicating the type of the event. The ParseBroEvent processor takes the incoming JSON document, sets the event type in a record_type field and sets the original sub-fields of the JSON event as first level fields in the record. Also any dot in a field name is transformed into an underscore. Thus, for instance, the field id.orig_h becomes id_orig_h. The next processors in the stream can then process the Bro events generated by this ParseBroEvent processor.

As an example here is an incoming event from Bro:

{

   "conn": {

     "id.resp_p": 9092,

     "resp_pkts": 0,

     "resp_ip_bytes": 0,

     "local_orig": true,

     "orig_ip_bytes": 0,

     "orig_pkts": 0,

     "missed_bytes": 0,

     "history": "Cc",

     "tunnel_parents": [],

     "id.orig_p": 56762,

     "local_resp": true,

     "uid": "Ct3Ms01I3Yc6pmMZx7",

     "conn_state": "OTH",

     "id.orig_h": "172.17.0.2",

     "proto": "tcp",

     "id.resp_h": "172.17.0.3",

     "ts": 1487596886.953917

   }

 }

It gets processed and transformed into the following Logisland record by the ParseBroEvent processor:

"@timestamp": "2017-02-20T13:36:32Z"

"record_id": "6361f80a-c5c9-4a16-9045-4bb51736333d"

"record_time": 1487597792782

"record_type": "conn"

"id_resp_p": 9092

"resp_pkts": 0

"resp_ip_bytes": 0

"local_orig": true

"orig_ip_bytes": 0

"orig_pkts": 0

"missed_bytes": 0

"history": "Cc"

"tunnel_parents": []

"id_orig_p": 56762

"local_resp": true

"uid": "Ct3Ms01I3Yc6pmMZx7"

"conn_state": "OTH"

"id_orig_h": "172.17.0.2"

"proto": "tcp"

"id_resp_h": "172.17.0.3"

"ts": 1487596886.953917

Module
______
com.hurence.logisland:logisland-processor-cyber-security:1.1.2

Class
_____
com.hurence.logisland.processor.bro.ParseBroEvent

Tags
____
bro, security, IDS, NIDS

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
The Gitlab logs processor is the Logisland entry point to get and process `Gitlab <https://www.gitlab.com>`_ logs. This allows for instance to monitor activities in your Gitlab server. The expected input of this processor are records from the production_json.log log file of Gitlab which contains JSON records. You can for instance use the `kafkacat <https://github.com/edenhill/kafkacat>`_ command to inject those logs into kafka and thus Logisland.

Module
______
com.hurence.logisland:logisland-processor-common-logs:1.1.2

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

.. _com.hurence.logisland.processor.netflow.ParseNetflowEvent: 

ParseNetflowEvent
-----------------
The `Netflow V5 <http://www.cisco.com/c/en/us/td/docs/ios/solutions_docs/netflow/nfwhite.html>`_ processor is the Logisland entry point to  process Netflow (V5) events. NetFlow is a feature introduced on Cisco routers that provides the ability to collect IP network traffic.We can distinguish 2 components:

	-Flow exporter: aggregates packets into flows and exports flow records (binary format) towards one or more flow collectors

	-Flow collector: responsible for reception, storage and pre-processing of flow data received from a flow exporter
The collected data are then available for analysis purpose (intrusion detection, traffic analysis...)
Netflow are sent to kafka in order to be processed by logisland.
In the tutorial we will simulate Netflow traffic using `nfgen <https://github.com/pazdera/NetFlow-Exporter-Simulator>`_. this traffic will be sent to port 2055. The we rely on nifi to listen of that port for   incoming netflow (V5) traffic and send them to a kafka topic. The Netflow processor could thus treat these events and generate corresponding logisland records. The following processors in the stream can then process the Netflow records generated by this processor.

Module
______
com.hurence.logisland:logisland-processor-cyber-security:1.1.2

Class
_____
com.hurence.logisland.processor.netflow.ParseNetflowEvent

Tags
____
netflow, security

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
The ParseNetworkPacket processor is the LogIsland entry point to parse network packets captured either off-the-wire (stream mode) or in pcap format (batch mode).  In batch mode, the processor decodes the bytes of the incoming pcap record, where a Global header followed by a sequence of [packet header, packet data] pairs are stored. Then, each incoming pcap event is parsed into n packet records. The fields of packet headers are then extracted and made available in dedicated record fields. See the `Capturing Network packets tutorial <http://logisland.readthedocs.io/en/latest/tutorials/indexing-network-packets.html>`_ for an example of usage of this processor.

Module
______
com.hurence.logisland:logisland-processor-cyber-security:1.1.2

Class
_____
com.hurence.logisland.processor.networkpacket.ParseNetworkPacket

Tags
____
PCap, security, IDS, NIDS

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
Parse a field made of key=value fields separated by spaces
a string like "a=1 b=2 c=3" will add a,b & c fields, respectively with values 1,2 & 3 to the current Record

Module
______
com.hurence.logisland:logisland-processor-common:1.1.2

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

.. _com.hurence.logisland.processor.useragent.ParseUserAgent: 

ParseUserAgent
--------------
The user-agent processor allows to decompose User-Agent value from an HTTP header into several attributes of interest. There is no standard format for User-Agent strings, hence it is not easily possible to use regexp to handle them. This processor rely on the `YAUAA library <https://github.com/nielsbasjes/yauaa>`_ to do the heavy work.

Module
______
com.hurence.logisland:logisland-processor-useragent:1.1.2

Class
_____
com.hurence.logisland.processor.useragent.ParseUserAgent

Tags
____
User-Agent, clickstream, DMP

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
Adds the Contents of a Record to HBase as the value of a single cell

Module
______
com.hurence.logisland:logisland-processor-hbase:1.1.2

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

----------

.. _com.hurence.logisland.redis.service.RedisKeyValueCacheService: 

RedisKeyValueCacheService
-------------------------
A controller service for caching records by key value pair with LRU (last recently used) strategy. using LinkedHashMap

Module
______
com.hurence.logisland:logisland-service-redis:1.1.2

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

   "**redis.mode**", "The type of Redis being communicated with - standalone, sentinel, or clustered.", "standalone (A single standalone Redis instance.), sentinel (Redis Sentinel which provides high-availability. Described further at https://redis.io/topics/sentinel), cluster (Clustered Redis which provides sharding and replication. Described further at https://redis.io/topics/cluster-spec)", "standalone", "", ""
   "**connection.string**", "The connection string for Redis. In a standalone instance this value will be of the form hostname:port. In a sentinel instance this value will be the comma-separated list of sentinels, such as host1:port1,host2:port2,host3:port3. In a clustered instance this value will be the comma-separated list of cluster masters, such as host1:port,host2:port,host3:port.", "", "null", "", ""
   "**database.index**", "The database index to be used by connections created from this connection pool. See the databases property in redis.conf, by default databases 0-15 will be available.", "", "0", "", ""
   "**communication.timeout**", "The timeout to use when attempting to communicate with Redis.", "", "10 seconds", "", ""
   "**cluster.max.redirects**", "The maximum number of redirects that can be performed when clustered.", "", "5", "", ""
   "sentinel.master", "The name of the sentinel master, require when Mode is set to Sentinel", "", "null", "", ""
   "password", "The password used to authenticate to the Redis server. See the requirepass property in redis.conf.", "", "null", "**true**", ""
   "**pool.max.total**", "The maximum number of connections that can be allocated by the pool (checked out to clients, or idle awaiting checkout). A negative value indicates that there is no limit.", "", "8", "", ""
   "**pool.max.idle**", "The maximum number of idle connections that can be held in the pool, or a negative value if there is no limit.", "", "8", "", ""
   "**pool.min.idle**", "The target for the minimum number of idle connections to maintain in the pool. If the configured value of Min Idle is greater than the configured value for Max Idle, then the value of Max Idle will be used instead.", "", "0", "", ""
   "**pool.block.when.exhausted**", "Whether or not clients should block and wait when trying to obtain a connection from the pool when the pool has no available connections. Setting this to false means an error will occur immediately when a client requests a connection and none are available.", "true, false", "true", "", ""
   "**pool.max.wait.time**", "The amount of time to wait for an available connection when Block When Exhausted is set to true.", "", "10 seconds", "", ""
   "**pool.min.evictable.idle.time**", "The minimum amount of time an object may sit idle in the pool before it is eligible for eviction.", "", "60 seconds", "", ""
   "**pool.time.between.eviction.runs**", "The amount of time between attempting to evict idle connections from the pool.", "", "30 seconds", "", ""
   "**pool.num.tests.per.eviction.run**", "The number of connections to tests per eviction attempt. A negative value indicates to test all connections.", "", "-1", "", ""
   "**pool.test.on.create**", "Whether or not connections should be tested upon creation.", "true, false", "false", "", ""
   "**pool.test.on.borrow**", "Whether or not connections should be tested upon borrowing from the pool.", "true, false", "false", "", ""
   "**pool.test.on.return**", "Whether or not connections should be tested upon returning to the pool.", "true, false", "false", "", ""
   "**pool.test.while.idle**", "Whether or not connections should be tested while idle.", "true, false", "true", "", ""
   "**record.recordSerializer**", "the way to serialize/deserialize the record", "kryo serialization (serialize events as json blocs), avro serialization (serialize events as json blocs), avro serialization (serialize events as avro blocs), byte array serialization (serialize events as byte arrays), Kura Protobuf serialization (serialize events as Kura protocol buffer), no serialization (send events as bytes)", "com.hurence.logisland.serializer.JsonSerializer", "", ""

----------

.. _com.hurence.logisland.processor.RemoveFields: 

RemoveFields
------------
Removes a list of fields defined by a comma separated list of field names or keeps only fields defined by a comma separated list of field names.

Module
______
com.hurence.logisland:logisland-processor-common:1.1.2

Class
_____
com.hurence.logisland.processor.RemoveFields

Tags
____
record, fields, remove, delete, keep

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
 !!!! WARNING !!!!

The RunPython processor is currently an experimental feature : it is delivered as is, with the current set of features and is subject to modifications in API or anything else in further logisland releases without warnings. There is no tutorial yet. If you want to play with this processor, use the python-processing.yml example and send the apache logs of the index apache logs tutorial. The debug stream processor at the end of the stream should output events in stderr file of the executors from the spark console.

This processor allows to implement and run a processor written in python. This can be done in 2 ways. Either directly defining the process method code in the **script.code.process** configuration property or poiting to an external python module script file in the **script.path** configuration property. Directly defining methods is called the inline mode whereas using a script file is called the file mode. Both ways are mutually exclusive. Whether using the inline of file mode, your python code may depend on some python dependencies. If the set of python dependencies already delivered with the Logisland framework is not sufficient, you can use the **dependencies.path** configuration property to give their location. Currently only the nltk python library is delivered with Logisland.

Module
______
com.hurence.logisland:logisland-processor-scripting:1.1.2

Class
_____
com.hurence.logisland.processor.scripting.python.RunPython

Tags
____
scripting, python

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
Query matching based on `Luwak <http://www.confluent.io/blog/real-time-full-text-search-with-luwak-and-samza/>`_

you can use this processor to handle custom events defined by lucene queries
a new record is added to output each time a registered query is matched

A query is expressed as a lucene query against a field like for example: 

.. code::

   message:'bad exception'
   error_count:[10 TO *]
   bytes_out:5000
   user_name:tom*

Please read the `Lucene syntax guide <https://lucene.apache.org/core/5_5_0/queryparser/org/apache/lucene/queryparser/classic/package-summary.html#package_description>`_ for supported operations

.. warning::
   don't forget to set numeric fields property to handle correctly numeric ranges queries

Module
______
com.hurence.logisland:logisland-processor-sampling:1.1.2

Class
_____
com.hurence.logisland.processor.SampleRecords

Tags
____
analytic, sampler, record, iot, timeseries

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
Keep only distinct records based on a given field

Module
______
com.hurence.logisland:logisland-processor-common:1.1.2

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

Module
______
com.hurence.logisland:logisland-processor-common:1.1.2

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

.. _com.hurence.logisland.processor.SetJsonAsFields: 

SetJsonAsFields
---------------
The SetJsonAsFields processor reads the content of a string field containing a json  string and sets each json attribute as a field of the current record. Note that this could be achieved with the EvaluateJsonPath processor, but this implies to declare each json first level attribute in the configuration and also to know by advance every one of them. Whereas for this simple case, the SetJsonAsFields processor does not require such a configuration and will work with any incoming json, regardless of the list of first level attributes.

Module
______
com.hurence.logisland:logisland-processor-common:1.1.2

Class
_____
com.hurence.logisland.processor.SetJsonAsFields

Tags
____
json

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
Implementation of ElasticsearchClientService for Solr 5.5.5.

Module
______
com.hurence.logisland:logisland-service-solr_5_5_5-client:1.1.2

Class
_____
com.hurence.logisland.service.solr.Solr_5_5_5_ClientService

Tags
____
solr, client

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
Implementation of ChronixClientService for Solr 6 4 2

Module
______
com.hurence.logisland:logisland-service-solr_chronix_6.4.2-client:1.1.2

Class
_____
com.hurence.logisland.service.solr.Solr_6_4_2_ChronixClientService

Tags
____
solr, client

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
Implementation of ElasticsearchClientService for Solr 5.5.5.

Module
______
com.hurence.logisland:logisland-service-solr_6_6_2-client:1.1.2

Class
_____
com.hurence.logisland.service.solr.Solr_6_6_2_ClientService

Tags
____
solr, client

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
This processor is used to create a new set of fields from one field (using split).

Module
______
com.hurence.logisland:logisland-processor-common:1.1.2

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

Module
______
com.hurence.logisland:logisland-processor-common:1.1.2

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

Module
______
com.hurence.logisland:logisland-processor-common:1.1.2

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

Module
______
com.hurence.logisland:logisland-processor-common:1.1.2

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

----------

.. _com.hurence.logisland.processor.webAnalytics.URLDecoder: 

URLDecoder
----------
Decode one or more field containing an URL with possibly special chars encoded
...

Module
______
com.hurence.logisland:logisland-processor-web-analytics:1.1.2

Class
_____
com.hurence.logisland.processor.webAnalytics.URLDecoder

Tags
____
record, fields, Decode

Properties
__________
In the list below, the names of required properties appear in **bold**. Any other properties (not in bold) are considered optional. The table also indicates any default values
.

.. csv-table:: allowable-values
   :header: "Name","Description","Allowable Values","Default Value","Sensitive","EL"
   :widths: 20,60,30,20,10,10

   "**decode.fields**", "List of fields (URL) to decode", "", "null", "", ""
   "charset", "Charset to use to decode the URL", "", "UTF-8", "", ""

Dynamic Properties
__________________
Dynamic Properties allow the user to specify both the name and value of a property.

.. csv-table:: dynamic-properties
   :header: "Name","Value","Description","EL"
   :widths: 20,20,40,10

   "fields to decode", "a default value", "Decode one or more fields from the record ", ""

----------

.. _com.hurence.logisland.processor.webAnalytics.setSourceOfTraffic: 

setSourceOfTraffic
------------------
Compute the source of traffic of a web session. Users arrive at a website or application through a variety of sources, 
including advertising/paying campaigns, search engines, social networks, referring sites or direct access. 
When analysing user experience on a webshop, it is crucial to collect, process, and report the campaign and traffic-source data. 
To compute the source of traffic of a web session, the user has to provide the utm_* related properties if available
i-e: **utm_source.field**, **utm_medium.field**, **utm_campaign.field**, **utm_content.field**, **utm_term.field**)
, the referer (**referer.field** property) and the first visited page of the session (**first.visited.page.field** property).
By default the source of traffic information are placed in a flat structure (specified by the **source_of_traffic.suffix** property
 with a default value of source_of_traffic_). To work properly the setSourceOfTraffic processor needs to have access to an 
Elasticsearch index containing a list of the most popular search engines and social networks. The ES index (specified by the **es.index** property) should be structured such that the _id of an ES document MUST be the name of the domain. If the domain is a search engine, the related ES doc MUST have a boolean field (default being search_engine) specified by the property **es.search_engine.field** with a value set to true. If the domain is a social network , the related ES doc MUST have a boolean field (default being social_network) specified by the property **es.social_network.field** with a value set to true. 

Module
______
com.hurence.logisland:logisland-processor-web-analytics:1.1.2

Class
_____
com.hurence.logisland.processor.webAnalytics.SetSourceOfTraffic

Tags
____
session, traffic, source, web, analytics

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
