Components
==========
You'll find here the list of all usable Processors, Engines, Services and other components that can be usable out of the box in your analytics streams


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

.. _com.hurence.logisland.processor.consolidateSession.ConsolidateSession: 

ConsolidateSession
------------------
The ConsolidateSession processor is the Logisland entry point to get and process events from the Web Analytics.As an example here is an incoming event from the Web Analytics:

"fields": [{ "name": "timestamp",              "type": "long" },{ "name": "remoteHost",             "type": "string"},{ "name": "record_type",            "type": ["null", "string"], "default": null },{ "name": "record_id",              "type": ["null", "string"], "default": null },{ "name": "location",               "type": ["null", "string"], "default": null },{ "name": "hitType",                "type": ["null", "string"], "default": null },{ "name": "eventCategory",          "type": ["null", "string"], "default": null },{ "name": "eventAction",            "type": ["null", "string"], "default": null },{ "name": "eventLabel",             "type": ["null", "string"], "default": null },{ "name": "localPath",              "type": ["null", "string"], "default": null },{ "name": "q",                      "type": ["null", "string"], "default": null },{ "name": "n",                      "type": ["null", "int"],    "default": null },{ "name": "referer",                "type": ["null", "string"], "default": null },{ "name": "viewportPixelWidth",     "type": ["null", "int"],    "default": null },{ "name": "viewportPixelHeight",    "type": ["null", "int"],    "default": null },{ "name": "screenPixelWidth",       "type": ["null", "int"],    "default": null },{ "name": "screenPixelHeight",      "type": ["null", "int"],    "default": null },{ "name": "partyId",                "type": ["null", "string"], "default": null },{ "name": "sessionId",              "type": ["null", "string"], "default": null },{ "name": "pageViewId",             "type": ["null", "string"], "default": null },{ "name": "is_newSession",          "type": ["null", "boolean"],"default": null },{ "name": "userAgentString",        "type": ["null", "string"], "default": null },{ "name": "pageType",               "type": ["null", "string"], "default": null },{ "name": "UserId",                 "type": ["null", "string"], "default": null },{ "name": "B2Bunit",                "type": ["null", "string"], "default": null },{ "name": "pointOfService",         "type": ["null", "string"], "default": null },{ "name": "companyID",              "type": ["null", "string"], "default": null },{ "name": "GroupCode",              "type": ["null", "string"], "default": null },{ "name": "userRoles",              "type": ["null", "string"], "default": null },{ "name": "is_PunchOut",            "type": ["null", "string"], "default": null }]The ConsolidateSession processor groups the records by sessions and compute the duration between now and the last received event. If the distance from the last event is beyond a given threshold (by default 30mn), then the session is considered closed.The ConsolidateSession is building an aggregated session object for each active session.This aggregated object includes: - The actual session duration. - A boolean representing wether the session is considered active or closed.   Note: it is possible to ressurect a session if for instance an event arrives after a session has been marked closed. - User related infos: userId, B2Bunit code, groupCode, userRoles, companyId - First visited page: URL - Last visited page: URL The properties to configure the processor are: - sessionid.field:          Property name containing the session identifier (default: sessionId). - timestamp.field:          Property name containing the timestamp of the event (default: timestamp). - session.timeout:          Timeframe of inactivity (in seconds) after which a session is considered closed (default: 30mn). - visitedpage.field:        Property name containing the page visited by the customer (default: location). - fields.to.return:         List of fields to return in the aggregated object. (default: N/A)

Class
_____
com.hurence.logisland.processor.consolidateSession.ConsolidateSession

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
.

.. csv-table:: allowable-values
   :header: "Name","Description","Allowable Values","Default Value","Sensitive","EL"
   :widths: 20,60,30,20,10,10

   "**elasticsearch.client.service**", "The instance of the Controller Service to use for accessing Elasticsearch.", "", "null", "", ""
   "**record.key**", "The name of field in the input record containing the document id to use in ES multiget query", "", "null", "", ""
   "**es.index**", "The name of the ES index to use in multiget query. ", "", "null", "", ""
   "es.type", "The name of the ES type to use in multiget query. ", "", "null", "", ""
   "es.includes.field", "The name of the ES fields to include in the record.", "", "*", "", ""
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
   "**fields.to.hash**", "the comma separated list of field names (e.g. : 'policyid,date_raw'", "", "record_raw_value", "", ""
   "**hash.charset**", "the charset to use to hash id string (e.g. 'UTF-8')", "", "UTF-8", "", ""
   "**hash.algorithm**", "the algorithme to use to hash id string (e.g. 'SHA-256'", "SHA-384, SHA-224, SHA-256, MD2, SHA, SHA-512, MD5", "SHA-256", "", ""
   "java.formatter.string", "the format to use to build id string (e.g. '%4$2s %3$2s %2$2s %1$2s' (see java Formatter)", "", "null", "", ""
   "**language.tag**", "the language to use to format numbers in string", "aa, ab, ae, af, ak, am, an, ar, as, av, ay, az, ba, be, bg, bh, bi, bm, bn, bo, br, bs, ca, ce, ch, co, cr, cs, cu, cv, cy, da, de, dv, dz, ee, el, en, eo, es, et, eu, fa, ff, fi, fj, fo, fr, fy, ga, gd, gl, gn, gu, gv, ha, he, hi, ho, hr, ht, hu, hy, hz, ia, id, ie, ig, ii, ik, in, io, is, it, iu, iw, ja, ji, jv, ka, kg, ki, kj, kk, kl, km, kn, ko, kr, ks, ku, kv, kw, ky, la, lb, lg, li, ln, lo, lt, lu, lv, mg, mh, mi, mk, ml, mn, mo, mr, ms, mt, my, na, nb, nd, ne, ng, nl, nn, no, nr, nv, ny, oc, oj, om, or, os, pa, pi, pl, ps, pt, qu, rm, rn, ro, ru, rw, sa, sc, sd, se, sg, si, sk, sl, sm, sn, so, sq, sr, ss, st, su, sv, sw, ta, te, tg, th, ti, tk, tl, tn, to, tr, ts, tt, tw, ty, ug, uk, ur, uz, ve, vi, vo, wa, wo, xh, yi, yo, za, zh, zu", "en", "", ""

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

   "**conflict.resolution.policy**", "waht to do when a field with the same name already exists ?", "nothing to do (leave record as it was), overwrite existing field (if field already exist), keep only old field and delete the other (keep only old field and delete the other), keep old field and new one (creates an alias for the new field)", "do_nothing", "", ""

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

   "debug", "Enable debug. If enabled, the original JSON string is embedded in the record_value field of the record.", "", "null", "", ""

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

   "debug", "Enable debug. If enabled, the original JSON string is embedded in the record_value field of the record.", "", "null", "", ""
   "output.record.type", "the output type of the record", "", "netflowevent", "", ""
   "enrich.record", "Enrich data. If enabledthe netflow record is enriched with inferred data", "", "false", "", ""

----------

.. _com.hurence.logisland.processor.networkpacket.ParseNetworkPacket: 

ParseNetworkPacket
------------------
The ParseNetworkPacket processor is the LogIsland entry point to parse network packets captured either off-the-wire (stream mode) or in pcap format (batch mode).  In batch mode, the processor decodes the bytes of the incoming pcap record, where a Global header followed by a sequence of [packet header, packet data] pairs are stored. Then, each incoming pcap event is parsed into n packet records. The fields of packet headers are then extracted and made available in dedicated record fields. See the `Capturing Network packets tutorial <http://logisland.readthedocs.io/en/latest/tutorials/indexing-network-packets.html>`_ for an example of usage of this processor.

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

.. _com.hurence.logisland.processor.RegexpProcessor: 

RegexpProcessor
---------------
This processor is used to create a new set of fields from one field (using regexp).

Class
_____
com.hurence.logisland.processor.RegexpProcessor

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
`com.hurence.logisland.processor.RegexpProcessor`_ 

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

.. _com.hurence.logisland.processor.scripting.python.RunPython: 

RunPython
---------
 !!!! WARNING !!!!

The RunPython processor is currently an experimental feature : it is delivered as is, with the current set of features and is subject to modifications in API or anything else in further logisland releases without warnings. There is no tutorial yet. If you want to play with this processor, use the python-processing.yml example and send the apache logs of the index apache logs tutorial. The debug stream processor at the end of the stream should output events in stderr file of the executors from the spark console.

This processor allows to implement and run a processor written in python. This can be done in 2 ways. Either directly defining the process method code in the **script.code.process** configuration property or poiting to an external python module script file in the **script.path** configuration property. Directly defining methods is called the inline mode whereas using a script file is called the file mode. Both ways are mutually exclusive. Whether using the inline of file mode, your python code may depend on some python dependencies. If the set of python dependencies already delivered with the Logisland framework is not sufficient, you can use the **dependencies.path** configuration property to give their location. Currently only the nltk python library is delivered with Logisland.

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

   "script.code.imports", "For inline mode only. This is the pyhton code that should hold the import statements if required.", "", "null", "", ""
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
   "key.fields", "a comma separated list of fields corresponding to matching groups for the message key", "", "record_raw_key", "", ""
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
   "key.fields", "a comma separated list of fields corresponding to matching groups for the message key", "", "record_raw_key", "", ""
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

.. _com.hurence.logisland.processor.ml.runDeepLearning: 

runDeepLearning
---------------
The ml processor has been writteen to describe how to implement machine Learning Capabilities to logismllandIt relies on deeplearning4j lmibraries to load the model and to run the Neural Network prediction

Class
_____
com.hurence.logisland.processor.ml.runDeepLearning

Tags
____
machine learning, deep learning

Properties
__________
In the list below, the names of required properties appear in **bold**. Any other properties (not in bold) are considered optional. The table also indicates any default values
.

.. csv-table:: allowable-values
   :header: "Name","Description","Allowable Values","Default Value","Sensitive","EL"
   :widths: 20,60,30,20,10,10

   "debug", "Enable debug.", "", "null", "", ""
   "**ml.model.file.path**", "the path of the ml model", "", "null", "", ""
