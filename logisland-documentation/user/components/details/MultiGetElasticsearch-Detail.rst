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
