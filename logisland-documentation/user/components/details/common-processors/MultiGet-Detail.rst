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

  - field name : the retrieved field name
  - field value : the retrieved field value
