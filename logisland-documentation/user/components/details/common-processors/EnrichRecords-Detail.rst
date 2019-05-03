Enrich input records with content indexed in datastore using multiget queries.
Each incoming record must be possibly enriched with information stored in datastore.
The plugin properties are :

- es.index (String)            : Name of the datastore index on which the multiget query will be performed. This field is mandatory and should not be empty, otherwise an error output record is sent for this specific incoming record.
- record.key (String)          : Name of the field in the input record containing the id to lookup document in elastic search. This field is mandatory.
- es.key (String)              : Name of the datastore key on which the multiget query will be performed. This field is mandatory.
- includes (ArrayList<String>) : List of patterns to filter in (include) fields to retrieve. Supports wildcards. This field is not mandatory.
- excludes (ArrayList<String>) : List of patterns to filter out (exclude) fields to retrieve. Supports wildcards. This field is not mandatory.

Each outcoming record holds at least the input record plus potentially one or more fields coming from of one datastore document.