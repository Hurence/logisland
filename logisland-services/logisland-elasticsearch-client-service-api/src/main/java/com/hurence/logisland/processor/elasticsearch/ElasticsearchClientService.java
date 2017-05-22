package com.hurence.logisland.processor.elasticsearch;


import com.hurence.logisland.annotation.documentation.CapabilityDescription;
import com.hurence.logisland.annotation.documentation.Tags;
import com.hurence.logisland.component.AllowableValue;
import com.hurence.logisland.component.PropertyDescriptor;
import com.hurence.logisland.controller.ControllerService;
import com.hurence.logisland.processor.elasticsearch.multiGet.MultiGetQueryRecord;
import com.hurence.logisland.processor.elasticsearch.multiGet.MultiGetResponseRecord;
import com.hurence.logisland.record.Record;
import com.hurence.logisland.validator.StandardValidators;
import com.hurence.logisland.validator.ValidationResult;
import com.hurence.logisland.validator.Validator;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;


@Tags({"elasticsearch", "client"})
@CapabilityDescription("A controller service for accessing an elasticsearch client.")
public interface ElasticsearchClientService extends ControllerService {

    //////////////////////////////////////
    // Properties of the backoff policy //
    //////////////////////////////////////

    AllowableValue NO_BACKOFF_POLICY = new AllowableValue("noBackoff", "No retry policy",
            "when a request fail there won't be any retry.");

    AllowableValue CONSTANT_BACKOFF_POLICY = new AllowableValue("constantBackoff", "wait a fixed amount of time between retries",
            "wait a fixed amount of time between retries, using user put retry number and throttling delay");

    AllowableValue EXPONENTIAL_BACKOFF_POLICY = new AllowableValue("exponentialBackoff", "custom exponential policy",
            "time waited between retries grow exponentially, using user put retry number and throttling delay");

    AllowableValue DEFAULT_EXPONENTIAL_BACKOFF_POLICY = new AllowableValue("defaultExponentialBackoff", "es default exponential policy",
            "time waited between retries grow exponentially, using es default parameters");

    PropertyDescriptor BULK_BACK_OFF_POLICY = new PropertyDescriptor.Builder()
            .name("backoff.policy")
            .description("strategy for retrying to execute requests in bulkRequest")
            .required(true)
            .allowableValues(NO_BACKOFF_POLICY, CONSTANT_BACKOFF_POLICY, EXPONENTIAL_BACKOFF_POLICY, DEFAULT_EXPONENTIAL_BACKOFF_POLICY)
            .defaultValue(DEFAULT_EXPONENTIAL_BACKOFF_POLICY.getValue())
            .build();

    PropertyDescriptor BULK_RETRY_NUMBER = new PropertyDescriptor.Builder()
            .name("num.retry")
            .description("number of time we should try to inject a bulk into es")
            .required(true)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .defaultValue("3")
            .build();

    PropertyDescriptor BULK_THROTTLING_DELAY = new PropertyDescriptor.Builder()
            .name("throttling.delay")
            .description("number of time we should wait between each retry (in milliseconds)")
            .required(true)
            .addValidator(StandardValidators.POSITIVE_LONG_VALIDATOR)
            .defaultValue("500")
            .build();

    ////////////////////////////////////////////////
    // Properties of elasticsearch bulk processor //
    ////////////////////////////////////////////////

    PropertyDescriptor BATCH_SIZE = new PropertyDescriptor.Builder()
            .name("batch.size")
            .description("The preferred number of Records to setField to the database in a single transaction")
            .required(false)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .defaultValue("1000")
            .build();

    PropertyDescriptor BULK_SIZE = new PropertyDescriptor.Builder()
            .name("bulk.size")
            .description("bulk size in MB")
            .required(false)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .defaultValue("5")
            .build();

    PropertyDescriptor FLUSH_INTERVAL = new PropertyDescriptor.Builder()
            .name("flush.interval")
            .description("flush interval in sec")
            .required(false)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .defaultValue("5")
            .build();

    PropertyDescriptor CONCURRENT_REQUESTS = new PropertyDescriptor.Builder()
            .name("concurrent.requests")
            .description("setConcurrentRequests")
            .required(false)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .defaultValue("2")
            .build();

    //////////////////////
    // Other properties //
    //////////////////////

    /**
     * This validator ensures the Elasticsearch hosts property is a valid list of hostname:port entries
     */
    Validator HOSTNAME_PORT_VALIDATOR = new Validator() {
        @Override
        public ValidationResult validate(final String subject, final String input) {
            final List<String> esList = Arrays.asList(input.split(","));
            for (String hostnamePort : esList) {
                String[] addresses = hostnamePort.split(":");
                // Protect against invalid input like http://127.0.0.1:9300 (URL scheme should not be there)
                if (addresses.length != 2) {
                    return new ValidationResult.Builder().subject(subject).input(input).explanation(
                            "Must be in hostname:port form (no scheme such as http://").valid(false).build();
                }
            }
            return new ValidationResult.Builder().subject(subject).input(input).explanation(
                    "Valid cluster definition").valid(true).build();
        }
    };

    PropertyDescriptor CLUSTER_NAME = new PropertyDescriptor.Builder()
            .name("cluster.name")
            .description("Name of the ES cluster (for example, elasticsearch_brew). Defaults to 'elasticsearch'")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .defaultValue("elasticsearch")
            .build();

    PropertyDescriptor HOSTS = new PropertyDescriptor.Builder()
            .name("hosts")
            .description("ElasticSearch Hosts, which should be comma separated and colon for hostname/port "
                    + "host1:port,host2:port,....  For example testcluster:9300.")
            .required(true)
            .expressionLanguageSupported(false)
            .addValidator(HOSTNAME_PORT_VALIDATOR)
            .build();

    PropertyDescriptor PROP_SSL_CONTEXT_SERVICE = new PropertyDescriptor.Builder()
            .name("ssl.context.service")
            .description("The SSL Context Service used to provide client certificate information for TLS/SSL "
                    + "connections. This service only applies if the Shield plugin is available.")
            .required(false)
            .build();

    PropertyDescriptor PROP_SHIELD_LOCATION = new PropertyDescriptor.Builder()
            .name("shield.location")
            .description("Specifies the path to the JAR for the Elasticsearch Shield plugin. "
                    + "If the Elasticsearch cluster has been secured with the Shield plugin, then the Shield plugin "
                    + "JAR must also be available to this processor. Note: Do NOT place the Shield JAR into NiFi's "
                    + "lib/ directory, doing so will prevent the Shield plugin from being loaded.")
            .required(false)
            .addValidator(StandardValidators.FILE_EXISTS_VALIDATOR)
            .build();

    PropertyDescriptor USERNAME = new PropertyDescriptor.Builder()
            .name("username")
            .description("Username to access the Elasticsearch cluster")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    PropertyDescriptor PASSWORD = new PropertyDescriptor.Builder()
            .name("password")
            .description("Password to access the Elasticsearch cluster")
            .required(false)
            .sensitive(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    PropertyDescriptor PING_TIMEOUT = new PropertyDescriptor.Builder()
            .name("ping.timeout")
            .description("The ping timeout used to determine when a node is unreachable. " +
                    "For example, 5s (5 seconds). If non-local recommended is 30s")
            .required(true)
            .defaultValue("5s")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    PropertyDescriptor SAMPLER_INTERVAL = new PropertyDescriptor.Builder()
            .name("sampler.interval")
            .description("How often to sample / ping the nodes listed and connected. For example, 5s (5 seconds). "
                    + "If non-local recommended is 30s.")
            .required(true)
            .defaultValue("5s")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    PropertyDescriptor CHARSET = new PropertyDescriptor.Builder()
            .name("charset")
            .description("Specifies the character set of the document data.")
            .required(true)
            .defaultValue("UTF-8")
            .addValidator(StandardValidators.CHARACTER_SET_VALIDATOR)
            .build();

    /**
     * Flush the bulk processor.
     */
    void flushBulkProcessor();

    /**
     * Get a list of documents based on their index, type and id.
     *
     * @param multiGetQueryRecords list of MultiGetQueryRecord to fetch
     * @return the list of fetched MultiGetResponseRecord records
     */
    List<MultiGetResponseRecord> multiGet(List<MultiGetQueryRecord> multiGetQueryRecords);

    /**
     * Put a given document in elasticsearch bulk processor.
     *
     * @param docIndex index name
     * @param docType type name
     * @param document document to index
     */
    void bulkPut(String docIndex, String docType, String document, Optional<String> OptionalId);

    /**
     * Put a given document in elasticsearch bulk processor.
     *
     * @param docIndex index name
     * @param docType type name
     * @param document document to index
     */
    void bulkPut(String docIndex, String docType, Map<String, ?> document, Optional<String> OptionalId);

    /**
     * Return true if the specified index exists (also true if the name is an alias to an index).
     */
    boolean existsIndex(String indexName) throws IOException ;

    /**
     * Wait until the specified index has integrated all previously-saved data.
     */
    void refreshIndex(String indexName) throws Exception ;

    /**
     * Save the specified object to the index.
     */
    void saveAsync(String indexName, String doctype, Map<String, Object> doc) throws Exception;

    /**
     * Save the specified object to the index.
     */
    void saveSync(String indexName, String doctype, Map<String, Object> doc) throws Exception;

    /**
     * Return the number of documents in the index.
     */
    long countIndex(String indexName) throws Exception;

    /**
     * Create the specified index.
     */
    void createIndex(int numShards, int numReplicas, String indexName) throws IOException;

    /**
     * Delete the specified index.
     */
    void dropIndex(String indexName) throws IOException;

    /**
     * Copy the contents of srcIndex into dstIndex.
     * <p>
     * Although ES provides a "reindex" REST endpoint, it does so via a "standard extension module" rather than
     * implementing the logic in ES core itself. This means there is no reindex java API; we must implement
     * reindexing as a search-scroll loop.
     * </p>
     * <p>
     * Credits: http://blog.davidvassallo.me/2016/10/11/elasticsearch-java-tips-for-faster-re-indexing/
     * </p>
     */
    void copyIndex(String reindexScrollTimeout, String srcIndex, String dstIndex) throws IOException;

    /**
     * Creates an alias.
     */
    void createAlias(String indexName, String aliasName) throws IOException;

    /**
     * Adds a mapping to an index, or overwrites an existing mapping.
     * <p>
     * If the new mapping is "not compatible" with the index, then false is returned. If a system-error occurred
     * while updating the index, an exception is thrown.
     * </p>
     */
    boolean putMapping(String indexName, String doctype, String mappingAsJsonString)
            throws IOException;

    /**
     * Number of Hits of a given search query.
     */
    long searchNumberOfHits(String docIndex, String docType, String docName, String docValue);

    /**
     * Converts a record into a string
     */
    String convertRecordToString(Record record);

}
