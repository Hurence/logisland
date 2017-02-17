
package com.hurence.logisland.kakfa.registry;

/**
 * Copyright 2014 Confluent Inc.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import com.hurence.logisland.agent.rest.model.Job;
import com.hurence.logisland.agent.rest.model.JobSummary;
import com.hurence.logisland.kakfa.registry.exceptions.RegistryException;
import com.hurence.logisland.kakfa.registry.exceptions.RegistryInitializationException;
import com.hurence.logisland.kakfa.registry.exceptions.RegistryStoreException;
import com.hurence.logisland.kakfa.registry.exceptions.RegistryTimeoutException;
import com.hurence.logisland.kakfa.serialization.Serializer;
import com.hurence.logisland.kakfa.store.*;
import com.hurence.logisland.kakfa.zookeeper.RegistryIdentity;
import com.hurence.logisland.kakfa.zookeeper.ZookeeperMasterElector;
import io.confluent.common.metrics.*;
import io.confluent.common.metrics.stats.Gauge;
import io.confluent.common.utils.SystemTime;
import io.confluent.kafka.schemaregistry.client.rest.RestService;
import io.confluent.kafka.schemaregistry.rest.SchemaRegistryConfig;
import io.confluent.rest.Application;
import io.confluent.rest.RestConfig;
import kafka.utils.ZkUtils;
import org.I0Itec.zkclient.exception.ZkNodeExistsException;
import org.apache.avro.reflect.Nullable;
import org.apache.commons.collections.IteratorUtils;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.security.JaasUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.net.URI;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.concurrent.TimeUnit;


public class KafkaRegistry {

    /**
     * Schema versions under a particular subject are indexed from MIN_VERSION.
     */
    public static final int ZOOKEEPER_SCHEMA_ID_COUNTER_BATCH_SIZE = 20;
    public static final String ZOOKEEPER_SCHEMA_ID_COUNTER = "/schema_id_counter";
    private static final int ZOOKEEPER_SCHEMA_ID_COUNTER_BATCH_WRITE_RETRY_BACKOFF_MS = 50;
    private static final Logger log = LoggerFactory.getLogger(KafkaRegistry.class);

    private final RegistryIdentity myIdentity;
    private final Object masterLock = new Object();
    private final String schemaRegistryZkNamespace;
    private final String kafkaClusterZkUrl;
    private final int zkSessionTimeoutMs;
    private final boolean isEligibleForMasterElector;
    private String schemaRegistryZkUrl;
    private ZkUtils zkUtils;
    private RegistryIdentity masterIdentity;
    private RestService masterRestService;
    private ZookeeperMasterElector masterElector = null;
    private Metrics metrics;
    private Sensor masterNodeSensor;

    private final KafkaStoreService jobsKafkaStore;


    // Hand out this id during the next schema registration. Indexed from 1.
    private int nextAvailableSchemaId;
    // Tracks the upper bound of the current id batch (inclusive). When nextAvailableSchemaId goes
    // above this value, it's time to allocate a new batch of ids
    private int idBatchInclusiveUpperBound;
    // Track the largest id in the kafka store so far (-1 indicates none in the store)
    // This is automatically updated by the KafkaStoreReaderThread every time a new Schema is added
    // Used to ensure that any newly allocated batch of ids does not overlap
    // with any id in the kafkastore. Primarily for bootstrapping the SchemaRegistry when
    // data is already in the kafkastore.
    private int maxIdInKafkaStore = -1;

    public KafkaRegistry(SchemaRegistryConfig config,
                         Serializer<RegistryKey, RegistryValue> serializer)
            throws RegistryException {

        String host = config.getString(SchemaRegistryConfig.HOST_NAME_CONFIG);
        int port = getPortForIdentity(
                config.getInt(SchemaRegistryConfig.PORT_CONFIG),
                config.getList(RestConfig.LISTENERS_CONFIG));
        this.schemaRegistryZkNamespace = config.getString(SchemaRegistryConfig.SCHEMAREGISTRY_ZK_NAMESPACE);
        this.isEligibleForMasterElector = config.getBoolean(SchemaRegistryConfig.MASTER_ELIGIBILITY);
        this.myIdentity = new RegistryIdentity(host, port, isEligibleForMasterElector);
        this.kafkaClusterZkUrl = config.getString(SchemaRegistryConfig.KAFKASTORE_CONNECTION_URL_CONFIG);
        this.zkSessionTimeoutMs = config.getInt(SchemaRegistryConfig.KAFKASTORE_ZK_SESSION_TIMEOUT_MS_CONFIG);


        jobsKafkaStore = new KafkaStoreService(this, config, serializer);


        /**
         * metrology section
         */
        MetricConfig metricConfig =
                new MetricConfig().samples(config.getInt(ProducerConfig.METRICS_NUM_SAMPLES_CONFIG))
                        .timeWindow(config.getLong(ProducerConfig.METRICS_SAMPLE_WINDOW_MS_CONFIG),
                                TimeUnit.MILLISECONDS);
        List<MetricsReporter> reporters =
                config.getConfiguredInstances(ProducerConfig.METRIC_REPORTER_CLASSES_CONFIG,
                        MetricsReporter.class);
        String jmxPrefix = "logisland.kafka.registry";
        reporters.add(new JmxReporter(jmxPrefix));
        this.metrics = new Metrics(metricConfig, reporters, new SystemTime());
        this.masterNodeSensor = metrics.sensor("master-slave-role");
        MetricName m = new MetricName("master-slave-role", "master-slave-role",
                "1.0 indicates the node is the active master in the cluster and is the"
                        + " node where all register schema and config update requests are "
                        + "served.");
        this.masterNodeSensor.add(m, new Gauge());
    }


    public Job addJob(Job job) throws RegistryException {


        Long jobId = job.getId();
        if (jobId != null && jobId >= 0) {
            job.setId(jobId);
        } else {
            job.setId((long) nextAvailableSchemaId);
            nextAvailableSchemaId++;
        }
        if (reachedEndOfIdBatch()) {
            idBatchInclusiveUpperBound = getInclusiveUpperBound(nextSchemaIdCounterBatch());
        }

        if(job.getSummary() == null)
            job.setSummary(new JobSummary());
        job.getSummary().dateModified(new Date());


        JobKey key = new JobKey(job.getName(), 1);
        JobValue value = new JobValue(job.getName(), job.getVersion(), job.getId(), job);

        JobValue jobAdded = (JobValue) jobsKafkaStore.create(key, value);
        return jobAdded.getJob();
    }

    public Job updateJob(Job job) throws RegistryException {


        Integer version = job.getVersion() + 1;
        job.version(version);

        if(job.getSummary() == null)
            job.setSummary(new JobSummary());
        job.getSummary().dateModified(new Date());

        JobKey key = new JobKey(job.getName(), 1);
        JobValue value = new JobValue(job.getName(), job.getVersion(), job.getId(), job);

        JobValue jobAdded = (JobValue) jobsKafkaStore.update(key, value);
        return jobAdded.getJob();
    }

    public Job getJob(String jobId) throws RegistryException {

        JobKey key = new JobKey(jobId, 1);
        JobValue value = (JobValue) jobsKafkaStore.get(key);
        return value.getJob();
    }

    public void deleteJob(String jobId) throws RegistryException {

        JobKey key = new JobKey(jobId, 1);
        jobsKafkaStore.delete(key);
    }

    public List<Job> getAllJobs() throws RegistryException {
        return IteratorUtils.toList(
                jobsKafkaStore.getAll()
                        .stream()
                        .map(value -> ((JobValue) value).getJob())
                        .iterator()
        );
    }

    /**
     * A Schema Registry instance's identity is in part the port it listens on. Currently the
     * port can either be configured via the deprecated `port` configuration, or via the `listeners`
     * configuration.
     * <p>
     * This method uses `Application.parseListeners()` from `rest-utils` to get a list of listeners, and
     * returns the port of the first listener to be used for the instance's identity.
     * <p>
     * In theory, any port from any listener would be sufficient. Choosing the first, instead of say the last,
     * is arbitrary.
     */
    // TODO: once RestConfig.PORT_CONFIG is deprecated, remove the port parameter.
    public static int getPortForIdentity(int port, List<String> configuredListeners) {
        List<URI> listeners = Application.parseListeners(configuredListeners, port,
                Arrays.asList("http", "https"), "http");
        return listeners.get(0).getPort();
    }


    public void init() throws RegistryInitializationException {
        try {
            jobsKafkaStore.init();
        } catch (Exception e) {
            throw new RegistryInitializationException(
                    "Error initializing kafka store while initializing schema registry", e);
        }

        try {
            createZkNamespace();
            masterElector = new ZookeeperMasterElector(zkUtils, myIdentity, this,
                    isEligibleForMasterElector);
        } catch (RegistryStoreException e) {
            throw new RegistryInitializationException(
                    "Error electing master while initializing schema registry", e);
        } catch (RegistryTimeoutException e) {
            throw new RegistryInitializationException(e);
        }
    }

    private void createZkNamespace() {
        int kafkaNamespaceIndex = kafkaClusterZkUrl.indexOf("/");
        String zkConnForNamespaceCreation = kafkaNamespaceIndex > 0 ?
                kafkaClusterZkUrl.substring(0, kafkaNamespaceIndex) :
                kafkaClusterZkUrl;

        String schemaRegistryNamespace = "/" + schemaRegistryZkNamespace;
        schemaRegistryZkUrl = zkConnForNamespaceCreation + schemaRegistryNamespace;

        ZkUtils zkUtilsForNamespaceCreation = ZkUtils.apply(
                zkConnForNamespaceCreation,
                zkSessionTimeoutMs, zkSessionTimeoutMs,
                JaasUtils.isZkSecurityEnabled());
        // create the zookeeper namespace using cluster.name if it doesn't already exist
        zkUtilsForNamespaceCreation.makeSurePersistentPathExists(
                schemaRegistryNamespace,
                zkUtilsForNamespaceCreation.DefaultAcls());
        log.info("Created schema registry namespace " +
                zkConnForNamespaceCreation + schemaRegistryNamespace);
        zkUtilsForNamespaceCreation.close();
        this.zkUtils = ZkUtils.apply(
                schemaRegistryZkUrl, zkSessionTimeoutMs, zkSessionTimeoutMs,
                JaasUtils.isZkSecurityEnabled());
    }

    public ZkUtils zkUtils() {
        return zkUtils;
    }

    public boolean isMaster() {
        synchronized (masterLock) {
            if (masterIdentity != null && masterIdentity.equals(myIdentity)) {
                return true;
            } else {
                return false;
            }
        }
    }

    /**
     * 'Inform' this SchemaRegistry instance which SchemaRegistry is the current master.
     * If this instance is set as the new master, ensure it is up-to-date with data in
     * the kafka store, and tell Zookeeper to allocate the next batch of schema IDs.
     *
     * @param newMaster Identity of the current master. null means no master is alive.
     * @throws RegistryException
     */
    public void setMaster(@Nullable RegistryIdentity newMaster)
            throws RegistryTimeoutException, RegistryStoreException {
        log.debug("Setting the master to " + newMaster);

        // Only schema registry instances eligible for master can be set to master
        if (newMaster != null && !newMaster.getMasterEligibility()) {
            throw new IllegalStateException(
                    "Tried to set an ineligible node to master: " + newMaster);
        }

        synchronized (masterLock) {
            RegistryIdentity previousMaster = masterIdentity;
            masterIdentity = newMaster;

            if (masterIdentity == null) {
                masterRestService = null;
            } else {
                masterRestService = new RestService(String.format("http://%s:%d",
                        masterIdentity.getHost(), masterIdentity.getPort()));
            }

            if (masterIdentity != null && !masterIdentity.equals(previousMaster) && isMaster()) {
                nextAvailableSchemaId = nextSchemaIdCounterBatch();
                idBatchInclusiveUpperBound = getInclusiveUpperBound(nextAvailableSchemaId);

                // The new master may not know the exact last offset in the Kafka log. So, mark the
                // last offset invalid here and let the logic in register() deal with it later.
                //jobsKafkaStore.markLastWrittenOffsetInvalid();
                //throw new SchemaRegistryStoreException("TODO handle here jobsKafkaStore.markLastWrittenOffsetInvalid();");
            }

            masterNodeSensor.record(isMaster() ? 1.0 : 0.0);
        }
    }

    /**
     * Return json data encoding basic information about this SchemaRegistry instance, such as
     * host, port, etc.
     */
    public RegistryIdentity myIdentity() {
        return myIdentity;
    }

    /**
     * Return the identity of the SchemaRegistry that this instance thinks is current master.
     * Any request that requires writing new data gets forwarded to the master.
     */
    public RegistryIdentity masterIdentity() {
        synchronized (masterLock) {
            return masterIdentity;
        }
    }


    public void close() {
        log.info("Shutting down schema registry");
        jobsKafkaStore.close();
        if (masterElector != null) {
            masterElector.close();
        }
        if (zkUtils != null) {
            zkUtils.close();
        }
    }

    /**
     * Allocate and lock the next batch of schema ids. Signal a global lock over the next batch by
     * writing the inclusive upper bound of the batch to ZooKeeper. I.e. the value stored in
     * ZOOKEEPER_SCHEMA_ID_COUNTER in ZooKeeper indicates the current max allocated id for assignment.
     * <p>
     * When a schema registry server is initialized, kafka may have preexisting persistent
     * schema -> id assignments, and zookeeper may have preexisting counter data.
     * Therefore, when allocating the next batch of ids, it's necessary to ensure the entire new batch
     * is greater than the greatest id in kafka and also greater than the previously recorded batch
     * in zookeeper.
     * <p>
     * Return the first available id in the newly allocated batch of ids.
     */
    protected Integer nextSchemaIdCounterBatch() throws RegistryStoreException {
        int nextIdBatchLowerBound = 1;

        while (true) {

            if (!zkUtils.zkClient().exists(ZOOKEEPER_SCHEMA_ID_COUNTER)) {
                // create ZOOKEEPER_SCHEMA_ID_COUNTER if it already doesn't exist

                try {
                    nextIdBatchLowerBound = getNextBatchLowerBoundFromKafkaStore();
                    int nextIdBatchUpperBound = getInclusiveUpperBound(nextIdBatchLowerBound);
                    zkUtils.createPersistentPath(ZOOKEEPER_SCHEMA_ID_COUNTER,
                            String.valueOf(nextIdBatchUpperBound),
                            zkUtils.DefaultAcls());
                    return nextIdBatchLowerBound;
                } catch (ZkNodeExistsException ignore) {
                    // A zombie master may have created this zk node after the initial existence check
                    // Ignore and try again
                }
            } else { // ZOOKEEPER_SCHEMA_ID_COUNTER exists

                // read the latest counter value
                final Tuple2<String, org.apache.zookeeper.data.Stat> counterValue = zkUtils.readData(ZOOKEEPER_SCHEMA_ID_COUNTER);
                final String counterData = counterValue._1();
                final org.apache.zookeeper.data.Stat counterStat = counterValue._2();
                if (counterData == null) {
                    throw new RegistryStoreException(
                            "Failed to read schema id counter " + ZOOKEEPER_SCHEMA_ID_COUNTER +
                                    " from zookeeper");
                }

                // Compute the lower bound of next id batch based on zk data and kafkastore data
                int zkIdCounterValue = Integer.valueOf(counterData);
                int zkNextIdBatchLowerBound = zkIdCounterValue + 1;
                if (zkIdCounterValue % ZOOKEEPER_SCHEMA_ID_COUNTER_BATCH_SIZE != 0) {
                    // ZooKeeper id counter should be an integer multiple of id batch size in normal
                    // operation; handle corrupted/stale id counter data gracefully by bumping
                    // up to the next id batch

                    // fixedZkIdCounterValue is the smallest multiple of
                    // ZOOKEEPER_SCHEMA_ID_COUNTER_BATCH_SIZE greater than the bad zkIdCounterValue
                    int fixedZkIdCounterValue = ZOOKEEPER_SCHEMA_ID_COUNTER_BATCH_SIZE *
                            (1 + zkIdCounterValue / ZOOKEEPER_SCHEMA_ID_COUNTER_BATCH_SIZE);
                    zkNextIdBatchLowerBound = fixedZkIdCounterValue + 1;

                    log.warn(
                            "Zookeeper schema id counter is not an integer multiple of id batch size." +
                                    " Zookeeper may have stale id counter data.\n" +
                                    "zk id counter: " + zkIdCounterValue + "\n" +
                                    "id batch size: " + ZOOKEEPER_SCHEMA_ID_COUNTER_BATCH_SIZE);
                }
                nextIdBatchLowerBound =
                        Math.max(zkNextIdBatchLowerBound, getNextBatchLowerBoundFromKafkaStore());
                String nextIdBatchUpperBound = String.valueOf(getInclusiveUpperBound(nextIdBatchLowerBound));

                // conditionally update the zookeeper path with the upper bound of the new id batch.
                // newSchemaIdCounterDataVersion < 0 indicates a failed conditional update.
                // Most probable cause is the existence of another master which tries to do the same
                // counter batch allocation at the same time. If this happens, re-read the value and
                // continue until one master is determined to be the zombie master.
                // NOTE: The handling of multiple masters is still a TODO
                int newSchemaIdCounterDataVersion =
                        (Integer) zkUtils.conditionalUpdatePersistentPath(
                                ZOOKEEPER_SCHEMA_ID_COUNTER,
                                nextIdBatchUpperBound,
                                counterStat.getVersion(),
                                null)._2();
                if (newSchemaIdCounterDataVersion >= 0) {
                    break;
                }
            }
            try {
                // Wait a bit and attempt id batch allocation again
                Thread.sleep(ZOOKEEPER_SCHEMA_ID_COUNTER_BATCH_WRITE_RETRY_BACKOFF_MS);
            } catch (InterruptedException ignored) {
            }
        }

        return nextIdBatchLowerBound;
    }

    int getMaxIdInKafkaStore() {
        return this.maxIdInKafkaStore;
    }

    /**
     * This should only be updated by the KafkastoreReaderThread.
     */
    void setMaxIdInKafkaStore(int id) {
        this.maxIdInKafkaStore = id;
    }

    /**
     * If true, it's time to allocate a new batch of ids with a call to nextSchemaIdCounterBatch()
     */
    private boolean reachedEndOfIdBatch() {
        return nextAvailableSchemaId > idBatchInclusiveUpperBound;
    }


    /**
     * E.g. if inclusiveLowerBound is 61, and BATCH_SIZE is 20, the inclusiveUpperBound should be 80.
     */
    private int getInclusiveUpperBound(int inclusiveLowerBound) {
        return inclusiveLowerBound + ZOOKEEPER_SCHEMA_ID_COUNTER_BATCH_SIZE - 1;
    }


    /**
     * Return a minimum lower bound on the next batch of ids based on ids currently in the
     * kafka store.
     */
    private int getNextBatchLowerBoundFromKafkaStore() {
        if (this.getMaxIdInKafkaStore() <= 0) {
            return 1;
        }

        int nextBatchLowerBound = 1 + this.getMaxIdInKafkaStore() / ZOOKEEPER_SCHEMA_ID_COUNTER_BATCH_SIZE;
        return 1 + nextBatchLowerBound * ZOOKEEPER_SCHEMA_ID_COUNTER_BATCH_SIZE;
    }

}
