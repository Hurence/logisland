
export default ['$http', AppSettings];

function AppSettings($http) {
  return {
    version: '0.1',
    jobs_api: 'http://localhost:8081/jobs',
    topics_api: 'http://localhost:8081/topics',
    plugins_api: 'http://localhost:8081/processors',
    DEFAULT_TOPIC: {
        name: "new_topic",
        partitions: 1,
        replicationFactor: 1,
        documentation: "describe here the content of the topic",
        serializer: "com.hurence.logisland.serializer.KryoSerializer",
        businessTimeField: "record_time",
        rowkeyField: "record_id",
        recordTypeField: "record_type",
        keySchema: [
            { name: "key1", encrypted: false, indexed: true, persistent: true, optional: true, type: "STRING" },
            { name: "key2", encrypted: false, indexed: true, persistent: true, optional: true, type: "STRING" }
        ],
        valueSchema: [
            { name: "value1", encrypted: false, indexed: true, persistent: true, optional: true, type: "STRING" },
            { name: "value2", encrypted: false, indexed: true, persistent: true, optional: true, type: "STRING" }
        ]
    },
    DEFAULT_PIPELINE: {
        name: "new_pipeline",
        component: "com.hurence.logisland.stream.spark.KafkaRecordStreamParallelProcessing",
        documentation: "parse all incoming raw logs",
        config: [
            { key: "kafka.input.topics", value: "_logs" },
            { key: "kafka.output.topics", value: "_records" },
            { key: "kafka.error.topics", value: "_errors" },
            { key: "logisland.agent.pull.throttling", value: "5" }
        ],
        processors: [
            {
                name: "apache_parser",
                component: "com.hurence.logisland.processor.SplitText",
                documentation: "produce records from an apache log REGEX",
                config: [
                    { key: "record.type", value: "apache_log" },
                    { key: "value.regex", value: "(\\S+)\\s+(\\S+)\\s+(\\S+)\\s+\\[([\\w:\\/]+\\s[+\\-]\\d{4})\\]\\s+\"(\\S+)\\s+(\\S+)\\s*(\\S*)\"\\s+(\\S+)\\s+(\\S+)" },
                    { key: "value.fields", value: "src_ip,identd,user,record_time,http_method,http_query,http_version,http_status,bytes_out" }
                ]
            }
        ]
    },
    DEFAULT_JOB: { 
        name: "new_job", 
        version: "1",
        summary: {
            usedCores: 4,
            usedMemory: 512,
            status: "STOPPED",
            documentation: "what can we do for you ?"
        },
        engine: {
            name: "spark_engine",
            component: "com.hurence.logisland.engine.spark.KafkaStreamProcessingEngine",
            config: [
                { key: "spark.app.name", value: "IndexApacheLogsDemo"},
                { key: "spark.master", value:  "yarn-client"},
                { key: "spark.driver.memory", value:  "1G"},
                { key: "spark.driver.cores", value:  "1"},
                { key: "spark.executor.memory", value:  "2G"},
                { key: "spark.executor.instances", value:  "4"},
                { key: "spark.executor.cores", value:  "2"},
                { key: "spark.yarn.queue", value:  "default"},
                { key: "spark.yarn.maxAppAttempts", value:  "4"},
                { key: "spark.yarn.am.attemptFailuresValidityInterval", value:  "1h"},
                { key: "spark.yarn.max.executor.failures", value:  "20"},
                { key: "spark.yarn.executor.failuresValidityInterval", value:  "1h"},
                { key: "spark.task.maxFailures", value:  "8"},
                { key: "spark.serializer", value:  "org.apache.spark.serializer.KryoSerializer"},
                { key: "spark.streaming.batchDuration", value:  "4000"},
                { key: "spark.streaming.backpressure.enabled", value:  "false"},
                { key: "spark.streaming.unpersist", value:  "false"},
                { key: "spark.streaming.blockInterval", value:  "500"},
                { key: "spark.streaming.kafka.maxRatePerPartition", value:  "3000"},
                { key: "spark.streaming.timeout", value:  "-1"},
                { key: "spark.streaming.kafka.maxRetries", value:  "3"},
                { key: "spark.streaming.ui.retainedBatches", value:  "200"},
                { key: "spark.streaming.receiver.writeAheadLog.enable", value:  "false"},
                { key: "spark.ui.port", value:  "4050"}
            ]
        },
        streams: [
            AppSettings.DEFAULT_PIPELINE
        ] 
    }
  };
};