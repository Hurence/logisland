package com.hurence.logisland.config;


import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import java.io.File;
import java.io.IOException;

/**
 * {
 * "engine": {
 * "version": "0.1.0",
 * "class": "com.hurence.logisland.job.EventProcessorJob",
 * "configuration": {
 * "spark": {
 * "batchDuration": 2000,
 * "appName": "My first stream processor",
 * "spark.streaming.kafka.maxRatePerPartition": null,
 * "checkpointingDirectory": "file:///tmp",
 * "spark.ui.port": 4050,
 * "spark.streaming.blockInterval": 350,
 * "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
 * "spark.streaming.backpressure.enabled": true,
 * "spark.streaming.unpersist": false
 * },
 * "kafka": {
 * "metadata.broker.list": "sandbox:2181"
 * }
 * },
 * "documentation": "Main Logisland job entry point"
 * },
 * "version": 0.10000000000000001,
 * "processors": [
 * {
 * "version": 1.0,
 * "processor": "com.hurence.logisland.processor.QueryMatcherProcessor",
 * "configuration": {
 * "rules": [
 * {
 * "query": "incident*",
 * "name": "rule 1"
 * },
 * {
 * "query": "vehicule*",
 * "name": "rule 2"
 * }
 * ],
 * "inputTopic": "kafka_log",
 * "outputTopic": "kafka_event"
 * },
 * "documentation": null
 * },
 * {
 * "version": 1.0,
 * "processor": "com.hurence.logisland.processor.OutlierProcessor",
 * "configuration": {
 * "sketchyOutlierAlgorithm": "SKETCHY_MOVING_MAD",
 * "batchOutlierAlgorithm": {
 * "type": "RAD",
 * "config": {
 * "minAmountToPredict": 100,
 * "reservoirSize": 100,
 * "minZscorePercentile": 95,
 * "zscoreCutoffs": {
 * "MODERATE_OUTLIER": 1.5,
 * "NORMAL": 1.0000000000000001e-15
 * }
 * }
 * },
 * "inputTopic": "kafka_log",
 * "rotationPolicy": {
 * "amount": 100,
 * "type": "BY_AMOUNT",
 * "unit": "POINTS"
 * },
 * "chunkingPolicy": {
 * "amount": 10,
 * "type": "BY_AMOUNT",
 * "unit": "POINTS"
 * },
 * "globalStatistics": null,
 * "outputTopic": "kafka_event"
 * },
 * "documentation": null
 * }
 * ],
 * "documentation": "LogIsland analytics main config file Put here every engine or processor config"
 * }
 */
public class LogislandSessionConfigReader {


    public LogislandSessionConfiguration loadConfig(String configFilePath) throws Exception {
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        File configFile = new File(configFilePath);

        return mapper.readValue(configFile, LogislandSessionConfiguration.class);
    }

}
