/*
 * Copyright (C) 2018 Hurence (support@hurence.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package com.hurence.logisland.util.kafkaconnect.source;

import com.hurence.logisland.stream.StreamProperties;
import org.apache.kafka.connect.source.SourceConnector;
import org.apache.kafka.connect.storage.Converter;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.execution.streaming.Source;
import org.apache.spark.sql.sources.StreamSourceProvider;
import org.apache.spark.sql.types.StructType;
import scala.Option;
import scala.Tuple2;
import scala.collection.immutable.Map;

import java.io.IOException;
import java.io.StringReader;
import java.util.Properties;
import java.util.stream.Collectors;

public class KafkaConnectStreamSourceProvider implements StreamSourceProvider {

    private Converter createConverter(Map<String, String> parameters, String classKey, String propertyKey)
            throws ClassNotFoundException, IllegalAccessException, InstantiationException, IOException {
        Converter ret = (Converter) Class.forName(parameters.get(classKey).get()).newInstance();
        ret.configure(propertiesToMap(parameters.get(propertyKey).get()), false);
        return ret;
    }

    private java.util.Map<String, String> propertiesToMap(String propertiesAsString) throws IOException {
        Properties props = new Properties();
        props.load(new StringReader(propertiesAsString));
        return props.entrySet().stream().collect(Collectors.toMap(e -> e.getKey().toString(), e -> e.getValue().toString()));
    }

    @Override
    public Source createSource(SQLContext sqlContext, String metadataPath, Option<StructType> schema, String providerName, Map<String, String> parameters) {
        try {
            Converter keyConverter = createConverter(parameters, StreamProperties.KAFKA_CONNECT_KEY_CONVERTER().getName(),
                    StreamProperties.KAFKA_CONNECT_KEY_CONVERTER_PROPERTIES().getName());
            Converter valueConverter = createConverter(parameters, StreamProperties.KAFKA_CONNECT_VALUE_CONVERTER().getName(),
                    StreamProperties.KAFKA_CONNECT_VALUE_CONVERTER_PROPERTIES().getName());
            return new KafkaConnectStreamSource(sqlContext,
                    propertiesToMap(parameters.get(StreamProperties.KAFKA_CONNECT_CONNECTOR_PROPERTIES().getName()).get()),
                    keyConverter,
                    valueConverter,
                    Integer.parseInt(parameters.get(StreamProperties.KAFKA_CONNECT_MAX_TASKS().getName()).get()),
                    (Class<? extends SourceConnector>) Class.forName(parameters.get(StreamProperties.KAFKA_CONNECT_CONNECTOR_CLASS().getName()).get()));
        } catch (Exception e) {
            throw new IllegalArgumentException("Unable to create kafka connect stream source", e);
        }


    }

    @Override
    public Tuple2<String, StructType> sourceSchema(SQLContext sqlContext, Option<StructType> schema, String providerName, Map<String, String> parameters) {
        return Tuple2.<String, StructType>apply(providerName, KafkaConnectStreamSource.DATA_SCHEMA);
    }
}
