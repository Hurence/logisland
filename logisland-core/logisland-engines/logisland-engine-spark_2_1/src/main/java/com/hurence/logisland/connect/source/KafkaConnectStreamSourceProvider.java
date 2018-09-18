/**
 * Copyright (C) 2016 Hurence (support@hurence.com)
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
package com.hurence.logisland.connect.source;

import com.hurence.logisland.connect.Utils;
import com.hurence.logisland.stream.spark.provider.StreamOptions;
import org.apache.kafka.connect.source.SourceConnector;
import org.apache.kafka.connect.storage.Converter;
import org.apache.kafka.connect.storage.OffsetBackingStore;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.execution.streaming.Source;
import org.apache.spark.sql.sources.StreamSourceProvider;
import org.apache.spark.sql.types.StructType;
import scala.Option;
import scala.Tuple2;
import scala.collection.immutable.Map;

/**
 * A {@link StreamSourceProvider} capable of creating spark {@link com.hurence.logisland.stream.spark.structured.StructuredStream}
 * enabled kafka sources.
 *
 * @author amarziali
 */
public class KafkaConnectStreamSourceProvider implements StreamSourceProvider {

    @Override
    public Source createSource(SQLContext sqlContext, String metadataPath, Option<StructType> schema, String providerName, Map<String, String> parameters) {
        try {
            Converter keyConverter = Utils.createConverter(parameters.get(StreamOptions.KAFKA_CONNECT_KEY_CONVERTER().getName()).get(),
                    parameters.get(StreamOptions.KAFKA_CONNECT_KEY_CONVERTER_PROPERTIES().getName()).get(), true);
            Converter valueConverter = Utils.createConverter(parameters.get(StreamOptions.KAFKA_CONNECT_VALUE_CONVERTER().getName()).get(),
                    parameters.get(StreamOptions.KAFKA_CONNECT_VALUE_CONVERTER_PROPERTIES().getName()).get(), false);
            //create the right backing store
            String bs = parameters.get(StreamOptions.KAFKA_CONNECT_OFFSET_BACKING_STORE().getName()).get();
            java.util.Map<String, String> offsetBackingStoreProperties =
                    Utils.propertiesToMap(parameters.get(StreamOptions.KAFKA_CONNECT_OFFSET_BACKING_STORE_PROPERTIES().getName()).get());
            OffsetBackingStore offsetBackingStore = Utils.createOffsetBackingStore(bs, offsetBackingStoreProperties);

            KafkaConnectStreamSource ret = new KafkaConnectStreamSource(sqlContext,
                    Utils.propertiesToMap(parameters.get(StreamOptions.KAFKA_CONNECT_CONNECTOR_PROPERTIES().getName()).get()),
                    keyConverter,
                    valueConverter,
                    offsetBackingStore,
                    Integer.parseInt(parameters.get(StreamOptions.KAFKA_CONNECT_MAX_TASKS().getName()).get()),
                    parameters.get(StreamOptions.KAFKA_CONNECT_CONNECTOR_CLASS().getName()).get());
            ret.start();
            return ret;
        } catch (Exception e) {
            throw new IllegalArgumentException("Unable to create kafka connect stream source: " + e.getMessage(), e);
        }


    }

    @Override
    public Tuple2<String, StructType> sourceSchema(SQLContext sqlContext, Option<StructType> schema, String providerName, Map<String, String> parameters) {
        return Tuple2.<String, StructType>apply(providerName, KafkaConnectStreamSource.DATA_SCHEMA);
    }
}
