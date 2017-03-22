package com.hurence.logisland.agent.rest.api.impl;

import com.hurence.logisland.agent.rest.api.ConfigsApiService;
import com.hurence.logisland.agent.rest.api.NotFoundException;
import com.hurence.logisland.agent.rest.model.Property;
import com.hurence.logisland.kafka.registry.KafkaRegistry;
import com.hurence.logisland.kafka.registry.KafkaRegistryConfig;

import javax.ws.rs.core.Response;
import javax.ws.rs.core.SecurityContext;
import java.util.ArrayList;
import java.util.List;


@javax.annotation.Generated(value = "class io.swagger.codegen.languages.JavaJerseyServerCodegen", date = "2017-02-28T17:12:21.474+01:00")
public class ConfigsApiServiceImpl extends ConfigsApiService {

    public ConfigsApiServiceImpl(KafkaRegistry kafkaRegistry) {
        super(kafkaRegistry);
    }

    @Override
    public Response getConfig(SecurityContext securityContext) throws NotFoundException {

        KafkaRegistryConfig config = kafkaRegistry.getConfig();
        List<Property> configs = new ArrayList<>();
        configs.add(new Property()
                .key(config.KAFKASTORE_TOPIC_JOBS_CONFIG)
                .value(config.getString(config.KAFKASTORE_TOPIC_JOBS_CONFIG)));
        configs.add(new Property()
                .key(config.KAFKASTORE_TOPIC_TOPICS_CONFIG)
                .value(config.getString(config.KAFKASTORE_TOPIC_TOPICS_CONFIG)));
        configs.add(new Property()
                .key(config.KAFKA_METADATA_BROKER_LIST_CONFIG)
                .value(config.getString(config.KAFKA_METADATA_BROKER_LIST_CONFIG)));
        configs.add(new Property()
                .key(config.KAFKA_ZOOKEEPER_QUORUM_CONFIG)
                .value(config.getString(config.KAFKA_ZOOKEEPER_QUORUM_CONFIG)));
        configs.add(new Property()
                .key(config.KAFKA_TOPIC_AUTOCREATE_CONFIG)
                .type("boolean")
                .value(String.valueOf(config.getBoolean(config.KAFKA_TOPIC_AUTOCREATE_CONFIG))));
        configs.add(new Property()
                .key(config.KAFKA_TOPIC_DEFAULT_PARTITION_CONFIG)
                .type("integer")
                .value(String.valueOf(config.getInt(config.KAFKA_TOPIC_DEFAULT_PARTITION_CONFIG))));
        configs.add(new Property()
                .key(config.KAFKA_TOPIC_DEFAULT_REPLICATION_FACTOR_CONFIG)
                .type("integer")
                .value(String.valueOf(config.getInt(config.KAFKA_TOPIC_DEFAULT_REPLICATION_FACTOR_CONFIG))));

        return Response.ok().entity(configs).build();
    }
}
