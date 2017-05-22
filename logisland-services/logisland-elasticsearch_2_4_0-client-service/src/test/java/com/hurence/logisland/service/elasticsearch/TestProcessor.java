
package com.hurence.logisland.service.elasticsearch;

import com.hurence.logisland.component.PropertyDescriptor;
import com.hurence.logisland.processor.AbstractProcessor;
import com.hurence.logisland.processor.ProcessContext;
import com.hurence.logisland.record.Record;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;



public class TestProcessor extends AbstractProcessor {

    public static final PropertyDescriptor ELASTICSEARCH_CLIENT_SERVICE = new PropertyDescriptor.Builder()
            .name("elasticsearch.client.service")
            .description("The instance of the Controller Service to use for accessing Elasticsearch.")
            .required(true)
            .identifiesControllerService(ElasticsearchClientService.class)
            .build();


    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        List<PropertyDescriptor> propDescs = new ArrayList<>();
        propDescs.add(ELASTICSEARCH_CLIENT_SERVICE);
        return propDescs;
    }

    @Override
    public Collection<Record> process(ProcessContext context, Collection<Record> records) {
        return Collections.emptyList();
    }


}
