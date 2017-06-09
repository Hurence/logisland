package com.hurence.logisland.service.rocksdb;

import com.hurence.logisland.component.PropertyDescriptor;
import com.hurence.logisland.processor.AbstractProcessor;
import com.hurence.logisland.processor.ProcessContext;
import com.hurence.logisland.record.Record;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * Created by gregoire on 06/06/17.
 */
public class TestProcessor extends AbstractProcessor {

    static final PropertyDescriptor ROCKSDB_CLIENT_SERVICE = new PropertyDescriptor.Builder()
            .name("rocksdb.client.service")
            .description("RocksDb client service")
            .identifiesControllerService(RocksdbClientService.class)
            .required(true)
            .build();


    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        List<PropertyDescriptor> propDescs = new ArrayList<>();
        propDescs.add(ROCKSDB_CLIENT_SERVICE);
        return propDescs;
    }

    @Override
    public Collection<Record> process(ProcessContext context, Collection<Record> records) {
        return Collections.emptyList();
    }


}
