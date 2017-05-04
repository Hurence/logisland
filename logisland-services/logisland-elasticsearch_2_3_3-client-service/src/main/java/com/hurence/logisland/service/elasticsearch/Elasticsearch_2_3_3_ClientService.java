package com.hurence.logisland.service.elasticsearch;

import com.hurence.logisland.annotation.behavior.DynamicProperty;
import com.hurence.logisland.annotation.documentation.CapabilityDescription;
import com.hurence.logisland.annotation.documentation.Tags;
import com.hurence.logisland.annotation.lifecycle.OnDisabled;
import com.hurence.logisland.annotation.lifecycle.OnEnabled;
import com.hurence.logisland.component.InitializationException;
import com.hurence.logisland.component.PropertyDescriptor;
import com.hurence.logisland.controller.AbstractControllerService;
import com.hurence.logisland.controller.ControllerServiceInitializationContext;
import com.hurence.logisland.processor.elasticsearch.ElasticsearchClientService;

import com.hurence.logisland.validator.StandardValidators;
import com.hurence.logisland.validator.ValidationContext;
import com.hurence.logisland.validator.ValidationResult;


import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.PrivilegedExceptionAction;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;

@Tags({ "hbase", "client"})
@CapabilityDescription("Implementation of HBaseClientService for HBase 1.1.2. This service can be configured by providing " +
        "a comma-separated list of configuration files, or by specifying values for the other properties. If configuration files " +
        "are provided, they will be loaded first, and the values of the additional properties will override the values from " +
        "the configuration files. In addition, any user defined properties on the processor will also be passed to the HBase " +
        "configuration.")
@DynamicProperty(name="The name of an HBase configuration property.", value="The value of the given HBase configuration property.",
        description="These properties will be set on the HBase configuration after loading any provided configuration files.")
public class Elasticsearch_2_3_3_ClientService extends AbstractControllerService implements ElasticsearchClientService {

    private volatile Connection connection;

    @Override
    @OnEnabled
    public void init(ControllerServiceInitializationContext context) throws InitializationException  {
        try {

        }catch (Exception e){
            throw new InitializationException(e);
        }
    }



    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {

        List<PropertyDescriptor> props = new ArrayList<>();
        //props.add(...);
        //props.add(...);

        return Collections.unmodifiableList(props);
    }

    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(String propertyDescriptorName) {
        return new PropertyDescriptor.Builder()
                .description("Specifies the value for '" + propertyDescriptorName + "' in the HBase configuration.")
                .name(propertyDescriptorName)
                .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
                .dynamic(true)
                .build();
    }

    @Override
    protected Collection<ValidationResult> customValidate(ValidationContext validationContext) {

        final List<ValidationResult> problems = new ArrayList<>();

        //...

        return problems;
    }


    protected Connection createConnection(final ControllerServiceInitializationContext context) throws IOException, InterruptedException {
        //...
        //return ConnectionFactory.createConnection(hbaseConfig);

    }


    @OnDisabled
    public void shutdown() {
        //...
        connection.close();
    }

    /*@Override
    public void put(final String tableName, final Collection<PutRecord> puts) throws IOException {
        try (final Table table = connection.getTable(TableName.valueOf(tableName))) {
            // Create one Put per row....
            final Map<String, Put> rowPuts = new HashMap<>();
            for (final PutRecord putFlowFile : puts) {
                //this is used for the map key as a byte[] does not work as a key.
                final String rowKeyString = new String(putFlowFile.getRow(), StandardCharsets.UTF_8);
                Put put = rowPuts.get(rowKeyString);
                if (put == null) {
                    put = new Put(putFlowFile.getRow());
                    rowPuts.put(rowKeyString, put);
                }

                for (final PutColumn column : putFlowFile.getColumns()) {
                    put.addColumn(
                            column.getColumnFamily(),
                            column.getColumnQualifier(),
                            column.getBuffer());
                }
            }

            table.put(new ArrayList<>(rowPuts.values()));
        }
    }*/

    /*@Override
    public void put(final String tableName, final byte[] rowId, final Collection<PutColumn> columns) throws IOException {
        try (final Table table = connection.getTable(TableName.valueOf(tableName))) {
            Put put = new Put(rowId);
            for (final PutColumn column : columns) {
                put.addColumn(
                        column.getColumnFamily(),
                        column.getColumnQualifier(),
                        column.getBuffer());
            }
            table.put(put);
        }
    }*/
}
