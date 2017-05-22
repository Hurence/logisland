package com.hurence.logisland.service.hbase;

import com.hurence.logisland.annotation.behavior.DynamicProperty;
import com.hurence.logisland.annotation.documentation.CapabilityDescription;
import com.hurence.logisland.annotation.documentation.Tags;
import com.hurence.logisland.annotation.lifecycle.OnDisabled;
import com.hurence.logisland.annotation.lifecycle.OnEnabled;
import com.hurence.logisland.component.InitializationException;
import com.hurence.logisland.component.PropertyDescriptor;
import com.hurence.logisland.controller.AbstractControllerService;
import com.hurence.logisland.controller.ControllerServiceInitializationContext;
import com.hurence.logisland.hadoop.KerberosProperties;
import com.hurence.logisland.hadoop.KerberosTicketRenewer;
import com.hurence.logisland.hadoop.SecurityUtil;
import com.hurence.logisland.service.hbase.put.PutColumn;
import com.hurence.logisland.service.hbase.put.PutRecord;
import com.hurence.logisland.service.hbase.scan.Column;
import com.hurence.logisland.service.hbase.scan.ResultCell;
import com.hurence.logisland.service.hbase.scan.ResultHandler;
import com.hurence.logisland.validator.StandardValidators;
import com.hurence.logisland.validator.ValidationContext;
import com.hurence.logisland.validator.ValidationResult;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.ParseFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.security.UserGroupInformation;


import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

@Tags({ "hbase", "client"})
@CapabilityDescription("Implementation of HBaseClientService for HBase 1.1.2. This service can be configured by providing " +
        "a comma-separated list of configuration files, or by specifying values for the other properties. If configuration files " +
        "are provided, they will be loaded first, and the values of the additional properties will override the values from " +
        "the configuration files. In addition, any user defined properties on the processor will also be passed to the HBase " +
        "configuration.")
@DynamicProperty(name="The name of an HBase configuration property.", value="The value of the given HBase configuration property.",
        description="These properties will be set on the HBase configuration after loading any provided configuration files.")
public class HBase_1_1_2_ClientService extends AbstractControllerService implements HBaseClientService {

    static final String HBASE_CONF_ZK_QUORUM = "hbase.zookeeper.quorum";
    static final String HBASE_CONF_ZK_PORT = "hbase.zookeeper.property.clientPort";
    static final String HBASE_CONF_ZNODE_PARENT = "zookeeper.znode.parent";
    static final String HBASE_CONF_CLIENT_RETRIES = "hbase.client.retries.number";

    static final long TICKET_RENEWAL_PERIOD = 60000;

    private volatile Connection connection;
    private volatile UserGroupInformation ugi;
    private volatile KerberosTicketRenewer renewer;

    private KerberosProperties kerberosProperties;
    private volatile File kerberosConfigFile = null;

    // Holder of cached Configuration information so validation does not reload the same config over and over
    private final AtomicReference<ValidationResources> validationResourceHolder = new AtomicReference<>();

    @Override
    @OnEnabled
    public void init(ControllerServiceInitializationContext context) throws InitializationException  {
        try {
            kerberosConfigFile = context.getKerberosConfigurationFile();
            kerberosProperties = getKerberosProperties(kerberosConfigFile);

            this.connection = createConnection(context);

            // connection check
            if (this.connection != null) {
                final Admin admin = this.connection.getAdmin();
                if (admin != null) {
                    admin.listTableNames();
                }

                // if we got here then we have a successful connection, so if we have a ugi then start a renewer
                if (ugi != null) {
                    final String id = getClass().getSimpleName();
                    renewer = SecurityUtil.startTicketRenewalThread(id, ugi, TICKET_RENEWAL_PERIOD, getLogger());
                }
            }
        }catch (Exception e){
            throw new InitializationException(e);
        }
    }

    protected KerberosProperties getKerberosProperties(File kerberosConfigFile) {
        return new KerberosProperties(kerberosConfigFile);
    }

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {

        List<PropertyDescriptor> props = new ArrayList<>();
        props.add(HADOOP_CONF_FILES);
        if(kerberosProperties !=null){
            props.add(kerberosProperties.getKerberosPrincipal());
            props.add(kerberosProperties.getKerberosKeytab());
        }

        props.add(ZOOKEEPER_QUORUM);
        props.add(ZOOKEEPER_CLIENT_PORT);
        props.add(ZOOKEEPER_ZNODE_PARENT);
        props.add(HBASE_CLIENT_RETRIES);
        props.add(PHOENIX_CLIENT_JAR_LOCATION);

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
        boolean confFileProvided = validationContext.getPropertyValue(HADOOP_CONF_FILES).isSet();
        boolean zkQuorumProvided = validationContext.getPropertyValue(ZOOKEEPER_QUORUM).isSet();
        boolean zkPortProvided = validationContext.getPropertyValue(ZOOKEEPER_CLIENT_PORT).isSet();
        boolean znodeParentProvided = validationContext.getPropertyValue(ZOOKEEPER_ZNODE_PARENT).isSet();
        boolean retriesProvided = validationContext.getPropertyValue(HBASE_CLIENT_RETRIES).isSet();

        final List<ValidationResult> problems = new ArrayList<>();

        if (!confFileProvided && (!zkQuorumProvided || !zkPortProvided || !znodeParentProvided || !retriesProvided)) {
            problems.add(new ValidationResult.Builder()
                    .valid(false)
                    .subject(this.getClass().getSimpleName())
                    .explanation("ZooKeeper Quorum, ZooKeeper Client Port, ZooKeeper ZNode Parent, and HBase Client Retries are required " +
                            "when Hadoop Configuration Files are not provided.")
                    .build());
        }

        if (confFileProvided) {
            final String configFiles = validationContext.getPropertyValue(HADOOP_CONF_FILES).asString();
            ValidationResources resources = validationResourceHolder.get();

            // if no resources in the holder, or if the holder has different resources loaded,
            // then load the Configuration and set the new resources in the holder
            if (resources == null || !configFiles.equals(resources.getConfigResources())) {
                getLogger().debug("Reloading validation resources");
                resources = new ValidationResources(configFiles, getConfigurationFromFiles(configFiles));
                validationResourceHolder.set(resources);
            }

            final Configuration hbaseConfig = resources.getConfiguration();
            final String principal = validationContext.getPropertyValue(kerberosProperties.getKerberosPrincipal()).asString();
            final String keytab = validationContext.getPropertyValue(kerberosProperties.getKerberosKeytab()).asString();

            problems.addAll(KerberosProperties.validatePrincipalAndKeytab(
                    this.getClass().getSimpleName(), hbaseConfig, principal, keytab, getLogger()));
        }

        return problems;
    }


    protected Connection createConnection(final ControllerServiceInitializationContext context) throws IOException, InterruptedException {
        final String configFiles = context.getPropertyValue(HADOOP_CONF_FILES).asString();
        final Configuration hbaseConfig = getConfigurationFromFiles(configFiles);

        // override with any properties that are provided
        if (context.getPropertyValue(ZOOKEEPER_QUORUM).isSet()) {
            hbaseConfig.set(HBASE_CONF_ZK_QUORUM, context.getPropertyValue(ZOOKEEPER_QUORUM).asString());
        }
        if (context.getPropertyValue(ZOOKEEPER_CLIENT_PORT).isSet()) {
            hbaseConfig.set(HBASE_CONF_ZK_PORT, context.getPropertyValue(ZOOKEEPER_CLIENT_PORT).asString());
        }
        if (context.getPropertyValue(ZOOKEEPER_ZNODE_PARENT).isSet()) {
            hbaseConfig.set(HBASE_CONF_ZNODE_PARENT, context.getPropertyValue(ZOOKEEPER_ZNODE_PARENT).asString());
        }
        if (context.getPropertyValue(HBASE_CLIENT_RETRIES).isSet()) {
            hbaseConfig.setInt(HBASE_CONF_CLIENT_RETRIES, context.getPropertyValue(HBASE_CLIENT_RETRIES).asInteger());
        }

        // add any dynamic properties to the HBase configuration
        for (final Map.Entry<PropertyDescriptor, String> entry : context.getProperties().entrySet()) {
            final PropertyDescriptor descriptor = entry.getKey();
            if (descriptor.isDynamic()) {
                hbaseConfig.set(descriptor.getName(), entry.getValue());
            }
        }

        if (SecurityUtil.isSecurityEnabled(hbaseConfig)) {
            final String principal = context.getPropertyValue(kerberosProperties.getKerberosPrincipal()).asString();
            final String keyTab = context.getPropertyValue(kerberosProperties.getKerberosKeytab()).asString();

            getLogger().info("HBase Security Enabled, logging in as principal {} with keytab {}", new Object[] {principal, keyTab});
            ugi = SecurityUtil.loginKerberos(hbaseConfig, principal, keyTab);
            getLogger().info("Successfully logged in as principal {} with keytab {}", new Object[] {principal, keyTab});

            return ugi.doAs(new PrivilegedExceptionAction<Connection>() {
                @Override
                public Connection run() throws Exception {
                    return ConnectionFactory.createConnection(hbaseConfig);
                }
            });

        } else {
            getLogger().info("Simple Authentication");
            return ConnectionFactory.createConnection(hbaseConfig);
        }

    }

    protected Configuration getConfigurationFromFiles(final String configFiles) {
        final Configuration hbaseConfig = HBaseConfiguration.create();
        if (StringUtils.isNotBlank(configFiles)) {
            for (final String configFile : configFiles.split(",")) {
                hbaseConfig.addResource(new Path(configFile.trim()));
            }
        }
        return hbaseConfig;
    }

    @OnDisabled
    public void shutdown() {
        if (renewer != null) {
            renewer.stop();
        }

        if (connection != null) {
            try {
                connection.close();
            } catch (final IOException ioe) {
                getLogger().warn("Failed to close connection to HBase due to {}", new Object[]{ioe});
            }
        }
    }

    @Override
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
    }

    @Override
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
    }

    @Override
    public void scan(final String tableName, final Collection<Column> columns, final String filterExpression, final long minTime, final ResultHandler handler)
            throws IOException {

        Filter filter = null;
        if (!StringUtils.isBlank(filterExpression)) {
            ParseFilter parseFilter = new ParseFilter();
            filter = parseFilter.parseFilterString(filterExpression);
        }

        try (final Table table = connection.getTable(TableName.valueOf(tableName));
             final ResultScanner scanner = getResults(table, columns, filter, minTime)) {

            for (final Result result : scanner) {
                final byte[] rowKey = result.getRow();
                final Cell[] cells = result.rawCells();

                if (cells == null) {
                    continue;
                }

                // convert HBase cells to NiFi cells
                final ResultCell[] resultCells = new ResultCell[cells.length];
                for (int i=0; i < cells.length; i++) {
                    final Cell cell = cells[i];
                    final ResultCell resultCell = getResultCell(cell);
                    resultCells[i] = resultCell;
                }

                // delegate to the handler
                handler.handle(rowKey, resultCells);
            }
        }
    }

    @Override
    public void scan(final String tableName, final byte[] startRow, final byte[] endRow, final Collection<Column> columns, final ResultHandler handler)
            throws IOException {

        try (final Table table = connection.getTable(TableName.valueOf(tableName));
             final ResultScanner scanner = getResults(table, startRow, endRow, columns)) {

            for (final Result result : scanner) {
                final byte[] rowKey = result.getRow();
                final Cell[] cells = result.rawCells();

                if (cells == null) {
                    continue;
                }

                // convert HBase cells to NiFi cells
                final ResultCell[] resultCells = new ResultCell[cells.length];
                for (int i=0; i < cells.length; i++) {
                    final Cell cell = cells[i];
                    final ResultCell resultCell = getResultCell(cell);
                    resultCells[i] = resultCell;
                }

                // delegate to the handler
                handler.handle(rowKey, resultCells);
            }
        }
    }

    // protected and extracted into separate method for testing
    protected ResultScanner getResults(final Table table, final byte[] startRow, final byte[] endRow, final Collection<Column> columns) throws IOException {
        final Scan scan = new Scan();
        scan.setStartRow(startRow);
        scan.setStopRow(endRow);

        if (columns != null) {
            for (Column col : columns) {
                if (col.getQualifier() == null) {
                    scan.addFamily(col.getFamily());
                } else {
                    scan.addColumn(col.getFamily(), col.getQualifier());
                }
            }
        }

        return table.getScanner(scan);
    }

    // protected and extracted into separate method for testing
    protected ResultScanner getResults(final Table table, final Collection<Column> columns, final Filter filter, final long minTime) throws IOException {
        // Create a new scan. We will set the min timerange as the latest timestamp that
        // we have seen so far. The minimum timestamp is inclusive, so we will get duplicates.
        // We will record any cells that have the latest timestamp, so that when we scan again,
        // we know to throw away those duplicates.
        final Scan scan = new Scan();
        scan.setTimeRange(minTime, Long.MAX_VALUE);

        if (filter != null) {
            scan.setFilter(filter);
        }

        if (columns != null) {
            for (Column col : columns) {
                if (col.getQualifier() == null) {
                    scan.addFamily(col.getFamily());
                } else {
                    scan.addColumn(col.getFamily(), col.getQualifier());
                }
            }
        }

        return table.getScanner(scan);
    }

    private ResultCell getResultCell(Cell cell) {
        final ResultCell resultCell = new ResultCell();
        resultCell.setRowArray(cell.getRowArray());
        resultCell.setRowOffset(cell.getRowOffset());
        resultCell.setRowLength(cell.getRowLength());

        resultCell.setFamilyArray(cell.getFamilyArray());
        resultCell.setFamilyOffset(cell.getFamilyOffset());
        resultCell.setFamilyLength(cell.getFamilyLength());

        resultCell.setQualifierArray(cell.getQualifierArray());
        resultCell.setQualifierOffset(cell.getQualifierOffset());
        resultCell.setQualifierLength(cell.getQualifierLength());

        resultCell.setTimestamp(cell.getTimestamp());
        resultCell.setTypeByte(cell.getTypeByte());
        resultCell.setSequenceId(cell.getSequenceId());

        resultCell.setValueArray(cell.getValueArray());
        resultCell.setValueOffset(cell.getValueOffset());
        resultCell.setValueLength(cell.getValueLength());

        resultCell.setTagsArray(cell.getTagsArray());
        resultCell.setTagsOffset(cell.getTagsOffset());
        resultCell.setTagsLength(cell.getTagsLength());
        return resultCell;
    }

    static protected class ValidationResources {
        private final String configResources;
        private final Configuration configuration;

        public ValidationResources(String configResources, Configuration configuration) {
            this.configResources = configResources;
            this.configuration = configuration;
        }

        public String getConfigResources() {
            return configResources;
        }

        public Configuration getConfiguration() {
            return configuration;
        }
    }

    @Override
    public byte[] toBytes(boolean b) {
        return Bytes.toBytes(b);
    }

    @Override
    public byte[] toBytes(long l) {
        return Bytes.toBytes(l);
    }

    @Override
    public byte[] toBytes(double d) {
        return Bytes.toBytes(d);
    }

    @Override
    public byte[] toBytes(String s) {
        return Bytes.toBytes(s);
    }

    @Override
    public byte[] toBytesBinary(String s) {
        return Bytes.toBytesBinary(s);
    }
}
