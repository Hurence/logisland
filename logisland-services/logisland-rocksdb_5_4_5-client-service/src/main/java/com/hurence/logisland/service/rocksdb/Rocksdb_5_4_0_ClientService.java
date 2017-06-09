package com.hurence.logisland.service.rocksdb;

import com.hurence.logisland.annotation.documentation.CapabilityDescription;
import com.hurence.logisland.annotation.documentation.Tags;
import com.hurence.logisland.annotation.lifecycle.OnDisabled;
import com.hurence.logisland.annotation.lifecycle.OnEnabled;
import com.hurence.logisland.component.InitializationException;
import com.hurence.logisland.component.PropertyDescriptor;
import com.hurence.logisland.controller.AbstractControllerService;
import com.hurence.logisland.controller.ControllerServiceInitializationContext;
import com.hurence.logisland.processor.ProcessException;
import com.hurence.logisland.serializer.Serializer;
import com.hurence.logisland.service.rocksdb.delete.DeleteRangeRequest;
import com.hurence.logisland.service.rocksdb.delete.DeleteRangeResponse;
import com.hurence.logisland.service.rocksdb.delete.DeleteRequest;
import com.hurence.logisland.service.rocksdb.delete.DeleteResponse;
import com.hurence.logisland.service.rocksdb.get.GetRequest;
import com.hurence.logisland.service.rocksdb.get.GetResponse;
import com.hurence.logisland.service.rocksdb.put.ValuePutRequest;
import com.hurence.logisland.service.rocksdb.scan.RocksIteratorHandler;
import com.hurence.logisland.service.rocksdb.scan.RocksIteratorRequest;
import com.hurence.logisland.validator.StandardValidators;
import org.rocksdb.*;

import java.util.*;

/**
 * Implementation of rocksDb 5.4.0
 *
 * To use a rockdb you need to specify
 * _a path to the db
 * _The options of the db (optionnal)
 * _if readAndWrite mode The description of each column family currently present in the db otherwise you get a "InvalidArgument" exception
 * _if readOnly you can specify a subset of all the columns family
 *
 * You can specify configuration of rocksDb with a File, using service properties or even dynamic properties
 * if some properties are not yet defined in the service
 *
 * Custom configuration via service properties will prevail on file properties. So you can override
 * one or more propertie from the file by filling correspondant properties
 *
 * Do not use Options object as it is still there only for backward compatibility
 */
@Tags({ "elasticsearch", "client"})
@CapabilityDescription("Implementation of RocksdbClientService for Elasticsearch 5.4.0.")
public class Rocksdb_5_4_0_ClientService extends AbstractControllerService implements RocksdbClientService {

    protected RocksDB db;
    protected DBOptions dbOptions;
//    protected volatile List<ColumnFamilyOptions> familiesOptions;
//    protected volatile List<ColumnFamilyDescriptor> familiesDescriptor;
//    protected volatile List<ColumnFamilyHandle> familiesHandler = new ArrayList<>();
    protected List<String> familiesName = new ArrayList<>();
    protected Map<String, ColumnFamilyHandle> familiesHandler = new HashMap<>();
    protected Map<String, ColumnFamilyDescriptor> familiesDescriptor = new HashMap<>();
    protected Serializer familiesNameSerialize;

    private static final String FAMILY_PREFIX = "family.";
    protected TableFormatConfig tableFormat;
    protected BlockBasedTableConfig a;
    protected Cache b;
    protected Filter c;



    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {

        List<PropertyDescriptor> props = new ArrayList<>();
        props.add(ROCKSDB_PATH);
//        if(kerberosProperties !=null){
//            props.add(kerberosProperties.getKerberosPrincipal());
//            props.add(kerberosProperties.getKerberosKeytab());
//        }
//
//        props.add(ZOOKEEPER_QUORUM);
//        props.add(ZOOKEEPER_CLIENT_PORT);
//        props.add(ZOOKEEPER_ZNODE_PARENT);
//        props.add(HBASE_CLIENT_RETRIES);
//        props.add(PHOENIX_CLIENT_JAR_LOCATION);
        return Collections.unmodifiableList(props);
    }

    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(String propertyDescriptorName) {
        //TODO use a regex
        if (propertyDescriptorName.startsWith(FAMILY_PREFIX)) {
            return new PropertyDescriptor.Builder()
                    .description("Specifies the value for '" + propertyDescriptorName + "' in the DbOptions of RocksDb configuration.")
                    .name(propertyDescriptorName)
                    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
                    .dynamic(true)
                    .build();
        } else {
            return null;
        }

    }

    @Override
    @OnEnabled
    public void init(ControllerServiceInitializationContext context) throws InitializationException  {
        synchronized(this) {
            try {
                createDbClient(context);
            }catch (Exception e){
                throw new InitializationException(e);
            }
        }
    }

    /**
     * Instantiate ElasticSearch Client. This chould be called by subclasses' @OnScheduled method to create a client
     * if one does not yet exist. If called when scheduled, closeClient() should be called by the subclasses' @OnStopped
     * method so the client will be destroyed when the processor is stopped.
     *
     * @param context The context for this processor
     * @throws ProcessException if an error occurs while creating an Elasticsearch client
     */
    protected void createDbClient(ControllerServiceInitializationContext context) throws ProcessException {
        //clean
        if (db != null) {
            return;
        }
        /*
            DBOptions
         */
        final DBOptions dbOptions = parseDbOptions(context);
        /*
            Families Descriptors
         */
        final List<ColumnFamilyDescriptor> familiesDescriptor = parseFamiliesDescriptor(context);
        /*
            Opening Db and filling up families handler
         */
        final String dbPath = context.getPropertyValue(ROCKSDB_PATH).asString();
        final String[] familiesName =  context.getPropertyValue(FAMILY_NAMES).asString().split(",");
        final List<ColumnFamilyHandle> familiesHandler = new ArrayList<>();
        try {
            db = RocksDB.open(dbOptions, dbPath, familiesDescriptor, familiesHandler);
        } catch (Exception e) {
            getLogger().error("Failed to create RocksDb client due to {}", new Object[]{e}, e);
            throw new RuntimeException(e);
        }
        //initialize map of handlers
        for (int i=0; i<familiesName.length;i++) {
            String familyName = familiesName[i];
            this.familiesHandler.put(familyName, familiesHandler.get(i));
        }
        if (db == null) {//RocksDB.open can return null
            getLogger().error("Failed to create RocksDb client for unknown reason");
            throw new RuntimeException("Failed to create RocksDb client for unknown reason");
        }
    }

    /**
     * options currently supported are (see rocksDb documentation)
     *
     * optimize_for_small_db
     * increase_parallelism
     * create_if_missing
     * create_missing_column_families
     * error_if_exists
     * paranoid_checks
     * rate_limiter
     * max_open_files
     * max_file_opening_threads
     * max_total_wal_size
     * use_fsync
     * db_paths
     * db_log_dir
     * wal_dir
     * delete_obsolete_files_period_micros
     * base_background_compactions
     * max_background_compactions
     * max_subcompactions
     * max_background_flushes
     * max_log_file_size
     * log_file_time_to_roll
     * keep_log_file_num
     * recycle_log_file_num
     * max_manifest_file_size
     * table_cache_numshardbits
     * setWalTtlSeconds
     * setWalSizeLimitMB
     * setManifestPreallocationSize
     * setUseDirectReads
     * setUseDirectIoForFlushAndCompaction
     * setAllowFAllocate
     * setAllowMmapReads
     * setAllowMmapWrites
     * setIsFdCloseOnExec
     * setStatsDumpPeriodSec
     * setAdviseRandomOnOpen
     * setDbWriteBufferSize
     * setAccessHintOnCompactionStart
     * setNewTableReaderForCompactionInputs
     * setCompactionReadaheadSize
     * setRandomAccessMaxBufferSize
     * setWritableFileMaxBufferSize
     * setUseAdaptiveMutex
     * setBytesPerSync
     * setWalBytesPerSync
     * setEnableThreadTracking
     * setDelayedWriteRate
     * setAllowConcurrentMemtableWrite
     * setEnableWriteThreadAdaptiveYield
     * setWriteThreadMaxYieldUsec
     * setWriteThreadSlowYieldUsec
     * setSkipStatsUpdateOnDbOpen
     * setWalRecoveryMode
     * setAllow2pc
     * setRowCache
     * setFailIfOptionsFileError
     * setDumpMallocStats
     * setAvoidFlushDuringRecovery
     * setAvoidFlushDuringShutdown
     * @param context
     * @return
     */
    protected DBOptions parseDbOptions(ControllerServiceInitializationContext context)
    {
        DBOptions dbOptions = new DBOptions();
        if (context.getPropertyValue(Rocksdb_5_4_0_ClientService.OPTIMIZE_FOR_SMALL_DB).isSet()) {
            if (context.getPropertyValue(Rocksdb_5_4_0_ClientService.OPTIMIZE_FOR_SMALL_DB).asBoolean())
                dbOptions.optimizeForSmallDb();
        }
        if (context.getPropertyValue(Rocksdb_5_4_0_ClientService.INCREASE_PARALLELISM).isSet()) {
            int parallelism = context.getPropertyValue(Rocksdb_5_4_0_ClientService.INCREASE_PARALLELISM).asInteger();
            dbOptions.setIncreaseParallelism(parallelism);
        }
        if (context.getPropertyValue(Rocksdb_5_4_0_ClientService.CREATE_IF_MISSING).isSet()) {
            if (context.getPropertyValue(Rocksdb_5_4_0_ClientService.CREATE_IF_MISSING).asBoolean())
                dbOptions.createIfMissing();
        }
        if (context.getPropertyValue(Rocksdb_5_4_0_ClientService.CREATE_MISSING_COLUMN_FAMILIES).isSet()) {
            if (context.getPropertyValue(Rocksdb_5_4_0_ClientService.CREATE_MISSING_COLUMN_FAMILIES).asBoolean())
                dbOptions.createIfMissing();
        }
        if (context.getPropertyValue(Rocksdb_5_4_0_ClientService.ERROR_IF_EXISTS).isSet()) {
            if (context.getPropertyValue(Rocksdb_5_4_0_ClientService.ERROR_IF_EXISTS).asBoolean())
                dbOptions.errorIfExists();
        }
        if (context.getPropertyValue(Rocksdb_5_4_0_ClientService.PARANOID_CHECKS).isSet()) {
            if (context.getPropertyValue(Rocksdb_5_4_0_ClientService.PARANOID_CHECKS).asBoolean())
                dbOptions.paranoidChecks();
        }
//        if (context.getPropertyValue(Rocksdb_5_4_0_ClientService.PARANOID_CHECKS).isSet()) {
            //TODO add possibility to use a custom Env
//            dbOptions.setEnv(<myEnv>);
//        }
//        if (context.getPropertyValue(Rocksdb_5_4_0_ClientService.PARANOID_CHECKS).isSet()) {
                //TODO
//                dbOptions.setRateLimiter(<myRateLimiter>);
//        }
        if (context.getPropertyValue(Rocksdb_5_4_0_ClientService.MAX_OPEN_FILES).isSet()) {
            int maxFiles = context.getPropertyValue(Rocksdb_5_4_0_ClientService.MAX_OPEN_FILES).asInteger();
            dbOptions.setMaxOpenFiles(maxFiles);
        }
        if (context.getPropertyValue(Rocksdb_5_4_0_ClientService.MAX_FILE_OPENING_THREADS).isSet()) {
            int maxThreads = context.getPropertyValue(Rocksdb_5_4_0_ClientService.MAX_FILE_OPENING_THREADS).asInteger();
            dbOptions.setMaxFileOpeningThreads(maxThreads);
        }
        if (context.getPropertyValue(Rocksdb_5_4_0_ClientService.MAX_TOTAL_WAL_SIZE).isSet()) {
            int maxWalSize = context.getPropertyValue(Rocksdb_5_4_0_ClientService.MAX_TOTAL_WAL_SIZE).asInteger();
            dbOptions.setMaxTotalWalSize(maxWalSize);
        }
        if (context.getPropertyValue(Rocksdb_5_4_0_ClientService.USE_FSYNC).isSet()) {
            boolean useFsync = context.getPropertyValue(Rocksdb_5_4_0_ClientService.USE_FSYNC).asBoolean();
            dbOptions.setUseFsync(useFsync);
        }
//        if (context.getPropertyValue(Rocksdb_5_4_0_ClientService.DB_PATHS).isSet()) {
//            //TODO parse field with regex
//            DbPath dbPath = new DbPath(path, size)
//            dbOptions.setDbPaths(dbPath);
//        }
        if (context.getPropertyValue(Rocksdb_5_4_0_ClientService.DB_LOG_DIR).isSet()) {
            String dbLogDir = context.getPropertyValue(Rocksdb_5_4_0_ClientService.DB_LOG_DIR).asString();
            dbOptions.setDbLogDir(dbLogDir);
        }
        if (context.getPropertyValue(Rocksdb_5_4_0_ClientService.WAL_DIR).isSet()) {
            String walDir = context.getPropertyValue(Rocksdb_5_4_0_ClientService.WAL_DIR).asString();
            dbOptions.setWalDir(walDir);
        }
        if (context.getPropertyValue(Rocksdb_5_4_0_ClientService.DELETE_OBSOLETE_FILES_PERIOD_MICROS).isSet()) {
            long period = context.getPropertyValue(Rocksdb_5_4_0_ClientService.DELETE_OBSOLETE_FILES_PERIOD_MICROS).asLong();
            dbOptions.setDeleteObsoleteFilesPeriodMicros(period);
        }
        if (context.getPropertyValue(Rocksdb_5_4_0_ClientService.BASE_BACKGROUND_COMPACTIONS).isSet()) {
            int baseBackgroundCompactions = context.getPropertyValue(Rocksdb_5_4_0_ClientService.BASE_BACKGROUND_COMPACTIONS).asInteger();
            dbOptions.setBaseBackgroundCompactions(baseBackgroundCompactions);
        }
        if (context.getPropertyValue(Rocksdb_5_4_0_ClientService.MAX_BACKGROUND_COMPACTIONS).isSet()) {
            int maxBackgroundCompaction = context.getPropertyValue(Rocksdb_5_4_0_ClientService.MAX_BACKGROUND_COMPACTIONS).asInteger();
            dbOptions.setMaxBackgroundCompactions(maxBackgroundCompaction);
        }
        if (context.getPropertyValue(Rocksdb_5_4_0_ClientService.MAX_SUBCOMPACTIONS).isSet()) {
            int maxSubCompactions = context.getPropertyValue(Rocksdb_5_4_0_ClientService.MAX_SUBCOMPACTIONS).asInteger();
            dbOptions.setMaxSubcompactions(maxSubCompactions);
        }
        if (context.getPropertyValue(Rocksdb_5_4_0_ClientService.MAX_BACKGROUND_FLUSHES).isSet()) {
            int maxBackgroundFlush = context.getPropertyValue(Rocksdb_5_4_0_ClientService.MAX_BACKGROUND_FLUSHES).asInteger();
            dbOptions.setMaxBackgroundFlushes(maxBackgroundFlush);
        }
        if (context.getPropertyValue(Rocksdb_5_4_0_ClientService.MAX_LOG_FILE_SIZE).isSet()) {
            int maxLogFileSize = context.getPropertyValue(Rocksdb_5_4_0_ClientService.MAX_LOG_FILE_SIZE).asInteger();
            dbOptions.setMaxLogFileSize(maxLogFileSize);
        }
        if (context.getPropertyValue(Rocksdb_5_4_0_ClientService.LOG_FILE_TIME_TO_ROLL).isSet()) {
            long logFileTimeToRoll = context.getPropertyValue(Rocksdb_5_4_0_ClientService.LOG_FILE_TIME_TO_ROLL).asLong();
            dbOptions.setLogFileTimeToRoll(logFileTimeToRoll);
        }
        if (context.getPropertyValue(Rocksdb_5_4_0_ClientService.KEEP_LOG_FILE_NUM).isSet()) {
            long keepLogFileNum = context.getPropertyValue(Rocksdb_5_4_0_ClientService.KEEP_LOG_FILE_NUM).asLong();
            dbOptions.setKeepLogFileNum(keepLogFileNum);
        }
        if (context.getPropertyValue(Rocksdb_5_4_0_ClientService.RECYCLE_LOG_FILE_NUM).isSet()) {
            long recycleLogFileNum = context.getPropertyValue(Rocksdb_5_4_0_ClientService.RECYCLE_LOG_FILE_NUM).asLong();
            dbOptions.setRecycleLogFileNum(recycleLogFileNum);
        }
        if (context.getPropertyValue(Rocksdb_5_4_0_ClientService.MAX_MANIFEST_FILE_SIZE).isSet()) {
            long maxManifestFileSize = context.getPropertyValue(Rocksdb_5_4_0_ClientService.MAX_MANIFEST_FILE_SIZE).asLong();
            dbOptions.setMaxManifestFileSize(maxManifestFileSize);
        }
        if (context.getPropertyValue(Rocksdb_5_4_0_ClientService.TABLE_CACHE_NUMSHARDBITS).isSet()) {
            int tableCacheNumshardbits = context.getPropertyValue(Rocksdb_5_4_0_ClientService.TABLE_CACHE_NUMSHARDBITS).asInteger();
            dbOptions.setTableCacheNumshardbits(tableCacheNumshardbits);
        }
        if (context.getPropertyValue(Rocksdb_5_4_0_ClientService.WAL_TTL_SECONDS).isSet()) {
            long walTtlSeconds = context.getPropertyValue(Rocksdb_5_4_0_ClientService.WAL_TTL_SECONDS).asLong();
            dbOptions.setWalTtlSeconds(walTtlSeconds);
        }
        if (context.getPropertyValue(Rocksdb_5_4_0_ClientService.WAL_SIZE_LIMIT_MB).isSet()) {
            long walSizeLimitMb = context.getPropertyValue(Rocksdb_5_4_0_ClientService.WAL_SIZE_LIMIT_MB).asLong();
            dbOptions.setWalSizeLimitMB(walSizeLimitMb);
        }
        if (context.getPropertyValue(Rocksdb_5_4_0_ClientService.MANIFEST_PREALLOCATION_SIZE).isSet()) {
            long manifestPreallocationSize = context.getPropertyValue(Rocksdb_5_4_0_ClientService.MANIFEST_PREALLOCATION_SIZE).asLong();
            dbOptions.setManifestPreallocationSize(manifestPreallocationSize);
        }
        if (context.getPropertyValue(Rocksdb_5_4_0_ClientService.USE_DIRECT_READS).isSet()) {
            boolean useDirectReads = context.getPropertyValue(Rocksdb_5_4_0_ClientService.USE_DIRECT_READS).asBoolean();
            dbOptions.setUseDirectReads(useDirectReads);
        }
        if (context.getPropertyValue(Rocksdb_5_4_0_ClientService.USE_DIRECT_IO_FOR_FLUSH_AND_COMPACTION).isSet()) {
            boolean useDirectIoForFlushAndCompaction = context.getPropertyValue(Rocksdb_5_4_0_ClientService.USE_DIRECT_IO_FOR_FLUSH_AND_COMPACTION).asBoolean();
            dbOptions.setUseDirectIoForFlushAndCompaction(useDirectIoForFlushAndCompaction);
        }
        if (context.getPropertyValue(Rocksdb_5_4_0_ClientService.ALLOW_F_ALLOCATE).isSet()) {
            boolean allowFAllocate = context.getPropertyValue(Rocksdb_5_4_0_ClientService.ALLOW_F_ALLOCATE).asBoolean();
            dbOptions.setAllowFAllocate(allowFAllocate);
        }
        if (context.getPropertyValue(Rocksdb_5_4_0_ClientService.ALLOW_MMAP_READS).isSet()) {
            boolean allowMmapReads = context.getPropertyValue(Rocksdb_5_4_0_ClientService.ALLOW_MMAP_READS).asBoolean();
            dbOptions.setAllowMmapReads(allowMmapReads);
        }
        if (context.getPropertyValue(Rocksdb_5_4_0_ClientService.ALLOW_MMAP_WRITES).isSet()) {
            boolean allowMmapWrites = context.getPropertyValue(Rocksdb_5_4_0_ClientService.ALLOW_MMAP_WRITES).asBoolean();
            dbOptions.setAllowMmapWrites(allowMmapWrites);
        }
        if (context.getPropertyValue(Rocksdb_5_4_0_ClientService.IS_FD_CLOSE_ON_EXEC).isSet()) {
            boolean isFdCloseOnExec = context.getPropertyValue(Rocksdb_5_4_0_ClientService.IS_FD_CLOSE_ON_EXEC).asBoolean();
            dbOptions.setIsFdCloseOnExec(isFdCloseOnExec);
        }
        if (context.getPropertyValue(Rocksdb_5_4_0_ClientService.STATS_DUMP_PERIOD_SEC).isSet()) {
            int statsDumpPeriodSec = context.getPropertyValue(Rocksdb_5_4_0_ClientService.STATS_DUMP_PERIOD_SEC).asInteger();
            dbOptions.setStatsDumpPeriodSec(statsDumpPeriodSec);
        }
        if (context.getPropertyValue(Rocksdb_5_4_0_ClientService.ADVISE_RANDOM_ON_OPEN).isSet()) {
            boolean adviseRandomOnOpen = context.getPropertyValue(Rocksdb_5_4_0_ClientService.ADVISE_RANDOM_ON_OPEN).asBoolean();
            dbOptions.setAdviseRandomOnOpen(adviseRandomOnOpen);
        }
        if (context.getPropertyValue(Rocksdb_5_4_0_ClientService.DB_WRITE_BUFFER_SIZE).isSet()) {
            long dbWriteBufferSize = context.getPropertyValue(Rocksdb_5_4_0_ClientService.DB_WRITE_BUFFER_SIZE).asLong();
            dbOptions.setDbWriteBufferSize(dbWriteBufferSize);
        }
//        if (context.getPropertyValue(Rocksdb_5_4_0_ClientService.ACCESS_HINT_ON_COMPACTION_START).isSet()) {
//            AccessHint accessHint = new AccessHint();
//            //TODO
//            dbOptions.setAccessHintOnCompactionStart(accessHint);
//        }
        if (context.getPropertyValue(Rocksdb_5_4_0_ClientService.NEW_TABLE_READER_FOR_COMPACTION_INPUTS).isSet()) {
            Boolean newTableReaderForCompactionInputs = context.getPropertyValue(Rocksdb_5_4_0_ClientService.NEW_TABLE_READER_FOR_COMPACTION_INPUTS).asBoolean();
            dbOptions.setNewTableReaderForCompactionInputs(newTableReaderForCompactionInputs);
        }
        if (context.getPropertyValue(Rocksdb_5_4_0_ClientService.COMPACTION_READAHEAD_SIZE).isSet()) {
            long compactionReadaheadSize = context.getPropertyValue(Rocksdb_5_4_0_ClientService.COMPACTION_READAHEAD_SIZE).asLong();
            dbOptions.setCompactionReadaheadSize(compactionReadaheadSize);
        }
        if (context.getPropertyValue(Rocksdb_5_4_0_ClientService.RANDOM_ACCESS_MAX_BUFFER_SIZE).isSet()) {
            long randomAccessmaxBufferSize = context.getPropertyValue(Rocksdb_5_4_0_ClientService.RANDOM_ACCESS_MAX_BUFFER_SIZE).asLong();
            dbOptions.setRandomAccessMaxBufferSize(randomAccessmaxBufferSize);
        }
        if (context.getPropertyValue(Rocksdb_5_4_0_ClientService.WRITABLE_FILE_MAX_BUFFER_SIZE).isSet()) {
            long writableFileMaxBufferSize = context.getPropertyValue(Rocksdb_5_4_0_ClientService.WRITABLE_FILE_MAX_BUFFER_SIZE).asLong();
            dbOptions.setWritableFileMaxBufferSize(writableFileMaxBufferSize);
        }
        if (context.getPropertyValue(Rocksdb_5_4_0_ClientService.USE_ADAPTIVE_MUTEX).isSet()) {
            boolean useAdaptiveMutex = context.getPropertyValue(Rocksdb_5_4_0_ClientService.USE_ADAPTIVE_MUTEX).asBoolean();
            dbOptions.setUseAdaptiveMutex(useAdaptiveMutex);
        }
        if (context.getPropertyValue(Rocksdb_5_4_0_ClientService.BYTES_PER_SYNC).isSet()) {
            long bytesPerSync = context.getPropertyValue(Rocksdb_5_4_0_ClientService.BYTES_PER_SYNC).asLong();
            dbOptions.setBytesPerSync(bytesPerSync);
        }
        if (context.getPropertyValue(Rocksdb_5_4_0_ClientService.WAL_BYTES_PER_SYNC).isSet()) {
            long walBytesPerSync = context.getPropertyValue(Rocksdb_5_4_0_ClientService.WAL_BYTES_PER_SYNC).asLong();
            dbOptions.setWalBytesPerSync(walBytesPerSync);
        }
        if (context.getPropertyValue(Rocksdb_5_4_0_ClientService.ENABLE_THREAD_TRACKING).isSet()) {
            boolean enableThreadTracking = context.getPropertyValue(Rocksdb_5_4_0_ClientService.ENABLE_THREAD_TRACKING).asBoolean();
            dbOptions.setEnableThreadTracking(enableThreadTracking);
        }
        if (context.getPropertyValue(Rocksdb_5_4_0_ClientService.DELAYED_WRITE_RATE).isSet()) {
            long delayedWriteRate = context.getPropertyValue(Rocksdb_5_4_0_ClientService.DELAYED_WRITE_RATE).asLong();
            dbOptions.setDelayedWriteRate(delayedWriteRate);
        }
        if (context.getPropertyValue(Rocksdb_5_4_0_ClientService.ALLOW_CONCURRENT_MEMTABLE_WRITE).isSet()) {
            boolean delayedWriteRate = context.getPropertyValue(Rocksdb_5_4_0_ClientService.ALLOW_CONCURRENT_MEMTABLE_WRITE).asBoolean();
            dbOptions.setAllowConcurrentMemtableWrite(delayedWriteRate);
        }
        if (context.getPropertyValue(Rocksdb_5_4_0_ClientService.ENABLE_WRITE_THREAD_ADAPTIVE_YIELD).isSet()) {
            boolean enableWriteThreadAdaptiveYield = context.getPropertyValue(Rocksdb_5_4_0_ClientService.ENABLE_WRITE_THREAD_ADAPTIVE_YIELD).asBoolean();
            dbOptions.setEnableWriteThreadAdaptiveYield(enableWriteThreadAdaptiveYield);
        }
        if (context.getPropertyValue(Rocksdb_5_4_0_ClientService.WRITE_THREAD_MAX_YIELD_USEC).isSet()) {
            long writeThreadMaxYieldUsec = context.getPropertyValue(Rocksdb_5_4_0_ClientService.WRITE_THREAD_MAX_YIELD_USEC).asLong();
            dbOptions.setWriteThreadMaxYieldUsec(writeThreadMaxYieldUsec);
        }
        if (context.getPropertyValue(Rocksdb_5_4_0_ClientService.WRITE_THREAD_SLOW_YIELD_USEC).isSet()) {
            long writeThreadSlowYieldUsec = context.getPropertyValue(Rocksdb_5_4_0_ClientService.WRITE_THREAD_SLOW_YIELD_USEC).asLong();
            dbOptions.setWriteThreadSlowYieldUsec(writeThreadSlowYieldUsec);
        }
        if (context.getPropertyValue(Rocksdb_5_4_0_ClientService.SKIP_STATS_UPDATE_ON_DB_OPEN).isSet()) {
            boolean skipStatsUpdateOnDbOpen = context.getPropertyValue(Rocksdb_5_4_0_ClientService.SKIP_STATS_UPDATE_ON_DB_OPEN).asBoolean();
            dbOptions.setSkipStatsUpdateOnDbOpen(skipStatsUpdateOnDbOpen);
        }
//        if (context.getPropertyValue(Rocksdb_5_4_0_ClientService.WAL_RECOVERY_MODE).isSet()) {
//            WALRecoveryMode mode = new WALRecoveryMode();
//            //TODO
//            dbOptions.setWalRecoveryMode(skipStatsUpdateOnDbOpen);
//        }
        if (context.getPropertyValue(Rocksdb_5_4_0_ClientService.ALLOW_2_PC).isSet()) {
            boolean allow2Pc = context.getPropertyValue(Rocksdb_5_4_0_ClientService.ALLOW_2_PC).asBoolean();
            dbOptions.setAllow2pc(allow2Pc);
        }
//        if (context.getPropertyValue(Rocksdb_5_4_0_ClientService.RowCache).isSet()) {
//            Cache cache = new LRUCache();
//            //TODO
//            dbOptions.setRowCache(cache);
//        }
        if (context.getPropertyValue(Rocksdb_5_4_0_ClientService.FAIL_IF_OPTIONS_FILE_ERROR).isSet()) {
            boolean failIfOptionsFileError = context.getPropertyValue(Rocksdb_5_4_0_ClientService.FAIL_IF_OPTIONS_FILE_ERROR).asBoolean();
            dbOptions.setFailIfOptionsFileError(failIfOptionsFileError);
        }
        if (context.getPropertyValue(Rocksdb_5_4_0_ClientService.DUMP_MALLOC_STATS).isSet()) {
            boolean dumpMallocStats = context.getPropertyValue(Rocksdb_5_4_0_ClientService.DUMP_MALLOC_STATS).asBoolean();
            dbOptions.setDumpMallocStats(dumpMallocStats);
        }
        if (context.getPropertyValue(Rocksdb_5_4_0_ClientService.AVOID_FLUSH_DURING_RECOVERY).isSet()) {
            boolean avoidFlushDuringRecovery = context.getPropertyValue(Rocksdb_5_4_0_ClientService.AVOID_FLUSH_DURING_RECOVERY).asBoolean();
            dbOptions.setAvoidFlushDuringRecovery(avoidFlushDuringRecovery);
        }
        if (context.getPropertyValue(Rocksdb_5_4_0_ClientService.AVOID_FLUSH_DURING_SHUTDOWN).isSet()) {
            boolean avoidFlushDuringShutdown = context.getPropertyValue(Rocksdb_5_4_0_ClientService.AVOID_FLUSH_DURING_SHUTDOWN).asBoolean();
            dbOptions.setAvoidFlushDuringShutdown(avoidFlushDuringShutdown);
        }
        return dbOptions;
    }

    /**
     * initialize {@link #familiesDescriptor}, {@link #familiesName}
     * @param context
     */
    protected  List<ColumnFamilyDescriptor> parseFamiliesDescriptor(ControllerServiceInitializationContext context)
    {
        /*
            initialize familiesName
         */
        final String[] familiesName =  context.getPropertyValue(FAMILY_NAMES).asString().split(",");
        this.familiesName = Arrays.asList(familiesName);
        /*
            initialize familiesDescriptor
        */
        final List<ColumnFamilyOptions> familiesOptions = parseFamiliesOptions(context);
        final List<ColumnFamilyDescriptor> familiesDescriptorList = new ArrayList<>();
        for (int i=0; i<familiesName.length;i++) {
            String familyName = familiesName[i];
            ColumnFamilyOptions familyOptions = familiesOptions.get(i);
            ColumnFamilyDescriptor fDescriptor = new ColumnFamilyDescriptor(familyName.getBytes() , familyOptions);
            familiesDescriptorList.add(fDescriptor);
            this.familiesDescriptor.put(familyName, fDescriptor);
        }
        return familiesDescriptorList;
    }
    protected List<ColumnFamilyOptions> parseFamiliesOptions(ControllerServiceInitializationContext context)
    {
        final List<ColumnFamilyOptions> familyOptions = new ArrayList<>();
        final String[] familiesName =  context.getPropertyValue(FAMILY_NAMES).asString().split(",");

        for (int i=0; i<familiesName.length;i++) {
            final String familyPrefix = FAMILY_PREFIX + familiesName + ".";
            final ColumnFamilyOptions familyOption = new ColumnFamilyOptions();
            if (context.getPropertyValue(familyPrefix + Rocksdb_5_4_0_ClientService.OPTIMIZE_FOR_SMALL_DB).isSet()) {
                if (context.getPropertyValue(familyPrefix + Rocksdb_5_4_0_ClientService.OPTIMIZE_FOR_SMALL_DB).asBoolean())
                    familyOption.optimizeForSmallDb();
            }
//            optimizeForPointLookup
//                    optimizeLevelStyleCompaction
//            optimizeLevelStyleCompaction
//                    optimizeUniversalStyleCompaction
//            optimizeUniversalStyleCompaction
//                    setComparator
//            setComparator
//                    setMergeOperatorName
//            setMergeOperator
//                    setCompactionFilter
//            setWriteBufferSize
//                    setMaxWriteBufferNumber
//            setMinWriteBufferNumberToMerge
//                    useFixedLengthPrefixExtractor
//            useCappedPrefixExtractor
//                    setCompressionType
//            setCompressionPerLevel
//                    setBottommostCompressionType
//            setCompressionOptions
//                    setNumLevels
//            setLevelZeroFileNumCompactionTrigger
//                    setLevelZeroSlowdownWritesTrigger
//            setLevelZeroStopWritesTrigger
//                    setTargetFileSizeBase
//            setTargetFileSizeMultiplier
//                    setMaxBytesForLevelBase
//            setLevelCompactionDynamicLevelBytes
//                    setMaxBytesForLevelMultiplier
//            setMaxCompactionBytes
//                    setArenaBlockSize
//            setDisableAutoCompactions
//                    setCompactionStyle
//            setMaxTableFilesSizeFIFO
//                    setMaxSequentialSkipInIterations
//            setMemTableConfig
//                    setTableFormatConfig
//            setInplaceUpdateSupport
//                    setInplaceUpdateNumLocks
//            setMemtablePrefixBloomSizeRatio
//                    setBloomLocality
//            setMaxSuccessiveMerges
//                    setOptimizeFiltersForHits
//            setMemtableHugePageSize
//                    setSoftPendingCompactionBytesLimit
//            setHardPendingCompactionBytesLimit
//                    setLevel0FileNumCompactionTrigger
//            setLevel0SlowdownWritesTrigger
//                    setLevel0StopWritesTrigger
//            setMaxBytesForLevelMultiplierAdditional
//                    setParanoidFileChecks
//            setMaxWriteBufferNumberToMaintain
//                    setCompactionPriority
//            setReportBgIoStats
//                    setCompactionOptionsUniversal
//            setCompactionOptionsFIFO
//                    setForceConsistencyChecks
            //TODO set up props
            familyOptions.add(familyOption);
        }
        return familyOptions;
    }

    @OnDisabled
    public void shutdown() {
        /*
        from rocksdb documentation:
        Even if ColumnFamilyHandle is pointing to a dropped Column Family, you can continue using it.
        The data is actually deleted only after you delete all outstanding ColumnFamilyHandles.
         */
        if (familiesHandler != null && !familiesHandler.isEmpty()) {
            for (ColumnFamilyHandle fHandle : familiesHandler.values()) {
                fHandle.close();
            }
        }
        if (db != null) {
            db.close();
        }
        if (dbOptions != null) {
            dbOptions.close();
        }
    }


    @Override
    public void put(Collection<ValuePutRequest> puts) throws RocksDBException {
        //TODO
    }

    @Override
    public void put(String familyName, byte[] key, byte[] value) throws RocksDBException {
        ColumnFamilyHandle fHandle = familiesHandler.get(familyName);
        db.put(fHandle, key, value);
    }

    @Override
    public void put(byte[] key, byte[] value) throws RocksDBException {
        db.put(key, value);
    }

    @Override
    public void put(String familyName, byte[] key, byte[] value, WriteOptions writeOptions) throws RocksDBException {
        ColumnFamilyHandle fHandle = familiesHandler.get(familyName);
        db.put(fHandle, writeOptions, key, value);
    }

    @Override
    public void put(byte[] key, byte[] value, WriteOptions writeOptions) throws RocksDBException {
        db.put(writeOptions, key, value);
    }

    @Override
    public Collection<GetResponse> multiGet(Collection<GetRequest> getRequests) throws RocksDBException {
        //TODO
        return null;
    }

    @Override
    public byte[] get(byte[] key) throws RocksDBException {
        return db.get(key);
    }

    @Override
    public byte[] get(byte[] key, ReadOptions rOption) throws RocksDBException {
        return db.get(rOption, key);
    }

    @Override
    public byte[] get(String familyName, byte[] key) throws RocksDBException {
        ColumnFamilyHandle fHandle = familiesHandler.get(familyName);
        return db.get(fHandle, key);
    }

    @Override
    public byte[] get(String familyName, byte[] key, ReadOptions rOption) throws RocksDBException {
        ColumnFamilyHandle fHandle = familiesHandler.get(familyName);
        return db.get(fHandle, rOption, key);
    }

    @Override
    public Collection<DeleteResponse> multiDelete(Collection<DeleteRequest> deleteRequests) throws RocksDBException {
        //TODO
        return null;
    }

    @Override
    public void delete(byte[] key) throws RocksDBException {
        db.delete(key);
    }

    @Override
    public void delete(byte[] key, WriteOptions wOption) throws RocksDBException {
        db.delete(wOption, key);
    }

    @Override
    public void delete(String familyName, byte[] key) throws RocksDBException {
        ColumnFamilyHandle fHandle = familiesHandler.get(familyName);
        db.delete(fHandle, key);
    }

    @Override
    public void delete(String familyName, byte[] key, WriteOptions wOption) throws RocksDBException {
        ColumnFamilyHandle fHandle = familiesHandler.get(familyName);
        db.delete(fHandle, wOption, key);
    }

    @Override
    public Collection<DeleteRangeResponse> multiDeleteRange(Collection<DeleteRangeRequest> deleteRangeRequests) throws RocksDBException {
        //TODO
        return null;
    }

    @Override
    public void deleteRange(byte[] keyStart, byte[] keyEnd) throws RocksDBException {
        db.deleteRange(keyStart, keyEnd);
    }

    @Override
    public void deleteRange(byte[] keyStart, byte[] keyEnd, WriteOptions wOption) throws RocksDBException {
        db.deleteRange(wOption, keyStart, keyEnd);
    }

    @Override
    public void deleteRange(String familyName, byte[] keyStart, byte[] keyEnd) throws RocksDBException {
        ColumnFamilyHandle fHandle = familiesHandler.get(familyName);
        db.deleteRange(fHandle, keyStart, keyEnd);
    }

    @Override
    public void deleteRange(String familyName, byte[] keyStart, byte[] keyEnd, WriteOptions wOption) throws RocksDBException {
        ColumnFamilyHandle fHandle = familiesHandler.get(familyName);
        db.deleteRange(fHandle, wOption, keyStart, keyEnd);
    }

    @Override
    public void scan(RocksIteratorHandler handler) throws RocksDBException {
        handler.handle(db.newIterator());
    }

    @Override
    public void scan(String familyName, RocksIteratorHandler handler) throws RocksDBException {
        ColumnFamilyHandle fHandle = familiesHandler.get(familyName);
        handler.handle(db.newIterator(fHandle));
    }

    @Override
    public void scan(String familyName, ReadOptions rOptions, RocksIteratorHandler handler) throws RocksDBException {
        ColumnFamilyHandle fHandle = familiesHandler.get(familyName);
        handler.handle(db.newIterator(fHandle, rOptions));
    }
}
