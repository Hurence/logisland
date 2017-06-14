package com.hurence.logisland.service.rocksdb;


import com.hurence.logisland.annotation.documentation.CapabilityDescription;
import com.hurence.logisland.annotation.documentation.Tags;
import com.hurence.logisland.component.AllowableValue;
import com.hurence.logisland.component.PropertyDescriptor;
import com.hurence.logisland.controller.ControllerService;
import com.hurence.logisland.service.rocksdb.delete.DeleteRangeRequest;
import com.hurence.logisland.service.rocksdb.delete.DeleteRangeResponse;
import com.hurence.logisland.service.rocksdb.delete.DeleteRequest;
import com.hurence.logisland.service.rocksdb.delete.DeleteResponse;
import com.hurence.logisland.service.rocksdb.put.ValuePutRequest;
import com.hurence.logisland.service.rocksdb.get.GetRequest;
import com.hurence.logisland.service.rocksdb.get.GetResponse;
import com.hurence.logisland.service.rocksdb.scan.RocksIteratorHandler;
import com.hurence.logisland.service.rocksdb.scan.RocksIteratorRequest;
import com.hurence.logisland.service.rocksdb.util.RocksDbStatics;
import com.hurence.logisland.validator.StandardValidators;
import org.rocksdb.*;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 *
 * This is the interface for the rocksDb service.
 * Here some thinking I got
 *
 *
 * @Note: A track for using rocksDb as a cache would be to use tmpfs/ramfs.
 *
 */

@Tags({"elasticsearch", "client"})
@CapabilityDescription("A controller service for accessing an elasticsearch client.")
public interface RocksdbClientService extends ControllerService {


    PropertyDescriptor ROCKSDB_PATH = new PropertyDescriptor.Builder()
            .name("rocksdb.path")
            .description("strategy for compaction")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

//TODO allowing readOnly mode
//    PropertyDescriptor ROCKSDB_READONLY = new PropertyDescriptor.Builder()
//            .name("rocksdb.readonly")
//            .description("Should the database be opened in readOnly mode ? You can use only one instance in read and write mode")//TODO look in documentation i dont remember well
//            .required(false)
//            .defaultValue("false")
//            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
//            .build();
    ///////////////////////////////////////
    // Properties of the column families //
    ///////////////////////////////////////
    /*
    You must specify all family currrently present in the database if you want to use the database in read and write mode.
    */

    PropertyDescriptor FAMILY_NAMES = new PropertyDescriptor.Builder()
            .name("rocksdb.family.name")
            .required(false)
            .defaultValue("default")
            .description("Comma-separated list of family names in rocksdb. You must specify all family currrently present in the database if you want to use the database in read and write mode (default).")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    ///////////////
    // DBOptions //
    ///////////////

    PropertyDescriptor OPTIMIZE_FOR_SMALL_DB = new PropertyDescriptor.Builder()
            .name("native.rocksdb.optimize_for_small_db")
            .description("TODO")
            .required(false)
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .build();
    PropertyDescriptor INCREASE_PARALLELISM = new PropertyDescriptor.Builder()
            .name("native.rocksdb.increase_parallelism")
            .description("TODO")
            .required(false)
            .addValidator(StandardValidators.INTEGER_VALIDATOR)//TODO check all int types
            .build();
    PropertyDescriptor CREATE_IF_MISSING = new PropertyDescriptor.Builder()
            .name("native.rocksdb.create_if_missing")
            .description("TODO")
            .required(false)
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .build();
    PropertyDescriptor CREATE_MISSING_COLUMN_FAMILIES = new PropertyDescriptor.Builder()
            .name("native.rocksdb.create_missing_column_families")
            .description("TODO")
            .required(false)
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .build();
    PropertyDescriptor ERROR_IF_EXISTS = new PropertyDescriptor.Builder()
            .name("native.rocksdb.error_if_exists")
            .description("TODO")
            .required(false)
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .build();
    PropertyDescriptor PARANOID_CHECKS = new PropertyDescriptor.Builder()
            .name("native.rocksdb.paranoid_checks")
            .description("TODO")
            .required(false)
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .build();
    PropertyDescriptor MAX_OPEN_FILES = new PropertyDescriptor.Builder()
            .name("native.rocksdb.max_open_files")
            .description("TODO")
            .required(false)
            .addValidator(StandardValidators.INTEGER_VALIDATOR)
            .build();
    PropertyDescriptor MAX_FILE_OPENING_THREADS = new PropertyDescriptor.Builder()
            .name("native.rocksdb.max_file_opening_threads")
            .description("TODO")
            .required(false)
            .addValidator(StandardValidators.INTEGER_VALIDATOR)
            .build();
    PropertyDescriptor MAX_TOTAL_WAL_SIZE = new PropertyDescriptor.Builder()
            .name("native.rocksdb.max_total_wal_size")
            .description("TODO")
            .required(false)
            .addValidator(StandardValidators.INTEGER_VALIDATOR)
            .build();
    PropertyDescriptor USE_FSYNC = new PropertyDescriptor.Builder()
            .name("native.rocksdb.use_fsync")
            .description("TODO")
            .required(false)
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .build();
    PropertyDescriptor DB_LOG_DIR = new PropertyDescriptor.Builder()
            .name("native.rocksdb.db_log_dir")
            .description("TODO")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    PropertyDescriptor WAL_DIR = new PropertyDescriptor.Builder()
            .name("native.rocksdb.wal_dir")
            .description("TODO")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    PropertyDescriptor DELETE_OBSOLETE_FILES_PERIOD_MICROS = new PropertyDescriptor.Builder()
            .name("native.rocksdb.delete_obsolete_files_period_micros")
            .description("TODO")
            .required(false)
            .addValidator(StandardValidators.LONG_VALIDATOR)
            .build();
    PropertyDescriptor BASE_BACKGROUND_COMPACTIONS = new PropertyDescriptor.Builder()
            .name("native.rocksdb.base_background_compactions")
            .description("TODO")
            .required(false)
            .addValidator(StandardValidators.INTEGER_VALIDATOR)
            .build();
    PropertyDescriptor MAX_BACKGROUND_COMPACTIONS = new PropertyDescriptor.Builder()
            .name("native.rocksdb.max_background_compactions")
            .description("TODO")
            .required(false)
            .addValidator(StandardValidators.INTEGER_VALIDATOR)
            .build();
    PropertyDescriptor MAX_SUBCOMPACTIONS = new PropertyDescriptor.Builder()
            .name("native.rocksdb.max_subcompactions")
            .description("TODO")
            .required(false)
            .addValidator(StandardValidators.INTEGER_VALIDATOR)
            .build();
    PropertyDescriptor MAX_BACKGROUND_FLUSHES = new PropertyDescriptor.Builder()
            .name("native.rocksdb.max_background_flushes")
            .description("TODO")
            .required(false)
            .addValidator(StandardValidators.INTEGER_VALIDATOR)
            .build();
    PropertyDescriptor MAX_LOG_FILE_SIZE = new PropertyDescriptor.Builder()
            .name("native.rocksdb.max_log_file_size")
            .description("TODO")
            .required(false)
            .addValidator(StandardValidators.INTEGER_VALIDATOR)
            .build();
    PropertyDescriptor LOG_FILE_TIME_TO_ROLL = new PropertyDescriptor.Builder()
            .name("native.rocksdb.log_file_time_to_roll")
            .description("TODO")
            .required(false)
            .addValidator(StandardValidators.LONG_VALIDATOR)
            .build();
    PropertyDescriptor KEEP_LOG_FILE_NUM = new PropertyDescriptor.Builder()
            .name("native.rocksdb.keep_log_file_num")
            .description("TODO")
            .required(false)
            .addValidator(StandardValidators.LONG_VALIDATOR)
            .build();
    PropertyDescriptor RECYCLE_LOG_FILE_NUM = new PropertyDescriptor.Builder()
            .name("native.rocksdb.recycle_log_file_num")
            .description("TODO")
            .required(false)
            .addValidator(StandardValidators.LONG_VALIDATOR)
            .build();
    PropertyDescriptor MAX_MANIFEST_FILE_SIZE = new PropertyDescriptor.Builder()
            .name("native.rocksdb.max_manifest_file_size")
            .description("TODO")
            .required(false)
            .addValidator(StandardValidators.LONG_VALIDATOR)
            .build();
    PropertyDescriptor TABLE_CACHE_NUMSHARDBITS = new PropertyDescriptor.Builder()
            .name("native.rocksdb.table_cache_numshardbits")
            .description("TODO")
            .required(false)
            .addValidator(StandardValidators.INTEGER_VALIDATOR)
            .build();
    PropertyDescriptor WAL_TTL_SECONDS = new PropertyDescriptor.Builder()
            .name("native.rocksdb.wal_ttl_seconds")
            .description("TODO")
            .required(false)
            .addValidator(StandardValidators.LONG_VALIDATOR)
            .build();
    PropertyDescriptor WAL_SIZE_LIMIT_MB = new PropertyDescriptor.Builder()
            .name("native.rocksdb.wal_size_limit_mb")
            .description("TODO")
            .required(false)
            .addValidator(StandardValidators.LONG_VALIDATOR)
            .build();
    PropertyDescriptor MANIFEST_PREALLOCATION_SIZE = new PropertyDescriptor.Builder()
            .name("native.rocksdb.manifest_reallocation_size")
            .description("TODO")
            .required(false)
            .addValidator(StandardValidators.LONG_VALIDATOR)
            .build();
    PropertyDescriptor USE_DIRECT_READS = new PropertyDescriptor.Builder()
            .name("native.rocksdb.use_direct_reads")
            .description("TODO")
            .required(false)
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .build();
    PropertyDescriptor USE_DIRECT_IO_FOR_FLUSH_AND_COMPACTION = new PropertyDescriptor.Builder()
            .name("native.rocksdb.use_direct_io_for_flush_and_compaction")
            .description("TODO")
            .required(false)
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .build();
    PropertyDescriptor ALLOW_F_ALLOCATE = new PropertyDescriptor.Builder()
            .name("native.rocksdb.allow_f_allocate")
            .description("TODO")
            .required(false)
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .build();
    PropertyDescriptor ALLOW_MMAP_READS = new PropertyDescriptor.Builder()
            .name("native.rocksdb.allow_mmap_reads")
            .description("TODO")
            .required(false)
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .build();
    PropertyDescriptor ALLOW_MMAP_WRITES = new PropertyDescriptor.Builder()
            .name("native.rocksdb.allow_mmap_writes")
            .description("TODO")
            .required(false)
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .build();
    PropertyDescriptor IS_FD_CLOSE_ON_EXEC = new PropertyDescriptor.Builder()
            .name("native.rocksdb.is_fd_close_on_exec")
            .description("TODO")
            .required(false)
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .build();
    PropertyDescriptor STATS_DUMP_PERIOD_SEC = new PropertyDescriptor.Builder()
            .name("native.rocksdb.stats_dump_period_sec")
            .description("TODO")
            .required(false)
            .addValidator(StandardValidators.INTEGER_VALIDATOR)
            .build();
    PropertyDescriptor ADVISE_RANDOM_ON_OPEN = new PropertyDescriptor.Builder()
            .name("native.rocksdb.advise_random_on_open")
            .description("TODO")
            .required(false)
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .build();
    PropertyDescriptor DB_WRITE_BUFFER_SIZE = new PropertyDescriptor.Builder()
            .name("native.rocksdb.db_write_buffer_size")
            .description("TODO")
            .required(false)
            .addValidator(StandardValidators.LONG_VALIDATOR)
            .build();
    PropertyDescriptor NEW_TABLE_READER_FOR_COMPACTION_INPUTS = new PropertyDescriptor.Builder()
            .name("native.rocksdb.new_table_reader_for_compaction_inputs")
            .description("TODO")
            .required(false)
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .build();
    PropertyDescriptor COMPACTION_READAHEAD_SIZE = new PropertyDescriptor.Builder()
            .name("native.rocksdb.compaction_readahead_size")
            .description("TODO")
            .required(false)
            .addValidator(StandardValidators.LONG_VALIDATOR)
            .build();
    PropertyDescriptor RANDOM_ACCESS_MAX_BUFFER_SIZE = new PropertyDescriptor.Builder()
            .name("native.rocksdb.random_access_max_buffer_size")
            .description("TODO")
            .required(false)
            .addValidator(StandardValidators.LONG_VALIDATOR)
            .build();
    PropertyDescriptor WRITABLE_FILE_MAX_BUFFER_SIZE = new PropertyDescriptor.Builder()
            .name("native.rocksdb.writable_file_max_buffer_size")
            .description("TODO")
            .required(false)
            .addValidator(StandardValidators.LONG_VALIDATOR)
            .build();
    PropertyDescriptor USE_ADAPTIVE_MUTEX = new PropertyDescriptor.Builder()
            .name("native.rocksdb.use_adaptive_mutex")
            .description("TODO")
            .required(false)
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .build();
    PropertyDescriptor BYTES_PER_SYNC = new PropertyDescriptor.Builder()
            .name("native.rocksdb.bytes_per_sync")
            .description("TODO")
            .required(false)
            .addValidator(StandardValidators.LONG_VALIDATOR)
            .build();
    PropertyDescriptor WAL_BYTES_PER_SYNC = new PropertyDescriptor.Builder()
            .name("native.rocksdb.wal_bytes_per_sync")
            .description("TODO")
            .required(false)
            .addValidator(StandardValidators.LONG_VALIDATOR)
            .build();
    PropertyDescriptor ENABLE_THREAD_TRACKING = new PropertyDescriptor.Builder()
            .name("native.rocksdb.enable_thread_tracking")
            .description("TODO")
            .required(false)
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .build();
    PropertyDescriptor DELAYED_WRITE_RATE = new PropertyDescriptor.Builder()
            .name("native.rocksdb.delayed_write_rate")
            .description("TODO")
            .required(false)
            .addValidator(StandardValidators.LONG_VALIDATOR)
            .build();
    PropertyDescriptor ALLOW_CONCURRENT_MEMTABLE_WRITE = new PropertyDescriptor.Builder()
            .name("native.rocksdb.allow_concurrent_memtable_write")
            .description("TODO")
            .required(false)
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .build();
    PropertyDescriptor ENABLE_WRITE_THREAD_ADAPTIVE_YIELD = new PropertyDescriptor.Builder()
            .name("native.rocksdb.allow_write_thread_adaptive_yield")
            .description("TODO")
            .required(false)
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .build();
    PropertyDescriptor WRITE_THREAD_MAX_YIELD_USEC = new PropertyDescriptor.Builder()
            .name("native.rocksdb.write_thread_max_yield_usec")
            .description("TODO")
            .required(false)
            .addValidator(StandardValidators.LONG_VALIDATOR)
            .build();
    PropertyDescriptor WRITE_THREAD_SLOW_YIELD_USEC = new PropertyDescriptor.Builder()
            .name("native.rocksdb.write_thread_slow_yield_usec")
            .description("TODO")
            .required(false)
            .addValidator(StandardValidators.LONG_VALIDATOR)
            .build();
    PropertyDescriptor SKIP_STATS_UPDATE_ON_DB_OPEN = new PropertyDescriptor.Builder()
            .name("native.rocksdb.skip_stats_update_on_db_open")
            .description("TODO")
            .required(false)
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .build();
    PropertyDescriptor ALLOW_2_PC = new PropertyDescriptor.Builder()
            .name("native.rocksdb.allow_2_pc")
            .description("TODO")
            .required(false)
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .build();
    PropertyDescriptor FAIL_IF_OPTIONS_FILE_ERROR = new PropertyDescriptor.Builder()
            .name("native.rocksdb.fail_if_options_file_error")
            .description("TODO")
            .required(false)
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .build();
    PropertyDescriptor DUMP_MALLOC_STATS = new PropertyDescriptor.Builder()
            .name("native.rocksdb.dump_malloc_stats")
            .description("TODO")
            .required(false)
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .build();
    PropertyDescriptor AVOID_FLUSH_DURING_RECOVERY = new PropertyDescriptor.Builder()
            .name("native.rocksdb.avoid_flush_during_recovery")
            .description("TODO")
            .required(false)
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .build();
    PropertyDescriptor AVOID_FLUSH_DURING_SHUTDOWN = new PropertyDescriptor.Builder()
            .name("native.rocksdb.avoid_flush_during_shutdown")
            .description("TODO")
            .required(false)
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .build();


    ///////////////////////
    // Family properties //
    ///////////////////////

    PropertyDescriptor OPTIMIZE_FOR_POINT_LOOKUP = new PropertyDescriptor.Builder()
            .name("native.rocksdb.optimize.for.point.lookup")
            .description("Use this if you don't need to keep the data sorted, i.e. you'll never use" +
                    " an iterator, only Put() and Get() API calls. indicate size for cache in MB.")
            .required(false)
            .addValidator(StandardValidators.POSITIVE_LONG_VALIDATOR)
            .build();
    PropertyDescriptor OPTIMIZE_LEVEL_STYLE_COMPACTION = new PropertyDescriptor.Builder()
            .name("native.rocksdb.optimize.level.style.compaction")
            .description("memtable memory budget in bytes for level style compaction.")
            .required(false)
            .addValidator(StandardValidators.POSITIVE_LONG_VALIDATOR)
            .build();
    PropertyDescriptor OPTIMIZE_UNIVERSAL_STYLE_COMPACTION = new PropertyDescriptor.Builder()
            .name("native.rocksdb.optimize.universal.style.compaction")
            .description("memtable memory budget in bytes for universal style compaction.")
            .required(false)
            .addValidator(StandardValidators.POSITIVE_LONG_VALIDATOR)
            .build();

    AllowableValue BYTEWISE_COMPARATOR = new AllowableValue("bytewise", "Default lexicographical order",
            "Order keys by lexicographical order.");
    AllowableValue REVERSE_BYTEWISE_COMPARATOR = new AllowableValue("reverseBytewise", "Reverse lexicographical order",
            "Order keys by reverse lexicographical order.");

    PropertyDescriptor FAMILY_KEY_COMPARATOR = new PropertyDescriptor.Builder()
            .name("native.rocksdb.key.comparator")
            .description("How to compare each key in database ? Comparator can be set once upon database creation.")
            .required(false)
            .allowableValues(BYTEWISE_COMPARATOR, REVERSE_BYTEWISE_COMPARATOR)
            .build();

    PropertyDescriptor MERGE_OPERATOR_NAME = new PropertyDescriptor.Builder()
            .name("native.rocksdb.merge.operator.name")
            .description("Set the merge operator to be used for merging two merge operands" +
                    " of the same key. The merge function is invoked during" +
                    " compaction and at lookup time, if multiple key/value pairs belonging" +
                    " to the same key are found in the database.")
            .required(false)
            .allowableValues("put", "uint64add", "stringappend", "stringappendtest")
            .build();
//    //TODO implement custom merge operator
//    PropertyDescriptor MERGE_OPERATOR = new PropertyDescriptor.Builder()
//            .name("native.rocksdb.merge.operator")
//            .description("Custom merge operator.")
//            .required(false)
//            .build();
//    //TODO implement custom compaction filter
//    PropertyDescriptor COMPACTION_FILTER = new PropertyDescriptor.Builder()
//            .name("native.rocksdb.compaction.filter")
//            .description("custom compaction filter.")
//            .required(false)
//            .addValidator(StandardValidators.POSITIVE_LONG_VALIDATOR)
//            .build();
    PropertyDescriptor WRITE_BUFFER_SIZE = new PropertyDescriptor.Builder()
            .name("native.rocksdb.write.buffer.size")
            .description("the size of write buffer. Amount of data to build up in memory (backed by an unsorted log" +
                    " on disk) before converting to a sorted on-disk file.")
            .required(false)
            .addValidator(StandardValidators.POSITIVE_LONG_VALIDATOR)
            .build();
    PropertyDescriptor MAX_WRITE_BUFFER_NUMBER = new PropertyDescriptor.Builder()
            .name("native.rocksdb.max.write.buffer.number")
            .description("Maximum number of write buffers.")
            .required(false)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .build();
    PropertyDescriptor MIN_WRITE_BUFFER_NUMBER_TO_MERGE = new PropertyDescriptor.Builder()
            .name("native.rocksdb.min.write.buffer.number.to.merge")
            .description("the minimum number of write buffers" +
                    " that will be merged together.")
            .required(false)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .build();
    PropertyDescriptor USE_FIXED_LENGTH_PREFIX_EXTRACTOR = new PropertyDescriptor.Builder()
            .name("native.rocksdb.fixed.length.prefix.extractor")
            .description("use the first n bytes of a key as its prefix.")
            .required(false)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .build();
    PropertyDescriptor USE_CAPPED_PREFIX_EXTRACTOR = new PropertyDescriptor.Builder()
            .name("native.rocksdb.capped.prefix.extractor")
            .description("use the first n bytes of a key as its prefix. Same as fixed length prefix extractor," +
                    " except that when slice is shorter than the fixed length, it will use the full key.")
            .required(false)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .build();
    PropertyDescriptor COMPRESSION_TYPE = new PropertyDescriptor.Builder()
            .name("native.rocksdb.compression.type")
            .description("the compression to use.")
            .required(false)
            .allowableValues(RocksDbStatics.compressionTypes)
            .build();
    PropertyDescriptor COMPRESSION_PER_LEVEL = new PropertyDescriptor.Builder()
            .name("native.rocksdb.compression.per.level")
            .description("Compression type for each level (L0..LN)")
            .required(false)
            .addValidator(RocksDbStatics.LIST_COMPRESSION_TYPE_VALIDATOR_COMMA_SEPARATED)
            .build();
//    //TODO implement BOTTOMMOST_COMPRESSION_TYPE
//    PropertyDescriptor BOTTOMMOST_COMPRESSION_TYPE = new PropertyDescriptor.Builder()
//            .name("native.rocksdb.bottommost.compression.type")
//            .description("use the first n bytes of a key as its prefix. Same as fixed length prefix extractor," +
//                    " except that when slice is shorter than the fixed length, it will use the full key.")
//            .required(false)
//            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
//            .build();
//    //TODO implement COMPRESSION_OPTIONS
//    PropertyDescriptor COMPRESSION_OPTIONS = new PropertyDescriptor.Builder()
//            .name("native.rocksdb.compression.options")
//            .description("use the first n bytes of a key as its prefix. Same as fixed length prefix extractor," +
//                    " except that when slice is shorter than the fixed length, it will use the full key.")
//            .required(false)
//            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
//            .build();
    PropertyDescriptor NUM_LEVELS = new PropertyDescriptor.Builder()
            .name("native.rocksdb.num.levels")
            .description("The number of levels. (compaction)")
            .required(false)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .build();
    PropertyDescriptor LEVEL_ZERO_FILE_NUM_COMPACTION_TRIGGER = new PropertyDescriptor.Builder()
            .name("native.rocksdb.level.zero.file.num.compaction.trigger")
            .description("The number of files in level-0 to trigger compaction.")
            .required(false)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .build();
    PropertyDescriptor LEVEL_ZERO_SLOWDOWN_WRITES_TRIGGER = new PropertyDescriptor.Builder()
            .name("native.rocksdb.level.zero.slowdown.writes.trigger")
            .description("Soft limit on number of level-0 files.")
            .required(false)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .build();
    PropertyDescriptor LEVEL_ZERO_STOP_WRITES_TRIGGER = new PropertyDescriptor.Builder()
            .name("native.rocksdb.level.zero.stop.writes.trigger")
            .description("The hard limit of the number of level-0 files.")
            .required(false)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .build();
    PropertyDescriptor TARGET_FILE_SIZE_BASE = new PropertyDescriptor.Builder()
            .name("native.rocksdb.target.file.size.base")
            .description("The target size of a level-0 file.")
            .required(false)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .build();
    PropertyDescriptor TARGET_FILE_SIZE_MULTIPLIER = new PropertyDescriptor.Builder()
            .name("native.rocksdb.target.file.size.multiplier")
            .description("the size ratio between a level-(L+1) file and level-L file.")
            .required(false)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .build();
    PropertyDescriptor MAX_BYTES_FOR_LEVEL_BASE = new PropertyDescriptor.Builder()
            .name("native.rocksdb.max.bytes.for.level.base")
            .description("Maximum bytes for level base.")
            .required(false)
            .addValidator(StandardValidators.POSITIVE_LONG_VALIDATOR)
            .build();
    PropertyDescriptor LEVEL_COMPACTION_DYNAMIC_LEVEL_BYTES = new PropertyDescriptor.Builder()
            .name("native.rocksdb.level.compaction.dynamic.level.bytes")
            .description("LevelCompactionDynamicLevelBytes should be enabled ? " +
            " @Experimental(Turning this feature on or off for an existing DB can cause" +
                    " unexpected LSM tree structure so it's not recommended)")
            .required(false)
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .build();
    PropertyDescriptor MAX_BYTES_FOR_LEVEL_MULTIPLIER = new PropertyDescriptor.Builder()
            .name("native.rocksdb.max.bytes.for.level.multiplier")
            .description("the ratio between the total size of level-(L+1)" +
                    " files and the total size of level-L files for all L.")
            .required(false)
            .addValidator(StandardValidators.DOUBLE_VALIDATOR)
            .build();
    PropertyDescriptor MAX_COMPACTION_BYTES = new PropertyDescriptor.Builder()
            .name("native.rocksdb.max.compaction.bytes")
            .description("Max bytes in a compaction.")
            .required(false)
            .addValidator(StandardValidators.LONG_VALIDATOR)
            .build();
    PropertyDescriptor ARENA_BLOCK_SIZE = new PropertyDescriptor.Builder()
            .name("native.rocksdb.arena.block.size")
            .description("The size of an arena block.")
            .required(false)
            .addValidator(StandardValidators.LONG_VALIDATOR)
            .build();
    PropertyDescriptor DISABLE_AUTO_COMPACTIONS = new PropertyDescriptor.Builder()
            .name("native.rocksdb.disable.auto.compactions")
            .description("Should auto-compactions be disabled ?")
            .required(false)
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .build();
    PropertyDescriptor COMPACTION_STYLE = new PropertyDescriptor.Builder()
            .name("native.rocksdb.compaction.style")
            .description("The compaction style to use")
            .required(false)
            .allowableValues(RocksDbStatics.compactionStyles)
            .build();
    PropertyDescriptor MAX_TABLE_FILES_SIZE_FIFO = new PropertyDescriptor.Builder()
            .name("native.rocksdb.max.table.files.size.fifo")
            .description("The size limit of the total sum of table files.")
            .required(false)
            .addValidator(StandardValidators.LONG_VALIDATOR)
            .build();
    PropertyDescriptor MAX_SEQUENTIAL_SKIP_IN_ITERATIONS = new PropertyDescriptor.Builder()
            .name("native.rocksdb.max.sequential.skip.in.iterations")
            .description("The number of keys could be skipped in a iteration.")
            .required(false)
            .addValidator(StandardValidators.LONG_VALIDATOR)
            .build();
//    //TODO implement MEM_TABLE_CONFIG
//    PropertyDescriptor MEM_TABLE_CONFIG = new PropertyDescriptor.Builder()
//            .name("native.rocksdb.mem.table.config")
//            .description("The number of keys could be skipped in a iteration.")
//            .required(false)
//            .addValidator(StandardValidators.LONG_VALIDATOR)
//            .build();
//    //TODO implement TABLE_FORMAT_CONFIG
//    PropertyDescriptor TABLE_FORMAT_CONFIG = new PropertyDescriptor.Builder()
//            .name("native.rocksdb.max.sequential.skip.in.iterations")
//            .description("The number of keys could be skipped in a iteration.")
//            .required(false)
//            .addValidator(StandardValidators.LONG_VALIDATOR)
//            .build();
    PropertyDescriptor IN_PLACE_UPDATE_SUPPORT = new PropertyDescriptor.Builder()
            .name("native.rocksdb.in.place.update.support")
            .description("True if thread-safe inplace updates are allowed.")
            .required(false)
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .build();
    PropertyDescriptor IN_PLACE_UPDATE_NUM_LOCKS = new PropertyDescriptor.Builder()
            .name("native.rocksdb.in.place.update.num.locks")
            .description("The number of locks used for inplace updates.")
            .required(false)
            .addValidator(StandardValidators.LONG_VALIDATOR)
            .build();
    PropertyDescriptor MEM_TABLE_PREFIX_BLOOM_SIZE_RATIO = new PropertyDescriptor.Builder()
            .name("native.rocksdb.mem.table.prefix.bloom.size.ratio")
            .description("The ratio. If prefix_extractor is set and memtable_prefix_bloom_size_ratio is not 0," +
                    " create prefix bloom for memtable with the size of" +
                    " write_buffer_size * memtable_prefix_bloom_size_ratio." +
                    " If it is larger than 0.25, it is santinized to 0.25.")
            .required(false)
            .addValidator(StandardValidators.DOUBLE_VALIDATOR)
            .build();
    PropertyDescriptor BLOOM_LOCALITY = new PropertyDescriptor.Builder()
            .name("native.rocksdb.bloom.locality")
            .description("The level of locality of bloom-filter probes.")
            .required(false)
            .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
            .build();
    PropertyDescriptor MAX_SUCCESSIVE_MERGES = new PropertyDescriptor.Builder()
            .name("native.rocksdb.max.successive.merges")
            .description("The maximum number of successive merges.")
            .required(false)
            .addValidator(StandardValidators.LONG_VALIDATOR)
            .build();
    PropertyDescriptor OPTIMIZE_FILTERS_FOR_HITS = new PropertyDescriptor.Builder()
            .name("native.rocksdb.optimize.filters.for.hits")
            .description("This flag specifies that the implementation should optimize the filters" +
                    " mainly for cases where keys are found rather than also optimize for keys" +
                    " missed. This would be used in cases where the application knows that" +
                    " there are very few misses or the performance in the case of misses is not" +
                    " important.")
            .required(false)
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .build();
    PropertyDescriptor MEMTABLE_HUGE_PAGE_SIZE = new PropertyDescriptor.Builder()
            .name("native.rocksdb.memtable.huge.page.size")
            .description("The page size of the huge page tlb")
            .required(false)
            .addValidator(StandardValidators.LONG_VALIDATOR)
            .build();
    PropertyDescriptor SOFT_PENDING_COMPACTION_BYTES_LIMIT = new PropertyDescriptor.Builder()
            .name("native.rocksdb.soft.pending.compaction.bytes.limit")
            .description("The soft limit to impose on compaction.  All writes will be slowed down to at least" +
                    " delayed_write_rate if estimated bytes needed to be compaction exceed this threshold.")
            .required(false)
            .addValidator(StandardValidators.LONG_VALIDATOR)
            .build();
    PropertyDescriptor HARD_PENDING_COMPACTION_BYTES_LIMIT = new PropertyDescriptor.Builder()
            .name("native.rocksdb.hard.pending.compaction.bytes.limit")
            .description("The hard limit to impose on compaction.  All writes are stopped if estimated bytes" +
                            " needed to be compaction exceed this threshold.")
            .required(false)
            .addValidator(StandardValidators.LONG_VALIDATOR)
            .build();
    PropertyDescriptor LEVEL0_FILE_NUM_COMPACTION_TRIGGER = new PropertyDescriptor.Builder()
            .name("native.rocksdb.level0.file.num.compaction.trigger")
            .description("The number of files to trigger level-0 compaction. 0 means that" +
                    " level-0 compaction will not be triggered by number of files at all.")
            .required(false)
            .addValidator(StandardValidators.INTEGER_VALIDATOR)
            .build();
    PropertyDescriptor LEVEL0_SLOWDOWN_WRITES_TRIGGER = new PropertyDescriptor.Builder()
            .name("native.rocksdb.level0.slowdown.writes.trigger")
            .description("The soft limit on the number of level-0 files.  0 means that no writing slow down will" +
                            "be triggered by number of files in level-0.")
            .required(false)
            .addValidator(StandardValidators.INTEGER_VALIDATOR)
            .build();
    PropertyDescriptor LEVEL0_STOP_WRITES_TRIGGER = new PropertyDescriptor.Builder()
            .name("native.rocksdb.level0.stop.writes.trigger")
            .description("The hard limit on the number of level-0 files. We stop writes at this point.")
            .required(false)
            .addValidator(StandardValidators.INTEGER_VALIDATOR)
            .build();
//    //TODO implement MAX_BYTES_FOR_LEVEL_MULTIPLIER_ADDITIONAL
//    PropertyDescriptor MAX_BYTES_FOR_LEVEL_MULTIPLIER_ADDITIONAL = new PropertyDescriptor.Builder()
//            .name("native.rocksdb.level0.stop.writes.trigger")
//            .description("The hard limit on the number of level-0 files. We stop writes at this point.")
//            .required(false)
//            .addValidator(StandardValidators.INTEGER_VALIDATOR)
//            .build();
    PropertyDescriptor PARANOID_FILE_CHECKS = new PropertyDescriptor.Builder()
            .name("native.rocksdb.paranoid.file.checks")
            .description("Enable paranoid file checks.  After writing every SST file, reopen it and read all the keys.")
            .required(false)
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .build();
    PropertyDescriptor MAX_WRITE_BUFFER_NUMBER_TO_MAINTAIN = new PropertyDescriptor.Builder()
            .name("native.rocksdb.max.write.buffer.number.to.maintain")
            .description("The maximum number of write buffers to maintain. See RocksDb documentation for more info.")
            .required(false)
            .addValidator(StandardValidators.INTEGER_VALIDATOR)
            .build();
//    //TODO implement COMPACTION_PRIORITY
//    PropertyDescriptor COMPACTION_PRIORITY = new PropertyDescriptor.Builder()
//            .name("native.rocksdb.max.write.buffer.number.to.maintain")
//            .description("The maximum number of write buffers to maintain. See RocksDb documentation for more info.")
//            .required(false)
//            .addValidator(StandardValidators.INTEGER_VALIDATOR)
//            .build();
    PropertyDescriptor REPORT_BG_IO_STATS = new PropertyDescriptor.Builder()
            .name("native.rocksdb.report.bg.io.stats")
            .description("Measure IO stats in compactions and flushes, if true.")
            .required(false)
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .build();
//    //TODO implement COMPACTION_OPTIONS_UNIVERSAL
//    PropertyDescriptor COMPACTION_OPTIONS_UNIVERSAL = new PropertyDescriptor.Builder()
//            .name("native.rocksdb.report.bg.io.stats")
//            .description("Measure IO stats in compactions and flushes, if true.")
//            .required(false)
//            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
//            .build();
//    //TODO implement COMPACTION_OPTIONS_FIFO
//    PropertyDescriptor COMPACTION_OPTIONS_FIFO = new PropertyDescriptor.Builder()
//            .name("native.rocksdb.report.bg.io.stats")
//            .description("Measure IO stats in compactions and flushes, if true.")
//            .required(false)
//            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
//            .build();
    PropertyDescriptor FORCE_CONSISTENCY_CHECKS = new PropertyDescriptor.Builder()
            .name("native.rocksdb.force.consistency.checks")
            .description("In debug mode, RocksDB run consistency checks on the LSM everytime the LSM" +
                    " change (Flush, Compaction, AddFile). These checks are disabled in release" +
                    " mode, use this option to enable them in release mode as well.")
            .required(false)
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .build();

    ////////////////////
    // Put operations //
    ////////////////////

    /**
     * Puts a batch of key value pairs in their column family using specific write option
     *
     * @param puts a list of put mutations
     * @throws RocksDBException thrown when there are communication errors with RocksDb
     */
    void multiPut(Collection<ValuePutRequest> puts) throws RocksDBException;

    /**
     * Puts a batch of key value pairs in their column family using specific write option
     *
     * @param put a put mutation
     * @throws RocksDBException thrown when there are communication errors with RocksDb
     * @throws NullPointerException if put is null
     */
    void put(ValuePutRequest put) throws RocksDBException;

    /**
     * Puts a key value pairs in their column family
     *
     * @param familyName family to put data in
     * @param key the key of the value to store
     * @param value the value to store in the specified family
     * @throws RocksDBException thrown when there are communication errors with RocksDb
     */
    void put(String familyName, byte[] key, byte[] value) throws RocksDBException;

    /**
     * Puts a batch of key value pairs in 'default' column family
     *
     * @param key the key of the value to store
     * @param value the value to store in the specified family
     * @throws RocksDBException thrown when there are communication errors with RocksDb
     */
    void put(byte[] key, byte[] value) throws RocksDBException;

    /**
     * Puts a key value pairs in their column family using specific option
     *
     * @param familyName family to put data in
     * @param key the key of the value to store
     * @param value the value to store in the specified family
     * @param writeOptions
     * @throws RocksDBException thrown when there are communication errors with RocksDb
     */
    void put(String familyName, byte[] key, byte[] value, WriteOptions writeOptions) throws RocksDBException;

    /**
     * Puts a key value pairs in 'default' column family using specific option
     *
     * @param key the key of the value to store
     * @param value the value to store in the specified family
     * @param writeOptions
     * @throws RocksDBException thrown when there are communication errors with RocksDb
     */
    void put(byte[] key, byte[] value, WriteOptions writeOptions) throws RocksDBException;

    ////////////////////
    // get operations //
    ////////////////////

    /**
     *
     *
     * @param getRequests a list of single get to do
     * @return a list of response
     * @throws RocksDBException thrown when there are communication errors with RocksDb
     */
    Collection<GetResponse> multiGet(Collection<GetRequest> getRequests) throws RocksDBException;

    /**
     *
     * @param getRequest a single get request
     * @return a single getResponse
     * @throws RocksDBException thrown when there are communication errors with RocksDb
     */
    GetResponse get(GetRequest getRequest) throws RocksDBException;

    /**
     *
     * @param key the key to retrieve the value in the 'default' family
     * @return
     * @throws RocksDBException thrown when there are communication errors with RocksDb
     */
    byte[] get(byte[] key) throws RocksDBException;

    /**
     *
     * @param key the key to retrieve the value in the 'default' family
     * @param rOption
     * @return
     * @throws RocksDBException thrown when there are communication errors with RocksDb
     */
    byte[] get(byte[] key, ReadOptions rOption) throws RocksDBException;

    /**
     *
     * @param familyName the family where to get the value from
     * @param key the key to retrieve the value in the family
     * @return
     * @throws RocksDBException thrown when there are communication errors with RocksDb
     */
    byte[] get(String familyName, byte[] key) throws RocksDBException;

    /**
     *
     * @param familyName the family where to get the value from
     * @param key the key to retrieve the value in the family
     * @param rOption
     * @return
     * @throws RocksDBException thrown when there are communication errors with RocksDb
     */
    byte[] get(String familyName, byte[] key, ReadOptions rOption) throws RocksDBException;

    ///////////////////////
    // remove operations //
    ///////////////////////

    /**
     *
     * @param deleteRequests a list of value to delete
     * @return
     * @throws RocksDBException thrown when there are communication errors with RocksDb
     */
    Collection<DeleteResponse> multiDelete(Collection<DeleteRequest> deleteRequests) throws RocksDBException;

    /**
     *
     * @param deleteRequest a value to delete
     * @return
     * @throws RocksDBException thrown when there are communication errors with RocksDb
     */
    DeleteResponse delete(DeleteRequest deleteRequest) throws RocksDBException;

    /**
     *
     * @param key a key to delete with his value in 'default' family
     * @throws RocksDBException thrown when there are communication errors with RocksDb
     */
    void delete(byte[] key) throws RocksDBException;

    /**
     *
     * @param key  a key to delete with his value in 'default' family
     * @param wOption
     * @throws RocksDBException thrown when there are communication errors with RocksDb
     */
    void delete(byte[] key, WriteOptions wOption) throws RocksDBException;

    /**
     *
     * @param familyName the family to do the delete
     * @param key  a key to delete with his value in family
     * @throws RocksDBException thrown when there are communication errors with RocksDb
     */
    void delete(String familyName, byte[] key) throws RocksDBException;

    /**
     *
     * @param familyName the family to do the delete
     * @param key a key to delete with his value in family
     * @param wOption
     * @throws RocksDBException thrown when there are communication errors with RocksDb
     */
    void delete(String familyName, byte[] key, WriteOptions wOption) throws RocksDBException;

    /////////////////////////////
    // singleDelete operations //
    /////////////////////////////

    //TODO but it seems really very very specific and has many constraint
    //https://github.com/facebook/rocksdb/wiki/Single-Delete

    ///////////////////////////////
    // delete range operations ////
    ///////////////////////////////

    /**
     *
     * @param keyStart first key to delete data from in 'default' family (included)
     * @param keyEnd last key to delete data from in 'default' family (excluded)
     * @throws RocksDBException thrown when there are communication errors with RocksDb
     */
    void deleteRange(byte[] keyStart, byte[] keyEnd) throws RocksDBException;

    /**
     *
     * @param keyStart first key to delete data from in 'default' family (included)
     * @param keyEnd last key to delete data from in 'default' family (excluded)
     * @param wOption
     * @throws RocksDBException thrown when there are communication errors with RocksDb
     */
    void deleteRange(byte[] keyStart, byte[] keyEnd, WriteOptions wOption) throws RocksDBException;

    /**
     *
     * @param familyName family name to delete data from
     * @param keyStart first key to delete data from in family (included)
     * @param keyEnd last key to delete data from in family (excluded)
     * @throws RocksDBException thrown when there are communication errors with RocksDb
     */
    void deleteRange(String familyName, byte[] keyStart, byte[] keyEnd) throws RocksDBException;

    /**
     *
     * @param familyName family name to delete data from
     * @param keyStart first key to delete data from in family (included)
     * @param keyEnd last key to delete data from in family (excluded)
     * @param wOption
     * @throws RocksDBException thrown when there are communication errors with RocksDb
     */
    void deleteRange(String familyName, byte[] keyStart, byte[] keyEnd, WriteOptions wOption) throws RocksDBException;

    ///////////////////////////////////////
    // scan operations (with iterator) ////
    ///////////////////////////////////////
    /**
     * Scans the 'default' family passing each result to the provided handler.
     *
     * @param handler  a handler to process iterators from rocksdb
     * @throws RocksDBException thrown when there are communication errors with RocksDb
     */
    void scan(RocksIteratorHandler handler) throws RocksDBException;
    /**
     * Scans the given family passing the result to the provided handler.
     *
     * @param familyName the column family to scan over
     * @param handler  a handler to process iterators from rocksdb
     * @throws RocksDBException thrown when there are communication errors with RocksDb
     */
    void scan(String familyName, RocksIteratorHandler handler) throws RocksDBException;

    /**
     * Scans the given family passes the result to the handler.
     *
     * @param familyName the column family to scan over
     * @param rOptions readOptions
     * @param handler a handler to process iterators from rocksdb
     * @throws RocksDBException thrown when there are communication errors with RocksDb
     */
    void scan(String familyName, ReadOptions rOptions, RocksIteratorHandler handler) throws RocksDBException;

}
