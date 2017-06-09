package com.hurence.logisland.service.rocksdb;

import com.hurence.logisland.component.InitializationException;
import com.hurence.logisland.controller.ControllerServiceInitializationContext;
import com.hurence.logisland.processor.ProcessException;
import com.hurence.logisland.util.runner.TestRunner;
import com.hurence.logisland.util.runner.TestRunners;
import org.hamcrest.collection.IsIterableContainingInAnyOrder;
import org.hamcrest.collection.IsIterableContainingInOrder;
import org.hamcrest.core.Is;
import org.hamcrest.core.IsCollectionContaining;
import org.hamcrest.core.IsEqual;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.rocksdb.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.*;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;


/**
 * Created by gregoire on 02/06/17.
 */
public class Rocksdb_5_4_0_ClientServiceTest {

    Logger log = LoggerFactory.getLogger(Rocksdb_5_4_0_ClientServiceTest.class);
    static protected final byte[] key1 = "key1".getBytes();
    static protected final byte[] value1 = "value1".getBytes();
    static protected final byte[] key2 = "key2".getBytes();
    static protected final byte[] value2 = "value2".getBytes();

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();


    @Test
    public void testServiceValidation() throws RocksDBException, InitializationException, IOException {

        final String dbName = "myDb";
        final String family1Name = "default";
        final String dbPath = folder.newFolder(dbName).getAbsolutePath();

        final String pathDb = "myDb";
        final TestRunner runner = TestRunners.newTestRunner(TestProcessor.class);

        // no db path so should be invalid
        Rocksdb_5_4_0_ClientService service = new Rocksdb_5_4_0_ClientService();
        service.setIdentifier("rocksDbClientService");
        final Map<String, String> props = new HashMap<>();
        runner.addControllerService(service, props);
        runner.assertNotValid(service);
        // should be valid
        runner.setProperty(service, Rocksdb_5_4_0_ClientService.ROCKSDB_PATH, dbPath);
        runner.assertValid(service);

        runner.enableControllerService(service);
    }

    @Test
    public void checkInitializationOfRocksWithAnExistingDb() throws RocksDBException, InitializationException, IOException {

        final String dbName = "myDb";
        final String family1Name = "default";
        final String dbPath = folder.newFolder(dbName).getAbsolutePath();
        final DBOptions dbOption = new DBOptions().setCreateIfMissing(true);
        final ColumnFamilyOptions fOptions = new ColumnFamilyOptions();
        final Options options = new Options(dbOption, fOptions);

        final List<ColumnFamilyDescriptor> familyDescriptors = new ArrayList<>();
        final List<ColumnFamilyHandle> familyHandles = new ArrayList<>();
        familyDescriptors.add(new ColumnFamilyDescriptor(family1Name.getBytes(), fOptions));

        RocksDB db = RocksDB.open(dbOption, dbPath, familyDescriptors, familyHandles);
        assertThat(RocksDB.listColumnFamilies(options, dbPath), hasSize(1));
        assertThat(RocksDB.listColumnFamilies(options, dbPath), hasItem(family1Name.getBytes()));

        db.put(key1, value1);
        Assert.assertArrayEquals(value1, db.get(key1));

    }

}
