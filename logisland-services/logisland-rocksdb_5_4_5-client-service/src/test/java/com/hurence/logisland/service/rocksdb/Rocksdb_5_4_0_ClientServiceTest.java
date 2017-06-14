package com.hurence.logisland.service.rocksdb;

import com.hurence.logisland.component.InitializationException;
import com.hurence.logisland.controller.ControllerServiceInitializationContext;
import com.hurence.logisland.processor.ProcessException;
import com.hurence.logisland.service.rocksdb.delete.DeleteRequest;
import com.hurence.logisland.service.rocksdb.delete.DeleteResponse;
import com.hurence.logisland.service.rocksdb.get.GetRequest;
import com.hurence.logisland.service.rocksdb.get.GetResponse;
import com.hurence.logisland.service.rocksdb.put.ValuePutRequest;
import com.hurence.logisland.service.rocksdb.scan.RocksIteratorHandler;
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
import static org.junit.Assert.fail;


/**
 * Created by gregoire on 02/06/17.
 */
public class Rocksdb_5_4_0_ClientServiceTest {

    static {
        RocksDB.loadLibrary();
    }

    Logger log = LoggerFactory.getLogger(Rocksdb_5_4_0_ClientServiceTest.class);
    static protected final String defaultFamily = "default";
    static protected final WriteOptions writeOptions = new WriteOptions();
    static protected final ReadOptions readOptions = new ReadOptions();
    static protected final byte[] key1 = "key1".getBytes();
    static protected final byte[] value1 = "value1".getBytes();
    static protected final byte[] key2 = "key2".getBytes();
    static protected final byte[] value2 = "value2".getBytes();
    static protected final byte[] key3 = "key3".getBytes();
    static protected final byte[] value3 = "value3".getBytes();
    static protected final byte[] key4 = "key4".getBytes();
    static protected final byte[] value4 = "value4".getBytes();
    static protected final byte[] key5 = "key5".getBytes();
    static protected final byte[] value5 = "value5".getBytes();
    static protected final byte[] nullKey = null;
    static protected final byte[] nullValue = null;
    static protected final GetRequest getRequestNull = null;
    static protected final DeleteRequest deleteRequestNull = null;
    static protected final ValuePutRequest valuePutRequestNull = null;
    @Rule
    public TemporaryFolder folder = new TemporaryFolder();


    @Test
    public void testSimpleGetApiAndPutApiInDifferentFamily() throws RocksDBException, InitializationException, IOException {

        final String dbName = "myDb";
        final String familyName1 = "fruits";
        final String familyName2 = "legumes";
        final String dbPath = folder.newFolder(dbName).getAbsolutePath();
        final TestRunner runner = TestRunners.newTestRunner(TestProcessor.class);

        // no db path so should be invalid
        Rocksdb_5_4_0_ClientService service = new Rocksdb_5_4_0_ClientService();
        service.setIdentifier("rocksDbClientService");
        runner.addControllerService(service);
        runner.setProperty(service, Rocksdb_5_4_0_ClientService.ROCKSDB_PATH, dbPath);
        runner.setProperty(service, Rocksdb_5_4_0_ClientService.CREATE_IF_MISSING, "true");
        runner.setProperty(service, Rocksdb_5_4_0_ClientService.FAMILY_NAMES, defaultFamily + "," +
                familyName1 +"," + familyName2);
        runner.setProperty(service, Rocksdb_5_4_0_ClientService.CREATE_MISSING_COLUMN_FAMILIES, "true");

        runner.assertValid(service);
        // should be valid
        runner.enableControllerService(service);

        //round1
        assertThat( service.get(key1), nullValue());
        service.put(key1, value1);
        service.put(familyName1, key1, value2);
        service.put(familyName2, key1, value3);
        service.put(familyName2, key2, value2, writeOptions);
        assertThat( service.get(key1), is(value1));
        assertThat( service.get(defaultFamily, key1), is(value1));
        assertThat( service.get(familyName1, key1), is(value2));
        assertThat( service.get(familyName2, key1), is(value3));
        assertThat( service.get(familyName2, key2, readOptions), is(value2));
        //round2
        service.put(key1, value3);
        service.put(familyName1, key1, value1);
        service.put(familyName2, key1, value2);
        service.put(familyName2, key3, value3, writeOptions);
        assertThat( service.get(key1), is(value3));
        assertThat( service.get(defaultFamily, key1), is(value3));
        assertThat( service.get(familyName1, key1), is(value1));
        assertThat( service.get(familyName2, key1), is(value2));
        assertThat( service.get(familyName2, key3, readOptions), is(value3));
    }
    @Test
    public void testPutApiWithPutRequest() throws RocksDBException, InitializationException, IOException {

        final String dbName = "myDb";
        final String dbPath = folder.newFolder(dbName).getAbsolutePath();
        final TestRunner runner = TestRunners.newTestRunner(TestProcessor.class);

        // no db path so should be invalid
        Rocksdb_5_4_0_ClientService service = new Rocksdb_5_4_0_ClientService();
        service.setIdentifier("rocksDbClientService");
        runner.addControllerService(service);
        runner.setProperty(service, Rocksdb_5_4_0_ClientService.ROCKSDB_PATH, dbPath);
        runner.setProperty(service, Rocksdb_5_4_0_ClientService.CREATE_IF_MISSING, "true");
        runner.setProperty(service, Rocksdb_5_4_0_ClientService.CREATE_MISSING_COLUMN_FAMILIES, "true");

        runner.assertValid(service);
        // should be valid
        runner.enableControllerService(service);

        //round3 put with PutRequest
        final ValuePutRequest putR = new ValuePutRequest();
        service.put(key1, value3, writeOptions);
        assertThat( service.get(key1), is(value3));
        try {
            service.put(putR);
            fail("expected IllegalArgumentException was not thrown");
        } catch (IllegalArgumentException e) {}
        putR.setKey(key2);
        try {
            service.put(putR);
            fail("expected IllegalArgumentException was not thrown");
        } catch (IllegalArgumentException e) {}
        assertThat( service.get(key2), nullValue());
        putR.setValue(value3);
        service.put(putR);
        assertThat( service.get(key2), is(value3));
        putR.setFamily(defaultFamily);
        service.put(putR);
        assertThat( service.get(defaultFamily, key2), is(value3));
        WriteOptions wr = new WriteOptions();
        wr.ignoreMissingColumnFamilies();
        putR.setwOptions(wr);
        putR.setFamily("fefe");
        try {
            service.put(putR);
            fail("expected IllegalArgumentException was not thrown");
        } catch (IllegalArgumentException e) {}
        putR.setFamily(defaultFamily);
        putR.setwOptions(null);
        service.put(putR);
    }

    @Test
    public void testGetApiWithGetRequest() throws RocksDBException, InitializationException, IOException {

        final String dbName = "myDb";
        final String dbPath = folder.newFolder(dbName).getAbsolutePath();
        final TestRunner runner = TestRunners.newTestRunner(TestProcessor.class);

        // no db path so should be invalid
        Rocksdb_5_4_0_ClientService service = new Rocksdb_5_4_0_ClientService();
        service.setIdentifier("rocksDbClientService");
        runner.addControllerService(service);
        runner.setProperty(service, Rocksdb_5_4_0_ClientService.ROCKSDB_PATH, dbPath);
        runner.setProperty(service, Rocksdb_5_4_0_ClientService.CREATE_IF_MISSING, "true");
        runner.setProperty(service, Rocksdb_5_4_0_ClientService.CREATE_MISSING_COLUMN_FAMILIES, "true");

        runner.assertValid(service);
        // should be valid
        runner.enableControllerService(service);

        service.put(key1, value3);
        service.put(key2, value2);
        service.put(key3, value3);
        final GetRequest getReq = new GetRequest();
        try {
            service.get(getReq);
            fail("expected IllegalArgumentException was not thrown");
        } catch (IllegalArgumentException e) {}
        getReq.setKey(key2);

        final GetResponse getRsp = new GetResponse();
        getRsp.setKey(getReq.getKey());
        getRsp.setFamily(defaultFamily);
        getRsp.setValue(value2);
        assertThat(service.get(getReq), is(getRsp));
        getReq.setKey(key3);
        getRsp.setKey(key3);
        getRsp.setValue(value3);
        assertThat(service.get(getReq), is(getRsp));
        getReq.setFamily(defaultFamily);
        assertThat(service.get(getReq), is(getRsp));
        getReq.setReadOption(readOptions);
        assertThat(service.get(getReq), is(getRsp));
    }

    @Test
    public void testDeleteApi() throws RocksDBException, InitializationException, IOException {

        final String dbName = "myDb";
        final String dbPath = folder.newFolder(dbName).getAbsolutePath();
        final TestRunner runner = TestRunners.newTestRunner(TestProcessor.class);

        // no db path so should be invalid
        Rocksdb_5_4_0_ClientService service = new Rocksdb_5_4_0_ClientService();
        service.setIdentifier("rocksDbClientService");
        runner.addControllerService(service);
        runner.setProperty(service, Rocksdb_5_4_0_ClientService.ROCKSDB_PATH, dbPath);
        runner.setProperty(service, Rocksdb_5_4_0_ClientService.CREATE_IF_MISSING, "true");
        runner.setProperty(service, Rocksdb_5_4_0_ClientService.CREATE_MISSING_COLUMN_FAMILIES, "true");

        runner.assertValid(service);
        // should be valid
        runner.enableControllerService(service);

        service.put(key1, value3);
        service.put(key2, value2);
        service.put(key3, value3);
        service.delete(key1);
        assertThat(service.get(key1), nullValue());
        service.delete(key2, writeOptions);
        assertThat(service.get(key2), nullValue());
        service.delete(defaultFamily, key3, writeOptions);
        assertThat(service.get(key3), nullValue());
        service.put(key1, value3);
        service.delete(defaultFamily, key1);
        assertThat(service.get(key1), nullValue());

        service.put(key1, value3);
        service.put(key2, value2);
        service.put(key3, value3);

        final DeleteRequest dReq = new DeleteRequest();
        try {
            service.delete(dReq);
            fail("expected IllegalArgumentException was not thrown");
        } catch (IllegalArgumentException e) {}
        dReq.setKey(key2);
        service.delete(dReq);
        assertThat(service.get(key2), nullValue());
        service.put(key2, value2);

        dReq.setFamily(defaultFamily);
        service.delete(dReq);
        assertThat(service.get(key2), nullValue());
        service.put(key2, value2);

        dReq.setWriteOptions(writeOptions);
        service.delete(dReq);
        assertThat(service.get(key2), nullValue());
        service.put(key2, value2);

    }

    @Test
    public void testMultiPutGetDelete() throws RocksDBException, InitializationException, IOException {

        final String dbName = "myDb";
        final String familyName1 = "fruits";
        final String familyName2 = "legumes";
        final String dbPath = folder.newFolder(dbName).getAbsolutePath();
        final TestRunner runner = TestRunners.newTestRunner(TestProcessor.class);

        // no db path so should be invalid
        Rocksdb_5_4_0_ClientService service = new Rocksdb_5_4_0_ClientService();
        service.setIdentifier("rocksDbClientService");
        runner.addControllerService(service);
        runner.setProperty(service, Rocksdb_5_4_0_ClientService.ROCKSDB_PATH, dbPath);
        runner.setProperty(service, Rocksdb_5_4_0_ClientService.CREATE_IF_MISSING, "true");
        runner.setProperty(service, Rocksdb_5_4_0_ClientService.FAMILY_NAMES, defaultFamily + "," +
                familyName1 +"," + familyName2);
        runner.setProperty(service, Rocksdb_5_4_0_ClientService.CREATE_MISSING_COLUMN_FAMILIES, "true");

        runner.assertValid(service);
        // should be valid
        runner.enableControllerService(service);

        final ValuePutRequest putR1 = new ValuePutRequest();
        putR1.setKey(key1);
        putR1.setValue(value1);
        final ValuePutRequest putR2 = new ValuePutRequest();
        putR2.setKey(key2);
        putR2.setValue(value2);
        final ValuePutRequest putR3 = new ValuePutRequest();
        putR3.setKey(key3);
        putR3.setValue(value3);
        final ValuePutRequest putR4 = new ValuePutRequest();
        putR4.setKey(key4);
        putR4.setValue(value4);
        final ValuePutRequest putR5 = new ValuePutRequest();
        putR5.setKey(key5);
        putR5.setValue(value5);
        final Collection<ValuePutRequest> putRequests = new ArrayList<>();
        putRequests.add(putR1);
        putRequests.add(putR2);
        putRequests.add(putR3);
        putRequests.add(putR4);
        putRequests.add(putR5);

        final GetRequest getR1 = new GetRequest();
        getR1.setKey(key1);
        final GetRequest getR2 = new GetRequest();
        getR2.setKey(key2);
        final GetRequest getR3 = new GetRequest();
        getR3.setKey(key3);
        final GetRequest getR4 = new GetRequest();
        getR4.setKey(key4);
        final GetRequest getR5 = new GetRequest();
        getR5.setKey(key5);
        final Collection<GetRequest> getRequests = new ArrayList<>();
        getRequests.add(getR1);
        getRequests.add(getR2);
        getRequests.add(getR3);
        getRequests.add(getR4);
        getRequests.add(getR5);

        final DeleteRequest delReq1 = new DeleteRequest();
        delReq1.setKey(key1);
        final DeleteRequest delReq3 = new DeleteRequest();
        delReq3.setKey(key3);
        final DeleteRequest delReq5 = new DeleteRequest();
        delReq5.setKey(key5);
        final Collection<DeleteRequest> delRequests = new ArrayList<>();
        delRequests.add(delReq1);
        delRequests.add(delReq3);
        delRequests.add(delReq5);

        final GetResponse getRes1 = new GetResponse();
        getRes1.setKey(key1);
        getRes1.setFamily(defaultFamily);
        getRes1.setValue(value1);
        final GetResponse getRes1Bis = new GetResponse();
        getRes1Bis.setKey(key1);
        getRes1Bis.setFamily(defaultFamily);
        getRes1Bis.setValue(null);
        final GetResponse getRes2 = new GetResponse();
        getRes2.setKey(key2);
        getRes2.setFamily(defaultFamily);
        getRes2.setValue(value2);
        final GetResponse getRes3 = new GetResponse();
        getRes3.setKey(key3);
        getRes3.setFamily(defaultFamily);
        getRes3.setValue(value3);
        final GetResponse getRes3Bis = new GetResponse();
        getRes3Bis.setKey(key3);
        getRes3Bis.setFamily(defaultFamily);
        getRes3Bis.setValue(null);
        final GetResponse getRes4 = new GetResponse();
        getRes4.setKey(key4);
        getRes4.setFamily(defaultFamily);
        getRes4.setValue(value4);
        final GetResponse getRes5 = new GetResponse();
        getRes5.setKey(key5);
        getRes5.setFamily(defaultFamily);
        getRes5.setValue(value5);
        final GetResponse getRes5Bis = new GetResponse();
        getRes5Bis.setKey(key5);
        getRes5Bis.setFamily(defaultFamily);
        getRes5Bis.setValue(null);
        final Collection<GetResponse> getResponsesExpected = new ArrayList<>();
        getResponsesExpected.add(getRes1);
        getResponsesExpected.add(getRes2);
        getResponsesExpected.add(getRes3);
        getResponsesExpected.add(getRes4);
        getResponsesExpected.add(getRes5);
        final Collection<GetResponse> getResponsesExpected2 = new ArrayList<>();
        getResponsesExpected2.add(getRes1Bis);
        getResponsesExpected2.add(getRes2);
        getResponsesExpected2.add(getRes3Bis);
        getResponsesExpected2.add(getRes4);
        getResponsesExpected2.add(getRes5Bis);
        final DeleteResponse delRes1 = new DeleteResponse();
        delRes1.setKey(key1);
        delRes1.setFamily(defaultFamily);
        final DeleteResponse delRes3 = new DeleteResponse();
        delRes3.setKey(key3);
        delRes3.setFamily(defaultFamily);
        final DeleteResponse delRes5 = new DeleteResponse();
        delRes5.setKey(key5);
        delRes5.setFamily(defaultFamily);
        final Collection<DeleteResponse> deleteResponsesExpected = new ArrayList<>();
        deleteResponsesExpected.add(delRes1);
        deleteResponsesExpected.add(delRes3);
        deleteResponsesExpected.add(delRes5);

        //round1
        assertThat( service.get(key1), nullValue());
        assertThat( service.get(key2), nullValue());
        assertThat( service.get(key3), nullValue());
        assertThat( service.get(key4), nullValue());
        assertThat( service.get(key5), nullValue());

        service.multiPut(putRequests);

        assertThat( service.get(key1), is(value1));
        assertThat( service.get(key2), is(value2));
        assertThat( service.get(key3), is(value3));
        assertThat( service.get(key4), is(value4));
        assertThat( service.get(key5), is(value5));

        Collection<GetResponse> getResponses = service.multiGet(getRequests);

        assertThat( getResponsesExpected, is(getResponses));

        Collection<DeleteResponse> deleteResponses = service.multiDelete(delRequests);

        assertThat( deleteResponsesExpected, is(deleteResponses));

        assertThat( service.get(key1), nullValue());
        assertThat( service.get(key2), is(value2));
        assertThat( service.get(key3), nullValue());
        assertThat( service.get(key4), is(value4));
        assertThat( service.get(key5), nullValue());

        getResponses = service.multiGet(getRequests);

        assertThat( getResponsesExpected2, is(getResponses));
    }

    @Test
    public void testDeleteRangeApi() throws RocksDBException, InitializationException, IOException {

        final String dbName = "myDb";
        final String familyName1 = "fruits";
        final String familyName2 = "legumes";
        final String dbPath = folder.newFolder(dbName).getAbsolutePath();
        final TestRunner runner = TestRunners.newTestRunner(TestProcessor.class);

        // no db path so should be invalid
        Rocksdb_5_4_0_ClientService service = new Rocksdb_5_4_0_ClientService();
        service.setIdentifier("rocksDbClientService");
        runner.addControllerService(service);
        runner.setProperty(service, Rocksdb_5_4_0_ClientService.ROCKSDB_PATH, dbPath);
        runner.setProperty(service, Rocksdb_5_4_0_ClientService.CREATE_IF_MISSING, "true");
        runner.setProperty(service, Rocksdb_5_4_0_ClientService.FAMILY_NAMES, defaultFamily + "," +
                familyName1 +"," + familyName2);
        runner.setProperty(service, Rocksdb_5_4_0_ClientService.CREATE_MISSING_COLUMN_FAMILIES, "true");

        runner.assertValid(service);
        // should be valid
        runner.enableControllerService(service);

        //round1
        service.put(key1, value1);
        service.put(key2, value2);
        service.put(key3, value3);
        service.put(key4, value4);
        service.put(key5, value5);
        service.deleteRange(key1, key4);
        assertThat( service.get(key1), nullValue());
        assertThat( service.get(key2), nullValue());
        assertThat( service.get(key3), nullValue());
        assertThat( service.get(key4), is(value4));
        assertThat( service.get(key5), is(value5));
        //round2
        service.put(key1, value1);
        service.put(key2, value2);
        service.put(key3, value3);
        service.put(key4, value4);
        service.put(key5, value5);
        service.deleteRange(key1, key4, writeOptions);
        assertThat( service.get(key1), nullValue());
        assertThat( service.get(key2), nullValue());
        assertThat( service.get(key3), nullValue());
        assertThat( service.get(key4), is(value4));
        assertThat( service.get(key5), is(value5));
        //round3
        service.put(key1, value1);
        service.put(key2, value2);
        service.put(key3, value3);
        service.put(key4, value4);
        service.put(key5, value5);
        service.deleteRange(defaultFamily, key1, key4);
        assertThat( service.get(key1), nullValue());
        assertThat( service.get(key2), nullValue());
        assertThat( service.get(key3), nullValue());
        assertThat( service.get(key4), is(value4));
        assertThat( service.get(key5), is(value5));
        //round4
        service.put(key1, value1);
        service.put(key2, value2);
        service.put(key3, value3);
        service.put(key4, value4);
        service.put(key5, value5);
        service.deleteRange(defaultFamily, key1, key4, writeOptions);
        assertThat( service.get(key1), nullValue());
        assertThat( service.get(key2), nullValue());
        assertThat( service.get(key3), nullValue());
        assertThat( service.get(key4), is(value4));
        assertThat( service.get(key5), is(value5));
    }


    @Test
    public void testScanApi() throws RocksDBException, InitializationException, IOException {

        final String dbName = "myDb";
        final String familyName1 = "fruits";
        final String familyName2 = "legumes";
        final String dbPath = folder.newFolder(dbName).getAbsolutePath();
        final TestRunner runner = TestRunners.newTestRunner(TestProcessor.class);

        // no db path so should be invalid
        Rocksdb_5_4_0_ClientService service = new Rocksdb_5_4_0_ClientService();
        service.setIdentifier("rocksDbClientService");
        runner.addControllerService(service);
        runner.setProperty(service, Rocksdb_5_4_0_ClientService.ROCKSDB_PATH, dbPath);
        runner.setProperty(service, Rocksdb_5_4_0_ClientService.CREATE_IF_MISSING, "true");
        runner.setProperty(service, Rocksdb_5_4_0_ClientService.FAMILY_NAMES, defaultFamily + "," +
                familyName1 +"," + familyName2);
        runner.setProperty(service, Rocksdb_5_4_0_ClientService.CREATE_MISSING_COLUMN_FAMILIES, "true");

        runner.assertValid(service);
        // should be valid
        runner.enableControllerService(service);

        RocksIteratorFillerHandler handler = new RocksIteratorFillerHandler();
        //round1
        service.put(key1, value1);
        service.put(key2, value2);
        service.put(key3, value3);
        service.put(key4, value4);
        service.put(key5, value5);
        service.scan(handler);
        assertThat(handler.getValues() , hasItems(value1, value2, value3, value4, value5));
        assertThat(handler.getValues() , hasSize(5));
        handler = new RocksIteratorFillerHandler();
        service.scan(defaultFamily, handler);
        assertThat(handler.getValues() , hasItems(value1, value2, value3, value4, value5));
        assertThat(handler.getValues() , hasSize(5));
        handler = new RocksIteratorFillerHandler();
        service.scan(defaultFamily, readOptions, handler);
        assertThat(handler.getValues() , hasItems(value1, value2, value3, value4, value5));
        assertThat(handler.getValues() , hasSize(5));
    }

    @Test
    public void testApiWithNull() throws RocksDBException, InitializationException, IOException {

        final String dbName = "myDb";
        final String familyName1 = "fruits";
        final String familyName2 = "legumes";
        final String dbPath = folder.newFolder(dbName).getAbsolutePath();
        final TestRunner runner = TestRunners.newTestRunner(TestProcessor.class);

        // no db path so should be invalid
        Rocksdb_5_4_0_ClientService service = new Rocksdb_5_4_0_ClientService();
        service.setIdentifier("rocksDbClientService");
        runner.addControllerService(service);
        runner.setProperty(service, Rocksdb_5_4_0_ClientService.ROCKSDB_PATH, dbPath);
        runner.setProperty(service, Rocksdb_5_4_0_ClientService.CREATE_IF_MISSING, "true");
        runner.setProperty(service, Rocksdb_5_4_0_ClientService.FAMILY_NAMES, defaultFamily + "," +
                familyName1 +"," + familyName2);
        runner.setProperty(service, Rocksdb_5_4_0_ClientService.CREATE_MISSING_COLUMN_FAMILIES, "true");

        runner.assertValid(service);
        // should be valid
        runner.enableControllerService(service);

        RocksIteratorFillerHandler handler = new RocksIteratorFillerHandler();
        //put
        try {
            service.put(valuePutRequestNull);
            fail("expected Npe did not occured");
        } catch (NullPointerException ex) {}
        try {
            service.put(null, null);
            fail("expected Npe did not occured");
        } catch (NullPointerException ex) {}
        try {
            service.put(key2, null);
            fail("expected Npe did not occured");
        } catch (NullPointerException ex) {}
        try {
            service.put(null, value3);
            fail("expected Npe did not occured");
        } catch (NullPointerException ex) {}
        try {
            service.put(null, key3, value3);
            fail("expected IllegalArgumentException did not occured");
        } catch (IllegalArgumentException ex) {}
        try {
            service.put(key3, value3, null);
            fail("expected Npe did not occured");
        } catch (NullPointerException ex) {}
        try {
            service.put(familyName1, key3, value3, null);
            fail("expected Npe did not occured");
        } catch (NullPointerException ex) {}

        //get
        try {
            service.get(getRequestNull);
            fail("expected Npe did not occured");
        } catch (NullPointerException ex) {}
        try {
            service.get(nullKey);
            fail("expected Npe did not occured");
        } catch (NullPointerException ex) {}
        try {
            service.get(nullKey, null);
            fail("expected Npe did not occured");
        } catch (NullPointerException ex) {}
        try {
            service.get(null, nullKey, null);
            fail("expected IllegalArgumentException did not occured");
        } catch (IllegalArgumentException ex) {}
        try {
            service.get(null, nullKey);
            fail("expected IllegalArgumentException did not occured");
        } catch (IllegalArgumentException ex) {}

        //delete
        try {
            service.delete(deleteRequestNull);
            fail("expected Npe did not occured");
        } catch (NullPointerException ex) {}
        try {
            service.delete(nullKey);
            fail("expected Npe did not occured");
        } catch (NullPointerException ex) {}
        try {
            service.delete(nullKey, null);
            fail("expected Npe did not occured");
        } catch (NullPointerException ex) {}
        try {
            service.delete(null, nullKey);
            fail("expected IllegalArgumentException did not occured");
        } catch (IllegalArgumentException ex) {}
        try {
            service.delete(null, nullKey, null);
            fail("expected IllegalArgumentException did not occured");
        } catch (IllegalArgumentException ex) {}

        //delete range
        try {
            service.deleteRange(nullKey, nullKey);
            fail("expected Npe did not occured");
        } catch (NullPointerException ex) {}
        try {
            service.deleteRange(key2, nullKey);
            fail("expected Npe did not occured");
        } catch (NullPointerException ex) {}
        try {
            service.deleteRange(nullKey, key2);
            fail("expected Npe did not occured");
        } catch (NullPointerException ex) {}
        try {
            service.deleteRange(key2, key3, null);
            fail("expected Npe did not occured");
        } catch (NullPointerException ex) {}
        try {
            service.deleteRange(key2, nullKey, null);
            fail("expected Npe did not occured");
        } catch (NullPointerException ex) {}
        try {
            service.deleteRange(null, key3, value3);
            fail("expected IllegalArgumentException did not occured");
        } catch (IllegalArgumentException ex) {}
        try {
            service.deleteRange(null, nullKey, value3, null);
            fail("expected IllegalArgumentException did not occured");
        } catch (IllegalArgumentException ex) {}


        //scan
        try {
            service.scan(null);
            fail("expected Npe did not occured");
        } catch (NullPointerException ex) {}
        try {
            service.scan( null, null);
            fail("expected IllegalArgumentException did not occured");
        } catch (IllegalArgumentException ex) {}
        try {
            service.scan(familyName2, null, null);
            fail("expected Npe did not occured");
        } catch (NullPointerException ex) {}

    }


    private class RocksIteratorFillerHandler implements RocksIteratorHandler {

        final private Collection<byte[]> values = new ArrayList<>();
        @Override
        public void handle(RocksIterator rocksIterator) throws RocksDBException {
            for (rocksIterator.seekToFirst(); rocksIterator.isValid(); rocksIterator.next()) {
                rocksIterator.status();
                assert (rocksIterator.key() != null);
                assert (rocksIterator.value() != null);
                values.add(rocksIterator.value());
            }
        }

        public Collection<byte[]> getValues() {
            return values;
        }
    }
}
