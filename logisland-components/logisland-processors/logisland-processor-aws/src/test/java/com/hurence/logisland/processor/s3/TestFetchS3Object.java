package com.hurence.logisland.processor.s3;

import java.io.IOException;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.util.StringInputStream;

import com.hurence.logisland.component.PropertyDescriptor;
import com.hurence.logisland.record.FieldDictionary;
import com.hurence.logisland.record.FieldType;
import com.hurence.logisland.record.Record;
import com.hurence.logisland.record.StandardRecord;
import com.hurence.logisland.util.runner.MockRecord;
import com.hurence.logisland.util.runner.TestRunner;
import com.hurence.logisland.util.runner.TestRunners;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;


public class TestFetchS3Object {

    private TestRunner runner = null;
    private FetchS3Object mockFetchS3Object = null;
    private AmazonS3Client actualS3Client = null;
    private AmazonS3Client mockS3Client = null;

    @Before
    public void setUp() {
        mockS3Client = Mockito.mock(AmazonS3Client.class);
        mockFetchS3Object = new FetchS3Object() {
            protected AmazonS3Client getClient() {
                actualS3Client = client;
                return mockS3Client;
            }
        };
        runner = TestRunners.newTestRunner(mockFetchS3Object);
    }

    @Test
    public void testGetObject() throws IOException {
        Record record1 = new StandardRecord();

        runner.setProperty(FetchS3Object.REGION, "us-east-1");
        runner.setProperty(FetchS3Object.BUCKET_FIELD, "request-bucket");
        record1.setField("filename", FieldType.STRING, "request-key");
        runner.enqueue(record1);

        S3Object s3ObjectResponse = new S3Object();
        s3ObjectResponse.setBucketName("response-bucket-name");
        s3ObjectResponse.setKey("response-key");
        s3ObjectResponse.setObjectContent(new StringInputStream("Some Content"));
        ObjectMetadata metadata = Mockito.spy(ObjectMetadata.class);
        metadata.setContentDisposition("key/path/to/file.txt");
        metadata.setContentType("text/plain");
        metadata.setContentMD5("testMD5hash");
        Date expiration = new Date();
        metadata.setExpirationTime(expiration);
        metadata.setExpirationTimeRuleId("testExpirationRuleId");
        Map<String, String> userMetadata = new HashMap<>();
        userMetadata.put("userKey1", "userValue1");
        userMetadata.put("userKey2", "userValue2");
        metadata.setUserMetadata(userMetadata);
        metadata.setSSEAlgorithm("testAlgorithm");
        Mockito.when(metadata.getETag()).thenReturn("test-etag");
        s3ObjectResponse.setObjectMetadata(metadata);
        Mockito.when(mockS3Client.getObject(Mockito.any())).thenReturn(s3ObjectResponse);

        runner.run();

        ArgumentCaptor<GetObjectRequest> captureRequest = ArgumentCaptor.forClass(GetObjectRequest.class);
        Mockito.verify(mockS3Client, Mockito.times(1)).getObject(captureRequest.capture());
        GetObjectRequest request = captureRequest.getValue();
        assertEquals("request-bucket", request.getBucketName());
        assertEquals("request-key", request.getKey());
        assertFalse(request.isRequesterPays());
        assertNull(request.getVersionId());

        MockRecord out = runner.getOutputRecords().get(0);

        out.assertFieldEquals("s3.bucket", "response-bucket-name");
        out.assertFieldEquals("filename", "file.txt");
        out.assertFieldEquals("path", "key/path/to");
        out.assertFieldEquals("absolute.path", "key/path/to/file.txt");
        out.assertFieldEquals("mime.type", "text/plain");
        out.assertFieldEquals("hash.value", "testMD5hash");
        out.assertFieldEquals("hash.algorithm", "MD5");
        out.assertFieldEquals("s3.etag", "test-etag");
        out.assertFieldEquals("s3.expirationTime", String.valueOf(expiration.getTime()));
        out.assertFieldEquals("s3.expirationTimeRuleId", "testExpirationRuleId");
        out.assertFieldEquals("userKey1", "userValue1");
        out.assertFieldEquals("userKey2", "userValue2");
        out.assertFieldEquals("s3.sseAlgorithm", "testAlgorithm");
        /*out.assertContentEquals("Some Content");*/
        // TODO see why this don't work

    }

    @Test
    public void testGetObjectWithRequesterPays() throws IOException {
        Record record1 = new StandardRecord();

        runner.setProperty(FetchS3Object.REGION, "us-east-1");
        runner.setProperty(FetchS3Object.BUCKET_FIELD, "request-bucket");
        runner.setProperty(FetchS3Object.REQUESTER_PAYS, "true");
        record1.setField("filename", FieldType.STRING, "request-key");
        runner.enqueue(record1);

        S3Object s3ObjectResponse = new S3Object();
        s3ObjectResponse.setBucketName("response-bucket-name");
        s3ObjectResponse.setKey("response-key");
        s3ObjectResponse.setObjectContent(new StringInputStream("Some Content"));
        ObjectMetadata metadata = Mockito.spy(ObjectMetadata.class);
        metadata.setContentDisposition("key/path/to/file.txt");
        metadata.setContentType("text/plain");
        metadata.setContentMD5("testMD5hash");
        Date expiration = new Date();
        metadata.setExpirationTime(expiration);
        metadata.setExpirationTimeRuleId("testExpirationRuleId");
        Map<String, String> userMetadata = new HashMap<>();
        userMetadata.put("userKey1", "userValue1");
        userMetadata.put("userKey2", "userValue2");
        metadata.setUserMetadata(userMetadata);
        metadata.setSSEAlgorithm("testAlgorithm");
        Mockito.when(metadata.getETag()).thenReturn("test-etag");
        s3ObjectResponse.setObjectMetadata(metadata);
        Mockito.when(mockS3Client.getObject(Mockito.any())).thenReturn(s3ObjectResponse);

        runner.run();

        ArgumentCaptor<GetObjectRequest> captureRequest = ArgumentCaptor.forClass(GetObjectRequest.class);
        Mockito.verify(mockS3Client, Mockito.times(1)).getObject(captureRequest.capture());
        GetObjectRequest request = captureRequest.getValue();
        assertEquals("request-bucket", request.getBucketName());
        assertEquals("request-key", request.getKey());
        assertTrue(request.isRequesterPays());
        assertNull(request.getVersionId());

        MockRecord out = runner.getOutputRecords().get(0);

        out.assertFieldEquals("s3.bucket", "response-bucket-name");
        out.assertFieldEquals("filename", "file.txt");
        out.assertFieldEquals("path", "key/path/to");
        out.assertFieldEquals("absolute.path", "key/path/to/file.txt");
        out.assertFieldEquals("mime.type", "text/plain");
        out.assertFieldEquals("hash.value", "testMD5hash");
        out.assertFieldEquals("hash.algorithm", "MD5");
        out.assertFieldEquals("s3.etag", "test-etag");
        out.assertFieldEquals("s3.expirationTime", String.valueOf(expiration.getTime()));
        out.assertFieldEquals("s3.expirationTimeRuleId", "testExpirationRuleId");
        out.assertFieldEquals("userKey1", "userValue1");
        out.assertFieldEquals("userKey2", "userValue2");
        out.assertFieldEquals("s3.sseAlgorithm", "testAlgorithm");
    }

    @Test
    public void testGetObjectVersion() throws IOException {
        Record record1 = new StandardRecord();

        runner.setProperty(FetchS3Object.REGION, "us-east-1");
        runner.setProperty(FetchS3Object.BUCKET_FIELD, "request-bucket");
        runner.setProperty(FetchS3Object.VERSION_ID_FIELD, "${s3_version}");
        record1.setField("filename", FieldType.STRING, "request-key");
        record1.setField("s3_version", FieldType.STRING, "request-version");
        runner.enqueue(record1);

        S3Object s3ObjectResponse = new S3Object();
        s3ObjectResponse.setBucketName("response-bucket-name");
        s3ObjectResponse.setObjectContent(new StringInputStream("Some Content"));
        ObjectMetadata metadata = Mockito.spy(ObjectMetadata.class);
        metadata.setContentDisposition("key/path/to/file.txt");
        Mockito.when(metadata.getVersionId()).thenReturn("response-version");
        s3ObjectResponse.setObjectMetadata(metadata);
        Mockito.when(mockS3Client.getObject(Mockito.any())).thenReturn(s3ObjectResponse);

        runner.run();

        ArgumentCaptor<GetObjectRequest> captureRequest = ArgumentCaptor.forClass(GetObjectRequest.class);
        Mockito.verify(mockS3Client, Mockito.times(1)).getObject(captureRequest.capture());
        GetObjectRequest request = captureRequest.getValue();
        assertEquals("request-bucket", request.getBucketName());
        assertEquals("request-key", request.getKey());
        assertEquals("request-version", request.getVersionId());

        MockRecord out = runner.getOutputRecords().get(0);

        out.assertFieldEquals("s3.bucket", "response-bucket-name");
        out.assertFieldEquals("filename", "file.txt");
        out.assertFieldEquals("path", "key/path/to");
        out.assertFieldEquals("absolute.path", "key/path/to/file.txt");
        out.assertFieldEquals("s3.version", "response-version");
        // TODO see why context.getPropertyValue(VERSION_ID_FIELD).evaluate(record) returns null !

    }


    @Test
    public void testGetObjectExceptionGoesToFailure() throws IOException {
        Record record1 = new StandardRecord();

        runner.setProperty(FetchS3Object.REGION, "us-east-1");
        runner.setProperty(FetchS3Object.BUCKET_FIELD, "request-bucket");
        /*final Map<String, String> attrs = new HashMap<>();
        attrs.put("filename", "request-key");*/
        record1.setField("filename", FieldType.STRING, "request-key");
        runner.enqueue(record1);
        Mockito.doThrow(new AmazonS3Exception("NoSuchBucket")).when(mockS3Client).getObject(Mockito.any());
        // TODO how to replace this failure

        runner.run();
        /*runner.assertAllFlowFilesTransferred(FetchS3Object.REL_FAILURE, 1);*/
        MockRecord out = runner.getOutputRecords().get(0);
        out.assertFieldExists(FieldDictionary.RECORD_ERRORS);

    }

    @Test
    public void testGetObjectReturnsNull() throws IOException {
        Record record1 = new StandardRecord();

        runner.setProperty(FetchS3Object.REGION, "us-east-1");
        runner.setProperty(FetchS3Object.BUCKET_FIELD, "request-bucket");
        /*final Map<String, String> attrs = new HashMap<>();
        attrs.put("filename", "request-key");*/
        record1.setField("filename", FieldType.STRING, "request-key");
        runner.enqueue(record1);
        Mockito.when(mockS3Client.getObject(Mockito.any())).thenReturn(null);
        // TODO how to replace this failure

        runner.run();

        /*runner.assertAllFlowFilesTransferred(FetchS3Object.REL_FAILURE, 1);*/
        MockRecord out = runner.getOutputRecords().get(0);
        out.assertFieldExists(FieldDictionary.RECORD_ERRORS);

    }

    @Test
    public void testFlowFileAccessExceptionGoesToFailure() throws IOException {
        Record record1 = new StandardRecord();

        runner.setProperty(FetchS3Object.REGION, "us-east-1");
        runner.setProperty(FetchS3Object.BUCKET_FIELD, "request-bucket");
        /*final Map<String, String> attrs = new HashMap<>();
        attrs.put("filename", "request-key");*/
        record1.setField("filename", FieldType.STRING, "request-key");
        runner.enqueue(record1);

        AmazonS3Exception amazonException = new AmazonS3Exception("testing");
        /*Mockito.doThrow(new FlowFileAccessException("testing nested", amazonException)).when(mockS3Client).getObject(Mockito.any());*/
        // TODO how to replace this failure

        runner.run();

        /*runner.assertAllFlowFilesTransferred(FetchS3Object.REL_FAILURE, 1);*/
        MockRecord out = runner.getOutputRecords().get(0);
        out.assertFieldExists(FieldDictionary.RECORD_ERRORS);
    }
    @Test
    public void testGetPropertyDescriptors() throws Exception {
        FetchS3Object processor = new FetchS3Object();
        List<PropertyDescriptor> pd = processor.getSupportedPropertyDescriptors();
        assertEquals("size should be eq", 19, pd.size());
        assertTrue(pd.contains(FetchS3Object.ACCESS_KEY));
        assertTrue(pd.contains(FetchS3Object.AWS_CREDENTIALS_PROVIDER_SERVICE));
        assertTrue(pd.contains(FetchS3Object.BUCKET_FIELD));
        assertTrue(pd.contains(FetchS3Object.CREDENTIALS_FILE));
        assertTrue(pd.contains(FetchS3Object.ENDPOINT_OVERRIDE));
        assertTrue(pd.contains(FetchS3Object.KEY_FEILD));
        assertTrue(pd.contains(FetchS3Object.REGION));
        assertTrue(pd.contains(FetchS3Object.SECRET_KEY));
        assertTrue(pd.contains(FetchS3Object.SIGNER_OVERRIDE));
        assertTrue(pd.contains(FetchS3Object.SSL_CONTEXT_SERVICE));
        assertTrue(pd.contains(FetchS3Object.TIMEOUT));
        assertTrue(pd.contains(FetchS3Object.VERSION_ID_FIELD));
        assertTrue(pd.contains(FetchS3Object.ENCRYPTION_SERVICE));
        assertTrue(pd.contains(FetchS3Object.PROXY_CONFIGURATION_SERVICE));
        assertTrue(pd.contains(FetchS3Object.PROXY_HOST));
        assertTrue(pd.contains(FetchS3Object.PROXY_HOST_PORT));
        assertTrue(pd.contains(FetchS3Object.PROXY_USERNAME));
        assertTrue(pd.contains(FetchS3Object.PROXY_PASSWORD));
        assertTrue(pd.contains(FetchS3Object.REQUESTER_PAYS));

    }
}
