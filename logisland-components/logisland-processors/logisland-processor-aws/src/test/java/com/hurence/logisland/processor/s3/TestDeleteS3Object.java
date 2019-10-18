package com.hurence.logisland.processor.s3;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.hurence.logisland.component.PropertyDescriptor;
import com.hurence.logisland.processor.s3.DeleteS3Object;
import com.hurence.logisland.record.FieldType;
import com.hurence.logisland.record.Record;
import com.hurence.logisland.record.StandardRecord;
import com.hurence.logisland.service.proxy.ProxyConfigurationService;
import com.hurence.logisland.util.runner.MockRecord;
import com.hurence.logisland.util.runner.TestRunner;
import com.hurence.logisland.util.runner.TestRunners;

import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.DeleteObjectRequest;
import com.amazonaws.services.s3.model.DeleteVersionRequest;

import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;


public class TestDeleteS3Object {

    private TestRunner runner = null;
    private DeleteS3Object mockDeleteS3Object = null;
    private AmazonS3Client actualS3Client = null;
    private AmazonS3Client mockS3Client = null;

    @Before
    public void setUp() {
        mockS3Client = Mockito.mock(AmazonS3Client.class);
        mockDeleteS3Object = new DeleteS3Object() {
            protected AmazonS3Client getClient() {
                actualS3Client = client;
                return mockS3Client;
            }
        };
        runner = TestRunners.newTestRunner(mockDeleteS3Object);
    }

    @Test
    public void testDeleteObjectSimple() throws IOException {
        Record record1 = new StandardRecord();

        runner.setProperty(DeleteS3Object.REGION, "us-west-2");
        runner.setProperty(DeleteS3Object.BUCKET_FIELD, "test-bucket");
        /*final Map<String, String> attrs = new HashMap<>();
        attrs.put("filename", "delete-key");*/
        record1.setField("filename", FieldType.STRING, "delete-key");
        runner.enqueue(record1);

        runner.run();

        /*runner.assertAllFlowFilesTransferred(DeleteS3Object.REL_SUCCESS, 1);*/
        ArgumentCaptor<DeleteObjectRequest> captureRequest = ArgumentCaptor.forClass(DeleteObjectRequest.class);
        Mockito.verify(mockS3Client, Mockito.times(1)).deleteObject(captureRequest.capture());
        DeleteObjectRequest request = captureRequest.getValue();
        assertEquals("test-bucket", request.getBucketName());
        assertEquals("delete-key", request.getKey());
        Mockito.verify(mockS3Client, Mockito.never()).deleteVersion(Mockito.any(DeleteVersionRequest.class));
    }

    @Test
    public void testDeleteObjectS3Exception() {
        Record record1 = new StandardRecord();

        runner.setProperty(DeleteS3Object.REGION, "us-west-2");
        runner.setProperty(DeleteS3Object.BUCKET_FIELD, "test-bucket");
        /*final Map<String, String> attrs = new HashMap<>();
        attrs.put("filename", "delete-key");*/
        record1.setField("filename", FieldType.STRING, "delete-key");
        runner.enqueue(record1);
        Mockito.doThrow(new AmazonS3Exception("NoSuchBucket")).when(mockS3Client).deleteObject(Mockito.any());

        runner.run();

        /*runner.assertAllFlowFilesTransferred(DeleteS3Object.REL_FAILURE, 1);*/
        ArgumentCaptor<DeleteObjectRequest> captureRequest = ArgumentCaptor.forClass(DeleteObjectRequest.class);
        Mockito.verify(mockS3Client, Mockito.never()).deleteVersion(Mockito.any(DeleteVersionRequest.class));
    }

    @Test
    public void testDeleteVersionSimple() {
        Record record1 = new StandardRecord();

        runner.setProperty(DeleteS3Object.REGION, "us-west-2");
        runner.setProperty(DeleteS3Object.BUCKET_FIELD, "test-bucket");
        runner.setProperty(DeleteS3Object.VERSION_ID_FIELD, "test-version");
        /*final Map<String, String> attrs = new HashMap<>();
        attrs.put("filename", "test-key");*/
        record1.setField("filename", FieldType.STRING, "test-key");
        runner.enqueue(record1);

        runner.run();

        /*runner.assertAllFlowFilesTransferred(DeleteS3Object.REL_SUCCESS, 1);*/
        ArgumentCaptor<DeleteVersionRequest> captureRequest = ArgumentCaptor.forClass(DeleteVersionRequest.class);
        Mockito.verify(mockS3Client, Mockito.times(1)).deleteVersion(captureRequest.capture());
        DeleteVersionRequest request = captureRequest.getValue();
        assertEquals("test-bucket", request.getBucketName());
        assertEquals("test-key", request.getKey());
        assertEquals("test-version", request.getVersionId());
        Mockito.verify(mockS3Client, Mockito.never()).deleteObject(Mockito.any(DeleteObjectRequest.class));
    }

    @Test
    public void testDeleteVersionFromExpressions() {
        Record record1 = new StandardRecord();

        runner.setProperty(DeleteS3Object.REGION, "us-west-2");
        runner.setProperty(DeleteS3Object.BUCKET_FIELD, "${s3.bucket}");
        runner.setProperty(DeleteS3Object.VERSION_ID_FIELD, "${s3.version}");
        /*final Map<String, String> attrs = new HashMap<>();
        attrs.put("filename", "test-key");
        attrs.put("s3.bucket", "test-bucket");
        attrs.put("s3.version", "test-version");*/
        record1.setField("filename", FieldType.STRING, "test-key");
        record1.setField("s3.bucket", FieldType.STRING, "test-bucket");
        record1.setField("s3.version", FieldType.STRING, "test-version");
        runner.enqueue(record1);

        runner.run();

        /*runner.assertAllFlowFilesTransferred(DeleteS3Object.REL_SUCCESS, 1);*/
        ArgumentCaptor<DeleteVersionRequest> captureRequest = ArgumentCaptor.forClass(DeleteVersionRequest.class);
        Mockito.verify(mockS3Client, Mockito.times(1)).deleteVersion(captureRequest.capture());
        DeleteVersionRequest request = captureRequest.getValue();
        assertEquals("test-bucket", request.getBucketName());
        assertEquals("test-key", request.getKey());
        assertEquals("test-version", request.getVersionId());
        Mockito.verify(mockS3Client, Mockito.never()).deleteObject(Mockito.any(DeleteObjectRequest.class));
    }

    @Test
    public void testGetPropertyDescriptors() throws Exception {
        DeleteS3Object processor = new DeleteS3Object();
        List<PropertyDescriptor> pd = processor.getSupportedPropertyDescriptors();
        assertEquals("size should be eq", 23, pd.size());
        assertTrue(pd.contains(processor.ACCESS_KEY));
        assertTrue(pd.contains(processor.AWS_CREDENTIALS_PROVIDER_SERVICE));
        assertTrue(pd.contains(processor.BUCKET_FIELD));
        assertTrue(pd.contains(processor.CREDENTIALS_FILE));
        assertTrue(pd.contains(processor.ENDPOINT_OVERRIDE));
        assertTrue(pd.contains(processor.FULL_CONTROL_USER_LIST));
        assertTrue(pd.contains(processor.KEY_FEILD));
        assertTrue(pd.contains(processor.OWNER));
        assertTrue(pd.contains(processor.READ_ACL_LIST));
        assertTrue(pd.contains(processor.READ_USER_LIST));
        assertTrue(pd.contains(processor.REGION));
        assertTrue(pd.contains(processor.SECRET_KEY));
        assertTrue(pd.contains(processor.SIGNER_OVERRIDE));
        assertTrue(pd.contains(processor.SSL_CONTEXT_SERVICE));
        assertTrue(pd.contains(processor.TIMEOUT));
        assertTrue(pd.contains(processor.VERSION_ID_FIELD));
        assertTrue(pd.contains(processor.WRITE_ACL_LIST));
        assertTrue(pd.contains(processor.WRITE_USER_LIST));
        assertTrue(pd.contains(ProxyConfigurationService.PROXY_CONFIGURATION_SERVICE));
        assertTrue(pd.contains(processor.PROXY_HOST));
        assertTrue(pd.contains(processor.PROXY_HOST_PORT));
        assertTrue(pd.contains(processor.PROXY_USERNAME));
        assertTrue(pd.contains(processor.PROXY_PASSWORD));
    }
}
