package com.hurence.logisland.processor.s3;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.amazonaws.services.s3.model.StorageClass;
import com.amazonaws.services.s3.model.Tag;


import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.ListMultipartUploadsRequest;
import com.amazonaws.services.s3.model.MultipartUploadListing;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.PutObjectResult;

import com.hurence.logisland.component.AllowableValue;
import com.hurence.logisland.component.PropertyDescriptor;
import com.hurence.logisland.processor.ProcessContext;
import com.hurence.logisland.record.*;
import com.hurence.logisland.util.runner.MockRecord;
import com.hurence.logisland.util.runner.TestRunner;
import com.hurence.logisland.util.runner.TestRunners;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;


public class PutS3ObjectTest {

    private TestRunner runner;
    private PutS3Object putS3Object;
    private AmazonS3Client mockS3Client;

    @Before
    public void setUp() {
        mockS3Client = Mockito.mock(AmazonS3Client.class);
        putS3Object = new PutS3Object() {
            protected AmazonS3Client getClient() {
                return mockS3Client;
            }
        };
        runner = TestRunners.newTestRunner(putS3Object);
    }

    @Test
    public void testPutSinglePart() {

        runner.setProperty("x-custom-prop", "hello");
        prepareTest();

        runner.run();

        ArgumentCaptor<PutObjectRequest> captureRequest = ArgumentCaptor.forClass(PutObjectRequest.class);
        Mockito.verify(mockS3Client, Mockito.times(1)).putObject(captureRequest.capture());
        PutObjectRequest request = captureRequest.getValue();
        assertEquals("test-bucket", request.getBucketName());

        /*runner.assertAllFlowFilesTransferred(PutS3Object.REL_SUCCESS, 1);*/
        // TODO see with what to replace that

        MockRecord out = runner.getOutputRecords().get(0);

        out.assertFieldEquals("filename", "testfile.txt");
        out.assertFieldEquals(PutS3Object.S3_ETAG_ATTR_KEY, "test-etag");
        out.assertFieldEquals(PutS3Object.S3_VERSION_ATTR_KEY, "test-version");
    }

    @Test
    public void testPutSinglePartException() {
        prepareTest();

        Mockito.when(mockS3Client.putObject(Mockito.any(PutObjectRequest.class))).thenThrow(new AmazonS3Exception("TestFail"));

        runner.run();

        /*runner.assertAllFlowFilesTransferred(PutS3Object.REL_FAILURE, 1);*/
        // TODO see how to replace this failure
        MockRecord out = runner.getOutputRecords().get(0);
        out.assertFieldExists(FieldDictionary.RECORD_ERRORS);
    }

    @Test
    public void testSignerOverrideOptions() {
        final AWSCredentialsProvider credentialsProvider = new DefaultAWSCredentialsProviderChain();
        final ClientConfiguration config = new ClientConfiguration();
        final PutS3Object processor = new PutS3Object();
        final TestRunner runner = TestRunners.newTestRunner(processor);

        final List<AllowableValue> allowableSignerValues = PutS3Object.SIGNER_OVERRIDE.getAllowableValues();
        final String defaultSignerValue = PutS3Object.SIGNER_OVERRIDE.getDefaultValue();

        for (AllowableValue allowableSignerValue : allowableSignerValues) {
            String signerType = allowableSignerValue.getValue();
            if (!signerType.equals(defaultSignerValue)) {
                runner.setProperty(PutS3Object.SIGNER_OVERRIDE, signerType);
                ProcessContext context = runner.getProcessContext();
                try {
                    processor.createClient(context, credentialsProvider, config);
                } catch (IllegalArgumentException argEx) {
                    Assert.fail(argEx.getMessage());
                }
            }
        }
    }

    @Test
    public void testObjectTags() {
        runner.setProperty(PutS3Object.OBJECT_TAGS_PREFIX, "tagS3");
        runner.setProperty(PutS3Object.REMOVE_TAG_PREFIX, "false");
        prepareTest();

        runner.run();

        ArgumentCaptor<PutObjectRequest> captureRequest = ArgumentCaptor.forClass(PutObjectRequest.class);
        Mockito.verify(mockS3Client, Mockito.times(1)).putObject(captureRequest.capture());
        PutObjectRequest request = captureRequest.getValue();

        List<Tag> tagSet = request.getTagging().getTagSet();

        assertEquals(1, tagSet.size());
        assertEquals("tagS3PII", tagSet.get(0).getKey());
        assertEquals("true", tagSet.get(0).getValue());
    }

    @Test
    public void testStorageClasses() {
        for (StorageClass storageClass : StorageClass.values()) {
            runner.setProperty(PutS3Object.STORAGE_CLASS, storageClass.name());
            prepareTest();

            runner.run();

            ArgumentCaptor<PutObjectRequest> captureRequest = ArgumentCaptor.forClass(PutObjectRequest.class);
            Mockito.verify(mockS3Client, Mockito.times(1)).putObject(captureRequest.capture());
            PutObjectRequest request = captureRequest.getValue();

            assertEquals(storageClass.toString(), request.getStorageClass());

            Mockito.reset(mockS3Client);
        }
    }

    @Test
    public void testFilenameWithNationalCharacters() throws UnsupportedEncodingException {
        prepareTest("Iñtërnâtiônàližætiøn.txt");

        runner.run();

        ArgumentCaptor<PutObjectRequest> captureRequest = ArgumentCaptor.forClass(PutObjectRequest.class);
        Mockito.verify(mockS3Client, Mockito.times(1)).putObject(captureRequest.capture());
        PutObjectRequest request = captureRequest.getValue();

        ObjectMetadata objectMetadata = request.getMetadata();
        assertEquals("Iñtërnâtiônàližætiøn.txt", URLDecoder.decode(objectMetadata.getContentDisposition(), "UTF-8"));
    }

    private void prepareTest() {
        prepareTest("testfile.txt");
    }

    private void prepareTest(String filename) {
        Record record1 = new StandardRecord();

        runner.setProperty(PutS3Object.REGION, "ap-northeast-1");
        runner.setProperty(PutS3Object.BUCKET_FIELD, "test-bucket");
        runner.assertValid();

        record1.setField("filename", FieldType.STRING, filename);
        record1.setField("tagS3PII", FieldType.STRING, "true");
        byte[] a = {0, 1, 2, 3, 4, 5};
        record1.setField(new Field(FieldDictionary.RECORD_VALUE, FieldType.BYTES, a));
        runner.enqueue(record1);

        PutObjectResult putObjectResult = new PutObjectResult();
        putObjectResult.setExpirationTime(new Date());
        putObjectResult.setMetadata(new ObjectMetadata());
        putObjectResult.setVersionId("test-version");
        putObjectResult.setETag("test-etag");

        Mockito.when(mockS3Client.putObject(Mockito.any(PutObjectRequest.class))).thenReturn(putObjectResult);

        MultipartUploadListing uploadListing = new MultipartUploadListing();
        Mockito.when(mockS3Client.listMultipartUploads(Mockito.any(ListMultipartUploadsRequest.class))).thenReturn(uploadListing);
    }

    @Test
    public void testGetPropertyDescriptors() {
        PutS3Object processor = new PutS3Object();
        List<PropertyDescriptor> pd = processor.getSupportedPropertyDescriptors();
        assertEquals("size should be eq", 34, pd.size());
        assertTrue(pd.contains(PutS3Object.ACCESS_KEY));
        assertTrue(pd.contains(PutS3Object.AWS_CREDENTIALS_PROVIDER_SERVICE));
        assertTrue(pd.contains(PutS3Object.BUCKET_FIELD));
        assertTrue(pd.contains(PutS3Object.CANNED_ACL));
        assertTrue(pd.contains(PutS3Object.CREDENTIALS_FILE));
        assertTrue(pd.contains(PutS3Object.ENDPOINT_OVERRIDE));
        assertTrue(pd.contains(PutS3Object.FULL_CONTROL_USER_LIST));
        assertTrue(pd.contains(PutS3Object.KEY_FEILD));
        assertTrue(pd.contains(PutS3Object.OWNER));
        assertTrue(pd.contains(PutS3Object.READ_ACL_LIST));
        assertTrue(pd.contains(PutS3Object.READ_USER_LIST));
        assertTrue(pd.contains(PutS3Object.REGION));
        assertTrue(pd.contains(PutS3Object.SECRET_KEY));
        assertTrue(pd.contains(PutS3Object.SIGNER_OVERRIDE));
        assertTrue(pd.contains(PutS3Object.SSL_CONTEXT_SERVICE));
        assertTrue(pd.contains(PutS3Object.TIMEOUT));
        assertTrue(pd.contains(PutS3Object.EXPIRATION_RULE_ID));
        assertTrue(pd.contains(PutS3Object.STORAGE_CLASS));
        assertTrue(pd.contains(PutS3Object.WRITE_ACL_LIST));
        assertTrue(pd.contains(PutS3Object.WRITE_USER_LIST));
        assertTrue(pd.contains(PutS3Object.SERVER_SIDE_ENCRYPTION));
        assertTrue(pd.contains(PutS3Object.ENCRYPTION_SERVICE));
        assertTrue(pd.contains(PutS3Object.PROXY_CONFIGURATION_SERVICE));
        assertTrue(pd.contains(PutS3Object.PROXY_HOST));
        assertTrue(pd.contains(PutS3Object.PROXY_HOST_PORT));
        assertTrue(pd.contains(PutS3Object.PROXY_USERNAME));
        assertTrue(pd.contains(PutS3Object.PROXY_PASSWORD));
        assertTrue(pd.contains(PutS3Object.OBJECT_TAGS_PREFIX));
        assertTrue(pd.contains(PutS3Object.REMOVE_TAG_PREFIX));
        assertTrue(pd.contains(PutS3Object.CONTENT_TYPE));
        assertTrue(pd.contains(PutS3Object.MULTIPART_THRESHOLD));
        assertTrue(pd.contains(PutS3Object.MULTIPART_PART_SIZE));
        assertTrue(pd.contains(PutS3Object.MULTIPART_S3_AGEOFF_INTERVAL));
        assertTrue(pd.contains(PutS3Object.MULTIPART_S3_MAX_AGE));
    }
}
