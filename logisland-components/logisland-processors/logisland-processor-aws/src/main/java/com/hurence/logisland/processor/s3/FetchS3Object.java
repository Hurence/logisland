package com.hurence.logisland.processor.s3;


import com.amazonaws.AmazonClientException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.SSEAlgorithm;
import com.hurence.logisland.annotation.behavior.WritesAttribute;
import com.hurence.logisland.annotation.behavior.WritesAttributes;
import com.hurence.logisland.annotation.documentation.CapabilityDescription;
import com.hurence.logisland.annotation.documentation.SeeAlso;
import com.hurence.logisland.annotation.documentation.Tags;
import com.hurence.logisland.component.AllowableValue;
import com.hurence.logisland.component.PropertyDescriptor;
import com.hurence.logisland.processor.ProcessContext;
import com.hurence.logisland.record.Field;
import com.hurence.logisland.record.FieldType;
import com.hurence.logisland.record.Record;
import com.hurence.logisland.validator.StandardValidators;
import com.hurence.logisland.validator.ValidationContext;
import com.hurence.logisland.validator.ValidationResult;
import org.apache.commons.lang3.exception.ExceptionUtils;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeUnit;

/*@SupportsBatching*/
@SeeAlso({PutS3Object.class, DeleteS3Object.class, ListS3.class})
/*@InputRequirement(Requirement.INPUT_REQUIRED)*/
@Tags({"Amazon", "S3", "AWS", "Get", "Fetch"})
@CapabilityDescription("Retrieves the contents of an S3 Object and writes it to the content of a FlowFile")
@WritesAttributes({
        @WritesAttribute(attribute = "s3.bucket", description = "The name of the S3 bucket"),
        @WritesAttribute(attribute = "path", description = "The path of the file"),
        @WritesAttribute(attribute = "absolute.path", description = "The path of the file"),
        @WritesAttribute(attribute = "filename", description = "The name of the file"),
        @WritesAttribute(attribute = "hash.value", description = "The MD5 sum of the file"),
        @WritesAttribute(attribute = "hash.algorithm", description = "MD5"),
        @WritesAttribute(attribute = "mime.type", description = "If S3 provides the content type/MIME type, this attribute will hold that file"),
        @WritesAttribute(attribute = "s3.etag", description = "The ETag that can be used to see if the file has changed"),
        @WritesAttribute(attribute = "s3.expirationTime", description = "If the file has an expiration date, this attribute will be set, containing the milliseconds since epoch in UTC time"),
        @WritesAttribute(attribute = "s3.expirationTimeRuleId", description = "The ID of the rule that dictates this object's expiration time"),
        @WritesAttribute(attribute = "s3.sseAlgorithm", description = "The server side encryption algorithm of the object"),
        @WritesAttribute(attribute = "s3.version", description = "The version of the S3 object"),
        @WritesAttribute(attribute = "s3.encryptionStrategy", description = "The name of the encryption strategy that was used to store the S3 object (if it is encrypted)"),})

public class FetchS3Object extends AbstractS3Processor {

    public static final PropertyDescriptor VERSION_ID_FEILD = new PropertyDescriptor.Builder()
            .name("Version")
            .description("The Version of the Object to download")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            /*.expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)*/
            .required(false)
            .build();
    public static final PropertyDescriptor REQUESTER_PAYS = new PropertyDescriptor.Builder()
            .name("requester-pays")
            .displayName("Requester Pays")
            .required(true)
            .description("If true, indicates that the requester consents to pay any charges associated with retrieving objects from "
                    + "the S3 bucket.  This sets the 'x-amz-request-payer' header to 'requester'.")
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .allowableValues(new AllowableValue("true", "True", "Indicates that the requester consents to pay any charges associated "
                    + "with retrieving objects from the S3 bucket."), new AllowableValue("false", "False", "Does not consent to pay "
                    + "requester charges for retrieving objects from the S3 bucket."))
            .defaultValue("false")
            .build();

    public static final List<PropertyDescriptor> properties = Collections.unmodifiableList(
            Arrays.asList(BUCKET_FIELD, KEY_FEILD, REGION, ACCESS_KEY, SECRET_KEY, CREDENTIALS_FILE, AWS_CREDENTIALS_PROVIDER_SERVICE, TIMEOUT, VERSION_ID_FEILD,
                    SSL_CONTEXT_SERVICE, ENDPOINT_OVERRIDE, SIGNER_OVERRIDE, ENCRYPTION_SERVICE, PROXY_CONFIGURATION_SERVICE, PROXY_HOST,
                    PROXY_HOST_PORT, PROXY_USERNAME, PROXY_PASSWORD, REQUESTER_PAYS));

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

    @Override
    protected Collection<ValidationResult> customValidate(ValidationContext validationContext) {
        final List<ValidationResult> problems = new ArrayList<>(super.customValidate(validationContext));

        AmazonS3EncryptionService encryptionService = validationContext.getPropertyValue(ENCRYPTION_SERVICE).asControllerService(AmazonS3EncryptionService.class);
        if (encryptionService != null) {
            String strategyName = encryptionService.getStrategyName();
            if (strategyName.equals(AmazonS3EncryptionService.STRATEGY_NAME_SSE_S3) || strategyName.equals(AmazonS3EncryptionService.STRATEGY_NAME_SSE_KMS)) {
                problems.add(new ValidationResult.Builder()
                        .subject(ENCRYPTION_SERVICE.getDisplayName())
                        .valid(false)
                        .explanation(encryptionService.getStrategyDisplayName() + " is not a valid encryption strategy for fetching objects. Decryption will be handled automatically " +
                                "during the fetch of S3 objects encrypted with " + encryptionService.getStrategyDisplayName())
                        .build()
                );
            }
        }

        return problems;
    }

    @Override
    public Collection<Record> process(ProcessContext context, Collection<Record> records) {
        /*FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }*/

        try {
            for (Record record : records) {
                final long startNanos = System.nanoTime();
                final String bucket = context.getPropertyValue(BUCKET_FIELD).evaluate(record).asString();
                final String key = context.getPropertyValue(KEY_FEILD).evaluate(record).asString();
                final String versionId = context.getPropertyValue(VERSION_ID_FEILD).evaluate(record).asString();
                final boolean requesterPays = context.getPropertyValue(REQUESTER_PAYS).asBoolean();

                final AmazonS3 client = getClient();
                final GetObjectRequest request;
                if (versionId == null) {
                    request = new GetObjectRequest(bucket, key);
                } else {
                    request = new GetObjectRequest(bucket, key, versionId);
                }
                request.setRequesterPays(requesterPays);

                final Map<String, Field> attributes = new HashMap<>();

                AmazonS3EncryptionService encryptionService = context.getPropertyValue(ENCRYPTION_SERVICE).asControllerService(AmazonS3EncryptionService.class);
                if (encryptionService != null) {
                    encryptionService.configureGetObjectRequest(request, new ObjectMetadata());
                    attributes.put("s3.encryptionStrategy", new Field(encryptionService.getStrategyName()));
                }

                try (final S3Object s3Object = client.getObject(request)) {
                    if (s3Object == null) {
                        throw new IOException("AWS refused to execute this request.");
                    }
                    /*flowFile = session.importFrom(s3Object.getObjectContent(), flowFile);*/
                    record.setField("s3Object", FieldType.ARRAY, s3Object.getObjectContent());
                    attributes.put("s3.bucket", new Field(s3Object.getBucketName()));

                    final ObjectMetadata metadata = s3Object.getObjectMetadata();
                    if (metadata.getContentDisposition() != null) {
                        final String fullyQualified = metadata.getContentDisposition();
                        final int lastSlash = fullyQualified.lastIndexOf("/");
                        if (lastSlash > -1 && lastSlash < fullyQualified.length() - 1) {
                            attributes.put("path"/*CoreAttributes.PATH.key()*/, new Field(fullyQualified.substring(0, lastSlash)));
                            attributes.put("absolute.path"/*CoreAttributes.ABSOLUTE_PATH.key()*/, new Field(fullyQualified));
                            attributes.put("filename"/*CoreAttributes.FILENAME.key()*/, new Field(fullyQualified.substring(lastSlash + 1)));
                        } else {
                            attributes.put("filename"/*CoreAttributes.FILENAME.key()*/, new Field(metadata.getContentDisposition()));
                        }
                    }
                    if (metadata.getContentMD5() != null) {
                        attributes.put("hash.value", new Field(metadata.getContentMD5()));
                        attributes.put("hash.algorithm", new Field("MD5"));
                    }
                    if (metadata.getContentType() != null) {
                        attributes.put("mime.type"/*CoreAttributes.MIME_TYPE.key()*/, new Field(metadata.getContentType()));
                    }
                    if (metadata.getETag() != null) {
                        attributes.put("s3.etag", new Field(metadata.getETag()));
                    }
                    if (metadata.getExpirationTime() != null) {
                        attributes.put("s3.expirationTime", new Field(String.valueOf(metadata.getExpirationTime().getTime())));
                    }
                    if (metadata.getExpirationTimeRuleId() != null) {
                        attributes.put("s3.expirationTimeRuleId", new Field(metadata.getExpirationTimeRuleId()));
                    }
                    if (metadata.getUserMetadata() != null) {
                        Map<String, Field> userMetadata = new HashMap<>();
                        for (Map.Entry<String, String> entry : metadata.getUserMetadata().entrySet()){
                            userMetadata.put(entry.getKey(), new Field(entry.getValue()));
                        }
                        attributes.putAll(userMetadata);
                    }
                    if (metadata.getSSEAlgorithm() != null) {
                        String sseAlgorithmName = metadata.getSSEAlgorithm();
                        attributes.put("s3.sseAlgorithm", new Field(sseAlgorithmName));
                        if (sseAlgorithmName.equals(SSEAlgorithm.AES256.getAlgorithm())) {
                            attributes.put("s3.encryptionStrategy", new Field(AmazonS3EncryptionService.STRATEGY_NAME_SSE_S3));
                        } else if (sseAlgorithmName.equals(SSEAlgorithm.KMS.getAlgorithm())) {
                            attributes.put("s3.encryptionStrategy", new Field(AmazonS3EncryptionService.STRATEGY_NAME_SSE_KMS));
                        }
                    }
                    if (metadata.getVersionId() != null) {
                        attributes.put("s3.version", new Field(metadata.getVersionId()));
                    }
                } catch (final IOException | AmazonClientException ioe) {
                    getLogger().error("Failed to retrieve S3 Object for {}; routing to failure", new Object[]{record, ioe});
                    /*flowFile = session.penalize(flowFile);
                    session.transfer(flowFile, REL_FAILURE);*/
                    return records;
                } /*catch (final FlowFileAccessException ffae) {
                    if (ExceptionUtils.indexOfType(ffae, AmazonClientException.class) != -1) {
                        getLogger().error("Failed to retrieve S3 Object for {}; routing to failure", new Object[]{flowFile, ffae});
                        flowFile = session.penalize(flowFile);
                        session.transfer(flowFile, REL_FAILURE);
                        return records;
                    }
                    throw ffae;
                }*/

                if (!attributes.isEmpty()) {
                    /*flowFile = session.putAllAttributes(flowFile, attributes);*/

                    record.addFields(attributes);
                }

                /*session.transfer(flowFile, REL_SUCCESS);*/
                final long transferMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);
                getLogger().info("Successfully retrieved S3 Object for {} in {} millis; routing to success", new Object[]{record, transferMillis});
                /*session.getProvenanceReporter().fetch(flowFile, "http://" + bucket + ".amazonaws.com/" + key, transferMillis);*/
            }
        } catch (Throwable t) {
            getLogger().error("error while processing records ", t);
        }
        return records;


    }

}
