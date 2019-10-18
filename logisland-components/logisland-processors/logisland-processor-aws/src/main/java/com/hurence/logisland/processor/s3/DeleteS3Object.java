package com.hurence.logisland.processor.s3;


import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.DeleteObjectRequest;
import com.amazonaws.services.s3.model.DeleteVersionRequest;
import com.hurence.logisland.annotation.documentation.CapabilityDescription;
import com.hurence.logisland.annotation.documentation.SeeAlso;
import com.hurence.logisland.annotation.documentation.Tags;
import com.hurence.logisland.component.PropertyDescriptor;
import com.hurence.logisland.processor.ProcessContext;
import com.hurence.logisland.record.Record;
import com.hurence.logisland.validator.StandardValidators;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

/*@SupportsBatching*/
@SeeAlso({PutS3Object.class, FetchS3Object.class, ListS3.class})
@Tags({"Amazon", "S3", "AWS", "Archive", "Delete"})
/*@InputRequirement(Requirement.INPUT_REQUIRED)*/
@CapabilityDescription("Deletes FlowFiles on an Amazon S3 Bucket. " +
        "If attempting to delete a file that does not exist, FlowFile is routed to success.")

public class DeleteS3Object extends AbstractS3Processor {

    public static final PropertyDescriptor VERSION_ID_FIELD = new PropertyDescriptor.Builder()
            .name("version.field")
            .description("The Version of the Object to delete")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(true/*ExpressionLanguageScope.FLOWFILE_ATTRIBUTES*/)
            .required(false)
            .build();

    public static final List<PropertyDescriptor> properties = Collections.unmodifiableList(
            Arrays.asList(KEY_FEILD, BUCKET_FIELD, ACCESS_KEY, SECRET_KEY, CREDENTIALS_FILE, AWS_CREDENTIALS_PROVIDER_SERVICE, REGION, TIMEOUT, VERSION_ID_FIELD,
                    FULL_CONTROL_USER_LIST, READ_USER_LIST, WRITE_USER_LIST, READ_ACL_LIST, WRITE_ACL_LIST, OWNER,
                    SSL_CONTEXT_SERVICE, ENDPOINT_OVERRIDE, SIGNER_OVERRIDE, PROXY_CONFIGURATION_SERVICE, PROXY_HOST, PROXY_HOST_PORT, PROXY_USERNAME, PROXY_PASSWORD));

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

    @Override
    public Collection<Record> process(ProcessContext context, Collection<Record> records) {
        /*FlowFile flowFile = session.get();
        if (flowFile == null) {
            return records;
        }*/

        final long startNanos = System.nanoTime();

        try {
            for (Record record : records) {
                final String bucket = context.getPropertyValue(BUCKET_FIELD).evaluate(record).asString();
                final String key = context.getPropertyValue(KEY_FEILD).evaluate(record).asString();
                final String versionId = context.getPropertyValue(VERSION_ID_FIELD).evaluate(record).asString();

                final AmazonS3 s3 = getClient();

                // Deletes a key on Amazon S3
                try {
                    if (versionId == null) {
                        final DeleteObjectRequest r = new DeleteObjectRequest(bucket, key);
                        // This call returns success if object doesn't exist
                        s3.deleteObject(r);
                    } else {
                        final DeleteVersionRequest r = new DeleteVersionRequest(bucket, key, versionId);
                        s3.deleteVersion(r);
                    }
                } catch (final AmazonServiceException ase) {
                    getLogger().error("Failed to delete S3 Object for {}; routing to failure", new Object[]{record, ase});
                    return records;
                }

                /*session.transfer(flowFile, REL_SUCCESS);*/
                final long transferMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);
                getLogger().info("Successfully delete S3 Object for {} in {} millis; routing to success", new Object[]{record, transferMillis});
                return records;
            }
        }catch (Throwable t) {
            getLogger().error("error while processing records ", t);
        }
        return records;


    }
}

