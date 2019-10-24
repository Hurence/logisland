package com.hurence.logisland.processor.s3;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.GetObjectTaggingRequest;
import com.amazonaws.services.s3.model.GetObjectTaggingResult;
import com.amazonaws.services.s3.model.ObjectTagging;
import com.amazonaws.services.s3.model.SetObjectTaggingRequest;
import com.amazonaws.services.s3.model.Tag;
import com.hurence.logisland.annotation.behavior.WritesAttribute;
import com.hurence.logisland.annotation.behavior.WritesAttributes;
import com.hurence.logisland.annotation.documentation.CapabilityDescription;
import com.hurence.logisland.annotation.documentation.SeeAlso;
import com.hurence.logisland.annotation.documentation.Tags;
import com.hurence.logisland.component.PropertyDescriptor;
import com.hurence.logisland.processor.ProcessContext;
import com.hurence.logisland.record.Field;
import com.hurence.logisland.record.Record;
import com.hurence.logisland.record.StandardRecord;
import com.hurence.logisland.util.string.StringUtils;
import com.hurence.logisland.validator.StandardValidators;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;


@WritesAttributes({
        @WritesAttribute(attribute = "s3.tag.___", description = "The tags associated with the S3 object will be " +
                "written as part of the FlowFile attributes")})
@SeeAlso({PutS3Object.class, FetchS3Object.class, ListS3.class})
@Tags({"Amazon", "S3", "AWS", "Archive", "Tag"})
@CapabilityDescription("Sets tags on a Field within an Amazon S3 Bucket. " +
        "If attempting to tag a file that does not exist, FlowFile is routed to success.")
public class TagS3Object extends AbstractS3Processor {

    public static final PropertyDescriptor TAG_KEY = new PropertyDescriptor.Builder()
            .name("tag-key")
            .displayName("Tag Key")
            .description("The key of the tag that will be set on the S3 Object")
            .addValidator(new StandardValidators.StringLengthValidator(1, 127))
            .expressionLanguageSupported(true)
            .required(true)
            .build();

    public static final PropertyDescriptor TAG_VALUE = new PropertyDescriptor.Builder()
            .name("tag-value")
            .displayName("Tag Value")
            .description("The value of the tag that will be set on the S3 Object")
            .addValidator(new StandardValidators.StringLengthValidator(1, 255))
            .expressionLanguageSupported(true)
            .required(true)
            .build();

    public static final PropertyDescriptor APPEND_TAG = new PropertyDescriptor.Builder()
            .name("append-tag")
            .displayName("Append Tag")
            .description("If set to true, the tag will be appended to the existing set of tags on the S3 object. " +
                    "Any existing tags with the same key as the new tag will be updated with the specified value. If " +
                    "set to false, the existing tags will be removed and the new tag will be set on the S3 object.")
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .allowableValues("true", "false")
            .expressionLanguageSupported(false)
            .required(true)
            .defaultValue("true")
            .build();

    public static final PropertyDescriptor VERSION_ID = new PropertyDescriptor.Builder()
            .name("version")
            .displayName("Version ID")
            .description("The Version of the Object to tag")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(true)
            .required(false)
            .build();

    public static final List<PropertyDescriptor> properties = Collections.unmodifiableList(
            Arrays.asList(KEY_FEILD, BUCKET_FIELD, VERSION_ID, TAG_KEY, TAG_VALUE, APPEND_TAG, ACCESS_KEY, SECRET_KEY,
                    CREDENTIALS_FILE, AWS_CREDENTIALS_PROVIDER_SERVICE, REGION, TIMEOUT, SSL_CONTEXT_SERVICE,
                    ENDPOINT_OVERRIDE, SIGNER_OVERRIDE, PROXY_CONFIGURATION_SERVICE, PROXY_HOST, PROXY_HOST_PORT,
                    PROXY_USERNAME, PROXY_PASSWORD));

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

    @Override
    public Collection<Record> process(ProcessContext context, Collection<Record> records) {

        final long startNanos = System.nanoTime();

        try {
            for (Record record : records) {
                final String bucket = context.getPropertyValue(BUCKET_FIELD).evaluate(record).asString();
                final String key = context.getPropertyValue(KEY_FEILD).evaluate(record).asString();
                final String newTagKey = context.getPropertyValue(TAG_KEY).evaluate(record).asString();
                final String newTagVal = context.getPropertyValue(TAG_VALUE).evaluate(record).asString();

                if(StringUtils.isBlank(bucket)){
                    failFlowWithBlankEvaluatedProperty(BUCKET_FIELD);
                    return records;
                }

                if(StringUtils.isBlank(key)){
                    failFlowWithBlankEvaluatedProperty(KEY_FEILD);
                    return records;
                }

                if(StringUtils.isBlank(newTagKey)){
                    failFlowWithBlankEvaluatedProperty(TAG_KEY);
                    return records;
                }

                if(StringUtils.isBlank(newTagVal)){
                    failFlowWithBlankEvaluatedProperty(TAG_VALUE);
                    return records;
                }

                final String version = context.getPropertyValue(VERSION_ID).evaluate(record).asString();

                final AmazonS3 s3 = getClient();

                SetObjectTaggingRequest r;
                List<Tag> tags = new ArrayList<>();

                try {
                    if(context.getPropertyValue(APPEND_TAG).asBoolean()) {
                        final GetObjectTaggingRequest gr = new GetObjectTaggingRequest(bucket, key);
                        GetObjectTaggingResult res = s3.getObjectTagging(gr);

                        // preserve tags on S3 object, but filter out existing tag keys that match the one we're setting
                        tags = res.getTagSet().stream().filter(t -> !t.getKey().equals(newTagKey)).collect(Collectors.toList());
                    }

                    tags.add(new Tag(newTagKey, newTagVal));

                    if(StringUtils.isBlank(version)){
                        r = new SetObjectTaggingRequest(bucket, key, new ObjectTagging(tags));
                    } else{
                        r = new SetObjectTaggingRequest(bucket, key, version, new ObjectTagging(tags));
                    }
                    s3.setObjectTagging(r);
                } catch (final AmazonServiceException ase) {
                    getLogger().error("Failed to tag S3 Object for {}; routing to failure", new Object[]{record, ase});
                    /*flowFile = session.penalize(flowFile);
                    session.transfer(flowFile, REL_FAILURE);*/
                    // TODO see how to replace this
                    return records;
                }

                record = setTagAttributes(record, tags);

                /*session.transfer(flowFile, REL_SUCCESS);*/
                final long transferMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);
                getLogger().info("Successfully tagged S3 Object for {} in {} millis; routing to success", new Object[]{record , transferMillis});
            }
        }catch (Throwable t) {
            getLogger().error("error while processing records ", t);
        }
        return records;
    }

    private void failFlowWithBlankEvaluatedProperty(PropertyDescriptor pd) {
        getLogger().error("{} value is blank after attribute expression language evaluation", new Object[]{pd.getName()});
        /*flowFile = session.penalize(flowFile);
        session.transfer(flowFile, REL_FAILURE);*/
        // TODO see how to replace this
    }

    private Record setTagAttributes(Record record, List<Tag> tags) {
        Record record1 = new StandardRecord(record);
        for (String name : record1.getAllFieldNames()) {
            if (name.matches("^s3\\.tag\\..*")) record.removeField(name);
        }
        for (Tag tag : tags) {
            record.setField(new Field("s3.tag." + tag.getKey(), tag.getValue()));
        }
        return record;
    }
}
