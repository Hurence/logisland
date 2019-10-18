package com.hurence.logisland.processor.s3;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.S3ClientOptions;
import com.amazonaws.services.s3.model.*;
import com.hurence.logisland.component.AllowableValue;
import com.hurence.logisland.component.PropertyDescriptor;
import com.hurence.logisland.processor.AbstractAWSCredentialsProviderProcessor;
import com.hurence.logisland.processor.ProcessContext;
import com.hurence.logisland.record.Record;
import com.hurence.logisland.validator.StandardValidators;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public abstract class AbstractS3Processor extends AbstractAWSCredentialsProviderProcessor<AmazonS3Client> {

    public static final PropertyDescriptor FULL_CONTROL_USER_LIST = new PropertyDescriptor.Builder()
            .name("full.control.user.list")
            .required(false)
            /*.expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)*/
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .description("A comma-separated list of Amazon User ID's or E-mail addresses that specifies who should have Full Control for an object")
            .defaultValue("${s3.permissions.full.users}")
            .build();
    public static final PropertyDescriptor READ_USER_LIST = new PropertyDescriptor.Builder()
            .name("read.permission.user.list")
            .required(false)
            /*.expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)*/
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .description("A comma-separated list of Amazon User ID's or E-mail addresses that specifies who should have Read Access for an object")
            .defaultValue("${s3.permissions.read.users}")
            .build();
    public static final PropertyDescriptor WRITE_USER_LIST = new PropertyDescriptor.Builder()
            .name("write.permission.user.list")
            .required(false)
            /*.expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)*/
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .description("A comma-separated list of Amazon User ID's or E-mail addresses that specifies who should have Write Access for an object")
            .defaultValue("${s3.permissions.write.users}")
            .build();
    public static final PropertyDescriptor READ_ACL_LIST = new PropertyDescriptor.Builder()
            .name("read.acl.user.list")
            .required(false)
            /*.expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)*/
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .description("A comma-separated list of Amazon User ID's or E-mail addresses that specifies who should have permissions to read the Access Control List for an object")
            .defaultValue("${s3.permissions.readacl.users}")
            .build();
    public static final PropertyDescriptor WRITE_ACL_LIST = new PropertyDescriptor.Builder()
            .name("write.acl.user.list")
            .required(false)
            /*.expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)*/
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .description("A comma-separated list of Amazon User ID's or E-mail addresses that specifies who should have permissions to change the Access Control List for an object")
            .defaultValue("${s3.permissions.writeacl.users}")
            .build();
    public static final PropertyDescriptor CANNED_ACL = new PropertyDescriptor.Builder()
            .name("canned.acl")
            .displayName("Canned ACL")
            .required(false)
            /*.expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)*/
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .description("Amazon Canned ACL for an object, one of: BucketOwnerFullControl, BucketOwnerRead, LogDeliveryWrite, AuthenticatedRead, PublicReadWrite, PublicRead, Private; " +
                    "will be ignored if any other ACL/permission/owner property is specified")
            .defaultValue("${s3.permissions.cannedacl}")
            .build();
    public static final PropertyDescriptor OWNER = new PropertyDescriptor.Builder()
            .name("owner")
            .required(false)
            /*.expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)*/
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .description("The Amazon ID to use for the object's owner")
            .defaultValue("${s3.owner}")
            .build();
    public static final PropertyDescriptor BUCKET_FIELD = new PropertyDescriptor.Builder()
            .name("bucket.field")
            /*.expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)*/
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    public static final PropertyDescriptor KEY_FEILD = new PropertyDescriptor.Builder()
            .name("key.field")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            /*.expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)*/
            .defaultValue("${filename}")
            .build();
    public static final PropertyDescriptor SIGNER_OVERRIDE = new PropertyDescriptor.Builder()
            .name("signer.override")
            .description("The AWS libraries use the default signer but this property allows you to specify a custom signer to support older S3-compatible services.")
            .required(false)
            .allowableValues(
                    new AllowableValue("Default Signature", "Default Signature"),
                    new AllowableValue("AWSS3V4SignerType", "Signature v4"),
                    new AllowableValue("S3SignerType", "Signature v2"))
            .defaultValue("Default Signature")
            .build();
    public static final PropertyDescriptor ENCRYPTION_SERVICE = new PropertyDescriptor.Builder()
            .name("encryption.service")
            .displayName("Encryption Service")
            .description("Specifies the Encryption Service Controller used to configure requests. " +
                    "PutS3Object: For backward compatibility, this value is ignored when 'Server Side Encryption' is set. " +
                    "FetchS3Object: Only needs to be configured in case of Server-side Customer Key, Client-side KMS and Client-side Customer Key encryptions.")
            .required(false)
            .identifiesControllerService(AmazonS3EncryptionService.class)
            .build();

    /**
     * Create client using credentials provider. This is the preferred way for creating clients
     */
    @Override
    protected AmazonS3Client createClient(final ProcessContext context, final AWSCredentialsProvider credentialsProvider, final ClientConfiguration config) {
        getLogger().info("Creating client with credentials provider");
        initializeSignerOverride(context, config);
        AmazonS3EncryptionService encryptionService = context.getPropertyValue(ENCRYPTION_SERVICE).asControllerService(AmazonS3EncryptionService.class);
        AmazonS3Client s3 = null;

        if (encryptionService != null) {
            s3 = encryptionService.createEncryptionClient(credentialsProvider, config);
        }

        if (s3 == null) {
            s3 = new AmazonS3Client(credentialsProvider, config);
        }

        initalizeEndpointOverride(context, s3);
        return s3;
    }

    private void initalizeEndpointOverride(final ProcessContext context, final AmazonS3Client s3) {
        // if ENDPOINT_OVERRIDE is set, use PathStyleAccess
        if(StringUtils.trimToEmpty(context.getPropertyValue(ENDPOINT_OVERRIDE).asString()).isEmpty() == false){
            final S3ClientOptions s3Options = new S3ClientOptions();
            s3Options.setPathStyleAccess(true);
            s3.setS3ClientOptions(s3Options);
        }
    }

    private void initializeSignerOverride(final ProcessContext context, final ClientConfiguration config) {
        String signer = context.getPropertyValue(SIGNER_OVERRIDE).getRawValue().toString();

        if (signer != null && !signer.equals(SIGNER_OVERRIDE.getDefaultValue())) {
            config.setSignerOverride(signer);
        }
    }

    /**
     * Create client using AWSCredentials
     *
     * @deprecated use {@link #createClient(ProcessContext, AWSCredentialsProvider, ClientConfiguration)} instead
     */
    @Override
    protected AmazonS3Client createClient(final ProcessContext context, final AWSCredentials credentials, final ClientConfiguration config) {
        getLogger().info("Creating client with AWS credentials");
        return createClient(context, new AWSStaticCredentialsProvider(credentials), config);
    }

    protected Grantee createGrantee(final String value) {
        if (StringUtils.isEmpty(value)) {
            return null;
        }

        if (value.contains("@")) {
            return new EmailAddressGrantee(value);
        } else {
            return new CanonicalGrantee(value);
        }
    }

    protected final List<Grantee> createGrantees(final String value) {
        if (StringUtils.isEmpty(value)) {
            return Collections.emptyList();
        }

        final List<Grantee> grantees = new ArrayList<>();
        final String[] vals = value.split(",");
        for (final String val : vals) {
            final String identifier = val.trim();
            final Grantee grantee = createGrantee(identifier);
            if (grantee != null) {
                grantees.add(grantee);
            }
        }
        return grantees;
    }

    protected String getUrlForObject(final String bucket, final String key) {
        Region region = getRegion();

        if (region == null) {
            return  DEFAULT_PROTOCOL.toString() + "://s3.amazonaws.com/" + bucket + "/" + key;
        } else {
            final String endpoint = region.getServiceEndpoint("s3");
            return DEFAULT_PROTOCOL.toString() + "://" + endpoint + "/" + bucket + "/" + key;
        }
    }

    /**
     * Create AccessControlList if appropriate properties are configured.
     *
     * @param context ProcessContext
     * @param record FlowFile
     * @return AccessControlList or null if no ACL properties were specified
     */
    protected final AccessControlList createACL(final ProcessContext context, final Record record) {
        // lazy-initialize ACL, as it should not be used if no properties were specified
        AccessControlList acl = null;

        final String ownerId = context.getPropertyValue(OWNER).evaluate(record).asString();
        if (!StringUtils.isEmpty(ownerId)) {
            final Owner owner = new Owner();
            owner.setId(ownerId);
            if (acl == null) {
                acl = new AccessControlList();
            }
            acl.setOwner(owner);
        }

        for (final Grantee grantee : createGrantees(context.getPropertyValue(FULL_CONTROL_USER_LIST).evaluate(record).asString())) {
            if (acl == null) {
                acl = new AccessControlList();
            }
            acl.grantPermission(grantee, Permission.FullControl);
        }

        for (final Grantee grantee : createGrantees(context.getPropertyValue(READ_USER_LIST).evaluate(record).asString())) {
            if (acl == null) {
                acl = new AccessControlList();
            }
            acl.grantPermission(grantee, Permission.Read);
        }

        for (final Grantee grantee : createGrantees(context.getPropertyValue(WRITE_USER_LIST).evaluate(record).asString())) {
            if (acl == null) {
                acl = new AccessControlList();
            }
            acl.grantPermission(grantee, Permission.Write);
        }

        for (final Grantee grantee : createGrantees(context.getPropertyValue(READ_ACL_LIST).evaluate(record).asString())) {
            if (acl == null) {
                acl = new AccessControlList();
            }
            acl.grantPermission(grantee, Permission.ReadAcp);
        }

        for (final Grantee grantee : createGrantees(context.getPropertyValue(WRITE_ACL_LIST).evaluate(record).asString())) {
            if (acl == null) {
                acl = new AccessControlList();
            }
            acl.grantPermission(grantee, Permission.WriteAcp);
        }

        return acl;
    }

    /**
     * Create CannedAccessControlList if {@link #CANNED_ACL} property specified.
     *
     * @param context ProcessContext
     * @param record FlowFile
     * @return CannedAccessControlList or null if not specified
     */
    protected final CannedAccessControlList createCannedACL(final ProcessContext context, final Record record) {
        CannedAccessControlList cannedAcl = null;

        final String cannedAclString = context.getPropertyValue(CANNED_ACL).evaluate(record).asString();
        if (!StringUtils.isEmpty(cannedAclString)) {
            cannedAcl = CannedAccessControlList.valueOf(cannedAclString);
        }

        return cannedAcl;
    }
}
