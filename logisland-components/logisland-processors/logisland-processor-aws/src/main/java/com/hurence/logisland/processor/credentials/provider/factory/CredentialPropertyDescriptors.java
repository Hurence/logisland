package com.hurence.logisland.processor.credentials.provider.factory;

import com.hurence.logisland.component.PropertyDescriptor;
import com.hurence.logisland.validator.StandardValidators;

/**
 * Shared definitions of properties that specify various AWS credentials.
 *
 * @see <a href="http://docs.aws.amazon.com/AWSSdkDocsJava/latest/DeveloperGuide/credentials.html">
 *     Providing AWS Credentials in the AWS SDK for Java</a>
 */
public class CredentialPropertyDescriptors {

    /**
     * Specifies use of the Default Credential Provider Chain
     *
     * @see <a href="http://docs.aws.amazon.com/AWSSdkDocsJava/latest/DeveloperGuide/credentials.html#id1">
     *     AWS SDK: Default Credential Provider Chain
     *     </a>
     */
    public static final PropertyDescriptor USE_DEFAULT_CREDENTIALS = new PropertyDescriptor.Builder()
            .name("default-credentials")
            .displayName("Use Default Credentials")
            .expressionLanguageSupported(false)
            .required(false)
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .sensitive(false)
            .allowableValues("true", "false")
            .defaultValue("false")
            .description("If true, uses the Default Credential chain, including EC2 instance profiles or roles, " +
                    "environment variables, default user credentials, etc.")
            .build();

    public static final PropertyDescriptor CREDENTIALS_FILE = new PropertyDescriptor.Builder()
            .name("Credentials File")
            .displayName("Credentials File")
            .expressionLanguageSupported(false)
            .required(false)
            .addValidator(StandardValidators.FILE_EXISTS_VALIDATOR)
            .description("Path to a file containing AWS access key and secret key in properties file format.")
            .build();

    public static final PropertyDescriptor ACCESS_KEY = new PropertyDescriptor.Builder()
            .name("Access Key")
            .displayName("Access Key ID")
            /*.expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)*/
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .sensitive(true)
            .build();

    public static final PropertyDescriptor SECRET_KEY = new PropertyDescriptor.Builder()
            .name("Secret Key")
            .displayName("Secret Access Key")
            .expressionLanguageSupported(false)
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .sensitive(true)
            .build();

    /**
     * Specifies use of a named profile credential.
     *
     * @see <a href="http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/auth/profile/ProfileCredentialsProvider.html">
     *     ProfileCredentialsProvider</a>
     */
    public static final PropertyDescriptor PROFILE_NAME = new PropertyDescriptor.Builder()
            .name("profile-name")
            .displayName("Profile Name")
            .expressionLanguageSupported(false)
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .sensitive(false)
            .description("The AWS profile name for credentials from the profile configuration file.")
            .build();

    public static final PropertyDescriptor USE_ANONYMOUS_CREDENTIALS = new PropertyDescriptor.Builder()
            .name("anonymous-credentials")
            .displayName("Use Anonymous Credentials")
            .expressionLanguageSupported(false)
            .required(false)
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .sensitive(false)
            .allowableValues("true", "false")
            .defaultValue("false")
            .description("If true, uses Anonymous credentials")
            .build();

    /**
     * AWS Role Arn used for cross account access
     *
     * @see <a href="http://docs.aws.amazon.com/general/latest/gr/aws-arns-and-namespaces.html#genref-arns">AWS ARN</a>
     */
    public static final PropertyDescriptor ASSUME_ROLE_ARN = new PropertyDescriptor.Builder()
            .name("Assume Role ARN")
            .displayName("Assume Role ARN")
            .expressionLanguageSupported(false)
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .sensitive(false)
            .description("The AWS Role ARN for cross account access. This is used in conjunction with role name and session timeout")
            .build();

    /**
     * The role name while creating aws role
     */
    public static final PropertyDescriptor ASSUME_ROLE_NAME = new PropertyDescriptor.Builder()
            .name("Assume Role Session Name")
            .displayName("Assume Role Session Name")
            .expressionLanguageSupported(false)
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .sensitive(false)
            .description("The AWS Role Name for cross account access. This is used in conjunction with role ARN and session time out")
            .build();

    /**
     * Max session time for role based credentials. The range is between 900 and 3600 seconds.
     */
    public static final PropertyDescriptor MAX_SESSION_TIME = new PropertyDescriptor.Builder()
            .name("Session Time")
            .description("Session time for role based session (between 900 and 3600 seconds). This is used in conjunction with role ARN and name")
            .defaultValue("3600")
            .required(false)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .sensitive(false)
            .build();

    /**
     * The ExternalId used while creating aws role.
     */
    public static final PropertyDescriptor ASSUME_ROLE_EXTERNAL_ID = new PropertyDescriptor.Builder()
            .name("assume-role-external-id")
            .displayName("Assume Role External ID")
            .expressionLanguageSupported(false)
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .sensitive(false)
            .description("External ID for cross-account access. This is used in conjunction with role arn, " +
                    "role name, and optional session time out")
            .build();

    /**
     * Assume Role Proxy variables for configuring proxy to retrieve keys
     */
    public static final PropertyDescriptor ASSUME_ROLE_PROXY_HOST = new PropertyDescriptor.Builder()
            .name("assume-role-proxy-host")
            .displayName("Assume Role Proxy Host")
            .expressionLanguageSupported(false)
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .sensitive(false)
            .description("Proxy host for cross-account access, if needed within your environment. This will configure a proxy to request for temporary access keys into another AWS account")
            .build();

    public static final PropertyDescriptor ASSUME_ROLE_PROXY_PORT = new PropertyDescriptor.Builder()
            .name("assume-role-proxy-port")
            .displayName("Assume Role Proxy Port")
            .expressionLanguageSupported(false)
            .required(false)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .sensitive(false)
            .description("Proxy pot for cross-account access, if needed within your environment. This will configure a proxy to request for temporary access keys into another AWS account")
            .build();
}