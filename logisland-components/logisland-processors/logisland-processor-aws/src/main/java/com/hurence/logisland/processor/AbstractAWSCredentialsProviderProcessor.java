package com.hurence.logisland.processor;

import com.amazonaws.AmazonWebServiceClient;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.hurence.logisland.annotation.lifecycle.OnScheduled;
import com.hurence.logisland.annotation.lifecycle.OnShutdown;
import com.hurence.logisland.component.PropertyDescriptor;
import com.hurence.logisland.controller.ControllerService;
import com.hurence.logisland.processor.services.AWSCredentailsProviderService;

public abstract class AbstractAWSCredentialsProviderProcessor<ClientType extends AmazonWebServiceClient>
        extends AbstractAWSProcessor<ClientType>  {

    /**
     * AWS credentials provider service
     *
     * @see  <a href="http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/auth/AWSCredentialsProvider.html">AWSCredentialsProvider</a>
     */
    public static final PropertyDescriptor AWS_CREDENTIALS_PROVIDER_SERVICE = new PropertyDescriptor.Builder()
            .name("aws.credentials.provider.service")
            .description("The Controller Service that is used to obtain aws credentials provider")
            .required(false)
            .identifiesControllerService(AWSCredentailsProviderService.class)
            .build();

    /**
     * This method checks if {#link {@link #AWS_CREDENTIALS_PROVIDER_SERVICE} is available and if it
     * is, uses the credentials provider, otherwise it invokes the {@link AbstractAWSProcessor#onScheduled(ProcessContext)}
     * which uses static AWSCredentials for the aws processors
     */
    @OnScheduled
    public void onScheduled(ProcessContext context) {
        ControllerService service = context.getPropertyValue(AWS_CREDENTIALS_PROVIDER_SERVICE).asControllerService();
        if (service != null) {
            getLogger().debug("Using aws credentials provider service for creating client");
            onScheduledUsingControllerService(context);
        } else {
            getLogger().debug("Using aws credentials for creating client");
            super.onScheduled(context);
        }
    }

    /**
     * Create aws client using credentials provider
     * @param context the process context
     */
    protected void onScheduledUsingControllerService(ProcessContext context) {
        final ClientType awsClient = createClient(context, getCredentialsProvider(context), createConfiguration(context));
        this.client = awsClient;
        super.initializeRegionAndEndpoint(context);

    }

    @OnShutdown
    public void onShutDown() {
        if ( this.client != null ) {
            this.client.shutdown();
        }
    }

    /**
     * Get credentials provider using the {@link AWSCredentailsProviderService}
     * @param context the process context
     * @return AWSCredentialsProvider the credential provider
     * @see  <a href="http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/auth/AWSCredentialsProvider.html">AWSCredentialsProvider</a>
     */
    protected AWSCredentialsProvider getCredentialsProvider(final ProcessContext context) {

        final AWSCredentailsProviderService awsCredentialsProviderService =
                context.getPropertyValue(AWS_CREDENTIALS_PROVIDER_SERVICE).asControllerService(AWSCredentailsProviderService.class);

        return awsCredentialsProviderService.getCredentialsProvider();

    }

    /**
     * Abstract method to create aws client using credentials provider.  This is the preferred method
     * for creating aws clients
     * @param context process context
     * @param credentialsProvider aws credentials provider
     * @param config aws client configuration
     * @return ClientType the client
     */
    protected abstract ClientType createClient(final ProcessContext context, final AWSCredentialsProvider credentialsProvider, final ClientConfiguration config);
}
