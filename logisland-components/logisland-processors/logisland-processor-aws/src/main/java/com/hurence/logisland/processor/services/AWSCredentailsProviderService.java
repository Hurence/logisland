package com.hurence.logisland.processor.services;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.hurence.logisland.annotation.documentation.CapabilityDescription;
import com.hurence.logisland.annotation.documentation.Tags;
import com.hurence.logisland.controller.ControllerService;
import com.hurence.logisland.processor.ProcessException;

/**
 * AWSCredentialsProviderService interface to support getting AWSCredentialsProvider used for instantiating
 * aws clients
 *
 * @see <a href="http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/auth/AWSCredentialsProvider.html">AWSCredentialsProvider</a>
 */
@Tags({"aws", "security", "credentials", "provider", "session"})
@CapabilityDescription("Provides AWSCredentialsProvider.")
public interface AWSCredentailsProviderService extends ControllerService {

    /**
     * Get credentials provider
     * @return credentials provider
     * @throws ProcessException process exception in case there is problem in getting credentials provider
     *
     * @see  <a href="http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/auth/AWSCredentialsProvider.html">AWSCredentialsProvider</a>
     */
    AWSCredentialsProvider getCredentialsProvider() throws ProcessException;
}
