package com.hurence.logisland.processor.services;

import com.hurence.logisland.annotation.documentation.CapabilityDescription;
import com.hurence.logisland.annotation.documentation.Tags;
import com.hurence.logisland.component.AllowableValue;
import com.hurence.logisland.controller.ControllerService;
import com.hurence.logisland.processor.ProcessException;

import javax.net.ssl.SSLContext;
import java.security.NoSuchAlgorithmException;
import java.util.*;

/**
 * Definition for SSLContextService.
 *
 */
@Tags({"ssl", "secure", "certificate", "keystore", "truststore", "jks", "p12", "pkcs12", "pkcs"})
@CapabilityDescription("Provides the ability to configure keystore and/or truststore properties once and reuse "
        + "that configuration throughout the application")
public interface SSLContextService extends ControllerService {

    public static enum ClientAuth {

        WANT,
        REQUIRED,
        NONE
    }

    public SSLContext createSSLContext(final ClientAuth clientAuth) throws ProcessException;

    public String getTrustStoreFile();

    public String getTrustStoreType();

    public String getTrustStorePassword();

    public boolean isTrustStoreConfigured();

    public String getKeyStoreFile();

    public String getKeyStoreType();

    public String getKeyStorePassword();

    public String getKeyPassword();

    public boolean isKeyStoreConfigured();

    String getSslAlgorithm();

    /**
     * Build a set of allowable TLS/SSL protocol algorithms based on JVM configuration.
     *
     * @return the computed set of allowable values
     */
    static AllowableValue[] buildAlgorithmAllowableValues() {
        final Set<String> supportedProtocols = new HashSet<>();

        /*
         * Prepopulate protocols with generic instance types commonly used
         * see: http://docs.oracle.com/javase/7/docs/technotes/guides/security/StandardNames.html#SSLContext
         */
        supportedProtocols.add("TLS");
        supportedProtocols.add("SSL");

        // Determine those provided by the JVM on the system
        try {
            supportedProtocols.addAll(Arrays.asList(SSLContext.getDefault().createSSLEngine().getSupportedProtocols()));
        } catch (NoSuchAlgorithmException e) {
            // ignored as default is used
        }

        final int numProtocols = supportedProtocols.size();

        // Sort for consistent presentation in configuration views
        final List<String> supportedProtocolList = new ArrayList<>(supportedProtocols);
        Collections.sort(supportedProtocolList);

        final List<AllowableValue> protocolAllowableValues = new ArrayList<>();
        for (final String protocol : supportedProtocolList) {
            protocolAllowableValues.add(new AllowableValue(protocol));
        }
        return protocolAllowableValues.toArray(new AllowableValue[numProtocols]);
    }
}
