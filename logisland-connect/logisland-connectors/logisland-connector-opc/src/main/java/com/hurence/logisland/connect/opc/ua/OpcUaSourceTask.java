/**
 * Copyright (C) 2016 Hurence (support@hurence.com)
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hurence.logisland.connect.opc.ua;

import com.hurence.logisland.connect.opc.CommonDefinitions;
import com.hurence.logisland.connect.opc.CommonOpcSourceTask;
import com.hurence.opc.ConnectionProfile;
import com.hurence.opc.OpcOperations;
import com.hurence.opc.SessionProfile;
import com.hurence.opc.auth.Credentials;
import com.hurence.opc.auth.UsernamePasswordCredentials;
import com.hurence.opc.auth.X509Credentials;
import com.hurence.opc.ua.OpcUaConnectionProfile;
import com.hurence.opc.ua.OpcUaSessionProfile;
import com.hurence.opc.ua.OpcUaTemplate;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.bouncycastle.openssl.PEMEncryptedKeyPair;
import org.bouncycastle.openssl.PEMKeyPair;
import org.bouncycastle.openssl.PEMParser;
import org.bouncycastle.openssl.jcajce.JcaPEMKeyConverter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.StringReader;
import java.net.URI;
import java.security.KeyPair;
import java.security.PrivateKey;
import java.security.Security;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.time.Duration;
import java.util.Map;

/**
 * OPC-UA source task.
 *
 * @author amarziali
 */
public class OpcUaSourceTask extends CommonOpcSourceTask {

    static {
        Security.addProvider(new BouncyCastleProvider());
    }

    private static final Logger logger = LoggerFactory.getLogger(OpcUaSourceTask.class);
    private Duration publicationRate;
    private URI connectionUri;
    private String userName;
    private String password;
    private String clientIdUri;
    private X509Credentials channelEncryption;
    private X509Credentials clientCredentials;
    private Duration socketTimeout;


    private X509Certificate decodePemCertificate(String certificate) {
        try {
            return (X509Certificate) CertificateFactory.getInstance("X.509")
                    .generateCertificate(new ByteArrayInputStream(certificate.getBytes("UTF8")));

        } catch (Exception e) {
            throw new IllegalArgumentException("Unable to decode certificate", e);
        }
    }

    private PrivateKey decodePemPrivateKey(String privateKey) {
        try {
            PEMParser pemParser = new PEMParser(new StringReader(privateKey));
            Object object = pemParser.readObject();
            JcaPEMKeyConverter converter = new JcaPEMKeyConverter().setProvider("BC");
            if (object instanceof PEMEncryptedKeyPair) {
                throw new UnsupportedOperationException("Encrypted keys are not yet supported");
            }
            PEMKeyPair ukp = (PEMKeyPair) object;
            KeyPair keyPair = converter.getKeyPair(ukp);
            return keyPair.getPrivate();
        } catch (Exception e) {
            throw new IllegalArgumentException("Unable to decode private key", e);
        }
    }

    private X509Credentials decodeCredentials(String certificate, String key) {
        return new X509Credentials()
                .withPrivateKey(decodePemPrivateKey(key))
                .withCertificate(decodePemCertificate(certificate));
    }


    @Override
    protected void setConfigurationProperties(Map<String, String> properties) {
        connectionUri = URI.create(properties.get(CommonDefinitions.PROPERTY_SERVER_URI));
        clientIdUri = properties.get(OpcUaSourceConnector.PROPERTY_CLIENT_URI);
        if (properties.containsKey(CommonDefinitions.PROPERTY_CONNECTION_SOCKET_TIMEOUT)) {
            socketTimeout = Duration.ofMillis(Long.parseLong(properties.get(CommonDefinitions.PROPERTY_CONNECTION_SOCKET_TIMEOUT)));
        }

        userName = properties.get(OpcUaSourceConnector.PROPERTY_AUTH_BASIC_USER);
        password = properties.get(OpcUaSourceConnector.PROPERTY_AUTH_BASIC_PASSWORD);
        if (properties.containsKey(OpcUaSourceConnector.PROPERTY_AUTH_X509_CERTIFICATE) &&
                properties.containsKey(OpcUaSourceConnector.PROPERTY_AUTH_X509_PRIVATE_KEY)) {
            clientCredentials = (decodeCredentials(properties.get(OpcUaSourceConnector.PROPERTY_AUTH_X509_CERTIFICATE),
                    properties.get(OpcUaSourceConnector.PROPERTY_AUTH_X509_PRIVATE_KEY)));
        }

        if (properties.containsKey(OpcUaSourceConnector.PROPERTY_CHANNEL_CERTIFICATE) &&
                properties.containsKey(OpcUaSourceConnector.PROPERTY_CHANNEL_PRIVATE_KEY)) {
            channelEncryption = decodeCredentials(properties.get(OpcUaSourceConnector.PROPERTY_CHANNEL_CERTIFICATE),
                    properties.get(OpcUaSourceConnector.PROPERTY_CHANNEL_PRIVATE_KEY));
        }
        publicationRate = Duration.parse(properties.get(OpcUaSourceConnector.PROPERTY_DATA_PUBLICATION_RATE));
    }

    @Override
    protected SessionProfile createSessionProfile() {
        return new OpcUaSessionProfile()
                .withDefaultPublicationInterval(publicationRate);
    }

    @Override
    protected ConnectionProfile createConnectionProfile() {
        OpcUaConnectionProfile ret = new OpcUaConnectionProfile()
                .withConnectionUri(connectionUri);
        Credentials credentials = Credentials.ANONYMOUS_CREDENTIALS;
        if (userName != null && password != null) {
            credentials = new UsernamePasswordCredentials()
                    .withPassword(password)
                    .withUser(userName);
        } else if (clientCredentials != null) {
            credentials = new X509Credentials().withCertificate(clientCredentials.getCertificate())
                    .withPrivateKey(clientCredentials.getPrivateKey());
        }
        ret.setCredentials(credentials);
        if (channelEncryption != null) {
            ret.setSecureChannelEncryption(channelEncryption);
        }
        if (socketTimeout != null) {
            ret.setSocketTimeout(socketTimeout);
        }
        if (clientIdUri != null) {
            ret.setClientIdUri(clientIdUri);
        }
        return ret;
    }

    @Override
    protected OpcOperations createOpcOperations() {
        return new OpcUaTemplate();
    }
}