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
package com.hurence.logisland.connect.opc.da;

import com.hurence.logisland.connect.opc.CommonDefinitions;
import com.hurence.logisland.connect.opc.CommonOpcSourceTask;
import com.hurence.opc.ConnectionProfile;
import com.hurence.opc.OpcOperations;
import com.hurence.opc.SessionProfile;
import com.hurence.opc.auth.NtlmCredentials;
import com.hurence.opc.da.OpcDaConnectionProfile;
import com.hurence.opc.da.OpcDaSessionProfile;
import com.hurence.opc.da.OpcDaTemplate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

/**
 * OPC-DA Worker task.
 */
public class OpcDaSourceTask extends CommonOpcSourceTask {

    private static final Logger logger = LoggerFactory.getLogger(OpcDaSourceTask.class);
    private URI connectionUri;
    private boolean directRead;
    private String clsId;
    private String progId;
    private String user;
    private String password;
    private String domain;
    private long groupRefreshRateMillis;
    private Duration socketTimeout;

    private final Map<String, Short> dataTypeOverride = new HashMap<>();


    @Override
    protected void setConfigurationProperties(Map<String, String> properties) {
        connectionUri = URI.create(properties.get(CommonDefinitions.PROPERTY_SERVER_URI));
        clsId = properties.get(OpcDaSourceConnector.PROPERTY_SERVER_CLSID);
        progId = properties.get(OpcDaSourceConnector.PROPERTY_SERVER_PROGID);
        domain = properties.get(OpcDaSourceConnector.PROPERTY_AUTH_NTLM_DOMAIN);
        user = properties.get(OpcDaSourceConnector.PROPERTY_AUTH_NTLM_USER);
        password = properties.get(OpcDaSourceConnector.PROPERTY_AUTH_NTLM_PASSWORD);
        directRead = Boolean.parseBoolean(properties.get(OpcDaSourceConnector.PROPERTY_SESSION_DIRECT_READ));
        groupRefreshRateMillis = Long.parseLong(properties.get(OpcDaSourceConnector.PROPERTY_SESSION_REFRESH_PERIOD));
        if (properties.containsKey(CommonDefinitions.PROPERTY_CONNECTION_SOCKET_TIMEOUT)) {
            socketTimeout = Duration.ofMillis(Long.parseLong(properties.get(CommonDefinitions.PROPERTY_CONNECTION_SOCKET_TIMEOUT)));
        }
        if (properties.containsKey(OpcDaSourceConnector.PROPERTY_TAGS_DATA_TYPE_OVERRIDE)) {
            String[] tags = properties.get(CommonDefinitions.PROPERTY_TAGS_ID).split(",");
            String[] types = properties.get(OpcDaSourceConnector.PROPERTY_TAGS_DATA_TYPE_OVERRIDE).split(",");
            for (int i = 0; i < tags.length; i++) {
                dataTypeOverride.put(tags[i], Short.parseShort(types[i]));
            }
        }

    }

    @Override
    protected SessionProfile createSessionProfile() {
        final OpcDaSessionProfile ret = new OpcDaSessionProfile()
                .withDirectRead(directRead)
                .withRefreshInterval(Duration.ofMillis(groupRefreshRateMillis));
        dataTypeOverride.forEach((k, v) -> ret.withDataTypeForTag(k, v));
        return ret;

    }

    @Override
    protected ConnectionProfile createConnectionProfile() {
        OpcDaConnectionProfile ret = new OpcDaConnectionProfile()
                .withComClsId(clsId)
                .withComProgId(progId)
                .withConnectionUri(connectionUri)
                .withCredentials(new NtlmCredentials()
                        .withDomain(domain)
                        .withUser(user)
                        .withPassword(password));

        if (socketTimeout != null) {
            ret.setSocketTimeout(socketTimeout);
        }
        return ret;

    }

    @Override
    protected OpcOperations createOpcOperations() {
        return new OpcDaTemplate();
    }


}
