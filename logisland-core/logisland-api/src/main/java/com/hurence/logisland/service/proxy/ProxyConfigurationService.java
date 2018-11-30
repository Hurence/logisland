/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hurence.logisland.service.proxy;

import com.hurence.logisland.component.PropertyDescriptor;
import com.hurence.logisland.controller.ControllerService;

/**
 * Provides configurations to access a Proxy server.
 *
 *
 * @see <a href="https://github.com/apache/nifi/blob/master/nifi-nar-bundles/nifi-standard-services/nifi-proxy-configuration-api/src/main/java/org/apache/nifi/proxy/ProxyConfigurationService.java">
 *      interface inspired from ProxyConfigurationService nifi
 *     </a>
 */
public interface ProxyConfigurationService extends ControllerService {

    /**
     * Returns proxy configurations.
     * Implementations should return a non-null ProxyConfiguration instance which returns DIRECT proxy type instead of returning null,
     * when underlying configuration or initialization is not done yet.
     * @return A ProxyConfiguration instance.
     */
    ProxyConfiguration getConfiguration();

}
