/**
 * Copyright (C) 2016 Hurence (support@hurence.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hurence.logisland.config.v2;


/**
 * Yaml definition of the Logisland service config
 *
 *
 * - controllerService: hbase_service
 *   component: com.hurence.logisland.service.hbase.HBase_1_1_2_ClientService
 *   type: service
 *   documentation: a processor that links
 *   configuration:
 *     hadoop.configuration.files: conf/
 *     zookeeper.quorum: sandbox
 *     zookeeper.client.port: 2181
 *     zookeeper.znode.parent:
 *     hbase.client.retries: 3
 *     phoenix.client.jar.location:
 *
 *
 */
public class ServiceConfig extends AbstractComponentConfig {

    private String service = "";

    public String getService() {
        return service;
    }

    public void setService(String service) {
        this.service = service;
    }


    @Override
    public String toString() {
        return "ServiceConfig{" +
                "service='" + service + '\'' +
                '}';
    }
}
