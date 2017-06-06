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
package com.hurence.logisland.agent.rest.api.factories;

import com.hurence.logisland.agent.rest.api.ConfigsApiService;
import com.hurence.logisland.agent.rest.api.impl.ConfigsApiServiceImpl;

import com.hurence.logisland.kafka.registry.KafkaRegistry;

@javax.annotation.Generated(value = "class io.swagger.codegen.languages.JavaJerseyServerCodegen", date = "2017-02-28T17:12:21.474+01:00")
public class ConfigsApiServiceFactory {
    private static ConfigsApiService service = null;

    public static ConfigsApiService getConfigsApi(KafkaRegistry kafkaRegistry) {
        if (service == null) {
            service = new ConfigsApiServiceImpl(kafkaRegistry);
        }
        return service;
    }
}