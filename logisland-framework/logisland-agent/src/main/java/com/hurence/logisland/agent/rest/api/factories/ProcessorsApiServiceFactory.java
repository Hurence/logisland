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

import com.hurence.logisland.agent.rest.api.ProcessorsApiService;
import com.hurence.logisland.agent.rest.api.impl.ProcessorsApiServiceImpl;

import com.hurence.logisland.kafka.registry.KafkaRegistry;

@javax.annotation.Generated(value = "class io.swagger.codegen.languages.JavaJerseyServerCodegen", date = "2017-03-23T11:55:20.570+01:00")
public class ProcessorsApiServiceFactory {
    private static ProcessorsApiService service = null;

    public static ProcessorsApiService getProcessorsApi(KafkaRegistry kafkaRegistry) {
        if (service == null) {
            service = new ProcessorsApiServiceImpl(kafkaRegistry);
        }
        return service;
    }
}