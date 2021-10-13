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
package com.hurence.logisland.connect.spooldir;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;

import java.util.Map;

/**
 * "This connector is used to [stream](https://en.wikipedia.org/wiki/JSON_Streaming) JSON files from a directory " +
 *     "while converting the data based on the schema supplied in the configuration."
 */

public class SpoolDirJsonSourceConnector extends SpoolDirSourceConnector<SpoolDirJsonSourceConnectorConfig> {

  @Override
  protected SpoolDirJsonSourceConnectorConfig config(Map<String, String> settings) {
    return new SpoolDirJsonSourceConnectorConfig(false, settings);
  }

  @Override
  protected SchemaGenerator<SpoolDirJsonSourceConnectorConfig> generator(Map<String, String> settings) {
    return new JsonSchemaGenerator(settings);
  }

  @Override
  public Class<? extends Task> taskClass() {
    return SpoolDirJsonSourceTask.class;
  }

  @Override
  public ConfigDef config() {
    return SpoolDirJsonSourceConnectorConfig.config();
  }
}
