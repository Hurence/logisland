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

import com.github.jcustenborder.kafka.connect.utils.VersionUtil;
import com.github.jcustenborder.kafka.connect.utils.jackson.ObjectMapperFactory;
import com.google.common.base.Preconditions;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.io.PatternFilenameFilter;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.source.SourceConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public abstract class SpoolDirSourceConnector<CONF extends SpoolDirSourceConnectorConfig> extends SourceConnector {
  private static Logger log = LoggerFactory.getLogger(SpoolDirSourceConnector.class);
  protected Map<String, String> settings;
  private CONF config;

  protected abstract CONF config(Map<String, String> settings);

  protected abstract SchemaGenerator<CONF> generator(Map<String, String> settings);

  @Override
  public String version() {
    return VersionUtil.version(this.getClass());
  }

  @Override
  public void start(final Map<String, String> input) {
    this.config = config(input);
    final Map<String, String> settings = new LinkedHashMap<>(input);
    //setup schema for key and value if needed
    if (null == this.config.valueSchema || null == this.config.keySchema) {
      log.info("Key or Value schema was not defined. Running schema generator.");
      SchemaGenerator<CONF> generator = generator(settings);

      try {
        /*List<File> inputFiles = Arrays.stream(this.config.inputPath.listFiles(this.config.inputFilenameFilter))
            .limit(5)
            .collect(Collectors.toList());
        */


        List<File> inputFiles = Files.find(Paths.get(this.config.inputPath.getAbsolutePath()),
                10,
                (filePath, fileAttr) -> fileAttr.isRegularFile() /*&&
                        !filePath.toUri().getPath().contains(".PROCESSING")*/ &&
                        config.inputFilenameFilter.accept(filePath.getParent().toFile(),filePath.getFileName().toString() ))
                .limit(3)
                .map(Path::toFile)
                .collect(Collectors.toList());


        Preconditions.checkState(
            !inputFiles.isEmpty(),
            "Could not find any input file(s) to infer schema from."
        );

        Map<String, Map.Entry<Schema, Schema>> schemas = new HashMap<>();
        Multimap<String, File> schemaToFiles = HashMultimap.create();

        for (File inputFile : inputFiles) {
          Map.Entry<Schema, Schema> schemaEntry = generator.generate(inputFile, this.config.keyFields);
          String schema = ObjectMapperFactory.INSTANCE.writeValueAsString(schemaEntry.getValue());
          schemaToFiles.put(schema, inputFile);
          schemas.put(schema, schemaEntry);
        }

        throwDataExceptionIfMoreThanOneSchemaFound(schemaToFiles.asMap());

        Map.Entry<Schema, Schema> schemaPair = null;
        for (Map.Entry<Schema, Schema> s : schemas.values()) {
          schemaPair = s;
          break;
        }

        if (null == schemaPair) {
          throw new DataException("Schema could not be generated.");
        }

        final String keySchema = ObjectMapperFactory.INSTANCE.writeValueAsString(schemaPair.getKey());
        log.info("Setting {} to {}", SpoolDirSourceConnectorConfig.KEY_SCHEMA_CONF, keySchema);
        final String valueSchema = ObjectMapperFactory.INSTANCE.writeValueAsString(schemaPair.getValue());
        log.info("Setting {} to {}", SpoolDirSourceConnectorConfig.VALUE_SCHEMA_CONF, valueSchema);
        settings.put(SpoolDirSourceConnectorConfig.KEY_SCHEMA_CONF, keySchema);
        settings.put(SpoolDirSourceConnectorConfig.VALUE_SCHEMA_CONF, valueSchema);
      } catch (IOException e) {
        throw new ConnectException("Exception thrown while generating schema", e);
      }
      this.settings = settings;
    }

    this.settings = settings;
  }

  private void throwDataExceptionIfMoreThanOneSchemaFound(Map<String, Collection<File>> schemaToFilesMap) {
    if (1 != schemaToFilesMap.keySet().size()) {
      StringBuilder builder = new StringBuilder();
      builder.append("More than one schema was found for the input pattern.\n");
      for (String schema : schemaToFilesMap.keySet()) {
        builder.append("Schema: ");
        builder.append(schema);
        builder.append("\n");

        for (File f : schemaToFilesMap.get(schema)) {
          builder.append("  ");
          builder.append(f);
          builder.append("\n");
        }
      }

      throw new DataException(builder.toString());
    }
  }

  @Override
  public List<Map<String, String>> taskConfigs(int i) {
    return Collections.nCopies(i, this.settings);
  }

  @Override
  public void stop() {

  }
}
