/**
 * Copyright (C) 2021 Hurence (support@hurence.com)
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
package com.hurence.logisland.stream.spark.structured.provider

import com.databricks.spark.avro.SchemaConverters

import java.util
import java.util.Collections
import com.hurence.logisland.annotation.documentation.CapabilityDescription
import com.hurence.logisland.annotation.lifecycle.OnEnabled
import com.hurence.logisland.component.{InitializationException, PropertyDescriptor}
import com.hurence.logisland.controller.{AbstractControllerService, ControllerServiceInitializationContext}
import com.hurence.logisland.record.{Field, Record}
import com.hurence.logisland.stream.spark.structured.provider.ElasticSearchStructuredStreamSinkProviderService._
import com.hurence.logisland.stream.spark.structured.provider.StructuredStreamProviderServiceWriter.{APPEND_MODE, COMPLETE_MODE, UPDATE_MODE}
import com.hurence.logisland.util.spark.ControllerServiceLookupSink
import com.hurence.logisland.validator.StandardValidators
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.{Dataset, Row, RowFactory}

import scala.collection.JavaConversions._
import org.apache.avro.Schema

/**
 * Service to allow writing to elasticsearch with structured streams
 */
@CapabilityDescription("Provides a ways to use elasticsearch as output in StructuredStream streams")
class ElasticSearchStructuredStreamSinkProviderService extends AbstractControllerService
  with StructuredStreamProviderServiceWriter {

  var esNodes : String = _
  var indexName : String = _
  var esOptions : Map[String, String] = Map[String, String]()
  var outputSchema : StructType = _
  var outputMode: String = _

  @OnEnabled
  @throws[InitializationException]
  override def init(context: ControllerServiceInitializationContext): Unit = {
    super.init(context)
    this.synchronized {
      try {
        // Es nodes (check this mandatory ES property (es.nodes)
        if (!context.getPropertyValue(ES_NODES).isSet) {
          throw new InitializationException(ES_NODES.getName + " not specified.")
        }
        esNodes = context.getPropertyValue(ES_NODES).asString()

        // Index name (check this mandatory ES property (es.resource)
        if (!context.getPropertyValue(ES_RESOURCE).isSet) {
          throw new InitializationException(ES_RESOURCE.getName + " not specified.")
        }
        indexName = context.getPropertyValue(ES_RESOURCE).asString()

        // Output schema
        if (!context.getPropertyValue(OUTPUT_SCHEMA).isSet) {
          throw new InitializationException(OUTPUT_SCHEMA.getName + " not specified.")
        }
        outputSchema = parseSchema(context)

        // Output mode
        outputMode = context.getPropertyValue(OUTPUT_MODE).asString()

        handleEsDynamicProperties(context.getProperties)
      } catch {
        case e: Exception =>
          throw new InitializationException(e)
      }
    }
  }

  /**
   * Converts the possed avro schema string into a spark scql schema
   * @param avroSchemaString
   * @return
   */
  @throws[InitializationException]
  private def parseSchema(context: ControllerServiceInitializationContext) : StructType = {

    // Load and parse avro schema string
    val avroSchemaString : String = context.getPropertyValue(OUTPUT_SCHEMA).asString()
    var schema : Schema = null
    val parser: Schema.Parser = new Schema.Parser
    try schema = parser.parse(avroSchemaString)
    catch {
      case e: Exception =>
        throw new InitializationException("Avro schema parsing error: " + e.getMessage + ": " + e.toString)
    }

    // Convert avro schema into spark SQL schema (StructType)
    var structType : StructType = new StructType()
    for (field <- schema.getFields) {
      structType = structType.add(field.name, SchemaConverters.toSqlType(field.schema).dataType)
    }
    structType
  }

  /**
   * Allows subclasses to register which property descriptor objects are
   * supported.
   *
   * @return PropertyDescriptor objects this processor currently supports
   */
  override def getSupportedPropertyDescriptors() = {
    val descriptors = new util.ArrayList[PropertyDescriptor]
    descriptors.add(ES_NODES)
    descriptors.add(ES_RESOURCE)
    descriptors.add(OUTPUT_SCHEMA)
    descriptors.add(OUTPUT_MODE)
    Collections.unmodifiableList(descriptors)
  }

  override def getSupportedDynamicPropertyDescriptor(propertyDescriptorName: String): PropertyDescriptor = {

    // Support any custom es configuration described here:
    // Inspired from https://www.elastic.co/guide/en/elasticsearch/hadoop/current/configuration.html
    if (propertyDescriptorName.startsWith(ES_CONFIG_PREFIX)) {
      return new PropertyDescriptor.Builder()
        .name(propertyDescriptorName)
        .expressionLanguageSupported(false)
        .required(false)
        .dynamic(true)
        .build
    }
    return null
  }

  /**
   * Configure the ES write stream
   *
   * @return DataFrame currently loaded
   */
  override def write(ds: Dataset[Record], controllerServiceLookupSink: Broadcast[ControllerServiceLookupSink]) = {

    getLogger.info(s"Will write stream to elasticsearch $esNodes in " +
      s"$indexName index")

    // Transform the dataset of Records into a dataset of Rows with the loaded
    // schema
    var rowDs : Dataset[Row] = ds.map( (record:Record) => {
      val values = outputSchema.map( (structField : StructField) => {
        val field : Field = record.getField(structField.name)
        var value : Object = null
        if (field != null) {
          value = field.getRawValue
        } else {
          // Field si absent from , set null value
        }
        value
      })

      RowFactory.create(values:_*)
    })(RowEncoder(outputSchema))

    rowDs.writeStream
      .format(ES_FORMAT)
      .outputMode(outputMode)
      // Apply all es.* dynamic options
      .options(esOptions)
  }

  // Any es.* property described at https://www.elastic.co/guide/en/elasticsearch/hadoop/current/configuration.html
  // is handled here
  protected def handleEsDynamicProperties(properties : java.util.Map[PropertyDescriptor, String]) = {

    properties.foreach(propertyAndValue => {

      val propertyDescriptor: PropertyDescriptor = propertyAndValue._1
      val esConfigKey: String = propertyDescriptor.getName
      // Handle any dynamic property of the form 'es.*: someValue' properties
      // which will be passed as DataStreamWriter option
      if (esConfigKey.startsWith(ES_CONFIG_PREFIX)) {
        val customSparkConfigKey: String = esConfigKey.substring(ES_CONFIG_PREFIX.length)
        if (customSparkConfigKey.length > 0) { // Ignore silly 'es.: missing_custom_key' property
          val esConfigValue: String = propertyAndValue._2
          esOptions += (esConfigKey -> esConfigValue)
        }
      }
    })
  }
}

object ElasticSearchStructuredStreamSinkProviderService {

  val ES_FORMAT : String = "org.elasticsearch.spark.sql"
  val ES_CONFIG_PREFIX = "es."

  // All ES properties are documented here:
  // https://www.elastic.co/guide/en/elasticsearch/hadoop/current/configuration.html

  val ES_NODES: PropertyDescriptor = new PropertyDescriptor.Builder()
    .name("es.nodes")
    .description("A comma-separated list of ElasticSearch hosts (host[:port],host2[:port2], etc.)")
    .required(true)
    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
    .build

  val ES_RESOURCE: PropertyDescriptor = new PropertyDescriptor.Builder()
    .name("es.resource") // Could also specify .option("path", indexName) instead
    .description("The index to write data to. It is of the form: indexName[/typeName]")
    .required(true)
    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
    .build

  val OUTPUT_SCHEMA: PropertyDescriptor = new PropertyDescriptor.Builder()
    .name("output.schema")
    .description("The output avro schema definition")
    .required(true)
    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
    .build

  // Redefine here as we want the default to be APPEND_MODE, not UPDATE_MODE
  val OUTPUT_MODE: PropertyDescriptor = new PropertyDescriptor.Builder()
    .name("output.mode")
    .description("Output mode for the streaming sink.")
    .defaultValue(APPEND_MODE)
    .allowableValues(APPEND_MODE, COMPLETE_MODE, UPDATE_MODE)
    .required(false)
    .build
}
