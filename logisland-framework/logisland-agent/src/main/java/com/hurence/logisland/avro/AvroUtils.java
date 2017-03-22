
package com.hurence.logisland.avro;

import org.apache.avro.Schema;
import org.apache.avro.SchemaParseException;

public class AvroUtils {

  /**
   * Convert a schema string into a schema object and a canonical schema string.
   *
   * @return A schema object and a canonical representation of the schema string. Return null if
   * there is any parsing error.
   */
  public static AvroSchema parseSchema(String schemaString) {
    try {
      Schema.Parser parser1 = new Schema.Parser();
      Schema schema = parser1.parse(schemaString);
      //TODO: schema.toString() is not canonical (issue-28)
      return new AvroSchema(schema, schema.toString());
    } catch (SchemaParseException e) {
      return null;
    }
  }
}
