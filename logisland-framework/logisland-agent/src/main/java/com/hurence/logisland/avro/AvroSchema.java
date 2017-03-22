package com.hurence.logisland.avro;

import org.apache.avro.Schema;

public class AvroSchema {

  public final Schema schemaObj;
  public final String canonicalString;

  public AvroSchema(Schema schemaObj, String canonicalString) {
    this.schemaObj = schemaObj;
    this.canonicalString = canonicalString;
  }
}
