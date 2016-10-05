package com.hurence.logisland.util.avro.eventgenerator;
/*
 *
 * Copyright 2013 LinkedIn Corp. All rights reserved
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
*/


import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.io.JsonEncoder;
import org.apache.commons.cli.*;

import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;


public class DataGenerator {

    public final static String MODULE = DataGenerator.class.getName();
    private static Logger logger = LoggerFactory.getLogger(DataGenerator.class);
    private final static String PRINT_AVRO_JSON_OPTNAME = "printAvroJson";
    Schema schema;

    /*
     * Takes a schema file as input
     */
    public DataGenerator(File schemaFile) throws IOException {
        Schema.Parser parser = new Schema.Parser();
        schema = parser.parse(schemaFile);
    }

    /*
     * Takes a schema string as an input
     */
    public DataGenerator(String schema) {
        Schema.Parser parser = new Schema.Parser();
        this.schema = parser.parse(schema);
    }


    /*
     * Generate random based on the avro schema
     * The schema must be of a record type to work
     *
     * @return returns the randomly generated record
     */
    public GenericRecord generateRandomRecord() throws UnknownTypeException {

        if (schema.getType() != Schema.Type.RECORD) {
            logger.error("The schema first level must be record.");
            return null;
        }

        GenericRecord record = new GenericData.Record(schema);
        for (Field field : schema.getFields()) {
            SchemaFiller schemaFill = SchemaFiller.createRandomField(field);
            schemaFill.writeToRecord(record);
        }
        return record;
    }


    static public void prettyPrint(GenericRecord record) {
        try {
            logger.info(new JSONObject(record.toString()).toString(2));
        } catch (JSONException e) {
            logger.error("Unable to parser json: The Json created by the generator is not valid!");
            e.printStackTrace();
        }
    }

    public static Options loadOptions() {
        Options opt = new Options();
        opt.addOption("s", "schemaLocation", true, "location of the schema");
        opt.addOption("minStringLength", true, "Minimum length of string to be generated");
        opt.addOption("maxStringLength", true, "Maximum length of string to be generated");
        opt.addOption("minIntegerRange", true, "Start range of integer");
        opt.addOption("maxIntegerRange", true, "End range of integer");
        opt.addOption("minLongRange", true, "Start range of long");
        opt.addOption("maxLongRange", true, "End range of long");
        opt.addOption("maxBytesLength", true, "Maximum length of the bytes to be generated");
        opt.addOption(PRINT_AVRO_JSON_OPTNAME, true, "Replace the default human-readable JSON serialization with" +
                " the standard Avro JSON serialization of the record which can be deserialized back to a " +
                " record. The result is printed out to a file or to the standard output (-).");

        return opt;
    }

    public static void printHelp(Options opts) {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("Generate a record with random data given an Avro schema", opts);
    }

    public static void main(String[] args) throws IOException, UnknownTypeException {

        // load and verify the options
        CommandLineParser parser = new PosixParser();
        Options opts = loadOptions();

        CommandLine cmd = null;
        try {
            cmd = parser.parse(opts, args);
        } catch (org.apache.commons.cli.ParseException parseEx) {
            logger.error("Invalid option");
            printHelp(opts);
            return;
        }

        // check for necessary options
        String fileLoc = cmd.getOptionValue("schemaLocation");
        if (fileLoc == null) {
            logger.error("schemaLocation not specified");
            printHelp(opts);
        }

        //getField string length and check if min is greater than 0

        // Generate the record
        File schemaFile = new File(fileLoc);
        DataGenerator dataGenerator = new DataGenerator(schemaFile);
        GenericRecord record = dataGenerator.generateRandomRecord();
        if (cmd.hasOption(PRINT_AVRO_JSON_OPTNAME)) {
            String outname = cmd.getOptionValue(PRINT_AVRO_JSON_OPTNAME);
            OutputStream outs = System.out;
            if (!outname.equals("-")) {
                outs = new FileOutputStream(outname);
            }
            printAvroJson(record, outs);
            if (!outname.equals("-")) {
                outs.close();
            }
        } else {
            DataGenerator.prettyPrint(record);
        }

    }

    private static void printAvroJson(GenericRecord record, OutputStream outs) throws IOException {
        JsonEncoder jsonEnc = EncoderFactory.get().jsonEncoder(record.getSchema(), outs); //new JsonEncoder(record.getSchema(), outs);
        GenericDatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(record.getSchema());
        datumWriter.write(record, jsonEnc);
        jsonEnc.flush();
    }

    // TODO add thread based generator here

}
