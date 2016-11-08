/**
 * Copyright (C) 2016 Hurence (bailet.thomas@gmail.com)
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hurence.logisland.util.record;

import com.hurence.logisland.processor.ProcessError;
import com.hurence.logisland.record.*;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;

import java.util.List;
import java.util.stream.Collectors;

import static org.apache.avro.Schema.Type.INT;

public class RecordSchemaUtil {


    public static Schema generateSchema(Record inputRecord) {

        SchemaBuilder.FieldAssembler<Schema> fields;
        SchemaBuilder.RecordBuilder<Schema> record = SchemaBuilder.record(inputRecord.getType());
        fields = record.namespace("com.hurence.logisland.record").fields();
        for (final Field field : inputRecord.getAllFieldsSorted()) {

            switch (field.getType()) {
                case STRING:
                    fields = fields.name(field.getName()).type().nullable().stringType().noDefault();
                    break;
                case INT:
                    fields = fields.name(field.getName()).type().nullable().intType().noDefault();
                    break;
                case LONG:
                    fields = fields.name(field.getName()).type().nullable().longType().noDefault();
                    break;
                case FLOAT:
                    fields = fields.name(field.getName()).type().nullable().floatType().noDefault();
                    break;
                case DOUBLE:
                    fields = fields.name(field.getName()).type().nullable().doubleType().noDefault();
                    break;
                case BOOLEAN:
                    fields = fields.name(field.getName()).type().nullable().booleanType().noDefault();
                    break;
                case ARRAY:
                    fields = fields.name(field.getName()).type().nullable().array().items().stringType().noDefault();
                    break;
                default:
                    fields = fields.name(field.getName()).type().nullable().stringType().noDefault();
                    break;
            }
        }

        Schema schema = fields.endRecord();
        return schema;
    }

    public static String generateTestCase(Record inputRecord) {
        StringBuilder builder = new StringBuilder();
        for (final Field field : inputRecord.getAllFieldsSorted()) {
            if (field.getName().equals(FieldDictionary.RECORD_ID))
                continue;

            builder.append("out.assertFieldEquals(\"");
            builder.append(field.getName());

            switch (field.getType()) {
                case INT:
                    builder.append("\", ");
                    builder.append(field.asInteger());
                    break;
                case LONG:
                    builder.append("\", ");
                    builder.append(field.asLong());
                    builder.append("L");
                    break;
                case FLOAT:
                    builder.append("\", ");
                    builder.append(field.asFloat());
                    builder.append("f");
                    break;
                case DOUBLE:
                    builder.append("\", ");
                    builder.append(field.asDouble());
                    builder.append("d");
                    break;
                case BOOLEAN:
                    builder.append("\", ");
                    builder.append(field.asBoolean());
                    break;
                default:
                    builder.append("\", \"");
                    builder.append(field.asString());
                    builder.append("\"");
                    break;
            }
            builder.append(");\n");
        }
        builder.append("out.assertRecordSizeEquals(").append(inputRecord.size()).append(");\n");

        return builder.toString();
    }

    public static synchronized Record convertToValidRecord(final Record inputRecord, final Schema schema) {
        final Record outputRecord = new StandardRecord(inputRecord.getType());

        int size = schema.getFields().size();

        schema.getFields().forEach(avroField -> {
            String avroFieldName = avroField.name();
            List<Schema.Type> avroTypes = avroField.schema().getTypes().stream()
                    .map(Schema::getType)
                    .collect(Collectors.toList());

            if (inputRecord.hasField(avroFieldName)) {
                Field logislandField = inputRecord.getField(avroFieldName);

                if (avroTypes.contains(Schema.Type.valueOf(logislandField.getType().toString().toUpperCase()))) {
                    outputRecord.setField(inputRecord.getField(avroFieldName));
                } else {
                    // conversion needed
                    avroTypes.remove(Schema.Type.NULL);
                    if (avroTypes.size() != 0) {

                        switch( avroTypes.get(0)){
                            case INT:
                                try{
                                    outputRecord.setField(avroFieldName, FieldType.INT, logislandField.asInteger());
                                }catch (Exception ex){
                                    outputRecord.addError(ProcessError.RECORD_CONVERSION_ERROR.getName(),
                                            "field " + avroFieldName +
                                                    " could be casted to " + avroTypes.get(0).getName());
                                }
                                break;

                            case LONG:
                                try{
                                    outputRecord.setField(avroFieldName, FieldType.LONG, logislandField.asLong());
                                }catch (Exception ex){
                                    outputRecord.addError(ProcessError.RECORD_CONVERSION_ERROR.getName(),
                                            "field " + avroFieldName +
                                                    " could be casted to " + avroTypes.get(0).getName());
                                }
                                break;

                            case FLOAT:
                                try{
                                    outputRecord.setField(avroFieldName, FieldType.FLOAT, logislandField.asFloat());
                                }catch (Exception ex){
                                    outputRecord.addError(ProcessError.RECORD_CONVERSION_ERROR.getName(),
                                            "field " + avroFieldName +
                                                    " could be casted to " + avroTypes.get(0).getName());
                                }
                                break;

                            case DOUBLE:
                                try{
                                    outputRecord.setField(avroFieldName, FieldType.DOUBLE, logislandField.asDouble());
                                }catch (Exception ex){
                                    outputRecord.addError(ProcessError.RECORD_CONVERSION_ERROR.getName(),
                                            "field " + avroFieldName +
                                                    " could be casted to " + avroTypes.get(0).getName());
                                }
                                break;

                            default:

                                    outputRecord.addError(ProcessError.RECORD_CONVERSION_ERROR.getName(),
                                            "field " + avroFieldName +
                                                    " unhandled type " + avroTypes.get(0).getName());

                                break;
                        }
                    } else {
                        outputRecord.addError(ProcessError.RECORD_CONVERSION_ERROR.getName(),
                                "field " + avroFieldName +
                                        " should be of type null but is of type " + logislandField.getType().getName());
                    }

                }
            } else if (!avroTypes.contains(Schema.Type.NULL)) {
                outputRecord.addError(ProcessError.RECORD_CONVERSION_ERROR.getName(),
                        "field " + avroFieldName + " cannot be null");
            }
        });


        return outputRecord;

    }
}
