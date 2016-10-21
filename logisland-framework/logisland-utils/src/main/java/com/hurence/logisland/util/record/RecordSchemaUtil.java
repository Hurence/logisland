/**
 * Copyright (C) 2016 Hurence (bailet.thomas@gmail.com)
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
package com.hurence.logisland.util.record;

import com.hurence.logisland.record.Field;
import com.hurence.logisland.record.FieldDictionary;
import com.hurence.logisland.record.Record;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;

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
            if(field.getName().equals(FieldDictionary.RECORD_ID))
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
}
