package com.hurence.logisland.hbase;


import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;

import com.hurence.logisland.annotation.documentation.*;
import com.hurence.logisland.component.AllowableValue;
import com.hurence.logisland.component.PropertyDescriptor;
import com.hurence.logisland.hbase.put.PutColumn;
import com.hurence.logisland.hbase.put.PutRecord;
import com.hurence.logisland.processor.ProcessContext;
import com.hurence.logisland.processor.ProcessException;
import com.hurence.logisland.record.Record;
import com.hurence.logisland.serializer.RecordSerializer;
import com.hurence.logisland.validator.StandardValidators;
import com.hurence.logisland.validator.ValidationContext;
import com.hurence.logisland.validator.ValidationResult;
import org.apache.commons.lang3.StringUtils;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Tags({"hadoop", "hbase", "put", "json"})
@CapabilityDescription("Adds rows to HBase based on the contents of incoming JSON documents. Each Record must contain a single " +
        "UTF-8 encoded JSON document, and any Records where the root element is not a single document will be routed to failure. " +
        "Each JSON field name and value will become a column qualifier and value of the HBase row. Any fields with a null value " +
        "will be skipped, and fields with a complex value will be handled according to the Complex Field Strategy. " +
        "The row id can be specified either directly on the processor through the Row Identifier property, or can be extracted from the JSON " +
        "document by specifying the Row Identifier Field Name property. This processor will hold the contents of all Records for the given batch " +
        "in memory at one time.")
public class PutHBaseJSON extends AbstractPutHBase {
    
    private static Logger logger = LoggerFactory.getLogger(PutHBaseJSON.class); 

    protected static final PropertyDescriptor ROW_FIELD_NAME = new PropertyDescriptor.Builder()
            .name("Row Identifier Field Name")
            .description("Specifies the name of a JSON element whose value should be used as the row id for the given JSON document.")
            .expressionLanguageSupported(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    protected static final String FAIL_VALUE = "Fail";
    protected static final String WARN_VALUE = "Warn";
    protected static final String IGNORE_VALUE = "Ignore";
    protected static final String TEXT_VALUE = "Text";

    protected static final AllowableValue COMPLEX_FIELD_FAIL = new AllowableValue(FAIL_VALUE, FAIL_VALUE, "Route entire Record to failure if any elements contain complex values.");
    protected static final AllowableValue COMPLEX_FIELD_WARN = new AllowableValue(WARN_VALUE, WARN_VALUE, "Provide a warning and do not include field in row sent to HBase.");
    protected static final AllowableValue COMPLEX_FIELD_IGNORE = new AllowableValue(IGNORE_VALUE, IGNORE_VALUE, "Silently ignore and do not include in row sent to HBase.");
    protected static final AllowableValue COMPLEX_FIELD_TEXT = new AllowableValue(TEXT_VALUE, TEXT_VALUE, "Use the string representation of the complex field as the value of the given column.");

    protected static final PropertyDescriptor COMPLEX_FIELD_STRATEGY = new PropertyDescriptor.Builder()
            .name("Complex Field Strategy")
            .description("Indicates how to handle complex fields, i.e. fields that do not have a single text value.")
            .expressionLanguageSupported(false)
            .required(true)
            .allowableValues(COMPLEX_FIELD_FAIL, COMPLEX_FIELD_WARN, COMPLEX_FIELD_IGNORE, COMPLEX_FIELD_TEXT)
            .defaultValue(COMPLEX_FIELD_TEXT.getValue())
            .build();



    protected static final AllowableValue FIELD_ENCODING_STRING = new AllowableValue(STRING_ENCODING_VALUE, STRING_ENCODING_VALUE,
            "Stores the value of each field as a UTF-8 String.");
    protected static final AllowableValue FIELD_ENCODING_BYTES = new AllowableValue(BYTES_ENCODING_VALUE, BYTES_ENCODING_VALUE,
            "Stores the value of each field as the byte representation of the type derived from the JSON.");

    protected static final PropertyDescriptor FIELD_ENCODING_STRATEGY = new PropertyDescriptor.Builder()
            .name("Field Encoding Strategy")
            .description(("Indicates how to store the value of each field in HBase. The default behavior is to convert each value from the " +
                    "JSON to a String, and store the UTF-8 bytes. Choosing Bytes will interpret the type of each field from " +
                    "the JSON, and convert the value to the byte representation of that type, meaning an integer will be stored as the " +
                    "byte representation of that integer."))
            .required(true)
            .allowableValues(FIELD_ENCODING_STRING, FIELD_ENCODING_BYTES)
            .defaultValue(FIELD_ENCODING_STRING.getValue())
            .build();

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(HBASE_CLIENT_SERVICE);
        properties.add(TABLE_NAME);
        properties.add(ROW_ID);
        properties.add(ROW_FIELD_NAME);
        properties.add(ROW_ID_ENCODING_STRATEGY);
        properties.add(COLUMN_FAMILY);
        properties.add(BATCH_SIZE);
        properties.add(COMPLEX_FIELD_STRATEGY);
        properties.add(FIELD_ENCODING_STRATEGY);
        return properties;
    }
    

    @Override
    protected Collection<ValidationResult> customValidate(ValidationContext validationContext) {
        final Collection<ValidationResult> results = new ArrayList<>();

        final String rowId = validationContext.getPropertyValue(ROW_ID).asString();
        final String rowFieldName = validationContext.getPropertyValue(ROW_FIELD_NAME).asString();

        if (StringUtils.isBlank(rowId) && StringUtils.isBlank(rowFieldName)) {
            results.add(new ValidationResult.Builder()
                    .subject(this.getClass().getSimpleName())
                    .explanation("Row Identifier or Row Identifier Field Name is required")
                    .valid(false)
                    .build());
        } else if (!StringUtils.isBlank(rowId) && !StringUtils.isBlank(rowFieldName)) {
            results.add(new ValidationResult.Builder()
                    .subject(this.getClass().getSimpleName())
                    .explanation("Row Identifier and Row Identifier Field Name can not be used together")
                    .valid(false)
                    .build());
        }

        return results;
    }

    @Override
    protected PutRecord createPut(final ProcessContext context, final Record record, final RecordSerializer serializer) {
        final String tableName = context.getPropertyValue(TABLE_NAME).asString();
        final String rowId = context.getPropertyValue(ROW_ID).asString();
        final String rowFieldName = context.getPropertyValue(ROW_FIELD_NAME).asString();
        final String columnFamily = context.getPropertyValue(COLUMN_FAMILY).asString();
        final boolean extractRowId = !StringUtils.isBlank(rowFieldName);
        final String complexFieldStrategy = context.getPropertyValue(COMPLEX_FIELD_STRATEGY).asString();
        final String fieldEncodingStrategy = context.getPropertyValue(FIELD_ENCODING_STRATEGY).asString();
        final String rowIdEncodingStrategy = context.getPropertyValue(ROW_ID_ENCODING_STRATEGY).asString();

        // Parse the JSON document
        final ObjectMapper mapper = new ObjectMapper();
        final AtomicReference<JsonNode> rootNodeRef = new AtomicReference<>(null);



        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            serializer.serialize(baos, record);
            final byte[] buffer = baos.toByteArray();
            baos.close();

            rootNodeRef.set(mapper.readTree(buffer));

        } catch (final ProcessException pe) {
            logger.error("Failed to parse {} as JSON due to {}; routing to failure", new Object[]{record, pe.toString()}, pe);
            return null;
        }catch (IOException e) {
            logger.error(e.toString());
        }

        final JsonNode rootNode = rootNodeRef.get();

        if (rootNode.isArray()) {
            logger.error("Root node of JSON must be a single document, found array for {}; routing to failure", new Object[]{record});
            return null;
        }

        final Collection<PutColumn> columns = new ArrayList<>();
        final AtomicReference<String> rowIdHolder = new AtomicReference<>(null);

        // convert each field/value to a column for the put, skip over nulls and arrays
        final Iterator<String> fieldNames = rootNode.getFieldNames();
        while (fieldNames.hasNext()) {
            final String fieldName = fieldNames.next();
            final AtomicReference<byte[]> fieldValueHolder = new AtomicReference<>(null);

            final JsonNode fieldNode = rootNode.get(fieldName);
            if (fieldNode.isNull()) {
                logger.debug("Skipping {} because value was null", new Object[]{fieldName});
            } else if (fieldNode.isValueNode()) {
                // for a value node we need to determine if we are storing the bytes of a string, or the bytes of actual types
                if (STRING_ENCODING_VALUE.equals(fieldEncodingStrategy)) {
                    final byte[] valueBytes = clientService.toBytes(fieldNode.asText());
                    fieldValueHolder.set(valueBytes);
                } else {
                    fieldValueHolder.set(extractJNodeValue(fieldNode));
                }
            } else {
                // for non-null, non-value nodes, determine what to do based on the handling strategy
                switch (complexFieldStrategy) {
                    case FAIL_VALUE:
                        logger.error("Complex value found for {}; routing to failure", new Object[]{fieldName});
                        return null;
                    case WARN_VALUE:
                        logger.warn("Complex value found for {}; skipping", new Object[]{fieldName});
                        break;
                    case TEXT_VALUE:
                        // use toString() here because asText() is only guaranteed to be supported on value nodes
                        // some other types of nodes, like ArrayNode, provide toString implementations
                        fieldValueHolder.set(clientService.toBytes(fieldNode.toString()));
                        break;
                    case IGNORE_VALUE:
                        // silently skip
                        break;
                    default:
                        break;
                }
            }

            // if we have a field value, then see if this is the row id field, if so store the value for later
            // otherwise add a new column where the fieldName and fieldValue are the column qualifier and value
            if (fieldValueHolder.get() != null) {
                if (extractRowId && fieldName.equals(rowFieldName)) {
                    rowIdHolder.set(fieldNode.asText());
                } else {
                    columns.add(new PutColumn(columnFamily.getBytes(StandardCharsets.UTF_8), fieldName.getBytes(StandardCharsets.UTF_8), fieldValueHolder.get()));
                }
            }
        }

        // if we are expecting a field name to use for the row id and the incoming document doesn't have it
        // log an error message so the user can see what the field names were and return null so it gets routed to failure
        if (extractRowId && rowIdHolder.get() == null) {
            final String fieldNameStr = StringUtils.join(rootNode.getFieldNames(), ",");
            logger.error("Row ID field named '{}' not found in field names '{}'; routing to failure", new Object[] {rowFieldName, fieldNameStr});
            return null;
        }

        final String putRowId = (extractRowId ? rowIdHolder.get() : rowId);

        byte[] rowKeyBytes = getRow(putRowId,context.getPropertyValue(ROW_ID_ENCODING_STRATEGY).asString());
        return new PutRecord(tableName, rowKeyBytes, columns, record);
    }

    /*
     *Handles the conversion of the JsonNode value into it correct underlying data type in the form of a byte array as expected by the columns.add function
     */
    private byte[] extractJNodeValue(final JsonNode n){
        if (n.isBoolean()){
            //boolean
            return clientService.toBytes(n.asBoolean());
        }else if(n.isNumber()){
            if(n.isIntegralNumber()){
                //interpret as Long
                return clientService.toBytes(n.asLong());
            }else{
                //interpret as Double
                return clientService.toBytes(n.asDouble());
            }
        }else{
            //if all else fails, interpret as String
            return clientService.toBytes(n.asText());
        }
    }

}
