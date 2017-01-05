package com.hurence.logisland.processor;

import com.google.common.collect.Lists;
import com.hurence.logisland.annotation.behavior.DynamicProperty;
import com.hurence.logisland.annotation.documentation.CapabilityDescription;
import com.hurence.logisland.annotation.documentation.Tags;
import com.hurence.logisland.component.PropertyDescriptor;
import com.hurence.logisland.record.Field;
import com.hurence.logisland.record.FieldType;
import com.hurence.logisland.record.Record;
import com.hurence.logisland.validator.StandardValidators;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Tags({"record", "properties", "parser"})
@CapabilityDescription("Parse a field made of key=value fields separated by spaces\n" +
        "a string like \"a=1 b=2 c=3\" will add a,b & c fields, respectively with values 1,2 & 3 to the current Record")
public class ParseProperties extends AbstractProcessor {

	private static final long serialVersionUID = 1L;

	private static final Logger logger = LoggerFactory.getLogger(ParseProperties.class);

	public static final PropertyDescriptor PROPERTIES_FIELD = new PropertyDescriptor.Builder()
			.name("properties.field")
			.description("the field containing the properties to split and treat")
			.required(true)
			.addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
			.build();

	private static final Pattern PROPERTIES_SPLITTER_PATTERN = Pattern.compile("([\\S]+)=([^=]+)(?:\\s|$)");

	public ParseProperties() {
		super();
	}

	@Override
	public Collection<Record> process(ProcessContext context, Collection<Record> records) {
		String property = context.getPropertyValue(PROPERTIES_FIELD).asString();
		for (Record record : records) {
			extractAndParsePropertiesField(record, property);
		}
		return records;
	}

	private void extractAndParsePropertiesField(Record outputRecord, String propertiesField) {
		Field field = outputRecord.getField(propertiesField);
		if (field != null) {
			String str = field.getRawValue().toString();
			Matcher matcher = PROPERTIES_SPLITTER_PATTERN.matcher(str);
			while (matcher.find()) {
				if (matcher.groupCount() == 2) {
					String key = matcher.group(1);
					String value = matcher.group(2).trim();

					// logger.debug(String.format("field %s = %s", key, value));
					outputRecord.setField(key, FieldType.STRING, value);
				}
			}
			outputRecord.removeField("properties");
		}
	}

	@Override
	public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
		return Lists.newArrayList(PROPERTIES_FIELD);
	}

}
