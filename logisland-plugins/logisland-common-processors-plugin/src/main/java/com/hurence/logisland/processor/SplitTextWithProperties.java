package com.hurence.logisland.processor;

import com.google.common.collect.Lists;
import com.hurence.logisland.component.PropertyDescriptor;
import com.hurence.logisland.record.Field;
import com.hurence.logisland.record.FieldDictionary;
import com.hurence.logisland.record.FieldType;
import com.hurence.logisland.record.Record;
import com.hurence.logisland.util.time.DateUtil;
import com.hurence.logisland.validator.StandardValidators;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class SplitTextWithProperties extends SplitText {

	private static final long serialVersionUID = 4180349996855996949L;

	private static final Logger logger = LoggerFactory.getLogger(SplitTextWithProperties.class);

	public static final PropertyDescriptor PROPERTIES_FIELD = new PropertyDescriptor.Builder()
			.name("properties.field")
			.description("the field containing the properties to split and treat")
			.required(true)
			.defaultValue("properties")
			.build();

	private static final Pattern PROPERTIES_SPLITTER_PATTERN = Pattern.compile("([\\S]+)=([^=]+)(?:\\s|$)");

	@Override
	public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> descriptors = new ArrayList<>();
        descriptors.add(VALUE_REGEX);
        descriptors.add(VALUE_FIELDS);
        descriptors.add(KEY_REGEX);
        descriptors.add(KEY_FIELDS);
        descriptors.add(RECORD_TYPE);
        descriptors.add(KEEP_RAW_CONTENT);
        descriptors.add(PROPERTIES_FIELD);

        return Collections.unmodifiableList(descriptors);
	}


	@Override
	public Collection<Record> process(ProcessContext context, Collection<Record> records) {
		List<Record> outputRecords = (List<Record>) super.process(context, records);

        String propertiesField = context.getPropertyValue(PROPERTIES_FIELD).asString();

		for (Record outputRecord : outputRecords) {
			Field field = outputRecord.getField(propertiesField);
			if (field != null) {
				String str = field.getRawValue().toString();
				Matcher matcher = PROPERTIES_SPLITTER_PATTERN.matcher(str);
				while (matcher.find()) {
					if (matcher.groupCount() == 2) {
						String key = matcher.group(1);
						String value = matcher.group(2);

						// logger.debug(String.format("field %s = %s", key, value));
						outputRecord.setField(key, FieldType.STRING, value);
					}
				}
				outputRecord.removeField("properties");
			}
		}
		return outputRecords;
	}

}
