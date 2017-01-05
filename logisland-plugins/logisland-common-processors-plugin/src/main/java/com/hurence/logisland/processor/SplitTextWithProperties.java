package com.hurence.logisland.processor;

import com.hurence.logisland.record.Field;
import com.hurence.logisland.record.FieldDictionary;
import com.hurence.logisland.record.FieldType;
import com.hurence.logisland.record.Record;
import com.hurence.logisland.util.time.DateUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class SplitTextWithProperties extends SplitText {

	private static final long serialVersionUID = 4180349996855996949L;

	private static final Logger logger = LoggerFactory.getLogger(SplitTextWithProperties.class);

	private static final String PROPERTIES_FIELD = "properties";
	private static final Pattern PROPERTIES_SPLITTER_PATTERN = Pattern.compile("([\\S]+)=([^=]+)(?:\\s|$)");

	public SplitTextWithProperties() {
		super();
	}

	@Override
	public Collection<Record> process(ProcessContext context, Collection<Record> records) {
		List<Record> outputRecords = (List<Record>) super.process(context, records);

		for (Record outputRecord : outputRecords) {
			extractAndParsePropertiesField(outputRecord);

			// TODO remove this with EL (specific code for traker syslog)
			if (outputRecord.getField("date") != null && outputRecord.getField("time") != null) {
				String eventTimeString = outputRecord.getField("date").asString() +
						" " +
						outputRecord.getField("time").asString();

				try {
					Date eventDate = DateUtil.parse(eventTimeString);

					if (eventDate != null) {
						outputRecord.setField(FieldDictionary.RECORD_TIME, FieldType.LONG, eventDate.getTime());
					}
				} catch (Exception e) {
					logger.warn("unable to parse date {}", eventTimeString);
				}

			}
		}
		return outputRecords;
	}

	private void extractAndParsePropertiesField(Record outputRecord) {
		Field field = outputRecord.getField(PROPERTIES_FIELD);
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

}
