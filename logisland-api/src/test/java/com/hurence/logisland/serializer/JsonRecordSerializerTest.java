/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.hurence.logisland.serializer;

import com.hurence.logisland.record.Record;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

import static org.junit.Assert.assertTrue;

/**
 *
 * @author tom
 */
public class JsonRecordSerializerTest {


	@Test
	public void validateJsonSerialization() throws IOException {

		final JsonRecordSerializer serializer = new JsonRecordSerializer();


		List<String> tags = new ArrayList<>(Arrays.asList("spam","filter","mail"));
		Record record = new Record("cisco");
		record.setField("timestamp", "long", new Date().getTime());
		record.setField("method", "string", "GET");
		record.setField("ip_source", "string", "123.34.45.123");
		record.setField("ip_target", "string", "255.255.255.255");
		record.setField("url_scheme", "string", "http");
		record.setField("url_host", "string", "origin-www.20minutes.fr");
		record.setField("url_port", "string", "80");
		record.setField("url_path", "string", "/r15lgc-100KB.js");
		record.setField("request_size", "int", 1399);
		record.setField("response_size", "int", 452);
		record.setField("is_outside_office_hours", "boolean", false);
		record.setField("is_host_blacklisted", "boolean", false);
		//event.setField("tags", "array", tags);

		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		serializer.serialize(baos, record);
		baos.close();


		String strEvent = new String(baos.toByteArray());
		ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
		Record deserializedRecord = serializer.deserialize(bais);

		assertTrue(deserializedRecord.equals(record));

	}

}