/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.hurence.logisland.serializer;

import com.hurence.logisland.record.FieldType;
import com.hurence.logisland.record.Record;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Date;

import static org.junit.Assert.assertTrue;

/**
 *
 * @author tom
 */
public class JsonSerializerTest {


	@Test
	public void validateJsonSerialization() throws IOException {

		final JsonSerializer serializer = new JsonSerializer();


		Record record = new Record("cisco");
		record.setId("firewall_record1");
		record.setField("timestamp", FieldType.LONG, new Date().getTime());
		record.setField("method", FieldType.STRING, "GET");
		record.setField("ip_source", FieldType.STRING, "123.34.45.123");
		record.setField("ip_target", FieldType.STRING, "255.255.255.255");
		record.setField("url_scheme", FieldType.STRING, "http");
		record.setField("url_host", FieldType.STRING, "origin-www.20minutes.fr");
		record.setField("url_port", FieldType.STRING, "80");
		record.setField("url_path", FieldType.STRING, "/r15lgc-100KB.js");
		record.setField("request_size", FieldType.INT, 1399);
		record.setField("response_size", FieldType.INT, 452);
		record.setField("is_outside_office_hours", FieldType.BOOLEAN, false);
		record.setField("is_host_blacklisted", FieldType.BOOLEAN, false);
		//record.setField("tags", FieldType.ARRAY, new ArrayList<>(Arrays.asList("spam", "filter", "mail")));


		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		serializer.serialize(baos, record);
		baos.close();


		String strEvent = new String(baos.toByteArray());
		ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
		Record deserializedRecord = serializer.deserialize(bais);

		assertTrue(deserializedRecord.equals(record));

	}

}