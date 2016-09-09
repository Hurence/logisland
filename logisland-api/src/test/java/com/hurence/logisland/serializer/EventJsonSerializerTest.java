/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.hurence.logisland.serializer;

import com.hurence.logisland.event.Event;
import org.apache.avro.Schema;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
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
public class EventJsonSerializerTest {


	@Test
	public void validateJsonSerialization() throws IOException {

		final EventJsonSerializer serializer = new EventJsonSerializer();


		List<String> tags = new ArrayList<>(Arrays.asList("spam","filter","mail"));
		Event event = new Event("cisco");
		event.put("timestamp", "long", new Date().getTime());
		event.put("method", "string", "GET");
		event.put("ipSource", "string", "123.34.45.123");
		event.put("ipTarget", "string", "255.255.255.255");
		event.put("urlScheme", "string", "http");
		event.put("urlHost", "string", "origin-www.20minutes.fr");
		event.put("urlPort", "string", "80");
		event.put("urlPath", "string", "/r15lgc-100KB.js");
		event.put("requestSize", "int", 1399);
		event.put("responseSize", "int", 452);
		event.put("isOutsideOfficeHours", "boolean", false);
		event.put("isHostBlacklisted", "boolean", false);
		//event.put("tags", "array", tags);

		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		serializer.serialize(baos, event);
		baos.close();


		String strEvent = new String(baos.toByteArray());
		ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
		Event deserializedEvent = serializer.deserialize(bais);

		assertTrue(deserializedEvent.equals(event));

	}

}