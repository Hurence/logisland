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
package com.hurence.logisland.processor.util;

import com.hurence.logisland.component.ComponentFactory;
import com.hurence.logisland.config.ConfigReader;
import com.hurence.logisland.config.LogislandConfiguration;
import com.hurence.logisland.config.ProcessorConfiguration;
import com.hurence.logisland.processor.ProcessContext;
import com.hurence.logisland.record.Field;
import com.hurence.logisland.util.runner.MockRecord;
import com.hurence.logisland.util.runner.TestRunner;
import com.hurence.logisland.util.runner.TestRunners;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.junit.Assert;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.zip.GZIPInputStream;

import static org.junit.Assert.assertEquals;

public abstract class BaseSyslogTest {

	protected static final String BASE_DATA_FOLDER = "/samples";

	public BaseSyslogTest() {
	}

	protected InputStream getFileInputStream(Path resourcePath) throws IOException {
		InputStream is = new FileInputStream(resourcePath.toAbsolutePath().toFile());
		if (resourcePath.toString().endsWith(".gz")) {
			is = new GZIPInputStream(is);
		}
		return new BufferedInputStream(is);
	}

	protected Set<String> getSchemaFields(String schema) throws FileNotFoundException, IOException, ParseException, URISyntaxException {
		Set<String> res = new HashSet<>();
		JSONParser parser = new JSONParser();
		JSONObject jsonObject = (JSONObject) parser.parse(new FileReader(this.getClass().getResource(schema).toURI().getPath()));
		JSONArray jsonArray = (JSONArray) jsonObject.get("fields");
		jsonArray.iterator().forEachRemaining(o -> res.add(((JSONObject) o).get("name").toString()));
		return res;
	}

	protected long countHasType(List<MockRecord> records, String type) {
		return records.stream()
				.filter(r -> {
					Field actualType = r.getField("source");
					return actualType != null && type.equals(actualType.asString());
				})
				.count();
	}

	protected long getFileLineCount(Path path) throws IOException {
		InputStreamReader isr = new InputStreamReader(getFileInputStream(path));
		return new BufferedReader(isr).lines().count();
	}

	protected Stream<Path> getResourceFiles(String type, Predicate<Path> filterPredicate) throws URISyntaxException, IOException {
		String baseFolder = String.format(BASE_DATA_FOLDER + "/%s", type);
		URI baseFolderURI = this.getClass().getResource(baseFolder).toURI();

		Stream<Path> files = Files
				.walk(Paths.get(baseFolderURI))
				.filter(filterPredicate);

		return files;
	}

	protected void checkAllFieldsUsed(TestRunner testRunner, String schema) throws FileNotFoundException, IOException, ParseException, URISyntaxException {
		// Get all fields seen for this testrunner
		Set<String> allFieldsNames = new HashSet<>();
		testRunner.getOutputRecords().forEach(r -> {
			allFieldsNames.addAll(r.getAllFieldNames());
		});

		System.out.println(allFieldsNames);
		// Check we found them all
		Set<String> schemaFields = getSchemaFields(schema);
		schemaFields.removeAll(allFieldsNames);
		Assert.assertEquals("Missing fields: " + schemaFields, 0, schemaFields.size());
	}

	protected void printFields(String message, MockRecord out) {
		System.out.println(message);
		out.getAllFields()
				.stream()
				.forEach(System.out::println);
	}

	protected String getValueRegEx(String type, String subType, String name) throws IOException {
		Properties props = new Properties();
		props.load(getClass().getResourceAsStream("/schemas/regexp.properties"));

		String propName = String.format("%s_%s_%s.regexp", type, subType, name);
		return props.getProperty(propName).trim();
	}

	protected String getValueFields(String type, String subType, String name) throws IOException {
		Properties props = new Properties();
		props.load(getClass().getResourceAsStream("/schemas/regexp.properties"));

		String propName = String.format("%s_%s_%s.fields", type, subType, name);
		return props.getProperty(propName).trim();
	}

	protected ProcessorConfiguration loadFirstProcessorConfiguration(String confPath, String processorName) throws Exception {
		return loadAllProcessorConfigurations(confPath, processorName).get(0);
	}

	protected List<ProcessorConfiguration> loadAllProcessorConfigurations(String confPath, String processorName) throws Exception {

		LogislandConfiguration config = ConfigReader.loadConfig(this.getClass().getResource(confPath).getFile());
		config.getEngine().getStreamConfigurations().forEach(pcc -> {
			pcc.getProcessorConfigurations().forEach(pc -> {
				System.out.println(pc);
			});
		});

		List<ProcessorConfiguration> processorsConfigurations = config.getEngine().getStreamConfigurations().stream()
				.map(pcc -> pcc.getProcessorConfigurations())
				.flatMap(pc -> pc.stream())
				.filter(pc -> processorName.equals(pc.getProcessor()))
				.collect(Collectors.toList());

		return processorsConfigurations;
	}

	protected TestRunner createTestRunner(String confPath, String processorName) throws Exception {
		ProcessorConfiguration processorConfiguration = loadFirstProcessorConfiguration(confPath, processorName);
		return createTestRunner(processorConfiguration);
	}

	protected TestRunner createTestRunner(ProcessorConfiguration processorsConfiguration) throws Exception {
		ProcessContext processContext = ComponentFactory.getProcessContext(processorsConfiguration).get();
		return TestRunners.newTestRunner(processContext);
	}

	public static class ResourceFilePredicate implements Predicate<Path> {
		private String name;
		private String date;
		private String subType;

		public ResourceFilePredicate(String name, String date, String subType) {
			this.name = name;
			this.date = date;
			this.subType = subType;
		}

		@Override
		public boolean test(Path p) {
			String fileName = p.getFileName().toString().toLowerCase();
			String fileDate = p.getParent().getFileName().toString();
			String fileSubtype = p.getParent().getParent().getFileName().toString();
			return fileName.startsWith(name)
					&& fileDate.equals(date)
					&& fileSubtype.equals(subType);
		}
	}
}
