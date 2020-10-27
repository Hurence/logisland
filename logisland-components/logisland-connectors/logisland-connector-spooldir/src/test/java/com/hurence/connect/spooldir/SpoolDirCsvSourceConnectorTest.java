package com.hurence.connect.spooldir;

import com.hurence.logisland.connect.spooldir.SpoolDirCsvSourceConnector;
import com.hurence.logisland.connect.spooldir.SpoolDirCsvSourceConnectorConfig;
import com.hurence.logisland.connect.spooldir.SpoolDirCsvSourceTask;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SpoolDirCsvSourceConnectorTest {

    private static final Logger logger = LoggerFactory.getLogger(SpoolDirCsvSourceConnectorTest.class);

    /**
     * batch size is 5000
     * line 20081 of file is KO
     * So only 20000 records should be correctly returned, then file should be in error folder.
     */
    @Test
    public void testKoFile() {
        final String koFileName="dataHistorian-row-20081-ko.csv";
        try {
            final Path tempDirWithPrefix = Files.createTempDirectory("testKoFile");
            final SpoolDirCsvSourceTask task = new SpoolDirCsvSourceTask();
            final SpoolDirCsvSourceConnector connector = new SpoolDirCsvSourceConnector();
            try {
                final File tmpDir = tempDirWithPrefix.toFile();
                final File inputDir = new File(tmpDir, "spooldir");
                inputDir.mkdir();
                final File outputDir = new File(tmpDir, "spooldir-finished");
                outputDir.mkdir();
                final File outputErrorDir = new File(tmpDir, "spooldir-finished-error");
                outputErrorDir.mkdir();

                copyResourceToInput(inputDir, "/spooldir/bug-csv/" + koFileName);

                final Map<String, String> properties = new HashMap<>();
                properties.put(SpoolDirCsvSourceConnectorConfig.INPUT_PATH_CONFIG, inputDir.getPath());
                properties.put(SpoolDirCsvSourceConnectorConfig.FINISHED_PATH_CONFIG, outputDir.getPath());
                properties.put(SpoolDirCsvSourceConnectorConfig.ERROR_PATH_CONFIG, outputErrorDir.getPath());
                properties.put(SpoolDirCsvSourceConnectorConfig.EMPTY_POLL_WAIT_MS_CONF, "500");
                properties.put(SpoolDirCsvSourceConnectorConfig.INPUT_FILE_PATTERN_CONF, "^.*.csv$");
                properties.put(SpoolDirCsvSourceConnectorConfig.HALT_ON_ERROR_CONF, "false");
                properties.put(SpoolDirCsvSourceConnectorConfig.TOPIC_CONF, "evoa_measures");
                properties.put(SpoolDirCsvSourceConnectorConfig.CSV_FIRST_ROW_AS_HEADER_CONF, "true");
                properties.put(SpoolDirCsvSourceConnectorConfig.SCHEMA_GENERATION_ENABLED_CONF, "false");
                properties.put(SpoolDirCsvSourceConnectorConfig.CSV_SEPARATOR_CHAR_CONF, "59");
                properties.put(SpoolDirCsvSourceConnectorConfig.BATCH_SIZE_CONF, "5000");
                properties.put(SpoolDirCsvSourceConnectorConfig.PARSER_TIMESTAMP_DATE_FORMATS_CONF, "dd/MM/yyyy HH:mm:ss");
                properties.put(SpoolDirCsvSourceConnectorConfig.KEY_SCHEMA_CONF, "{\"name\":\"com.hurence.logisland.record.TimeserieRecordKey\",\"type\":\"STRUCT\",\"isOptional\":true,\"fieldSchemas\":{\"tagname\":{\"type\":\"STRING\",\"isOptional\":false}}}");
                properties.put(SpoolDirCsvSourceConnectorConfig.VALUE_SCHEMA_CONF, "{\"name\":\"com.hurence.logisland.record.TimeserieRecordValue\",\"type\":\"STRUCT\",\"isOptional\":false,\"fieldSchemas\":{\"timestamp\":{\"name\":\"org.apache.kafka.connect.data.Timestamp\",\"type\":\"INT64\",\"version\":1,\"isOptional\":false},\"value\":{\"type\":\"FLOAT64\",\"isOptional\":false},\"tagname\":{\"type\":\"STRING\",\"isOptional\":false},\"quality\":{\"type\":\"FLOAT32\",\"isOptional\":true}}}");
                connector.start(properties);
                task.start(connector.taskConfigs(1).get(0));


                boolean empty = false;
                long counter = 0;
                while (!empty) {
                    List<SourceRecord> records = task.poll();
                    empty = records.isEmpty();
                    counter += records.size();
                }
                Assert.assertEquals(20000, counter);
                Assert.assertTrue(new File(outputErrorDir, koFileName).exists());
            } catch (Exception e){
                logger.error("error while processing", e);
            } finally {
                task.stop();
                connector.stop();
                deleteDirectory(tempDirWithPrefix.toFile());
            }
        } catch (IOException e) {
            logger.error("error setuping tmp dir", e);
        }
    }

    private boolean deleteDirectory(File directoryToBeDeleted) {
        File[] allContents = directoryToBeDeleted.listFiles();
        if (allContents != null) {
            for (File file : allContents) {
                deleteDirectory(file);
            }
        }
        return directoryToBeDeleted.delete();
    }

    private void copyResourceToInput(File inputDir, String resourcePath) throws URISyntaxException, IOException {
        String fileName = new File(resourcePath).getName();
        URI csvUri = getClass().getResource(resourcePath).toURI();
        Files.copy(Paths.get(csvUri), Paths.get(new File(inputDir, fileName).toURI()));
    }

}
