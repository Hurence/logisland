package com.hurence.logisland.processor;

import com.hurence.logisland.record.Record;
import com.hurence.logisland.log.LogParserException;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.SuffixFileFilter;
import org.apache.commons.io.filefilter.TrueFileFilter;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.List;

/**
 * Created by mike on 15/04/16.
 */
public class CsvLoaderTest {
    private static final Logger logger = LoggerFactory.getLogger(TimeSeriesCsvLoader.class);
    final static Charset ENCODING = StandardCharsets.UTF_8;
    final String RESOURCES_DIRECTORY = "target/test-classes/benchmark_data/";
    private static final DateTimeFormatter inputDateFormat = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss");
    //private static final DateTimeFormatter defaultOutputDateFormat = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss.SSSZ");


    @Test
    public void CsvLoaderTest() throws IOException, LogParserException {
        File f = new File(RESOURCES_DIRECTORY);

        for (File file : FileUtils.listFiles(f, new SuffixFileFilter(".csv"), TrueFileFilter.INSTANCE)) {
            BufferedReader reader = Files.newBufferedReader(file.toPath(), ENCODING);
            List<Record> records = TimeSeriesCsvLoader.load(reader, true, inputDateFormat);



            Assert.assertTrue(!records.isEmpty());
            //Assert.assertTrue("should be 4032, was : " + events.size(), events.size() == 4032);

            for (Record record : records) {
                Assert.assertTrue("should be sensors, was " + record.getType(), record.getType().equals("sensors"));
                Assert.assertTrue("should be 5, was " + record.getFieldsEntrySet().size(), record.getFieldsEntrySet().size() == 5);
                Assert.assertTrue(record.getAllFieldNames().contains("timestamp"));
                if(!record.getAllFieldNames().contains("value"))
                    System.out.println("stop");
                Assert.assertTrue(record.getAllFieldNames().contains("value"));

                Assert.assertTrue(record.getField("timestamp").getRawValue() instanceof Long);
                Assert.assertTrue(record.getField("value").getRawValue() instanceof Double);
            }
        }
    }

    @Test
    public void CsvLoaderUnparsableExceptionTest() throws IOException, LogParserException {
        File f = new File(RESOURCES_DIRECTORY + "artificialWithAnomaly/art_daily_flatmiddle.csv");

        DateTimeFormatter wrongInputDateFormat = DateTimeFormat.forPattern("yyyy-MM-dd HH-mm-ss");

        BufferedReader reader = Files.newBufferedReader(f.toPath(), ENCODING);
        try {
            TimeSeriesCsvLoader.load(reader, true, wrongInputDateFormat);
        }
        catch (LogParserException e) {
            return;
        }

        Assert.fail("Should have failed with an UnparsableException exception");
    }
}
