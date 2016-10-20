package com.hurence.logisland.processor;

import com.hurence.logisland.record.Record;
import com.hurence.logisland.record.StandardRecord;
import com.hurence.logisland.util.runner.TestRunner;
import com.hurence.logisland.util.runner.TestRunners;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.SuffixFileFilter;
import org.apache.commons.io.filefilter.TrueFileFilter;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Collection;
import java.util.List;


public class OutlierProcessorTest {
    private static final Logger logger = LoggerFactory.getLogger(TimeSeriesCsvLoader.class);
    private final static Charset ENCODING = StandardCharsets.UTF_8;
    private final String RESOURCES_DIRECTORY = "target/test-classes/benchmark_data/";
    private static final DateTimeFormatter inputDateFormat = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss");



    @Test
    @Ignore("too long")
    public void testDetection() throws IOException {
        final TestRunner testRunner = TestRunners.newTestRunner(new OutlierProcessor());
        testRunner.setProperty(OutlierProcessor.BATCH_OUTLIER_ALGORITHM, "RAD");
        testRunner.assertValid();


        File f = new File(RESOURCES_DIRECTORY);

        for (File file : FileUtils.listFiles(f, new SuffixFileFilter(".csv"), TrueFileFilter.INSTANCE)) {
            BufferedReader reader = Files.newBufferedReader(file.toPath(), ENCODING);
            List<Record> records = TimeSeriesCsvLoader.load(reader, true, inputDateFormat);
            Assert.assertTrue(!records.isEmpty());


            testRunner.clearQueues();
            testRunner.enqueue(records.toArray(new Record[records.size()]));
            testRunner.run();
            testRunner.assertAllInputRecordsProcessed();

            System.out.println("records.size() = " + records.size());
            System.out.println("testRunner.getOutputRecords().size() = " + testRunner.getOutputRecords().size());
          //  testRunner.assertOutputRecordsCount(533);
        }

    }


}
