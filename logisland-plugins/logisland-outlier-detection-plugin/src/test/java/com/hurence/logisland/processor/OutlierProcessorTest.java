package com.hurence.logisland.processor;

import com.hurence.logisland.event.Event;
import com.hurence.logisland.log.LogParserException;
import org.junit.Assert;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.SuffixFileFilter;
import org.apache.commons.io.filefilter.TrueFileFilter;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
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

/**
 * Created by mike on 15/04/16.
 */
public class OutlierProcessorTest {
    private static final Logger logger = LoggerFactory.getLogger(TimeSeriesCsvLoader.class);
    final static Charset ENCODING = StandardCharsets.UTF_8;
    final String RESOURCES_DIRECTORY = "target/test-classes/benchmark_data/";
    private static final DateTimeFormatter inputDateFormat = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss");
    //private static final DateTimeFormatter defaultOutputDateFormat = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss.SSSZ");


    @Test
    public void testDetection() throws IOException, LogParserException {
        File f = new File(RESOURCES_DIRECTORY);

        for (File file : FileUtils.listFiles(f, new SuffixFileFilter(".csv"), TrueFileFilter.INSTANCE)) {
            BufferedReader reader = Files.newBufferedReader(file.toPath(), ENCODING);
            List<Event> events = TimeSeriesCsvLoader.load(reader, true, inputDateFormat);
            Assert.assertTrue(!events.isEmpty());


            AbstractEventProcessor processor = new OutlierProcessor();
            StandardProcessorInstance instance = new StandardProcessorInstance(processor, "0");
          //  instance.setProperty("rules",rulesAsString);
            ProcessContext context = new StandardProcessContext(instance);
            processor.init(context);
            Collection<Event> outliersEvents = processor.process(context, events);

            logger.info(outliersEvents.toString());
        }
    }


}
