package com.hurence.logisland.processor.parser;

import com.hurence.logisland.components.ComponentsFactory;
import com.hurence.logisland.config.ComponentConfiguration;
import com.hurence.logisland.event.Event;
import com.hurence.logisland.log.StandardParserContext;
import com.hurence.logisland.log.StandardParserInstance;
import com.hurence.logisland.processor.ProcessContext;
import com.hurence.logisland.processor.StandardProcessContext;
import com.hurence.logisland.processor.StandardProcessorInstance;
import com.hurence.logisland.utils.string.Multiline;
import org.apache.avro.Schema;
import org.apache.commons.lang3.text.StrTokenizer;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.*;


public class SplitTextTest {

    public static final String DATA_TRAKER1_LOG = "/data/traker1.log";
    private static Logger logger = LoggerFactory.getLogger(SplitTextTest.class);


    @Test
    public void testLoadConfig() throws Exception {


        Map<String, String> conf = new HashMap<>();
        conf.put("regex", "(\\D{3}\\s+\\d{1,2} \\d{2}:\\d{2}:\\d{2})\\s+(\\S*)\\s+(?:date=(\\S*)\\s+)?(?:time=(\\S*)\\s+)?(?:devname=(\\S*)\\s+)?(?:devid=(\\S*)\\s+)?(?:logid=(\\S*)\\s+)?(?:type=(\\S*)\\s+)?(?:subtype=(\\S*)\\s+)?(?:level=(\\S*)\\s+)?(?:vd=(\\S*)\\s+)?(?:srcip=(\\S*)\\s+)?(?:srcport=(\\S*)\\s+)?(?:srcintf=(\\S*)\\s+)?(?:dstip=(\\S*)\\s+)?(?:dstport=(\\S*)\\s+)?(?:dstintf=(\\S*)\\s+)?(?:poluuid=(\\S*)\\s+)?(?:sessionid=(\\S*)\\s+)?(?:proto=(\\S*)\\s+)?(?:action=(\\S*)\\s+)?(?:policyid=(\\S*)\\s+)?(?:dstcountry=(\\S*)\\s+)?(?:srccountry=(\\S* \\S*)\\s+)?(?:trans*disp=(\\S*)\\s+)?(?:trans*ip=(\\S*)\\s+)?(?:trans*port=(\\S*)\\s+)?(?:service=(\\S*)\\s+)?(?:duration=(\\S*)\\s+)?(?:sentbyte=(\\S*)\\s+)?(?:rcvdbyte=(\\S*)\\s+)?(?:sentpkt=(\\S*)\\s+)?(?:rcvdpkt=(\\S*)\\s+)?(?:appcat=(\\S*)\\s+)?(?:crscore=(\\S*)\\s+)?(?:craction=(\\S*)\\s+)?(?:crlevel=(\\S*)\\s+)?");
        conf.put("fields", "raw_content,line_date,host,date,time,devname,devid,logid,type,subtype,level,vd,src_ip,src_port,src_inf,dest_ip,dest_port,dest_inf,pol_uuid,session_id,proto,action,policy_id,dest_country,src_country,tran_isp,tran_ip,tran_port,service,duration,bytes_out,bytes_in,packets_out,packets_in,app_cat,cr_score,cr_action,cr_level");

        ComponentConfiguration componentConfiguration = new ComponentConfiguration();

        componentConfiguration.setComponent("com.hurence.logisland.processor.parser.SplitText");
        componentConfiguration.setType("parser");
        componentConfiguration.setConfiguration(conf);

        StandardParserInstance instance = ComponentsFactory.getParserInstance(componentConfiguration);
        ProcessContext context = new StandardParserContext(instance);
        Assert.assertTrue(instance != null);






        InputStreamReader isr;
        BufferedReader bsr = null;
        try {
            final FileInputStream fis = new FileInputStream(SplitTextTest.class.getResource(DATA_TRAKER1_LOG).getFile());
            isr = new InputStreamReader(fis, "UTF-8");
            bsr = new BufferedReader(isr);

            logger.debug("start parsing traker log file : " + DATA_TRAKER1_LOG);
            int nblines = 0;
            int numEvents = 0;
            String line;
            while ((line = bsr.readLine()) != null) {

                final List<Event> events = new ArrayList<>(instance.getParser().parse(context, line));

                if(events.isEmpty())
                    System.out.println(line);
                else numEvents ++;

                nblines ++;
                //   Assert.assertTrue(events.size() == 1);
             //   Assert.assertTrue(events.get(0).entrySet().size() == 35);
            }
            System.out.println("events count :" + numEvents);

            Assert.assertTrue(numEvents ==nblines );

        } catch (FileNotFoundException ex) {
            logger.error("file not found : " + DATA_TRAKER1_LOG);
        } catch (IOException ex) {
            logger.error("unknown error while parsing : " + DATA_TRAKER1_LOG);
        } finally {
            try {
                if (bsr != null) {
                    bsr.close();
                }
            } catch (IOException ex) {
                logger.error("unknown error while parsing : " + DATA_TRAKER1_LOG);
            }

        }





    }
}
