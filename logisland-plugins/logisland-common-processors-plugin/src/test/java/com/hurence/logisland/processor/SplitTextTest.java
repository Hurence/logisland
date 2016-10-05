package com.hurence.logisland.processor;

import com.hurence.logisland.component.ComponentType;
import com.hurence.logisland.config.ComponentFactory;
import com.hurence.logisland.config.ProcessorConfiguration;
import com.hurence.logisland.record.Record;
import com.hurence.logisland.record.RecordUtils;
import com.sun.source.tree.AssertTree;
import static org.junit.Assert.*;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.*;


public class SplitTextTest {

    private static final String DATA_USR_BACKEND_LOG = "/data/usr_backend_application.log";
    private static final String DATA_USR_GATEWAY_LOG = "/data/usr_gateway_application.log";
    private static final String DATA_USR_BACKEND_LOG2 = "/data/USR-fail2.log";
    private static final String DATA_TRAKER1_LOG = "/data/traker1_with_key.log";

    private static Logger logger = LoggerFactory.getLogger(SplitTextTest.class);


    @Test
    public void testUsrBackend() throws IOException {


        Map<String, String> conf = new HashMap<>();
        conf.put(SplitText.VALUE_REGEX.getName(), "\\[(\\S*)\\]\\s+(\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2},\\d{3})\\s+\\[SES:([^:]*):([^\\]]*)\\]\\s*\\[ACC:([^\\]]*)\\]\\[SRV:([^\\]]*)\\]\\s+(\\S*)\\s+(\\S*)\\s+(\\S*)\\s+(.*)\\s*");
        conf.put(SplitText.VALUE_FIELDS.getName(), "raw_content,component,event_time,player_type,session,user_id,srv,log_level,logger,none,trace");
        conf.put(SplitText.KEY_REGEX.getName(), "(\\S*):(\\S*)");
        conf.put(SplitText.KEY_FIELDS.getName(), "es_index,host_name");

        ProcessorConfiguration componentConfiguration = new ProcessorConfiguration();
        componentConfiguration.setComponent(SplitText.class.getName());
        componentConfiguration.setType(ComponentType.PARSER.toString());
        componentConfiguration.setConfiguration(conf);

        Optional<StandardProcessorInstance> instance = ComponentFactory.getProcessorInstance(componentConfiguration);
        assertTrue(instance.isPresent());
        ProcessContext context = new StandardProcessContext(instance.get());

        InputStreamReader isr;
        BufferedReader bsr = null;

        final FileInputStream fis = new FileInputStream(SplitTextTest.class.getResource(DATA_USR_BACKEND_LOG).getFile());
        isr = new InputStreamReader(fis, "UTF-8");
        bsr = new BufferedReader(isr);

        logger.debug("start parsing traker log file : " + DATA_USR_BACKEND_LOG);
        int nblines = 0;
        int numEvents = 0;
        String line;
        while ((line = bsr.readLine()) != null) {


            // String[] kvLine = line.split("@");
            final Collection<Record> inputRecords = Collections.singleton(RecordUtils.getKeyValueRecord("", line));
            final List<Record> records = new ArrayList<>(instance.get().getProcessor().process(context, inputRecords));

            if (records.isEmpty())
                System.out.println(line);
            else numEvents++;

            nblines++;
            //   Assert.assertTrue(events.size() == 1);
            //   Assert.assertTrue(events.getField(0).entrySet().size() == 35);
        }
        System.out.println("events count :" + numEvents);

        assertTrue(numEvents == 1);


    }


    @Test
    public void testUsrBackend2() throws IOException {


        Map<String, String> conf = new HashMap<>();
        conf.put("value.regex", "\\[(\\S*)\\]\\s+(\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2},\\d{3})\\s+\\[SES:([^:]*):([^\\]]*)\\]\\s*\\[ACC:([^\\]]*)\\]\\[SRV:([^\\]]*)\\]\\s+(\\S*)\\s+(\\S*)\\s+(\\S*)\\s+(.*)\\s*");
        conf.put("value.fields", "raw_content,component,event_time,player_type,session,user_id,srv,log_level,logger,none,trace");

        conf.put("key.regex", "(\\S*):(\\S*)");
        conf.put("key.fields", "es_index,host_name");

        ProcessorConfiguration componentConfiguration = new ProcessorConfiguration();

        componentConfiguration.setComponent("com.hurence.logisland.processor.SplitText");
        componentConfiguration.setType("parser");
        componentConfiguration.setConfiguration(conf);

        Optional<StandardProcessorInstance> instance = ComponentFactory.getProcessorInstance(componentConfiguration);
        assertTrue(instance.isPresent());
        ProcessContext context = new StandardProcessContext(instance.get());

        InputStreamReader isr;
        BufferedReader bsr = null;

        final FileInputStream fis = new FileInputStream(SplitTextTest.class.getResource(DATA_USR_BACKEND_LOG2).getFile());
        isr = new InputStreamReader(fis, "UTF-8");
        bsr = new BufferedReader(isr);

        int nblines = 0;
        String line;
        List<Record> totalRecords = new ArrayList<>();
        while ((line = bsr.readLine()) != null) {


            // String[] kvLine = line.split("@");
            final Collection<Record> inputRecords = Collections.singleton(RecordUtils.getKeyValueRecord("", line));
            final List<Record> records = new ArrayList<>(instance.get().getProcessor().process(context, inputRecords));

            if (records.isEmpty())
                System.out.println(line);
            else totalRecords.addAll(records);

            nblines++;
            //   Assert.assertTrue(events.size() == 1);
            //   Assert.assertTrue(events.getField(0).entrySet().size() == 35);
        }
        System.out.println("events count :" + totalRecords.size());

        assertTrue(totalRecords.size() == 8);


    }

    @Test
    public void testUsrGateway() throws IOException {


        Map<String, String> conf = new HashMap<>();
        conf.put("value.regex", "\\[(\\S*)\\]\\s+(\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2},\\d{3})\\s+\\[SES:(\\S*)\\]\\s+(\\S*)\\s+(\\S*) - (.*)");
        conf.put("value.fields", "raw_content,component,event_time,session,log_level,logger,trace");

        conf.put("key.regex", "(\\S*):(\\S*)");
        conf.put("key.fields", "es_index,host_name");

        ProcessorConfiguration componentConfiguration = new ProcessorConfiguration();

        componentConfiguration.setComponent("com.hurence.logisland.processor.SplitText");
        componentConfiguration.setType("parser");
        componentConfiguration.setConfiguration(conf);

        Optional<StandardProcessorInstance> instance = ComponentFactory.getProcessorInstance(componentConfiguration);
        assertTrue(instance.isPresent());
        ProcessContext context = new StandardProcessContext(instance.get());

        InputStreamReader isr;
        BufferedReader bsr = null;

        final FileInputStream fis = new FileInputStream(SplitTextTest.class.getResource(DATA_USR_GATEWAY_LOG).getFile());
        isr = new InputStreamReader(fis, "UTF-8");
        bsr = new BufferedReader(isr);

        int nblines = 0;
        String line;
        List<Record> totalRecords = new ArrayList<>();
        while ((line = bsr.readLine()) != null) {


            // String[] kvLine = line.split("@");
            final Collection<Record> inputRecords = Collections.singleton(RecordUtils.getKeyValueRecord("", line));
            final List<Record> records = new ArrayList<>(instance.get().getProcessor().process(context, inputRecords));

            if (records.isEmpty())
                System.out.println(line);
            else totalRecords.addAll(records);

            nblines++;
            //   Assert.assertTrue(events.size() == 1);
            //   Assert.assertTrue(events.getField(0).entrySet().size() == 35);
        }
        System.out.println("events count :" + totalRecords.size());

        assertTrue(totalRecords.size() == 73);


    }


    @Test
    public void testLoadConfig() throws Exception {


        Map<String, String> conf = new HashMap<>();
        conf.put("value.regex", "(\\D{3}\\s+\\d{1,2} \\d{2}:\\d{2}:\\d{2})\\s+(\\S*)\\s+(?:date=(\\S*)\\s+)?(?:time=(\\S*)\\s+)?(?:devname=(\\S*)\\s+)?(?:devid=(\\S*)\\s+)?(?:logid=(\\S*)\\s+)?(?:type=(\\S*)\\s+)?(?:subtype=(\\S*)\\s+)?(?:level=(\\S*)\\s+)?(?:vd=(\\S*)\\s+)?(?:srcip=(\\S*)\\s+)?(?:srcport=(\\S*)\\s+)?(?:srcintf=(\\S*)\\s+)?(?:dstip=(\\S*)\\s+)?(?:dstport=(\\S*)\\s+)?(?:dstintf=(\\S*)\\s+)?(?:poluuid=(\\S*)\\s+)?(?:sessionid=(\\S*)\\s+)?(?:proto=(\\S*)\\s+)?(?:action=(\\S*)\\s+)?(?:policyid=(\\S*)\\s+)?(?:dstcountry=(\\S*)\\s+)?(?:srccountry=(\\S* \\S*)\\s+)?(?:trans*disp=(\\S*)\\s+)?(?:trans*ip=(\\S*)\\s+)?(?:trans*port=(\\S*)\\s+)?(?:service=(\\S*)\\s+)?(?:duration=(\\S*)\\s+)?(?:sentbyte=(\\S*)\\s+)?(?:rcvdbyte=(\\S*)\\s+)?(?:sentpkt=(\\S*)\\s+)?(?:rcvdpkt=(\\S*)\\s+)?(?:appcat=(\\S*)\\s+)?(?:crscore=(\\S*)\\s+)?(?:craction=(\\S*)\\s+)?(?:crlevel=(\\S*)\\s+)?");
        conf.put("value.fields", "raw_content,line_date,host,date,time,devname,devid,logid,type,subtype,level,vd,src_ip,src_port,src_inf,dest_ip,dest_port,dest_inf,pol_uuid,session_id,proto,action,policy_id,dest_country,src_country,tran_isp,tran_ip,tran_port,service,duration,bytes_out,bytes_in,packets_out,packets_in,app_cat,cr_score,cr_action,cr_level");

        conf.put("key.regex", "(\\S*):(\\S*)");
        conf.put("key.fields", "es_index,host_name");

        ProcessorConfiguration componentConfiguration = new ProcessorConfiguration();

        componentConfiguration.setComponent(SplitText.class.getName());
        componentConfiguration.setType(ComponentType.PARSER.toString());
        componentConfiguration.setConfiguration(conf);

        Optional<StandardProcessorInstance> instance = ComponentFactory.getProcessorInstance(componentConfiguration);
        assertTrue(instance.isPresent());
        assertTrue(instance.get().isValid());
        ProcessContext context = new StandardProcessContext(instance.get());

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


                String[] kvLine = line.split("@");
                final Collection<Record> inputRecords = Collections.singleton(RecordUtils.getKeyValueRecord(kvLine[0], kvLine[1]));
                final List<Record> records = new ArrayList<>(instance.get().getProcessor().process(context, inputRecords));

                if (records.isEmpty())
                    System.out.println(line);
                else numEvents++;

                nblines++;
                //   Assert.assertTrue(events.size() == 1);
                //   Assert.assertTrue(events.getField(0).entrySet().size() == 35);
            }
            System.out.println("events count :" + numEvents);

            assertTrue(numEvents == nblines);

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
