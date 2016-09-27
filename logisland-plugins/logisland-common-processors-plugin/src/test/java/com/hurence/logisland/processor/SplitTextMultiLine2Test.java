package com.hurence.logisland.processor;

import com.hurence.logisland.config.ComponentFactory;
import com.hurence.logisland.config.ConfigReader;
import com.hurence.logisland.config.LogislandConfiguration;
import com.hurence.logisland.engine.StandardEngineContext;
import com.hurence.logisland.engine.StandardEngineInstance;
import com.hurence.logisland.engine.StreamProcessingEngine;
import com.hurence.logisland.record.Record;
import com.hurence.logisland.serializer.KryoSerializer;
import com.hurence.logisland.serializer.RecordSerializer;
import com.hurence.logisland.utils.kafka.KafkaUnit;
import kafka.producer.KeyedMessage;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.List;
import java.util.Optional;


public class SplitTextMultiLine2Test {

    private static Logger logger = LoggerFactory.getLogger(SplitTextMultiLine2Test.class);
    KafkaUnit kafkaServer = new KafkaUnit(2181, 9092);

    @Before
    public void setUp() throws Exception {
        kafkaServer.startup();
    }

    @After
    public void close() throws Exception {
        kafkaServer.shutdown();
    }

    @Test
    public void testStreamSplitTextMultiLine() throws Exception {
        try {
            logger.info("START JOB");
            String configFile = SplitTextMultiLine2Test.class.getResource("/traker.yml").getFile();

            // load the YAML config
            LogislandConfiguration sessionConf = ConfigReader.loadConfig(configFile);

            // instanciate engine and all the processor from the config
          /*  List<StandardParserInstance> parsers = ComponentFactory.getAllParserInstances(sessionConf);
            List<StandardProcessorInstance> processors = ComponentFactory.getAllProcessorInstances(sessionConf);*/
            Optional<StandardEngineInstance> engineInstance = ComponentFactory.getEngineInstance(sessionConf.getEngine());

            // start the engine
            if (engineInstance.isPresent()) {
                StandardEngineContext engineContext = new StandardEngineContext(engineInstance.get());

                Runnable myRunnable = new Runnable() {
                    @Override
                    public void run() {
                        engineInstance.get().getEngine().start(engineContext);
                    }
                };
                Thread t = new Thread(myRunnable);
                logger.info("STARTING THREAD {}", t.getId());
                t.start();
//                Thread.sleep(10000);
                //creates topics
                logger.info("sending input into topic {}", "logisland-mock-in");
                KeyedMessage<String, String> keyedMessage = new KeyedMessage<>("logisland-mock-in", "10.105.64.777",
                        "2015-11-09 00:00:00.235  INFO   [http-1580-exec-15             ] --- o.a.c.interceptor.LoggingOutInterceptor  - SESSION[PLAYER-WEB-LVS:402206286:3FA833BF4D9BA6AE6389B7357EE897E6:oadp] - USERID[NONE] - Outbound Message\n" +
                                "---------------------------\n" +
                                "ID: 519739\n" +
                                "Response-Code: 200\n" +
                                "Content-Type: application/vnd.lotsys.motors.ongoing.gamesessions.response-1+json\n" +
                                "Headers: {Content-Type=[application/vnd.lotsys.motors.ongoing.gamesessions.response-1+json], Date=[Sun, 08 Nov 2015 23:00:00 GMT]}\n" +
                                "Payload: {\"resultStatus\":\"ok\",\"data\":{\"games\":[]}}\n" +
                                "--------------------------------------");


                kafkaServer.sendMessages(keyedMessage);
//                Thread.sleep(10000);
                int size = 0;
                while (size == 0) {
                    kafkaServer.sendMessages(keyedMessage);
                    Thread.sleep(5000);
                    logger.info("reading topic");
                    List<String> messagesOut = kafkaServer.readMessages("logisland-mock-out");
                    size = messagesOut.size();
                }

                Assert.assertEquals(1, size);

            }
            logger.info("END JOB");
        } catch (Exception e) {
            logger.error("unable to launch runner :", e);
        }
    }

    @Test
    public void testStreamSplitText() throws Exception {
        try {
            logger.info("START JOB");
            String configFile = SplitTextMultiLine2Test.class.getResource("/traker.yml").getFile();

            // load the YAML config
            LogislandConfiguration sessionConf = new ConfigReader().loadConfig(configFile);

            // instanciate engine and all the processor from the config
            /*  List<StandardParserInstance> parsers = ComponentFactory.getAllParserInstances(sessionConf);
            List<StandardProcessorInstance> processors = ComponentFactory.getAllProcessorInstances(sessionConf);*/
            Optional<StandardEngineInstance> engineInstanceO = ComponentFactory.getEngineInstance(sessionConf.getEngine());

            // start the engine
            if (engineInstanceO.isPresent()) {
                StandardEngineContext engineContext = new StandardEngineContext(engineInstanceO.get());


                Thread t = new Thread(new Runnable() {
                    @Override
                    public void run() {
                        engineInstanceO.get().getEngine().start(engineContext);
                    }
                });
                StreamProcessingEngine engineInstance = engineInstanceO.get().getEngine();

                logger.info("STARTING THREAD that start engine ,id:{}, name: {}", t.getId(), t.getName());
                t.start();
                Thread.sleep(5000);
                //creates topics

                logger.info("sending input into topic {}", "logisland-mock-in");
                KeyedMessage<String, String> keyedMessage = new KeyedMessage<>("logisland-mock-in", "10.105.64.777",
                        "Jul 21 00:59:54 traker1.prod.fdj.fr/traker1.prod.fdj.fr date=2016-07-21 time=00:59:53 devname=traker1M devid=FG3K0B3I11700181 logid=0000000013 type=traffic subtype=forward level=notice vd=root srcip=10.3.41.100 srcport=22769 srcintf=\"port15\" dstip=194.2.0.20 dstport=53 dstintf=\"aggregate-inet\" poluuid=7d2ce9d0-bd34-51e4-80b8-1ab8854b36cb sessionid=2881568097 proto=17 action=accept policyid=259 dstcountry=\"France\" srccountry=\"Reserved\" trandisp=snat transip=194.4.210.8 transport=22769 service=\"DNS\" duration=40 sentbyte=80 rcvdbyte=40 sentpkt=1 rcvdpkt=1 appcat=\"unscanned\"");

                //when


                kafkaServer.sendMessages(keyedMessage);

//                Thread.sleep(10000);
                int size = 0;
                List<Record> messagesOut = new LinkedList();
                List<Record> messagesOutErr = new LinkedList();
                RecordSerializer serializer = new KryoSerializer(true);
                while (size == 0) {
                    kafkaServer.sendMessages(keyedMessage);
                    Thread.sleep(5000);
                    logger.info("reading topic");
                    messagesOut = kafkaServer.readEvent("logisland-mock-out", serializer);
                    size = messagesOut.size();
                    messagesOutErr = kafkaServer.readEvent("logisland-error", serializer);
                    if (size == 0) {
                        size = messagesOutErr.size();
                    }
                }
                Assert.assertNotEquals(0, size);
                Record record = messagesOut.get(0);

                Assert.assertEquals(33, record.getAllFieldNames().size());
                Assert.assertEquals("2016-07-21", (String) record.getField("date").getRawValue());
                Assert.assertEquals("FG3K0B3I11700181", (String) record.getField("devid").getRawValue());
                Assert.assertEquals("259", (String) record.getField("policy_id").getRawValue());
                Assert.assertEquals("40", (String) record.getField("bytes_in").getRawValue());
                Assert.assertEquals("1", (String) record.getField("packets_out").getRawValue());
                Assert.assertEquals("traffic", (String) record.getField("type").getRawValue());
                Assert.assertEquals("10.3.41.100", (String) record.getField("src_ip").getRawValue());
                Assert.assertEquals("40", (String) record.getField("duration").getRawValue());
                Assert.assertEquals("port15", (String) record.getField("src_inf").getRawValue());
                Assert.assertEquals("aggregate-inet", (String) record.getField("dest_inf").getRawValue());
                Assert.assertEquals("forward", (String) record.getField("subtype").getRawValue());
                Assert.assertEquals("traker1.prod.fdj.fr/traker1.prod.fdj.fr", (String) record.getField("host").getRawValue());
                Assert.assertEquals("accept", (String) record.getField("action").getRawValue());
                Assert.assertEquals("traker1M", (String) record.getField("devname").getRawValue());
                Assert.assertEquals("53", (String) record.getField("dest_port").getRawValue());
                Assert.assertEquals("194.4.210.8", (String) record.getField("tran_ip").getRawValue());
                Assert.assertEquals("Jul 21 00:59:54 traker1.prod.fdj.fr/traker1.prod.fdj.fr date=2016-07-21 time=00:59:53 devname=traker1M devid=FG3K0B3I11700181 logid=0000000013 type=traffic subtype=forward level=notice vd=root srcip=10.3.41.100 srcport=22769 srcintf=\"port15\" dstip=194.2.0.20 dstport=53 dstintf=\"aggregate-inet\" poluuid=7d2ce9d0-bd34-51e4-80b8-1ab8854b36cb sessionid=2881568097 proto=17 action=accept policyid=259 dstcountry=\"France\" srccountry=\"Reserved\" trandisp=snat transip=194.4.210.8 transport=22769 service=\"DNS\" duration=40 sentbyte=80 rcvdbyte=40 sentpkt=1 rcvdpkt=1 appcat=\"unscanned\"", (String) record.getField("raw_content").getRawValue());
                Assert.assertEquals("notice", (String) record.getField("level").getRawValue());
                Assert.assertEquals("2881568097", (String) record.getField("session_id").getRawValue());
                Assert.assertEquals("1", (String) record.getField("packets_in").getRawValue());
                Assert.assertEquals("root", (String) record.getField("vd").getRawValue());
                Assert.assertEquals("22769", (String) record.getField("src_port").getRawValue());
                Assert.assertEquals("Jul 21 00:59:54", (String) record.getField("line_date").getRawValue());
                Assert.assertEquals("80", (String) record.getField("bytes_out").getRawValue());
                Assert.assertEquals("Reserved trandisp=snat", (String) record.getField("src_country").getRawValue());
                Assert.assertEquals("DNS", (String) record.getField("service").getRawValue());
                Assert.assertEquals("194.2.0.20", (String) record.getField("dest_ip").getRawValue());
                Assert.assertEquals("17", (String) record.getField("proto").getRawValue());
                Assert.assertEquals("7d2ce9d0-bd34-51e4-80b8-1ab8854b36cb", (String) record.getField("pol_uuid").getRawValue());
                Assert.assertEquals("0000000013", (String) record.getField("logid").getRawValue());
                Assert.assertEquals("00:59:53", (String) record.getField("time").getRawValue());
                Assert.assertEquals("22769", (String) record.getField("tran_port").getRawValue());
                Assert.assertEquals("France", (String) record.getField("dest_country").getRawValue());
                Assert.assertEquals("Tue Jul 26 12:35:52 CEST 2016", (String) record.getField("creationDate").getRawValue());

            }
            logger.info("END JOB");
        } catch (Exception e) {
            logger.error("unable to launch runner :", e);
        }
    }


}
