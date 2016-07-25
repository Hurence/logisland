package com.hurence.logisland.processor.parser;

import com.hurence.logisland.components.ComponentsFactory;
import com.hurence.logisland.config.LogislandSessionConfigReader;
import com.hurence.logisland.config.LogislandSessionConfiguration;
import com.hurence.logisland.engine.StandardEngineContext;
import com.hurence.logisland.engine.StandardEngineInstance;
import com.hurence.logisland.log.StandardParserInstance;
import com.hurence.logisland.processor.StandardProcessorInstance;
import com.hurence.logisland.utils.kafka.KafkaUnit;
import kafka.producer.KeyedMessage;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static org.junit.Assert.assertEquals;

/**
 * Created by gregoire on 25/07/16.
 */
public class SplitTextMultiLine2Test {

    private static Logger logger = LoggerFactory.getLogger(SplitTextMultiLine2Test.class);
    KafkaUnit kafkaServer = new KafkaUnit(9000, 9001);

    @Before
    public void setUp() throws Exception {
        kafkaServer.startup();
    }

    @After
    public void close() throws Exception {
        kafkaServer.shutdown();
    }

    @Test
    public void testStream() throws Exception {
        try {
            String configFile = SplitTextMultiLine2Test.class.getResource("/test.yml").getFile();

            // load the YAML config
            LogislandSessionConfiguration sessionConf = new LogislandSessionConfigReader().loadConfig(configFile);

            // instanciate engine and all the processor from the config
            List<StandardParserInstance> parsers = ComponentsFactory.getAllParserInstances(sessionConf);
            List<StandardProcessorInstance> processors = ComponentsFactory.getAllProcessorInstances(sessionConf);
            Optional<StandardEngineInstance> engineInstance = ComponentsFactory.getEngineInstance(sessionConf);

            // start the engine
            if (engineInstance.isPresent()) {
                StandardEngineContext engineContext = new StandardEngineContext(engineInstance.get());

                Runnable myRunnable = new Runnable() {
                    @Override
                    public void run() {
                        engineInstance.get().getEngine().start(engineContext, processors, parsers);
                    }
                };
                Thread t = new Thread(myRunnable);
                t.start();
//                Thread.sleep(10000);
                //creates topics
                KeyedMessage<String, String> keyedMessage = new KeyedMessage<>("logisland-mock-in", "10.105.64.777",
                        "2015-11-09 00:00:00.235  INFO   [http-1580-exec-15             ] --- o.a.c.interceptor.LoggingOutInterceptor  - SESSION[PLAYER-WEB-LVS:402206286:3FA833BF4D9BA6AE6389B7357EE897E6:oadp] - USERID[NONE] - Outbound Message\n" +
                        "---------------------------\n" +
                        "ID: 519739\n" +
                        "Response-Code: 200\n" +
                        "Content-Type: application/vnd.lotsys.motors.ongoing.gamesessions.response-1+json\n" +
                        "Headers: {Content-Type=[application/vnd.lotsys.motors.ongoing.gamesessions.response-1+json], Date=[Sun, 08 Nov 2015 23:00:00 GMT]}\n" +
                        "Payload: {\"resultStatus\":\"ok\",\"data\":{\"games\":[]}}\n" +
                        "--------------------------------------");

                //when

                //then
//                assertEquals(Collections.singletonList("2015-11-09 00:00:00.235  INFO   [http-1580-exec-15             ] --- o.a.c.interceptor.LoggingOutInterceptor  - SESSION[PLAYER-WEB-LVS:402206286:3FA833BF4D9BA6AE6389B7357EE897E6:oadp] - USERID[NONE] - Outbound Message\n" +
//                        "---------------------------\n" +
//                        "ID: 519739\n" +
//                        "Response-Code: 200\n" +
//                        "Content-Type: application/vnd.lotsys.motors.ongoing.gamesessions.response-1+json\n" +
//                        "Headers: {Content-Type=[application/vnd.lotsys.motors.ongoing.gamesessions.response-1+json], Date=[Sun, 08 Nov 2015 23:00:00 GMT]}\n" +
//                        "Payload: {\"resultStatus\":\"ok\",\"data\":{\"games\":[]}}\n" +
//                        "--------------------------------------"), kafkaServer.readMessages("logisland-mock-in"));
//                assertEquals(Collections.singletonList("2015-11-09 00:00:00.235  INFO   [http-1580-exec-15             ] --- o.a.c.interceptor.LoggingOutInterceptor  - SESSION[PLAYER-WEB-LVS:402206286:3FA833BF4D9BA6AE6389B7357EE897E6:oadp] - USERID[NONE] - Outbound Message\n" +
//                        "---------------------------\n" +
//                        "ID: 519739\n" +
//                        "Response-Code: 200\n" +
//                        "Content-Type: application/vnd.lotsys.motors.ongoing.gamesessions.response-1+json\n" +
//                        "Headers: {Content-Type=[application/vnd.lotsys.motors.ongoing.gamesessions.response-1+json], Date=[Sun, 08 Nov 2015 23:00:00 GMT]}\n" +
//                        "Payload: {\"resultStatus\":\"ok\",\"data\":{\"games\":[]}}\n" +
//                        "--------------------------------------"), kafkaServer.readMessages("logisland-mock-in"));
//                assertEquals(Collections.singletonList("2015-11-09 00:00:00.235  INFO   [http-1580-exec-15             ] --- o.a.c.interceptor.LoggingOutInterceptor  - SESSION[PLAYER-WEB-LVS:402206286:3FA833BF4D9BA6AE6389B7357EE897E6:oadp] - USERID[NONE] - Outbound Message\n" +
//                        "---------------------------\n" +
//                        "ID: 519739\n" +
//                        "Response-Code: 200\n" +
//                        "Content-Type: application/vnd.lotsys.motors.ongoing.gamesessions.response-1+json\n" +
//                        "Headers: {Content-Type=[application/vnd.lotsys.motors.ongoing.gamesessions.response-1+json], Date=[Sun, 08 Nov 2015 23:00:00 GMT]}\n" +
//                        "Payload: {\"resultStatus\":\"ok\",\"data\":{\"games\":[]}}\n" +
//                        "--------------------------------------"), kafkaServer.readMessages("logisland-mock-in"));


                    kafkaServer.sendMessages(keyedMessage);

//                Thread.sleep(10000);
                int size = 0;
//                while (size == 0) {
//                    kafkaServer.sendMessages(keyedMessage);
//                    Thread.sleep(5000);
//                    logger.info("reading topic");
//                    List<String> messagesOut = kafkaServer.readMessages("logisland-mock-out");
//                    size = messagesOut.size();
//                }

                Assert.assertEquals(1, size);

            }

        } catch (Exception e) {
            logger.error("unable to launch runner : {}", e);
        }
    }


}
