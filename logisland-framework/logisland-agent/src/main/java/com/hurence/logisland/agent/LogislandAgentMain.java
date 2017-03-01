package com.hurence.logisland.agent;

import com.hurence.logisland.kakfa.registry.KafkaRegistryConfig;
import com.hurence.logisland.kakfa.registry.KafkaRegistryRestApplication;
import io.confluent.rest.RestConfigException;
import org.eclipse.jetty.server.Server;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class LogislandAgentMain {

    private static final org.slf4j.Logger log = LoggerFactory.getLogger(LogislandAgentMain.class);

    /**
     * Starts an embedded Jetty server running the REST server.
     */
    public static void main(String[] args) throws IOException {

        try {
            if (args.length != 1) {
                log.error("Properties file is required to start the schema registry REST instance");
                System.exit(1);
            }


            KafkaRegistryConfig config = new KafkaRegistryConfig(args[0]);
            KafkaRegistryRestApplication app = new KafkaRegistryRestApplication(config);
            Server server = app.createServer();
            server.start();
            log.info("Server started, listening for requests...");
            server.join();
        } catch (RestConfigException e) {
            log.error("Server configuration failed: ", e);
            System.exit(1);
        } catch (Exception e) {
            log.error("Server died unexpectedly: ", e);
            System.exit(1);
        }
    }
}
