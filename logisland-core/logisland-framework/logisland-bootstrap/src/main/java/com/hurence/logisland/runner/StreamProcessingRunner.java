/**
 * Copyright (C) 2016 Hurence (support@hurence.com)
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
package com.hurence.logisland.runner;

import com.hurence.logisland.BannerLoader;
import com.hurence.logisland.component.ComponentFactory;
import com.hurence.logisland.config.ConfigReader;
import com.hurence.logisland.config.LogislandConfiguration;
import com.hurence.logisland.engine.EngineContext;
import org.apache.commons.cli.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.channels.IllegalChannelGroupException;
import java.util.Optional;


public class StreamProcessingRunner {

    private static Logger logger = LoggerFactory.getLogger(StreamProcessingRunner.class);


    /**
     * main entry point
     *
     * @param args
     */
    public static void main(String[] args) {

        logger.info("Starting StreamProcessingRunner");

        //////////////////////////////////////////
        // Commande lien management
        Parser parser = new GnuParser();
        Options options = new Options();


        String helpMsg = "Print this message.";
        Option help = new Option("help", helpMsg);
        options.addOption(help);

        OptionBuilder.withArgName("conf");
        OptionBuilder.withLongOpt("config-file");
        OptionBuilder.isRequired();
        OptionBuilder.hasArg();
        OptionBuilder.withDescription("config file path");
        Option conf = OptionBuilder.create("conf");
        options.addOption(conf);

        OptionBuilder.withArgName("databricks");
        OptionBuilder.withLongOpt("databricks-mode");
        OptionBuilder.isRequired(false);
        OptionBuilder.hasArg(false);
        OptionBuilder.withDescription("Databricks mode (configuration is read from DBFS)");
        Option databricks = OptionBuilder.create("databricks");
        options.addOption(databricks);

        Optional<EngineContext> engineInstance = Optional.empty();
        try {
            System.out.println(BannerLoader.loadBanner());

            // parse the command line arguments
            CommandLine line = parser.parse(options, args);
            String configFile = line.getOptionValue("conf");

            // load the YAML config
            LogislandConfiguration sessionConf;

            boolean databricksMode = line.hasOption("databricks");

            if (databricksMode) {
                sessionConf = ConfigReader.loadConfigFromSharedFS(configFile);
            } else {
                sessionConf = ConfigReader.loadConfig(configFile);
            }
            logger.info("Configuration loaded");

            // instantiate engine and all the processor from the config
            engineInstance = ComponentFactory.getEngineContext(sessionConf.getEngine());
            if (!engineInstance.isPresent()) {
                throw new IllegalArgumentException("engineInstance could not be instantiated");
            }
            if (!engineInstance.get().isValid()) {
                throw new IllegalArgumentException("engineInstance is not valid with input configuration !");
            }
            logger.info("Starting Logisland session version {}", sessionConf.getVersion());
            logger.info(sessionConf.getDocumentation());
        } catch (Exception e) {
            logger.error("Unable to launch runner", e);
            System.exit(-1);
        }
        String engineName = engineInstance.get().getEngine().getIdentifier();
        try {
            // start the engine
            EngineContext engineContext = engineInstance.get();
            logger.info("Start engine {}", engineName);
            engineInstance.get().getEngine().start(engineContext);
            logger.info("Waiting termination of engine {}", engineName);
            engineContext.getEngine().awaitTermination(engineContext);
            logger.info("Engine {} terminated", engineName);
            System.exit(0);
        } catch (Exception e) {
            logger.error("Something went bad while running the job {} : {}", engineName, e);
            System.exit(-1);
        }


    }
}
