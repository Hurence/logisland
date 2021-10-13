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

import java.lang.reflect.Method;
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

        // Configuration file
        OptionBuilder.withArgName("conf");
        OptionBuilder.withLongOpt("config-file");
        OptionBuilder.isRequired();
        OptionBuilder.hasArg();
        OptionBuilder.withDescription("config file path");
        Option conf = OptionBuilder.create("conf");
        options.addOption(conf);

        // Databricks mode
        OptionBuilder.withArgName("databricks");
        OptionBuilder.withLongOpt("databricks-mode");
        OptionBuilder.isRequired(false);
        OptionBuilder.hasArg(false);
        OptionBuilder.withDescription("Databricks mode (configuration is read from DBFS)");
        Option databricks = OptionBuilder.create("databricks");
        options.addOption(databricks);

        // Checkpoint directory
        OptionBuilder.withArgName("chkploc");
        OptionBuilder.withLongOpt("checkpoint-location");
        OptionBuilder.isRequired(false);
        OptionBuilder.hasArg(true);
        OptionBuilder.withDescription("Checkpoint location used by some engines");
        Option checkpointLocation = OptionBuilder.create("chkploc");
        options.addOption(checkpointLocation);

        // Checkpoint directory
        OptionBuilder.withArgName("strict");
        OptionBuilder.withLongOpt("strict-conf-check");
        OptionBuilder.isRequired(false);
        OptionBuilder.hasArg(false);
        OptionBuilder.withDescription("If set, no not allowed configuration will be tolerated. " +
                "A full check of the conf will be done before running the job. " +
                "This way your conf file will not work if it is not clean.");
        Option checkConfOpt = OptionBuilder.create("strict");
        options.addOption(checkConfOpt);

        Optional<EngineContext> engineInstance = Optional.empty();
        try {
            System.out.println(BannerLoader.loadBanner());

            // parse the command line arguments
            CommandLine line = parser.parse(options, args);
            String configFile = line.getOptionValue("conf");

            // load the YAML config
            LogislandConfiguration sessionConf;

            boolean databricksMode = line.hasOption("databricks");

            if (!databricksMode) {
                sessionConf = ConfigReader.loadConfig(configFile);
            } else {
                logger.info("Running in databricks mode");
                GlobalOptions.databricks = true;
                sessionConf = loadConfigFromSharedFS(configFile);
            }
            logger.info("Configuration loaded");

            // Get checkpoint location if any
            boolean chkploc = line.hasOption("chkploc");
            if (databricksMode && !chkploc) {
                logger.error("Databricks mode requires checkpoint location to be set");
                System.exit(-1);
            }
            GlobalOptions.checkpointLocation = line.getOptionValue("chkploc");
            logger.info("Using checkpoint location: " + GlobalOptions.checkpointLocation);

            boolean strictCheckConf = line.hasOption("strict");
            // instantiate engine and all the processor from the config
            // This init the engine as well
            engineInstance = ComponentFactory.buildAndSetUpEngineContext(sessionConf.getEngine());
            if (!engineInstance.isPresent()) {
                throw new IllegalArgumentException("engineInstance could not be instantiated");
            }
            //At the moment strictCheckConf has no effect ! But we could imagine latter an usage.
            if (!engineInstance.get().isValid(strictCheckConf)) {
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
            logger.info("Init engine {}", engineName);
            engineInstance.get().getEngine().init(engineContext);
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

    /**
     * Loads the configuration from the shared filesystem
     * @param configFile Configuration path to load.
     *                   With databricks, no need to put the 'dbfs:' scheme: use /foo/logisland.yml instead of
     *                   dbfs:/foo/logisland.yml
     * @return The read configuration
     */
    private static LogislandConfiguration loadConfigFromSharedFS(String configFile) {

        // Load the spark config reader. This is only expected to work if a spark 2+ engine is being
        // used and available in the classpath (which should be the case in the azure databricks environment)
        Class<?> sparkConfigReaderClass = null;
        try {
            sparkConfigReaderClass = Class.forName("com.hurence.logisland.util.spark.SparkConfigReader");
        } catch(Exception e) {
            logger.error("Could not load the SparkConfigReader class", e);
            System.exit(-1);
        }
        // Prepare to call the loadConfigFromSharedFS method
        Method loadConfigFromSharedFSMethod = null;
        try {
            loadConfigFromSharedFSMethod = sparkConfigReaderClass.getMethod("loadConfigFromSharedFS", String.class);
        } catch (Exception e) {
            logger.error("Could not find method loadConfigFromSharedFS in SparkConfigReader class", e);
            System.exit(-1);
        }
        // Call the loadConfigFromSharedFS method to read the configuration from the shared filesystem
        LogislandConfiguration LogislandConfiguration = null;
        try {
            LogislandConfiguration = (LogislandConfiguration)loadConfigFromSharedFSMethod.invoke(null, configFile);
        } catch(Exception e) {
            logger.error("Could not load configuration from shared filesystem", e);
            System.exit(-1);
        }

        return LogislandConfiguration;
    }
}
