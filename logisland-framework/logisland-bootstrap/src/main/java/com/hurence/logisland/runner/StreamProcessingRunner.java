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

import com.hurence.logisland.component.ComponentFactory;
import com.hurence.logisland.config.ConfigReader;
import com.hurence.logisland.config.LogislandConfiguration;
import com.hurence.logisland.engine.EngineContext;
import org.apache.commons.cli.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;


public class StreamProcessingRunner {

    private static Logger logger = LoggerFactory.getLogger(StreamProcessingRunner.class);


    /**
     * main entry point
     *
     * @param args
     */
    public static void main(String[] args) {

        logger.info("starting StreamProcessingRunner");

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


        String logisland =
                "██╗      ██████╗  ██████╗   ██╗███████╗██╗      █████╗ ███╗   ██╗██████╗ \n" +
                "██║     ██╔═══██╗██╔════╝   ██║██╔════╝██║     ██╔══██╗████╗  ██║██╔══██╗\n" +
                "██║     ██║   ██║██║  ███╗  ██║███████╗██║     ███████║██╔██╗ ██║██║  ██║\n" +
                "██║     ██║   ██║██║   ██║  ██║╚════██║██║     ██╔══██║██║╚██╗██║██║  ██║\n" +
                "███████╗╚██████╔╝╚██████╔╝  ██║███████║███████╗██║  ██║██║ ╚████║██████╔╝\n" +
                "╚══════╝ ╚═════╝  ╚═════╝   ╚═╝╚══════╝╚══════╝╚═╝  ╚═╝╚═╝  ╚═══╝╚═════╝   v0.14.0\n\n\n";

        System.out.println(logisland);
        Optional<EngineContext> engineInstance = Optional.empty();
        try {
            // parse the command line arguments
            CommandLine line = parser.parse(options, args);
            String configFile = line.getOptionValue("conf");

            // load the YAML config
            LogislandConfiguration sessionConf = ConfigReader.loadConfig(configFile);

            // instantiate engine and all the processor from the config
            engineInstance = ComponentFactory.getEngineContext(sessionConf.getEngine());
            assert engineInstance.isPresent();
            assert engineInstance.get().isValid();

            logger.info("starting Logisland session version {}", sessionConf.getVersion());
            logger.info(sessionConf.getDocumentation());
        } catch (Exception e) {
            logger.error("unable to launch runner : {}", e);
        }

        try {
            // start the engine
            EngineContext engineContext = engineInstance.get();
            engineInstance.get().getEngine().start(engineContext);
            engineContext.getEngine().awaitTermination(engineContext);
        } catch (Exception e) {
            logger.error("something went bad while running the job : {}", e);
            System.exit(-1);
        }


    }
}
