package com.hurence.logisland.runner;

import com.hurence.logisland.components.ComponentsFactory;
import com.hurence.logisland.config.LogislandSessionConfigReader;
import com.hurence.logisland.config.LogislandSessionConfiguration;
import com.hurence.logisland.engine.StandardEngineContext;
import com.hurence.logisland.engine.StandardEngineInstance;
import com.hurence.logisland.log.StandardParserInstance;
import com.hurence.logisland.processor.StandardProcessorInstance;
import org.apache.commons.cli.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Optional;

/**
 * Created by tom on 06/07/16.
 */
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


        try {
            // parse the command line arguments
            CommandLine line = parser.parse(options, args);
            String configFile = line.getOptionValue("conf");

            // load the YAML config
            LogislandSessionConfiguration sessionConf = new LogislandSessionConfigReader().loadConfig(configFile);

            // instanciate engine and all the processor from the config
            List<StandardParserInstance> parsers = ComponentsFactory.getAllParserInstances(sessionConf);
            List<StandardProcessorInstance> processors = ComponentsFactory.getAllProcessorInstances(sessionConf);
            Optional<StandardEngineInstance> engineInstance = ComponentsFactory.getEngineInstance(sessionConf);

            // start the engine
            if (engineInstance.isPresent()) {
                StandardEngineContext engineContext = new StandardEngineContext(engineInstance.get());
                engineInstance.get().getEngine().start(engineContext, processors, parsers);
            }

        } catch (Exception e) {
            logger.error("unable to launch runner : {}", e);
        }


    }
}
