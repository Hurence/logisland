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

import com.hurence.logisland.component.RestComponentFactory;
import com.hurence.logisland.engine.EngineContext;
import org.apache.commons.cli.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;


public class SparkJobLauncher {

    public static final String AGENT = "agent";
    public static final String JOB = "job";
    private static Logger logger = LoggerFactory.getLogger(SparkJobLauncher.class);


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

        OptionBuilder.withArgName(AGENT);
        OptionBuilder.withLongOpt("agent-quorum");
        OptionBuilder.isRequired();
        OptionBuilder.hasArg();
        OptionBuilder.withDescription("logisland agent quorum like host1:8081,host2:8081");
        Option agent = OptionBuilder.create(AGENT);
        options.addOption(agent);

        OptionBuilder.withArgName(JOB);
        OptionBuilder.withLongOpt("job-name");
        OptionBuilder.isRequired();
        OptionBuilder.hasArg();
        OptionBuilder.withDescription("logisland agent quorum like host1:8081,host2:8081");
        Option job = OptionBuilder.create(JOB);
        options.addOption(job);

        String logisland =
                "██╗      ██████╗  ██████╗   ██╗███████╗██╗      █████╗ ███╗   ██╗██████╗ \n" +
                "██║     ██╔═══██╗██╔════╝   ██║██╔════╝██║     ██╔══██╗████╗  ██║██╔══██╗\n" +
                "██║     ██║   ██║██║  ███╗  ██║███████╗██║     ███████║██╔██╗ ██║██║  ██║\n" +
                "██║     ██║   ██║██║   ██║  ██║╚════██║██║     ██╔══██║██║╚██╗██║██║  ██║\n" +
                "███████╗╚██████╔╝╚██████╔╝  ██║███████║███████╗██║  ██║██║ ╚████║██████╔╝\n" +
                "╚══════╝ ╚═════╝  ╚═════╝   ╚═╝╚══════╝╚══════╝╚═╝  ╚═╝╚═╝  ╚═══╝╚═════╝   v0.12.0-SNAPSHOT\n\n\n";

        System.out.println(logisland);
        Optional<EngineContext> engineInstance = Optional.empty();
        try {
            // parse the command line arguments
            CommandLine line = parser.parse(options, args);
            String agentQuorum = line.getOptionValue(AGENT);
            String jobName = line.getOptionValue(JOB);



            // instanciate engine and all the processor from the config
            engineInstance = new RestComponentFactory(agentQuorum).getEngineContext(jobName);
            assert engineInstance.isPresent();
            assert engineInstance.get().isValid();

            logger.info("starting Logisland session version {}", engineInstance.get());
        } catch (Exception e) {
            logger.error("unable to launch runner : {}", e.toString());
        }

        try {
            // start the engine
            EngineContext engineContext = engineInstance.get();
            engineInstance.get().getEngine().start(engineContext);
        } catch (Exception e) {
            logger.error("something went bad while running the job : {}", e);
            System.exit(-1);
        }


    }
}
