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
package com.hurence.logisland.documentation;

import org.apache.commons.cli.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

/**
 * Uses the ExtensionManager to get a list of Processor, ControllerService, and
 * Connectors classes that were loaded and generate documentation for them.
 */
public class DocGenerator {

    private static final Logger logger = LoggerFactory.getLogger(DocGenerator.class);
    public static final String OUTPUT_FILE = "components";

    private final static String HELP_LONG_OPT ="help";
    private final static String HELP_OPT ="h";
    private final static String DIR_LONG_OPT ="doc-dir";
    private final static String DIR_OPT ="d";
    private final static String FILE_LONG_OPT ="file-name";
    private final static String FILE_OPT ="f";
    private final static String APPEND_LONG_OPT ="append";
    private final static String APPEND_OPT ="a";

    public static void main(String[] args) {
        Options options = new Options();
        options.addOption(OptionBuilder
                .withDescription("Print this help.")
                .withLongOpt(HELP_LONG_OPT)
                .create(HELP_OPT));
        options.addOption(OptionBuilder
                .withDescription("dir to generate documentation")
                .withLongOpt(DIR_LONG_OPT)
                .hasArg()
                .create(DIR_OPT));
        options.addOption(OptionBuilder
                .withDescription("file name to generate documentation about components in classpath")
                .withLongOpt(FILE_LONG_OPT)
                .hasArg()
                .create(FILE_OPT));
        options.addOption(OptionBuilder
                .withDescription("Whether to append or replace file")
                .withLongOpt(APPEND_LONG_OPT)
                .create(APPEND_OPT));

        String dir = ".";
        String fileName = OUTPUT_FILE;
        boolean append = false;

        try {
            final CommandLine commandLine = new PosixParser().parse(options, args);
            System.out.println(commandLine.getArgList());
            if (commandLine.hasOption(HELP_OPT)) {
                printUsage(options);
            }
            if (commandLine.hasOption(DIR_OPT)) {
                dir = commandLine.getOptionValue(DIR_OPT);
            }
            if (commandLine.hasOption(FILE_OPT)) {
                fileName = commandLine.getOptionValue(FILE_OPT);
            }
            if (commandLine.hasOption(APPEND_OPT)) {
                append = true;
            }
        } catch (ParseException e) {
            if (!options.hasOption(HELP_OPT)) {
                System.err.println(e.getMessage());
                System.out.println();
            }
            printUsage(options);
        }

        File rootDocDir = new File(dir);
        DocGeneratorUtils.generate(rootDocDir, fileName, "rst", append);
    }

    private static void printUsage(Options options) {
        System.out.println();
        new HelpFormatter().printHelp(180,
                DocGenerator.class.getCanonicalName(), "\n", options, "\n",
                true);
        System.exit(0);
    }
}
