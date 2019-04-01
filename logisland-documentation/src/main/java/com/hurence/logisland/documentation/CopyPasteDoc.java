/**
 * Copyright (C) 2016 Hurence (support@hurence.com)
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hurence.logisland.documentation;

import org.apache.commons.cli.*;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.FileFilterUtils;
import org.apache.commons.io.filefilter.WildcardFileFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;

/**
 * Uses the ExtensionManager to get a list of Processor, ControllerService, and
 * Connectors classes that were loaded and generate documentation for them.
 */
public class CopyPasteDoc {

    private static final Logger logger = LoggerFactory.getLogger(CopyPasteDoc.class);
    public static final String OUTPUT_FILE = "components";

    private final static String HELP_LONG_OPT ="help";
    private final static String HELP_OPT ="h";
    private final static String DIR_LONG_OPT ="doc-dir";
    private final static String DIR_OPT ="d";

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


        String dir = ".";

        try {
            final CommandLine commandLine = new PosixParser().parse(options, args);
            System.out.println(commandLine.getArgList());
            if (commandLine.hasOption(HELP_OPT)) {
                printUsage(options);
            }
            if (commandLine.hasOption(DIR_OPT)) {
                dir = commandLine.getOptionValue(DIR_OPT);
            }
        } catch (ParseException e) {
            if (!options.hasOption(HELP_OPT)) {
                System.err.println(e.getMessage());
                System.out.println();
            }
            printUsage(options);
        }

        File rootDocDir = new File(dir);

        try {
            copyDirectory(rootDocDir,
                    new File("../logisland-core/logisland-framework/logisland-resources/src/main/resources/docs"),
                    new WildcardFileFilter("*.rst"));

            copyDirectory( new File(rootDocDir, "tutorials"),
                    new File("../logisland-core/logisland-framework/logisland-resources/src/main/resources/docs/tutorials"),
                    new WildcardFileFilter("*.rst"));

            copyDirectory( new File(rootDocDir, "components"),
                    new File("../logisland-core/logisland-framework/logisland-resources/src/main/resources/docs/components"),
                    new WildcardFileFilter("*.rst"));

            copyDirectory( new File(rootDocDir, "components/engines"),
                    new File("../logisland-core/logisland-framework/logisland-resources/src/main/resources/docs/components/engines"),
                    new WildcardFileFilter("*.rst"));

            copyDirectory(new File(rootDocDir, "_static"),
                    new File("../logisland-core/logisland-framework/logisland-resources/src/main/resources/docs/_static"));

        } catch (IOException e) {
            logger.error("I/O error", e);
        }
    }

    private static void copyDirectory(File sourceDir, File destDir, FileFilter filter) throws IOException {
        FileUtils.copyDirectory(sourceDir, destDir, filter);
        logger.info("copied {} into {} with filter {}", sourceDir, destDir, filter);
    }

    private static void copyDirectory(File sourceDir, File destDir) throws IOException {
        FileUtils.copyDirectory(sourceDir, destDir, true);
        logger.info("copied {} into {}", sourceDir, destDir);
    }


    private static void printUsage(Options options) {
        System.out.println();
        new HelpFormatter().printHelp(180,
                CopyPasteDoc.class.getCanonicalName(), "\n", options, "\n",
                true);
        System.exit(0);
    }
}
