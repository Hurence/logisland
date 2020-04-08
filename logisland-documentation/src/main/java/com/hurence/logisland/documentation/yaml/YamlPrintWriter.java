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
package com.hurence.logisland.documentation.yaml;

import java.io.OutputStream;
import java.io.PrintWriter;
import java.util.Arrays;

public class YamlPrintWriter extends PrintWriter {

    private static final Character[] SECTION_DELIMITERS = new Character[]{'=', '-', '_', '.', ':', '`', '\'', '\"', '~', '^', '*', '+', '#'};

    public YamlPrintWriter(OutputStream out, boolean autoFlush) {
        super(out, autoFlush);
    }


    public void printDescriptionString(String descriptionString) {
        println(descriptionString);
    }


    public void writeProperty(final int sectionLevel, final String key, final String value) {
        assert key != null;
        assert !key.isEmpty();

        char[] charArray = new char[2 * sectionLevel];
        Arrays.fill(charArray, ' ');
        String delimiter = ": ";

        print(charArray);
        print(key);
        print(delimiter);

        if (value.contains("\n")) {
            String[] desc = value.split("\n");
            println(" >");
            for (String line : desc) {
                print(charArray);
                print("  ");
                println(line);
            }
        } else {
            println(value);
        }


    }


    public void printCsvTable(final String title, final String[] headers, final int[] widths, final Character escape) {
        println();
        print(".. csv-table:: ");
        println(title);


        if (headers != null) {
            StringBuilder strHeaders = new StringBuilder();
            for (int i = 0; i < headers.length; i++) {
                strHeaders.append('"');
                strHeaders.append(headers[i]);
                strHeaders.append('"');
                if (i < headers.length - 1)
                    strHeaders.append(',');
            }
            print("   :header: ");
            println(strHeaders.toString());
        }

        if (widths != null) {
            StringBuilder strWidths = new StringBuilder();
            for (int i = 0; i < widths.length; i++) {
                strWidths.append(widths[i]);
                if (i < widths.length - 1)
                    strWidths.append(',');
            }
            print("   :widths: ");
            println(strWidths.toString());
        }
        if (escape != null) {
            print("   :escape: ");
            println(escape);
        }

        println();
    }
}
