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
package com.hurence.logisland.documentation.rst;

import java.io.OutputStream;
import java.io.PrintWriter;
import java.util.Arrays;

public class RstPrintWriter extends PrintWriter {

    private static final Character[] SECTION_DELIMITERS = new Character[]{'=', '-', '_', '.', ':', '`', '\'', '\"', '~', '^', '*', '+', '#'};

    public RstPrintWriter(OutputStream out, boolean autoFlush) {
        super(out, autoFlush);
    }

    /**
     * Writes a section title followed by an RST delimiter
     *
     * @param sectionLevel
     * @param title
     */
    public void writeSectionTitle(final int sectionLevel, final String title) {
        assert sectionLevel < SECTION_DELIMITERS.length;
        assert title != null;
        assert !title.isEmpty();

        char[] charArray = new char[title.length()];
        Arrays.fill(charArray, SECTION_DELIMITERS[sectionLevel - 1]);
        String delimiter = new String(charArray);

        println();
        println(title);
        println(delimiter);
    }


    /**
     * Writes a transistion. Transitions are commonly seen in novels and short fiction,
     * as a gap spanning one or more lines, with or without a type ornament such as a row of asterisks.
     * Transitions separate other body elements.
     * A transition should not begin or end a section or document,
     * nor should two transitions be immediately adjacent.
     * <p>
     * The syntax for a transition marker is a horizontal line of 4 or more repeated punctuation characters.
     * The syntax is the same as section title underlines without title text.
     * Transition markers require blank lines before and after:
     */
    public void writeTransition() {
        char[] charArray = new char[10];
        Arrays.fill(charArray, SECTION_DELIMITERS[1]);
        String delimiter = new String(charArray);

        println();
        println(delimiter);
        println();
    }


    /**
     * Writes a begin element, an id attribute(if specified), then text, then
     * end element for element of the users choosing. Example: &lt;p
     * id="p-id"&gt;text&lt;/p&gt;
     *
     * @param characters the text of the element
     */
    public void printStrong(final String characters) {

        print("**");
        print(characters);
        print("**");

    }


    /**
     * A helper method to write a link
     *
     * @param text     the text of the link
     * @param location the location of the link
     */
    public void writeLink(final String text, final String location) {
        print(" `");
        print(text);
        print(" <");
        print(location);
        print(">`_ ");
    }


    /**
     * A helper method to write a crossreference target
     * <p>
     * .. _example:
     *
     * @param name the text of the target
     */
    public void writeInternalReference(final String name) {
        print(".. _");
        print(name);
        print(": ");
        println();
    }

    /**
     * A helper method to write an internal link
     *
     * @param name the text of the link
     */
    public void writeInternalReferenceLink(final String name) {
        print("`");
        print(name);
        print("`_ ");
    }


    /**
     * .. image:: picture.jpeg
     * :height: 100px
     * :width: 200 px
     * :scale: 50 %
     * :alt: alternate text
     * :align: right
     * <p>
     * print(".. image:: ");
     */
    public void writeImage(final String imageSrc, final String alt, final String align, final Integer height, final Integer width, final Integer scale) {

        print(".. image:: ");
        println(imageSrc);
        if (alt != null) {
            print("   :alt: ");
            println(alt);
        }
        if (align != null) {
            print("   :align: ");
            println(align);
        }
        if (height != null) {
            print("   :height: ");
            print(height);
            println(" px");
        }
        if (width != null) {
            print("   :width: ");
            print(width);
            println(" px");
        }
        if (scale != null) {
            print("   :scale: ");
            print(scale);
            println(" %");
        }
    }

    /**
     * A helper method to write an unordered list item
     *
     * @param content the text
     */
    public void printListItem(final String content) {
        print("- ");
        println(content);
    }


    public void printCsvTable(final String title, final String[] headers, final int[] widths) {
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

        println();
    }
}
