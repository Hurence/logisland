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
package com.hurence.logisland.timeseries.sax;

import com.hurence.logisland.processor.ProcessException;
import com.hurence.logisland.record.Record;
import net.seninp.jmotif.sax.SAXProcessor;
import net.seninp.jmotif.sax.alphabet.NormalAlphabet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class SaxConverter {

    private static Logger logger = LoggerFactory.getLogger(SaxConverter.class.getName());
    public static SAXProcessor saxProcessor = new SAXProcessor();
    public static NormalAlphabet normalAlphabet = new NormalAlphabet();

    private SAXOptions saxOptions;

    private SaxConverter(SAXOptions saxOptions) {
        this.saxOptions = saxOptions;
    }

    private SaxConverter() {
        this(new SAXOptions() {
            @Override
            public int paaSize() {
                return 3;
            }

            @Override
            public double nThreshold() {
                return 0;
            }

            @Override
            public int alphabetSize() {
                return 3;
            }
        });
    }

    public SAXOptions getSaxOptions() {
        return saxOptions;
    }

    /**
     * Compact a related list of records a single chunked one
     *
     * @param records
     * @return
     * @throws ProcessException
     */
    public String getSaxString(List<Record> records) throws ProcessException {
        return null;//TODO
        //find points
//        List<Point> points = extractPoints(records.stream()).collect(Collectors.toList());
//        //compress chunk into binaries
//        if (hasBinaryCompression()) {
//            chunkrecord.setCompressedPoints(compressPoints(points.stream()));
//        }
//        //sax encoding
//        if (hasSaxConverson()) {
//            double[] valuePoints = points.stream().map(Point::getValue).mapToDouble(x -> x).toArray();
//            try {
//                SAXOptions saxOptions = saxConverter.getSaxOptions();
//                char[] saxString = SaxConverter.saxProcessor
//                        .ts2string(valuePoints, saxOptions.paaSize(), SaxConverter.normalAlphabet.getCuts(saxOptions.alphabetSize()), saxOptions.nThreshold());
//                chunkrecord.setSaxPoints(saxString);
//            } catch (SAXException e) {
//                logger.error("error while trying to calculate sax string for chunk", e);
//                chunkrecord.addError(ProcessError.RECORD_CONVERSION_ERROR.toString(), e.getMessage());
//            }
//        }
//        return chunkrecord;
    }


    public static final class Builder {

        private int paaSize = 3;
        private double nThreshold = 0;
        private int alphabetSize = 3;

        public SaxConverter.Builder paaSize(final int paaSize) {
            this.paaSize = paaSize;
            return this;
        }
        public SaxConverter.Builder nThreshold(final double nThreshold) {
            this.nThreshold = nThreshold;
            return this;
        }
        public SaxConverter.Builder alphabetSize(final int alphabetSize) {
            this.alphabetSize = alphabetSize;
            return this;
        }

        /**
         * @return a BinaryCompactionConverter as configured
         *
         */
        public SaxConverter build() {
            return new SaxConverter(new SAXOptionsImpl(paaSize, nThreshold, alphabetSize));
        }
    }
}
