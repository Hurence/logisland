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
package com.hurence.logisland.timeseries.converter.common;

import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

/**
 * Class that provides a simple gzip compression and decompression
 *
 * @author f.lautenschlager
 */
public final class Compression {

    private static final Logger LOGGER = LoggerFactory.getLogger(Compression.class);

    private Compression() {
        //avoid instances
    }

    /**
     * Compresses the given byte[]
     *
     * @param decompressed - the byte[] to compress
     * @return the byte[] compressed
     */
    public static byte[] compress(byte[] decompressed) {
        if (decompressed == null) {
            return new byte[]{};
        }
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream(decompressed.length);
        OutputStream gzipOutputStream = null;
        try {
            gzipOutputStream = new GZIPOutputStream(byteArrayOutputStream);
            gzipOutputStream.write(decompressed);
            gzipOutputStream.flush();
            byteArrayOutputStream.flush();
        } catch (IOException e) {
            LOGGER.error("Exception occurred while compressing gzip stream.", e);
            return null;
        } finally {
            IOUtils.closeQuietly(gzipOutputStream);
            IOUtils.closeQuietly(byteArrayOutputStream);
        }

        return byteArrayOutputStream.toByteArray();
    }

    /**
     * Decompressed the given byte[]
     *
     * @param compressed - the compressed byte[]
     * @return an input stream of the uncompressed byte[]
     */
    public static byte[] decompress(byte[] compressed) {

        if (compressed == null) {
            LOGGER.debug("Compressed bytes[] are null. Returning empty byte[].");
            return new byte[]{};
        }
        try {
            InputStream decompressed = decompressToStream(compressed);
            if (decompressed != null) {
                return IOUtils.toByteArray(decompressed);
            }

        } catch (IOException e) {
            LOGGER.error("Exception occurred while decompressing gzip stream. Returning empty byte[].", e);
        }
        return new byte[]{};
    }

    /**
     * Decompresses the given byte[]
     *
     * @param compressed - the compressed bytes
     * @return an input stream on the decompressed bytes
     */
    public static InputStream decompressToStream(byte[] compressed) {
        if (compressed == null) {
            LOGGER.debug("Compressed bytes[] are null. Returning null.");
            return null;
        }
        try {
            return new GZIPInputStream(new ByteArrayInputStream(compressed));
        } catch (IOException e) {
            LOGGER.error("Exception occurred while decompressing gzip stream. Returning null.", e);
        }
        return null;
    }

    /***
     * Compressed the given stream using gzip.
     *
     * @param stream the input stream
     * @return an byte[] with the compressed data from the stream
     */
    public static byte[] compressFromStream(InputStream stream) {

        if (stream == null) {
            LOGGER.debug("Stream is null. Returning null.");
            return null;

        }

        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        OutputStream zippedStream = null;
        try {

            zippedStream = new GZIPOutputStream(byteArrayOutputStream);

            int nRead;
            byte[] data = new byte[16384];

            while ((nRead = stream.read(data, 0, data.length)) != -1) {
                zippedStream.write(data, 0, nRead);
            }
            zippedStream.flush();
            byteArrayOutputStream.flush();

        } catch (IOException e) {
            LOGGER.error("Exception occurred while compressing gzip stream.", e);
            return null;
        } finally {
            IOUtils.closeQuietly(zippedStream);
            IOUtils.closeQuietly(byteArrayOutputStream);
        }
        return byteArrayOutputStream.toByteArray();
    }
}
