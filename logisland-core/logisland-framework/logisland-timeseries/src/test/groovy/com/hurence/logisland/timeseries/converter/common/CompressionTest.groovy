/*
 * Copyright (C) 2016 QAware GmbH
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */
package com.hurence.logisland.timeseries.converter.common

import org.apache.commons.io.IOUtils
import spock.lang.Specification

/**
 * The unit test for the compression class
 * @author f.lautenschlager
 */
class CompressionTest extends Specification {

    def "test compress and decompress"() {

        when:
        def compressed = Compression.compress(data)
        def decompressed = Compression.decompress(compressed);

        then:
        data == decompressed;

        where:
        data << ["Some Bytes".bytes]

    }

    def "test compress and decompress from stream"() {
        when:
        def compressed = Compression.compressFromStream(new ByteArrayInputStream(data.getBytes("UTF-8")))
        def decompressed = Compression.decompressToStream(compressed)

        then:
        data == IOUtils.toString(decompressed)

        where:
        data << ["Some Bytes"]
    }

    def "test compression null value behaviour"() {
        when:
        def result = Compression.compress(null)
        def decompressedResult = Compression.decompress(null)
        def decompressedResultAsStream = Compression.decompressToStream(null)
        def compressedResultAsStream = Compression.compressFromStream(null)

        then:
        noExceptionThrown()
        result.length == 0
        decompressedResult.length == 0
        decompressedResultAsStream == null
        compressedResultAsStream == null
    }

    def "test decompress exceptional behaviour"() {
        when:
        def notGzipCompressed = "notCompressed".bytes
        def result = Compression.decompress(notGzipCompressed)

        then:
        noExceptionThrown()
        result.length == 0
    }

    def "test private constructor"() {
        when:
        Compression.newInstance()

        then:
        noExceptionThrown()
    }
}
