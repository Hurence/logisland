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
package com.hurence.logisland.timeseries.converter.serializer.protobuf

import com.hurence.logisland.timeseries.converter.common.Compression
import com.hurence.logisland.timeseries.MetricTimeSeries
import com.hurence.logisland.record.Point
import spock.lang.Shared
import spock.lang.Specification
import spock.lang.Unroll

import java.text.DecimalFormat
import java.time.Instant
import java.util.stream.Collectors
import java.util.zip.GZIPInputStream

/**
 * Unit test for the protocol buffers serializer
 * @author f.lautenschlager
 */
class ProtoBufMetricGenericTimeSeriesSerializerTest extends Specification {

    def "test from without range query"() {
        given:
        def points = []
        100.times {
            points.add(new Point(it, it, it * 100))
        }
        def compressedProtoPoints = ProtoBufMetricTimeSeriesSerializer.to(points.iterator())

        when:
        def builder = new MetricTimeSeries.Builder("name", "metric")
        ProtoBufMetricTimeSeriesSerializer.from(new ByteArrayInputStream(compressedProtoPoints), 0, points.size(), 0, points.size(), builder)
        def ts = builder.build()
        then:
        100.times {
            ts.getValue(it) == it * 100
            ts.getTime(it) == it
        }
    }

    @Shared
    def start = Instant.now()
    @Shared
    def end = start.plusSeconds(100 * 100)

    def "test from with range query"() {
        given:
        def points = []

        100.times {
            points.add(new Point(it, start.plusSeconds(it).toEpochMilli(), it * 100))
        }
        def serializedPoints = ProtoBufMetricTimeSeriesSerializer.to(points.iterator())
        def builder = new MetricTimeSeries.Builder("name", "metric")

        when:
        ProtoBufMetricTimeSeriesSerializer.from(new ByteArrayInputStream(serializedPoints), start.toEpochMilli(), end.toEpochMilli(), from, to, builder)
        def ts = builder.build()

        then:
        List<Point> list = ts.points().collect(Collectors.toList())
        list.size() == size
        if (size == 21) {
            list.get(0).timestamp == 1456394850774
            list.get(0).value == 5000.0d

            list.get(20).timestamp == 1456394870774
            list.get(20).value == 7000.0d
        }

        where:
        from << [end.toEpochMilli() + 2, 0, start.toEpochMilli() + 4, start.plusSeconds(50).toEpochMilli()]
        to << [end.toEpochMilli() + 3, 0, start.toEpochMilli() + 2, start.plusSeconds(70).toEpochMilli()]
        size << [0, 0, 0, 21]
    }

    def "test convert to protocol buffers points"() {
        given:
        def points = []
        100.times {
            points.add(new Point(it, it + 15, it * 100))
        }
        //Points that are null are ignored
        points.add(null)
        def builder = new MetricTimeSeries.Builder("", "metric")
        when:
        def serializedPoints = ProtoBufMetricTimeSeriesSerializer.to(points.iterator())
        ProtoBufMetricTimeSeriesSerializer.from(new ByteArrayInputStream(serializedPoints), 0, 114, builder)

        then:
        builder.build().size() == 100
    }

    def "test iterator with invalid arguments"() {
        when:
        ProtoBufMetricTimeSeriesSerializer.from(null, 0, 0, from, to, new MetricTimeSeries.Builder("", ""))
        then:
        thrown IllegalArgumentException
        where:
        from << [-1, 0, -1]
        to << [0, -1, -1]

    }


    def "test private constructor"() {
        when:
        ProtoBufMetricTimeSeriesSerializer.newInstance()
        then:
        noExceptionThrown()
    }

    def "test date-delta-compaction with almost_equals = 0"() {
        given:
        def points = []
        points.add(new Point(0, 1, 10))
        points.add(new Point(1, 5, 20))
        points.add(new Point(2, 8, 30))
        points.add(new Point(3, 16, 40))
        points.add(new Point(4, 21, 50))

        def builder = new MetricTimeSeries.Builder("name", "metric")

        when:
        def serializedPoints = ProtoBufMetricTimeSeriesSerializer.to(points.iterator())
        ProtoBufMetricTimeSeriesSerializer.from(new ByteArrayInputStream(serializedPoints), 1l, 1036l, 1l, 1036l, builder)
        def ts = builder.build()
        def listPoints = ts.points().collect(Collectors.toList()) as List<Point>

        then:
        listPoints.get(0).timestamp == 1
        listPoints.get(1).timestamp == 5
        listPoints.get(2).timestamp == 8
        listPoints.get(3).timestamp == 16
        listPoints.get(4).timestamp == 21

    }

    def "test date-delta-compaction used in the paper"() {
        given:
        def points = []
        points.add(new Point(0, 1, 10))
        points.add(new Point(1, 5, 20))
        points.add(new Point(2, 8, 30))
        points.add(new Point(3, 16, 40))
        points.add(new Point(4, 21, 50))

        def builder = new MetricTimeSeries.Builder("name", "metric")

        when:
        def serializedPoints = ProtoBufMetricTimeSeriesSerializer.to(points.iterator(), 4)
        ProtoBufMetricTimeSeriesSerializer.from(new ByteArrayInputStream(serializedPoints), 1l, 1036l, builder)
        def ts = builder.build()
        def listPoints = ts.points().collect(Collectors.toList()) as List<Point>

        then:
        listPoints.get(0).timestamp == 1//offset: 4
        listPoints.get(1).timestamp == 5//offset: 4
        listPoints.get(2).timestamp == 9//offset: 4
        listPoints.get(3).timestamp == 16//offset: 7
        listPoints.get(4).timestamp == 21//offset: 7

    }


    def "test date-delta-compaction"() {
        given:
        def points = []
        points.add(new Point(0, 10, -10))
        points.add(new Point(1, 20, -20))
        points.add(new Point(2, 30, -30))
        points.add(new Point(3, 39, -39))
        points.add(new Point(4, 48, -48))
        points.add(new Point(5, 57, -57))
        points.add(new Point(6, 66, -66))
        points.add(new Point(7, 75, -75))
        points.add(new Point(8, 84, -84))
        points.add(new Point(9, 93, -93))
        points.add(new Point(10, 102, -102))
        points.add(new Point(11, 111, -109))
        points.add(new Point(12, 120, -118))
        points.add(new Point(13, 129, -127))
        points.add(new Point(14, 138, -136))

        def builder = new MetricTimeSeries.Builder("name", "metric")

        when:
        def serializedPoints = ProtoBufMetricTimeSeriesSerializer.to(points.iterator(), 10)
        ProtoBufMetricTimeSeriesSerializer.from(new ByteArrayInputStream(serializedPoints), 10l, 1036l, builder)
        def ts = builder.build()
        def listPoints = ts.points().collect(Collectors.toList()) as List<Point>

        then:                            //diff to origin
        listPoints.get(0).timestamp == 10//0
        listPoints.get(1).timestamp == 20//0
        listPoints.get(2).timestamp == 30//0
        listPoints.get(3).timestamp == 40//1
        listPoints.get(4).timestamp == 50//2
        listPoints.get(5).timestamp == 60//3
        listPoints.get(6).timestamp == 70//4
        listPoints.get(7).timestamp == 75//5 drift detected OK

        listPoints.get(8).timestamp == 84//9
        listPoints.get(9).timestamp == 93//9
        listPoints.get(10).timestamp == 102//9
        listPoints.get(11).timestamp == 111//9
        listPoints.get(12).timestamp == 120//9
        listPoints.get(13).timestamp == 129//9
        listPoints.get(14).timestamp == 138//9
    }


    def "test date-delta-compaction with different values"() {
        given:
        def points = []
        points.add(new Point(0, 1462892410, 10))
        points.add(new Point(1, 1462892420, 20))
        points.add(new Point(2, 1462892430, 30))
        points.add(new Point(3, 1462892439, 39))
        points.add(new Point(4, 1462892448, 48))
        points.add(new Point(5, 1462892457, 57))
        points.add(new Point(6, 1462892466, 66))
        points.add(new Point(7, 1462892475, 10))
        points.add(new Point(8, 1462892484, 84))
        points.add(new Point(9, 1462892493, 93))
        points.add(new Point(10, 1462892502, -102))
        points.add(new Point(11, 1462892511, 109))
        points.add(new Point(12, 1462892520, 118))
        points.add(new Point(13, 1462892529, 127))
        points.add(new Point(14, 1462892538, 136))

        def builder = new MetricTimeSeries.Builder("metric1", "metric")

        when:
        def serializedPoints = ProtoBufMetricTimeSeriesSerializer.to(points.iterator(), 10)
        ProtoBufMetricTimeSeriesSerializer.from(new ByteArrayInputStream(serializedPoints), 1462892410L, 1462892538L, builder)
        def ts = builder.build()
        def listPoints = ts.points().collect(Collectors.toList()) as List<Point>

        then:                            //diff to origin
        listPoints.get(0).timestamp == 1462892410//0
        listPoints.get(0).value == 10//0

        listPoints.get(1).timestamp == 1462892420//0
        listPoints.get(1).value == 20//0

        listPoints.get(2).timestamp == 1462892430//0
        listPoints.get(2).value == 30//0

        listPoints.get(3).timestamp == 1462892440//1
        listPoints.get(3).value == 39//0

        listPoints.get(4).timestamp == 1462892450//2
        listPoints.get(4).value == 48//0

        listPoints.get(5).timestamp == 1462892460//3
        listPoints.get(5).value == 57//0

        listPoints.get(6).timestamp == 1462892470//4
        listPoints.get(6).value == 66//0

        listPoints.get(7).timestamp == 1462892475//5
        listPoints.get(7).value == 10//0

        listPoints.get(8).timestamp == 1462892484//5
        listPoints.get(8).value == 84//0

        listPoints.get(9).timestamp == 1462892493//5
        listPoints.get(9).value == 93//0

        listPoints.get(10).timestamp == 1462892502//5
        listPoints.get(10).value == -102//0

        listPoints.get(11).timestamp == 1462892511//5
        listPoints.get(11).value == 109//0

        listPoints.get(12).timestamp == 1462892520//5
        listPoints.get(12).value == 118//0

        listPoints.get(13).timestamp == 1462892529//5
        listPoints.get(13).value == 127//0

        listPoints.get(14).timestamp == 1462892538//0
        listPoints.get(14).value == 136//0

    }

    def "test rearrange points"() {
        given:
        def points = []
        points.add(new Point(0, 100, 10))
        points.add(new Point(1, 202, 20))
        points.add(new Point(2, 305, 30))
        points.add(new Point(3, 401, 39))
        points.add(new Point(4, 509, 48))
        points.add(new Point(5, 510, 48))
        points.add(new Point(6, 511, 48))
        points.add(new Point(7, 509, 10))

        def builder = new MetricTimeSeries.Builder("rearrange", "metric")

        when:
        def serializedPoints = ProtoBufMetricTimeSeriesSerializer.to(points.iterator(), 10)
        ProtoBufMetricTimeSeriesSerializer.from(new ByteArrayInputStream(serializedPoints), 100L, 510L, builder)

        def ts = builder.build()
        def listPoints = ts.points().collect(Collectors.toList()) as List<Point>

        then:
        listPoints.get(7).timestamp == 509

    }

    def "test ddc threshold -1"() {
        when:
        ProtoBufMetricTimeSeriesSerializer.to(null, -1)
        then:
        thrown(IllegalArgumentException)
    }

    def "test raw time series with almost_equals = 0"() {
        given:
        def rawTimeSeriesList = readTimeSeriesData()

        when:
        rawTimeSeriesList.each {
            println "Checking file ${it.key}"
            def rawTimeSeries = it.value
            rawTimeSeries.sort()

            def start = System.currentTimeMillis()
            def serializedPoints = ProtoBufMetricTimeSeriesSerializer.to(rawTimeSeries.points().iterator())
            def end = System.currentTimeMillis()

            println "Serialization took ${end - start} ms"

            def builder = new MetricTimeSeries.Builder("heap", "metric")

            start = System.currentTimeMillis()
            ProtoBufMetricTimeSeriesSerializer.from(new ByteArrayInputStream(serializedPoints), rawTimeSeries.start, rawTimeSeries.end, builder)
            end = System.currentTimeMillis()

            println "Deserialization took ${end - start} ms"
            def modifiedTimeSeries = builder.build()

            def count = rawTimeSeries.size()
            println "Checking $count points for almost_equals = 0"

            for (int i = 0; i < count; i++) {
                if (rawTimeSeries.getTime(i) != modifiedTimeSeries.getTime(i)) {
                    long delta = rawTimeSeries.getTime(i) - modifiedTimeSeries.getTime(i)
                    throw new IllegalStateException("Points are not equals at " + i + ". Should " + rawTimeSeries.getTime(i) + " but is " + modifiedTimeSeries.getTime(i) + " a delta of " + delta)
                }
                if (rawTimeSeries.getValue(i) != modifiedTimeSeries.getValue(i)) {
                    double delta = rawTimeSeries.getValue(i) - modifiedTimeSeries.getValue(i)

                    throw new IllegalStateException("Values not equals at " + i + ". Should " + rawTimeSeries.getValue(i) + " but is " + modifiedTimeSeries.getValue(i) + " a delta of " + delta)

                }
            }
        }
        then:
        noExceptionThrown()

    }

    def "test change rate"() {

        given:
        def rawTimeSeriesList = readTimeSeriesData()


        when:
        def file = new File("target/ddc-threshold.csv")
        file.write("")
        writeToFile(file, "Index;AlmostEquals;Legend;Value")

        def index = 1

        [0, 10, 25, 50, 100].eachWithIndex { def almostEquals, def almostEqualsIndex ->

            println "============================================================="
            println "===================     ${almostEquals} ms       ========================="
            println "============================================================="

            def changes = 0
            def sumOfPoint = 0
            def maxDeviation = 0
            def indexwiseDeltaRaw = 0
            def indexwiseDeltaMod = 0
            def averageDeviation = 0

            def totalBytes = 0
            def totalSerializedBytes = 0


            rawTimeSeriesList.each { pair, rawTimeSeries ->

                rawTimeSeries.sort()

                def changesTS = 0
                def sumOfPointTS = 0
                def maxDeviationTS = 0
                def indexwiseDeltaRawTS = 0
                def indexwiseDeltaModTS = 0
                def averageDeviationTS = 0
                def rawBytes = rawTimeSeries.attribute("bytes") as Integer
                def compressedBytes = 0

                def builder = new MetricTimeSeries.Builder(rawTimeSeries.getName(), "metric")
                def modTimeSeries = ProtoBufMetricTimeSeriesSerializer.to(rawTimeSeries.points().iterator(), almostEquals)

                def bytes = modTimeSeries.length
                compressedBytes = Compression.compress(modTimeSeries).length

                totalBytes += rawBytes
                totalSerializedBytes += bytes

                ProtoBufMetricTimeSeriesSerializer.from(new ByteArrayInputStream(modTimeSeries), rawTimeSeries.getStart(), rawTimeSeries.getEnd(), builder)

                modTimeSeries = builder.build()

                sumOfPoint += rawTimeSeries.size()
                sumOfPointTS = rawTimeSeries.size()

                for (int j = 0; j < rawTimeSeries.size(); j++) {

                    def rawTS = rawTimeSeries.getTime(j)
                    def modTS = modTimeSeries.getTime(j)

                    def deviation = Math.abs(rawTS - modTS)

                    averageDeviationTS += deviation
                    averageDeviation += deviation

                    if (deviation > maxDeviation) {
                        maxDeviation = deviation
                    }

                    if (deviation > maxDeviationTS) {
                        maxDeviationTS = deviation
                    }

                    if (j + 1 < rawTimeSeries.size()) {
                        indexwiseDeltaRaw += Math.abs(rawTimeSeries.getTime(j + 1) - rawTS)
                        indexwiseDeltaMod += Math.abs(modTimeSeries.getTime(j + 1) - modTS)

                        indexwiseDeltaRawTS += indexwiseDeltaRaw
                        indexwiseDeltaModTS += indexwiseDeltaMod
                    }

                    if (rawTS != modTS) {
                        changes++
                        changesTS++
                    }
                }
                println "======================================================="
                println "TS ${rawTimeSeries.getName()} start: ${Instant.ofEpochMilli(rawTimeSeries.getStart())} end: ${Instant.ofEpochMilli(rawTimeSeries.getEnd())}"
                println "TS-MOD ${modTimeSeries.getName()} start: ${Instant.ofEpochMilli(modTimeSeries.getStart())} end: ${Instant.ofEpochMilli(modTimeSeries.getEnd())}"
                println "Max deviation: $maxDeviationTS in milliseconds"
                println "Raw: Sum of deltas: $indexwiseDeltaRawTS in minutes"
                println "Mod: Sum of deltas: $indexwiseDeltaModTS in minutes"
                println "Change rate per point: ${changesTS / sumOfPointTS}"
                println "Average deviation per point: ${averageDeviationTS / sumOfPointTS}"
                println "Bytes after serialization: $bytes"
                println "Bytes before serialization: $rawBytes"
                println "Safes: ${(1 - bytes / rawBytes) * 100} %"
                println "Bytes per point: ${bytes / sumOfPointTS}"
                println "Compressed Bytes per point: ${compressedBytes / sumOfPointTS}"
                println "======================================================="
            }
            println "======================================================="
            println "= Overall almost equals: $almostEquals"
            println "======================================================="
            println "Max deviation: $maxDeviation in milliseconds"
            println "Raw: Sum of deltas: $indexwiseDeltaRaw in minutes"
            println "Mod: Sum of deltas: $indexwiseDeltaMod in minutes"
            println "Change rate per point: ${changes / sumOfPoint}"
            println "Average deviation per point: ${averageDeviation / sumOfPoint}"


            def changeRate = (changes / sumOfPoint) * 100
            def compressionRate = (1 - (totalSerializedBytes / totalBytes)) * 100

            writeToFile(file, "${index};${almostEquals};Timstamp Change Rate;${changeRate}")
            writeToFile(file, "${index};${almostEquals};Compression Rate;${compressionRate}")
            index++

        }



        then:
        noExceptionThrown()
    }

    void writeToFile(file, message) {
        file.append(message)
        file.append("\n")
    }

    @Unroll
    def "test raw time series with almost_equals = #almostEquals"() {
        given:
        def rawTimeSeriesList = readTimeSeriesData()

        when:
        rawTimeSeriesList.each {
            println "Checking file ${it.key}"
            def rawTimeSeries = it.value;
            rawTimeSeries.sort()

            def unique = new MetricTimeSeries.Builder("Unique", "metric")

            List<Point> list = rawTimeSeries.points().collect(Collectors.toList());

            def prevDate = list.get(0).timestamp;

            for (int i = 1; i < list.size(); i++) {

                def currentDate = list.get(i).timestamp

                if (currentDate != prevDate) {
                    unique.point(currentDate, list.get(i).value);
                }
                prevDate = currentDate

            }

            MetricTimeSeries uniqueTS = unique.build()
            uniqueTS.sort()

            def start = System.currentTimeMillis()
            def serializedPoints = ProtoBufMetricTimeSeriesSerializer.to(uniqueTS.points().iterator(), almostEquals)
            def end = System.currentTimeMillis()

            println "Serialization took ${end - start} ms"

            def builder = new MetricTimeSeries.Builder("heap", "metric")

            start = System.currentTimeMillis()
            ProtoBufMetricTimeSeriesSerializer.from(new ByteArrayInputStream(serializedPoints), uniqueTS.getStart(), uniqueTS.getEnd(), builder)
            end = System.currentTimeMillis()

            println "Deserialization took ${end - start} ms"
            def modifiedTimeSeries = builder.build()

            def count = uniqueTS.size();

            for (int i = 0; i < count; i++) {
                if (modifiedTimeSeries.getTime(i) - uniqueTS.getTime(i) > almostEquals) {
                    println("Position ${i}: Time diff is ${modifiedTimeSeries.getTime(i) - uniqueTS.getTime(i)}. Orginal ts: ${uniqueTS.getTime(i)}. Reconstructed ts: ${modifiedTimeSeries.getTime(i)}")
                }
            }
        }
        then:
        noExceptionThrown()

        where:
        almostEquals << [10]

    }


    static def readTimeSeriesData() {
        def url = ProtoBufMetricGenericTimeSeriesSerializerTest.getResource("/data-mini")
        def tsDir = new File(url.toURI())

        def documents = new HashMap<String, MetricTimeSeries>()

        tsDir.listFiles().each { File file ->
            println("Processing file $file")
            def bytes = new GZIPInputStream(new FileInputStream(file)).getBytes()

            documents.put(file.name, new MetricTimeSeries.Builder(file.name, "metric").attribute("bytes", bytes.length).build())

            def nf = DecimalFormat.getInstance(Locale.ENGLISH)
            def filePoints = 0

            def unzipped = new GZIPInputStream(new FileInputStream(file))

            unzipped.splitEachLine(";") { fields ->
                //Its the first line of a csv file
                if ("Date" != fields[0]) {
                    //First field is the timestamp: 26.08.2013 00:00:17.361
                    def date = Instant.parse(fields[0])
                    fields.subList(1, fields.size()).eachWithIndex { String value, int i ->
                        documents.get(file.name).add(date.toEpochMilli(), nf.parse(value).doubleValue())
                        filePoints = i
                    }
                }
            }

        }
        documents
    }
}
