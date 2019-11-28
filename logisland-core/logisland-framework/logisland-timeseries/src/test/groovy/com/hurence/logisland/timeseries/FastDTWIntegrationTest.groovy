/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2016 Stan Salvador (stansalvador@hotmail.com), Philip Chan (pkc@cs.fit.edu), QAware GmbH
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */
package com.hurence.logisland.timeseries

import com.hurence.logisland.timeseries.distance.DistanceFunctionEnum
import com.hurence.logisland.timeseries.distance.DistanceFunctionFactory
import com.hurence.logisland.timeseries.dtw.FastDTW
import spock.lang.Specification
import spock.lang.Unroll

/**
 * Unit test to the FastDTW implementation
 * @author f.lautenschlager
 */
class FastDTWIntegrationTest extends Specification {

    public static void main(String[] args) {
        def distFn = DistanceFunctionFactory.getDistanceFunction(DistanceFunctionEnum.EUCLIDEAN)
        def tsI = new MultivariateTimeSeries(1)
        def tsJ = new MultivariateTimeSeries(1)

        fillTimeSeries("CPU-Load.csv", tsI, tsJ)

        def start = System.currentTimeMillis();
        def info = FastDTW.getWarpInfoBetween(tsI, tsJ, 1, distFn)
        def end = System.currentTimeMillis();
        println "FastDTW for search radius: 1 took: ${end - start}"

    }

    @Unroll
    def "test warp path of two time series: search radius: #searchRadius, faster than: #maxTime ms."() {
        given:
        def distFn = DistanceFunctionFactory.getDistanceFunction(DistanceFunctionEnum.EUCLIDEAN)
        def tsI = new MultivariateTimeSeries(1)
        def tsJ = new MultivariateTimeSeries(1)

        fillTimeSeries("CPU-Load.csv", tsI, tsJ)

        when:
        def start = System.currentTimeMillis();
        def info = FastDTW.getWarpInfoBetween(tsI, tsJ, searchRadius, distFn)
        def end = System.currentTimeMillis();

        then:
        info.getDistance() == distance
        info.getPath().size()
        info.getNormalizedDistance() == normalizedDistance
        (end - start) < maxTime

        println "FastDTW for search radius: $searchRadius took: ${end - start}"

        where:
        searchRadius << [1, 5, 10, 15, 20, 25, 30]
        //That are the result of the default implementation
        distance << [79057.83999997916d, 67460.60999996655d, 63196.48999995647d, 61447.68999994993d, 61721.41999994916d, 60734.809999950994d, 59639.289999948676d]
        normalizedDistance << [0.17885337061616094d, 0.15261683701856124d, 0.14297007415843518d, 0.13901374579764522d, 0.13963300801298828d, 0.13740098998690348d, 0.13492258373930194d]
        maxTime << [6000, 6000, 6000, 7000, 7000, 7000, 8000]
        //Old values changed because randomly failed in travis ci build
//        maxTime << [3000, 3000, 3000, 4000, 4000, 4000, 5000]
    }


    static void fillTimeSeries(String filePath, def tsI, def tsJ) {
        def inputStream = FastDTWIntegrationTest.getResourceAsStream("/$filePath")
        def index = 0;
        def firstLine = true;
        inputStream.eachLine() {
            if (!firstLine) {
                def columns = it.split(",")
                if (columns.length == 3) {
                    if (!columns[1].isEmpty()) {
                        tsI.add(index, [columns[1] as double] as double[])
                    }
                    tsJ.add(index, [columns[2] as double] as double[])
                } else if (columns.length == 2) {
                    tsI.add(index, [columns[1] as double] as double[])
                }

                index++;
            } else {
                firstLine = false
            }
        }
    }

}
