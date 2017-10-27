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
package com.caseystella.analytics.outlier.streaming.mad;

import com.caseystella.analytics.DataPoint;
import com.caseystella.analytics.outlier.Outlier;
import com.caseystella.analytics.UnitTestHelper;
import com.caseystella.analytics.outlier.Severity;
import com.caseystella.analytics.util.JSONUtil;
import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import com.hurence.logisland.util.string.Multiline;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.junit.Assert;
import org.junit.Test;

import javax.annotation.Nullable;
import java.io.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

public class SketchyMovingMADIntegrationTest {

    String benchmarkRoot = UnitTestHelper.findDir("benchmark_data");
    public static final SimpleDateFormat TS_FORMAT  = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    /**
    {
         "valueConverter" : "CSVConverter"
       , "valueConverterConfig" : {
                                    "columnMap" : {
                                                  "timestamp" : 0
                                                 ,"data" : 1
                                                  }
                                  }
       , "measurements" : [
            {
             "source" : "benchmark"
            ,"timestampField" : "timestamp"
            ,"timestampConverter" : "DateConverter"
            ,"timestampConverterConfig" : {
                                            "format" : "yyyy-MM-dd HH:mm:ss"
                                          }
            ,"measurementField" : "data"
            }
                          ]
     }
     */
    @Multiline
    public static String extractorConfigStr;

    /**
    {
     "rotationPolicy" : {
                        "type" : "BY_AMOUNT"
                       ,"amount" : 100
                       ,"unit" : "POINTS"
                        }
     ,"chunkingPolicy" : {
                        "type" : "BY_AMOUNT"
                       ,"amount" : 10
                       ,"unit" : "POINTS"
                         }
     ,"outlierAlgorithm" : "SKETCHY_MOVING_MAD"
     ,"globalStatistics": {
                          "min" : -200
                          }
     ,"config" : {
                 "minAmountToPredict" : 100
                ,"zscoreCutoffs" : {
                                    "NORMAL" : 0.001
                                   ,"MODERATE_OUTLIER" : 1.0
                                   }
                ,"minZscorePercentile" : 75
                 }
     }
     */
    //@Multiline
    //public static String outlierConfig;

 /**
    {
     "rotationPolicy" : {
                        "type" : "BY_AMOUNT"
                       ,"amount" : 100
                       ,"unit" : "POINTS"
                        }
     ,"chunkingPolicy" : {
                        "type" : "BY_AMOUNT"
                       ,"amount" : 10
                       ,"unit" : "POINTS"
                         }
     ,"sketchyOutlierAlgorithm" : "SKETCHY_MOVING_MAD"
     ,"globalStatistics": {
                          "min" : -200
                          }
     ,"config" : {
                 "minAmountToPredict" : 100
                ,"zscoreCutoffs" : {
                                    "NORMAL" : 0.000000000000001
                                   ,"MODERATE_OUTLIER" : 1.5
                                   }
                ,"minZscorePercentile" : 95
                 }
     }
     */
    @Multiline
    public static String outlierConfig;

    public static Function<String, Long> STR_TO_TS = new Function<String, Long>() {

        @Nullable
        @Override
        public Long apply(@Nullable String s) {
            try {
                return TS_FORMAT.parse(s).getTime();
            } catch (ParseException e) {
                throw new RuntimeException("Unable to parse " + s, e);
            }
        }
    };



    @Test
    public void runAccuracyBenchmark() throws IOException {
        Map<String, List<String>> benchmarks = JSONUtil.INSTANCE.load(new FileInputStream(new File(new File(benchmarkRoot)
                        , "combined_labels.json")
                )
                , new TypeReference<Map<String, List<String>>>(){}
        );
        Assert.assertTrue(benchmarks.size() > 0);
        Map<ConfusionMatrix.ConfusionEntry, Long> overallConfusionMatrix = new HashMap<>();
        DescriptiveStatistics globalExpectedScores = new DescriptiveStatistics();
        long total = 0;
        for(Map.Entry<String, List<String>> kv : benchmarks.entrySet()) {
            File dataFile = new File(new File(benchmarkRoot), kv.getKey());
            File plotFile = new File(new File(benchmarkRoot), kv.getKey() + ".dat");
            Assert.assertTrue(dataFile.exists());
            Set<Long> expectedOutliers = Sets.newHashSet(Iterables.transform(kv.getValue(), STR_TO_TS));
            OutlierRunner runner = new OutlierRunner(outlierConfig, extractorConfigStr);
            final long[] numObservations = {0L};
            final long[] lastTimestamp = {Long.MIN_VALUE};
            final DescriptiveStatistics timeDiffStats = new DescriptiveStatistics();
            final Map<Long, Outlier> outlierMap = new HashMap<>();
            final PrintWriter pw = new PrintWriter(plotFile);
            List<Outlier> outliers = runner.run(dataFile, 1
                    , EnumSet.of(Severity.SEVERE_OUTLIER)
                    , new Function<Map.Entry<DataPoint, Outlier>, Void>() {
                        @Nullable
                        @Override
                        public Void apply(@Nullable Map.Entry<DataPoint, Outlier> kv) {
                            DataPoint dataPoint = kv.getKey();
                            Outlier outlier = kv.getValue();
                            pw.println(dataPoint.getTimestamp() + " " + outlier.getDataPoint().getValue() + " " + ((outlier.getSeverity() == Severity.SEVERE_OUTLIER)?"outlier":"normal"));
                            outlierMap.put(dataPoint.getTimestamp(), outlier);
                            numObservations[0] += 1;
                            if(lastTimestamp[0] != Long.MIN_VALUE) {
                                timeDiffStats.addValue(dataPoint.getTimestamp() - lastTimestamp[0]);
                            }
                            lastTimestamp[0] = dataPoint.getTimestamp();
                            return null;
                        }
                    }
            );
            pw.close();
            total += numObservations[0];
            Set<Long> calculatedOutliers = Sets.newHashSet(Iterables.transform(outliers, OutlierRunner.OUTLIER_TO_TS));
            double stdDevDiff = Math.sqrt(timeDiffStats.getVariance());
            System.out.println("Running data from " + kv.getKey() + " - E[time delta]: " + ConfusionMatrix.timeConversion((long) timeDiffStats.getMean()) + ", StdDev[time delta]: " + ConfusionMatrix.timeConversion((long) stdDevDiff) + " mean: " + runner.getMean());
            Map<ConfusionMatrix.ConfusionEntry, Long> confusionMatrix = ConfusionMatrix.getConfusionMatrix( expectedOutliers
                                                                                                          , calculatedOutliers
                                                                                                          , numObservations[0]
                                                                                                          , (long)timeDiffStats.getMean()
                                                                                                          , 3 //stdDevDiff > 30000?0:3
                                                                                                          , outlierMap
                                                                                                          , globalExpectedScores
                                                                                                          );

            ConfusionMatrix.printConfusionMatrix(confusionMatrix);
            overallConfusionMatrix = ConfusionMatrix.merge(overallConfusionMatrix, confusionMatrix);
        }
        System.out.println("Really ran " + total);
        ConfusionMatrix.printConfusionMatrix(overallConfusionMatrix);
        ConfusionMatrix.printStats("Global Expected Outlier Scores", globalExpectedScores);
    }


}
