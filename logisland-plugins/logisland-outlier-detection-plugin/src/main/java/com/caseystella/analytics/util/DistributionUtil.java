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
package com.caseystella.analytics.util;

import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;

import java.io.PrintStream;
import java.io.PrintWriter;

public enum DistributionUtil {
    INSTANCE;
    public void summary(String title, DescriptiveStatistics statistics) {
        summary(title, statistics, System.out);
    }
    public void summary(String title, DescriptiveStatistics statistics, PrintStream pw) {
        pw.println(title + ": "
                + "\n\tMin: " + statistics.getMin()
                + "\n\t1th: " + statistics.getPercentile(1)
                + "\n\t5th: " + statistics.getPercentile(5)
                + "\n\t10th: " + statistics.getPercentile(10)
                + "\n\t25th: " + statistics.getPercentile(25)
                + "\n\t50th: " + statistics.getPercentile(50)
                + "\n\t90th: " + statistics.getPercentile(90)
                + "\n\t95th: " + statistics.getPercentile(95)
                + "\n\t99th: " + statistics.getPercentile(99)
                + "\n\tMax: " + statistics.getMax()
                + "\n\tMean: " + statistics.getMean()
                + "\n\tStdDev: " + statistics.getStandardDeviation()
        );
    }
}
