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
import com.caseystella.analytics.extractor.Extractor;
import com.caseystella.analytics.outlier.Outlier;
import com.caseystella.analytics.extractor.DataPointExtractor;
import com.caseystella.analytics.extractor.DataPointExtractorConfig;
import com.caseystella.analytics.outlier.streaming.OutlierConfig;
import com.caseystella.analytics.outlier.Severity;
import com.caseystella.analytics.util.JSONUtil;
import com.google.common.base.Function;


import javax.annotation.Nullable;
import java.io.*;
import java.util.*;

public class OutlierRunner {
    OutlierConfig config;
    Extractor extractor;

    public OutlierRunner(String outlierConfig, String extractorConfig) throws IOException {
        {
            config = JSONUtil.INSTANCE.load(outlierConfig, OutlierConfig.class);
            config.getSketchyOutlierAlgorithm().configure(config);
        }
        {
            DataPointExtractorConfig config = JSONUtil.INSTANCE.load(extractorConfig, DataPointExtractorConfig.class);
            extractor = new DataPointExtractor(config);
        }
    }

    public double getMean() {
        return ((SketchyMovingMAD)config.getSketchyOutlierAlgorithm()).getValueDistributions().get("benchmark").getCurrentDistribution().getMean();
    }

    public List<Outlier> run(File csv, int linesToSkip, final EnumSet<Severity> reportedSeverities, Function<Map.Entry<DataPoint, Outlier>, Void> callback) throws IOException {
        final List<Outlier> ret = new ArrayList<>();
        BufferedReader br = new BufferedReader(new FileReader(csv));
        int numLines = 0;
        for(String line = null;(line = br.readLine()) != null;numLines++){
            if(numLines >= linesToSkip) {
                for(DataPoint dp : extractor.extract(null, line.getBytes(), true)) {
                    Outlier o = config.getSketchyOutlierAlgorithm().analyze(dp);
                    callback.apply(new AbstractMap.SimpleEntry<>(dp, o));
                    if(reportedSeverities.contains(o.getSeverity())) {
                        ret.add(o);
                    }
                }
            }
        }
        return ret;
    }
    public static Function<Outlier, Long> OUTLIER_TO_TS = new Function<Outlier, Long>() {
        @Nullable
        @Override
        public Long apply(@Nullable Outlier outlier) {
            return outlier.getDataPoint().getTimestamp();
        }
    };
}
