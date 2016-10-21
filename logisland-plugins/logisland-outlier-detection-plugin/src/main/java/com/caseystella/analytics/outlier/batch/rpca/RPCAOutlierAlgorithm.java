/**
 * Copyright (C) 2016 Hurence (bailet.thomas@gmail.com)
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
package com.caseystella.analytics.outlier.batch.rpca;

import com.caseystella.analytics.DataPoint;
import com.caseystella.analytics.distribution.GlobalStatistics;
import com.caseystella.analytics.distribution.scaling.ScalingFunctions;
import com.caseystella.analytics.outlier.Outlier;
import com.caseystella.analytics.outlier.OutlierMetadataConstants;
import com.caseystella.analytics.outlier.Severity;
import com.caseystella.analytics.outlier.batch.OutlierAlgorithm;
import com.caseystella.analytics.outlier.streaming.OutlierConfig;
import com.caseystella.analytics.util.ConfigUtil;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;

import java.util.HashMap;
import java.util.List;

public class RPCAOutlierAlgorithm implements OutlierAlgorithm{
    private static final double EPSILON = 1e-12;
    private static final String THRESHOLD_CONF = "rpca.threshold";

    private final double LPENALTY_DEFAULT = 1;
    private final double SPENALTY_DEFAULT = 1.4;
    private final String LPENALTY_CONFIG = "lpenalty";
    private final String SPENALTY_CONFIG = "spenalty";
    private final String FORCE_DIFF_CONFIG = "forceDiff";
    private final String MIN_RECORDS_CONFIG = "minRecords";

    private Double  lpenalty;
    private Double  spenalty;
    private Boolean isForceDiff = false;
    private int minRecords = 0;
    private double threshold = EPSILON;
    private ScalingFunctions scaling = ScalingFunctions.NONE;

    public RPCAOutlierAlgorithm() {

    }

    public RPCAOutlierAlgorithm withLPenalty(double lPenalty) {
        this.lpenalty = lPenalty;
        return this;
    }
    public RPCAOutlierAlgorithm withSPenalty(double sPenalty) {
        this.spenalty = sPenalty;
        return this;
    }

    public RPCAOutlierAlgorithm withForceDiff(boolean forceDiff) {
        this.isForceDiff = forceDiff;
        return this;
    }
    public RPCAOutlierAlgorithm withMinRecords(int minRecords) {
        this.minRecords = minRecords;
        return this;
    }
    public RPCAOutlierAlgorithm withScalingFunction(ScalingFunctions scaling) {
        this.scaling = scaling;
        return this;
    }
    // Helper Function
    public double[][] VectorToMatrix(double[] x, int rows, int cols) {
        double[][] input2DArray = new double[rows][cols];
        for (int n= 0; n< x.length; n++) {
            int i = n % rows;
            int j = (int) Math.floor(n / rows);
            input2DArray[i][j] = x[n];
        }
        return input2DArray;
    }




    public double outlierScore(List<DataPoint> dataPoints, DataPoint value) {
        double[] inputData = new double[dataPoints.size() + 1];
        int numNonZero = 0;
        if(scaling != ScalingFunctions.NONE) {
            int i = 0;
            final DescriptiveStatistics stats = new DescriptiveStatistics();
            for (DataPoint dp : dataPoints) {
                inputData[i++] = dp.getValue();

                stats.addValue(dp.getValue());
                numNonZero += dp.getValue() > EPSILON ? 1 : 0;
            }
            inputData[i] = value.getValue();
            GlobalStatistics globalStats = new GlobalStatistics() {{
                setMax(stats.getMax());
                setMin(stats.getMin());
                setMax(stats.getMean());
                setStddev(stats.getStandardDeviation());
            }};
            for(i = 0;i < inputData.length;++i) {
                inputData[i] = scaling.scale(inputData[i], globalStats);
            }
        }
        else {
            int i = 0;
            for (DataPoint dp : dataPoints) {
                inputData[i++] = dp.getValue();
                numNonZero += dp.getValue() > EPSILON ? 1 : 0;
            }
            inputData[i] = value.getValue();
        }
        int nCols = 1;
        int nRows = inputData.length;
        if(numNonZero > minRecords) {
            AugmentedDickeyFuller dickeyFullerTest = new AugmentedDickeyFuller(inputData);
            double[] inputArrayTransformed = inputData;
            if (!this.isForceDiff && dickeyFullerTest.isNeedsDiff()) {
                // Auto Diff
                inputArrayTransformed = dickeyFullerTest.getZeroPaddedDiff();
            } else if (this.isForceDiff) {
                // Force Diff
                inputArrayTransformed = dickeyFullerTest.getZeroPaddedDiff();
            }

            if (this.spenalty == null) {
                this.lpenalty = this.LPENALTY_DEFAULT;
                this.spenalty = this.SPENALTY_DEFAULT/ Math.sqrt(Math.max(nCols, nRows));
            }


            // Calc Mean
            double mean  = 0;
            for (int n=0; n < inputArrayTransformed.length; n++) {
                mean += inputArrayTransformed[n];
            }
            mean /= inputArrayTransformed.length;

            // Calc STDEV
            double stdev = 0;
            for (int n=0; n < inputArrayTransformed.length; n++) {
                stdev += Math.pow(inputArrayTransformed[n] - mean,2) ;
            }
            stdev = Math.sqrt(stdev / (inputArrayTransformed.length - 1));

            // Transformation: Zero Mean, Unit Variance
            for (int n=0; n < inputArrayTransformed.length; n++) {
                inputArrayTransformed[n] = (inputArrayTransformed[n]-mean)/stdev;
            }

            // Read Input Data into Array
            // Read Input Data into Array
            double[][] input2DArray = new double[nRows][nCols];
            input2DArray = VectorToMatrix(inputArrayTransformed, nRows, nCols);

            RPCA rSVD = new RPCA(input2DArray, this.lpenalty, this.spenalty);

            double[][] outputE = rSVD.getE().getData();
            double[][] outputS = rSVD.getS().getData();
            double[][] outputL = rSVD.getL().getData();
            return outputS[nRows-1][0];
        }
        else {
            return Double.NaN;
        }
    }

    @Override
    public Outlier analyze(Outlier outlierCandidate, List<DataPoint> context, DataPoint dp) {
        double score = outlierScore(context, dp);
        Severity severity = Severity.NOT_ENOUGH_DATA;
        if(!Double.isNaN(score)) {
            severity = Math.abs(score) > threshold?Severity.SEVERE_OUTLIER:Severity.NORMAL;
        }
        outlierCandidate.setSeverity(severity);
        if(severity == Severity.SEVERE_OUTLIER) {
            if(dp.getMetadata() == null) {
                dp.setMetadata(new HashMap<String, String>());
            }
            dp.getMetadata().put(OutlierMetadataConstants.REAL_OUTLIER_SCORE.toString(), Math.abs(score) + "");
        }
        return outlierCandidate;
    }

    @Override
    public void configure(OutlierConfig config) {
        {
            Object thresholdObj = config.getConfig().get(THRESHOLD_CONF);
            if(thresholdObj != null) {
                threshold = ConfigUtil.INSTANCE.coerceDouble(THRESHOLD_CONF, thresholdObj);
            }
        }
        {
            Object lPenaltyObj = config.getConfig().get(LPENALTY_CONFIG);
            if (lPenaltyObj != null) {
                withLPenalty(ConfigUtil.INSTANCE.coerceDouble(LPENALTY_CONFIG, lPenaltyObj));
            }
        }
        {
            Object sPenaltyObj = config.getConfig().get(SPENALTY_CONFIG);
            if (sPenaltyObj != null) {
                withSPenalty(ConfigUtil.INSTANCE.coerceDouble(SPENALTY_CONFIG, sPenaltyObj));
            }
        }
        {
            Object forceDiffObj= config.getConfig().get(FORCE_DIFF_CONFIG);
            if (forceDiffObj != null) {
                withForceDiff(ConfigUtil.INSTANCE.coerceBoolean(FORCE_DIFF_CONFIG, forceDiffObj));
            }
        }
        {
            Object minRecordsObj = config.getConfig().get(MIN_RECORDS_CONFIG);
            if (minRecordsObj!= null) {
                withMinRecords(ConfigUtil.INSTANCE.coerceInteger(MIN_RECORDS_CONFIG, minRecordsObj));
            }
        }
    }
}
