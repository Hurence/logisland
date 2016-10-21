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
import com.caseystella.analytics.outlier.Outlier;
import com.caseystella.analytics.outlier.OutlierHelper;
import com.caseystella.analytics.outlier.Severity;
import com.caseystella.analytics.outlier.batch.rpca.RPCAOutlierAlgorithm;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class RPCATest {
    @Test
    public void test() throws Exception {
        Random r = new Random(0);
        List<DataPoint> points = new ArrayList<>();
        for(int i = 0; i < 100;++i) {
            double val = r.nextDouble()*1000;
            DataPoint dp = (new DataPoint(i, val, null, "foo"));
            points.add(dp);
        }
        DataPoint evaluationPoint = new DataPoint(101, 10000, null, "foo");
        RPCAOutlierAlgorithm detector = new RPCAOutlierAlgorithm();
        Outlier result = detector.analyze( new Outlier( evaluationPoint
                                , Severity.NORMAL
                                ,null, 0d, points.size()
                        )
                        , points
                        , evaluationPoint
                );
        Assert.assertEquals( Severity.SEVERE_OUTLIER , result.getSeverity() );
    }
}
