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
package com.caseystella.analytics.outlier.batch;

import com.caseystella.analytics.DataPoint;
import com.caseystella.analytics.outlier.Outlier;
import com.caseystella.analytics.outlier.streaming.OutlierConfig;

import java.io.Serializable;
import java.util.List;

/**
 * Created by cstella on 3/5/16.
 */
public interface OutlierAlgorithm extends Serializable {
    Outlier analyze(Outlier outlierCandidate, List<DataPoint> context, DataPoint dp);
    void configure(OutlierConfig configStr);
}
