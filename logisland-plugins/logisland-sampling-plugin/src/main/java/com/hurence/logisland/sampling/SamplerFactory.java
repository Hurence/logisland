/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hurence.logisland.sampling;

public class SamplerFactory {

    /**
     * Instanciates a sampler.
     *
     * @param algorithm the sampling algorithm
     * @param valueFieldName the name of the field containing the point value (Y)
     * @param timeFieldName the name of the field containing the point time (X)
     * @param parameter an int parameter
     * @return the sampler
     */
    public static Sampler getSampler(SamplingAlgorithm algorithm,
                                     String valueFieldName,
                                     String timeFieldName,
                                     int parameter) {

        switch (algorithm) {
            case LTTB:
                return new LTTBSampler(valueFieldName, timeFieldName, parameter);
            case FIRST_ITEM:
                return new FirstItemSampler(valueFieldName, timeFieldName, parameter);
            case AVERAGE:
                return new AverageSampler(valueFieldName, timeFieldName, parameter);
            case NONE:
            default:
                return new IsoSampler();
        }
    }
}
