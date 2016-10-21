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
package com.caseystella.analytics.outlier.streaming;

import com.caseystella.analytics.outlier.streaming.mad.SketchyMovingMAD;

public enum OutlierAlgorithms {
    SKETCHY_MOVING_MAD(SketchyMovingMAD.class)
    ;
    Class<? extends OutlierAlgorithm> clazz;
    OutlierAlgorithms(Class<? extends OutlierAlgorithm> clazz) {
        this.clazz = clazz;
    }

    public OutlierAlgorithm newInstance()  {
        try {
            return clazz.newInstance();
        } catch (InstantiationException e) {
            throw new RuntimeException("Unable to instantiate outlier algorithm.", e);
        } catch (IllegalAccessException e) {
            throw new RuntimeException("Unable to instantiate outlier algorithm.", e);
        }
    }
    public static OutlierAlgorithm newInstance(String outlierAlgorithm) {
        try {
            return OutlierAlgorithms.valueOf(outlierAlgorithm).newInstance();
        }
        catch(Throwable t) {
            try {
                return (OutlierAlgorithm) OutlierAlgorithm.class.forName(outlierAlgorithm).newInstance();
            } catch (InstantiationException e) {
                throw new RuntimeException("Unable to instantiate " + outlierAlgorithm + " or find it in the OutlierAlgorithms enum", e);
            } catch (IllegalAccessException e) {
                throw new RuntimeException("Unable to instantiate " + outlierAlgorithm + " or find it in the OutlierAlgorithms enum", e);
            } catch (ClassNotFoundException e) {
                throw new RuntimeException("Unable to instantiate " + outlierAlgorithm + " or find it in the OutlierAlgorithms enum", e);
            }
        }
    }

}
