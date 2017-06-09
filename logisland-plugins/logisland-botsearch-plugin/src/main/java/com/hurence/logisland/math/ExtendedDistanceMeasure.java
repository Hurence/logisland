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
/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.hurence.logisland.math;

import org.apache.commons.math3.ml.distance.DistanceMeasure;


/**
 * Interface for distance measures of n-dimensional vectors.
 *
 * @version $Id $
 * @since 3.2
 */
public interface ExtendedDistanceMeasure<T> extends DistanceMeasure {

    /**
     * Compute the distance between two n-dimensional vectors.
     * <p>
     * The two vectors are not required to have the same dimension.
     *
     * @param a the first vector
     * @param b the second vector
     * @return the distance between the two vectors
     */
    double compute(T a, T b);
}
