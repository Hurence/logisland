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
