/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.hurence.logisland.math;

import org.apache.commons.math3.exception.NotPositiveException;
import org.apache.commons.math3.ml.clustering.Clusterable;
import org.apache.commons.math3.ml.clustering.DBSCANClusterer;
import org.apache.commons.math3.ml.distance.DistanceMeasure;

/**
 *
 * @author tom
 */
public class ExtendedDBSCANClusterer<T extends Clusterable>
		extends DBSCANClusterer<T> {

	public ExtendedDBSCANClusterer(double eps, int minPts)
			throws NotPositiveException {
		super(eps, minPts);
	}

	public ExtendedDBSCANClusterer(double eps, int minPts, DistanceMeasure measure) throws NotPositiveException {
		super(eps, minPts, measure);
	}

	/**
	 * Calculates the distance between two {@link Clusterable} instances with
	 * the configured {@link ExtendedDistanceMeasure}.
	 *
	 * @param p1 the first clusterable
	 * @param p2 the second clusterable
	 * @return the distance between the two clusterables
	 */
	@Override
	protected double distance(final Clusterable p1, final Clusterable p2) {
		ExtendedDistanceMeasure<Clusterable> extDistance = (ExtendedDistanceMeasure<Clusterable>) getDistanceMeasure();

		return extDistance.compute(p1, p2);
	}
}
