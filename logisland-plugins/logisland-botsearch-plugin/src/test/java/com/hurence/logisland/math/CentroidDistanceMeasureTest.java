/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.hurence.logisland.math;

import com.hurence.logisland.botsearch.Trace;
import org.junit.Test;
import static org.junit.Assert.assertEquals;/**
 *
 * @author tom
 */
public class CentroidDistanceMeasureTest {

	public CentroidDistanceMeasureTest() {
	}

	/**
	 * Test of computeNearestCentroid method, of class CentroidDistanceMeasure.
	 */
	@Test
	public void testComputeNearestCentroid() {
		System.out.println("computeNearestCentroid");
		Trace trace = new Trace();
		trace.setAvgUploadedBytes(247.49756097561112);
		trace.setAvgDownloadedBytes(9446.419512195122);
		trace.setAvgTimeBetweenTwoFLows(7386808.3028083155);
		trace.setMostSignificantFrequency(21.5836249209525);

		CentroidDistanceMeasure.computeNearestCentroid(trace);

		double d = 1877971.9852130862;
		String center = "VL-77597";

		assertEquals( d, trace.getDistanceToNearestCentroid(), 0.01);
		assertEquals(center, trace.getCentroidName());
	}
}