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