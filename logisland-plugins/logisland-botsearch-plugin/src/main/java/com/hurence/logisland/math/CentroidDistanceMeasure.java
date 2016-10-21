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
import org.apache.commons.math3.ml.distance.EuclideanDistance;

/**
 *
 * @author tom
 */
public class CentroidDistanceMeasure {
	/*
	 * 
	 * VL-77597{n=12154 c=[233.732, 12892.600, 9264777.126, 17.612] r=[1112.345, 160544.331, 3296987.624, 5.236]}
	 * VL-77599{n=53711 c=[147.818, 14724.463, 793783.566, 21.408] r=[4989.834, 140429.760, 1361003.379, 5.619]}
	 * VL-77600{n=4157 c=[409.262, 16689.075, 41373451.041, 17.439] r=[6472.338, 682625.375, 6579341.651, 3.290]}
	 * VL-77602{n=7581 c=[362.329, 10877.741, 23047279.573, 17.980] r=[5529.206, 131812.702, 4522009.724, 4.471]}
	 */

	private static double[][] centroids = {
		{233.732, 12892.600, 9264777.126, 17.612},
		{147.818, 14724.463, 793783.566, 21.408},
		{409.262, 16689.075, 41373451.041, 17.439},
		{362.329, 10877.741, 23047279.573, 17.980},};
	private static String[] centroidNames = {
		"VL-77597",
		"VL-77599",
		"VL-77600",
		"VL-77602",};

	public static void computeNearestCentroid(Trace trace) {
		double[] point = trace.getPoint();
		EuclideanDistance distance = new EuclideanDistance();

		trace.setDistanceToNearestCentroid(distance.compute(point, centroids[0]));
		trace.setCentroidName(centroidNames[0]);
		for (int i = 1; i < centroids.length; i++) {
			double d1 = distance.compute(point, centroids[i]);
			if (d1 < trace.getDistanceToNearestCentroid()) {
				trace.setDistanceToNearestCentroid(d1);
				trace.setCentroidName(centroidNames[i]);
			}
		}
	}
}
