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

import com.hurence.logisland.botsearch.HttpFlow;
import com.hurence.logisland.botsearch.Trace;
import java.util.List;
import org.apache.commons.math3.ml.distance.EuclideanDistance;
import org.apache.commons.math3.stat.StatUtils;


/**
 *
 * @author tom
 */
public class TraceDistanceMeasure implements ExtendedDistanceMeasure<Trace>{

	static final EuclideanDistance d = new EuclideanDistance();

	@Override
	public double compute(Trace a, Trace b) {
		
		List<HttpFlow> flowsA = a.getFlows();
		List<HttpFlow> flowsB = b.getFlows();
		
		
		
		// allocate an array
		double[] distances = new double[flowsA.size()];
		
		for (int i = 0; i < flowsA.size(); i++) {
			// array allocation
			double[] tmpDistances = new double[flowsB.size()];
			HttpFlow currentFlowA = flowsA.get(i);
			
			for (int j = 0; j < flowsB.size(); j++) {
				HttpFlow currentFlowB = flowsB.get(j);
				tmpDistances[j] = FlowDistanceMeasure.distance(currentFlowA, currentFlowB);
			}
			distances[i] = StatUtils.min(tmpDistances);
		}

		return StatUtils.mean(distances);
		
	}

	@Override
	public double compute(double[] a, double[] b) {
		
		return d.compute(a, b);
	}
	
}
