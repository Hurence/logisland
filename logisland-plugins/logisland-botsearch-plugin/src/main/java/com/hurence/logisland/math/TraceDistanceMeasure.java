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
