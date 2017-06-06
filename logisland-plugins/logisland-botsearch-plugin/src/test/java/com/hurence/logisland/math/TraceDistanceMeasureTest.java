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

import com.hurence.logisland.botsearch.HttpFlow;
import com.hurence.logisland.botsearch.Trace;
import com.hurence.logisland.util.string.HttpUtil;

import java.util.List;
import static org.junit.Assert.*;
import org.junit.Test;

/**
 *
 * @author tom
 */
public class TraceDistanceMeasureTest {

	private HttpFlow createFlow(String scheme, String path, String params) {
		HttpFlow f = new HttpFlow();

		List<String> queryKeys = HttpUtil.getQueryKeys(params);
		List<String> queryValues = HttpUtil.getQueryValues(params);
		f.setUrlQueryKeys(queryKeys);
		f.setUrlQueryValues(queryValues);
		f.setUrlScheme(scheme);
		f.setUrlPath(path);

		return f;
	}

	/**
	 * Test of distance method, of class FlowDistanceMeasure.
	 */
	@Test
	public void testFlowDistance() {
		HttpFlow f1 = createFlow("POST", "/fwlink/", "LinkId=89409&locale=40c&geoid=54&version=11.0.5721.5280&userlocale=40c");
		HttpFlow f2 = createFlow("POST", "/fwlink/", "LinkId=89409&locale=40c&geoid=54&version=11.0.5721.5280&userlocale=40c");
		HttpFlow f3 = createFlow("GET", "/fwlink/", "LinkId=89409&locale=40c&geoid=54&version=11.0.5721.5280&userlocale=40c");
		HttpFlow f4 = createFlow("POST", "/fwlink2/", "LinkId=89409&locale=40c&geoid=54&version=11.0.5721.5280&userlocale=40c");
		HttpFlow f5 = createFlow("POST", "/another/path", "LinkId=89409&locale=40c&geoid=54&version=11.0.5721.5280&userlocale=40c");
		HttpFlow f6 = createFlow("POST", "/fwlink/", "flip=flop&LinkId=89409&locale=40c&geoid=54&version=11.0.5721.5280&userlocale=40c");
		HttpFlow f7 = createFlow("POST", "/fwlink/", "flip=flop&LinkId=89409&locale=40c&geoid=54&version=11.0.5721.5281&userlocale=40c");

		assertEquals(0.0, FlowDistanceMeasure.distance(f1, f2), 0.001);
		assertEquals(10.0, FlowDistanceMeasure.distance(f1, f3), 0.001);
		double d = FlowDistanceMeasure.distance(f1, f4);
		System.out.println("d = " + d);
		assertTrue(FlowDistanceMeasure.distance(f1, f4) > 0.0);
		assertTrue(FlowDistanceMeasure.distance(f1, f4) < FlowDistanceMeasure.distance(f1, f5));
		assertTrue(FlowDistanceMeasure.distance(f1, f6) < FlowDistanceMeasure.distance(f1, f7));
	}

	/**
	 * Test of compute method, of class TraceDistanceMeasure.
	 */
	@Test
	public void testTraceDistance() {


		Trace a = new Trace();
		List<HttpFlow> flowsA = a.getFlows();
		flowsA.add(createFlow("POST", "/fwlink/", "LinkId=89409&locale=40c&geoid=54&version=11.0.5721.5280&userlocale=40c"));
		flowsA.add(createFlow("POST", "/fwlink/", "LinkId=89409&locale=40c&geoid=54&version=112.10.121.530&userlocale=40c"));
		flowsA.add(createFlow("POST", "/ouplala/", "version=11.0.5721.5280&userlocale=40c"));

		Trace b = new Trace();
		List<HttpFlow> flowsB = b.getFlows();
		flowsB.add(createFlow("GET", "/fwlink/", "LinkId=89409&userlocale=40c&hi=yes"));
		flowsB.add(createFlow("GET", "/miclink/", "version=112.10.121.530&userlocale=40c"));
		flowsB.add(createFlow("POST", "/ouplala/", "version=11.0.5721.5280&userlocale=fr"));

		TraceDistanceMeasure measure = new TraceDistanceMeasure();

		double d = measure.compute(a, b);

		assertEquals(5.209, d, 0.01);
	}
}