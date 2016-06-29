/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.hurence.logisland.math;

import java.util.ArrayList;
import java.util.List;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 *
 * @author tom
 */
public class JaccardDistanceMeasureTest {

	/**
	 * Test of jaccardSimilarity method, of class JaccardDistanceMeasure.
	 */
	@Test
	public void testJaccardSimilarity() {



		List<Integer> A = new ArrayList<Integer>();
		A.add(1);
		A.add(2);
		A.add(3);
		A.add(4);

		List<Integer> B = new ArrayList<Integer>();
		B.add(1);
		B.add(2);
		B.add(5);
		B.add(6);
		
		assertEquals(0.666, JaccardDistanceMeasure.distance(A, B), 0.001);
		
		String[] paramsA = {"sessionid", "oups", "browser", "ploky", "mik"};
		String[] paramsB = {"sessionid", "oupla", "browser", "ploky"};
		assertEquals(0.5, JaccardDistanceMeasure.similarity(paramsA, paramsB), 0.001);
		
		
		List<Integer> empty = new ArrayList<Integer>();
		assertEquals(0.0, JaccardDistanceMeasure.distance(empty, empty), 0.001);
		assertEquals(1.0, JaccardDistanceMeasure.distance(A, empty), 0.001);
		assertEquals(1.0, JaccardDistanceMeasure.distance(empty, B), 0.001);
	}
}