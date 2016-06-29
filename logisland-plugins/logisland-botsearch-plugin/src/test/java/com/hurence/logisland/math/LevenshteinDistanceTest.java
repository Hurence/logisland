/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.hurence.logisland.math;

import org.junit.Test;
import static org.junit.Assert.*;

/**
 *
 * @author tom
 */
public class LevenshteinDistanceTest {
	
	/**
	 * For example, the Levenshtein distance between "kitten" and "sitting" is 3,
	 * since the following three edits change one into the other, and there is 
	 * no way to do it with fewer than three edits:
	 * 
	 * kitten → sitten (substitution of "s" for "k")
	 * sitten → sittin (substitution of "i" for "e")
	 * sittin → sitting (insertion of "g" at the end).
	 */
	@Test
	public void testDistance() {
		assertEquals(3, LevenshteinDistanceMeasure.distance("sitting", "kitten"));
		assertEquals(3, LevenshteinDistanceMeasure.distance("sunday", "saturday"));
		
		
		assertEquals(3.0/8.0, LevenshteinDistanceMeasure.normalizedDistance("sunday", "saturday"), 0.001);
	}
}