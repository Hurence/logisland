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