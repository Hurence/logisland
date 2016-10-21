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

/**
 * In information theory and computer science, the Levenshtein distance is a
 * string metric for measuring the difference between two sequences.
 *
 * Informally, the Levenshtein distance between two words is the minimum number
 * of single-character edits (insertion, deletion, substitution) required to
 * change one word into the other. The phrase edit distance is often used to
 * refer specifically to Levenshtein distance.
 *
 * It is named after Vladimir Levenshtein, who considered this distance in 1965.
 * [1] It is closely related to pairwise string alignments.
 *
 * @author tom
 */
public class LevenshteinDistanceMeasure {

	private static int minimum(int a, int b, int c) {
		return Math.min(Math.min(a, b), c);
	}

	public static int distance(CharSequence str1,
			CharSequence str2) {
		int[][] distance = new int[str1.length() + 1][str2.length() + 1];

		for (int i = 0; i <= str1.length(); i++) {
			distance[i][0] = i;
		}
		for (int j = 1; j <= str2.length(); j++) {
			distance[0][j] = j;
		}

		for (int i = 1; i <= str1.length(); i++) {
			for (int j = 1; j <= str2.length(); j++) {
				distance[i][j] = minimum(
						distance[i - 1][j] + 1,
						distance[i][j - 1] + 1,
						distance[i - 1][j - 1]
						+ ((str1.charAt(i - 1) == str2.charAt(j - 1)) ? 0
						: 1));
			}
		}

		return distance[str1.length()][str2.length()];
	}

	public static double normalizedDistance(CharSequence str1,
			CharSequence str2) {
		double max = (double) Math.max(str1.length(), str2.length());

		if (max != 0.0) {
			return (double) distance(str1, str2) / max;
		}else
			return 0.0;
	}
}