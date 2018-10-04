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
 * useful topics
 * 
 * https://www.damballa.com/downloads/a_pubs/Usenix10.pdf
 * http://skife.org/mahout/2013/02/14/first_steps_with_mahout.html
 */
package com.hurence.logisland.math;

import com.hurence.logisland.botsearch.HttpFlow;
import java.util.List;

/**
 * In the fine-grained clustering step, we consider the structural similarity
 * among sequences of HTTP requests (as opposed to the statistical similarity
 * used for coarsegrained clustering). Our objective is to group together
 * malware that interact with Web applications in a similar way. For example, we
 * want to group together bots that rely on the same Web-based C&C application.
 * Our approach is based on the observation that two different malware samples
 * that rely on the sameWeb server application will query URLs structured in a
 * similar way, and in a similar sequence. In order to capture these
 * similarities, we first define a measure of distance between two HTTP requests
 * rk and rh
 *
 * - m represents the request method (e.g., GET, POST, HEADER, etc.). 
 * - p stands for page, namely the first part of the URL that includes the 
 * path and page name, but does not include the parameters. 
 * - n represents the set of parameter names (i.e., n = {id, version, cc} in 
 * the example in Figure 2). 
 * - v is the set of parameter values.
 *
 */
public class FlowDistanceMeasure {

	// weight factors
	private static double wm = 10;
	private static double wp = 8;
	private static double wn = 3;
	private static double wv = 1;

	/**
	 * the overall distance between two HTTP requests as
	 *
	 * dr(rk, rh) = wm · dm(rk,rh) + wp · dp(rk, rh) + wn · dn(rk, rh) + wv ·
	 * dv(rk, rh)
	 *
	 * @param e1
	 * @param e2
	 * @return
	 */
	public static double distance(HttpFlow e1, HttpFlow e2) {
		return wm * dm(e1, e2) + wp * dp(e1, e2) + wn * dn(e1, e2) + wv * dv(e1, e2);
	}

	/**
	 * We define a distance function dm(rk, rh) that is equal to 0 if the
	 * requests rk, and rh both use the same method (e.g, both are GET
	 * requests), otherwise it is equal to 1.
	 *
	 * @param e1
	 * @param e2
	 * @return
	 */
	private static double dm(HttpFlow e1, HttpFlow e2) {
		if (e1.getUrlScheme().equals(e2.getUrlScheme())) {
			return 0;
		} else {
			return 1;
		}
	}

	/**
	 * We define dp(rk, rh) to be equal to the normalized Levenshtein distance1
	 * between the strings related to the path and pages that appear in the two
	 * requests rk and rh.
	 *
	 * @param e1
	 * @param e2
	 * @return
	 */
	private static double dp(HttpFlow e1, HttpFlow e2) {
		return LevenshteinDistanceMeasure.normalizedDistance(e1.getUrlPath(), e2.getUrlPath());
	}

	/**
	 * We define dn(rk, rh) as the Jaccard distance between the sets of
	 * parameters names in the two requests.
	 *
	 * @param e1
	 * @param e2
	 * @return
	 */
	private static double dn(HttpFlow e1, HttpFlow e2) {

		List<String> n1 = e1.getUrlQueryKeys();
		List<String> n2 = e2.getUrlQueryKeys();

		return JaccardDistanceMeasure.distance(n1, n2);
	}

	/**
	 *
	 * We define dv(rk, rh) to be equal to the normalized Levenshtein distance
	 * between strings obtained by concatenating the parameter values (e.g.,
	 * 1.0.0-RC1US).
	 */
	private static double dv(HttpFlow e1, HttpFlow e2) {
		List<String> v1 = e1.getUrlQueryValues();
		List<String> v2 = e2.getUrlQueryValues();

		if (v1 != null && v2 != null) {
			StringBuffer sb1 = new StringBuffer();
			for (String string : v1) {
				sb1.append(string);
			}

			StringBuffer sb2 = new StringBuffer();
			for (String string : v2) {
				sb2.append(string);
			}

			// concat strings ands
			return LevenshteinDistanceMeasure.normalizedDistance(sb1, sb2);
		} else {
			return 0;
		}
	}
}
