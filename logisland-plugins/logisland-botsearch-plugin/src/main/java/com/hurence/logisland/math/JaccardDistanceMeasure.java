/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.hurence.logisland.math;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * The Jaccard index, also known as the Jaccard similarity coefficient
 * (originally coined coefficient de communauté by Paul Jaccard), is a statistic
 * used for comparing the similarity and diversity of sample sets.
 *
 * The Jaccard coefficient measures similarity between sample sets, and is
 * defined as the size of the intersection divided by the size of the union of
 * the sample sets.
 *
 * @author hattori_tsukasa
 *
 */
public class JaccardDistanceMeasure {

	/**
	 *
	 * @param a
	 * @param b
	 * @return
	 */
	public static double similarity(Object[] a, Object[] b) {
		int alen = a.length;
		int blen = b.length;
		Set<Object> set = new HashSet<Object>(alen + blen);
		set.addAll(Arrays.asList(a));
		set.addAll(Arrays.asList(b));

		return innerCalc(alen, blen, set.size());
	}

	/**
	 *
	 * @param a
	 * @param b
	 * @return
	 */
	public static double similarity(List<? extends Object> a, List<? extends Object> b) {
		int alen = 0;
		int blen = 0;

		// TODO check if guiving the iniial size to this sert is more efficient
		//Set<Object> set = new HashSet<Object>(alen + blen);
		
		Set<Object> set = new HashSet<Object>();
		
		if (a != null) {
			alen = a.size();
			set.addAll(a);
		}
		if (b != null) {
			blen = b.size();
			set.addAll(b);
		}

		if (alen == 0 && blen == 0) {
			return 1.0;
		} else {
			return innerCalc(alen, blen, set.size());
		}

		




	}

	public static double distance(List<? extends Object> a, List<? extends Object> b) {
		return 1.0 - similarity(a, b);
	}

	public static <K extends Comparable<K>> double calcByMerge(K[] a, K[] b) {
		return calcByMerge(a, 0, b, 0);
	}

	public static <K extends Comparable<K>> double calcByMerge(K[] a, int offsetA, K[] b, int offsetB) {

		int aLen = a.length - offsetA;
		int bLen = b.length - offsetB;

		int overlap = 0;
		int i = offsetA;
		int j = offsetB;
		while (i < a.length && j < b.length) {
			if (a[i].equals(b[j])) {
				overlap++;
				i++;
				j++;
			} else if (a[i].compareTo(b[j]) < 0) // a < b
			{
				i++;
			} else {
				j++;
			}
		}
		return overlap / (double) (aLen + bLen - overlap);

	}

	/**
	 *
	 * @param alen
	 * @param blen
	 * @param union
	 * @return
	 */
	private static double innerCalc(int alen, int blen, int union) {
		double overlap = alen + blen - union;
		if (overlap <= 0 || union == 0) {
			return 0.0;
		}
		return overlap / union;
	}


	/*
	 * Jaccard Similarity is a similarity function which is calculated by 
	 * first tokenizing the strings into sets and then taking the ratio of
	 * (weighted) intersection to their union 
	 */
	public static double jaccardSimilarity(String similar1, String similar2) {
		HashSet<String> h1 = new HashSet<String>();
		HashSet<String> h2 = new HashSet<String>();

		for (String s : similar1.split("\\s+")) {
			h1.add(s);
		}
		System.out.println("h1 " + h1);
		for (String s : similar2.split("\\s+")) {
			h2.add(s);
		}
		System.out.println("h2 " + h2);

		int sizeh1 = h1.size();
		//Retains all elements in h3 that are contained in h2 ie intersection
		h1.retainAll(h2);
		//h1 now contains the intersection of h1 and h2
		System.out.println("Intersection " + h1);


		h2.removeAll(h1);
		//h2 now contains unique elements
		System.out.println("Unique in h2 " + h2);

		//Union 
		int union = sizeh1 + h2.size();
		int intersection = h1.size();

		return (double) intersection / union;

	}
}
