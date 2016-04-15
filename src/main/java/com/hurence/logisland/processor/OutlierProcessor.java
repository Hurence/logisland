package com.hurence.logisland.processor;

import com.hurence.logisland.event.Event;
import com.hurence.logisland.event.EventProcessor;

import java.util.Collection;

/**
 * Outlier Analysis: A Hybrid Approach
 *
 * In order to function at scale, a two-phase approach is taken
 *
 * - For every data point
 *      - Detect outlier candidates using a robust estimator of variability (e.g. median absolute deviation) that uses distributional sketching (e.g. Q-trees)
 *      - Gather a biased sample (biased by recency)
 *      - Extremely deterministic in space and cheap in computation
 *
 * - For every outlier candidate
 *      - Use traditional, more computationally complex approaches to outlier analysis (e.g. Robust PCA) on the biased sample
 *      - Expensive computationally, but run infrequently
 *
 * This becomes a data filter which can be attached to a timeseries data stream within a distributed computational framework (i.e. Storm, Spark, Flink, NiFi) to detect outliers.
 */
public class OutlierProcessor implements EventProcessor {

    /**
     * take a line of csv and convert it to a NetworkFlow
     *
     * @param
     * @return
     */
    public Collection<Event> process(Collection<Event> events) {

        return null;
    }
}
