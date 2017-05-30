package com.hurence.logisland.processor.useragent;

import nl.basjes.parse.useragent.UserAgentAnalyzer;
import org.apache.commons.pool2.PooledObjectFactory;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

/**
 * Pool to hold instances of the UserAgentAnalyzer object
 */
public class UserAgentAnalyzerPool extends GenericObjectPool<UserAgentAnalyzer> {

    /**
     * Constructor.
     * It uses the default configuration for pool provided by
     * apache-commons-pool2.
     *
     * @param factory
     */
    public UserAgentAnalyzerPool(PooledObjectFactory<UserAgentAnalyzer> factory) {
        super(factory);
    }

    /**
     * Constructor.
     * This can be used to have full control over the pool using configuration
     * object.
     *
     * @param factory
     * @param config
     */
    public UserAgentAnalyzerPool(PooledObjectFactory<UserAgentAnalyzer> factory, GenericObjectPoolConfig config) {
        super(factory, config);
    }
}
