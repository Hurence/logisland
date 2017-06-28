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
