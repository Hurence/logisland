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
package com.hurence.logisland.service.cache.model;

import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;

/**
 * Created by gregoire on 18/05/17.
 */
public class LRUCacheTest {

    private static Logger logger = LoggerFactory.getLogger(LRUCacheTest.class);

    @Test
    public void cacheTestLastRecentUsed() {
        LRUCache cache = new LRUCache<String, String>(5);
        Assert.assertEquals(0, cache.getClosestPowerOf2Lt(0));
        Assert.assertEquals(1, cache.getClosestPowerOf2Lt(1));
        Assert.assertEquals(2, cache.getClosestPowerOf2Lt(2));
        Assert.assertEquals(4, cache.getClosestPowerOf2Lt(5));
        Assert.assertEquals(4, cache.getClosestPowerOf2Lt(8));
        Assert.assertEquals(8, cache.getClosestPowerOf2Lt(10));
        Assert.assertEquals(8, cache.getClosestPowerOf2Lt(16));
        Assert.assertEquals(16, cache.getClosestPowerOf2Lt(17));
        Assert.assertEquals(16, cache.getClosestPowerOf2Lt(32));
        Assert.assertEquals(32, cache.getClosestPowerOf2Lt(33));
        Assert.assertEquals(32, cache.getClosestPowerOf2Lt(64));
        Assert.assertEquals(64, cache.getClosestPowerOf2Lt(65));
        Assert.assertEquals(64, cache.getClosestPowerOf2Lt(128));
        Assert.assertEquals(128, cache.getClosestPowerOf2Lt(129));
        Assert.assertEquals(128, cache.getClosestPowerOf2Lt(256));
        Assert.assertEquals(256, cache.getClosestPowerOf2Lt(257));
        Assert.assertEquals(256, cache.getClosestPowerOf2Lt(512));
        Assert.assertEquals(512, cache.getClosestPowerOf2Lt(513));
        Assert.assertEquals(512, cache.getClosestPowerOf2Lt(1024));
        Assert.assertEquals(1024, cache.getClosestPowerOf2Lt(1025));
        Assert.assertEquals(1024, cache.getClosestPowerOf2Lt(2048));
        Assert.assertEquals(2048, cache.getClosestPowerOf2Lt(2049));
        Assert.assertEquals(2048, cache.getClosestPowerOf2Lt(4096));
        Assert.assertEquals(4096, cache.getClosestPowerOf2Lt(4097));
        Assert.assertEquals(8192, cache.getClosestPowerOf2Lt(9000));
        Assert.assertEquals(16384, cache.getClosestPowerOf2Lt(18000));
        Assert.assertEquals(32768, cache.getClosestPowerOf2Lt(36000));
        Assert.assertEquals(65536, cache.getClosestPowerOf2Lt(72000));
        Assert.assertEquals(131072, cache.getClosestPowerOf2Lt(144000));
        Assert.assertEquals(262144, cache.getClosestPowerOf2Lt(288000));//2^18
        Assert.assertEquals(1073741824, cache.getClosestPowerOf2Lt(Integer.MAX_VALUE));//2^30 (max value = 2^31)

    }
}
