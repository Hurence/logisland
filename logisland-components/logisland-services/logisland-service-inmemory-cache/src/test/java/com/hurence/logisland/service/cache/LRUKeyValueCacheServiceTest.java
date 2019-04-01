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
package com.hurence.logisland.service.cache;

import com.hurence.logisland.classloading.PluginProxy;
import com.hurence.logisland.component.InitializationException;
import com.hurence.logisland.controller.ControllerServiceInitializationContext;
import com.hurence.logisland.service.cache.model.Cache;
import com.hurence.logisland.service.cache.model.LRUCache;
import com.hurence.logisland.util.runner.TestRunner;
import com.hurence.logisland.util.runner.TestRunners;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;

/**
 * Created by gregoire on 19/05/17.
 */
public class LRUKeyValueCacheServiceTest {

    @Before
    public void setup() {
        // needed for calls to UserGroupInformation.setConfiguration() to work when passing in
        // config with Kerberos authentication enabled
        System.setProperty("java.security.krb5.realm", "logisland.com");
        System.setProperty("java.security.krb5.kdc", "logisland.kdc");
    }

    @Test
    public void testCache() throws InitializationException, IOException {

        final TestRunner runner = TestRunners.newTestRunner(new TestProcessor());

        // create the controller service and link it to the test processor
        final CacheService<String, String> service = new MockCacheService<String, String>(3);
        runner.addControllerService("lruCache", service);
        runner.enableControllerService(service);
        runner.setProperty(TestProcessor.CACHE_SERVICE, "lruCache");
        runner.assertValid(service);

        // try to put a single cell
        final CacheService cacheService = PluginProxy.unwrap(
                runner.getProcessContext().getPropertyValue(TestProcessor.CACHE_SERVICE).asControllerService()
        );

        cacheService.set("1", "1");
        cacheService.set("2", "2");
        cacheService.set("3", "3");
        cacheService.set("4", "4");
        cacheService.set("5", "5");

        assertEquals(null, cacheService.get("1"));
        assertEquals(null, cacheService.get("2"));
        assertEquals("4", cacheService.get("4"));
        assertEquals("5", cacheService.get("5"));
        assertEquals("3", cacheService.get("3"));

        cacheService.set("6", "6");

        assertEquals(null, cacheService.get("1"));
        assertEquals(null, cacheService.get("2"));
        assertEquals(null, cacheService.get("4"));
        assertEquals("5", cacheService.get("5"));
        assertEquals("3", cacheService.get("3"));
        assertEquals("6", cacheService.get("6"));
    }

    private class MockCacheService<K, V> extends LRUKeyValueCacheService<K, V> {

        private int cacheSize;

        public MockCacheService(final int cacheSize) {
            this.cacheSize = cacheSize;
        }

        @Override
        protected Cache<K, V> createCache(ControllerServiceInitializationContext context) throws IOException, InterruptedException {
            return new LRUCache<K, V>(cacheSize);
        }
    }
}
