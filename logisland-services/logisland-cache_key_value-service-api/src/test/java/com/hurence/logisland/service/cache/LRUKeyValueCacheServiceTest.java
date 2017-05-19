package com.hurence.logisland.service.cache;

import com.hurence.logisland.component.InitializationException;
import com.hurence.logisland.controller.ControllerServiceInitializationContext;
import com.hurence.logisland.service.cache.model.Cache;
import com.hurence.logisland.service.cache.model.LRUCache;
import com.hurence.logisland.util.runner.TestRunner;
import com.hurence.logisland.util.runner.TestRunners;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.*;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

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

        final TestRunner runner = TestRunners.newTestRunner(TestProcessor.class);

        // create the controller service and link it to the test processor
        final CacheService<String, String> service = new MockCacheService<String, String>(3);
        runner.addControllerService("lruCache", service);
        runner.enableControllerService(service);
        runner.setProperty(TestProcessor.CACHE_SERVICE, "lruCache");
        runner.assertValid(service);

        // try to put a single cell
        final CacheService cacheService = runner.getProcessContext().getPropertyValue(TestProcessor.CACHE_SERVICE)
                .asControllerService(CacheService.class);

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
    private class MockCacheService<K,V> extends LRUKeyValueCacheService<K,V> {

        private int cacheSize;

        public MockCacheService(final int cacheSize) {
            this.cacheSize = cacheSize;
        }

        @Override
        protected Cache<K, V> createCache(ControllerServiceInitializationContext context) throws IOException, InterruptedException {
            return new LRUCache<K,V>(cacheSize);
        }
    }
}
