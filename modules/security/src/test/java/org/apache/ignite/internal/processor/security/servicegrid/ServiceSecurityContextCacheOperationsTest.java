/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processor.security.servicegrid;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteServices;
import org.apache.ignite.internal.processor.security.AbstractSecurityTest;
import org.apache.ignite.plugin.security.SecurityException;
import org.apache.ignite.plugin.security.SecurityPermissionSet;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.services.Service;
import org.apache.ignite.services.ServiceContext;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import static org.apache.ignite.plugin.security.SecurityPermission.CACHE_PUT;
import static org.apache.ignite.plugin.security.SecurityPermission.CACHE_READ;
import static org.apache.ignite.plugin.security.SecurityPermission.CACHE_REMOVE;
import static org.apache.ignite.plugin.security.SecurityPermission.SERVICE_CANCEL;
import static org.apache.ignite.plugin.security.SecurityPermission.SERVICE_DEPLOY;
import static org.apache.ignite.plugin.security.SecurityPermission.SERVICE_INVOKE;

/**
 * Testing permissions on cache operations in services.
 */
@RunWith(JUnit4.class)
public class ServiceSecurityContextCacheOperationsTest extends AbstractSecurityTest {
    /** */
    private static final String TEST_SERVICE_NAME = "testService";

    /**
     *
     */
    private interface TestService extends Service, Runnable {
    }

    /**
     *
     */
    private static class PlainTestServiceImpl implements TestService {
        /** */
        @IgniteInstanceResource
        Ignite ignite;

        /** */
        boolean isDone;

        /**
         *
         */
        @Override public synchronized void init(ServiceContext ctx) {
            isDone = false;

            cachePut("kInitPut", "vInitPut");
            cacheRemove("kInitRemove");
        }

        /**
         *
         */
        @Override public synchronized void execute(ServiceContext ctx) {
            try {
                cachePut("kExecPut", "vExecPut");
                cacheRemove("kExecRemove");
            }
            finally {
                isDone = true;

                notifyAll();
            }
        }

        /**
         *
         */
        @Override public synchronized void cancel(ServiceContext ctx) {
            while (!isDone) {
                try {
                    wait(5000);
                }
                catch (InterruptedException e) {
                    Thread.currentThread().interrupt();

                    return;
                }
            }

            cachePut("kCancelPut", "vCancelPut");
            cacheRemove("kCancelRemove");
        }

        /**
         *
         */
        @Override public void run() {
            cachePut("kInvokePut", "vInvokePut");
            cacheRemove("kInvokeRemove");
        }

        /**
         *
         */
        void cachePut(String k, String v) {
            try {
                ignite.getOrCreateCache(DEFAULT_CACHE_NAME).put(k, v);
            }
            catch (SecurityException ignored) {}
        }

        /**
         *
         */
        void cacheRemove(String k) {
            try {
                ignite.getOrCreateCache(DEFAULT_CACHE_NAME).remove(k);
            }
            catch (SecurityException ignored) {}
        }
    }

    /**
     *
     */
    @Before
    public void before() throws Exception {
        cleanPersistenceDir();
    }

    /**
     *
     */
    @After
    public void after() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();
    }

    /**
     *
     */
    @Test
    public void testPlain() throws Exception {
        SecurityPermissionSet permSet0 = builder()
            .appendCachePermissions(DEFAULT_CACHE_NAME, CACHE_READ)
            .appendServicePermissions(TEST_SERVICE_NAME, SERVICE_DEPLOY, SERVICE_INVOKE, SERVICE_CANCEL)
            .build();

        SecurityPermissionSet permSet1 = builder()
            .appendCachePermissions(DEFAULT_CACHE_NAME, CACHE_READ, CACHE_PUT, CACHE_REMOVE)
            .build();

        String testLogin = "scott";

        Ignite ignite0 = startGrid("node0", testLogin, "", permSet0);
        Ignite ignite1 = startGrid("node1", testLogin, "", permSet1);

        ignite0.cluster().active(true);

        IgniteCache<String, String> cache1 = ignite1.getOrCreateCache(DEFAULT_CACHE_NAME);

        cache1.put("kInitRemove", "vInitRemove");
        cache1.put("kExecRemove", "vExecRemove");
        cache1.put("kInvokeRemove", "vInvokeRemove");
        cache1.put("kCancelRemove", "vCancelRemove");

        IgniteServices svcs = remoteServices(ignite0);

        svcs.deployNodeSingleton(TEST_SERVICE_NAME, new PlainTestServiceImpl());

        Runnable svcProxy = svcs.serviceProxy(TEST_SERVICE_NAME, Runnable.class, true);

        svcProxy.run();

        svcs.cancel(TEST_SERVICE_NAME);

        assertNull("IMDBGG-1483", cache1.get("kInitPut"));
        assertNull("IMDBGG-1617", cache1.get("kExecPut"));
        assertNull(cache1.get("kInvokePut"));
        assertNull("IMDBGG-1483", cache1.get("kCancelPut"));

        assertEquals("IMDBGG-1483", "vInitRemove", cache1.get("kInitRemove"));
        assertEquals("IMDBGG-1617", "vExecRemove", cache1.get("kExecRemove"));
        assertEquals("vInvokeRemove", cache1.get("kInvokeRemove"));
        assertEquals("IMDBGG-1483", "vCancelRemove", cache1.get("kCancelRemove"));

        stopAllGrids();
    }

    /**
     *
     */
    private static class DerivingTestServiceImpl implements TestService {
        /** */
        static volatile boolean executed;

        /** */
        static volatile boolean cancelled;

        /** */
        @IgniteInstanceResource
        Ignite ignite;

        /**
         *
         */
        DerivingTestServiceImpl() {
            executed = cancelled = false;
        }

        /**
         *
         */
        @Override public void init(ServiceContext ctx) {
            // No-op.
        }

        /**
         *
         */
        @Override public void execute(ServiceContext ctx) {
            remoteServices(ignite).deployNodeSingleton(TEST_SERVICE_NAME, new PlainTestServiceImpl());

            executed = true;
        }

        /**
         *
         */
        @Override public void cancel(ServiceContext ctx) {
            remoteServices(ignite).cancelAsync(TEST_SERVICE_NAME).listen(fut -> cancelled = true);
        }

        /**
         *
         */
        @Override public void run() {
            Runnable svcProxy = remoteServices(ignite).serviceProxy(TEST_SERVICE_NAME, Runnable.class, true);

            svcProxy.run();
        }
    }

    /**
     *
     */
    @Test
    public void testDerivedService() throws Exception {
        String derivingTestSrvcName = "derivingTestService";

        SecurityPermissionSet permSet0 = builder()
            .appendCachePermissions(DEFAULT_CACHE_NAME, CACHE_READ)
            .appendServicePermissions(derivingTestSrvcName, SERVICE_DEPLOY, SERVICE_INVOKE, SERVICE_CANCEL)
            .build();

        SecurityPermissionSet permSet1 = builder()
            .appendCachePermissions(DEFAULT_CACHE_NAME, CACHE_READ, CACHE_PUT, CACHE_REMOVE)
            .appendServicePermissions(TEST_SERVICE_NAME, SERVICE_DEPLOY, SERVICE_INVOKE, SERVICE_CANCEL)
            .build();

        String testLogin = "scott";

        Ignite ignite0 = startGrid("node0", testLogin, "", permSet0);
        Ignite ignite1 = startGrid("node1", testLogin, "", permSet1);

        ignite0.cluster().active(true);

        IgniteCache<String, String> cache1 = ignite1.getOrCreateCache(DEFAULT_CACHE_NAME);

        cache1.put("kInitRemove", "vInitRemove");
        cache1.put("kExecRemove", "vExecRemove");
        cache1.put("kInvokeRemove", "vInvokeRemove");
        cache1.put("kCancelRemove", "vCancelRemove");

        IgniteServices svcs = localServices(ignite0);

        svcs.deployNodeSingleton(derivingTestSrvcName, new DerivingTestServiceImpl());

        assertTrue(GridTestUtils.waitForCondition(() -> DerivingTestServiceImpl.executed, 3000));

        Runnable svcProxy = svcs.serviceProxy(derivingTestSrvcName, Runnable.class, true);

        svcProxy.run();

        svcs.cancel(derivingTestSrvcName);
        assertTrue(GridTestUtils.waitForCondition(() -> DerivingTestServiceImpl.cancelled, 3000));

        assertNull("IMDBGG-1483", cache1.get("kInitPut"));
        assertNull("IMDBGG-1617", cache1.get("kExecPut"));
        assertNull(cache1.get("kInvokePut"));
        assertNull("IMDBGG-1483", cache1.get("kCancelPut"));

        assertEquals("IMDBGG-1483", "vInitRemove", cache1.get("kInitRemove"));
        assertEquals("IMDBGG-1617", "vExecRemove", cache1.get("kExecRemove"));
        assertEquals("vInvokeRemove", cache1.get("kInvokeRemove"));
        assertEquals("IMDBGG-1483", "vCancelRemove", cache1.get("kCancelRemove"));

        stopAllGrids();
    }

    /**
     *
     */
    private static IgniteServices localServices(Ignite ignite) {
        return ignite.services(ignite.cluster().forLocal());
    }

    /**
     *
     */
    private static IgniteServices remoteServices(Ignite ignite) {
        return ignite.services(ignite.cluster().forRemotes());
    }
}
