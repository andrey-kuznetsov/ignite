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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.function.BinaryOperator;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteServices;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.ComputeJobAdapter;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.compute.ComputeTaskAdapter;
import org.apache.ignite.internal.processor.security.AbstractSecurityTest;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.plugin.security.SecurityException;
import org.apache.ignite.plugin.security.SecurityPermissionSet;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.services.Service;
import org.apache.ignite.services.ServiceContext;
import org.apache.ignite.testframework.GridTestUtils;
import org.jetbrains.annotations.Nullable;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import static org.apache.ignite.plugin.security.SecurityPermission.CACHE_PUT;
import static org.apache.ignite.plugin.security.SecurityPermission.CACHE_READ;
import static org.apache.ignite.plugin.security.SecurityPermission.CACHE_REMOVE;
import static org.apache.ignite.plugin.security.SecurityPermission.SERVICE_CANCEL;
import static org.apache.ignite.plugin.security.SecurityPermission.SERVICE_DEPLOY;
import static org.apache.ignite.plugin.security.SecurityPermission.SERVICE_INVOKE;
import static org.apache.ignite.plugin.security.SecurityPermission.TASK_EXECUTE;

/**
 * Testing permissions on various operations in services.
 */
@RunWith(JUnit4.class)
public class ServiceSecurityContextTest extends AbstractSecurityTest {
    /** */
    private static final String TEST_SERVICE_NAME = "testService";

    /** */
    private static final ConcurrentMap<String, Boolean> OPERATIONS_STATUS = new ConcurrentHashMap<>();

    /**
     *
     */
    private interface TestService extends Service, Runnable {
    }

    /**
     *
     */
    private abstract static class AbstractPlainTestService implements TestService {
        /** */
        @IgniteInstanceResource
        Ignite ignite;

        /** */
        boolean isDone;

        /** */
        abstract void doCacheOperations(Runnable r);

        /**
         *
         */
        @Override public synchronized void init(ServiceContext ctx) {
            isDone = false;

            doCacheOperations(() -> {
                cachePut("init/put", "nevermind");
                cacheRemove("init/remove");
            });
        }

        /**
         *
         */
        @Override public synchronized void execute(ServiceContext ctx) {
            try {
                doCacheOperations(() -> {
                    cachePut("exec/put", "nevermind");
                    cacheRemove("exec/remove");
                });
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

            doCacheOperations(() -> {
                cachePut("cancel/put", "nevermind");
                cacheRemove("cancel/remove");
            });
        }

        /**
         *
         */
        @Override public void run() {
            doCacheOperations(() -> {
                cachePut("invoke/put", "nevermind");
                cacheRemove("invoke/remove");
            });
        }

        /**
         *
         */
        void cachePut(String k, String v) {
            try {
                ignite.getOrCreateCache(DEFAULT_CACHE_NAME).put(k, v);

                OPERATIONS_STATUS.put(k, true);
            }
            catch (SecurityException ignored) {
                OPERATIONS_STATUS.put(k, false);
            }
        }

        /**
         *
         */
        void cacheRemove(String k) {
            try {
                ignite.getOrCreateCache(DEFAULT_CACHE_NAME).remove(k);

                OPERATIONS_STATUS.put(k, true);
            }
            catch (SecurityException ignored) {
                OPERATIONS_STATUS.put(k, false);
            }
        }
    }

    /**
     *
     */
    private static class PlainTestServiceSyncImpl extends AbstractPlainTestService {
        /**
         *
         */
        @Override void doCacheOperations(Runnable r) {
            r.run();
        }
    }

    /**
     *
     */
    private static class PlainTestServiceAsyncNewThreadImpl extends AbstractPlainTestService {
        /**
         *
         */
        @Override void doCacheOperations(Runnable r) {
            Thread t = new Thread(r);

            t.start();

            try {
                t.join(5000);
            }
            catch (InterruptedException ignored) {}
        }
    }

    /**
     *
     */
    private static class PlainTestServiceAsyncCommonPoolImpl extends AbstractPlainTestService {
        /**
         *
         */
        @Override void doCacheOperations(Runnable r) {
            try {
                CompletableFuture.runAsync(r).get(5, TimeUnit.SECONDS);
            }
            catch (Exception ignored) {}
        }
    }

    /**
     *
     */
    @Before
    public void before() throws Exception {
        cleanPersistenceDir();

        OPERATIONS_STATUS.clear();
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
    public void testPlainSyncCacheOperations() throws Exception {
        doTestPlain(new PlainTestServiceSyncImpl());
    }

    /**
     *
     */
    @Test
    @Ignore("IMDBGG-1576")
    public void testPlainWithCacheOperationsInNewThread() throws Exception {
        doTestPlain(new PlainTestServiceAsyncNewThreadImpl());
    }

    /**
     *
     */
    @Test
    @Ignore("IMDBGG-1576")
    public void testPlainWithCacheOperationsInCommonPool() throws Exception {
        doTestPlain(new PlainTestServiceAsyncCommonPoolImpl());
    }

    /**
     *
     */
    private void doTestPlain(AbstractPlainTestService svc) throws Exception {
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

        String[] lifecycle = { "init", "exec", "invoke", "cancel" };

        for (String phase : lifecycle)
            cache1.put(phase + "/remove", "nevermind");

        IgniteServices svcs = remoteServices(ignite0);

        svcs.deployNodeSingleton(TEST_SERVICE_NAME, svc);

        Runnable svcProxy = svcs.serviceProxy(TEST_SERVICE_NAME, Runnable.class, true);

        svcProxy.run();

        svcs.cancel(TEST_SERVICE_NAME);

        for (String phase : lifecycle) {
            assertFalse("Put operation should fail on " + phase, OPERATIONS_STATUS.remove(phase + "/put"));
            assertFalse("Remove operation should fail on " + phase, OPERATIONS_STATUS.remove(phase + "/remove"));
        }
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
            remoteServices(ignite).deployNodeSingleton(TEST_SERVICE_NAME, new PlainTestServiceSyncImpl());

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

        String[] lifecycle = { "init", "exec", "invoke", "cancel" };

        for (String phase : lifecycle)
            cache1.put(phase + "/remove", "nevermind");

        IgniteServices svcs = localServices(ignite0);

        svcs.deployNodeSingleton(derivingTestSrvcName, new DerivingTestServiceImpl());

        assertTrue(GridTestUtils.waitForCondition(() -> DerivingTestServiceImpl.executed, 3000));

        Runnable svcProxy = svcs.serviceProxy(derivingTestSrvcName, Runnable.class, true);

        svcProxy.run();

        svcs.cancel(derivingTestSrvcName);
        assertTrue(GridTestUtils.waitForCondition(() -> DerivingTestServiceImpl.cancelled, 3000));

        for (String phase : lifecycle) {
            assertFalse("Put operation should fail on " + phase, OPERATIONS_STATUS.remove(phase + "/put"));
            assertFalse("Remove operation should fail on " + phase, OPERATIONS_STATUS.remove(phase + "/remove"));
        }
    }

    /**
     *
     */
    private static class ComputeTestServiceImpl implements TestService {
        /** */
        @IgniteInstanceResource
        Ignite ignite;

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
            // No-op.
        }

        /**
         *
         */
        @Override public void cancel(ServiceContext ctx) {
            // No-op.
        }

        /**
         *
         */
        @Override public void run() {
            ignite.compute().execute(TestComputeTask.class, null);
        }
    }

    /**
     *
     */
    private static class TestComputeTask extends ComputeTaskAdapter<Void, Integer> {
        /** */
        static final BinaryOperator<Integer> reducer = (a, b) -> (a + b) % 10;

        /** {@inheritDoc} */
        @Override public @Nullable Map<? extends ComputeJob, ClusterNode> map(
            List<ClusterNode> subgrid,
            @Nullable Void arg) throws IgniteException {

            Map<ComputeJobAdapter, ClusterNode> result = new HashMap<>();

            for (ClusterNode node : subgrid) {
                List<Integer> ints = IntStream.generate(new Random()::nextInt)
                    .limit(100)
                    .boxed()
                    .collect(Collectors.toList());

                result.put(new ComputeJobAdapter() {
                    @Override public Integer execute() throws IgniteException {
                        return ints.stream().reduce(0, reducer);
                    }
                }, node);
            }

            return result;
        }

        /** {@inheritDoc} */
        @Nullable @Override public Integer reduce(List<ComputeJobResult> results) throws IgniteException {
            return results.stream().map(ComputeJobResult::getData).map(Integer.class::cast).reduce(0, reducer);
        }
    }

    /**
     *
     */
    @Test
    public void testComputeInService() throws Exception {
        String computeTestSrvcName = "computeTestSrvcName";

        SecurityPermissionSet permSet0 = builder()
            .appendServicePermissions(computeTestSrvcName, SERVICE_DEPLOY, SERVICE_INVOKE, SERVICE_CANCEL)
            .appendTaskPermissions(TestComputeTask.class.getName())
            .build();

        SecurityPermissionSet permSet1 = builder()
            .appendTaskPermissions(TestComputeTask.class.getName(), TASK_EXECUTE)
            .build();

        String testLogin = "scott";

        Ignite ignite0 = startGrid("node0", testLogin, "", permSet0);

        startGrid("node1", testLogin, "", permSet1);

        ignite0.cluster().active(true);

        IgniteServices svcs = remoteServices(ignite0);

        svcs.deployNodeSingleton(computeTestSrvcName, new ComputeTestServiceImpl());

        Runnable svcProxy = svcs.serviceProxy(computeTestSrvcName, Runnable.class, true);

        try {
            svcProxy.run();

            fail();
        }
        catch (Exception e) {
            assertTrue(X.hasCause(e, IgniteException.class));
        }
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
