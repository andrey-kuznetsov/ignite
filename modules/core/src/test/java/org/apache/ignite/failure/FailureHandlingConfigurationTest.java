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

package org.apache.ignite.failure;

import java.lang.management.ManagementFactory;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import javax.management.MBeanServer;
import javax.management.MBeanServerInvocationHandler;
import javax.management.ObjectName;
import org.apache.ignite.Ignite;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.persistence.IgniteCacheDatabaseSharedManager;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.worker.BlockingOperationControlMXBeanImpl;
import org.apache.ignite.internal.worker.WorkersRegistry;
import org.apache.ignite.mxbean.BlockingOperationControlMXBean;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_CHECKPOINT_READ_LOCK_TIMEOUT;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_SYSTEM_WORKER_BLOCKED_TIMEOUT;

/**
 * Tests configuration parameters related to failure handling.
 */
public class FailureHandlingConfigurationTest extends GridCommonAbstractTest {
    /** */
    private Long checkpointReadLockTimeout;

    /** */
    private Long sysWorkerBlockedTimeout;

    /** */
    private CountDownLatch failureLatch;

    /** */
    private class TestFailureHandler extends AbstractFailureHandler {
        /** */
        TestFailureHandler() {
            failureLatch = new CountDownLatch(1);
        }

        /** {@inheritDoc} */
        @Override protected boolean handle(Ignite ignite, FailureContext failureCtx) {
            failureLatch.countDown();

            return false;
        }
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setFailureHandler(new TestFailureHandler());

        DataRegionConfiguration drCfg = new DataRegionConfiguration();
        drCfg.setPersistenceEnabled(true);

        DataStorageConfiguration dsCfg = new DataStorageConfiguration();
        dsCfg.setDefaultDataRegionConfiguration(drCfg);

        cfg.setDataStorageConfiguration(dsCfg);

        if (checkpointReadLockTimeout != null)
            cfg.setCheckpointReadLockTimeout(checkpointReadLockTimeout);

        if (sysWorkerBlockedTimeout != null)
            cfg.setSystemWorkerBlockedTimeout(sysWorkerBlockedTimeout);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        cleanPersistenceDir();

        sysWorkerBlockedTimeout = null;
        checkpointReadLockTimeout = null;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();
    }

    /**
     * @throws Exception If failed.
     */
    public void testCfgParamsPropagation() throws Exception {
        sysWorkerBlockedTimeout = 30_000L;
        checkpointReadLockTimeout = 20_000L;

        IgniteEx ignite = startGrid(0);

        ignite.cluster().active(true);

        WorkersRegistry reg = ignite.context().workersRegistry();

        IgniteCacheDatabaseSharedManager dbMgr = ignite.context().cache().context().database();

        BlockingOperationControlMXBean mBean = getMBean();

        assertEquals(sysWorkerBlockedTimeout.longValue(), reg.getSystemWorkerBlockedTimeout());
        assertEquals(checkpointReadLockTimeout.longValue(), dbMgr.getCheckpointReadLockTimeout());

        assertEquals(sysWorkerBlockedTimeout.longValue(), mBean.getSystemWorkerBlockedTimeout());
        assertEquals(checkpointReadLockTimeout.longValue(), mBean.getCheckpointReadLockTimeout());
    }

    /**
     * @throws Exception If failed.
     */
    public void testNegativeParamValues() throws Exception {
        sysWorkerBlockedTimeout = -1L;
        checkpointReadLockTimeout = -85L;

        IgniteEx ignite = startGrid(0);

        ignite.cluster().active(true);

        WorkersRegistry reg = ignite.context().workersRegistry();

        IgniteCacheDatabaseSharedManager dbMgr = ignite.context().cache().context().database();

        BlockingOperationControlMXBean mBean = getMBean();

        assertEquals(0L, reg.getSystemWorkerBlockedTimeout());
        assertEquals(0L, dbMgr.getCheckpointReadLockTimeout());

        assertEquals(0L, mBean.getSystemWorkerBlockedTimeout());
        assertEquals(0L, mBean.getCheckpointReadLockTimeout());
    }

    /**
     * @throws Exception If failed.
     */
    public void testOverridingBySysProps() throws Exception {
        String prevWorkerProp = System.getProperty(IGNITE_SYSTEM_WORKER_BLOCKED_TIMEOUT);
        String prevCheckpointProp = System.getProperty(IGNITE_CHECKPOINT_READ_LOCK_TIMEOUT);

        long workerPropVal = 80_000;
        long checkpointPropVal = 90_000;

        System.setProperty(IGNITE_SYSTEM_WORKER_BLOCKED_TIMEOUT, String.valueOf(workerPropVal));
        System.setProperty(IGNITE_CHECKPOINT_READ_LOCK_TIMEOUT, String.valueOf(checkpointPropVal));

        try {
            sysWorkerBlockedTimeout = 1L;
            checkpointReadLockTimeout = 2L;

            IgniteEx ignite = startGrid(0);

            ignite.cluster().active(true);

            WorkersRegistry reg = ignite.context().workersRegistry();

            IgniteCacheDatabaseSharedManager dbMgr = ignite.context().cache().context().database();

            BlockingOperationControlMXBean mBean = getMBean();

            assertEquals(sysWorkerBlockedTimeout, ignite.configuration().getSystemWorkerBlockedTimeout());
            assertEquals(checkpointReadLockTimeout, ignite.configuration().getCheckpointReadLockTimeout());

            assertEquals(workerPropVal, reg.getSystemWorkerBlockedTimeout());
            assertEquals(checkpointPropVal, dbMgr.getCheckpointReadLockTimeout());

            assertEquals(workerPropVal, mBean.getSystemWorkerBlockedTimeout());
            assertEquals(checkpointPropVal, mBean.getCheckpointReadLockTimeout());
        }
        finally {
            if (prevWorkerProp != null)
                System.setProperty(IGNITE_SYSTEM_WORKER_BLOCKED_TIMEOUT, prevWorkerProp);
            else
                System.clearProperty(IGNITE_SYSTEM_WORKER_BLOCKED_TIMEOUT);

            if (prevCheckpointProp != null)
                System.setProperty(IGNITE_CHECKPOINT_READ_LOCK_TIMEOUT, prevCheckpointProp);
            else
                System.clearProperty(IGNITE_CHECKPOINT_READ_LOCK_TIMEOUT);
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testMBeanParamsChanging() throws Exception {
        IgniteEx ignite = startGrid(0);

        ignite.cluster().active(true);

        BlockingOperationControlMXBean mBean = getMBean();

        mBean.setSystemWorkerBlockedTimeout(80_000L);
        assertEquals(80_000L, ignite.context().workersRegistry().getSystemWorkerBlockedTimeout());

        mBean.setCheckpointReadLockTimeout(90_000L);
        assertEquals(90_000L, ignite.context().cache().context().database().getCheckpointReadLockTimeout());
    }

    /**
     * @throws Exception If failed.
     */
    public void testFailureOnMBeanParamChange() throws Exception {
        IgniteEx ignite = startGrid(0);

        ignite.cluster().active(true);

        BlockingOperationControlMXBean mBean = getMBean();

        mBean.setSystemWorkerBlockedTimeout(1);

        assertTrue(failureLatch.await(3, TimeUnit.SECONDS));
    }

    /** */
    private BlockingOperationControlMXBean getMBean() throws Exception {
        ObjectName name = U.makeMBeanName(getTestIgniteInstanceName(0), "Kernal",
            BlockingOperationControlMXBeanImpl.class.getSimpleName());

        MBeanServer srv = ManagementFactory.getPlatformMBeanServer();

        assertTrue(srv.isRegistered(name));

        return MBeanServerInvocationHandler.newProxyInstance(srv, name, BlockingOperationControlMXBean.class, true);
    }
}
