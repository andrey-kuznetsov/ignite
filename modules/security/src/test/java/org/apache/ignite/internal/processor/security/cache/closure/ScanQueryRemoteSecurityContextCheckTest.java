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

package org.apache.ignite.internal.processor.security.cache.closure;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.UUID;
import javax.cache.Cache;
import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.internal.processor.security.AbstractCacheOperationRemoteSecurityContextCheckTest;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.lang.IgniteClosure;
import org.apache.ignite.lang.IgniteRunnable;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Testing operation security context when the filter of ScanQuery is executed on remote node.
 * <p>
 * The initiator node broadcasts a task to feature call node that starts ScanQuery's filter. That filter is executed on
 * feature transition node and broadcasts a task to endpoint nodes. On every step, it is performed verification that
 * operation security context is the initiator context.
 */
@RunWith(JUnit4.class)
public class ScanQueryRemoteSecurityContextCheckTest extends AbstractCacheOperationRemoteSecurityContextCheckTest {
    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGrid(SRV_INITIATOR, allowAllPermissionSet());

        startClient(CLNT_INITIATOR, allowAllPermissionSet());

        startGrid(SRV_RUN, allowAllPermissionSet());

        startClient(CLNT_RUN, allowAllPermissionSet());

        startGrid(SRV_CHECK, allowAllPermissionSet());

        startGrid(SRV_ENDPOINT, allowAllPermissionSet());

        startClient(CLNT_ENDPOINT, allowAllPermissionSet());

        G.allGrids().get(0).cluster().active(true);
    }

    /** {@inheritDoc} */
    @Override protected void setupVerifier(Verifier verifier) {
        verifier
            .expect(SRV_RUN, 1)
            .expect(CLNT_RUN, 1)
            .expect(SRV_CHECK, 2)
            .expect(SRV_ENDPOINT, 2)
            .expect(CLNT_ENDPOINT, 2);
    }

    /**
     *
     */
    @Test
    public void test() throws Exception {
        grid(SRV_INITIATOR).cache(CACHE_NAME)
            .put(prmKey(grid(SRV_CHECK)), 1);

        awaitPartitionMapExchange();

        runAndCheck(grid(SRV_INITIATOR), runnables());
        runAndCheck(grid(CLNT_INITIATOR), runnables());
    }

    /** {@inheritDoc} */
    @Override protected Collection<UUID> nodesToCheck() {
        return Collections.singletonList(nodeId(SRV_CHECK));
    }

    /**
     *
     */
    private Collection<IgniteRunnable> runnables() {
        return Arrays.asList(
            () -> {
                register();

                Ignition.localIgnite().cache(CACHE_NAME).query(
                    new ScanQuery<>(
                        new CommonClosure(SRV_CHECK, endpoints())
                    )
                ).getAll();
            },
            () -> {
                register();

                Ignition.localIgnite().cache(CACHE_NAME).query(
                    new ScanQuery<>((k, v) -> true),
                    new CommonClosure(SRV_CHECK, endpoints())
                ).getAll();
            }
        );
    }

    /**
     * Common closure for tests.
     */
    static class CommonClosure implements
        IgniteClosure<Cache.Entry<Integer, Integer>, Integer>,
        IgniteBiPredicate<Integer, Integer> {
        /** Expected local node name. */
        private final String node;

        /** Collection of endpont nodes ids. */
        private final Collection<UUID> endpoints;

        /**
         * @param node Expected local node name.
         * @param endpoints Collection of endpont nodes ids.
         */
        public CommonClosure(String node, Collection<UUID> endpoints) {
            assert !endpoints.isEmpty();

            this.node = node;
            this.endpoints = endpoints;
        }

        /** {@inheritDoc} */
        @Override public Integer apply(Cache.Entry<Integer, Integer> entry) {
            verifyAndBroadcast();

            return entry.getValue();
        }

        /** {@inheritDoc} */
        @Override public boolean apply(Integer s, Integer i) {
            verifyAndBroadcast();

            return false;
        }

        /**
         *
         */
        private void verifyAndBroadcast() {
            Ignite loc = Ignition.localIgnite();

            if (node.equals(loc.name())) {
                register();

                compute(loc, endpoints)
                    .broadcast(() -> register());
            }
        }
    }
}
