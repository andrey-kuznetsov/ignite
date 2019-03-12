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
import java.util.List;
import java.util.UUID;
import javax.cache.processor.EntryProcessor;
import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.MutableEntry;
import org.apache.ignite.Ignition;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processor.security.AbstractCacheOperationRemoteSecurityContextCheckTest;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.lang.IgniteRunnable;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Testing permissions when EntryProcessor closure is executed cache operations on remote node.
 */
@RunWith(JUnit4.class)
public class EntryProcessorRemoteSecurityContextCheckTest extends AbstractCacheOperationRemoteSecurityContextCheckTest {
    /** Name of server initiator node. */
    private static final String SRV_INITIATOR = "srv_initiator";

    /** Name of client initiator node. */
    private static final String CLNT_INITIATOR = "clnt_initiator";

    /** Name of server feature call node. */
    private static final String SRV_FEATURE_CALL = "srv_feature_call";

    /** Name of server feature transit node. */
    private static final String SRV_FEATURE_TRANSITION = "srv_feature_transition";

    /** Name of server endpoint node. */
    private static final String SRV_ENDPOINT = "srv_endpoint";

    /** Name of client endpoint node. */
    private static final String CLNT_ENDPOINT = "clnt_endpoint";

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGrid(SRV_INITIATOR, allowAllPermissionSet());

        startGrid(CLNT_INITIATOR, allowAllPermissionSet(), true);

        startGrid(SRV_FEATURE_CALL, allowAllPermissionSet());

        startGrid(SRV_FEATURE_TRANSITION, allowAllPermissionSet());

        startGrid(SRV_ENDPOINT, allowAllPermissionSet());

        startGrid(CLNT_ENDPOINT, allowAllPermissionSet(), true);

        G.allGrids().get(0).cluster().active(true);
    }

    /** {@inheritDoc} */
    @Override protected void setupVerifier(Verifier verifier) {
        verifier
            .add(SRV_FEATURE_CALL, 1)
            .add(SRV_FEATURE_TRANSITION, 1)
            .add(SRV_ENDPOINT, 1)
            .add(CLNT_ENDPOINT, 1);
    }

    /**
     *
     */
    @Test
    public void test() {
        runAndCheck(grid(SRV_INITIATOR));
        runAndCheck(grid(CLNT_INITIATOR));
    }

    /**
     * @param initiator Node that initiates an execution.
     */
    private void runAndCheck(IgniteEx initiator) {
        UUID secSubjectId = secSubjectId(initiator);

        for (IgniteRunnable r : featureRuns()) {
            runAndCheck(
                secSubjectId,
                () -> compute(initiator, nodeId(SRV_FEATURE_CALL)).broadcast(
                    new IgniteRunnable() {
                        @Override public void run() {
                            register();

                            r.run();
                        }
                    }
                )
            );
        }
    }

    /**
     * @return Collection of runnables to call invoke methods.
     */
    private List<IgniteRunnable> featureRuns() {
        final Integer key = prmKey(grid(SRV_FEATURE_TRANSITION));

        return Arrays.asList(
            () -> Ignition.localIgnite().<Integer, Integer>cache(CACHE_NAME)
                .invoke(key, new TestEntryProcessor(endpoints())),

            () -> Ignition.localIgnite().<Integer, Integer>cache(CACHE_NAME)
                .invokeAll(Collections.singleton(key), new TestEntryProcessor(endpoints())),

            () -> Ignition.localIgnite().<Integer, Integer>cache(CACHE_NAME)
                .invokeAsync(key, new TestEntryProcessor(endpoints()))
                .get(),

            () -> Ignition.localIgnite().<Integer, Integer>cache(CACHE_NAME)
                .invokeAllAsync(Collections.singleton(key), new TestEntryProcessor(endpoints()))
                .get()
        );
    }

    /**
     * @return Collection of endpont nodes ids.
     */
    private Collection<UUID> endpoints() {
        return Arrays.asList(
            nodeId(SRV_ENDPOINT),
            nodeId(CLNT_ENDPOINT)
        );
    }

    /**
     * Entry processor for tests with transition invoke call.
     */
    static class TestEntryProcessor implements EntryProcessor<Integer, Integer, Object> {
        /** Collection of endpont nodes ids. */
        private final Collection<UUID> endpoints;

        /**
         * @param endpoints Collection of endpont nodes ids.
         */
        public TestEntryProcessor(Collection<UUID> endpoints) {
            assert !endpoints.isEmpty();

            this.endpoints = endpoints;
        }

        /** {@inheritDoc} */
        @Override public Object process(MutableEntry<Integer, Integer> entry, Object... objects)
            throws EntryProcessorException {
            register();

            compute(Ignition.localIgnite(), endpoints).broadcast(new IgniteRunnable() {
                @Override public void run() {
                    register();
                }
            });

            return null;
        }
    }
}
