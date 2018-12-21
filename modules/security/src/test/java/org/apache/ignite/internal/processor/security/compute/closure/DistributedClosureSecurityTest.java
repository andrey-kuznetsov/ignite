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

package org.apache.ignite.internal.processor.security.compute.closure;

import java.util.Arrays;
import java.util.Collection;
import org.apache.ignite.IgniteCompute;
import org.apache.ignite.Ignition;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.lang.IgniteClosure;
import org.apache.ignite.plugin.security.SecurityException;
import org.apache.ignite.testframework.GridTestUtils;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertThat;

/**
 * Testing permissions when the compute closure is executed cache operations on remote node.
 */
public class DistributedClosureSecurityTest extends AbstractComputeTaskSecurityTest {
    /** {@inheritDoc} */
    @Override protected void checkSuccess(IgniteEx initiator, IgniteEx remote) {
        for(TriConsumer<IgniteCompute, String, Integer> c : consumers())
            successClosure(initiator, remote, c);
    }

    /** {@inheritDoc} */
    @Override protected void checkFail(IgniteEx initiator, IgniteEx remote) {
        for(TriConsumer<IgniteCompute, String, Integer> c : consumers())
            failClosure(initiator, remote, c);
    }

    /**
     * @return Collection of TriConsumers that invoke IgniteCompute methods.
     */
    private Collection<TriConsumer<IgniteCompute, String, Integer>> consumers() {
        return Arrays.asList(
            (cmp, k, v) -> cmp.broadcast(
                () -> Ignition.localIgnite().cache(CACHE_NAME).put(k, v)
            ),

            (cmp, k, v) -> cmp.broadcastAsync(
                () -> Ignition.localIgnite().cache(CACHE_NAME).put(k, v)
            ).get(),

            (cmp, k, v) -> cmp.call(
                () -> {
                    Ignition.localIgnite().cache(CACHE_NAME).put(k, v);

                    return null;
                }
            ),

            (cmp, k, v) -> cmp.callAsync(
                () -> {
                    Ignition.localIgnite().cache(CACHE_NAME).put(k, v);

                    return null;
                }
            ).get(),

            (cmp, k, v) -> cmp.run(
                () -> Ignition.localIgnite().cache(CACHE_NAME).put(k, v)
            ),

            (cmp, k, v) -> cmp.runAsync(
                () -> Ignition.localIgnite().cache(CACHE_NAME).put(k, v)
            ).get(),

            (cmp, k, v) -> cmp.apply(
                new IgniteClosure<Object, Object>() {
                    @Override public Object apply(Object o) {
                        Ignition.localIgnite().cache(CACHE_NAME).put(k, v);

                        return null;
                    }
                }, new Object()
            ),

            (cmp, k, v) -> cmp.applyAsync(
                new IgniteClosure<Object, Object>() {
                    @Override public Object apply(Object o) {
                        Ignition.localIgnite().cache(CACHE_NAME).put(k, v);

                        return null;
                    }
                }, new Object()
            ).get()
        );
    }

    /**
     * @param initiator Initiator node.
     * @param remote Remote node.
     * @param consumer Consumer.
     */
    private void successClosure(IgniteEx initiator, IgniteEx remote,
        TriConsumer<IgniteCompute, String, Integer> consumer) {
        int val = values.getAndIncrement();

        consumer.accept(initiator.compute(initiator.cluster().forNode(remote.localNode())), "key", val);

        assertThat(remote.cache(CACHE_NAME).get("key"), is(val));
    }

    /**
     * @param initiator Initiator node.
     * @param remote Remote node.
     * @param consumer Consumer.
     */
    private void failClosure(IgniteEx initiator, IgniteEx remote,
        TriConsumer<IgniteCompute, String, Integer> consumer) {
        assertCauseSecurityException(
            GridTestUtils.assertThrowsWithCause(
                () ->
                    consumer.accept(
                        initiator.compute(initiator.cluster().forNode(remote.localNode())), "fail_key", -1
                    ), SecurityException.class
            )
        );

        assertThat(remote.cache(CACHE_NAME).get("fail_key"), nullValue());
    }
}
