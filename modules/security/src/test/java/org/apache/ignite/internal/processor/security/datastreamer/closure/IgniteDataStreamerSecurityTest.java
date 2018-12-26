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

package org.apache.ignite.internal.processor.security.datastreamer.closure;

import java.util.Map;
import java.util.UUID;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.Ignition;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processor.security.AbstractCacheSecurityTest;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.lang.IgniteBiInClosure;
import org.apache.ignite.stream.StreamVisitor;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Testing permissions when the closure od DataStreamer is executed cache operations on remote node.
 */
@RunWith(JUnit4.class)
public class IgniteDataStreamerSecurityTest extends AbstractCacheSecurityTest {
    /**
     *
     */
    @Test
    public void testDataStreamer() {
        assertAllowed((t) -> load(clntAllPerms, srvAllPerms, t));
        assertAllowed((t) -> load(clntAllPerms, srvReadOnlyPerm, t));
        assertAllowed((t) -> load(srvAllPerms, srvAllPerms, t));
        assertAllowed((t) -> load(srvAllPerms, srvReadOnlyPerm, t));

        assertForbidden((t) -> load(clntReadOnlyPerm, srvAllPerms, t));
        assertForbidden((t) -> load(srvReadOnlyPerm, srvAllPerms, t));
        assertForbidden((t) -> load(srvReadOnlyPerm, srvReadOnlyPerm, t));
    }

    /**
     * @param initiator Initiator node.
     * @param remote Remoute node.
     */
    private void load(IgniteEx initiator, IgniteEx remote, T2<String, Integer> entry) {
        try (IgniteDataStreamer<Integer, Integer> strm = initiator.dataStreamer(COMMON_USE_CACHE)) {
            strm.receiver(
                StreamVisitor.from(
                    new TestClosure(remote.localNode().id(), entry.getKey(), entry.getValue())
                ));

            strm.addData(primaryKey(remote), 100);
        }
    }

    /**
     * Closure for tests.
     */
    static class TestClosure implements
        IgniteBiInClosure<IgniteCache<Integer, Integer>, Map.Entry<Integer, Integer>> {
        /** Remote node id. */
        private final UUID remoteId;

        /** Key. */
        private final String key;

        /** Value. */
        private final Integer val;

        /**
         * @param remoteId Remote node id.
         * @param key Key.
         * @param val Value.
         */
        public TestClosure(UUID remoteId, String key, Integer val) {
            this.remoteId = remoteId;
            this.key = key;
            this.val = val;
        }

        /** {@inheritDoc} */
        @Override public void apply(IgniteCache<Integer, Integer> entries,
            Map.Entry<Integer, Integer> entry) {
            Ignite loc = Ignition.localIgnite();

            if (remoteId.equals(loc.cluster().localNode().id()))
                loc.cache(CACHE_NAME).put(key, val);
        }
    }
}
