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

package org.apache.ignite.internal.processor.security.cache;

import java.util.Arrays;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.stream.Stream;
import org.apache.ignite.Ignite;
import org.apache.ignite.cache.CacheEntryProcessor;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processor.security.AbstractCacheOperationPermissionCheckTest;
import org.apache.ignite.internal.util.typedef.T2;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import static java.util.Collections.singleton;
import static org.apache.ignite.plugin.security.SecurityPermission.CACHE_PUT;
import static org.apache.ignite.plugin.security.SecurityPermission.CACHE_READ;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsNull.nullValue;
import static org.junit.Assert.assertThat;

/**
 * Test cache permission for Entry processor.
 */
@RunWith(JUnit4.class)
public class EntryProcessorPermissionCheckTest extends AbstractCacheOperationPermissionCheckTest {
    /**
     *
     */
    @Test
    public void test() throws Exception {
        IgniteEx srvNode = startGrid("server_node",
            builder()
                .appendCachePermissions(CACHE_NAME, CACHE_READ, CACHE_PUT)
                .appendCachePermissions(FORBIDDEN_CACHE, EMPTY_PERMS).build());

        IgniteEx clientNode = startGrid("client_node",
            builder()
                .appendCachePermissions(CACHE_NAME, CACHE_PUT, CACHE_READ)
                .appendCachePermissions(FORBIDDEN_CACHE, EMPTY_PERMS).build(), true);

        srvNode.cluster().active(true);

        Stream.of(srvNode, clientNode)
            .forEach(
                (n) -> consumers(n).forEach(
                    (c) -> {
                        assertAllowed(n, c);

                        assertForbidden(n, c);
                    }
                )
            );
    }

    /**
     * @return Collection of consumers to invoke entry processor.
     */
    private List<BiConsumer<String, T2<String, Integer>>> consumers(final Ignite node) {
        return Arrays.asList(
            (cacheName, t) -> node.cache(cacheName).invoke(
                t.getKey(), processor(t)
            ),
            (cacheName, t) -> node.cache(cacheName).invokeAll(
                singleton(t.getKey()), processor(t)
            ),
            (cacheName, t) -> node.cache(cacheName).invokeAsync(
                t.getKey(), processor(t)
            ).get(),
            (cacheName, t) -> node.cache(cacheName).invokeAllAsync(
                singleton(t.getKey()), processor(t)
            ).get()
        );
    }

    /**
     * @param t T2.
     */
    private static CacheEntryProcessor<Object, Object, Object> processor(T2<String, Integer> t) {
        return (entry, o) -> {
            entry.setValue(t.getValue());

            return null;
        };
    }

    /**
     * @param c Consumer.
     */
    private void assertAllowed(Ignite node, BiConsumer<String, T2<String, Integer>> c) {
        T2<String, Integer> entry = entry();

        c.accept(CACHE_NAME, entry);

        assertThat(node.cache(CACHE_NAME).get(entry.getKey()), is(entry.getValue()));
    }

    /**
     * @param c Consumer.
     */
    private void assertForbidden(Ignite node, BiConsumer<String, T2<String, Integer>> c) {
        T2<String, Integer> entry = entry();

        assertForbidden(() -> c.accept(FORBIDDEN_CACHE, entry));

        assertThat(node.cache(CACHE_NAME).get(entry.getKey()), nullValue());
    }
}
