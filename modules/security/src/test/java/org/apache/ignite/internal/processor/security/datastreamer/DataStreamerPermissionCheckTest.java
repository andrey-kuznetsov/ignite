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

package org.apache.ignite.internal.processor.security.datastreamer;

import java.util.Collection;
import java.util.Map;
import java.util.function.Consumer;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processor.security.AbstractCacheOperationPermissionCheckTest;
import org.apache.ignite.internal.processor.security.TestSecurityContext;
import org.apache.ignite.internal.processor.security.TestSecuritySubject;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.plugin.security.SecurityBasicPermissionSet;
import org.apache.ignite.plugin.security.SecurityException;
import org.apache.ignite.plugin.security.SecurityPermission;
import org.apache.ignite.plugin.security.SecurityPermissionSet;
import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.junit.Assert.assertThat;

/**
 * Test cache permissions for Data Streamer.
 */
@RunWith(JUnit4.class)
public class DataStreamerPermissionCheckTest extends AbstractCacheOperationPermissionCheckTest {
    /**
     * @throws Exception If failed.
     */
    @Test
    public void testDataStreamerCreationWithPutPermission() throws Exception {
        startGrid(loginPrefix(false) + "_test_node", getPermissionSet()).dataStreamer(CACHE_NAME).close();
    }

    /**
     * @throws Exception If failed.
     */
    @Test(expected = SecurityException.class)
    public void testDataStreamerCreationWithoutPutPermission() throws Exception {
        startGrid(loginPrefix(false) + "_test_node", getPermissionSet()).dataStreamer(FORBIDDEN_CACHE);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testServerNode() throws Exception {
        testDataStreamer(startGrid(loginPrefix(false) + "_test_node", getPermissionSet(), false));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testClientNode() throws Exception {
        startGrid(loginPrefix(false) + "_test_node", getPermissionSet(), false);

        testDataStreamer(startGrid(loginPrefix(true) + "_test_node", getPermissionSet(), true));
    }

    /**
     *
     */
    @After
    public void after() {
        stopAllGrids();
    }

    /**
     *
     */
    private SecurityPermissionSet getPermissionSet() {
        return builder()
            .appendCachePermissions(CACHE_NAME, SecurityPermission.CACHE_PUT, SecurityPermission.CACHE_REMOVE)
            .appendCachePermissions(FORBIDDEN_CACHE, SecurityPermission.CACHE_READ)
            .build();
    }

    /**
     * @param node Ignite node to run test on.
     */
    private void testDataStreamer(Ignite node) {
        performOnAllowedCache(node, s -> s.addData("k", 1));
        performOnForbiddenCache(node, s -> s.addData("k", 1));

        performOnAllowedCache(node, s -> s.addData(singletonMap("key", 2)));
        performOnForbiddenCache(node, s -> s.addData(singletonMap("key", 2)));

        Map.Entry<String, Integer> entry = entry();

        performOnAllowedCache(node, s -> s.addData(entry));
        performOnForbiddenCache(node, s -> s.addData(entry));

        performOnAllowedCache(node, s -> s.addData(singletonList(entry())));
        performOnForbiddenCache(node, s -> s.addData(singletonList(entry())));

        performOnAllowedCache(node, s -> s.removeData("k"));
        performOnForbiddenCache(node, s -> s.removeData("k"));
    }

    /**
     * @param node Ignite node to run test on.
     * @param streamerAction action to perform with streamer.
     */
    private void performOnAllowedCache(Ignite node, Consumer<IgniteDataStreamer<String, Integer>> streamerAction) {
        try (IgniteDataStreamer<String, Integer> s = node.dataStreamer(CACHE_NAME)) {
            streamerAction.accept(s);
        }
    }

    /**
     * @param node Ignite node to run test on.
     * @param streamerAction action to perform with streamer.
     */
    private void performOnForbiddenCache(Ignite node, Consumer<IgniteDataStreamer<String, Integer>> streamerAction) {
        IgniteEx nodeEx = (IgniteEx)node;

        TestSecurityContext secCtx = (TestSecurityContext)nodeEx.context().security().securityContext();

        TestSecuritySubject secSubj = (TestSecuritySubject)secCtx.subject();

        SecurityBasicPermissionSet permSet = (SecurityBasicPermissionSet)secSubj.permissions();

        Collection<SecurityPermission> cachePerms = permSet.cachePermissions().get(FORBIDDEN_CACHE);

        assertFalse(cachePerms.contains(SecurityPermission.CACHE_PUT));
        assertFalse(cachePerms.contains(SecurityPermission.CACHE_REMOVE));

        cachePerms.add(SecurityPermission.CACHE_PUT);
        cachePerms.add(SecurityPermission.CACHE_REMOVE);

        try (IgniteDataStreamer<String, Integer> s = node.dataStreamer(FORBIDDEN_CACHE)) {
            cachePerms.remove(SecurityPermission.CACHE_PUT);
            cachePerms.remove(SecurityPermission.CACHE_REMOVE);

            try {
                streamerAction.accept(s);

                fail("Should not happen");
            }
            catch (Throwable e) {
                assertThat(X.cause(e, SecurityException.class), notNullValue());
            }
        }
    }
}
