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

package org.apache.ignite.internal.processor.security;

import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.Ignition;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.testframework.GridTestUtils;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertThat;

/**
 * Security tests for a compute task.
 */
public class ComputeTaskTest extends AbstractContextResolverSecurityProcessorTest {
    /** Values. */
    private AtomicInteger values = new AtomicInteger(0);
    //todo MY_TODO нужно тестировать все вызовы, т.е. все методы сервиса.
    /** */
    public void testCompute() {
        successCompute(succsessClnt, failClnt);
        successCompute(succsessClnt, failSrv);
        successCompute(succsessSrv, failClnt);
        successCompute(succsessSrv, failSrv);
        successCompute(succsessSrv, succsessSrv);
        successCompute(succsessClnt, succsessClnt);

        failCompute(failClnt, succsessSrv);
        failCompute(failClnt, succsessClnt);
        failCompute(failSrv, succsessSrv);
        failCompute(failSrv, succsessClnt);
        failCompute(failSrv, failSrv);
        failCompute(failClnt, failClnt);
    }

    /**
     * @param initiator Initiator node.
     * @param remote Remote node.
     */
    private void successCompute(IgniteEx initiator, IgniteEx remote) {
        int val = values.getAndIncrement();

        initiator.compute(initiator.cluster().forNode(remote.localNode()))
            .broadcast(() ->
                Ignition.localIgnite().cache(CACHE_NAME)
                    .put("key", val)
            );

        assertThat(remote.cache(CACHE_NAME).get("key"), is(val));
    }

    /**
     * @param initiator Initiator node.
     * @param remote Remote node.
     */
    private void failCompute(IgniteEx initiator, IgniteEx remote) {
        assertCauseMessage(
            GridTestUtils.assertThrowsWithCause(
                () -> initiator.compute(initiator.cluster().forNode(remote.localNode()))
                    .broadcast(() ->
                        Ignition.localIgnite().cache(CACHE_NAME).put("fail_key", -1)
                    )
                , SecurityException.class
            )
        );

        assertThat(remote.cache(CACHE_NAME).get("fail_key"), nullValue());
    }
}