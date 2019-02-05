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

package org.apache.ignite.internal.processor.security.messaging;

import java.util.UUID;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.ignite.IgniteMessaging;
import org.apache.ignite.Ignition;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processor.security.AbstractResolveSecurityContextTest;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.testframework.junits.GridAbstractTest;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Testing permissions when the message listener is executed cache operations on remote node.
 */
@RunWith(JUnit4.class)
public class IgniteMessagingResolveSecurityContextTest extends AbstractResolveSecurityContextTest {
    /** Barrier. */
    private static final CyclicBarrier BARRIER = new CyclicBarrier(3);

    /** Client node. */
    protected IgniteEx clntTransition;

    /** Client node. */
    protected IgniteEx clntEndpoint;

    /** {@inheritDoc} */
    @Override protected void startNodes() throws Exception {
        super.startNodes();

        clntTransition = startGrid("clnt_transition", allowAllPermissionSet(), true);

        clntEndpoint = startGrid("clnt_endpoint", allowAllPermissionSet());
    }

    /**
     *
     */
    @Test
    public void test() {
        messaging(srvInitiator, srvTransition);
        messaging(srvInitiator, clntTransition);

        messaging(clntInitiator, srvTransition);
        messaging(clntInitiator, clntTransition);
    }

    /**
     * Performs the test.
     *
     * @param lsnrNode Node that registers a listener on a remote node.
     * @param evtNode Node that generates an event.
     */
    private void messaging(IgniteEx lsnrNode, IgniteEx evtNode) {
        VERIFIER.start(secSubjectId(lsnrNode))
            .add(srvEndpoint.name(), 1)
            .add(clntEndpoint.name(), 1);

        BARRIER.reset();

        IgniteMessaging messaging = lsnrNode.message(
            lsnrNode.cluster().forNode(srvEndpoint.localNode(), clntEndpoint.localNode())
        );

        String topic = "HOT_TOPIC";

        UUID lsnrId = messaging.remoteListen(topic,
            new IgniteBiPredicate<UUID, Object>() {
                @Override public boolean apply(UUID uuid, Object o) {
                    try {
                        VERIFIER.verify(Ignition.localIgnite());

                        return true;
                    }
                    finally {
                        barrierAwait();
                    }
                }
            }
        );

        try {
            evtNode.message().send(topic, "Fire!");
        }
        finally {
            barrierAwait();

            messaging.stopRemoteListen(lsnrId);
        }

        VERIFIER.checkResult();
    }

    /**
     * Call await method on {@link #BARRIER} with {@link GridAbstractTest#getTestTimeout()} timeout.
     */
    private void barrierAwait() {
        try {
            BARRIER.await(getTestTimeout(), TimeUnit.MILLISECONDS);
        }
        catch (InterruptedException | BrokenBarrierException | TimeoutException e) {
            fail(e.toString());
        }
    }
}
