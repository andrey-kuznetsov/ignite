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
import java.util.UUID;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processor.security.AbstractResolveSecurityTest;

/**
 * Abstract compute security test.
 */
public abstract class AbstractComputeResolveSecurityTest extends AbstractResolveSecurityTest {
    /** Client node. */
    protected IgniteEx clntTransition;

    /** Client node. */
    protected IgniteEx clntEndpoint;

    /** {@inheritDoc} */
    @Override protected void startNodes() throws Exception{
        super.startNodes();

        clntTransition = startGrid("clnt_transition", allowAllPermissionSet(), true);

        clntEndpoint = startGrid("clnt_endpoint", allowAllPermissionSet());
    }

    /**
     * Sets up VERIFIER, performs the runnable and checks the result.
     *
     * @param node Node.
     * @param r Runnable.
     */
    protected void perform(IgniteEx node, Runnable r) {
        VERIFIER.start(secSubjectId(node))
            .add(srvTransition.name(), 1)
            .add(clntTransition.name(), 1)
            .add(srvEndpoint.name(), 2)
            .add(clntEndpoint.name(), 2);

        r.run();

        VERIFIER.checkResult();
    }

    /**
     * @return Collection of transition node ids.
     */
    protected Collection<UUID> transitions() {
        return Arrays.asList(
            srvTransition.localNode().id(),
            clntTransition.localNode().id()
        );
    }

    /**
     * @return Collection of endpont nodes ids.
     */
    protected Collection<UUID> endpoints() {
        return Arrays.asList(
            srvEndpoint.localNode().id(),
            clntEndpoint.localNode().id()
        );
    }
}