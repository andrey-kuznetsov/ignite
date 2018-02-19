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

package org.apache.ignite.internal.util.lang.gridfunc;

import java.util.Deque;
import java.util.concurrent.ConcurrentLinkedDeque;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.lang.IgniteCallable;
import org.apache.ignite.util.deque.LongSizeCountingDeque;

/**
 * Deque factory.
 */
public class ConcurrentDequeFactoryCallable implements IgniteCallable<Deque> {
    /** */
    private static final long serialVersionUID = 0L;

    /** {@inheritDoc} */
    @Override public Deque call() {
        return new LongSizeCountingDeque<>(new ConcurrentLinkedDeque<>());
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(ConcurrentDequeFactoryCallable.class, this);
    }
}
