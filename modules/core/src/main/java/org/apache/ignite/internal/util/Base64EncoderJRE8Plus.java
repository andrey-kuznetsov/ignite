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

package org.apache.ignite.internal.util;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;

/**
 * Implementation of {@link Base64Encoder} for Java 8 and above.
 */
public class Base64EncoderJRE8Plus implements Base64Encoder {
    /** {@inheritDoc} */
    @Override public String encode(byte[] msg) {
        try {
            Object encoder = Class.forName("java.util.Base64").getDeclaredMethod("getEncoder").invoke(null);

            Method encodeMtd = Class.forName("java.util.Base64$Encoder").getDeclaredMethod("encode", byte[].class);

            return new String((byte[])encodeMtd.invoke(encoder, (Object)msg), StandardCharsets.ISO_8859_1);
        }
        catch (ClassNotFoundException | IllegalAccessException | NoSuchMethodException | InvocationTargetException e) {
            throw new RuntimeException("Failed to encode Base64 message", e);
        }
    }
}
