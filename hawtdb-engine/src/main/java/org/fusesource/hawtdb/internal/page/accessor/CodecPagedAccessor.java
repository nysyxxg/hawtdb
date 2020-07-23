/**
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
package org.fusesource.hawtdb.internal.page.accessor;

import org.fusesource.hawtbuf.codec.Codec;
import org.fusesource.hawtdb.internal.page.Paged;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/**
 * 使用封送器对值进行编码/解码的编码器。
 * A EncoderDecoder which uses a Marshaller to encode/decode the values.
 */
public class CodecPagedAccessor<T> extends AbstractStreamPagedAccessor<T> {

    private final Codec<T> codec;

    public CodecPagedAccessor(Codec<T> codec) {
        this.codec = codec;
    }

    @Override
    protected void encode(Paged paged, DataOutputStream os, T data) throws IOException {
        codec.encode(data, os);
    }

    @Override
    protected T decode(Paged paged, DataInputStream is) throws IOException {
        return codec.decode(is);
    }
}
