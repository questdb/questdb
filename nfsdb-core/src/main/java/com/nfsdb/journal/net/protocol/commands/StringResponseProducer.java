/*
 * Copyright (c) 2014. Vlad Ilyushchenko
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.nfsdb.journal.net.protocol.commands;

import com.nfsdb.journal.net.AbstractObjectProducer;

import java.nio.ByteBuffer;

public class StringResponseProducer extends AbstractObjectProducer<String> {
    @Override
    protected int getBufferSize(String value) {
        return value.length() * 2;
    }

    @Override
    protected void write(String value, ByteBuffer buffer) {
        for (int i = 0; i < value.length(); i++) {
            buffer.putChar(value.charAt(i));
        }
    }
}
