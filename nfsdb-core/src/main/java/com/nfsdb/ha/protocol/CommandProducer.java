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

package com.nfsdb.ha.protocol;

import com.nfsdb.ha.AbstractObjectProducer;
import com.nfsdb.ha.model.Command;

import java.nio.ByteBuffer;

public class CommandProducer extends AbstractObjectProducer<Command> {
    @Override
    protected int getBufferSize(Command value) {
        return Command.BUFFER_SIZE;
    }

    @Override
    protected void write(Command value, ByteBuffer buffer) {
        buffer.putChar(Command.AUTHENTICITY_KEY);
        buffer.put(value.getCmd());
    }
}
