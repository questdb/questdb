/*******************************************************************************
 *   _  _ ___ ___     _ _
 *  | \| | __/ __| __| | |__
 *  | .` | _|\__ \/ _` | '_ \
 *  |_|\_|_| |___/\__,_|_.__/
 *
 *  Copyright (c) 2014-2015. The NFSdb project and its contributors.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 ******************************************************************************/
package com.nfsdb.ha;

import java.nio.ByteBuffer;

public abstract class AbstractMutableObjectConsumer<T> extends AbstractObjectConsumer {

    private T value;

    public final T getValue() {
        return value;
    }

    @Override
    protected final void commit() {
        ByteBuffer valueBuffer = getValueBuffer();

        valueBuffer.flip();
        if (value == null) {
            value = newInstance();
        }
        read(valueBuffer, value);
    }

    abstract protected T newInstance();

    abstract protected void read(ByteBuffer buffer, T obj);
}
