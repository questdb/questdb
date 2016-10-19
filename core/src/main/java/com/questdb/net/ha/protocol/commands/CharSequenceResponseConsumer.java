/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2016 Appsicle
 *
 * This program is free software: you can redistribute it and/or  modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 ******************************************************************************/

package com.questdb.net.ha.protocol.commands;

import com.questdb.misc.ByteBuffers;
import com.questdb.net.ha.AbstractImmutableObjectConsumer;
import com.questdb.std.str.DirectCharSequence;

import java.nio.ByteBuffer;

public class CharSequenceResponseConsumer extends AbstractImmutableObjectConsumer<CharSequence> {
    private final DirectCharSequence charSequence = new DirectCharSequence();

    @Override
    protected CharSequence read(ByteBuffer buffer) {
        long address = ByteBuffers.getAddress(buffer);
        charSequence.of(address, address + buffer.remaining());
        return charSequence;
    }
}
