/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2018 Appsicle
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

package com.questdb.net.ha.protocol;

import com.questdb.net.ha.AbstractObjectConsumer;
import com.questdb.net.ha.model.Command;

import java.nio.ByteBuffer;

public class CommandConsumer extends AbstractObjectConsumer {

    private byte command;

    public final byte getCommand() {
        return command;
    }

    @Override
    protected final void commit() {
        ByteBuffer valueBuffer = getValueBuffer();
        valueBuffer.flip();
        char authKey = valueBuffer.getChar();
        byte cmd = valueBuffer.get();

        if (authKey != Command.AUTHENTICITY_KEY) {
            this.command = Command.UNAUTHENTIC;
        } else {
            this.command = cmd;
        }
    }
}
