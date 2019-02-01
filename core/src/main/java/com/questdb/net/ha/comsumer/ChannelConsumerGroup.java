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

package com.questdb.net.ha.comsumer;

import com.questdb.net.ha.AbstractChannelConsumer;
import com.questdb.net.ha.ChannelConsumer;
import com.questdb.std.ex.JournalNetworkException;

import java.nio.channels.ReadableByteChannel;

public class ChannelConsumerGroup extends AbstractChannelConsumer {

    private final ChannelConsumer[] consumers;

    ChannelConsumerGroup(ChannelConsumer... consumers) {
        this.consumers = consumers;
    }

    @Override
    public void free() {
        for (int i = 0; i < consumers.length; i++) {
            consumers[i].free();
        }
    }

    @Override
    protected void doRead(ReadableByteChannel channel) throws JournalNetworkException {
        for (int i = 0; i < consumers.length; i++) {
            consumers[i].read(channel);
        }
    }
}
