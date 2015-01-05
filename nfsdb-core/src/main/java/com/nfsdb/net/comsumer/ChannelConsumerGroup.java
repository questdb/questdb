/*
 * Copyright (c) 2014-2015. Vlad Ilyushchenko
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

package com.nfsdb.net.comsumer;

import com.nfsdb.exceptions.JournalNetworkException;
import com.nfsdb.net.AbstractChannelConsumer;
import com.nfsdb.net.ChannelConsumer;

import java.nio.channels.ReadableByteChannel;

public class ChannelConsumerGroup extends AbstractChannelConsumer {

    private final ChannelConsumer consumers[];

    ChannelConsumerGroup(ChannelConsumer... consumers) {
        this.consumers = consumers;
    }

    @Override
    public boolean isComplete() {
        // consumers are filled from first to last. So incomplete status
        // is better visible from tail of sink array.
        for (int i = consumers.length - 1; i >= 0; i--) {
            if (!consumers[i].isComplete()) {
                return false;
            }
        }
        return true;
    }

    @Override
    public void reset() {
        super.reset();
        for (int i = 0; i < consumers.length; i++) {
            consumers[i].reset();
        }
    }

    @Override
    protected void doRead(ReadableByteChannel channel) throws JournalNetworkException {
        for (int i = 0; i < consumers.length; i++) {
            consumers[i].read(channel);
        }
    }
}
