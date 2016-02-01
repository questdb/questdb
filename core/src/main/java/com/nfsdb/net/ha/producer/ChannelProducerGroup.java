/*******************************************************************************
 *  _  _ ___ ___     _ _
 * | \| | __/ __| __| | |__
 * | .` | _|\__ \/ _` | '_ \
 * |_|\_|_| |___/\__,_|_.__/
 *
 * Copyright (c) 2014-2016. The NFSdb project and its contributors.
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
 ******************************************************************************/

package com.nfsdb.net.ha.producer;

import com.nfsdb.ex.JournalNetworkException;
import com.nfsdb.net.ha.ChannelProducer;
import com.nfsdb.std.ObjList;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.nio.channels.WritableByteChannel;

@SuppressFBWarnings({"LII_LIST_INDEXED_ITERATING"})
public class ChannelProducerGroup<T extends ChannelProducer> implements ChannelProducer {

    private final ObjList<T> producers = new ObjList<>();
    private boolean hasContent = false;

    @Override
    public void free() {
        for (int i = 0, sz = producers.size(); i < sz; i++) {
            producers.getQuick(i).free();
        }
    }

    @Override
    public boolean hasContent() {
        return hasContent;
    }

    @Override
    public void write(WritableByteChannel channel) throws JournalNetworkException {
        if (hasContent) {
            for (int i = 0, sz = producers.size(); i < sz; i++) {
                producers.getQuick(i).write(channel);
            }
            hasContent = false;
        }
    }

    @Override
    public String toString() {
        return "ChannelProducerGroup{" +
                "size=" + producers.size() +
                '}';
    }

    void addProducer(T producer) {
        this.producers.add(producer);
    }

    void computeHasContent() {
        for (int i = 0, sz = producers.size(); i < sz; i++) {
            if (this.hasContent = producers.getQuick(i).hasContent()) {
                break;
            }
        }
    }

    ObjList<T> getProducers() {
        return producers;
    }
}

