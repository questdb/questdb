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

package com.nfsdb.ha.producer;

import com.nfsdb.exceptions.JournalNetworkException;
import com.nfsdb.ha.ChannelProducer;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.nio.channels.WritableByteChannel;
import java.util.ArrayList;
import java.util.List;

@SuppressFBWarnings({"LII_LIST_INDEXED_ITERATING"})
public class ChannelProducerGroup<T extends ChannelProducer> implements ChannelProducer {

    private final List<T> producers = new ArrayList<>();
    private boolean hasContent = false;

    @Override
    public void free() {
        for (int i = 0, sz = producers.size(); i < sz; i++) {
            producers.get(i).free();
        }
    }

    @Override
    public boolean hasContent() {
        return hasContent;
    }

    @Override
    public String toString() {
        return "ChannelProducerGroup{" +
                "size=" + producers.size() +
                '}';
    }

    @Override
    public void write(WritableByteChannel channel) throws JournalNetworkException {
        if (hasContent) {
            for (int i = 0, sz = producers.size(); i < sz; i++) {
                producers.get(i).write(channel);
            }
            hasContent = false;
        }
    }

    void addProducer(T producer) {
        this.producers.add(producer);
    }

    void computeHasContent() {
        for (int i = 0, sz = producers.size(); i < sz; i++) {
            if (this.hasContent = producers.get(i).hasContent()) {
                break;
            }
        }
    }

    protected List<T> getProducers() {
        return producers;
    }
}

