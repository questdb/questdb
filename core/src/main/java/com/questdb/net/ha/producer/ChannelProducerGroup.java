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

package com.questdb.net.ha.producer;

import com.questdb.ex.JournalNetworkException;
import com.questdb.net.ha.ChannelProducer;
import com.questdb.std.ObjList;
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

