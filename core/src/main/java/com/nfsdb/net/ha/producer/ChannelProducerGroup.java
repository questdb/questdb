/*******************************************************************************
 *  _  _ ___ ___     _ _
 * | \| | __/ __| __| | |__
 * | .` | _|\__ \/ _` | '_ \
 * |_|\_|_| |___/\__,_|_.__/
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
 * As a special exception, the copyright holders give permission to link the
 * code of portions of this program with the OpenSSL library under certain
 * conditions as described in each individual source file and distribute
 * linked combinations including the program with the OpenSSL library. You
 * must comply with the GNU Affero General Public License in all respects for
 * all of the code used other than as permitted herein. If you modify file(s)
 * with this exception, you may extend this exception to your version of the
 * file(s), but you are not obligated to do so. If you do not wish to do so,
 * delete this exception statement from your version. If you delete this
 * exception statement from all source files in the program, then also delete
 * it in the license file.
 *
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

