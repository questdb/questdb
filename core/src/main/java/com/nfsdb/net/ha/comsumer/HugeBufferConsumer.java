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

package com.nfsdb.net.ha.comsumer;

import com.nfsdb.JournalMode;
import com.nfsdb.ex.JournalException;
import com.nfsdb.ex.JournalNetworkException;
import com.nfsdb.factory.configuration.Constants;
import com.nfsdb.misc.ByteBuffers;
import com.nfsdb.misc.Unsafe;
import com.nfsdb.net.ha.ChannelConsumer;
import com.nfsdb.store.UnstructuredFile;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.ReadableByteChannel;

public class HugeBufferConsumer implements ChannelConsumer, Closeable {
    private final ByteBuffer header = ByteBuffer.allocateDirect(8).order(ByteOrder.LITTLE_ENDIAN);
    private final long headerAddress = ByteBuffers.getAddress(header);
    private final UnstructuredFile hb;

    public HugeBufferConsumer(File file) throws JournalException {
        hb = new UnstructuredFile(file, Constants.HB_HINT, JournalMode.APPEND);
    }

    @Override
    public void close() {
        free();
    }

    public void free() {
        hb.close();
    }

    @Override
    public void read(ReadableByteChannel channel) throws JournalNetworkException {
        try {
            header.position(0);
            channel.read(header);
            long target = Unsafe.getUnsafe().getLong(headerAddress);

            long pos = 0;
            while (pos < target) {
                pos += ByteBuffers.copy(channel, hb.getBuffer(pos, 1), target - pos);
            }
        } catch (IOException e) {
            throw new JournalNetworkException(e);
        }
    }

    public UnstructuredFile getHb() {
        return hb;
    }
}
