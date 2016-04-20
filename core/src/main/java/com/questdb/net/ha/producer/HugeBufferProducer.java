/*******************************************************************************
 * ___                  _   ____  ____
 * / _ \ _   _  ___  ___| |_|  _ \| __ )
 * | | | | | | |/ _ \/ __| __| | | |  _ \
 * | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 * \__\_\\__,_|\___||___/\__|____/|____/
 * <p>
 * Copyright (C) 2014-2016 Appsicle
 * <p>
 * This program is free software: you can redistribute it and/or  modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 * <p>
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 * <p>
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 * <p>
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
 ******************************************************************************/

package com.questdb.net.ha.producer;

import com.questdb.JournalMode;
import com.questdb.ex.JournalException;
import com.questdb.ex.JournalNetworkException;
import com.questdb.factory.configuration.Constants;
import com.questdb.misc.ByteBuffers;
import com.questdb.misc.Unsafe;
import com.questdb.net.ha.ChannelProducer;
import com.questdb.store.UnstructuredFile;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;

public class HugeBufferProducer implements ChannelProducer, Closeable {
    private final ByteBuffer header = ByteBuffer.allocateDirect(8);
    private final long headerAddress = ByteBuffers.getAddress(header);
    private final UnstructuredFile hb;

    public HugeBufferProducer(File file) throws JournalException {
        hb = new UnstructuredFile(file, Constants.HB_HINT, JournalMode.READ);
    }

    @Override
    public void close() {
        free();
    }

    @Override
    public void free() {
        ByteBuffers.release(header);
        hb.close();
    }

    @Override
    public boolean hasContent() {
        return true;
    }

    @Override
    public void write(WritableByteChannel channel) throws JournalNetworkException {
        long target = hb.getAppendOffset();
        Unsafe.getUnsafe().putLong(headerAddress, target);
        header.position(0);
        try {
            channel.write(header);
            long pos = 0;
            while (pos < target) {
                pos += ByteBuffers.copy(hb.getBuffer(pos, 1), channel, target - pos);
            }
        } catch (IOException e) {
            throw new JournalNetworkException(e);
        }
    }
}
