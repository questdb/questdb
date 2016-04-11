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

package com.nfsdb.io.sink;

import com.nfsdb.ex.JournalRuntimeException;
import com.nfsdb.misc.ByteBuffers;
import com.nfsdb.misc.Unsafe;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

@SuppressFBWarnings({"EXS_EXCEPTION_SOFTENING_NO_CONSTRAINTS"})
public class FileSink extends AbstractCharSink implements Closeable {

    private final RandomAccessFile raf;
    private final FileChannel channel;
    private ByteBuffer buffer;
    private long pos;
    private long addr;
    private long limit;

    public FileSink(File file) throws IOException {
        this.raf = new RandomAccessFile(file, "rw");
        this.channel = raf.getChannel();
        this.pos = 0L;
        this.addr = this.limit = 0;
    }

    @Override
    public void close() throws IOException {
        ByteBuffers.release(buffer);
        if (addr > 0 && pos > 0) {
            channel.truncate(pos - (limit - addr));
        }
        channel.close();
        raf.close();
    }

    @Override
    public void flush() {

    }

    @Override
    public CharSink put(CharSequence cs) {
        if (cs == null) {
            return this;
        }

        for (int i = 0, l = cs.length(); i < l; i++) {
            if (addr == limit) {
                map();
            }
            Unsafe.getUnsafe().putByte(addr++, (byte) cs.charAt(i));
        }
        return this;
    }

    @Override
    public CharSink put(char c) {
        if (addr == limit) {
            map();
        }
        Unsafe.getUnsafe().putByte(addr++, (byte) c);
        return this;
    }

    private void map() {
        if (buffer != null) {
            ByteBuffers.release(buffer);
        }
        try {
            this.buffer = channel.map(FileChannel.MapMode.READ_WRITE, this.pos, ByteBuffers.getMaxMappedBufferSize(Long.MAX_VALUE));
            pos += buffer.limit();
            addr = ByteBuffers.getAddress(buffer);
            limit = addr + buffer.remaining();
        } catch (IOException e) {
            throw new JournalRuntimeException(e);
        }
    }
}
