/*******************************************************************************
 *  _  _ ___ ___     _ _
 * | \| | __/ __| __| | |__
 * | .` | _|\__ \/ _` | '_ \
 * |_|\_|_| |___/\__,_|_.__/
 *
 * Copyright (c) 2014-2015. The NFSdb project and its contributors.
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

package com.nfsdb.io.sink;

import com.nfsdb.exceptions.JournalRuntimeException;
import com.nfsdb.utils.ByteBuffers;
import com.nfsdb.utils.Unsafe;
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
