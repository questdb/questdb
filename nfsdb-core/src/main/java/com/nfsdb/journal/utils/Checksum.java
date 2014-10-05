/*
 * Copyright (c) 2014. Vlad Ilyushchenko
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

package com.nfsdb.journal.utils;

import com.nfsdb.journal.exceptions.JournalRuntimeException;
import com.nfsdb.journal.factory.configuration.ColumnMetadata;
import com.nfsdb.journal.factory.configuration.JournalMetadata;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public final class Checksum {

    private static final ThreadLocal<MessageDigest> localMd = new ThreadLocal<>();
    private static final ThreadLocal<ByteBuffer> localBuf = new ThreadLocal<>();

    public static byte[] getChecksum(JournalMetadata<?> metadata) throws JournalRuntimeException {
        try {
            MessageDigest md = localMd.get();
            if (md == null) {
                md = MessageDigest.getInstance("SHA");
                localMd.set(md);
            }

            ByteBuffer buf = localBuf.get();
            if (buf == null) {
                buf = ByteBuffer.allocate(1024).order(ByteOrder.LITTLE_ENDIAN);
                localBuf.set(buf);
            }
            buf.clear();
            String type = metadata.getModelClass().getName();
            // model class
            flushBuf(md, buf, type.length() * 2).put(type.getBytes(Files.UTF_8));
            flushBuf(md, buf, metadata.getPartitionType().name().length() * 2).put(metadata.getPartitionType().name().getBytes(Files.UTF_8));
            for (int i = 0; i < metadata.getColumnCount(); i++) {
                ColumnMetadata m = metadata.getColumnMetadata(i);
                flushBuf(md, buf, m.name.length() * 2).put(m.name.getBytes(Files.UTF_8));
                flushBuf(md, buf, 4).putInt(m.size);
                flushBuf(md, buf, 4).putInt(m.distinctCountHint);
                flushBuf(md, buf, 1).put((byte) (m.indexed ? 1 : 0));
                if (m.sameAs != null) {
                    flushBuf(md, buf, m.sameAs.length() * 2).put(m.sameAs.getBytes(Files.UTF_8));
                }
            }
            buf.flip();
            md.update(buf);
            return md.digest();

        } catch (NoSuchAlgorithmException e) {
            throw new JournalRuntimeException("Cannot create MD5 digest.", e);
        }
    }

    public static int hash(String s, int M) {
        return s == null ? 0 : (s.hashCode() & 0xFFFFFFF) % M;
    }

    private Checksum() {
    }

    private static ByteBuffer flushBuf(MessageDigest md, ByteBuffer buf, int len) {
        if (buf.remaining() < len) {
            buf.flip();
            md.update(buf);
            buf.clear();
        }
        return buf;
    }
}
