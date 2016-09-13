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

package com.questdb.ql.impl.sys;

import com.questdb.PartitionBy;
import com.questdb.factory.JournalReaderFactory;
import com.questdb.factory.configuration.JournalConfiguration;
import com.questdb.factory.configuration.RecordMetadata;
import com.questdb.log.Log;
import com.questdb.log.LogFactory;
import com.questdb.misc.Files;
import com.questdb.misc.Misc;
import com.questdb.misc.Os;
import com.questdb.misc.Unsafe;
import com.questdb.ql.CancellationHandler;
import com.questdb.ql.RecordCursor;
import com.questdb.ql.impl.RecordList;
import com.questdb.ql.ops.AbstractRecordSource;
import com.questdb.std.CharSink;
import com.questdb.std.CompositePath;
import com.questdb.std.NativeLPSZ;
import com.questdb.std.Path;

public class $TabsRecordSource extends AbstractRecordSource {

    private static final Log LOG = LogFactory.getLog($TabsRecordSource.class);
    private static final ThreadLocal<Path> tlPath = new ThreadLocal<Path>() {
        @Override
        protected Path initialValue() {
            return new Path();
        }
    };
    private static final ThreadLocal<CompositePath> tlCompositePath = new ThreadLocal<CompositePath>() {
        @Override
        protected CompositePath initialValue() {
            return new CompositePath();
        }
    };

    private static final ThreadLocal<NativeLPSZ> tlNativeLpsz = new ThreadLocal<NativeLPSZ>() {
        @Override
        protected NativeLPSZ initialValue() {
            return new NativeLPSZ();
        }
    };

    private final RecordList records;
    private final $TabsRecordMetadata metadata;

    public $TabsRecordSource(int pageSize) {
        this.metadata = new $TabsRecordMetadata();
        this.records = new RecordList(metadata, pageSize);
    }

    public static void init() {
        // This is static to load thread locals to ensure they don't participate in memory leak analysis.
        tlPath.get();
        tlCompositePath.get();
    }

    @Override
    public void close() {
        Misc.free(records);
    }

    @Override
    public RecordMetadata getMetadata() {
        return metadata;
    }

    @Override
    public RecordCursor prepareCursor(JournalReaderFactory factory, CancellationHandler cancellationHandler) {
        records.clear();
        NativeLPSZ name = tlNativeLpsz.get();
        int bufSz = 2 * 1024;
        long buf = Unsafe.malloc(bufSz);

        try {
            String base = factory.getConfiguration().getJournalBase().getAbsolutePath();
            Path path = tlPath.get().of(base);
            long find = Files.findFirst(path);
            if (find > 0) {
                try {
                    long p = -1;
                    CompositePath compositePath = tlCompositePath.get();
                    do {
                        cancellationHandler.check();
                        if (Files.findType(find) == Files.DT_DIR) {
                            name.of(Files.findName(find));

                            if (Files.isDots(name)) {
                                continue;
                            }

                            long lastModified = Files.getLastModified(compositePath.of(base).concat(name).$());

                            compositePath.of(base).concat(name).concat(JournalConfiguration.FILE_NAME).$();

                            if (Files.exists(compositePath)) {

                                long fd = Files.openRO(compositePath);
                                if (fd < 0) {
                                    LOG.error().$("Cannot open: ").$(compositePath).$(" [").$(Os.errno()).$(']').$();
                                    continue;
                                }

                                int columnCount;
                                int partitionBy;
                                try {
                                    long len = Files.length(compositePath);
                                    // don't bother with huge meta files. There is a high chance of them being fake.
                                    if (len > 16 * 1024 * 1024) {
                                        LOG.error().$("File : ").$(compositePath).$(" is too large [").$(len).$(']').$();
                                        continue;
                                    }

                                    // read the whole file into memory

                                    if (len > bufSz) {
                                        Unsafe.free(buf, bufSz);
                                        buf = Unsafe.malloc(bufSz = (int) len);
                                    }

                                    Files.read(fd, buf, (int) len, 0);
                                    // skip over append offset
                                    long readPtr = buf + 8;
                                    // skip over ID string
                                    readPtr += Unsafe.getUnsafe().getInt(readPtr) * 2 + 4;
                                    // skip over location string
                                    readPtr += Unsafe.getUnsafe().getInt(readPtr) * 2 + 4;
                                    partitionBy = Unsafe.getUnsafe().getInt(readPtr);
                                    columnCount = Unsafe.getUnsafe().getInt(readPtr + 4);
                                } finally {
                                    Files.close(fd);
                                }
                                p = records.beginRecord(p);
                                // name
                                records.appendStr(name);
                                // partition type
                                records.appendStr(PartitionBy.toString(partitionBy));
                                // partition count
                                records.appendInt(countDirs(base, name));
                                // column count
                                records.appendInt(columnCount);
                                // last modified
                                records.appendLong(lastModified);
                            }
                        }
                    } while (Files.findNext(find));
                } finally {
                    Files.findClose(find);
                }
            }
        } finally {
            Unsafe.free(buf, bufSz);
        }

        records.toTop();
        return records;
    }

    @Override
    public boolean supportsRowIdAccess() {
        return false;
    }

    @Override
    public void toSink(CharSink sink) {
    }

    private static int countDirs(CharSequence base, CharSequence name) {
        int count = 0;
        CompositePath path = tlCompositePath.get();
        path.of(base).concat(name).$();

        long find = Files.findFirst(path);
        if (find < 0) {
            return 0;
        }

        NativeLPSZ file = tlNativeLpsz.get();
        try {
            do {
                file.of(Files.findName(find));

                if (Files.isDots(file)) {
                    continue;
                }

                if (Files.findType(find) == Files.DT_DIR) {
                    count++;
                }
            } while (Files.findNext(find));
        } finally {
            Files.findClose(find);
        }
        return count;
    }
}
