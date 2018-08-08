/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2018 Appsicle
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

package com.questdb.ql.sys;

import com.questdb.log.Log;
import com.questdb.log.LogFactory;
import com.questdb.ql.CancellationHandler;
import com.questdb.ql.MasterStorageFacade;
import com.questdb.ql.RecordList;
import com.questdb.ql.ops.AbstractRecordSource;
import com.questdb.std.Files;
import com.questdb.std.Misc;
import com.questdb.std.Os;
import com.questdb.std.Unsafe;
import com.questdb.std.str.CharSink;
import com.questdb.std.str.LPSZ;
import com.questdb.std.str.NativeLPSZ;
import com.questdb.std.str.Path;
import com.questdb.store.Record;
import com.questdb.store.RecordCursor;
import com.questdb.store.RecordMetadata;
import com.questdb.store.factory.ReaderFactory;
import com.questdb.store.factory.configuration.JournalConfiguration;

public class $TabsRecordSource extends AbstractRecordSource {

    private static final Log LOG = LogFactory.getLog($TabsRecordSource.class);
    private static final ThreadLocal<Path> tlPath = ThreadLocal.withInitial(Path::new);
    private static final ThreadLocal<Path> tlCompositePath = ThreadLocal.withInitial(Path::new);

    private static final ThreadLocal<NativeLPSZ> tlNativeLpsz = ThreadLocal.withInitial(NativeLPSZ::new);

    private final RecordList records;
    private final $TabsRecordMetadata metadata;
    private final int metaSize;
    private final int maxMetaSize;

    public $TabsRecordSource(int pageSize, int metaSize, int maxMetaSize) {
        this.metadata = new $TabsRecordMetadata();
        this.records = new RecordList(metadata, pageSize);
        this.records.setStorageFacade(new MasterStorageFacade().of(metadata));
        this.metaSize = metaSize;
        this.maxMetaSize = maxMetaSize;
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
    public RecordCursor prepareCursor(ReaderFactory factory, CancellationHandler cancellationHandler) {
        records.clear();
        NativeLPSZ name = tlNativeLpsz.get();
        int bufSz = metaSize;
        long buf = Unsafe.malloc(bufSz);

        try {
            String base = factory.getConfiguration().getJournalBase().getAbsolutePath();
            Path path = tlPath.get().of(base).$();
            long find = Files.findFirst(path);
            if (find > 0) {
                try {
                    long p = -1;
                    Path compositePath = tlCompositePath.get();
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
                                    if (len > maxMetaSize) {
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
                                    partitionBy = Unsafe.getUnsafe().getInt(readPtr);
                                    columnCount = Unsafe.getUnsafe().getInt(readPtr + 4);
                                } finally {
                                    Files.close(fd);
                                }
                                p = records.beginRecord(p);
                                // name
                                records.appendStr(name);
                                // partition type
                                records.appendInt(partitionBy);
                                // partition count
                                records.appendInt(countDirs(compositePath.of(base).concat(name).$()));
                                // column count
                                records.appendInt(columnCount);
                                // last modified
                                records.appendLong(lastModified);
                                // size
                                records.appendLong(sumSizes(compositePath));
                            }
                        }
                    } while (Files.findNext(find) > 0);
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
    public Record getRecord() {
        return records.getRecord();
    }

    @Override
    public Record newRecord() {
        return records.newRecord();
    }

    @Override
    public void toSink(CharSink sink) {
        sink.put('{');
        sink.putQuoted("op").put(':').putQuoted("$tabs");
        sink.put('}');
    }

    private static int countDirs(LPSZ path) {
        int count = 0;
        long find = Files.findFirst(path);
        if (find == 0) {
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
            } while (Files.findNext(find) > 0);
        } finally {
            Files.findClose(find);
        }
        return count;
    }

    private static long sumSizes(LPSZ path) {
        long total = 0;
        long find = Files.findFirst(path);
        if (find == 0) {
            return 0;
        }

        try (Path cp = new Path()) {
            NativeLPSZ file = tlNativeLpsz.get();
            try {
                do {
                    file.of(Files.findName(find));

                    if (Files.isDots(file)) {
                        continue;
                    }

                    LPSZ n = cp.of(path).concat(file).$();
                    if (Files.findType(find) == Files.DT_DIR) {
                        total += sumSizes(n);
                    } else {
                        total += Files.length(n);
                    }
                } while (Files.findNext(find) > 0);
            } finally {
                Files.findClose(find);
            }
        }
        return total;
    }
}
