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
import com.questdb.std.str.DirectCharSequence;
import com.questdb.std.str.NativeLPSZ;
import com.questdb.std.str.Path;
import com.questdb.store.Record;
import com.questdb.store.RecordCursor;
import com.questdb.store.RecordMetadata;
import com.questdb.store.SymbolTable;
import com.questdb.store.factory.ReaderFactory;
import com.questdb.store.factory.configuration.JournalConfiguration;

public class $ColsRecordSource extends AbstractRecordSource {
    private static final Log LOG = LogFactory.getLog($ColsRecordSource.class);
    private static final ThreadLocal<Path> tlPath = ThreadLocal.withInitial(Path::new);
    private static final ThreadLocal<Path> tlCompositePath = ThreadLocal.withInitial(Path::new);

    private static final ThreadLocal<NativeLPSZ> tlNativeLpsz = ThreadLocal.withInitial(NativeLPSZ::new);

    private static final ThreadLocal<DirectCharSequence> tlDcs = ThreadLocal.withInitial(DirectCharSequence::new);

    private final RecordMetadata metadata;
    private final RecordList records;
    private final int metaSize;
    private final int maxMetaSize;

    public $ColsRecordSource(int pageSize, int metaSize, int maxMetaSize) {
        this.metadata = new $ColsRecordMetadata();
        this.records = new RecordList(this.metadata, pageSize);
        this.records.setStorageFacade(new MasterStorageFacade().of(metadata));
        this.metaSize = metaSize;
        this.maxMetaSize = maxMetaSize;
    }

    public static void init() {
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
        DirectCharSequence dcs = tlDcs.get();
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

                            compositePath.of(base).concat(name).concat(JournalConfiguration.FILE_NAME).$();

                            if (Files.exists(compositePath)) {

                                long fd = Files.openRO(compositePath);
                                if (fd < 0) {
                                    LOG.error().$("Cannot open: ").$(compositePath).$(" [").$(Os.errno()).$(']').$();
                                    continue;
                                }

                                int columnCount;
                                int partitionBy;
                                int timestampIndex;
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
//                                    // skip over location string
//                                    readPtr += Unsafe.getUnsafe().getInt(readPtr) * 2 + 4;

                                    partitionBy = Unsafe.getUnsafe().getInt(readPtr);
                                    columnCount = Unsafe.getUnsafe().getInt(readPtr + 4);
                                    timestampIndex = Unsafe.getUnsafe().getInt(readPtr + 8);

                                    readPtr += 12;

                                    for (int i = 0; i < columnCount; i++) {

                                        p = records.beginRecord(p);
                                        // table_name
                                        records.appendStr(name);

                                        // column_name
                                        int l = Unsafe.getUnsafe().getInt(readPtr) * 2;
                                        readPtr += 4;
                                        records.appendStr(dcs.of(readPtr, readPtr + l));
                                        readPtr += l;
                                        // column_type
                                        records.appendInt(Unsafe.getUnsafe().getInt(readPtr));
                                        readPtr += 12; // skip size and avgsize
                                        // timestamp
                                        records.appendBool(i == timestampIndex);
                                        // partition_by
                                        records.appendInt(i == timestampIndex ? partitionBy : SymbolTable.VALUE_IS_NULL);
                                        // indexed
                                        records.appendBool(Unsafe.getBool(readPtr));
                                        readPtr += 9; // skip 2 ints
                                        // index_buckets
                                        records.appendInt(Unsafe.getUnsafe().getInt(readPtr));
                                        readPtr += 4;
                                        l = Unsafe.getUnsafe().getInt(readPtr);
                                        readPtr += 4;
                                        if (l > -1) {
                                            readPtr += (l * 2);
                                        }
                                        readPtr += 1;
                                    }

                                } finally {
                                    Files.close(fd);
                                }
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
        sink.putQuoted("op").put(':').putQuoted("$cols");
        sink.put('}');
    }
}
