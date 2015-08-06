/*
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
 */

package com.nfsdb.io;

import com.nfsdb.collections.CharSequenceObjHashMap;
import com.nfsdb.collections.ObjList;
import com.nfsdb.io.parser.CsvParser;
import com.nfsdb.io.parser.listener.Listener;
import com.nfsdb.logging.Logger;
import com.nfsdb.storage.ColumnType;
import com.nfsdb.utils.ByteBuffers;
import com.nfsdb.utils.Numbers;
import com.nfsdb.utils.Unsafe;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

@SuppressFBWarnings({"CLI_CONSTANT_LIST_INDEX"})
public class ImportSchema {

    private static final Logger LOGGER = Logger.getLogger(ImportSchema.class);

    private static final CharSequenceObjHashMap<ImportedColumnType> importedTypeLookup = new CharSequenceObjHashMap<ImportedColumnType>() {{
        put("YYYY-MM-DDThh:mm:ss", ImportedColumnType.DATE_ISO);
        put("YYYY-MM-DD hh:mm:ss", ImportedColumnType.DATE_1);
        put("MM/DD/YYYY", ImportedColumnType.DATE_2);
    }};

    private final ObjList<ImportedColumnMetadata> metadata = new ObjList<>();

    public ImportSchema(String content) {
        final long mem = Unsafe.getUnsafe().allocateMemory(content.length());
        try {
            long p = mem;
            for (int i = 0; i < content.length(); i++) {
                Unsafe.getUnsafe().putByte(p++, (byte) content.charAt(i));
            }
            load(mem, content.length());
        } finally {
            Unsafe.getUnsafe().freeMemory(mem);
        }
    }

    public ImportSchema(File schema) throws IOException {
        try (RandomAccessFile raf = new RandomAccessFile(schema, "r")) {
            try (FileChannel channel = raf.getChannel()) {
                MappedByteBuffer buf = channel.map(FileChannel.MapMode.READ_ONLY, 0, channel.size());
                try {
                    load(ByteBuffers.getAddress(buf), channel.size());
                } finally {
                    ByteBuffers.release(buf);
                }
            }
        }

    }

    public ObjList<ImportedColumnMetadata> getMetadata() {
        return metadata;
    }

    private void load(long lo, long len) {
        try (CsvParser parser = new CsvParser()) {
            parser.parse(lo, len, Integer.MAX_VALUE, new Listener() {
                @Override
                public void onError(int line) {
                    LOGGER.warn("Error on schema line " + line);
                }

                @Override
                public void onFieldCount(int count) {

                }

                @SuppressFBWarnings({"RCN_REDUNDANT_NULLCHECK_OF_NONNULL_VALUE"})
                @Override
                public void onFields(int line, CharSequence[] values, int hi) {
                    if (hi < 1) {
                        LOGGER.warn("Ignoring schema line " + line);
                        return;
                    }

                    ImportedColumnMetadata meta = new ImportedColumnMetadata();


                    try {
                        meta.columnIndex = Numbers.parseInt(values[0].toString().trim());
                        meta.type = ColumnType.valueOf(values[1].toString().toUpperCase().trim());
                        if (meta.type == null) {
                            LOGGER.error("Invalid column type: " + values[1]);
                            return;
                        }
                        meta.size = meta.type.size();

                        if (values[2].length() > 0) {
                            meta.importedType = importedTypeLookup.get(values[2]);
                            if (meta.importedType == null) {
                                LOGGER.error("Unsupported format: " + values[2]);
                            }
                        }

                        if (meta.importedType == null) {
                            meta.importedType = ImportedColumnType.valueOf(values[1].toString().toUpperCase().trim());
                        }

                        metadata.add(meta);
                    } catch (NumberFormatException e) {
                        LOGGER.error("Ignoring schema line " + line);
                    }

                }

                @Override
                public void onHeader(CharSequence[] values, int hi) {

                }

                @Override
                public void onLineCount(int count) {

                }
            });
            parser.parseLast();
        }

    }
}
