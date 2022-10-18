/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 ******************************************************************************/

package io.questdb.cliutil;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryCMARW;
import io.questdb.cairo.vm.api.MemoryMR;
import io.questdb.log.LogFactory;
import io.questdb.std.FilesFacade;
import io.questdb.std.FilesFacadeImpl;
import io.questdb.std.MemoryTag;
import io.questdb.std.Vect;
import io.questdb.std.str.Path;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;

import static io.questdb.cairo.TableUtils.*;

public class TxSerializer {
    static long TX_OFFSET_TXN = TX_OFFSET_TXN_64;
    static long TX_OFFSET_SEQ_TXN = TX_OFFSET_SEQ_TXN_64;
    static long TX_OFFSET_DATA_VERSION = TX_OFFSET_DATA_VERSION_64;
    static long TX_OFFSET_PARTITION_TABLE_VERSION = TX_OFFSET_PARTITION_TABLE_VERSION_64;
    static long TX_OFFSET_MAP_WRITER_COUNT = TX_OFFSET_MAP_WRITER_COUNT_32;
    static long TX_OFFSET_TRANSIENT_ROW_COUNT = TX_OFFSET_TRANSIENT_ROW_COUNT_64;
    static long TX_OFFSET_FIXED_ROW_COUNT = TX_OFFSET_FIXED_ROW_COUNT_64;
    static long TX_OFFSET_STRUCT_VERSION = TX_OFFSET_STRUCT_VERSION_64;
    static long TX_OFFSET_MIN_TIMESTAMP = TX_OFFSET_MIN_TIMESTAMP_64;
    static long TX_OFFSET_MAX_TIMESTAMP = TX_OFFSET_MAX_TIMESTAMP_64;
    static long TX_OFFSET_TRUNCATE_VERSION = TX_OFFSET_TRUNCATE_VERSION_64;
    static FilesFacade ff = new FilesFacadeImpl();

    /*
     * Read _txn file and prints to std output JSON translation.
     * Reads json file and saves it to binary _txn format.
     *
     *  Command line arguments: -s <json_path> <txn_path> | -d <txn_path>
     */
    public static void main(String[] args) throws IOException {
        LogFactory.configureSync();
        if (args.length < 2 || args.length > 3) {
            printUsage();
            return;
        }


        TxSerializer serializer = new TxSerializer();
        if ("-s".equals(args[0])) {
            if (args.length != 3) {
                printUsage();
                return;
            }
            serializer.serializeFile(args[1], args[2]);
        }

        if ("-d".equals(args[0])) {
            String json = serializer.toJson(args[1]);
            if (json != null) {
                System.out.println(json);
            }
        }
    }

    public void serializeJson(String json, String target) {
        Gson des = new Gson();
        TxFileStruct tx = des.fromJson(json, TxFileStruct.class);

        long version = tx.TX_OFFSET_TXN;
        boolean isA = (version & 1L) == 0L;
        long baseOffset = TX_BASE_HEADER_SIZE;
        long offsetOffset = isA ? TX_BASE_OFFSET_A_32 : TX_BASE_OFFSET_B_32;
        long symbolSizeOffset = isA ? TX_BASE_OFFSET_SYMBOLS_SIZE_A_32 : TX_BASE_OFFSET_SYMBOLS_SIZE_B_32;
        long partitionSizeOffst = isA ? TX_BASE_OFFSET_PARTITIONS_SIZE_A_32 : TX_BASE_OFFSET_PARTITIONS_SIZE_B_32;

        if (tx.ATTACHED_PARTITION_SIZE != 0 && (tx.ATTACHED_PARTITIONS == null || tx.ATTACHED_PARTITION_SIZE != tx.ATTACHED_PARTITIONS.size())) {
            String arraySize = tx.ATTACHED_PARTITIONS == null ? "null" : Integer.toString(tx.ATTACHED_PARTITIONS.size());
            throw new IllegalArgumentException("ATTACHED_PARTITIONS array size of " + arraySize + " is different from ATTACHED_PARTITION_SIZE of " + tx.ATTACHED_PARTITION_SIZE);
        }

        if (tx.TX_OFFSET_MAP_WRITER_COUNT != 0 && (tx.SYMBOLS == null || tx.TX_OFFSET_MAP_WRITER_COUNT != tx.SYMBOLS.size())) {
            String arraySize = tx.SYMBOLS == null ? "null" : Integer.toString(tx.SYMBOLS.size());
            throw new IllegalArgumentException("SYMBOLS array size if " + arraySize + "is different from MAP_WRITER_COUNT of " + tx.TX_OFFSET_MAP_WRITER_COUNT);
        }

        long fileSize = tx.calculateFileSize();
        try (Path path = new Path()) {
            path.put(target).$();
            try (MemoryCMARW rwTxMem = Vm.getSmallCMARWInstance(ff, path, MemoryTag.MMAP_DEFAULT, CairoConfiguration.O_NONE)) {
                Vect.memset(rwTxMem.addressOf(0), fileSize, 0);
                rwTxMem.putLong(TX_BASE_OFFSET_VERSION_64, version);
                rwTxMem.putLong(offsetOffset, baseOffset);
                rwTxMem.putLong(symbolSizeOffset, tx.TX_OFFSET_MAP_WRITER_COUNT * 8L);
                rwTxMem.putLong(partitionSizeOffst, tx.ATTACHED_PARTITION_SIZE * 8L * 4L);

                rwTxMem.setTruncateSize(fileSize);
                rwTxMem.putLong(baseOffset + TX_OFFSET_TXN, tx.TX_OFFSET_TXN);
                rwTxMem.putLong(baseOffset + TX_OFFSET_TRANSIENT_ROW_COUNT, tx.TX_OFFSET_TRANSIENT_ROW_COUNT);
                rwTxMem.putLong(baseOffset + TX_OFFSET_FIXED_ROW_COUNT, tx.TX_OFFSET_FIXED_ROW_COUNT);
                rwTxMem.putLong(baseOffset + TX_OFFSET_MIN_TIMESTAMP, tx.TX_OFFSET_MIN_TIMESTAMP);
                rwTxMem.putLong(baseOffset + TX_OFFSET_MAX_TIMESTAMP, tx.TX_OFFSET_MAX_TIMESTAMP);
                rwTxMem.putLong(baseOffset + TX_OFFSET_DATA_VERSION, tx.TX_OFFSET_DATA_VERSION);
                rwTxMem.putLong(baseOffset + TX_OFFSET_STRUCT_VERSION, tx.TX_OFFSET_STRUCT_VERSION);
                rwTxMem.putLong(baseOffset + TX_OFFSET_PARTITION_TABLE_VERSION, tx.TX_OFFSET_PARTITION_TABLE_VERSION);
                rwTxMem.putLong(baseOffset + TX_OFFSET_COLUMN_VERSION_64, tx.TX_OFFSET_COLUMN_VERSION);
                rwTxMem.putLong(baseOffset + TX_OFFSET_TRUNCATE_VERSION, tx.TX_OFFSET_TRUNCATE_VERSION);
                rwTxMem.putLong(baseOffset + TX_OFFSET_SEQ_TXN, tx.TX_OFFSET_SEQ_TXN);
                rwTxMem.putInt(baseOffset + TX_OFFSET_MAP_WRITER_COUNT, tx.TX_OFFSET_MAP_WRITER_COUNT);

                if (tx.TX_OFFSET_MAP_WRITER_COUNT != 0) {
                    int isym = 0;
                    for (TxFileStruct.SymbolInfo si : tx.SYMBOLS) {
                        long offset = baseOffset + getSymbolWriterIndexOffset(isym++);
                        rwTxMem.putInt(offset, si.COUNT);
                        offset += 4;
                        rwTxMem.putInt(offset, si.UNCOMMITTED_COUNT);
                    }
                }

                rwTxMem.putInt(baseOffset + getPartitionTableSizeOffset(tx.TX_OFFSET_MAP_WRITER_COUNT), tx.ATTACHED_PARTITION_SIZE * 8 * 4);
                if (tx.ATTACHED_PARTITION_SIZE != 0) {
                    int ipart = 0;
                    for (TxFileStruct.AttachedPartition part : tx.ATTACHED_PARTITIONS) {
                        long offset = baseOffset + getPartitionTableIndexOffset(tx.TX_OFFSET_MAP_WRITER_COUNT, 4 * ipart++);
                        rwTxMem.putLong(offset, part.TS);
                        offset += 8;
                        rwTxMem.putLong(offset, part.SIZE);
                        offset += 8;
                        rwTxMem.putLong(offset, part.NAME_TX);
                        offset += 8;
                        rwTxMem.putLong(offset, part.DATA_TX);
                    }
                }
            }
        }
    }

    public String toJson(String txPath) {
        TxFileStruct tx = new TxFileStruct();

        try (Path path = new Path()) {
            path.put(txPath).$();
            if (!ff.exists(path)) {
                System.out.println("error: " + txPath + " does not exist");
            }
            try (MemoryMR roTxMem = Vm.getMRInstance(ff, path, ff.length(path), MemoryTag.MMAP_DEFAULT)) {
                roTxMem.growToFileSize();
                long version = roTxMem.getLong(TX_BASE_OFFSET_VERSION_64);
                boolean isA = (version & 1L) == 0L;
                long baseOffset = isA ? roTxMem.getInt(TX_BASE_OFFSET_A_32) : roTxMem.getInt(TX_BASE_OFFSET_B_32);
                tx.TX_OFFSET_TXN = roTxMem.getLong(baseOffset + TX_OFFSET_TXN);
                tx.TX_OFFSET_TRANSIENT_ROW_COUNT = roTxMem.getLong(baseOffset + TX_OFFSET_TRANSIENT_ROW_COUNT);
                tx.TX_OFFSET_FIXED_ROW_COUNT = roTxMem.getLong(baseOffset + TX_OFFSET_FIXED_ROW_COUNT);
                tx.TX_OFFSET_MIN_TIMESTAMP = roTxMem.getLong(baseOffset + TX_OFFSET_MIN_TIMESTAMP);
                tx.TX_OFFSET_MAX_TIMESTAMP = roTxMem.getLong(baseOffset + TX_OFFSET_MAX_TIMESTAMP);
                tx.TX_OFFSET_DATA_VERSION = roTxMem.getLong(baseOffset + TX_OFFSET_DATA_VERSION);
                tx.TX_OFFSET_STRUCT_VERSION = roTxMem.getLong(baseOffset + TX_OFFSET_STRUCT_VERSION);
                tx.TX_OFFSET_MAP_WRITER_COUNT = roTxMem.getInt(baseOffset + TX_OFFSET_MAP_WRITER_COUNT);
                tx.TX_OFFSET_PARTITION_TABLE_VERSION = roTxMem.getLong(baseOffset + TX_OFFSET_PARTITION_TABLE_VERSION);
                tx.TX_OFFSET_COLUMN_VERSION = roTxMem.getLong(baseOffset + TX_OFFSET_COLUMN_VERSION_64);
                tx.TX_OFFSET_TRUNCATE_VERSION = roTxMem.getLong(baseOffset + TX_OFFSET_TRUNCATE_VERSION);
                tx.TX_OFFSET_SEQ_TXN = roTxMem.getLong(baseOffset + TX_OFFSET_SEQ_TXN);

                int symbolsCount = tx.TX_OFFSET_MAP_WRITER_COUNT;
                tx.SYMBOLS = new ArrayList<>();
                long offset = baseOffset + getSymbolWriterIndexOffset(0);
                while (offset + 3 < Math.min(roTxMem.size(), baseOffset + getSymbolWriterIndexOffset(symbolsCount))) {
                    TxFileStruct.SymbolInfo symbol = new TxFileStruct.SymbolInfo();
                    tx.SYMBOLS.add(symbol);
                    symbol.COUNT = roTxMem.getInt(offset);
                    offset += 4;
                    if (offset + 3 < roTxMem.size()) {
                        symbol.UNCOMMITTED_COUNT = roTxMem.getInt(offset);
                        offset += 4;
                    }
                }

                int txAttachedPartitionsSize = roTxMem.getInt(baseOffset + getPartitionTableSizeOffset(symbolsCount)) / Long.BYTES / 4;
                tx.ATTACHED_PARTITION_SIZE = txAttachedPartitionsSize;
                tx.ATTACHED_PARTITIONS = new ArrayList<>();
                offset = baseOffset + getPartitionTableIndexOffset(symbolsCount, 0);

                while (offset + 7 < Math.min(roTxMem.size(), baseOffset + getPartitionTableIndexOffset(symbolsCount, txAttachedPartitionsSize * 4))) {
                    TxFileStruct.AttachedPartition partition = new TxFileStruct.AttachedPartition();
                    tx.ATTACHED_PARTITIONS.add(partition);
                    partition.TS = roTxMem.getLong(offset);
                    offset += 8;
                    if (offset + 7 < roTxMem.size()) {
                        partition.SIZE = roTxMem.getLong(offset);
                        offset += 8;
                    }
                    if (offset + 7 < roTxMem.size()) {
                        partition.NAME_TX = roTxMem.getLong(offset);
                        offset += 8;
                    }
                    if (offset + 7 < roTxMem.size()) {
                        partition.DATA_TX = roTxMem.getLong(offset);
                        offset += 8;
                    }
                }
            }
        }

        Gson gson = new GsonBuilder().setPrettyPrinting().create();
        return gson.toJson(tx);
    }

    private static void printUsage() {
        System.out.println("usage: " + TxSerializer.class.getName() + " -s <json_path> <txn_path> | -d <json_path>");
    }

    private void serializeFile(String jsonFile, String target) throws IOException {
        String json = new String(Files.readAllBytes(Paths.get(jsonFile)), StandardCharsets.UTF_8);
        serializeJson(json, target);
    }
}
