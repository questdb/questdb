/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2026 QuestDB
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
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.TxReader;
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
    private static final Gson GSON = new GsonBuilder().setPrettyPrinting().create();
    private static final FilesFacade ff = new FilesFacadeImpl();

    /*
     * Reads _txn file and prints to std output JSON translation.
     * Reads json file and saves it to binary _txn format.
     *
     *  Command line arguments: -s <json_path> <txn_path> | -d <txn_path>
     */
    public static void main(String[] args) throws IOException {
        LogFactory.enableGuaranteedLogging();
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

    public void serializeJson(String json, String targetPath) {
        final TxFileStruct tx = GSON.fromJson(json, TxFileStruct.class);

        if (tx.ATTACHED_PARTITIONS_COUNT != 0 && (tx.ATTACHED_PARTITIONS == null || tx.ATTACHED_PARTITIONS_COUNT != tx.ATTACHED_PARTITIONS.size())) {
            throw new IllegalArgumentException(String.format(
                    "ATTACHED_PARTITIONS array size of [%s] is different from ATTACHED_PARTITION_SIZE of [%d]",
                    tx.ATTACHED_PARTITIONS == null ? "null" : Integer.toString(tx.ATTACHED_PARTITIONS.size()),
                    tx.ATTACHED_PARTITIONS_COUNT
            ));
        }

        if (tx.TX_OFFSET_MAP_WRITER_COUNT != 0 && (tx.SYMBOLS == null || tx.TX_OFFSET_MAP_WRITER_COUNT != tx.SYMBOLS.size())) {
            throw new IllegalArgumentException(String.format(
                    "SYMBOLS array size if [%s] is different from MAP_WRITER_COUNT of [%d]",
                    tx.SYMBOLS == null ? "null" : Integer.toString(tx.SYMBOLS.size()),
                    tx.TX_OFFSET_MAP_WRITER_COUNT
            ));
        }

        try (
                Path path = new Path().of(targetPath);
                MemoryCMARW rwTxMem = Vm.getSmallCMARWInstance(ff, path.$(), MemoryTag.MMAP_DEFAULT, CairoConfiguration.O_NONE)
        ) {
            final int symbolsSize = tx.TX_OFFSET_MAP_WRITER_COUNT * Long.BYTES;
            final int partitionSegmentSize = tx.ATTACHED_PARTITIONS_COUNT * LONGS_PER_TX_ATTACHED_PARTITION * Long.BYTES;
            final long fileSize = calculateTxRecordSize(symbolsSize, partitionSegmentSize);
            rwTxMem.jumpTo(fileSize);
            Vect.memset(rwTxMem.addressOf(0), fileSize, 0);
            rwTxMem.setTruncateSize(fileSize);

            final long version = tx.TX_OFFSET_TXN;
            final boolean isA = (version & 1L) == 0L;
            final int baseOffset = TX_BASE_HEADER_SIZE;
            rwTxMem.putLong(TX_BASE_OFFSET_VERSION_64, version);
            rwTxMem.putInt(isA ? TX_BASE_OFFSET_A_32 : TX_BASE_OFFSET_B_32, baseOffset);
            rwTxMem.putInt(isA ? TX_BASE_OFFSET_SYMBOLS_SIZE_A_32 : TX_BASE_OFFSET_SYMBOLS_SIZE_B_32, symbolsSize);
            rwTxMem.putInt(isA ? TX_BASE_OFFSET_PARTITIONS_SIZE_A_32 : TX_BASE_OFFSET_PARTITIONS_SIZE_B_32, partitionSegmentSize);

            rwTxMem.putLong(baseOffset + TX_OFFSET_TXN_64, tx.TX_OFFSET_TXN);
            rwTxMem.putLong(baseOffset + TX_OFFSET_TRANSIENT_ROW_COUNT_64, tx.TX_OFFSET_TRANSIENT_ROW_COUNT);
            rwTxMem.putLong(baseOffset + TX_OFFSET_FIXED_ROW_COUNT_64, tx.TX_OFFSET_FIXED_ROW_COUNT);
            rwTxMem.putLong(baseOffset + TX_OFFSET_MIN_TIMESTAMP_64, tx.TX_OFFSET_MIN_TIMESTAMP);
            rwTxMem.putLong(baseOffset + TX_OFFSET_MAX_TIMESTAMP_64, tx.TX_OFFSET_MAX_TIMESTAMP);
            rwTxMem.putLong(baseOffset + TX_OFFSET_DATA_VERSION_64, tx.TX_OFFSET_DATA_VERSION);
            rwTxMem.putLong(baseOffset + TX_OFFSET_STRUCT_VERSION_64, tx.TX_OFFSET_STRUCT_VERSION);
            rwTxMem.putLong(baseOffset + TX_OFFSET_PARTITION_TABLE_VERSION_64, tx.TX_OFFSET_PARTITION_TABLE_VERSION);
            rwTxMem.putLong(baseOffset + TX_OFFSET_COLUMN_VERSION_64, tx.TX_OFFSET_COLUMN_VERSION);
            rwTxMem.putLong(baseOffset + TX_OFFSET_TRUNCATE_VERSION_64, tx.TX_OFFSET_TRUNCATE_VERSION);
            rwTxMem.putLong(baseOffset + TX_OFFSET_SEQ_TXN_64, tx.TX_OFFSET_SEQ_TXN);
            rwTxMem.putInt(baseOffset + TX_OFFSET_MAP_WRITER_COUNT_32, tx.TX_OFFSET_MAP_WRITER_COUNT);
            rwTxMem.putInt(baseOffset + TX_OFFSET_LAG_ROW_COUNT_32, tx.TX_OFFSET_LAG_ROW_COUNT);
            rwTxMem.putInt(baseOffset + TX_OFFSET_LAG_TXN_COUNT_32, tx.TX_OFFSET_LAG_TXN_COUNT);
            rwTxMem.putLong(baseOffset + TX_OFFSET_LAG_MAX_TIMESTAMP_64, tx.TX_OFFSET_LAG_MAX_TIMESTAMP);
            rwTxMem.putLong(baseOffset + TX_OFFSET_LAG_MIN_TIMESTAMP_64, tx.TX_OFFSET_LAG_MIN_TIMESTAMP);
            rwTxMem.putInt(baseOffset + TX_OFFSET_CHECKSUM_32, tx.TX_OFFSET_CHECKSUM);

            if (tx.TX_OFFSET_MAP_WRITER_COUNT != 0) {
                int isym = 0;
                for (TxFileStruct.SymbolInfo si : tx.SYMBOLS) {
                    long offset = baseOffset + getSymbolWriterIndexOffset(isym++);
                    rwTxMem.putInt(offset, si.COUNT);
                    offset += 4;
                    rwTxMem.putInt(offset, si.UNCOMMITTED_COUNT);
                }
            }

            final long partitionTableOffset = TableUtils.getPartitionTableSizeOffset(tx.TX_OFFSET_MAP_WRITER_COUNT);
            rwTxMem.jumpTo(baseOffset + getPartitionTableIndexOffset(partitionTableOffset, 0) - Integer.BYTES);
            rwTxMem.putInt(partitionSegmentSize);
            if (tx.ATTACHED_PARTITIONS_COUNT != 0) {
                for (TxFileStruct.AttachedPartition part : tx.ATTACHED_PARTITIONS) {
                    rwTxMem.putLong(part.TS);
                    long maskedSize = ((part.MASK << 44) & TxReader.PARTITION_FLAGS_MASK) | (part.SIZE & TxReader.PARTITION_SIZE_MASK);
                    rwTxMem.putLong(maskedSize);
                    rwTxMem.putLong(part.NAME_TX);
                    rwTxMem.putLong(part.DATA_TX);
                }
            }
        }
    }

    public String toJson(String srcTxFilePath) {
        TxFileStruct tx = new TxFileStruct();

        try (Path path = new Path().put(srcTxFilePath)) {
            if (!ff.exists(path.$())) {
                System.err.printf("file does not exist: %s%n", srcTxFilePath);
                return null;
            }
            try (MemoryMR roTxMem = Vm.getCMRInstance(ff, path.$(), ff.length(path.$()), MemoryTag.MMAP_DEFAULT)) {
                roTxMem.growToFileSize();
                final long version = roTxMem.getLong(TX_BASE_OFFSET_VERSION_64);
                final boolean isA = (version & 1L) == 0L;
                final long baseOffset = isA ? roTxMem.getInt(TX_BASE_OFFSET_A_32) : roTxMem.getInt(TX_BASE_OFFSET_B_32);
                final int symbolsSize = isA ? roTxMem.getInt(TX_BASE_OFFSET_SYMBOLS_SIZE_A_32) : roTxMem.getInt(TX_BASE_OFFSET_SYMBOLS_SIZE_B_32);
                final int partitionSegmentSize = isA ? roTxMem.getInt(TX_BASE_OFFSET_PARTITIONS_SIZE_A_32) : roTxMem.getInt(TX_BASE_OFFSET_PARTITIONS_SIZE_B_32);

                tx.TX_OFFSET_TXN = roTxMem.getLong(baseOffset + TX_OFFSET_TXN_64);
                tx.TX_OFFSET_TRANSIENT_ROW_COUNT = roTxMem.getLong(baseOffset + TX_OFFSET_TRANSIENT_ROW_COUNT_64);
                tx.TX_OFFSET_FIXED_ROW_COUNT = roTxMem.getLong(baseOffset + TX_OFFSET_FIXED_ROW_COUNT_64);
                tx.TX_OFFSET_MIN_TIMESTAMP = roTxMem.getLong(baseOffset + TX_OFFSET_MIN_TIMESTAMP_64);
                tx.TX_OFFSET_MAX_TIMESTAMP = roTxMem.getLong(baseOffset + TX_OFFSET_MAX_TIMESTAMP_64);
                tx.TX_OFFSET_DATA_VERSION = roTxMem.getLong(baseOffset + TX_OFFSET_DATA_VERSION_64);
                tx.TX_OFFSET_STRUCT_VERSION = roTxMem.getLong(baseOffset + TX_OFFSET_STRUCT_VERSION_64);
                tx.TX_OFFSET_MAP_WRITER_COUNT = roTxMem.getInt(baseOffset + TX_OFFSET_MAP_WRITER_COUNT_32); // symbolColumnCount
                tx.TX_OFFSET_PARTITION_TABLE_VERSION = roTxMem.getLong(baseOffset + TX_OFFSET_PARTITION_TABLE_VERSION_64);
                tx.TX_OFFSET_COLUMN_VERSION = roTxMem.getLong(baseOffset + TX_OFFSET_COLUMN_VERSION_64);
                tx.TX_OFFSET_LAG_ROW_COUNT = roTxMem.getInt(baseOffset + TX_OFFSET_LAG_ROW_COUNT_32);
                tx.TX_OFFSET_LAG_TXN_COUNT = roTxMem.getInt(baseOffset + TX_OFFSET_LAG_TXN_COUNT_32);
                tx.TX_OFFSET_TRUNCATE_VERSION = roTxMem.getLong(baseOffset + TX_OFFSET_TRUNCATE_VERSION_64);
                tx.TX_OFFSET_LAG_MIN_TIMESTAMP = roTxMem.getLong(baseOffset + TX_OFFSET_LAG_MIN_TIMESTAMP_64);
                tx.TX_OFFSET_LAG_MAX_TIMESTAMP = roTxMem.getLong(baseOffset + TX_OFFSET_LAG_MAX_TIMESTAMP_64);
                tx.TX_OFFSET_CHECKSUM = roTxMem.getInt(baseOffset + TX_OFFSET_LAG_MAX_TIMESTAMP_64);
                tx.TX_OFFSET_SEQ_TXN = roTxMem.getLong(baseOffset + TX_OFFSET_SEQ_TXN_64);

                final int symbolColumnCount = symbolsSize / Long.BYTES;
                tx.SYMBOLS = new ArrayList<>(symbolColumnCount);
                long offset = baseOffset + TX_OFFSET_MAP_WRITER_COUNT_32 + Integer.BYTES;
                final long maxOffsetSymbols = offset + symbolsSize;
                while (offset + 3 < Math.min(roTxMem.size(), maxOffsetSymbols)) {
                    TxFileStruct.SymbolInfo symbol = new TxFileStruct.SymbolInfo();
                    tx.SYMBOLS.add(symbol);
                    symbol.COUNT = roTxMem.getInt(offset);
                    offset += Integer.BYTES;
                    if (offset + 3 < roTxMem.size()) {
                        symbol.UNCOMMITTED_COUNT = roTxMem.getInt(offset);
                        offset += Integer.BYTES;
                    }
                }

                final int txAttachedPartitionsCount = partitionSegmentSize / LONGS_PER_TX_ATTACHED_PARTITION / Long.BYTES;
                tx.ATTACHED_PARTITIONS_COUNT = txAttachedPartitionsCount;
                tx.ATTACHED_PARTITIONS = new ArrayList<>(txAttachedPartitionsCount);
                final long partitionTableOffset = TableUtils.getPartitionTableSizeOffset(tx.TX_OFFSET_MAP_WRITER_COUNT);
                offset = baseOffset + getPartitionTableIndexOffset(partitionTableOffset, 0);
                final long maxOffsetPartitions = offset + partitionSegmentSize;
                while (offset + 7 < Math.min(roTxMem.size(), maxOffsetPartitions)) {
                    TxFileStruct.AttachedPartition partition = new TxFileStruct.AttachedPartition();
                    tx.ATTACHED_PARTITIONS.add(partition);
                    partition.TS = roTxMem.getLong(offset);
                    offset += Long.BYTES;
                    if (offset + 7 < roTxMem.size()) {
                        long maskedSize = roTxMem.getLong(offset);
                        partition.MASK = (maskedSize & TxReader.PARTITION_FLAGS_MASK) >>> 44;
                        partition.SIZE = maskedSize & TxReader.PARTITION_SIZE_MASK;
                        offset += Long.BYTES;
                    }
                    if (offset + 7 < roTxMem.size()) {
                        partition.NAME_TX = roTxMem.getLong(offset);
                        offset += Long.BYTES;
                    }
                    if (offset + 7 < roTxMem.size()) {
                        partition.DATA_TX = roTxMem.getLong(offset);
                        offset += Long.BYTES;
                    }
                }
            }
        }
        return GSON.toJson(tx);
    }

    private static void printUsage() {
        System.out.println("usage: " + TxSerializer.class.getName() + " -s <json_path> <txn_path> | -d <txn_path>");
    }

    private void serializeFile(String jsonFile, String target) throws IOException {
        String json = new String(Files.readAllBytes(Paths.get(jsonFile)), StandardCharsets.UTF_8);
        serializeJson(json, target);
    }
}
