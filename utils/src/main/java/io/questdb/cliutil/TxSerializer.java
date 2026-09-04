/*+*****************************************************************************
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

        // Plan 3b Task 3 fix wave 1: toJson() below refuses to ever produce JSON from a composite
        // (stride-8) source _txn, so the documented dump-edit-restore round trip can never feed a
        // composite JSON back into this method. But nothing stops an operator from hand-crafting or
        // editing a JSON file and pointing it at an EXISTING composite table's _txn path -- this method
        // would then silently overwrite that table's stride-8 partition region with stride-4 data (the
        // memset/jumpTo below wipes it unconditionally). Peek the target's existing marker first, via a
        // short-lived read-only view that is fully closed before the destructive read-write open below,
        // and refuse the same way toJson() does rather than silently corrupting it.
        refuseIfExistingCompositeTarget(targetPath);

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
            // Plan 3b Task 1: self-describing partition-stride marker -- a GLOBAL property (not part of
            // either A/B section). This CLI tool's TxFileStruct.AttachedPartition has no cellKey field and
            // the partition write loop below is hardcoded to LONGS_PER_TX_ATTACHED_PARTITION (4 longs per
            // partition), so it can only ever emit plain/stride-4 data.
            // Plan 3b Task 3 fix wave 1: TxSerializer is STRIDE-4-ONLY and refuses to read a composite
            // (stride-8) source _txn at all (see the guard in toJson() and refuseIfExistingCompositeTarget()
            // above) -- no composite JSON can ever reach this method to be written back out. Route the
            // write through the shared partitionStrideMarker() helper (the same single source of truth
            // used by TxReader#dumpTo/TableUtils#createTxn/TxWriter#finishABHeader) instead of a bare
            // literal 0, so this is truthful and self-describing rather than relying on the memset
            // zero-fill above, and so intent survives a future change to what the marker values mean.
            rwTxMem.putInt(TX_BASE_OFFSET_PARTITION_STRIDE_32, partitionStrideMarker(LONGS_PER_TX_ATTACHED_PARTITION));

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
                    rwTxMem.putLong(part.PM_FILE_SIZE);
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

                // Plan 3b Task 3 fix wave 1: this tool's TxFileStruct/AttachedPartition model has no
                // cellKey field, and both directions of this class are hardcoded to
                // LONGS_PER_TX_ATTACHED_PARTITION (4 longs/partition, see the partition-table loop below
                // and the mirrored write in serializeJson) -- it structurally can only represent a plain
                // (stride-4) _txn. Read the same GLOBAL, non-A/B, non-versioned stride marker
                // TxReader#unsafeLoadBaseOffset() reads (see TX_BASE_OFFSET_PARTITION_STRIDE_32's field
                // doc in TableUtils) and refuse outright on a composite (stride-8) source file, rather
                // than silently mis-folding its attached-partitions region at the wrong stride (the
                // pre-existing gap this guard closes: before this check, a composite _txn's partition
                // region was divided by 4 instead of 8, corrupting the count and every field's offset).
                final int partitionStrideMarker = roTxMem.getInt(TX_BASE_OFFSET_PARTITION_STRIDE_32);
                if (partitionStrideMarker == LONGS_PER_TX_ATTACHED_PARTITION_COMPOSITE) {
                    throw new UnsupportedOperationException(
                            "TxSerializer does not support composite-partitioned tables (_txn stride marker=" +
                                    partitionStrideMarker + ")");
                }

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
                        partition.PM_FILE_SIZE = roTxMem.getLong(offset);
                        offset += Long.BYTES;
                    }
                }
            }
        }
        return GSON.toJson(tx);
    }

    /**
     * Plan 3b Task 3 fix wave 1: defense-in-depth companion to the {@link #toJson} guard. Peeks
     * {@code targetPath}'s existing on-disk stride marker (if the file already exists and is big enough
     * to hold one) via a short-lived, independent read-only view -- fully closed before {@link
     * #serializeJson} opens its destructive read-write handle on the same path -- and refuses with the
     * same error {@link #toJson} would give, rather than silently letting {@link #serializeJson}
     * overwrite an existing composite table's stride-8 partition region with stride-4 data.
     */
    private void refuseIfExistingCompositeTarget(String targetPath) {
        try (Path path = new Path().of(targetPath)) {
            if (!ff.exists(path.$())) {
                return;
            }
            final long len = ff.length(path.$());
            if (len < TX_BASE_OFFSET_PARTITION_STRIDE_32 + Integer.BYTES) {
                // Too small to even hold the marker (e.g. a brand-new/empty file) -- nothing to protect.
                return;
            }
            try (MemoryMR roTxMem = Vm.getCMRInstance(ff, path.$(), len, MemoryTag.MMAP_DEFAULT)) {
                roTxMem.growToFileSize();
                final int existingMarker = roTxMem.getInt(TX_BASE_OFFSET_PARTITION_STRIDE_32);
                if (existingMarker == LONGS_PER_TX_ATTACHED_PARTITION_COMPOSITE) {
                    throw new UnsupportedOperationException(
                            "TxSerializer does not support composite-partitioned tables (_txn stride marker=" +
                                    existingMarker + "); refusing to overwrite existing composite _txn at " + targetPath);
                }
            }
        }
    }

    private static void printUsage() {
        System.out.println("usage: " + TxSerializer.class.getName() + " -s <json_path> <txn_path> | -d <txn_path>");
    }

    private void serializeFile(String jsonFile, String target) throws IOException {
        String json = new String(Files.readAllBytes(Paths.get(jsonFile)), StandardCharsets.UTF_8);
        serializeJson(json, target);
    }
}
