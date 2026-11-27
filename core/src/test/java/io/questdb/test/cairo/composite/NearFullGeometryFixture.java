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

package io.questdb.test.cairo.composite;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.PartitionGeometryFile;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.TxReader;
import io.questdb.cairo.TxWriter;
import io.questdb.std.FilesFacade;
import io.questdb.std.MemoryTag;
import io.questdb.std.ObjList;
import io.questdb.std.str.Path;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;

/**
 * Re-points a composite partition's {@code _txn} geometry ref at a sparse copy of its committed record near {@link
 * PartitionGeometryFile#MAX_FILE_SIZE}: placed to leave room for exactly a record of {@code roomForPieces} pieces, or,
 * with 0, 8 bytes short of the cap, less than any record needs. A larger publish has to rotate, and from {@link
 * #LAST_GENERATION} cannot.
 */
final class NearFullGeometryFixture {
    static final int LAST_GENERATION = 15;
    private static final long NEAR_FULL_OFFSET = PartitionGeometryFile.MAX_FILE_SIZE - 8;

    private NearFullGeometryFixture() {
    }

    static void plantCommittedRecord(CairoEngine engine, CharSequence tableName, long partitionTs, int generation) {
        plant(engine, tableName, partitionTs, generation, 0, false, 0, 0, 0);
    }

    static void plantCommittedRecord(
            CairoEngine engine,
            CharSequence tableName,
            long partitionTs,
            int generation,
            int roomForPieces,
            boolean isDeadSpaceDropped,
            long cutRow,
            long headTsHi,
            long tailTsLo
    ) {
        Assert.assertTrue("the cut splits rows of one timestamp", cutRow == 0 || headTsHi < tailTsLo);
        plant(engine, tableName, partitionTs, generation, roomForPieces, isDeadSpaceDropped, cutRow, headTsHi, tailTsLo);
    }

    private static void plant(
            CairoEngine engine,
            CharSequence tableName,
            long partitionTs,
            int generation,
            int roomForPieces,
            boolean isDeadSpaceDropped,
            long cutRow,
            long headTsHi,
            long tailTsLo
    ) {
        final CairoConfiguration configuration = engine.getConfiguration();
        final FilesFacade ff = configuration.getFilesFacade();
        try (
                TableWriter writer = TestUtils.getWriter(engine, tableName);
                Path path = new Path();
                PartitionGeometryFile committed = new PartitionGeometryFile(MemoryTag.NATIVE_DEFAULT);
                PartitionGeometryFile planted = new PartitionGeometryFile(MemoryTag.NATIVE_DEFAULT)
        ) {
            final TxWriter txWriter = writer.getTxWriter();
            final int partitionIndex = txWriter.getPartitionIndex(partitionTs);
            Assert.assertTrue("partition is not composite ahead of the plant", txWriter.isPartitionComposite(partitionIndex));
            final long committedRef = txWriter.getGeometryRef(partitionIndex);
            path.of(configuration.getDbRoot()).concat(writer.getTableToken());
            TableUtils.setPathForNativePartition(
                    path,
                    writer.getTimestampType(),
                    writer.getPartitionBy(),
                    partitionTs,
                    txWriter.getPartitionNameTxn(partitionIndex)
            );
            committed.read(ff, path, TxReader.geometryGeneration(committedRef), TxReader.geometryOffset(committedRef));
            final PartitionGeometryFile record;
            if (!isDeadSpaceDropped && cutRow == 0) {
                record = committed;
            } else {
                final int pieceCount = committed.getPieceCount();
                Assert.assertTrue("only a one-piece record is cut", cutRow == 0 || pieceCount == 1);
                planted.beginRecord(committed.getWriterTxn(), committed.getSeqTxn(), pieceCount + 1);
                for (int p = 0; p < pieceCount; p++) {
                    final long rowOffset = committed.getPieceRowOffset(p);
                    final long rowCount = committed.getPieceRowCount(p);
                    final long writerTxn = committed.getPieceWriterTxn(p);
                    final long lastWriteMicros = committed.getPieceLastWriteMicros(p);
                    final long dedupSortVersion = committed.getPieceDedupSortVersion(p);
                    if (cutRow == 0) {
                        planted.addPiece(committed.getPieceTimestampLo(p), committed.getPieceTimestampHi(p), rowOffset, rowCount, writerTxn, lastWriteMicros, dedupSortVersion);
                    } else {
                        Assert.assertTrue("the cut leaves a half empty", cutRow < rowCount);
                        planted.addPiece(committed.getPieceTimestampLo(p), headTsHi, rowOffset, cutRow, writerTxn, lastWriteMicros, dedupSortVersion);
                        planted.addPiece(tailTsLo, committed.getPieceTimestampHi(p), rowOffset + cutRow, rowCount - cutRow, writerTxn, lastWriteMicros, dedupSortVersion);
                    }
                }
                planted.setPhysicalRows(isDeadSpaceDropped ? committed.getLiveRows() : committed.getPhysicalRows());
                planted.setLiveRows(committed.getLiveRows());
                planted.setLastWriteMicros(committed.getLastWriteMicros());
                record = planted;
            }
            final long offset = roomForPieces == 0
                    ? NEAR_FULL_OFFSET
                    : PartitionGeometryFile.MAX_FILE_SIZE - record.getRecordSize() - PartitionGeometryFile.recordSize(roomForPieces);
            record.append(ff, path, generation, offset, configuration.getCommitMode());
            writer.setPartitionGeometryRefForTest(partitionTs, TxReader.packGeometryRef(generation, offset));
            txWriter.commit(new ObjList<>());
        }
    }
}
