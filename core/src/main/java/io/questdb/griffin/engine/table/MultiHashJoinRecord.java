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

package io.questdb.griffin.engine.table;

import io.questdb.cairo.ColumnTypes;
import io.questdb.cairo.arr.ArrayView;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.engine.join.NullRecordFactory;
import io.questdb.std.BinarySequence;
import io.questdb.std.Decimal128;
import io.questdb.std.Decimal256;
import io.questdb.std.IntList;
import io.questdb.std.Interval;
import io.questdb.std.Long256;
import io.questdb.std.ObjList;
import io.questdb.std.str.CharSink;
import io.questdb.std.str.Utf8Sequence;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * The joined row of a hash join with N stages, read flat: every output column maps to one source
 * record and a column of that record, so a getter costs three array reads and one call, however many
 * joins the operator stacks, where nested {@code JoinRecord}s cost a comparison and a call per join.
 * <p>
 * Source 0 is the probe record, the scan the operator streams. Source {@code k + 1} is stage
 * {@code k}'s payload record while the stage has a match, and a record of typed NULLs, which
 * {@link NullRecordFactory} builds over the stage's payload columns, while it has none (a LEFT
 * join's miss).
 * <p>
 * The record keeps one column map per orientation. In orientation A the probe record is the left
 * input of the first join and stage 0's payload is its right input. In orientation B the runtime
 * input swap builds the left input and probes the right one: the left columns come from stage 0's
 * payload record and stage 0's columns from the probe record. Later stages read their own payload
 * in both orientations. {@link #getRowId()} and {@link #getUpdateRowId()} read the left input's
 * record, as {@code JoinRecord} reads its master's.
 * <p>
 * A caller picks the orientation with {@link #setOrientation(boolean)}, then binds the probe
 * record and each stage's payload record, then marks each row's stages matched or not with
 * {@link #setHasMatch(int, boolean)}.
 */
public class MultiHashJoinRecord implements Record {
    private final int[] columnIndexesA;
    private final int @Nullable [] columnIndexesB;
    private final int[] columnSourcesA;
    private final int @Nullable [] columnSourcesB;
    private final ObjList<Record> nullRecordsA;
    @Nullable
    private final ObjList<Record> nullRecordsB;
    // Source 0 is the probe record, source k + 1 is stage k's payload or null record.
    private final ObjList<Record> sources;
    private final ObjList<Record> stageRecords;
    private int[] columnIndexes;
    private int[] columnSources;
    private int leftSource;
    private ObjList<Record> nullRecords;

    /**
     * @param columnSourcesA  orientation A: for each output column, its source, 0 for the probe
     *                        record and {@code k + 1} for stage {@code k}
     * @param columnIndexesA  orientation A: for each output column, its column in that source
     * @param payloadTypesA   orientation A: for each stage, the column types of its payload
     *                        record, which type the stage's NULLs
     * @param columnSourcesB  orientation B, the swapped first join, as {@code columnSourcesA};
     *                        null when the operator never swaps
     * @param columnIndexesB  orientation B, as {@code columnIndexesA}; null when the operator
     *                        never swaps
     * @param payloadTypesB   orientation B, as {@code payloadTypesA}: stage 0's payload is the
     *                        left input; null when the operator never swaps
     */
    public MultiHashJoinRecord(
            @NotNull IntList columnSourcesA,
            @NotNull IntList columnIndexesA,
            @NotNull ObjList<? extends ColumnTypes> payloadTypesA,
            @Nullable IntList columnSourcesB,
            @Nullable IntList columnIndexesB,
            @Nullable ObjList<? extends ColumnTypes> payloadTypesB
    ) {
        final int stageCount = payloadTypesA.size();
        assert stageCount > 0;
        assert (columnSourcesB == null) == (columnIndexesB == null) && (columnSourcesB == null) == (payloadTypesB == null);
        this.columnSourcesA = toColumnMap(columnSourcesA, columnIndexesA, stageCount);
        this.columnIndexesA = columnIndexesA.toArray();
        this.nullRecordsA = newNullRecords(payloadTypesA);
        if (payloadTypesB != null) {
            assert payloadTypesB.size() == stageCount && columnSourcesB.size() == columnSourcesA.size();
            this.columnSourcesB = toColumnMap(columnSourcesB, columnIndexesB, stageCount);
            this.columnIndexesB = columnIndexesB.toArray();
            this.nullRecordsB = newNullRecords(payloadTypesB);
        } else {
            this.columnSourcesB = null;
            this.columnIndexesB = null;
            this.nullRecordsB = null;
        }
        this.sources = new ObjList<>(stageCount + 1);
        this.sources.setPos(stageCount + 1);
        this.stageRecords = new ObjList<>(stageCount);
        this.stageRecords.setPos(stageCount);
        // Orientation A until the caller picks one.
        this.columnSources = this.columnSourcesA;
        this.columnIndexes = this.columnIndexesA;
        this.nullRecords = this.nullRecordsA;
    }

    @Override
    public ArrayView getArray(int col, int columnType) {
        return sources.getQuick(columnSources[col]).getArray(columnIndexes[col], columnType);
    }

    @Override
    public int getArrayDimLen(int col, int columnType, int dim) {
        return sources.getQuick(columnSources[col]).getArrayDimLen(columnIndexes[col], columnType, dim);
    }

    @Override
    public double getArrayDouble1d2d(int col, int columnType, int idx0, int idx1) {
        return sources.getQuick(columnSources[col]).getArrayDouble1d2d(columnIndexes[col], columnType, idx0, idx1);
    }

    @Override
    public BinarySequence getBin(int col) {
        return sources.getQuick(columnSources[col]).getBin(columnIndexes[col]);
    }

    @Override
    public long getBinLen(int col) {
        return sources.getQuick(columnSources[col]).getBinLen(columnIndexes[col]);
    }

    @Override
    public boolean getBool(int col) {
        return sources.getQuick(columnSources[col]).getBool(columnIndexes[col]);
    }

    @Override
    public byte getByte(int col) {
        return sources.getQuick(columnSources[col]).getByte(columnIndexes[col]);
    }

    @Override
    public char getChar(int col) {
        return sources.getQuick(columnSources[col]).getChar(columnIndexes[col]);
    }

    @Override
    public long getDate(int col) {
        return sources.getQuick(columnSources[col]).getDate(columnIndexes[col]);
    }

    @Override
    public void getDecimal128(int col, Decimal128 sink) {
        sources.getQuick(columnSources[col]).getDecimal128(columnIndexes[col], sink);
    }

    @Override
    public short getDecimal16(int col) {
        return sources.getQuick(columnSources[col]).getDecimal16(columnIndexes[col]);
    }

    @Override
    public void getDecimal256(int col, Decimal256 sink) {
        sources.getQuick(columnSources[col]).getDecimal256(columnIndexes[col], sink);
    }

    @Override
    public int getDecimal32(int col) {
        return sources.getQuick(columnSources[col]).getDecimal32(columnIndexes[col]);
    }

    @Override
    public long getDecimal64(int col) {
        return sources.getQuick(columnSources[col]).getDecimal64(columnIndexes[col]);
    }

    @Override
    public byte getDecimal8(int col) {
        return sources.getQuick(columnSources[col]).getDecimal8(columnIndexes[col]);
    }

    @Override
    public double getDouble(int col) {
        return sources.getQuick(columnSources[col]).getDouble(columnIndexes[col]);
    }

    @Override
    public float getFloat(int col) {
        return sources.getQuick(columnSources[col]).getFloat(columnIndexes[col]);
    }

    @Override
    public byte getGeoByte(int col) {
        return sources.getQuick(columnSources[col]).getGeoByte(columnIndexes[col]);
    }

    @Override
    public int getGeoInt(int col) {
        return sources.getQuick(columnSources[col]).getGeoInt(columnIndexes[col]);
    }

    @Override
    public long getGeoLong(int col) {
        return sources.getQuick(columnSources[col]).getGeoLong(columnIndexes[col]);
    }

    @Override
    public short getGeoShort(int col) {
        return sources.getQuick(columnSources[col]).getGeoShort(columnIndexes[col]);
    }

    @Override
    public int getIPv4(int col) {
        return sources.getQuick(columnSources[col]).getIPv4(columnIndexes[col]);
    }

    @Override
    public int getInt(int col) {
        return sources.getQuick(columnSources[col]).getInt(columnIndexes[col]);
    }

    @Override
    public Interval getInterval(int col) {
        return sources.getQuick(columnSources[col]).getInterval(columnIndexes[col]);
    }

    @Override
    public long getLong(int col) {
        return sources.getQuick(columnSources[col]).getLong(columnIndexes[col]);
    }

    @Override
    public long getLong128Hi(int col) {
        return sources.getQuick(columnSources[col]).getLong128Hi(columnIndexes[col]);
    }

    @Override
    public long getLong128Lo(int col) {
        return sources.getQuick(columnSources[col]).getLong128Lo(columnIndexes[col]);
    }

    @Override
    public void getLong256(int col, CharSink<?> sink) {
        sources.getQuick(columnSources[col]).getLong256(columnIndexes[col], sink);
    }

    @Override
    public Long256 getLong256A(int col) {
        return sources.getQuick(columnSources[col]).getLong256A(columnIndexes[col]);
    }

    @Override
    public Long256 getLong256B(int col) {
        return sources.getQuick(columnSources[col]).getLong256B(columnIndexes[col]);
    }

    @Override
    public Record getRecord(int col) {
        return sources.getQuick(columnSources[col]).getRecord(columnIndexes[col]);
    }

    /**
     * The left input's row id, as {@code JoinRecord} returns its master's: the probe record's in
     * orientation A, stage 0's payload record's in orientation B.
     */
    @Override
    public long getRowId() {
        return sources.getQuick(leftSource).getRowId();
    }

    @Override
    public short getShort(int col) {
        return sources.getQuick(columnSources[col]).getShort(columnIndexes[col]);
    }

    @Override
    public CharSequence getStrA(int col) {
        return sources.getQuick(columnSources[col]).getStrA(columnIndexes[col]);
    }

    @Override
    public CharSequence getStrB(int col) {
        return sources.getQuick(columnSources[col]).getStrB(columnIndexes[col]);
    }

    @Override
    public int getStrLen(int col) {
        return sources.getQuick(columnSources[col]).getStrLen(columnIndexes[col]);
    }

    @Override
    public CharSequence getSymA(int col) {
        return sources.getQuick(columnSources[col]).getSymA(columnIndexes[col]);
    }

    @Override
    public CharSequence getSymB(int col) {
        return sources.getQuick(columnSources[col]).getSymB(columnIndexes[col]);
    }

    @Override
    public long getTimestamp(int col) {
        return sources.getQuick(columnSources[col]).getTimestamp(columnIndexes[col]);
    }

    @Override
    public long getUpdateRowId() {
        return sources.getQuick(leftSource).getUpdateRowId();
    }

    @Override
    public Utf8Sequence getVarcharA(int col) {
        return sources.getQuick(columnSources[col]).getVarcharA(columnIndexes[col]);
    }

    @Override
    public Utf8Sequence getVarcharB(int col) {
        return sources.getQuick(columnSources[col]).getVarcharB(columnIndexes[col]);
    }

    @Override
    public int getVarcharSize(int col) {
        return sources.getQuick(columnSources[col]).getVarcharSize(columnIndexes[col]);
    }

    /**
     * Marks whether stage {@code stage} has a match for the current row: its columns then read its
     * payload record, or typed NULLs when it has none.
     */
    public void setHasMatch(int stage, boolean hasMatch) {
        sources.setQuick(stage + 1, hasMatch ? stageRecords.getQuick(stage) : nullRecords.getQuick(stage));
    }

    /**
     * Picks the column map of orientation B, the swapped first join, or of orientation A, and
     * marks every stage matched. Bind the probe record and the payload records of the
     * orientation's stages after this call: stage 0's payload is a different record in each.
     */
    public void setOrientation(boolean isSwapped) {
        if (isSwapped) {
            assert columnSourcesB != null : "the record has no column map for the swapped orientation";
            columnSources = columnSourcesB;
            columnIndexes = columnIndexesB;
            nullRecords = nullRecordsB;
            // The left input is stage 0's payload.
            leftSource = 1;
        } else {
            columnSources = columnSourcesA;
            columnIndexes = columnIndexesA;
            nullRecords = nullRecordsA;
            leftSource = 0;
        }
        for (int stage = 0, n = stageRecords.size(); stage < n; stage++) {
            sources.setQuick(stage + 1, stageRecords.getQuick(stage));
        }
    }

    /**
     * Binds the record of the scan the operator streams: the left input in orientation A, the
     * first join's right input in orientation B.
     */
    public void setProbeRecord(Record probeRecord) {
        sources.setQuick(0, probeRecord);
    }

    /**
     * Binds stage {@code stage}'s payload record, which the stage positions on each match, and
     * marks the stage matched.
     */
    public void setStageRecord(int stage, Record payloadRecord) {
        stageRecords.setQuick(stage, payloadRecord);
        sources.setQuick(stage + 1, payloadRecord);
    }

    private static ObjList<Record> newNullRecords(ObjList<? extends ColumnTypes> payloadTypes) {
        final int stageCount = payloadTypes.size();
        final ObjList<Record> nullRecords = new ObjList<>(stageCount);
        for (int stage = 0; stage < stageCount; stage++) {
            nullRecords.add(NullRecordFactory.getInstance(payloadTypes.getQuick(stage)));
        }
        return nullRecords;
    }

    private static int[] toColumnMap(IntList columnSources, IntList columnIndexes, int stageCount) {
        assert columnSources.size() == columnIndexes.size();
        for (int col = 0, n = columnSources.size(); col < n; col++) {
            final int source = columnSources.getQuick(col);
            assert source >= 0 && source <= stageCount : "column " + col + " maps to source " + source;
            assert columnIndexes.getQuick(col) >= 0 : "column " + col + " maps to a negative index";
        }
        return columnSources.toArray();
    }
}
