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

package io.questdb.recovery;

import io.questdb.cairo.wal.WalTxnType;

/**
 * Immutable representation of a single WAL event record. Created by
 * {@link BoundedWalEventReader} via static factory methods for each
 * event type ({@code DATA}, {@code SQL}, {@code TRUNCATE}, etc.).
 *
 * <p>Fields not applicable to a given type hold sentinel values
 * ({@link TxnState#UNSET_LONG}, {@link TxnState#UNSET_INT}, or {@code null}).
 */
public final class WalEventEntry {
    private final int cmdType;
    private final long endRowID;
    private final long maxTimestamp;
    private final long minTimestamp;
    private final boolean outOfOrder;
    private final int rawLength;
    private final long rawOffset;
    private final String sqlText;
    private final long startRowID;
    private final long txn;
    private final byte type;
    private final String typeName;

    private WalEventEntry(
            long txn, byte type, String typeName, long rawOffset, int rawLength,
            long startRowID, long endRowID, long minTimestamp, long maxTimestamp, boolean outOfOrder,
            int cmdType, String sqlText
    ) {
        this.cmdType = cmdType;
        this.endRowID = endRowID;
        this.maxTimestamp = maxTimestamp;
        this.minTimestamp = minTimestamp;
        this.outOfOrder = outOfOrder;
        this.rawLength = rawLength;
        this.rawOffset = rawOffset;
        this.sqlText = sqlText;
        this.startRowID = startRowID;
        this.txn = txn;
        this.type = type;
        this.typeName = typeName;
    }

    public static WalEventEntry ofData(
            long txn, long rawOffset, int rawLength,
            long startRowID, long endRowID, long minTimestamp, long maxTimestamp, boolean outOfOrder
    ) {
        return new WalEventEntry(txn, WalTxnType.DATA, typeName(WalTxnType.DATA), rawOffset, rawLength,
                startRowID, endRowID, minTimestamp, maxTimestamp, outOfOrder,
                TxnState.UNSET_INT, null);
    }

    public static WalEventEntry ofMatViewData(
            long txn, long rawOffset, int rawLength,
            long startRowID, long endRowID, long minTimestamp, long maxTimestamp, boolean outOfOrder
    ) {
        return new WalEventEntry(txn, WalTxnType.MAT_VIEW_DATA, typeName(WalTxnType.MAT_VIEW_DATA), rawOffset, rawLength,
                startRowID, endRowID, minTimestamp, maxTimestamp, outOfOrder,
                TxnState.UNSET_INT, null);
    }

    public static WalEventEntry ofMatViewInvalidate(long txn, long rawOffset, int rawLength) {
        return new WalEventEntry(txn, WalTxnType.MAT_VIEW_INVALIDATE, typeName(WalTxnType.MAT_VIEW_INVALIDATE), rawOffset, rawLength,
                TxnState.UNSET_LONG, TxnState.UNSET_LONG, TxnState.UNSET_LONG, TxnState.UNSET_LONG, false,
                TxnState.UNSET_INT, null);
    }

    public static WalEventEntry ofSql(long txn, long rawOffset, int rawLength, int cmdType, String sqlText) {
        return new WalEventEntry(txn, WalTxnType.SQL, typeName(WalTxnType.SQL), rawOffset, rawLength,
                TxnState.UNSET_LONG, TxnState.UNSET_LONG, TxnState.UNSET_LONG, TxnState.UNSET_LONG, false,
                cmdType, sqlText);
    }

    public static WalEventEntry ofTruncate(long txn, long rawOffset, int rawLength) {
        return new WalEventEntry(txn, WalTxnType.TRUNCATE, typeName(WalTxnType.TRUNCATE), rawOffset, rawLength,
                TxnState.UNSET_LONG, TxnState.UNSET_LONG, TxnState.UNSET_LONG, TxnState.UNSET_LONG, false,
                TxnState.UNSET_INT, null);
    }

    public static WalEventEntry ofUnknown(long txn, byte type, long rawOffset, int rawLength) {
        return new WalEventEntry(txn, type, typeName(type), rawOffset, rawLength,
                TxnState.UNSET_LONG, TxnState.UNSET_LONG, TxnState.UNSET_LONG, TxnState.UNSET_LONG, false,
                TxnState.UNSET_INT, null);
    }

    public static WalEventEntry ofViewDefinition(long txn, long rawOffset, int rawLength, String viewSql) {
        return new WalEventEntry(txn, WalTxnType.VIEW_DEFINITION, typeName(WalTxnType.VIEW_DEFINITION), rawOffset, rawLength,
                TxnState.UNSET_LONG, TxnState.UNSET_LONG, TxnState.UNSET_LONG, TxnState.UNSET_LONG, false,
                TxnState.UNSET_INT, viewSql);
    }

    public static String typeName(byte type) {
        return switch (type) {
            case WalTxnType.DATA -> "DATA";
            case WalTxnType.SQL -> "SQL";
            case WalTxnType.TRUNCATE -> "TRUNCATE";
            case WalTxnType.MAT_VIEW_DATA -> "MAT_VIEW_DATA";
            case WalTxnType.MAT_VIEW_INVALIDATE -> "MAT_VIEW_INVALIDATE";
            case WalTxnType.VIEW_DEFINITION -> "VIEW_DEFINITION";
            default -> "UNKNOWN(" + type + ")";
        };
    }

    public int getCmdType() {
        return cmdType;
    }

    public long getEndRowID() {
        return endRowID;
    }

    public long getMaxTimestamp() {
        return maxTimestamp;
    }

    public long getMinTimestamp() {
        return minTimestamp;
    }

    public int getRawLength() {
        return rawLength;
    }

    public long getRawOffset() {
        return rawOffset;
    }

    public String getSqlText() {
        return sqlText;
    }

    public long getStartRowID() {
        return startRowID;
    }

    public long getTxn() {
        return txn;
    }

    public byte getType() {
        return type;
    }

    public String getTypeName() {
        return typeName;
    }

    public boolean isOutOfOrder() {
        return outOfOrder;
    }
}
