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

package io.questdb.griffin.engine.join;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.griffin.LogicalPlans;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.Plannable;
import io.questdb.std.Chars;
import io.questdb.std.IntList;
import io.questdb.std.str.StringSink;
import io.questdb.std.str.Utf8s;

/**
 * Compares the master columns of the join keys that share a slave column, for an ASOF, LT or HORIZON JOIN that looks up
 * one key per shared slave column, see {@link io.questdb.griffin.plan.logical.JoinKey#getLookupKeyIndex}. No slave row
 * matches a master row whose two columns of a pair differ, so the join answers such a row with no slave row and skips
 * the lookup. Two columns compare equal when both are NULL, as the join keys do; their types pass
 * {@link LogicalPlans#isKeyMatchType}. The check holds no state, so the workers of a parallel join share one instance.
 */
public final class JoinKeyCheck implements Plannable {
    private final int[] checkedColumns;
    private final int[] checkedTypes;
    private final int[] lookupColumns;
    private final int[] lookupTypes;
    private final String text;

    /**
     * @param lookupColumns  the master column index of the looked-up key of each pair
     * @param checkedColumns the master column index of the checked key of each pair
     */
    public JoinKeyCheck(RecordMetadata masterMetadata, IntList lookupColumns, IntList checkedColumns) {
        final int pairCount = lookupColumns.size();
        this.lookupColumns = new int[pairCount];
        this.checkedColumns = new int[pairCount];
        this.lookupTypes = new int[pairCount];
        this.checkedTypes = new int[pairCount];
        final StringSink sink = new StringSink();
        for (int i = 0; i < pairCount; i++) {
            final int lookupColumn = lookupColumns.getQuick(i);
            final int checkedColumn = checkedColumns.getQuick(i);
            this.lookupColumns[i] = lookupColumn;
            this.checkedColumns[i] = checkedColumn;
            lookupTypes[i] = masterMetadata.getColumnType(lookupColumn);
            checkedTypes[i] = masterMetadata.getColumnType(checkedColumn);
            assert LogicalPlans.isKeyMatchType(lookupTypes[i], checkedTypes[i]);
            if (i > 0) {
                sink.put(" and ");
            }
            sink.put(masterMetadata.getColumnName(lookupColumn)).put('=').put(masterMetadata.getColumnName(checkedColumn));
        }
        this.text = sink.toString();
    }

    /**
     * True when the two columns of every pair hold the same value in the master record.
     */
    public boolean isMatchable(Record record) {
        for (int i = 0, n = lookupColumns.length; i < n; i++) {
            if (!isEqual(record, lookupColumns[i], lookupTypes[i], checkedColumns[i], checkedTypes[i])) {
                return false;
            }
        }
        return true;
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.val(text);
    }

    private static CharSequence charsA(Record record, int column, int type) {
        return ColumnType.isSymbol(type) ? record.getSymA(column) : record.getStrA(column);
    }

    private static CharSequence charsB(Record record, int column, int type) {
        return ColumnType.isSymbol(type) ? record.getSymB(column) : record.getStrB(column);
    }

    private static boolean isEqual(Record record, int columnA, int typeA, int columnB, int typeB) {
        if (ColumnType.isVarchar(typeA)) {
            return ColumnType.isVarchar(typeB)
                    ? Utf8s.equals(record.getVarcharA(columnA), record.getVarcharB(columnB))
                    : Utf8s.equalsUtf16Nc(charsB(record, columnB, typeB), record.getVarcharA(columnA));
        }
        if (ColumnType.isVarchar(typeB)) {
            return Utf8s.equalsUtf16Nc(charsA(record, columnA, typeA), record.getVarcharB(columnB));
        }
        return switch (ColumnType.tagOf(typeA)) {
            case ColumnType.SYMBOL, ColumnType.STRING ->
                    Chars.equalsNullable(charsA(record, columnA, typeA), charsB(record, columnB, typeB));
            case ColumnType.BOOLEAN -> record.getBool(columnA) == record.getBool(columnB);
            case ColumnType.BYTE -> record.getByte(columnA) == record.getByte(columnB);
            case ColumnType.SHORT -> record.getShort(columnA) == record.getShort(columnB);
            case ColumnType.CHAR -> record.getChar(columnA) == record.getChar(columnB);
            case ColumnType.INT -> record.getInt(columnA) == record.getInt(columnB);
            case ColumnType.IPv4 -> record.getIPv4(columnA) == record.getIPv4(columnB);
            case ColumnType.LONG -> record.getLong(columnA) == record.getLong(columnB);
            case ColumnType.DATE -> record.getDate(columnA) == record.getDate(columnB);
            case ColumnType.TIMESTAMP -> record.getTimestamp(columnA) == record.getTimestamp(columnB);
            case ColumnType.GEOBYTE -> record.getGeoByte(columnA) == record.getGeoByte(columnB);
            case ColumnType.GEOSHORT -> record.getGeoShort(columnA) == record.getGeoShort(columnB);
            case ColumnType.GEOINT -> record.getGeoInt(columnA) == record.getGeoInt(columnB);
            case ColumnType.GEOLONG -> record.getGeoLong(columnA) == record.getGeoLong(columnB);
            case ColumnType.UUID, ColumnType.LONG128 -> record.getLong128Lo(columnA) == record.getLong128Lo(columnB)
                    && record.getLong128Hi(columnA) == record.getLong128Hi(columnB);
            default -> throw new UnsupportedOperationException();
        };
    }
}
