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

package io.questdb.cairo;

import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.std.str.CharSink;

import java.util.Objects;

/**
 * Immutable carrier for a single dimension of a composite partition key, i.e.
 * one component of a partitioning scheme that combines a time unit with one
 * or more non-time (typically SYMBOL) columns.
 * <p>
 * A dimension is either derived from a source column (identity, hash or
 * truncate transforms) or from an arbitrary expression.
 */
public final class PartitionDimension {

    // raw value of a SYMBOL column
    public static final byte KIND_IDENTITY = 0;
    // hash(col, N) -> 0..N-1
    public static final byte KIND_HASH = 1;
    // truncate(col, N) -> first N chars
    public static final byte KIND_TRUNCATE = 2;
    // (expr) AS alias
    public static final byte KIND_EXPRESSION = 3;

    private final String alias;         // dir key label + dimension name; never null
    private final int columnIndex;      // source SYMBOL column; -1 for KIND_EXPRESSION
    private final String exprText;      // serialized expression for KIND_EXPRESSION; null otherwise
    private final byte kind;
    private final int param;            // N for HASH/TRUNCATE; 0 otherwise

    public PartitionDimension(byte kind, int columnIndex, int param, String alias, String exprText) {
        this.kind = kind;
        this.columnIndex = columnIndex;
        this.param = param;
        this.alias = alias;
        this.exprText = exprText;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        PartitionDimension that = (PartitionDimension) o;
        return kind == that.kind
                && columnIndex == that.columnIndex
                && param == that.param
                && Objects.equals(alias, that.alias)
                && Objects.equals(exprText, that.exprText);
    }

    public String getAlias() {
        return alias;
    }

    public int getColumnIndex() {
        return columnIndex;
    }

    public String getExprText() {
        return exprText;
    }

    public byte getKind() {
        return kind;
    }

    public int getParam() {
        return param;
    }

    @Override
    public int hashCode() {
        return Objects.hash(kind, columnIndex, param, alias, exprText);
    }

    /**
     * Renders the normalized transform token for this dimension, e.g.
     * {@code hash(symbol, 32)}, {@code exchange}, {@code truncate(symbol, 3)}
     * or {@code (expr) AS asset_class}. The source column name (for
     * IDENTITY/HASH/TRUNCATE) is resolved from {@code columnNames} using this
     * dimension's {@link #columnIndex}.
     */
    public void toSink(CharSink<?> sink, RecordMetadata columnNames) {
        switch (kind) {
            case KIND_IDENTITY:
                sink.put(columnNames.getColumnName(columnIndex));
                break;
            case KIND_HASH:
                sink.put("hash(").put(columnNames.getColumnName(columnIndex)).put(", ").put(param).put(')');
                break;
            case KIND_TRUNCATE:
                sink.put("truncate(").put(columnNames.getColumnName(columnIndex)).put(", ").put(param).put(')');
                break;
            case KIND_EXPRESSION:
                sink.put('(').put(exprText).put(") AS ").put(alias);
                break;
            default:
                throw new UnsupportedOperationException("Unsupported partition dimension kind: " + kind);
        }
    }
}
