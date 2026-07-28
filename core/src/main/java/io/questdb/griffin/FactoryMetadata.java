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

package io.questdb.griffin;

import io.questdb.cairo.TableColumnMetadata;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.std.str.Utf16Sink;

/**
 * Presents a factory's output as {@link RecordMetadata}, adding the INT width answers that a bare
 * {@link RecordMetadata} cannot express. Every other question goes straight to the factory's own
 * metadata, so this stays a faithful view of what the cursor produces; only
 * {@link #isColumnIntWidthStable(int)} and {@link #isColumnRowStable(int)} ask the factory.
 * <p>
 * {@link SqlCodeGenerator} compiles a WHERE clause against it so that
 * {@link FunctionParser#createColumn(int, CharSequence, RecordMetadata)} emits the same column
 * function for an alias as the projection above it does. A filter compiled against the bare
 * metadata took the conservative width-stable default and emitted {@code IntColumn}, so it read an
 * overflowing INT expression at INT width while the projection read it wide - the two halves of one
 * query then disagreed about the same alias, and {@code WHERE a = <64-bit literal>} excluded a row
 * whose own {@code a::LONG} was that literal.
 * <p>
 * This borrows the factory - it never owns or closes it - so it must not outlive it. Its answers are
 * fixed once the factory is built, and the compiler asks at most once per referenced column, so
 * borrowing costs nothing beyond the compile.
 *
 * @see RecordCursorFactory#isColumnIntWidthStable(int)
 * @see FactoryColumnTypes
 */
public class FactoryMetadata implements RecordMetadata {
    private final RecordCursorFactory factory;
    private final RecordMetadata metadata;

    public FactoryMetadata(RecordCursorFactory factory) {
        this.factory = factory;
        this.metadata = factory.getMetadata();
    }

    @Override
    public int getColumnCount() {
        return metadata.getColumnCount();
    }

    @Override
    public int getColumnIndex(CharSequence columnName) {
        return metadata.getColumnIndex(columnName);
    }

    @Override
    public int getColumnIndexQuiet(CharSequence columnName, int lo, int hi) {
        return metadata.getColumnIndexQuiet(columnName, lo, hi);
    }

    @Override
    public int getColumnIndexQuiet(CharSequence columnName) {
        return metadata.getColumnIndexQuiet(columnName);
    }

    @Override
    public byte getColumnIndexType(int columnIndex) {
        return metadata.getColumnIndexType(columnIndex);
    }

    @Override
    public TableColumnMetadata getColumnMetadata(int columnIndex) {
        return metadata.getColumnMetadata(columnIndex);
    }

    @Override
    public String getColumnName(int columnIndex) {
        return metadata.getColumnName(columnIndex);
    }

    @Override
    public int getColumnType(int columnIndex) {
        return metadata.getColumnType(columnIndex);
    }

    @Override
    public int getColumnType(CharSequence columnName) {
        return metadata.getColumnType(columnName);
    }

    @Override
    public int getIndexValueBlockCapacity(int columnIndex) {
        return metadata.getIndexValueBlockCapacity(columnIndex);
    }

    @Override
    public int getIndexValueBlockCapacity(CharSequence columnName) {
        return metadata.getIndexValueBlockCapacity(columnName);
    }

    @Override
    public RecordMetadata getMetadata(int columnIndex) {
        return metadata.getMetadata(columnIndex);
    }

    @Override
    public int getTimestampIndex() {
        return metadata.getTimestampIndex();
    }

    @Override
    public int getTimestampType() {
        return metadata.getTimestampType();
    }

    @Override
    public int getWriterIndex(int columnIndex) {
        return metadata.getWriterIndex(columnIndex);
    }

    @Override
    public boolean hasColumn(int columnIndex) {
        return metadata.hasColumn(columnIndex);
    }

    @Override
    public boolean isColumnIndexed(int columnIndex) {
        return metadata.isColumnIndexed(columnIndex);
    }

    @Override
    public boolean isColumnIntWidthStable(int columnIndex) {
        return factory.isColumnIntWidthStable(columnIndex);
    }

    /**
     * Paired with {@link #isColumnIntWidthStable(int)} and read only where that one answers false.
     * The two are a pair: forwarding one while inheriting the other's default would report a
     * width-unstable column as row-unstable and move a comparison to long width on both operands.
     */
    @Override
    public boolean isColumnRowStable(int columnIndex) {
        return factory.isColumnRowStable(columnIndex);
    }

    @Override
    public boolean isDedupKey(int columnIndex) {
        return metadata.isDedupKey(columnIndex);
    }

    @Override
    public boolean isSymbolTableStatic(int columnIndex) {
        return metadata.isSymbolTableStatic(columnIndex);
    }

    @Override
    public boolean isSymbolTableStatic(CharSequence columnName) {
        return metadata.isSymbolTableStatic(columnName);
    }

    @Override
    public boolean isWalEnabled() {
        return metadata.isWalEnabled();
    }

    @Override
    public boolean splitsOnDot() {
        // Forwarded so a wrapped join metadata keeps resolving a composed table.column name by
        // splitting on the dot, and so SqlUtil's quote-strip retry skips it exactly as it does for
        // the bare metadata this stands in for.
        return metadata.splitsOnDot();
    }

    @Override
    public void toJson(Utf16Sink sink) {
        metadata.toJson(sink);
    }

    @Override
    public void toPlan(PlanSink sink) {
        metadata.toPlan(sink);
    }
}
