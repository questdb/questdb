/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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

package io.questdb.griffin.engine.window;

import io.questdb.cairo.ArrayColumnTypes;
import io.questdb.cairo.arr.ArrayView;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.WindowSPI;
import io.questdb.griffin.SqlCodeGenerator;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.model.ExpressionNode;
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

public interface WindowFunction extends Function {
    int ONE_PASS = 1;
    int TWO_PASS = 2;
    int ZERO_PASS = 0;

    default void computeNext(Record record) {
    }

    @Override
    default ArrayView getArray(Record rec) {
        throw new UnsupportedOperationException();
    }

    @Override
    default BinarySequence getBin(Record rec) {
        throw new UnsupportedOperationException();
    }

    @Override
    default long getBinLen(Record rec) {
        throw new UnsupportedOperationException();
    }

    @Override
    default boolean getBool(Record rec) {
        throw new UnsupportedOperationException();
    }

    @Override
    default byte getByte(Record rec) {
        throw new UnsupportedOperationException();
    }

    @Override
    default char getChar(Record rec) {
        throw new UnsupportedOperationException();
    }

    @Override
    default long getDate(Record rec) {
        throw new UnsupportedOperationException();
    }

    @Override
    default void getDecimal128(Record rec, Decimal128 sink) {
        throw new UnsupportedOperationException();
    }

    @Override
    default short getDecimal16(Record rec) {
        throw new UnsupportedOperationException();
    }

    @Override
    default void getDecimal256(Record rec, Decimal256 sink) {
        throw new UnsupportedOperationException();
    }

    @Override
    default int getDecimal32(Record rec) {
        throw new UnsupportedOperationException();
    }

    @Override
    default long getDecimal64(Record rec) {
        throw new UnsupportedOperationException();
    }

    @Override
    default byte getDecimal8(Record rec) {
        throw new UnsupportedOperationException();
    }

    @Override
    default double getDouble(Record rec) {
        // unused
        throw new UnsupportedOperationException();
    }

    @Override
    default float getFloat(Record rec) {
        throw new UnsupportedOperationException();
    }

    @Override
    default byte getGeoByte(Record rec) {
        throw new UnsupportedOperationException();
    }

    @Override
    default int getGeoInt(Record rec) {
        throw new UnsupportedOperationException();
    }

    @Override
    default long getGeoLong(Record rec) {
        throw new UnsupportedOperationException();
    }

    @Override
    default short getGeoShort(Record rec) {
        throw new UnsupportedOperationException();
    }

    @Override
    default int getIPv4(Record rec) {
        throw new UnsupportedOperationException();
    }

    @Override
    default int getInt(Record rec) {
        throw new UnsupportedOperationException();
    }

    @Override
    default @NotNull Interval getInterval(Record rec) {
        throw new UnsupportedOperationException();
    }

    @Override
    default long getLong(Record rec) {
        throw new UnsupportedOperationException();
    }

    @Override
    default long getLong128Hi(Record rec) {
        throw new UnsupportedOperationException();
    }

    @Override
    default long getLong128Lo(Record rec) {
        throw new UnsupportedOperationException();
    }

    @Override
    default void getLong256(Record rec, CharSink<?> sink) {
        throw new UnsupportedOperationException();
    }

    @Override
    default Long256 getLong256A(Record rec) {
        throw new UnsupportedOperationException();
    }

    @Override
    default Long256 getLong256B(Record rec) {
        throw new UnsupportedOperationException();
    }

    /**
     * @return pass1 scan direction.
     * Some {@link #ONE_PASS} and {@link #TWO_PASS} window functions may be more efficient when using a backward scan.
     */
    default Pass1ScanDirection getPass1ScanDirection() {
        return Pass1ScanDirection.FORWARD;
    }

    /**
     * @return number of additional passes over base data set required to calculate this function.
     * {@link  #ZERO_PASS} means window function can be calculated on the fly and doesn't require additional passes .
     */
    default int getPassCount() {
        return ONE_PASS;
    }

    @Override
    default RecordCursorFactory getRecordCursorFactory() {
        throw new UnsupportedOperationException();
    }

    @Override
    default short getShort(Record rec) {
        throw new UnsupportedOperationException();
    }

    @Override
    default CharSequence getStrA(Record rec) {
        throw new UnsupportedOperationException();
    }

    @Override
    default CharSequence getStrB(Record rec) {
        throw new UnsupportedOperationException();
    }

    @Override
    default int getStrLen(Record rec) {
        throw new UnsupportedOperationException();
    }

    @Override
    default CharSequence getSymbol(Record rec) {
        throw new UnsupportedOperationException();
    }

    @Override
    default CharSequence getSymbolB(Record rec) {
        throw new UnsupportedOperationException();
    }

    @Override
    default long getTimestamp(Record rec) {
        throw new UnsupportedOperationException();
    }

    @Override
    default Utf8Sequence getVarcharA(Record rec) {
        throw new UnsupportedOperationException();
    }

    @Override
    default Utf8Sequence getVarcharB(Record rec) {
        throw new UnsupportedOperationException();
    }

    @Override
    default int getVarcharSize(Record rec) {
        throw new UnsupportedOperationException();
    }

    default void initRecordComparator(
            SqlCodeGenerator sqlGenerator,
            RecordMetadata metadata,
            ArrayColumnTypes chainTypes,
            IntList orderIndices,
            ObjList<ExpressionNode> orderBy,
            IntList orderByDirections
    ) throws SqlException {
    }

    default boolean isIgnoreNulls() {
        return false;
    }

    void pass1(Record record, long recordOffset, WindowSPI spi);

    default void pass2(Record record, long recordOffset, WindowSPI spi) {
    }

    default void preparePass2() {
    }

    /**
     * Releases native memory and resets internal state to default/initial.
     * It differs from close() in that it doesn't release memory held by metadata, e.g. partition by key functions.
     * This means function may still be used after calling reopen().
     **/
    void reset();

    /*
      Set index of record chain column used to store window function result.
     */
    void setColumnIndex(int columnIndex);

    enum Pass1ScanDirection {
        FORWARD, BACKWARD
    }
}
