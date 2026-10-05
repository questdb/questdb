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

import io.questdb.cairo.sql.BindVariableService;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.vm.api.MemoryA;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.TypeConstant;
import io.questdb.griffin.engine.functions.constants.ConstantFunction;

/**
 * Base of the drivers for types stored as a fixed number of bytes per row. A type driver here
 * is its {@link TypeFacts} plus the six answers that carry code, all passed to the constructor:
 * a type driver that leaves one out does not compile. Each code answer has its own functional
 * interface, so two of them cannot trade places either.
 * <p>
 * The code answers reach query-engine classes only when they run, through the lambda or method
 * reference a type driver passes, never when the type driver initialises.
 * <p>
 * Each type driver declares its width once, as its data-movement tier ({@link #getMovement()});
 * the width and log2 width derive from it. {@link ColumnType#isFixedSize(int)} is not the same
 * fact: SYMBOL and INTERVAL have a fixed width and a type driver here, yet it reports them as not
 * fixed-size, and it reports an encoded geohash or decimal type as not fixed-size while their
 * tags are, a known inconsistency of that method.
 */
public abstract class FixedSizeTypeDriver implements TypeDriver {
    private final BindVariableDefiner bindVariableDefiner;
    private final ColumnFunctionFactory columnFunctionFactory;
    private final TypeFacts facts;
    private final NullAppenderFactory nullAppenderFactory;
    private final NullConstantSource nullConstantSource;
    private final NullFiller nullFiller;
    private final TypeConstantSource typeConstantSource;

    protected FixedSizeTypeDriver(
            TypeFacts facts,
            BindVariableDefiner bindVariableDefiner,
            NullConstantSource nullConstantSource,
            TypeConstantSource typeConstantSource,
            ColumnFunctionFactory columnFunctionFactory,
            NullAppenderFactory nullAppenderFactory,
            NullFiller nullFiller
    ) {
        assert facts.movement() != PhysicalDescriptor.Movement.VAR : "fixed-size type with a var-size layout: " + facts.tag();
        this.facts = facts;
        this.bindVariableDefiner = bindVariableDefiner;
        this.nullConstantSource = nullConstantSource;
        this.typeConstantSource = typeConstantSource;
        this.columnFunctionFactory = columnFunctionFactory;
        this.nullAppenderFactory = nullAppenderFactory;
        this.nullFiller = nullFiller;
    }

    @Override
    public final int defineBindVariable(BindVariableService service, int index, int columnType, int position) throws SqlException {
        return bindVariableDefiner.define(service, index, columnType, position);
    }

    @Override
    public final PhysicalDescriptor.Accessor getAccessor() {
        return facts.accessor();
    }

    @Override
    public final PhysicalDescriptor.Arithmetic getArithmetic() {
        return facts.arithmetic();
    }

    @Override
    public final short[] getImplicitCasts() {
        return facts.implicitCasts();
    }

    @Override
    public final PhysicalDescriptor.Movement getMovement() {
        return facts.movement();
    }

    /**
     * The name of the bare tag; any other encoding of the tag has no name. A type that names
     * several encodings answers itself.
     */
    @Override
    public String getName(int columnType) {
        return columnType == facts.tag().code() ? facts.name() : ColumnType.UNKNOWN_NAME;
    }

    /**
     * Derived from the storage NULL: the low {@link #getWidth()} bytes of the NULL word,
     * sign-extended, for a value up to 8 bytes wide; 0 for wider values, which no long slot can
     * hold.
     */
    @Override
    public long getNullAsLong() {
        final long nullWord = facts.nullWord();
        return switch (facts.movement()) {
            case W1 -> (byte) nullWord;
            case W2 -> (short) nullWord;
            case W4 -> (int) nullWord;
            case W8 -> nullWord;
            case W16, W32, VAR -> 0L;
        };
    }

    @Override
    public final ConstantFunction getNullConstant(int columnType) {
        return nullConstantSource.nullConstant(columnType);
    }

    /**
     * The NULL word, whatever the long index. A type whose NULL longs differ answers itself.
     */
    @Override
    public long getNullLong(int longIndex) {
        return facts.nullWord();
    }

    @Override
    public final NullPolicy getNullPolicy() {
        return facts.nullPolicy();
    }

    @Override
    public final int getPgArrayOid() {
        return facts.pgArrayOid();
    }

    @Override
    public final int getPgOid() {
        return facts.pgOid();
    }

    /**
     * log2 of the width in bytes, as {@link ColumnType#pow2SizeOf(int)} reports it.
     */
    public final int getPow2Width() {
        return facts.movement().pow2Size();
    }

    @Override
    public final int getRelationBits() {
        return facts.relationBits();
    }

    @Override
    public final RelationKind getRelationKind() {
        return facts.relationKind();
    }

    @Override
    public final char getSignatureChar() {
        return facts.signatureChar();
    }

    @Override
    public final ColumnTypeTag getTag() {
        return facts.tag();
    }

    @Override
    public final TypeConstant getTypeConstant(int columnType) {
        return typeConstantSource.typeConstant(columnType);
    }

    /**
     * Width of one value in bytes, as {@link ColumnType#sizeOf(int)} reports it.
     */
    public final int getWidth() {
        return facts.movement().size();
    }

    @Override
    public final WireKind getWireKind() {
        return facts.wireKind();
    }

    @Override
    public final boolean isCastTarget(boolean isFromNull) {
        return switch (facts.castTarget()) {
            case ALWAYS -> true;
            case FROM_NULL_ONLY -> isFromNull;
            case NEVER -> false;
        };
    }

    @Override
    public final Function newColumnFunction(int columnIndex, int columnType) {
        return columnFunctionFactory.newColumnFunction(columnIndex, columnType);
    }

    @Override
    public final Runnable newNullAppender(MemoryA dataMem, MemoryA auxMem) {
        return nullAppenderFactory.newNullAppender(dataMem, auxMem);
    }

    @Override
    public final void setNull(long addr, long count) {
        nullFiller.setNull(addr, count);
    }

    /**
     * {@link TypeDriver#defineBindVariable(BindVariableService, int, int, int)} of a type.
     */
    @FunctionalInterface
    public interface BindVariableDefiner {
        int define(BindVariableService service, int index, int columnType, int position) throws SqlException;
    }

    /**
     * {@link TypeDriver#newColumnFunction(int, int)} of a type.
     */
    @FunctionalInterface
    public interface ColumnFunctionFactory {
        Function newColumnFunction(int columnIndex, int columnType);
    }

    /**
     * {@link TypeDriver#newNullAppender(MemoryA, MemoryA)} of a type: called once per column at
     * writer setup; the {@link Runnable} it returns runs per NULL row.
     */
    @FunctionalInterface
    public interface NullAppenderFactory {
        Runnable newNullAppender(MemoryA dataMem, MemoryA auxMem);
    }

    /**
     * {@link TypeDriver#getNullConstant(int)} of a type.
     */
    @FunctionalInterface
    public interface NullConstantSource {
        ConstantFunction nullConstant(int columnType);
    }

    /**
     * {@link TypeDriver#setNull(long, long)} of a type: one native fill per run of rows.
     */
    @FunctionalInterface
    public interface NullFiller {
        void setNull(long addr, long count);
    }

    /**
     * {@link TypeDriver#getTypeConstant(int)} of a type.
     */
    @FunctionalInterface
    public interface TypeConstantSource {
        TypeConstant typeConstant(int columnType);
    }
}
