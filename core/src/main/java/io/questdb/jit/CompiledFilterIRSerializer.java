/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
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
package io.questdb.jit;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.GeoHashes;
import io.questdb.cairo.SymbolMapReader;
import io.questdb.cairo.sql.*;
import io.questdb.cairo.vm.api.MemoryCARW;
import io.questdb.griffin.*;
import io.questdb.griffin.engine.functions.bind.CompiledFilterSymbolBindVariable;
import io.questdb.griffin.engine.functions.constants.ConstantFunction;
import io.questdb.griffin.engine.functions.constants.SymbolConstant;
import io.questdb.griffin.model.ExpressionNode;
import io.questdb.std.*;

import java.util.Arrays;

/**
 * Intermediate representation (IR) serializer for filters (think, WHERE clause)
 * to be used in SQL JIT compiler.
 */
public class CompiledFilterIRSerializer implements PostOrderTreeTraversalAlgo.Visitor, Mutable {

    // Column type codes
    static final byte MEM_I1 = 0;
    static final byte MEM_I2 = 1;
    static final byte MEM_I4 = 2;
    static final byte MEM_I8 = 3;
    static final byte MEM_F4 = 4;
    static final byte MEM_F8 = 5;
    // Constant type codes
    static final byte IMM_I1 = 6;
    static final byte IMM_I2 = 7;
    static final byte IMM_I4 = 8;
    static final byte IMM_I8 = 9;
    static final byte IMM_F4 = 10;
    static final byte IMM_F8 = 11;
    // Operator codes
    static final byte NEG = 12;               // -a
    static final byte NOT = 13;               // !a
    static final byte AND = 14;               // a && b
    static final byte OR = 15;                // a || b
    static final byte EQ = 16;                // a == b
    static final byte NE = 17;                // a != b
    static final byte LT = 18;                // a <  b
    static final byte LE = 19;                // a <= b
    static final byte GT = 20;                // a >  b
    static final byte GE = 21;                // a >= b
    static final byte ADD = 22;               // a + b
    static final byte SUB = 23;               // a - b
    static final byte MUL = 24;               // a * b
    static final byte DIV = 25;               // a / b
    static final byte MOD = 26;               // a % b
    static final byte JZ = 27;                // if a == 0 jp b
    static final byte JNZ = 28;               // if a != 0 jp b
    static final byte JP = 29;                // jp a
    static final byte RET = 30;               // ret a
    // Bind variables and deferred symbols
    static final byte VAR_I1 = 32;
    static final byte VAR_I2 = 33;
    static final byte VAR_I4 = 34;
    static final byte VAR_I8 = 35;
    static final byte VAR_F4 = 36;
    static final byte VAR_F8 = 37;
    // Temp code stub
    static final byte UNDEFINED_CODE = -1;

    private final PostOrderTreeTraversalAlgo traverseAlgo = new PostOrderTreeTraversalAlgo();
    private final PredicateContext predicateContext = new PredicateContext();
    // contains <memory_offset, constant_node> pairs for backfilling purposes
    private final LongObjHashMap<ExpressionNode> backfillNodes = new LongObjHashMap<>();
    private final LongObjHashMap.LongObjConsumer<ExpressionNode> backfillNodeConsumer = this::backfillNode;

    // internal flag used to forcefully enable scalar mode based on filter's contents
    private boolean forceScalarMode;

    private MemoryCARW memory;
    private SqlExecutionContext executionContext;
    private RecordMetadata metadata;
    private PageFrameCursor pageFrameCursor;
    private ObjList<Function> bindVarFunctions;

    public CompiledFilterIRSerializer of(
            MemoryCARW memory,
            SqlExecutionContext executionContext,
            RecordMetadata metadata,
            PageFrameCursor pageFrameCursor,
            ObjList<Function> bindVarFunctions
    ) {
        this.memory = memory;
        this.executionContext = executionContext;
        this.metadata = metadata;
        this.pageFrameCursor = pageFrameCursor;
        this.bindVarFunctions = bindVarFunctions;
        return this;
    }

    /**
     * Writes IR of the filter described by the given expression tree to memory.
     *
     * @param node   filter expression tree's root node.
     * @param scalar set use only scalar instruction set execution hint in the returned options.
     * @param debug  set enable debug flag in the returned options.
     * @return JIT compiler options stored in a single int in the following way:
     * <ul>
     * <li>1 LSB - debug flag</li>
     * <li>2-3 LSBs - filter's arithmetic type size (widest type size): 0 - 1B, 1 - 2B, 2 - 4B, 3 - 8B</li>
     * <li>4-5 LSBs - filter's execution hint: 0 - scalar, 1 - single size (SIMD-friendly), 2 - mixed sizes</li>
     * <li>6 LSB - flag to include null checks for column values into compiled filter</li>
     * </ul>
     * <p>
     * Examples:
     * <ul>
     * <li>00000000 00000000 00000000 00010100 - 4B, mixed types, debug off, null checks disabled</li>
     * <li>00000000 00000000 00000000 00100111 - 8B, scalar, debug on, null checks enabled</li>
     * </ul>
     * @throws SqlException thrown when IR serialization failed.
     */
    public int serialize(ExpressionNode node, boolean scalar, boolean debug, boolean nullChecks) throws SqlException {
        traverseAlgo.traverse(node, this);
        memory.putByte(RET);

        TypesObserver typesObserver = predicateContext.globalTypesObserver;
        int options = debug ? 1 : 0;
        int typeSize = typesObserver.maxSize();
        if (typeSize > 0) {
            // typeSize is 2^n, so number of trailing zeros is equal to log2
            int log2 = Integer.numberOfTrailingZeros(typeSize);
            options = options | (log2 << 1);
        }
        if (!scalar && !forceScalarMode) {
            int executionHint = typesObserver.hasMixedSizes() ? 2 : 1;
            options = options | (executionHint << 3);
        }

        options = options | ( (nullChecks ? 1 : 0) << 5);

        return options;
    }

    @Override
    public void clear() {
        memory = null;
        metadata = null;
        pageFrameCursor = null;
        forceScalarMode = false;
        predicateContext.clear();
        backfillNodes.clear();
    }

    @Override
    public boolean descend(ExpressionNode node) throws SqlException {
        if (node.token == null) {
            throw SqlException.position(node.position)
                    .put("non-null token expected: ")
                    .put(node.token);
        }

        // Check if we're at the start of an arithmetic expression
        predicateContext.onNodeDescended(node);

        // Look ahead for negative const
        if (node.type == ExpressionNode.OPERATION && node.paramCount == 1 && Chars.equals(node.token, "-")) {
            ExpressionNode nextNode = node.lhs != null ? node.lhs : node.rhs;
            if (nextNode != null && nextNode.paramCount == 0 && nextNode.type == ExpressionNode.CONSTANT) {
                // Store negation node for later backfilling
                serializeConstantStub(node);
                return false;
            }
        }

        return true;
    }

    @Override
    public void visit(ExpressionNode node) throws SqlException {
        int argCount = node.paramCount;
        if (argCount == 0) {
            switch (node.type) {
                case ExpressionNode.LITERAL:
                    serializeColumn(node.position, node.token);
                    break;
                case ExpressionNode.BIND_VARIABLE:
                    serializeBindVariable(node);
                    break;
                case ExpressionNode.CONSTANT:
                    // Write stub values to be backfilled later
                    serializeConstantStub(node);
                    break;
                default:
                    throw SqlException.position(node.position)
                            .put("unsupported token: ")
                            .put(node.token);
            }
        } else {
            serializeOperator(node.position, node.token, argCount);
        }

        boolean predicateLeft = predicateContext.onNodeVisited(node);

        if (predicateLeft) {
            // We're out of a predicate

            // Force scalar mode if the predicate had byte or short arithmetic operations.
            // That's because SIMD mode uses byte/short-sized overflows for arithmetic
            // calculations instead of implicit upcast to int done by *.sql.Function classes.
            forceScalarMode |=
                    predicateContext.hasArithmeticOperations && predicateContext.localTypesObserver.maxSize() <= 2;

            // Then backfill constants and symbol bind variables and clean up
            try {
                backfillNodes.forEach(backfillNodeConsumer);
                backfillNodes.clear();
            } catch (SqlWrapperException e) {
                throw e.wrappedException;
            }
        }
    }

    private void backfillNode(long key, ExpressionNode value) {
        try {
            switch (value.type) {
                case ExpressionNode.CONSTANT:
                case ExpressionNode.OPERATION: // constant negation case
                    backfillConstant(key, value);
                    break;
                case ExpressionNode.BIND_VARIABLE:
                    backfillSymbolBindVariable(key, value);
                    break;
                default:
                    throw SqlException.position(value.position)
                            .put("unexpected backfill token: ")
                            .put(value.token);
            }
        } catch (SqlException e) {
            throw new SqlWrapperException(e);
        }
    }

    private void serializeColumn(int position, final CharSequence token) throws SqlException {
        if (!predicateContext.isActive()) {
            throw SqlException.position(position)
                    .put("non-boolean column outside of predicate: ")
                    .put(token);
        }

        final int index = metadata.getColumnIndexQuiet(token);
        if (index == -1) {
            throw SqlException.invalidColumn(position, token);
        }

        final int columnType = metadata.getColumnType(index);
        final int columnTypeTag = ColumnType.tagOf(columnType);
        byte typeCode = columnTypeCode(columnTypeTag);
        if (typeCode == UNDEFINED_CODE) {
            throw SqlException.position(position)
                    .put("unsupported column type: ")
                    .put(ColumnType.nameOf(columnTypeTag));
        }

        // In case of a top level boolean column, expand it to "boolean_column = true" expression.
        if (predicateContext.singleBooleanColumn && columnTypeTag == ColumnType.BOOLEAN) {
            // "true" constant
            memory.putByte(IMM_I1);
            memory.putLong(1);
            // column
            memory.putByte(typeCode);
            memory.putLong(index);
            // =
            memory.putByte(EQ);
            return;
        }

        memory.putByte(typeCode);
        memory.putLong(index);
    }

    private void serializeBindVariable(final ExpressionNode node) throws SqlException {
        if (!predicateContext.isActive()) {
            throw SqlException.position(node.position)
                    .put("bind variable outside of predicate: ")
                    .put(node.token);
        }

        Function varFunction = getBindVariableFunction(node.position, node.token);

        final int columnType = varFunction.getType();
        // Treat string bind variable to be of symbol type
        if (columnType == ColumnType.STRING) {
            // We're going to backfill this variable later since we may
            // not have symbol column index at this point
            long offset = memory.getAppendOffset();
            backfillNodes.put(offset, node);
            memory.putByte(UNDEFINED_CODE);
            memory.putLong(0);
            return;
        }

        final int columnTypeTag = ColumnType.tagOf(columnType);
        byte typeCode = bindVariableTypeCode(columnTypeTag);
        if (typeCode == UNDEFINED_CODE) {
            throw SqlException.position(node.position)
                    .put("unsupported bind variable type: ")
                    .put(ColumnType.nameOf(columnTypeTag));
        }

        bindVarFunctions.add(varFunction);
        int index = bindVarFunctions.size() - 1;

        memory.putByte(typeCode);
        memory.putLong(index);
    }

    private void backfillSymbolBindVariable(long offset, final ExpressionNode node) throws SqlException {
        if (predicateContext.symbolColumnIndex == -1) {
            throw SqlException.position(node.position)
                    .put("symbol column index is missing for bind variable: ")
                    .put(node.token);
        }

        Function varFunction = getBindVariableFunction(node.position, node.token);

        final int columnType = varFunction.getType();
        // Treat string bind variable to be of symbol type
        if (columnType != ColumnType.STRING) {
            throw SqlException.position(node.position)
                    .put("unexpected symbol bind variable type: ")
                    .put(ColumnType.nameOf(columnType));
        }

        byte typeCode = bindVariableTypeCode(columnType);
        if (typeCode == UNDEFINED_CODE) {
            throw SqlException.position(node.position)
                    .put("unsupported bind variable type: ")
                    .put(ColumnType.nameOf(columnType));
        }

        bindVarFunctions.add(new CompiledFilterSymbolBindVariable(varFunction, predicateContext.symbolColumnIndex));
        int index = bindVarFunctions.size() - 1;

        memory.putByte(offset, typeCode);
        memory.putLong(offset + Byte.BYTES, index);
    }

    private Function getBindVariableFunction(int position, CharSequence token) throws SqlException {
        Function varFunction;

        if (token.charAt(0) == ':') {
            // name bind variable case
            varFunction = getBindVariableService().getFunction(token);
        } else {
            // indexed bind variable case
            try {
                final int variableIndex = Numbers.parseInt(token, 1, token.length());
                if (variableIndex < 1) {
                    throw SqlException.$(position, "invalid bind variable index [value=").put(variableIndex).put(']');
                }
                varFunction = getBindVariableService().getFunction(variableIndex - 1);
            } catch (NumericException e) {
                throw SqlException.$(position, "invalid bind variable index [value=").put(token).put(']');
            }
        }

        if (varFunction == null) {
            throw SqlException.position(position).put("failed to find function for bind variable: ").put(token);
        }

        return varFunction;
    }

    private BindVariableService getBindVariableService() throws SqlException {
        final BindVariableService bindVariableService = executionContext.getBindVariableService();
        if (bindVariableService == null) {
            throw SqlException.$(0, "bind variable service is not provided");
        }
        return bindVariableService;
    }

    private void serializeConstantStub(final ExpressionNode node) throws SqlException {
        if (!predicateContext.isActive()) {
            throw SqlException.position(node.position)
                    .put("constant outside of predicate: ")
                    .put(node.token);
        }

        long offset = memory.getAppendOffset();
        backfillNodes.put(offset, node);
        memory.putByte(UNDEFINED_CODE);
        memory.putLong(0);
    }

    private void backfillConstant(long offset, final ExpressionNode node) throws SqlException {
        int position = node.position;
        CharSequence token = node.token;
        boolean negate = false;
        // Check for negation case
        if (node.type == ExpressionNode.OPERATION) {
            ExpressionNode nextNode = node.lhs != null ? node.lhs : node.rhs;
            if (nextNode != null) {
                position = nextNode.position;
                token = nextNode.token;
                negate = true;
            }
        }

        serializeConstant(offset, position, token, negate);
    }

    private void serializeConstant(long offset, int position, final CharSequence token, boolean negated) throws SqlException {
        final int len = token.length();
        final byte typeCode = predicateContext.localTypesObserver.constantTypeCode();
        if (typeCode == UNDEFINED_CODE) {
            throw SqlException.position(position).put("all constants expression: ").put(token);
        }

        if (SqlKeywords.isNullKeyword(token)) {
            boolean geoHashExpression = PredicateType.GEO_HASH == predicateContext.type;
            serializeNull(offset, position, typeCode, geoHashExpression);
            return;
        }

        if (PredicateType.SYMBOL == predicateContext.type) {
            serializeSymbolConstant(offset, position, token);
            return;
        }

        if (Chars.isQuoted(token)) {
            if (PredicateType.CHAR != predicateContext.type) {
                throw SqlException.position(position).put("char constant in non-char expression: ").put(token);
            }
            if (len == 3) {
                // this is 'x' - char
                memory.putByte(offset, IMM_I2);
                memory.putLong(offset + Byte.BYTES, token.charAt(1));
                return;
            }
            throw SqlException.position(position).put("unsupported string constant: ").put(token);
        }

        if (SqlKeywords.isTrueKeyword(token)) {
            if (PredicateType.BOOLEAN != predicateContext.type) {
                throw SqlException.position(position).put("boolean constant in non-boolean expression: ").put(token);
            }
            memory.putByte(offset, IMM_I1);
            memory.putLong(offset + Byte.BYTES, 1);
            return;
        }

        if (SqlKeywords.isFalseKeyword(token)) {
            if (PredicateType.BOOLEAN != predicateContext.type) {
                throw SqlException.position(position).put("boolean constant in non-boolean expression: ").put(token);
            }
            memory.putByte(offset, IMM_I1);
            memory.putLong(offset + Byte.BYTES, 0);
            return;
        }

        if (len > 1 && token.charAt(0) == '#') {
            if (PredicateType.GEO_HASH != predicateContext.type) {
                throw SqlException.position(position).put("geo hash constant in non-geo hash expression: ").put(token);
            }
            ConstantFunction geoConstant = GeoHashUtil.parseGeoHashConstant(position, token, len);
            if (geoConstant != null) {
                serializeGeoHash(offset, position, geoConstant, typeCode);
                return;
            }
        }

        if (PredicateType.NUMERIC != predicateContext.type) {
            throw SqlException.position(position).put("numeric constant in non-numeric expression: ").put(token);
        }
        if (predicateContext.localTypesObserver.hasMixedSizes()) {
            serializeUntypedNumber(offset, position, token, negated);
        } else {
            serializeNumber(offset, position, token, typeCode, negated);
        }
    }

    private void serializeNull(long offset, int position, byte typeCode, boolean geoHashExpression) throws SqlException {
        memory.putByte(offset, typeCode);
        switch (typeCode) {
            case IMM_I1:
                if (!geoHashExpression) {
                    throw SqlException.position(position).put("byte type is not nullable");
                }
                memory.putLong(offset + Byte.BYTES, GeoHashes.BYTE_NULL);
                break;
            case IMM_I2:
                if (!geoHashExpression) {
                    throw SqlException.position(position).put("short type is not nullable");
                }
                memory.putLong(offset + Byte.BYTES, GeoHashes.SHORT_NULL);
                break;
            case IMM_I4:
                memory.putLong(offset + Byte.BYTES, geoHashExpression ? GeoHashes.INT_NULL : Numbers.INT_NaN);
                break;
            case IMM_I8:
                memory.putLong(offset + Byte.BYTES, geoHashExpression ? GeoHashes.NULL : Numbers.LONG_NaN);
                break;
            case IMM_F4:
                memory.putDouble(offset + Byte.BYTES, Float.NaN);
                break;
            case IMM_F8:
                memory.putDouble(offset + Byte.BYTES, Double.NaN);
                break;
            default:
                throw SqlException.position(position).put("unexpected null type: ").put(typeCode);
        }
    }

    private void serializeSymbolConstant(long offset, int position, final CharSequence token) throws SqlException {
        final int len = token.length();
        CharSequence symbol = token;
        if (Chars.isQuoted(token)) {
            if (len < 3) {
                throw SqlException.position(position).put("unsupported symbol constant: ").put(token);
            }
            symbol = symbol.subSequence(1, len - 1);
        }

        if (predicateContext.symbolMapReader == null || predicateContext.symbolColumnIndex == -1) {
            throw SqlException.position(position).put("reader or column index is missing for symbol constant: ").put(token);
        }

        final int key = predicateContext.symbolMapReader.keyOf(symbol);
        if (key != SymbolTable.VALUE_NOT_FOUND) {
            // Known symbol constant case
            memory.putByte(offset, IMM_I4);
            memory.putLong(offset + Byte.BYTES, key);
            return;
        }

        // Unknown symbol constant case. Create a fake bind variable function to handle it.
        final SymbolConstant function = SymbolConstant.newInstance(symbol);
        bindVarFunctions.add(new CompiledFilterSymbolBindVariable(function, predicateContext.symbolColumnIndex));
        int index = bindVarFunctions.size() - 1;

        byte typeCode = bindVariableTypeCode(ColumnType.STRING);
        memory.putByte(offset, typeCode);
        memory.putLong(offset + Byte.BYTES, index);
    }

    private void serializeGeoHash(long offset, int position, final ConstantFunction geoHashConstant, byte typeCode) throws SqlException {
        memory.putByte(offset, typeCode);
        try {
            switch (typeCode) {
                case IMM_I1:
                    memory.putLong(offset + Byte.BYTES, geoHashConstant.getGeoByte(null));
                    break;
                case IMM_I2:
                    memory.putLong(offset + Byte.BYTES, geoHashConstant.getGeoShort(null));
                    break;
                case IMM_I4:
                    memory.putLong(offset + Byte.BYTES, geoHashConstant.getGeoInt(null));
                    break;
                case IMM_I8:
                    memory.putLong(offset + Byte.BYTES, geoHashConstant.getGeoLong(null));
                    break;
                default:
                    throw SqlException.position(position).put("unexpected type code for geo hash: ").put(typeCode);
            }
        } catch (UnsupportedOperationException e) {
            throw SqlException.position(position).put("unexpected type for geo hash: ").put(typeCode);
        }
    }

    private void serializeNumber(long offset, int position, final CharSequence token, byte typeCode, boolean negated) throws SqlException {
        long sign = negated ? -1 : 1;
        try {
            switch (typeCode) {
                case IMM_I1:
                    final byte b = (byte) Numbers.parseInt(token);
                    memory.putByte(offset, IMM_I1);
                    memory.putLong(offset + Byte.BYTES, sign * b);
                    break;
                case IMM_I2:
                    final short s = (short) Numbers.parseInt(token);
                    memory.putByte(offset, IMM_I2);
                    memory.putLong(offset + Byte.BYTES, sign * s);
                    break;
                case IMM_I4:
                case IMM_F4:
                    try {
                        final int i = Numbers.parseInt(token);
                        memory.putByte(offset, IMM_I4);
                        memory.putLong(offset + Byte.BYTES, sign * i);
                    } catch (NumericException e) {
                        final float fi = Numbers.parseFloat(token);
                        memory.putByte(offset, IMM_F4);
                        memory.putDouble(offset + Byte.BYTES, sign * fi);
                    }
                    break;
                case IMM_I8:
                case IMM_F8:
                    try {
                        final long l = Numbers.parseLong(token);
                        memory.putByte(offset, IMM_I8);
                        memory.putLong(offset + Byte.BYTES, sign * l);
                    } catch (NumericException e) {
                        final double dl = Numbers.parseDouble(token);
                        memory.putByte(offset, IMM_F8);
                        memory.putDouble(offset + Byte.BYTES, sign * dl);
                    }
                    break;
                default:
                    throw SqlException.position(position)
                            .put("unexpected non-numeric constant: ").put(token)
                            .put(", expected type: ").put(typeCode);
            }
        } catch (NumericException e) {
            throw SqlException.position(position)
                    .put("could not parse constant: ").put(token)
                    .put(", expected type: ").put(typeCode);
        }
    }

    private void serializeUntypedNumber(long offset, int position, final CharSequence token, boolean negated) throws SqlException {
        long sign = negated ? -1 : 1;

        try {
            final int i = Numbers.parseInt(token);
            memory.putByte(offset, IMM_I4);
            memory.putLong(offset + Byte.BYTES, sign * i);
            return;
        } catch (NumericException ignore) {
        }

        try {
            final long l = Numbers.parseLong(token);
            memory.putByte(offset, IMM_I8);
            memory.putLong(offset + Byte.BYTES, sign * l);
            return;
        } catch (NumericException ignore) {
        }

        try {
            final double d = Numbers.parseDouble(token);
            memory.putByte(offset, IMM_F8);
            memory.putDouble(offset + Byte.BYTES, sign * d);
            return;
        } catch (NumericException ignore) {
        }

        try {
            final float f = Numbers.parseFloat(token);
            memory.putByte(offset, IMM_F4);
            memory.putDouble(offset + Byte.BYTES, sign * f);
            return;
        } catch (NumericException ignore) {
        }

        throw SqlException.position(position).put("unexpected non-numeric constant: ").put(token);
    }

    private void serializeOperator(int position, final CharSequence token, int argCount) throws SqlException {
        if (SqlKeywords.isNotKeyword(token)) {
            memory.putByte(NOT);
            return;
        }
        if (SqlKeywords.isAndKeyword(token)) {
            memory.putByte(AND);
            return;
        }
        if (SqlKeywords.isOrKeyword(token)) {
            memory.putByte(OR);
            return;
        }
        if (Chars.equals(token, "=")) {
            memory.putByte(EQ);
            return;
        }
        if (Chars.equals(token, "<>") || Chars.equals(token, "!=")) {
            memory.putByte(NE);
            return;
        }
        if (Chars.equals(token, "<")) {
            memory.putByte(LT);
            return;
        }
        if (Chars.equals(token, "<=")) {
            memory.putByte(LE);
            return;
        }
        if (Chars.equals(token, ">")) {
            memory.putByte(GT);
            return;
        }
        if (Chars.equals(token, ">=")) {
            memory.putByte(GE);
            return;
        }
        if (Chars.equals(token, "+")) {
            if (argCount == 2) {
                memory.putByte(ADD);
            } // ignore unary
            return;
        }
        if (Chars.equals(token, "-")) {
            if (argCount == 2) {
                memory.putByte(SUB);
            } else if (argCount == 1) {
                memory.putByte(NEG);
            }
            return;
        }
        if (Chars.equals(token, "*")) {
            memory.putByte(MUL);
            return;
        }
        if (Chars.equals(token, "/")) {
            memory.putByte(DIV);
            return;
        }
        throw SqlException.position(position).put("invalid operator: ").put(token);
    }

    private static byte columnTypeCode(int columnTypeTag) {
        switch (columnTypeTag) {
            case ColumnType.BOOLEAN:
            case ColumnType.BYTE:
            case ColumnType.GEOBYTE:
                return MEM_I1;
            case ColumnType.SHORT:
            case ColumnType.GEOSHORT:
            case ColumnType.CHAR:
                return MEM_I2;
            case ColumnType.INT:
            case ColumnType.GEOINT:
            case ColumnType.SYMBOL:
                return MEM_I4;
            case ColumnType.FLOAT:
                return MEM_F4;
            case ColumnType.LONG:
            case ColumnType.GEOLONG:
            case ColumnType.DATE:
            case ColumnType.TIMESTAMP:
                return MEM_I8;
            case ColumnType.DOUBLE:
                return MEM_F8;
            default:
                return UNDEFINED_CODE;
        }
    }

    private static byte bindVariableTypeCode(int columnTypeTag) {
        switch (columnTypeTag) {
            case ColumnType.BOOLEAN:
            case ColumnType.BYTE:
            case ColumnType.GEOBYTE:
                return VAR_I1;
            case ColumnType.SHORT:
            case ColumnType.GEOSHORT:
            case ColumnType.CHAR:
                return VAR_I2;
            case ColumnType.INT:
            case ColumnType.GEOINT:
            case ColumnType.STRING: // symbol variables are represented with string type
                return VAR_I4;
            case ColumnType.FLOAT:
                return VAR_F4;
            case ColumnType.LONG:
            case ColumnType.GEOLONG:
            case ColumnType.DATE:
            case ColumnType.TIMESTAMP:
                return VAR_I8;
            case ColumnType.DOUBLE:
                return VAR_F8;
            default:
                return UNDEFINED_CODE;
        }
    }

    private static boolean isTopLevelOperation(ExpressionNode node) {
        final CharSequence token = node.token;
        if (SqlKeywords.isNotKeyword(token)) {
            return true;
        }
        if (node.paramCount < 2) {
            return false;
        }
        if (Chars.equals(token, "=")) {
            return true;
        }
        if (Chars.equals(token, "<>") || Chars.equals(token, "!=")) {
            return true;
        }
        if (Chars.equals(token, "<")) {
            return true;
        }
        if (Chars.equals(token, "<=")) {
            return true;
        }
        if (Chars.equals(token, ">")) {
            return true;
        }
        return Chars.equals(token, ">=");
    }

    private boolean isTopLevelBooleanColumn(ExpressionNode node) {
        if (node.type == ExpressionNode.LITERAL && isBooleanColumn(node)) {
            return true;
        }
        // Lookahead for "not boolean_column" case
        final CharSequence token = node.token;
        if (SqlKeywords.isNotKeyword(token)) {
            ExpressionNode columnNode = node.lhs != null ? node.lhs : node.rhs;
            return columnNode != null && isBooleanColumn(columnNode);
        }
        return false;
    }

    private boolean isBooleanColumn(ExpressionNode node) {
        if (node.type != ExpressionNode.LITERAL) {
            return false;
        }
        int index = metadata.getColumnIndexQuiet(node.token);
        if (index == -1) {
            return false;
        }
        final int columnType = metadata.getColumnType(index);
        final int columnTypeTag = ColumnType.tagOf(columnType);
        return columnTypeTag == ColumnType.BOOLEAN;
    }

    private static boolean isArithmeticOperation(ExpressionNode node) {
        final CharSequence token = node.token;
        if (node.paramCount < 2) {
            return false;
        }
        if (Chars.equals(token, "+")) {
            return true;
        }
        if (Chars.equals(token, "-")) {
            return true;
        }
        if (Chars.equals(token, "*")) {
            return true;
        }
        return Chars.equals(token, "/");
    }

    /**
     * Helper class that tracks types and arithmetic operations in a predicate.
     * <p>
     * A "predicate" stands for any arithmetical expression or single boolean
     * column expression present in the filter. Predicates are combined with
     * each other via binary boolean operators (and, or).
     * <p>
     * For example, we consider the below filter:
     * <pre>
     * long_col - 42 > 0 and (not bool_col1 or bool_col2)
     * </pre>
     * to contain three predicates:
     * <pre>
     * long_col - 42 > 0
     * not bool_col1
     * bool_col2
     * </pre>
     */
    private class PredicateContext implements Mutable {

        private ExpressionNode rootNode;
        PredicateType type;
        SymbolMapReader symbolMapReader; // used for known symbol constant lookups
        int symbolColumnIndex; // used for symbol deferred constants and bind variables
        boolean singleBooleanColumn;
        boolean hasArithmeticOperations;

        final TypesObserver localTypesObserver = new TypesObserver();
        final TypesObserver globalTypesObserver = new TypesObserver();

        @Override
        public void clear() {
            reset();
            globalTypesObserver.clear();
        }

        private void reset() {
            rootNode = null;
            type = null;
            symbolMapReader = null;
            symbolColumnIndex = -1;
            singleBooleanColumn = false;
            hasArithmeticOperations = false;
            localTypesObserver.clear();
        }

        public boolean isActive() {
            return rootNode != null;
        }

        public void onNodeDescended(final ExpressionNode node) {
            if (rootNode == null) {
                boolean topLevelOperation = isTopLevelOperation(node);
                boolean topLevelBooleanColumn = isTopLevelBooleanColumn(node);
                if (topLevelOperation || topLevelBooleanColumn) {
                    // We entered a predicate.
                    reset();
                    rootNode = node;
                }
                if (topLevelBooleanColumn) {
                    type = PredicateType.BOOLEAN;
                    singleBooleanColumn = true;
                }
            }
        }

        public boolean onNodeVisited(final ExpressionNode node) throws SqlException {
            boolean predicateLeft = false;
            if (node == rootNode) {
                // We left the predicate.
                rootNode = null;
                predicateLeft = true;
            }

            switch (node.type) {
                case ExpressionNode.LITERAL:
                    handleColumn(node);
                    break;
                case ExpressionNode.BIND_VARIABLE:
                    handleBindVariable(node);
                    break;
                case ExpressionNode.OPERATION:
                    handleOperation(node);
                    break;
            }

            return predicateLeft;
        }

        private void handleColumn(ExpressionNode node) throws SqlException {
            final int columnIndex = metadata.getColumnIndexQuiet(node.token);
            if (columnIndex == -1) {
                throw SqlException.invalidColumn(node.position, node.token);
            }
            final int columnType = metadata.getColumnType(columnIndex);
            final int columnTypeTag = ColumnType.tagOf(columnType);
            if (columnTypeTag == ColumnType.SYMBOL) {
                symbolMapReader = pageFrameCursor.getSymbolMapReader(columnIndex);
                symbolColumnIndex = columnIndex;
            }

            updateType(node.position, columnTypeTag);

            byte code = columnTypeCode(columnTypeTag);
            localTypesObserver.observe(code);
            globalTypesObserver.observe(code);
        }

        private void handleBindVariable(ExpressionNode node) throws SqlException {
            Function varFunction = getBindVariableFunction(node.position, node.token);
            // We treat bind variables as columns here for the sake of simplicity
            final int columnType = varFunction.getType();
            int columnTypeTag = ColumnType.tagOf(columnType);
            // Treat string bind variable to be of symbol type
            if (columnTypeTag == ColumnType.STRING) {
                columnTypeTag = ColumnType.SYMBOL;
            }

            updateType(node.position, columnTypeTag);

            byte code = columnTypeCode(columnTypeTag);
            localTypesObserver.observe(code);
            globalTypesObserver.observe(code);
        }

        private void handleOperation(ExpressionNode node) {
            hasArithmeticOperations |= isArithmeticOperation(node);
        }

        private void updateType(int position, int columnTypeTag) throws SqlException {
            switch (columnTypeTag) {
                case ColumnType.BOOLEAN:
                    if (type != null && type != PredicateType.BOOLEAN) {
                        throw SqlException.position(position)
                                .put("non-boolean column in boolean expression: ")
                                .put(ColumnType.nameOf(columnTypeTag));
                    }
                    type = PredicateType.BOOLEAN;
                    break;
                case ColumnType.GEOBYTE:
                case ColumnType.GEOSHORT:
                case ColumnType.GEOINT:
                case ColumnType.GEOLONG:
                    if (type != null && type != PredicateType.GEO_HASH) {
                        throw SqlException.position(position)
                                .put("non-geohash column in geohash expression: ")
                                .put(ColumnType.nameOf(columnTypeTag));
                    }
                    type = PredicateType.GEO_HASH;
                    break;
                case ColumnType.CHAR:
                    if (type != null && type != PredicateType.CHAR) {
                        throw SqlException.position(position)
                                .put("non-char column in char expression: ")
                                .put(ColumnType.nameOf(columnTypeTag));
                    }
                    type = PredicateType.CHAR;
                    break;
                case ColumnType.SYMBOL:
                    if (type != null && type != PredicateType.SYMBOL) {
                        throw SqlException.position(position)
                                .put("non-symbol column in symbol expression: ")
                                .put(ColumnType.nameOf(columnTypeTag));
                    }
                    type = PredicateType.SYMBOL;
                    break;
                default:
                    if (type != null && type != PredicateType.NUMERIC) {
                        throw SqlException.position(position)
                                .put("non-numeric column in numeric expression: ")
                                .put(ColumnType.nameOf(columnTypeTag));
                    }
                    type = PredicateType.NUMERIC;
                    break;
            }
        }
    }

    /**
     * Helper class for accumulating column and bind variable types information.
     */
    private static class TypesObserver implements Mutable {

        private static final int I1_INDEX = 0;
        private static final int I2_INDEX = 1;
        private static final int I4_INDEX = 2;
        private static final int F4_INDEX = 3;
        private static final int I8_INDEX = 4;
        private static final int F8_INDEX = 5;
        private static final int TYPES_COUNT = F8_INDEX + 1;

        private final byte[] sizes = new byte[TYPES_COUNT];

        public void observe(byte code) {
            switch (code) {
                case MEM_I1:
                    sizes[I1_INDEX] = 1;
                    break;
                case MEM_I2:
                    sizes[I2_INDEX] = 2;
                    break;
                case MEM_I4:
                    sizes[I4_INDEX] = 4;
                    break;
                case MEM_F4:
                    sizes[F4_INDEX] = 4;
                    break;
                case MEM_I8:
                    sizes[I8_INDEX] = 8;
                    break;
                case MEM_F8:
                    sizes[F8_INDEX] = 8;
                    break;
            }
        }

        /**
         * Returns the expected constant type calculated based on the "widest" observed column
         * or bind variable type. The result contains IMM_* value or UNDEFINED_CODE value.
         */
        public byte constantTypeCode() {
            for (int i = sizes.length - 1; i > -1; i--) {
                byte size = sizes[i];
                if (size > 0) {
                    // If floats are present, we need to cast longs to double.
                    if (i == I8_INDEX && sizes[F4_INDEX] > 0) {
                        return IMM_F8;
                    }
                    return indexToTypeCode(i);
                }
            }
            return UNDEFINED_CODE;
        }

        private byte indexToTypeCode(int index) {
            switch (index) {
                case I1_INDEX:
                    return IMM_I1;
                case I2_INDEX:
                    return IMM_I2;
                case I4_INDEX:
                    return IMM_I4;
                case F4_INDEX:
                    return IMM_F4;
                case I8_INDEX:
                    return IMM_I8;
                case F8_INDEX:
                    return IMM_F8;
            }
            return UNDEFINED_CODE;
        }

        /**
         * Returns size in bytes of the "widest" observed column or bind variable type.
         */
        public int maxSize() {
            for (int i = sizes.length - 1; i > -1; i--) {
                byte size = sizes[i];
                if (size > 0) {
                    return size;
                }
            }
            return 0;
        }

        public boolean hasMixedSizes() {
            byte prevSize = 0;
            for (byte size : sizes) {
                prevSize = prevSize == 0 ? size : prevSize;
                if (prevSize > 0) {
                    if (size > 0 && size != prevSize) {
                        return true;
                    }
                } else {
                    prevSize = size;
                }
            }
            return false;
        }

        @Override
        public void clear() {
            Arrays.fill(sizes, (byte) 0);
        }
    }

    private static class SqlWrapperException extends RuntimeException {

        final SqlException wrappedException;

        SqlWrapperException(SqlException wrappedException) {
            this.wrappedException = wrappedException;
        }
    }

    private enum PredicateType {
        NUMERIC, CHAR, SYMBOL, BOOLEAN, GEO_HASH
    }
}
