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
package io.questdb.jit;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.GeoHashes;
import io.questdb.cairo.MicrosTimestampDriver;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.VarcharTypeDriver;
import io.questdb.cairo.sql.BindVariableService;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.PageFrameCursor;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.StaticSymbolTable;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.cairo.vm.api.MemoryCARW;
import io.questdb.griffin.GeoHashUtil;
import io.questdb.griffin.PostOrderTreeTraversalAlgo;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.SqlKeywords;
import io.questdb.griffin.engine.functions.bind.CompiledFilterSymbolBindVariable;
import io.questdb.griffin.engine.functions.bind.IndexedParameterLinkFunction;
import io.questdb.griffin.engine.functions.bind.NamedParameterLinkFunction;
import io.questdb.griffin.engine.functions.constants.ConstantFunction;
import io.questdb.griffin.engine.functions.constants.SymbolConstant;
import io.questdb.griffin.model.ExpressionNode;
import io.questdb.griffin.model.IntervalUtils;
import io.questdb.std.Chars;
import io.questdb.std.GenericLexer;
import io.questdb.std.IntStack;
import io.questdb.std.LongList;
import io.questdb.std.LongObjHashMap;
import io.questdb.std.Mutable;
import io.questdb.std.Numbers;
import io.questdb.std.NumericException;
import io.questdb.std.ObjList;
import io.questdb.std.Uuid;
import io.questdb.std.str.StringSink;
import org.jetbrains.annotations.NotNull;

import java.util.Arrays;

/**
 * Intermediate representation (IR) serializer for filters (think, WHERE clause)
 * to be used in SQL JIT compiler.
 *
 * <pre>
 * IR instruction format:
 * | opcode | options | payload |
 * | int    | int     | long    |
 * </pre>
 */
public class CompiledFilterIRSerializer implements PostOrderTreeTraversalAlgo.Visitor, Mutable {
    public static final int ADD = 14; // a + b
    public static final int AND = 6;  // a && b
    public static final int AND_SC = 18; // short-circuit AND: if false, jump to label[payload] (0 = next_row)
    public static final int BEGIN_SC = 20; // create label at index payload
    public static final int BINARY_HEADER_TYPE = 8;
    public static final int DIV = 17; // a / b
    public static final int END_SC = 21;   // bind label at index payload
    public static final int EQ = 8;   // a == b
    public static final int F4_TYPE = 3;
    public static final int F8_TYPE = 5;
    public static final int GE = 13;  // a >= b
    public static final int GT = 12;  // a >  b
    public static final int I16_TYPE = 6;
    // Options:
    // Data types
    public static final int I1_TYPE = 0;
    public static final int I2_TYPE = 1;
    public static final int I4_TYPE = 2;
    public static final int I8_TYPE = 4;
    // Constants
    public static final int IMM = 1;
    public static final int LE = 11;  // a <= b
    public static final int LT = 10;  // a <  b
    // Columns
    public static final int MEM = 2;
    public static final int MUL = 16; // a * b
    public static final int NE = 9;   // a != b
    // Operator codes
    public static final int NEG = 4;  // -a
    public static final int NOT = 5;  // !a
    public static final int OR = 7;   // a || b
    public static final int OR_SC = 19;  // short-circuit OR: if true, jump to label[payload] (0 = next_row)
    // Opcodes:
    // Return code. Breaks the loop
    public static final int RET = 0;  // ret
    public static final int STRING_HEADER_TYPE = 7;
    public static final int SUB = 15;  // a - b
    // Bind variables and deferred symbols
    public static final int VAR = 3;
    public static final int VARCHAR_HEADER_TYPE = 9;
    // Stub value for opcodes and options
    static final int UNDEFINED_CODE = -1;
    private static final int EXEC_HINT_MIXED_SIZE_TYPE = 2;
    private static final int EXEC_HINT_SCALAR = 0;
    private static final int EXEC_HINT_SINGLE_SIZE_TYPE = 1;
    private static final int INSTRUCTION_SIZE = Integer.BYTES + Integer.BYTES + Long.BYTES + Long.BYTES;
    // Maximum number of labels supported by the backend (must match LabelArray::MAX_LABELS in x86.h)
    private static final int MAX_LABELS = 8;
    // Predicate priority for short-circuit evaluation
    private static final int PRIORITY_I16_EQ = 0;  // highest priority
    private static final int PRIORITY_I16_NEQ = 10; // lowest priority
    private static final int PRIORITY_I4_EQ = 2;
    private static final int PRIORITY_I4_NEQ = 8;
    private static final int PRIORITY_I8_EQ = 1;
    private static final int PRIORITY_I8_NEQ = 9;
    private static final int PRIORITY_OTHER = 5;
    private static final int PRIORITY_OTHER_EQ = 4;
    private static final int PRIORITY_OTHER_NEQ = 6;
    private static final int PRIORITY_SYM_EQ = 3;
    private static final int PRIORITY_SYM_NEQ = 7;
    // contains <memory_offset, constant_node> pairs for backfilling purposes
    private final LongObjHashMap<ExpressionNode> backfillNodes = new LongObjHashMap<>();
    // List to collect predicates from AND chains for reordering
    private final ObjList<ExpressionNode> collectedPredicates = new ObjList<>();
    private final PredicateContext predicateContext = new PredicateContext();
    private final ScalarModeDetector scalarModeDetector = new ScalarModeDetector();
    private final StringSink sink = new StringSink();
    private final PostOrderTreeTraversalAlgo traverseAlgo = new PostOrderTreeTraversalAlgo();
    private final IntStack typeStack = new IntStack();
    private ObjList<Function> bindVarFunctions;
    private final LongObjHashMap.LongObjConsumer<ExpressionNode> backfillNodeConsumer = this::backfillNode;
    private SqlExecutionContext executionContext;
    // internal flag used to forcefully enable scalar mode based on filter's contents
    private boolean forceScalarMode;
    private MemoryCARW memory;
    private RecordMetadata metadata;
    private PageFrameCursor pageFrameCursor;

    @Override
    public void clear() {
        memory = null;
        metadata = null;
        pageFrameCursor = null;
        forceScalarMode = false;
        predicateContext.clear();
        backfillNodes.clear();
        collectedPredicates.clear();
    }

    @Override
    public boolean descend(ExpressionNode node) throws SqlException {
        if (node.token == null) {
            throw SqlException.position(node.position)
                    .put("non-null token expected: ")
                    .put(node.token);
        }

        if (predicateContext.inOperationNode != null && !predicateContext.currentInSerialization) {
            return false;
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
     * @param node        filter expression tree's root node.
     * @param forceScalar set use only scalar instruction set execution hint in the returned options.
     * @param debug       set enable the debug flag in the returned options.
     * @param nullChecks  a flag for JIT, allowing or disallowing generation of null check
     * @return JIT compiler options stored in a single int in the following way:
     * <ul>
     * <li>1 LSB - debug flag</li>
     * <li>2-4 LSBs - filter's arithmetic type size (widest type size): 0 - 1B, 1 - 2B, 2 - 4B, 3 - 8B, 4 - 16B</li>
     * <li>5-6 LSBs - filter's execution hint: 0 - scalar, 1 - single size (SIMD-friendly), 2 - mixed sizes</li>
     * <li>7 LSB - flag to include null checks for column values in compiled filter</li>
     * </ul>
     * <p>
     * Examples:
     * <ul>
     * <li>00000000 00000000 00000000 00100100 - 4B, mixed types, debug off, null checks disabled</li>
     * <li>00000000 00000000 00000000 01000111 - 8B, scalar, debug on, null checks enabled</li>
     * </ul>
     * @throws SqlException thrown when IR serialization failed.
     */
    public int serialize(ExpressionNode node, boolean forceScalar, boolean debug, boolean nullChecks) throws SqlException {
        // Detect if scalar mode is guaranteed by checking for mixed column sizes.
        // Short-circuit optimizations (including IN() short-circuit) only work correctly
        // in scalar mode, so we only enable them when scalar mode is certain.
        boolean scalarModeDetected = forceScalar;
        if (!scalarModeDetected) {
            scalarModeDetector.clear();
            traverseAlgo.traverse(node, scalarModeDetector);
            scalarModeDetected = scalarModeDetector.hasMixedSizes();
        }

        // Check if we can apply predicate reordering for short-circuit evaluation
        if (scalarModeDetected) {
            if (isPureAndChain(node)) {
                collectedPredicates.clear();
                collectAndPredicates(node, collectedPredicates);
                if (collectedPredicates.size() > 1) {
                    sortPredicatesByPriority(collectedPredicates);
                    return serializePredicatesAndSc(collectedPredicates, forceScalar, debug, nullChecks);
                }
            } else if (isPureOrChain(node)) {
                collectedPredicates.clear();
                collectOrPredicates(node, collectedPredicates);
                if (collectedPredicates.size() > 1) {
                    sortPredicatesByInvertedPriority(collectedPredicates);
                    return serializePredicatesOrSc(collectedPredicates, forceScalar, debug, nullChecks);
                }
            }
        }

        // Not a pure AND/OR chain or SIMD mode possible, use normal serialization
        traverseAlgo.traverse(node, this);
        putOperator(RET);

        ensureOnlyVarSizeHeaderChecks();
        return getOptions(forceScalar, debug, nullChecks);
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
            serializeOperator(node.position, node.token, argCount, node.type);
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

    private static byte bindVariableTypeCode(int columnTypeTag) {
        return switch (columnTypeTag) {
            case ColumnType.BOOLEAN, ColumnType.BYTE, ColumnType.GEOBYTE -> I1_TYPE;
            case ColumnType.SHORT, ColumnType.GEOSHORT, ColumnType.CHAR -> I2_TYPE;
            case ColumnType.INT, ColumnType.IPv4, ColumnType.GEOINT,
                 ColumnType.STRING -> // symbol variables are represented with the string type
                    I4_TYPE;
            case ColumnType.FLOAT -> F4_TYPE;
            case ColumnType.LONG, ColumnType.GEOLONG, ColumnType.DATE, ColumnType.TIMESTAMP -> I8_TYPE;
            case ColumnType.DOUBLE -> F8_TYPE;
            case ColumnType.LONG128, ColumnType.UUID -> I16_TYPE;
            default -> UNDEFINED_CODE;
        };
    }

    private static int columnTypeCode(int columnTypeTag) {
        return switch (columnTypeTag) {
            case ColumnType.BOOLEAN, ColumnType.BYTE, ColumnType.GEOBYTE -> I1_TYPE;
            case ColumnType.SHORT, ColumnType.GEOSHORT, ColumnType.CHAR -> I2_TYPE;
            case ColumnType.INT, ColumnType.IPv4, ColumnType.GEOINT, ColumnType.SYMBOL -> I4_TYPE;
            case ColumnType.FLOAT -> F4_TYPE;
            case ColumnType.LONG, ColumnType.GEOLONG, ColumnType.DATE, ColumnType.TIMESTAMP -> I8_TYPE;
            case ColumnType.DOUBLE -> F8_TYPE;
            case ColumnType.LONG128, ColumnType.UUID -> I16_TYPE;
            case ColumnType.STRING -> STRING_HEADER_TYPE;
            case ColumnType.BINARY -> BINARY_HEADER_TYPE;
            case ColumnType.VARCHAR -> VARCHAR_HEADER_TYPE;
            default -> UNDEFINED_CODE;
        };
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

    private static boolean isGeoHash(int columnType) {
        return switch (ColumnType.tagOf(columnType)) {
            case ColumnType.GEOBYTE, ColumnType.GEOSHORT, ColumnType.GEOINT, ColumnType.GEOLONG -> true;
            default -> false;
        };
    }

    // Stands for PredicateType.NUMERIC
    private static boolean isNumeric(int columnTypeTag) {
        return switch (columnTypeTag) {
            case ColumnType.BYTE, ColumnType.SHORT, ColumnType.INT, ColumnType.LONG, ColumnType.FLOAT,
                 ColumnType.DOUBLE, ColumnType.LONG128 -> true;
            default -> false;
        };
    }

    private static boolean isTopLevelOperation(ExpressionNode node) {
        final CharSequence token = node.token;
        if (SqlKeywords.isNotKeyword(token)) {
            return true;
        }
        if (node.paramCount < 2) {
            return false;
        }
        if (SqlKeywords.isInKeyword(token)) {
            return true;
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

    private static boolean isVarSizeType(int type) {
        return type == STRING_HEADER_TYPE || type == BINARY_HEADER_TYPE || type == VARCHAR_HEADER_TYPE;
    }

    private void backfillConstant(long offset, final ExpressionNode node) throws SqlException {
        int position = node.position;
        CharSequence token = node.token;
        boolean negate = false;
        // Check for the negation case
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

    private void backfillSymbolBindVariable(long offset, final ExpressionNode node) throws SqlException {
        if (predicateContext.symbolColumnIndex == -1) {
            throw SqlException.position(node.position)
                    .put("symbol column index is missing for bind variable: ")
                    .put(node.token);
        }

        Function varFunction = getBindVariableFunction(node.position, node.token);

        final int columnType = varFunction.getType();
        // Treat string bind variable to be of the symbol type
        if (columnType != ColumnType.STRING) {
            throw SqlException.position(node.position)
                    .put("unexpected symbol bind variable type: ")
                    .put(ColumnType.nameOf(columnType));
        }

        int typeCode = bindVariableTypeCode(ColumnType.tagOf(columnType));
        if (typeCode == UNDEFINED_CODE) {
            throw SqlException.position(node.position)
                    .put("unsupported bind variable type: ")
                    .put(ColumnType.nameOf(columnType));
        }

        bindVarFunctions.add(new CompiledFilterSymbolBindVariable(varFunction, predicateContext.symbolColumnIndex));
        int index = bindVarFunctions.size() - 1;

        putOperand(offset, VAR, typeCode, index);
    }

    /**
     * Collects all predicates from an AND chain into the provided list.
     */
    private void collectAndPredicates(ExpressionNode node, ObjList<ExpressionNode> predicates) {
        if (node == null) {
            return;
        }
        if (node.type == ExpressionNode.OPERATION && SqlKeywords.isAndKeyword(node.token)) {
            collectAndPredicates(node.lhs, predicates);
            collectAndPredicates(node.rhs, predicates);
        } else {
            predicates.add(node);
        }
    }

    /**
     * Collects all predicates from an OR chain into the provided list.
     */
    private void collectOrPredicates(ExpressionNode node, ObjList<ExpressionNode> predicates) {
        if (node == null) {
            return;
        }
        if (node.type == ExpressionNode.OPERATION && SqlKeywords.isOrKeyword(node.token)) {
            collectOrPredicates(node.lhs, predicates);
            collectOrPredicates(node.rhs, predicates);
        } else {
            predicates.add(node);
        }
    }

    private void ensureOnlyVarSizeHeaderChecks() throws SqlException {
        typeStack.clear();
        for (long offset = 0; offset < memory.size(); offset += INSTRUCTION_SIZE) {
            int opCode = memory.getInt(offset);
            int typeCode = memory.getInt(offset + Integer.BYTES);
            switch (opCode) {
                case -1:
                    throw SqlException.$(0, "invalid opcode");
                case RET:
                    return;
                case VAR:
                case MEM:
                case IMM:
                    typeStack.push(typeCode);
                    break;
                case NEG:
                case NOT:
                    typeStack.pop();
                    typeStack.push(typeCode);
                    break;
                default:
                    // If none of the above, assume it's a binary operator
                    int lhsType = typeStack.pop();
                    int rhsType = typeStack.pop();
                    if ((lhsType != rhsType && isVarSizeType(lhsType) && isVarSizeType(rhsType))
                            || (lhsType == rhsType && isVarSizeType(lhsType))) {
                        throw SqlException.$(0, "var-size columns can only be used in NULL checks");
                    }
                    typeStack.push(typeCode);
            }
        }
    }

    /**
     * Finds the column type involved in an operation.
     * Returns UNDEFINED if no column is found.
     */
    private int findOperandColumnType(ExpressionNode node) {
        if (node == null) {
            return ColumnType.UNDEFINED;
        }
        if (node.type == ExpressionNode.LITERAL) {
            int index = metadata.getColumnIndexQuiet(node.token);
            if (index != -1) {
                return ColumnType.tagOf(metadata.getColumnType(index));
            }
        }
        // Recursively search children
        int leftType = findOperandColumnType(node.lhs);
        if (leftType != ColumnType.UNDEFINED) {
            return leftType;
        }
        return findOperandColumnType(node.rhs);
    }

    private Function getBindVariableFunction(int position, CharSequence token) throws SqlException {
        Function varFunction;

        if (token.charAt(0) == ':') {
            // name bind variable case
            Function bindFunction = getBindVariableService().getFunction(token);
            if (bindFunction == null) {
                throw SqlException.position(position).put("failed to find function for bind variable: ").put(token);
            }
            varFunction = new NamedParameterLinkFunction(Chars.toString(token), bindFunction.getType());
        } else {
            // indexed bind variable case
            try {
                final int variableIndex = Numbers.parseInt(token, 1, token.length());
                if (variableIndex < 1) {
                    throw SqlException.$(position, "invalid bind variable index [value=").put(variableIndex).put(']');
                }
                Function bindFunction = getBindVariableService().getFunction(variableIndex - 1);
                if (bindFunction == null) {
                    throw SqlException.position(position).put("failed to find function for bind variable: ").put(token);
                }
                varFunction = new IndexedParameterLinkFunction(
                        variableIndex - 1,
                        bindFunction.getType(),
                        position);

            } catch (NumericException e) {
                throw SqlException.$(position, "invalid bind variable index [value=").put(token).put(']');
            }
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

    private int getExecHint(boolean forceScalar) {
        final TypesObserver typesObserver = predicateContext.globalTypesObserver;
        if (!forceScalar && !forceScalarMode) {
            return typesObserver.hasMixedSizes() ? EXEC_HINT_MIXED_SIZE_TYPE : EXEC_HINT_SINGLE_SIZE_TYPE;
        }
        return EXEC_HINT_SCALAR;
    }

    private int getOptions(boolean forceScalar, boolean debug, boolean nullChecks) {
        final TypesObserver typesObserver = predicateContext.globalTypesObserver;
        int options = debug ? 1 : 0;
        final int typeSize = typesObserver.maxSize();
        if (typeSize > 0) {
            // typeSize is 2^n, so the number of trailing zeros is equal to log2
            final int log2 = Integer.numberOfTrailingZeros(typeSize);
            options = options | (log2 << 1);
        }

        final int execHint = getExecHint(forceScalar);
        options = options | (execHint << 4);

        options = options | ((nullChecks ? 1 : 0) << 6);
        return options;
    }

    /**
     * Determines the priority of a predicate for short-circuit evaluation.
     * Lower value = higher priority (evaluated first).
     * Priority: uuid eq > long eq > ... > others > ... > long neq > uuid neq
     */
    private int getPredicatePriority(ExpressionNode node) {
        if (node == null || node.type != ExpressionNode.OPERATION) {
            return PRIORITY_OTHER;
        }
        // Check if it's an equality operation
        if (Chars.equals(node.token, '=')) {
            // Find the column type involved in this equality
            return getPredicatePriority0(node, PRIORITY_I16_EQ, PRIORITY_I8_EQ, PRIORITY_I4_EQ, PRIORITY_SYM_EQ, PRIORITY_OTHER_EQ);
        } else if (Chars.equals(node.token, "<>") || Chars.equals(node.token, "!=")) {
            // Find the column type involved in this inequality
            return getPredicatePriority0(node, PRIORITY_I16_NEQ, PRIORITY_I8_NEQ, PRIORITY_I4_NEQ, PRIORITY_SYM_NEQ, PRIORITY_OTHER_NEQ);
        }
        return PRIORITY_OTHER;
    }

    private int getPredicatePriority0(
            ExpressionNode node,
            int priorityI16Neq,
            int priorityI8Neq,
            int priorityI4Neq,
            int prioritySymNeq,
            int priorityOtherNeq
    ) {
        final int columnType = findOperandColumnType(node);
        return switch (columnType) {
            case ColumnType.UUID, ColumnType.LONG128 -> priorityI16Neq;
            case ColumnType.LONG, ColumnType.TIMESTAMP, ColumnType.DATE, ColumnType.GEOLONG -> priorityI8Neq;
            case ColumnType.INT, ColumnType.IPv4, ColumnType.GEOINT -> priorityI4Neq;
            case ColumnType.SYMBOL -> prioritySymNeq;
            default -> priorityOtherNeq;
        };
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

    private boolean isInTimestampPredicate() throws SqlException {
        // visit inOperationNode to get an expression type
        predicateContext.onNodeVisited(predicateContext.inOperationNode.rhs);
        predicateContext.onNodeVisited(predicateContext.inOperationNode.lhs);

        // check predicate type is timestamp
        return ColumnType.isTimestamp(predicateContext.columnType);
    }

    /**
     * Checks if the expression tree is a pure AND chain (no OR at top level).
     */
    private boolean isPureAndChain(ExpressionNode node) {
        if (node == null) {
            return false;
        }
        if (node.type == ExpressionNode.OPERATION) {
            if (SqlKeywords.isAndKeyword(node.token)) {
                return isPureAndChain(node.lhs) && isPureAndChain(node.rhs);
            }
            return !SqlKeywords.isOrKeyword(node.token);
        }
        // Leaf predicate or non-OR operation
        return true;
    }

    /**
     * Checks if the expression tree is a pure OR chain (no AND at top level).
     */
    private boolean isPureOrChain(ExpressionNode node) {
        if (node == null) {
            return false;
        }
        if (node.type == ExpressionNode.OPERATION) {
            if (SqlKeywords.isOrKeyword(node.token)) {
                return isPureOrChain(node.lhs) && isPureOrChain(node.rhs);
            }
            return !SqlKeywords.isAndKeyword(node.token);
        }
        // Leaf predicate or non-AND operation
        return true;
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

    private void putDoubleOperand(long offset, int type, double payload) {
        memory.putInt(offset, CompiledFilterIRSerializer.IMM);
        memory.putInt(offset + Integer.BYTES, type);
        memory.putDouble(offset + 2 * Integer.BYTES, payload);
        memory.putLong(offset + 2 * Integer.BYTES + Double.BYTES, 0L);
    }

    private void putOperand(int opcode, int type, long payload) {
        memory.putInt(opcode);
        memory.putInt(type);
        memory.putLong(payload);
        memory.putLong(0L);
    }

    private void putOperand(long offset, int opcode, int type, long payload) {
        putOperand(offset, opcode, type, payload, 0L);
    }

    private void putOperand(long offset, int opcode, int type, long lo, long hi) {
        memory.putInt(offset, opcode);
        memory.putInt(offset + Integer.BYTES, type);
        memory.putLong(offset + 2 * Integer.BYTES, lo);
        memory.putLong(offset + 2 * Integer.BYTES + Long.BYTES, hi);
    }

    private void putOperator(int opcode) {
        memory.putInt(opcode);
        // pad unused fields with zeros
        memory.putInt(0);
        memory.putLong(0L);
        memory.putLong(0L);
    }

    // Emit operator with label index in payload (for short-circuit opcodes)
    // Label conventions:
    // - Label 0 (l_next_row): skip row storage, move to next row (used by AND_SC on false)
    // - Label 1 (l_store_row): store row, then move to next row (used by OR_SC on true)
    // - Labels 2+: user-defined labels for IN() short-circuit, etc.
    private void putOperatorWithLabel(int opcode, int labelIndex) {
        // Currently we use up to 3 labels simultaneously
        assert labelIndex >= 0 && labelIndex < MAX_LABELS : "label index out of bounds: " + labelIndex;
        memory.putInt(opcode);
        memory.putInt(0); // options unused
        memory.putLong(labelIndex); // payload.lo = label index
        memory.putLong(0L); // payload.hi unused
    }

    private void rejectSymbol(final CharSequence token, int position) throws SqlException {
        // >, >=, < and <= for symbols should use string and not int value comparison
        // since string is not supported in JIT, we reject it here and allow code generator to fall back to non-JIT implementation
        if (predicateContext.columnType == ColumnType.SYMBOL) {
            throw SqlException.position(position)
                    .put("operator: ").put(token).put(" is not supported for SYMBOL type");
        }
    }

    private void serializeBindVariable(final ExpressionNode node) throws SqlException {
        if (predicateContext.isActive()) {
            Function varFunction = getBindVariableFunction(node.position, node.token);

            final int columnType = varFunction.getType();
            // Treat string bind variable to be of the symbol type
            if (columnType == ColumnType.STRING) {
                // We're going to backfill this variable later since we may
                // not have a symbol column index at this point
                long offset = memory.getAppendOffset();
                backfillNodes.put(offset, node);
                putOperand(UNDEFINED_CODE, UNDEFINED_CODE, 0);
                return;
            }

            final int columnTypeTag = ColumnType.tagOf(columnType);
            int typeCode = bindVariableTypeCode(columnTypeTag);
            if (typeCode == UNDEFINED_CODE) {
                throw SqlException.position(node.position)
                        .put("unsupported bind variable type: ")
                        .put(ColumnType.nameOf(columnTypeTag));
            }

            bindVarFunctions.add(varFunction);
            int index = bindVarFunctions.size() - 1;
            putOperand(VAR, typeCode, index);
        } else {
            throw SqlException.position(node.position)
                    .put("bind variable outside of predicate: ")
                    .put(node.token);
        }
    }

    private void serializeColumn(int position, final CharSequence token) throws SqlException {
        if (predicateContext.isActive()) {
            final int index = metadata.getColumnIndexQuiet(token);
            if (index == -1) {
                throw SqlException.invalidColumn(position, token);
            }

            final int columnType = metadata.getColumnType(index);
            final int columnTypeTag = ColumnType.tagOf(columnType);
            int typeCode = columnTypeCode(columnTypeTag);
            if (typeCode == UNDEFINED_CODE) {
                throw SqlException.position(position)
                        .put("unsupported column type: ")
                        .put(ColumnType.nameOf(columnTypeTag));
            }

            // In the case of a top level boolean column, expand it to "boolean_column = true" expression.
            if (predicateContext.singleBooleanColumn && columnTypeTag == ColumnType.BOOLEAN) {
                // "true" constant
                putOperand(IMM, I1_TYPE, 1);
                // column
                putOperand(MEM, typeCode, index);
                // =
                putOperator(EQ);
                return;
            }
            putOperand(MEM, typeCode, index);
        } else {
            throw SqlException.position(position)
                    .put("non-boolean column outside of predicate: ")
                    .put(token);
        }
    }

    private void serializeConstant(long offset, int position, final CharSequence token, boolean negated) throws SqlException {
        final int len = token.length();
        final int typeCode = predicateContext.localTypesObserver.constantTypeCode();
        if (typeCode == UNDEFINED_CODE) {
            throw SqlException.position(position).put("all constants expression: ").put(token);
        }

        if (SqlKeywords.isNullKeyword(token)) {
            serializeNull(offset, position, typeCode, predicateContext.columnType);
            return;
        }

        if (predicateContext.columnType == ColumnType.SYMBOL) {
            serializeSymbolConstant(offset, position, token);
            return;
        }

        if (Chars.isQuoted(token)) {
            if (ColumnType.isTimestamp(predicateContext.columnType)) {
                try {
                    putOperand(
                            offset,
                            IMM,
                            I8_TYPE,
                            ColumnType.getTimestampDriver(predicateContext.columnType).parseQuotedLiteral(token)
                    );
                } catch (NumericException e) {
                    throw SqlException.invalidDate(token, position);
                }
                return;
            } else if (predicateContext.columnType == ColumnType.DATE) {
                try {
                    // This is a hack for DATA column type. We use a TIMESTAMP specific driver to
                    // do the work and then derive millis
                    putOperand(offset, IMM, I8_TYPE, MicrosTimestampDriver.INSTANCE.toDate(MicrosTimestampDriver.INSTANCE.parseQuotedLiteral(token)));
                } catch (NumericException e) {
                    throw SqlException.invalidDate(token, position);
                }
                return;
            } else if (len == 3) {
                if (predicateContext.columnType != ColumnType.CHAR) {
                    throw SqlException.position(position).put("char constant in non-char expression: ").put(token);
                }
                // this is 'x' - char
                putOperand(offset, IMM, I2_TYPE, token.charAt(1));
                return;
            } else if (len == 2 + Uuid.UUID_LENGTH) {
                if (predicateContext.columnType != ColumnType.UUID) {
                    throw SqlException.position(position).put("uuid constant in non-uuid expression: ").put(token);
                }
                try {
                    // skip first and last char which are quotes
                    Uuid.checkDashesAndLength(token, 1, token.length() - 1);
                    putOperand(offset, IMM, I16_TYPE, Uuid.parseLo(token, 1), Uuid.parseHi(token, 1));
                } catch (NumericException e) {
                    throw SqlException.position(position).put("invalid uuid constant: ").put(token);
                }
                return;
            }
            throw SqlException.position(position).put("unsupported string constant: ").put(token);
        }

        if (SqlKeywords.isTrueKeyword(token)) {
            if (predicateContext.columnType != ColumnType.BOOLEAN) {
                throw SqlException.position(position).put("boolean constant in non-boolean expression: ").put(token);
            }
            putOperand(offset, IMM, I1_TYPE, 1);
            return;
        }

        if (SqlKeywords.isFalseKeyword(token)) {
            if (predicateContext.columnType != ColumnType.BOOLEAN) {
                throw SqlException.position(position).put("boolean constant in non-boolean expression: ").put(token);
            }
            putOperand(offset, IMM, I1_TYPE, 0);
            return;
        }

        if (len > 1 && token.charAt(0) == '#') {
            if (isGeoHash(predicateContext.columnType)) {
                ConstantFunction geoConstant = GeoHashUtil.parseGeoHashConstant(position, token, len);
                if (geoConstant != null) {
                    serializeGeoHash(offset, position, geoConstant, typeCode);
                    return;
                }
            } else {
                throw SqlException.position(position).put("geo hash constant in non-geo hash expression: ").put(token);
            }
        }

        if (!isNumeric(predicateContext.columnType) && !ColumnType.isTimestamp(predicateContext.columnType)) {
            throw SqlException.position(position).put("numeric constant in non-numeric expression: ").put(token);
        }
        if (predicateContext.localTypesObserver.hasMixedSizes()) {
            serializeUntypedNumber(offset, position, token, negated);
        } else {
            serializeNumber(offset, position, token, typeCode, negated);
        }
    }

    private void serializeConstantStub(final ExpressionNode node) throws SqlException {
        if (predicateContext.isActive()) {
            long offset = memory.getAppendOffset();
            backfillNodes.put(offset, node);
            putOperand(UNDEFINED_CODE, UNDEFINED_CODE, 0);
        } else {
            throw SqlException.position(node.position)
                    .put("constant outside of predicate: ")
                    .put(node.token);
        }
    }

    private void serializeGeoHash(long offset, int position, final ConstantFunction geoHashConstant, int typeCode) throws SqlException {
        try {
            switch (typeCode) {
                case I1_TYPE:
                    putOperand(offset, IMM, typeCode, geoHashConstant.getGeoByte(null));
                    break;
                case I2_TYPE:
                    putOperand(offset, IMM, typeCode, geoHashConstant.getGeoShort(null));
                    break;
                case I4_TYPE:
                    putOperand(offset, IMM, typeCode, geoHashConstant.getGeoInt(null));
                    break;
                case I8_TYPE:
                    putOperand(offset, IMM, typeCode, geoHashConstant.getGeoLong(null));
                    break;
                default:
                    throw SqlException.position(position).put("unexpected type code for geo hash: ").put(typeCode);
            }
        } catch (UnsupportedOperationException e) {
            throw SqlException.position(position).put("unexpected type for geo hash: ").put(typeCode);
        }
    }

    private void serializeIn() throws SqlException {
        predicateContext.currentInSerialization = true;

        final ObjList<ExpressionNode> args = predicateContext.inOperationNode.args;

        if (args.size() > executionContext.getCairoEngine().getConfiguration().getSqlJitMaxInListSizeThreshold()) {
            throw SqlException.$(args.getQuick(0).position, "exceeded JIT IN list threshold [threshold=")
                    .put(executionContext.getCairoEngine().getConfiguration().getSqlJitMaxInListSizeThreshold())
                    .put(", actual=").put(args.size()).put(']');
        }

        // Short-circuit mode: only use when IN() is the root of the predicate (top-level) AND
        // we're in an AND chain. For OR chains, the l_next_row label skips row storage which is
        // wrong when IN() matches (we want to store the row). For nested IN() we must also fall
        // back to boolean ORs because AND_SC(0) would incorrectly skip the row.
        final boolean isTopLevelIn = predicateContext.inOperationNode == predicateContext.rootNode;
        if (predicateContext.shortCircuitMode == PredicateContext.SC_AND && isTopLevelIn) {
            if (args.size() < 3) {
                // Single value: short-circuit, unrolled version of the below loop
                // Two values: short-circuit, unrolled version of the below loop
                traverseAlgo.traverse(predicateContext.inOperationNode.rhs, this);
                traverseAlgo.traverse(predicateContext.inOperationNode.lhs, this);
                putOperator(EQ);
                putOperatorWithLabel(AND_SC, 0); // if false, jump to next_row
            } else {
                // Multiple values: BEGIN_SC(2), [EQ, OR_SC(2)]*, EQ, AND_SC(0), END_SC(2)
                // Label 0 = next_row (skip this row) - reserved by backend
                // Label 1 = store_row (accept row) - reserved by backend
                // Label 2 = success (at least one IN match)
                putOperatorWithLabel(BEGIN_SC, 2); // create success label
                for (int i = 0, n = predicateContext.inOperationNode.args.size() - 1; i < n; i++) {
                    traverseAlgo.traverse(args.get(i), this);
                    traverseAlgo.traverse(args.getLast(), this);
                    putOperator(EQ);
                    if (i < n - 1) {
                        putOperatorWithLabel(OR_SC, 2); // if true, jump to success
                    } else {
                        putOperatorWithLabel(AND_SC, 0); // if false, jump to next_row
                        putOperatorWithLabel(END_SC, 2); // bind success label
                    }
                }
            }
            // Mark that this predicate handled its own short-circuit exit
            // so the parent AND chain doesn't emit another AND_SC
            predicateContext.handledShortCircuitExit = true;
            return;
        }

        // Non-short-circuit mode: use traditional boolean ORs
        if (args.size() < 3) {
            traverseAlgo.traverse(predicateContext.inOperationNode.rhs, this);
            traverseAlgo.traverse(predicateContext.inOperationNode.lhs, this);
            putOperator(EQ);
        }

        int orCount = -1;
        for (int i = 0, n = predicateContext.inOperationNode.args.size() - 1; i < n; i++) {
            traverseAlgo.traverse(args.get(i), this);
            traverseAlgo.traverse(args.getLast(), this);
            putOperator(EQ);
            orCount++;
        }

        for (int i = 0; i < orCount; i++) {
            putOperator(OR);
        }
    }

    private void serializeInTimestampRange(int position) throws SqlException {
        predicateContext.currentInSerialization = true;

        final CharSequence token = predicateContext.inOperationNode.rhs.token;
        final CharSequence intervalEx = token == null || SqlKeywords.isNullKeyword(token) ? null : GenericLexer.unquote(token);

        final LongList intervals = predicateContext.inIntervals;
        IntervalUtils.parseTickExprAndIntersect(
                ColumnType.getTimestampDriver(predicateContext.columnType),
                executionContext.getCairoEngine().getConfiguration(),
                intervalEx,
                intervals,
                position,
                sink,
                true
        );

        final ExpressionNode lhs = predicateContext.inOperationNode.lhs;

        int orCount = -1;
        for (int i = 0, n = intervals.size(); i < n; i += 2) {
            long lo = IntervalUtils.decodeIntervalLo(intervals, i);
            long hi = IntervalUtils.decodeIntervalHi(intervals, i);
            putOperand(IMM, I8_TYPE, lo);
            traverseAlgo.traverse(lhs, this);
            putOperator(GE);
            putOperand(IMM, I8_TYPE, hi);
            traverseAlgo.traverse(lhs, this);
            putOperator(LE);
            putOperator(AND);
            orCount++;
        }

        for (int i = 0; i < orCount; i++) {
            putOperator(OR);
        }
    }

    private void serializeNull(long offset, int position, int typeCode, int columnType) throws SqlException {
        switch (typeCode) {
            case I1_TYPE:
                if (!isGeoHash(columnType)) {
                    throw SqlException.position(position).put("byte type is not nullable");
                }
                putOperand(offset, IMM, typeCode, GeoHashes.BYTE_NULL);
                break;
            case I2_TYPE:
                if (!isGeoHash(columnType)) {
                    throw SqlException.position(position).put("short type is not nullable");
                }
                putOperand(offset, IMM, typeCode, GeoHashes.SHORT_NULL);
                break;
            case I4_TYPE:
                switch (ColumnType.tagOf(columnType)) {
                    case ColumnType.GEOBYTE:
                    case ColumnType.GEOSHORT:
                    case ColumnType.GEOINT:
                    case ColumnType.GEOLONG:
                    case ColumnType.GEOHASH:
                        putOperand(offset, IMM, typeCode, GeoHashes.INT_NULL);
                        break;
                    case ColumnType.IPv4:
                        putOperand(offset, IMM, typeCode, Numbers.IPv4_NULL);
                        break;
                    default:
                        putOperand(offset, IMM, typeCode, Numbers.INT_NULL);
                        break;
                }
                break;
            case I8_TYPE:
                putOperand(offset, IMM, typeCode, isGeoHash(columnType) ? GeoHashes.NULL : Numbers.LONG_NULL);
                break;
            case F4_TYPE:
                putDoubleOperand(offset, typeCode, Float.NaN);
                break;
            case F8_TYPE:
                putDoubleOperand(offset, typeCode, Double.NaN);
                break;
            case I16_TYPE:
                putOperand(offset, IMM, typeCode, Numbers.LONG_NULL, Numbers.LONG_NULL);
                break;
            case STRING_HEADER_TYPE:
                putOperand(offset, IMM, I4_TYPE, TableUtils.NULL_LEN);
                break;
            case BINARY_HEADER_TYPE:
                putOperand(offset, IMM, I8_TYPE, TableUtils.NULL_LEN);
                break;
            case VARCHAR_HEADER_TYPE: // varchar headers are stored in aux vector
                putOperand(offset, IMM, I8_TYPE, VarcharTypeDriver.VARCHAR_HEADER_FLAG_NULL);
                break;
            default:
                throw SqlException.position(position).put("unexpected null type: ").put(typeCode);
        }
    }

    private void serializeNumber(
            long offset,
            int position,
            final CharSequence token,
            int typeCode,
            boolean negated
    ) throws SqlException {
        long sign = negated ? -1 : 1;
        try {
            switch (typeCode) {
                case I1_TYPE:
                    final byte b = (byte) Numbers.parseInt(token);
                    putOperand(offset, IMM, I1_TYPE, sign * b);
                    break;
                case I2_TYPE:
                    final short s = (short) Numbers.parseInt(token);
                    putOperand(offset, IMM, I2_TYPE, sign * s);
                    break;
                case I4_TYPE:
                case F4_TYPE:
                    try {
                        final int i = Numbers.parseInt(token);
                        putOperand(offset, IMM, I4_TYPE, sign * i);
                    } catch (NumericException e) {
                        final float fi = Numbers.parseFloat(token);
                        putDoubleOperand(offset, F4_TYPE, sign * fi);
                    }
                    break;
                case I8_TYPE:
                case F8_TYPE:
                    try {
                        final long l = Numbers.parseLong(token);
                        putOperand(offset, IMM, I8_TYPE, sign * l);
                    } catch (NumericException e) {
                        final double dl = Numbers.parseDouble(token);
                        putDoubleOperand(offset, F8_TYPE, sign * dl);
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

    private void serializeOperator(int position, final CharSequence token, int argCount, int type) throws SqlException {
        if (SqlKeywords.isInKeyword(token)) {
            if (type == ExpressionNode.FUNCTION) {
                serializeIn();
                return;
            } else if (type == ExpressionNode.SET_OPERATION && isInTimestampPredicate()) {
                serializeInTimestampRange(position);
                return;
            }
        }
        if (SqlKeywords.isNotKeyword(token)) {
            putOperator(NOT);
            return;
        }
        if (SqlKeywords.isAndKeyword(token)) {
            putOperator(AND);
            return;
        }
        if (SqlKeywords.isOrKeyword(token)) {
            putOperator(OR);
            return;
        }
        if (Chars.equals(token, "=")) {
            putOperator(EQ);
            return;
        }
        if (Chars.equals(token, "<>") || Chars.equals(token, "!=")) {
            putOperator(NE);
            return;
        }
        if (Chars.equals(token, "<")) {
            rejectSymbol(token, position);
            putOperator(LT);
            return;
        }
        if (Chars.equals(token, "<=")) {
            rejectSymbol(token, position);
            putOperator(LE);
            return;
        }
        if (Chars.equals(token, ">")) {
            rejectSymbol(token, position);
            putOperator(GT);
            return;
        }
        if (Chars.equals(token, ">=")) {
            rejectSymbol(token, position);
            putOperator(GE);
            return;
        }
        if (Chars.equals(token, "+")) {
            if (argCount == 2) {
                putOperator(ADD);
            } // ignore unary
            return;
        }
        if (Chars.equals(token, "-")) {
            if (argCount == 2) {
                putOperator(SUB);
            } else if (argCount == 1) {
                putOperator(NEG);
            }
            return;
        }
        if (Chars.equals(token, "*")) {
            putOperator(MUL);
            return;
        }
        if (Chars.equals(token, "/")) {
            putOperator(DIV);
            return;
        }
        throw SqlException.position(position).put("invalid operator: ").put(token);
    }

    /**
     * Serializes predicates in priority order with short-circuit ANDs for high-priority ones.
     * Must be used only in scalar compilation mode.
     */
    private int serializePredicatesAndSc(
            @NotNull ObjList<ExpressionNode> predicates,
            boolean forceScalar,
            boolean debug,
            boolean nullChecks
    ) throws SqlException {
        final int n = predicates.size();
        assert n > 0;

        // Enable AND short-circuit mode for IN() optimization
        predicateContext.shortCircuitMode = PredicateContext.SC_AND;
        try {
            // Serialize all predicates in the priority order with short-circuit ANDs
            for (int i = 0; i < n; i++) {
                traverseAlgo.traverse(predicates.getQuick(i), this);
                if (i != n - 1) {
                    // Only emit AND_SC if the predicate didn't handle its own short-circuit exit.
                    // IN() with short-circuit mode emits its own AND_SC(0), so we skip it here.
                    if (!predicateContext.handledShortCircuitExit) {
                        putOperatorWithLabel(AND_SC, 0); // label 0 = next_row
                    }
                }
            }

            // Check if the backend is going to use SIMD, although we expected scalar mode.
            final int execHint = getExecHint(forceScalar);
            if (execHint == EXEC_HINT_SINGLE_SIZE_TYPE) {
                // We could handle this via the non-short-circuit code path, but if we get here,
                // it means that scalarModeDetector did a false-positive scalar mode detection.
                // In such case, it's a bug we should fix, so let's fail JIT compilation to flag that.
                throw SqlException.position(0).put("expected scalar compilation mode, got: ").put(execHint);
            }

            putOperator(RET);

            ensureOnlyVarSizeHeaderChecks();
            return getOptions(forceScalar, debug, nullChecks);
        } finally {
            predicateContext.shortCircuitMode = PredicateContext.SC_NONE;
        }
    }

    /**
     * Serializes predicates in priority order with short-circuit ORs for low-priority ones.
     * Must be used only in scalar compilation mode.
     */
    private int serializePredicatesOrSc(
            @NotNull ObjList<ExpressionNode> predicates,
            boolean forceScalar,
            boolean debug,
            boolean nullChecks
    ) throws SqlException {
        final int n = predicates.size();
        assert n > 0;

        // Enable OR short-circuit mode (IN() should NOT use short-circuit in OR chains)
        predicateContext.shortCircuitMode = PredicateContext.SC_OR;
        try {
            // Serialize all predicates in the inverted priority order with short-circuit ORs
            for (int i = 0; i < n; i++) {
                traverseAlgo.traverse(predicates.getQuick(i), this);
                if (i != n - 1) {
                    putOperatorWithLabel(OR_SC, 1); // label 1 = store_row (accept row on true)
                }
            }

            // Check if the backend is going to use SIMD, although we expected scalar mode.
            final int execHint = getExecHint(forceScalar);
            if (execHint == EXEC_HINT_SINGLE_SIZE_TYPE) {
                // We could handle this via the non-short-circuit code path, but if we get here,
                // it means that scalarModeDetector did a false-positive scalar mode detection.
                // In such case, it's a bug we should fix, so let's fail JIT compilation to flag that.
                throw SqlException.position(0).put("expected scalar compilation mode, got: ").put(execHint);
            }

            putOperator(RET);

            ensureOnlyVarSizeHeaderChecks();
            return getOptions(forceScalar, debug, nullChecks);
        } finally {
            predicateContext.shortCircuitMode = PredicateContext.SC_NONE;
        }
    }

    private void serializeSymbolConstant(long offset, int position, final CharSequence token) throws SqlException {
        final int len = token.length();
        CharSequence symbol = token;
        if (Chars.isQuoted(token)) {
            if (len < 3) {
                throw SqlException.position(position).put("unsupported symbol constant: ").put(token);
            }
            sink.clear();
            Chars.unescape(symbol, 1, len - 1, '\'', sink);
            symbol = sink;
        }

        if (predicateContext.symbolTable == null || predicateContext.symbolColumnIndex == -1) {
            throw SqlException.position(position).put("reader or column index is missing for symbol constant: ").put(token);
        }

        final int key = predicateContext.symbolTable.keyOf(symbol);
        if (key != SymbolTable.VALUE_NOT_FOUND) {
            // Known symbol constant case
            putOperand(offset, IMM, I4_TYPE, key);
            return;
        }

        // Unknown symbol constant case. Create a fake bind variable function to handle it.
        final SymbolConstant function = SymbolConstant.newInstance(symbol);
        bindVarFunctions.add(new CompiledFilterSymbolBindVariable(function, predicateContext.symbolColumnIndex));
        int index = bindVarFunctions.size() - 1;

        int typeCode = bindVariableTypeCode(ColumnType.STRING);
        putOperand(offset, VAR, typeCode, index);
    }

    private void serializeUntypedNumber(long offset, int position, final CharSequence token, boolean negated) throws SqlException {
        long sign = negated ? -1 : 1;

        try {
            final int i = Numbers.parseInt(token);
            putOperand(offset, IMM, I4_TYPE, sign * i);
            return;
        } catch (NumericException ignore) {
        }

        try {
            final long l = Numbers.parseLong(token);
            putOperand(offset, IMM, I8_TYPE, sign * l);
            return;
        } catch (NumericException ignore) {
        }

        try {
            final double d = Numbers.parseDouble(token);
            putDoubleOperand(offset, F8_TYPE, sign * d);
            return;
        } catch (NumericException ignore) {
        }

        try {
            final float f = Numbers.parseFloat(token);
            putDoubleOperand(offset, F4_TYPE, sign * f);
            return;
        } catch (NumericException ignore) {
        }

        throw SqlException.position(position).put("unexpected non-numeric constant: ").put(token);
    }

    /**
     * Sorts predicates by inverted priority using simple insertion sort.
     * Stable sort to preserve original order for equal priorities.
     */
    private void sortPredicatesByInvertedPriority(ObjList<ExpressionNode> predicates) {
        for (int i = 1, n = predicates.size(); i < n; i++) {
            final ExpressionNode key = predicates.getQuick(i);
            final int keyPriority = getPredicatePriority(key);
            int j = i - 1;
            while (j >= 0 && getPredicatePriority(predicates.getQuick(j)) < keyPriority) {
                predicates.setQuick(j + 1, predicates.getQuick(j));
                j--;
            }
            predicates.setQuick(j + 1, key);
        }
    }

    /**
     * Sorts predicates by priority using simple insertion sort.
     * Stable sort to preserve original order for equal priorities.
     */
    private void sortPredicatesByPriority(ObjList<ExpressionNode> predicates) {
        for (int i = 1, n = predicates.size(); i < n; i++) {
            final ExpressionNode key = predicates.getQuick(i);
            final int keyPriority = getPredicatePriority(key);
            int j = i - 1;
            while (j >= 0 && getPredicatePriority(predicates.getQuick(j)) > keyPriority) {
                predicates.setQuick(j + 1, predicates.getQuick(j));
                j--;
            }
            predicates.setQuick(j + 1, key);
        }
    }

    private static class SqlWrapperException extends RuntimeException {

        final SqlException wrappedException;

        SqlWrapperException(SqlException wrappedException) {
            this.wrappedException = wrappedException;
        }
    }

    /**
     * Helper class for accumulating column and bind variable types information.
     */
    private static class TypesObserver implements Mutable {
        private static final int BINARY_HEADER_INDEX = 8;
        private static final int F4_INDEX = 3;
        private static final int F8_INDEX = 5;
        private static final int I16_INDEX = 6;
        private static final int I1_INDEX = 0;
        private static final int I2_INDEX = 1;
        private static final int I4_INDEX = 2;
        private static final int I8_INDEX = 4;
        private static final int STRING_HEADER_INDEX = 7;
        private static final int VARCHAR_HEADER_INDEX = 9;
        private static final int TYPES_COUNT = VARCHAR_HEADER_INDEX + 1;

        private final byte[] sizes = new byte[TYPES_COUNT];

        @Override
        public void clear() {
            Arrays.fill(sizes, (byte) 0);
        }

        /**
         * Returns the expected constant type calculated based on the "widest" observed column
         * or bind variable type. The result contains *_TYPE value or UNDEFINED_CODE value.
         */
        public int constantTypeCode() {
            for (int i = sizes.length - 1; i > -1; i--) {
                byte size = sizes[i];
                if (size > 0) {
                    // If floats are present, we need to cast longs to double.
                    if (i == I8_INDEX && sizes[F4_INDEX] > 0) {
                        return F8_TYPE;
                    }
                    return indexToTypeCode(i);
                }
            }
            return UNDEFINED_CODE;
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

        public void observe(int code) {
            int index = typeCodeToIndex(code);
            if (index >= 0) {
                sizes[index] = typeSizeBytes(code);
            }
        }

        private static int indexToTypeCode(int index) {
            return switch (index) {
                case I1_INDEX -> I1_TYPE;
                case I2_INDEX -> I2_TYPE;
                case I4_INDEX -> I4_TYPE;
                case F4_INDEX -> F4_TYPE;
                case I8_INDEX -> I8_TYPE;
                case F8_INDEX -> F8_TYPE;
                case I16_INDEX -> I16_TYPE;
                case STRING_HEADER_INDEX -> STRING_HEADER_TYPE;
                case BINARY_HEADER_INDEX -> BINARY_HEADER_TYPE;
                case VARCHAR_HEADER_INDEX -> VARCHAR_HEADER_TYPE;
                default -> UNDEFINED_CODE;
            };
        }

        private static int typeCodeToIndex(int code) {
            return switch (code) {
                case I1_TYPE -> I1_INDEX;
                case I2_TYPE -> I2_INDEX;
                case I4_TYPE -> I4_INDEX;
                case F4_TYPE -> F4_INDEX;
                case I8_TYPE -> I8_INDEX;
                case F8_TYPE -> F8_INDEX;
                case I16_TYPE -> I16_INDEX;
                case STRING_HEADER_TYPE -> STRING_HEADER_INDEX;
                case BINARY_HEADER_TYPE -> BINARY_HEADER_INDEX;
                case VARCHAR_HEADER_TYPE -> VARCHAR_HEADER_INDEX;
                default -> -1;
            };
        }

        /**
         * Returns the size in bytes for a given type code.
         */
        private static byte typeSizeBytes(int typeCode) {
            return switch (typeCode) {
                case I1_TYPE -> 1;
                case I2_TYPE -> 2;
                case I4_TYPE, F4_TYPE -> 4;
                case I8_TYPE, F8_TYPE, STRING_HEADER_TYPE, BINARY_HEADER_TYPE, VARCHAR_HEADER_TYPE -> 8;
                case I16_TYPE -> 16;
                default -> 0;
            };
        }
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
        static final int SC_AND = 1;   // AND chain short-circuit (scalar only)
        static final int SC_NONE = 0;  // not in short-circuit mode
        static final int SC_OR = 2;    // OR chain short-circuit (scalar only)

        final TypesObserver globalTypesObserver = new TypesObserver();
        final TypesObserver localTypesObserver = new TypesObserver();
        private final LongList inIntervals = new LongList();
        int columnType;
        boolean hasArithmeticOperations;
        int shortCircuitMode = SC_NONE; // short-circuit evaluation mode
        boolean singleBooleanColumn;
        int symbolColumnIndex; // used for symbol deferred constants and bind variables
        StaticSymbolTable symbolTable; // used for known symbol constant lookups
        private boolean currentInSerialization = false;
        private boolean handledShortCircuitExit = false; // true if predicate emitted its own AND_SC/OR_SC exit
        private ExpressionNode inOperationNode = null;
        private ExpressionNode rootNode;

        @Override
        public void clear() {
            reset();
            globalTypesObserver.clear();
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
                    columnType = ColumnType.BOOLEAN;
                    singleBooleanColumn = true;
                }
            }

            if (SqlKeywords.isInKeyword(node.token)) {
                inOperationNode = node;
            }
        }

        public boolean onNodeVisited(final ExpressionNode node) throws SqlException {
            boolean predicateLeft = false;
            if (node == rootNode) {
                // We left the predicate.
                rootNode = null;
                predicateLeft = true;
            }

            if (node == inOperationNode) {
                inOperationNode = null;
                currentInSerialization = false;
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

        private void handleBindVariable(ExpressionNode node) throws SqlException {
            Function varFunction = getBindVariableFunction(node.position, node.token);
            // We treat bind variables as columns here for the sake of simplicity
            final int columnType = varFunction.getType();
            int columnTypeTag = ColumnType.tagOf(columnType);
            // Treat string bind variable to be of a symbol type
            if (columnTypeTag == ColumnType.STRING) {
                columnTypeTag = ColumnType.SYMBOL;
            }

            updateType(node.position, columnType == ColumnType.STRING ? ColumnType.SYMBOL : columnType);
            int code = columnTypeCode(columnTypeTag);
            localTypesObserver.observe(code);
            globalTypesObserver.observe(code);
        }

        private void handleColumn(ExpressionNode node) throws SqlException {
            final int columnIndex = metadata.getColumnIndexQuiet(node.token);
            if (columnIndex == -1) {
                throw SqlException.invalidColumn(node.position, node.token);
            }
            final int columnType = metadata.getColumnType(columnIndex);
            final int columnTypeTag = ColumnType.tagOf(columnType);
            if (columnTypeTag == ColumnType.SYMBOL) {
                if (symbolColumnIndex != -1 && symbolColumnIndex != columnIndex) {
                    throw SqlException.position(node.position)
                            .put("operators on different symbol columns are not supported by JIT: ")
                            .put(node.token);
                }
                symbolTable = pageFrameCursor.getSymbolTable(columnIndex);
                symbolColumnIndex = columnIndex;
            }

            updateType(node.position, columnType);

            int typeCode = columnTypeCode(columnTypeTag);
            localTypesObserver.observe(typeCode);
            globalTypesObserver.observe(typeCode);
        }

        private void handleOperation(ExpressionNode node) {
            hasArithmeticOperations |= isArithmeticOperation(node);
        }

        private void reset() {
            rootNode = null;
            columnType = ColumnType.UNDEFINED;
            symbolTable = null;
            symbolColumnIndex = -1;
            singleBooleanColumn = false;
            hasArithmeticOperations = false;
            localTypesObserver.clear();
            currentInSerialization = false;
            handledShortCircuitExit = false;
            inOperationNode = null;
            inIntervals.clear();
            // Note: shortCircuitMode is NOT reset here; it's managed by serializePredicates*Sc methods
        }

        private void updateType(int position, int columnType0) throws SqlException {
            switch (ColumnType.tagOf(columnType0)) {
                case ColumnType.BOOLEAN:
                    if (this.columnType != ColumnType.UNDEFINED && this.columnType != columnType0) {
                        throw SqlException.position(position)
                                .put("non-boolean column in boolean expression: ")
                                .put(ColumnType.nameOf(columnType0));
                    }
                    columnType = columnType0;
                    break;
                case ColumnType.GEOBYTE:
                case ColumnType.GEOSHORT:
                case ColumnType.GEOINT:
                case ColumnType.GEOLONG:
                    if (columnType != ColumnType.UNDEFINED && !isGeoHash(columnType)) {
                        throw SqlException.position(position)
                                .put("non-geohash column in geohash expression: ")
                                .put(ColumnType.nameOf(columnType0));
                    }
                    columnType = columnType0;
                    break;
                case ColumnType.IPv4:
                    if (columnType != ColumnType.UNDEFINED && columnType != columnType0) {
                        throw SqlException.position(position)
                                .put("non-ipv4 column in ipv4 expression: ")
                                .put(ColumnType.nameOf(columnType0));
                    }
                    columnType = columnType0;
                    break;
                case ColumnType.CHAR:
                    if (columnType != ColumnType.UNDEFINED && columnType != columnType0) {
                        throw SqlException.position(position)
                                .put("non-char column in char expression: ")
                                .put(ColumnType.nameOf(columnType0));
                    }
                    columnType = columnType0;
                    break;
                case ColumnType.SYMBOL:
                    if (columnType != ColumnType.UNDEFINED && columnType != columnType0) {
                        throw SqlException.position(position)
                                .put("non-symbol column in symbol expression: ")
                                .put(ColumnType.nameOf(columnType0));
                    }
                    columnType = columnType0;
                    break;
                case ColumnType.UUID:
                    if (columnType != ColumnType.UNDEFINED && columnType != columnType0) {
                        throw SqlException.position(position)
                                .put("non-uuid column in uuid expression: ")
                                .put(ColumnType.nameOf(columnType0));
                    }
                    columnType = columnType0;
                    break;
                case ColumnType.TIMESTAMP:
                    if (columnType != ColumnType.UNDEFINED && columnType != columnType0) {
                        throw SqlException.position(position)
                                .put("non-timestamp column in timestamp expression: ")
                                .put(ColumnType.nameOf(columnType0));
                    }
                    columnType = columnType0;
                    break;
                case ColumnType.DATE:
                    if (columnType != ColumnType.UNDEFINED && columnType != columnType0) {
                        throw SqlException.position(position)
                                .put("non-date column in date expression: ")
                                .put(ColumnType.nameOf(columnType0));
                    }
                    columnType = columnType0;
                    break;
                default:
                    boolean numeric = isNumeric(columnType);
                    if ((columnType != ColumnType.UNDEFINED && !numeric) || (!isNumeric(columnType0) && numeric)) {
                        throw SqlException.position(position)
                                .put("non-numeric column in numeric expression: ")
                                .put(ColumnType.nameOf(columnType0));
                    }
                    columnType = columnType0;
                    break;
            }
        }
    }

    /**
     * A lightweight visitor that pre-scans the expression tree to detect if scalar mode
     * will be used by the JIT backend.
     * <p>
     * This detector is run BEFORE predicate reordering and short-circuit serialization
     * to determine if short-circuit optimizations can be safely applied. Short-circuit
     * evaluation (AND_SC, OR_SC opcodes) only works correctly in scalar mode because
     * SIMD processes multiple rows in parallel and cannot branch per-lane.
     * <p>
     * Scalar mode is guaranteed when columns of different sizes are found (mixed sizes),
     * which sets exec_hint to EXEC_HINT_MIXED_SIZE_TYPE, forcing the scalar code path.
     */
    private class ScalarModeDetector implements PostOrderTreeTraversalAlgo.Visitor, Mutable {
        private final TypesObserver typesObserver = new TypesObserver();

        @Override
        public void clear() {
            typesObserver.clear();
        }

        @Override
        public boolean descend(ExpressionNode node) {
            return true; // Always descend
        }

        @Override
        public void visit(ExpressionNode node) {
            if (node.type == ExpressionNode.LITERAL) {
                int columnIndex = metadata.getColumnIndexQuiet(node.token);
                if (columnIndex != -1) {
                    int columnType = metadata.getColumnType(columnIndex);
                    int typeCode = columnTypeCode(ColumnType.tagOf(columnType));
                    typesObserver.observe(typeCode);
                }
            }
        }

        boolean hasMixedSizes() {
            return typesObserver.hasMixedSizes();
        }
    }
}
