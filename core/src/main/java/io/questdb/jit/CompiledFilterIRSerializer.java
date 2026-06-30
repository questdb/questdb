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
    public static final int AND = 6; // a && b
    public static final int AND_SC = 18; // short-circuit AND: if false, jump to label[payload] (0 = next_row)
    public static final int BEGIN_SC = 20; // create label at index payload
    public static final int BINARY_HEADER_TYPE = 8;
    public static final int DIV = 17; // a / b
    public static final int END_SC = 21; // bind label at index payload
    public static final int EQ = 8; // a == b
    public static final int F4_TYPE = 3;
    public static final int F8_TYPE = 5;
    public static final int GE = 13; // a >= b
    public static final int GT = 12; // a >  b
    public static final int I16_TYPE = 6;
    // Options:
    // Data types
    public static final int I1_TYPE = 0;
    public static final int I2_TYPE = 1;
    public static final int I4_TYPE = 2;
    public static final int I8_TYPE = 4;
    // Constants
    public static final int IMM = 1;
    public static final int LE = 11; // a <= b
    public static final int LT = 10; // a <  b
    // Columns
    public static final int MEM = 2;
    public static final int MUL = 16; // a * b
    public static final int NE = 9; // a != b
    // Operator codes
    public static final int NEG = 4; // -a
    public static final int NOT = 5; // !a
    public static final int OR = 7; // a || b
    public static final int OR_SC = 19;  // short-circuit OR: if true, jump to label[payload] (0 = next_row)
    // Opcodes:
    // Return code. Breaks the loop
    public static final int RET = 0; // ret
    public static final int STRING_HEADER_TYPE = 7;
    public static final int SUB = 15; // a - b
    public static final int SX_I64 = 22; // sign-extend top of stack to i64
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
    // Overflowing constant-arithmetic fold roots that the Java filter reads at
    // long width (a genuine LONG leaf, or a LONG operand promoting the enclosing
    // arithmetic op / comparison), so descend() must emit a full I8 IMM rather
    // than a wrapped I4. Compared by identity. See markI64WidenFoldRoots.
    private final ObjList<ExpressionNode> i64WidenFoldRoots = new ObjList<>();
    // Leaf nodes (column / bind variable / constant) the float-suppressed
    // narrow-i64 widening must sign-extend to i64 for the current predicate.
    // Holds node references, compared by identity. See markFloatI64WidenLeaves.
    private final ObjList<ExpressionNode> i64WidenLeaves = new ObjList<>();
    private final NarrowI64WidenDetector narrowI64WidenDetector = new NarrowI64WidenDetector();
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

        // Constant integer arithmetic subtree that overflows INT. The Java
        // filter wraps a pure-INT subtree (getInt() mod 2^32) but evaluates a
        // LONG-typed subtree at full width (getLong(), never wraps). Mirror
        // that: emit a full-width I8 IMM when the fold root is read at long
        // width and a wrapped I4 IMM otherwise. markI64WidenFoldRoots tags the
        // long-width roots up front from each fold's OWN comparison/arithmetic
        // context (a LONG leaf, or a LONG operand promoting the enclosing
        // comparison) -- a per-comparison signal, so a predicate that mixes
        // widths across a boolean equality of two comparisons gets each fold
        // right instead of forcing one width on all of them.
        if (predicateContext.isActive() && node.type == ExpressionNode.OPERATION) {
            try {
                long longVal = tryFoldConstantArith(node);
                if ((int) longVal != longVal) {
                    // Children are skipped, so flag the fold root as arithmetic
                    // and observe the emitted IMM (markFoldedI4Imm/I8Imm) for the
                    // scalar-mode forcer and getExecHint() mixed-size detection.
                    if (isI64WidenFoldRoot(node)) {
                        predicateContext.markFoldedI8Imm();
                        putOperand(IMM, I8_TYPE, longVal);
                    } else {
                        // INT-width comparison: replicate the Java filter's per-op
                        // INT wrapping (getInt() recurses getInt()), which differs
                        // from (int) longVal for a non-modular operator such as
                        // division, e.g. (1000000 * 1000000) / 7.
                        int intVal = tryFoldConstantArithI4(node);
                        predicateContext.markFoldedI4Imm();
                        putOperand(IMM, I4_TYPE, intVal);
                    }
                    return false;
                }
            } catch (NumericException ignored) {
                // Not a pure-constant integer arithmetic subtree; descend normally.
            }
        }

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
                    serializeColumn(node, node.position, node.token);
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

            // Force scalar mode if the predicate has byte or short arithmetic operations.
            // SIMD mode operates at byte/short width and overflows on intermediate values
            // (e.g. SHORT * SHORT for c1=200 yields -25536, not 40000); scalar mode upcasts
            // to int. This applies whether the predicate is all-narrow (maxSize <= 2) or
            // mixes narrow with wider operands -- both are unsafe under SIMD.
            // Also force scalar when narrow-to-i64 widening is in play; SX_I64 is not
            // implemented on the SIMD path (see avx2.h) and the compiled SIMD filter
            // would silently miscompare a sign-extended narrow column against an i64
            // operand. The float-suppressed widen set (i64WidenLeaves) emits SX_I64
            // too, so it forces scalar as well.
            forceScalarMode |= predicateContext.hasArithmeticOperations
                    && (predicateContext.localTypesObserver.maxSize() <= 2
                    || predicateContext.localTypesObserver.hasNarrowInt()
                    || predicateContext.needsNarrowI64Widening)
                    || i64WidenLeaves.size() > 0;

            // Then backfill constants and symbol bind variables and clean up
            try {
                backfillNodes.forEach(backfillNodeConsumer);
                backfillNodes.clear();
            } catch (SqlWrapperException e) {
                throw e.wrappedException;
            }
        }
    }

    private void addNarrowLeaf(ExpressionNode node) {
        int typeCode = arithExprType(node);
        if (typeCode == I1_TYPE || typeCode == I2_TYPE || typeCode == I4_TYPE) {
            i64WidenLeaves.add(node);
        }
    }

    /**
     * Classifies the arithmetic result type code of a numeric expression
     * subtree (column / bind variable / numeric constant / + - * / over them),
     * or {@link #UNDEFINED_CODE} for anything that is not pure numeric
     * arithmetic. Mirrors the implicit-promotion rules the Java filter applies,
     * so {@link #markFloatI64WidenLeaves} can find the LONG-width subtrees whose
     * narrow operands the Java filter reads at 64 bits.
     */
    private int arithExprType(ExpressionNode node) {
        if (node == null) {
            return UNDEFINED_CODE;
        }
        switch (node.type) {
            case ExpressionNode.LITERAL: {
                int index = metadata.getColumnIndexQuiet(node.token);
                if (index == -1) {
                    return UNDEFINED_CODE;
                }
                return columnTypeCode(ColumnType.tagOf(metadata.getColumnType(index)));
            }
            case ExpressionNode.BIND_VARIABLE: {
                Function bindFunction = lookupBindVariable(node.token);
                return bindFunction != null
                        ? bindVariableTypeCode(ColumnType.tagOf(bindFunction.getType()))
                        : UNDEFINED_CODE;
            }
            case ExpressionNode.CONSTANT: {
                int typeCode = floatConstantTypeCode(node.token);
                if (typeCode != UNDEFINED_CODE) {
                    return typeCode;
                }
                typeCode = longConstantTypeCode(node.token);
                if (typeCode != UNDEFINED_CODE) {
                    return typeCode;
                }
                // Plain int literal stays I4; non-numeric tokens are not arithmetic.
                try {
                    Numbers.parseInt(node.token);
                    return I4_TYPE;
                } catch (NumericException notInt) {
                    return UNDEFINED_CODE;
                }
            }
            case ExpressionNode.OPERATION: {
                if (node.paramCount == 1 && Chars.equals(node.token, '-')) {
                    return arithExprType(node.rhs != null ? node.rhs : node.lhs);
                }
                if (!isArithmeticOperation(node)) {
                    return UNDEFINED_CODE;
                }
                // A pure-constant subtree folds in descend(), so type it by the
                // folded value's width; it never wraps at runtime.
                try {
                    long folded = tryFoldConstantArith(node);
                    return (int) folded != folded ? I8_TYPE : I4_TYPE;
                } catch (NumericException notConstant) {
                    // Not pure-constant; fall through to operand promotion.
                }
                return promoteArithType(arithExprType(node.lhs), arithExprType(node.rhs));
            }
            default:
                return UNDEFINED_CODE;
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
            case ColumnType.VARCHAR, ColumnType.VARCHAR_SLICE -> VARCHAR_HEADER_TYPE;
            default -> UNDEFINED_CODE;
        };
    }

    /**
     * Returns {@link #F4_TYPE} or {@link #F8_TYPE} when {@code token} is the
     * lexical form of a FLOAT or DOUBLE constant, {@link #UNDEFINED_CODE}
     * otherwise. Recognises a trailing {@code 'f'/'F'} suffix as FLOAT, and
     * a {@code '.'}, {@code 'e'} or {@code 'E'} character anywhere in the
     * token as DOUBLE; quoted, prefixed and bind-variable-shaped tokens
     * return {@code UNDEFINED_CODE} so {@link NarrowI64WidenDetector}
     * doesn't mistake a date string or geo-hash literal for a numeric
     * constant. Used by the narrow-widen pre-pass to suppress widening when
     * the predicate has a FLOAT/DOUBLE source.
     */
    private static int floatConstantTypeCode(CharSequence token) {
        int len = token.length();
        if (len == 0) {
            return UNDEFINED_CODE;
        }
        // Reserved literal keywords (true / false) embed an 'e' that would
        // otherwise trip the float scan below. Bail out before the numeric
        // shape checks; null is rejected here too for symmetry with
        // longConstantTypeCode, even though its shape would not match this
        // detector.
        if (isReservedConstantKeyword(token)) {
            return UNDEFINED_CODE;
        }
        char first = token.charAt(0);
        if (first == '\'' || first == '"' || first == '`' || first == '#' || first == ':') {
            return UNDEFINED_CODE;
        }
        char last = token.charAt(len - 1);
        if ((last == 'f' || last == 'F') && len > 1) {
            return F4_TYPE;
        }
        for (int i = 0; i < len; i++) {
            char c = token.charAt(i);
            if (c == '.' || c == 'e' || c == 'E') {
                return F8_TYPE;
            }
        }
        return UNDEFINED_CODE;
    }

    /**
     * Folds one operand's {@link #genuineArithType} into a running
     * comparison-width accumulator. Unlike {@link #promoteArithType}, a
     * non-numeric ({@link #UNDEFINED_CODE}) operand is treated as identity
     * rather than absorbing the result: an IN list keeps its column operand last
     * in {@code args} (with {@code lhs} / {@code rhs} null in the multi-value
     * form), so a plain promote seeded from the null operands would stay
     * UNDEFINED and read a LONG-width fold as a wrapped I4.
     */
    private int foldCmpType(int cmpType, ExpressionNode operand) {
        int operandType = genuineArithType(operand);
        if (operandType == UNDEFINED_CODE) {
            return cmpType;
        }
        if (cmpType == UNDEFINED_CODE) {
            return operandType;
        }
        return promoteArithType(cmpType, operandType);
    }

    /**
     * Real arithmetic result type of a numeric subtree, evaluated with the
     * actual Java function types. Unlike {@link #arithExprType}, an overflowing
     * pure-constant INT subtree stays {@link #I4_TYPE} (the runtime MulInt /
     * AddInt / DivInt keeps INT and wraps under getInt()), not {@link #I8_TYPE}.
     * This lets {@link #markI64WidenFoldRoots} tell a genuine LONG-width
     * ancestor (e.g. {@code c0_long + ...}, which reads its operand via getLong()
     * at full width) from a fake one promoted only by a fold overflow (e.g.
     * {@code c8_int + (const * const)}, which the Java filter still reads via
     * getInt() and wraps).
     */
    private int genuineArithType(ExpressionNode node) {
        if (node != null && node.type == ExpressionNode.OPERATION) {
            if (node.paramCount == 1 && Chars.equals(node.token, '-')) {
                return genuineArithType(node.rhs != null ? node.rhs : node.lhs);
            }
            if (!isArithmeticOperation(node)) {
                return UNDEFINED_CODE;
            }
            return promoteArithType(genuineArithType(node.lhs), genuineArithType(node.rhs));
        }
        // Leaves (column / bind variable / constant) carry their real type
        // already; only the OPERATION fold-overflow shortcut differs.
        return arithExprType(node);
    }

    private static boolean isArithmeticOperation(ExpressionNode node) {
        final CharSequence token = node.token;
        if (node.paramCount < 2) {
            return false;
        }
        return Chars.equals(token, '+') || Chars.equals(token, '-')
                || Chars.equals(token, '*') || Chars.equals(token, '/');
    }

    /**
     * Reports whether {@code node} is the kind of pure-constant integer
     * arithmetic subtree that {@link #descend} collapses into a single IMM, i.e.
     * it folds via {@link #tryFoldConstantArith} and the long-width result
     * overflows int.
     */
    private boolean isFoldableOverflowConst(ExpressionNode node) {
        try {
            long v = tryFoldConstantArith(node);
            return (int) v != v;
        } catch (NumericException notConstant) {
            return false;
        }
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

    private static boolean isReservedConstantKeyword(CharSequence token) {
        return SqlKeywords.isNullKeyword(token)
                || SqlKeywords.isTrueKeyword(token)
                || SqlKeywords.isFalseKeyword(token);
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

    // A leaf operand the IR emits as a single value: a column, bind variable,
    // numeric constant, or the unary-minus node descend() stubs for a negative
    // numeric literal.
    private static boolean isWidenableLeaf(ExpressionNode node) {
        if (node == null) {
            return false;
        }
        switch (node.type) {
            case ExpressionNode.LITERAL:
            case ExpressionNode.BIND_VARIABLE:
            case ExpressionNode.CONSTANT:
                return true;
            case ExpressionNode.OPERATION:
                if (node.paramCount == 1 && Chars.equals(node.token, '-')) {
                    ExpressionNode operand = node.rhs != null ? node.rhs : node.lhs;
                    return operand != null && operand.type == ExpressionNode.CONSTANT;
                }
        }
        return false;
    }

    /**
     * Returns {@link #I8_TYPE} when {@code token} is the lexical form of a
     * LONG integer constant, {@link #UNDEFINED_CODE} otherwise. Recognises a
     * trailing {@code 'L'/'l'} suffix or a magnitude that overflows
     * {@code int}; non-numeric and float-shaped tokens return
     * {@code UNDEFINED_CODE}. Used by the narrow-widen pre-pass so a literal
     * LONG operand alone (e.g. {@code c4 * c8 >= -432577L}) is enough to
     * pull narrow integer operands up to i64 and match the Java filter's
     * {@code MulInt.getLong} long-width arithmetic.
     */
    private static int longConstantTypeCode(CharSequence token) {
        int len = token.length();
        if (len == 0) {
            return UNDEFINED_CODE;
        }
        // Reserved literal keywords (null / NULL, true, false) end in
        // 'l' / 'e' and would otherwise be folded into a bogus I8
        // observation by the suffix check below. They have their own
        // dedicated emission paths in serializeConstant and must not
        // influence the narrow-widen pre-pass.
        if (isReservedConstantKeyword(token)) {
            return UNDEFINED_CODE;
        }
        char first = token.charAt(0);
        if (first == '\'' || first == '"' || first == '`' || first == '#' || first == ':') {
            return UNDEFINED_CODE;
        }
        // Floats are detected separately; do not classify them as LONG.
        if (floatConstantTypeCode(token) != UNDEFINED_CODE) {
            return UNDEFINED_CODE;
        }
        char last = token.charAt(len - 1);
        if (last == 'L' || last == 'l') {
            return I8_TYPE;
        }
        // No suffix: classify by magnitude. A parseInt success means the
        // value fits in i32; a parseLong success after parseInt failure means
        // it doesn't and the constant is effectively LONG.
        try {
            Numbers.parseInt(token);
            return UNDEFINED_CODE;
        } catch (NumericException ignored) {
        }
        try {
            Numbers.parseLong(token);
            return I8_TYPE;
        } catch (NumericException ignored) {
        }
        return UNDEFINED_CODE;
    }

    /**
     * Promotes two arithmetic operand type codes to the result type code of a
     * binary arithmetic operation, following QuestDB's widening rules: a
     * DOUBLE / FLOAT operand makes the result floating point, otherwise the
     * result is the wider of the two integer widths. Returns
     * {@link #UNDEFINED_CODE} when either operand is not a recognised numeric
     * type.
     */
    private static int promoteArithType(int a, int b) {
        if (a == UNDEFINED_CODE || b == UNDEFINED_CODE) {
            return UNDEFINED_CODE;
        }
        if (a == F8_TYPE || b == F8_TYPE) {
            return F8_TYPE;
        }
        if (a == F4_TYPE || b == F4_TYPE) {
            return F4_TYPE;
        }
        // Integer widths order as I1=0 < I2=1 < I4=2 < I8=4, so the wider type
        // is the larger code.
        return Math.max(a, b);
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

        serializeConstant(offset, position, token, negate, isI64WidenLeaf(node));
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
                case SX_I64:
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
                    // serializeNull() emits IMM with I8_TYPE / I4_TYPE for var-size
                    // header NULL sentinels, so the rule above does not catch
                    // <varsize> >= null and similar. Only IS [NOT] NULL, which
                    // lowers to EQ/NE against the NULL header IMM, is meaningful
                    // for var-size operands; every other operator must fall back
                    // to the Java filter.
                    if ((isVarSizeType(lhsType) || isVarSizeType(rhsType))
                            && opCode != EQ && opCode != NE) {
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

    // Reference (not value) membership: the same node objects are marked and folded.
    private boolean isI64WidenFoldRoot(ExpressionNode node) {
        for (int i = 0, n = i64WidenFoldRoots.size(); i < n; i++) {
            if (i64WidenFoldRoots.getQuick(i) == node) {
                return true;
            }
        }
        return false;
    }

    // Reference (not value) membership: the same node objects are marked and serialized.
    private boolean isI64WidenLeaf(ExpressionNode node) {
        for (int i = 0, n = i64WidenLeaves.size(); i < n; i++) {
            if (i64WidenLeaves.getQuick(i) == node) {
                return true;
            }
        }
        return false;
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

    private Function lookupBindVariable(CharSequence token) {
        BindVariableService svc = executionContext.getBindVariableService();
        if (svc == null || token == null || token.isEmpty()) {
            return null;
        }
        if (token.charAt(0) == ':') {
            return svc.getFunction(token);
        }
        try {
            int idx = Numbers.parseInt(token, 1, token.length());
            if (idx >= 1) {
                return svc.getFunction(idx - 1);
            }
        } catch (NumericException ignore) {
        }
        return null;
    }

    /**
     * Sign-extends a narrow integer leaf to i64 so the next arithmetic op
     * dispatches to int64_*, matching the Java filter's MulInt / AddInt#getLong.
     * Fires either for the whole predicate (NarrowI64WidenDetector saw
     * arithmetic + LONG + INT, no float) or, when a float suppresses that, just
     * for the leaves under a LONG-width subtree that {@code node} belongs to.
     */
    private void maybeEmitI64Widening(ExpressionNode node, int typeCode) {
        if (typeCode != I1_TYPE && typeCode != I2_TYPE && typeCode != I4_TYPE) {
            return;
        }
        if (predicateContext.needsNarrowI64Widening || isI64WidenLeaf(node)) {
            putOperator(SX_I64);
        }
    }

    /**
     * Marks the narrow-int leaves that need i64 widening when a float source
     * suppresses the global widening. The Java filter computes a LONG-width
     * arithmetic subtree at 64 bits (operands flow through #getLong), so an
     * INT-width op inside it must not wrap mod 2^32. Only the operands of a
     * narrow-width op computed at 64 bits need sign-extending: a wide (I8) op
     * already promotes its narrow operand through convert().
     */
    private void markFloatI64WidenLeaves(ExpressionNode node) {
        markI64Widen(node, false);
    }

    // underLong: whether the parent reads `node` at 64-bit width.
    private void markI64Widen(ExpressionNode node, boolean underLong) {
        if (node == null || node.type != ExpressionNode.OPERATION) {
            return; // a bare leaf is widened, if needed, by its parent below
        }
        if (isArithmeticOperation(node)) {
            int type = arithExprType(node);
            boolean thisLong = underLong || type == I8_TYPE;
            markI64WidenOperand(node.lhs, type, thisLong);
            markI64WidenOperand(node.rhs, type, thisLong);
            return;
        }
        if (node.paramCount == 1 && Chars.equals(node.token, '-')) {
            markI64Widen(node.rhs != null ? node.rhs : node.lhs, underLong);
            return;
        }
        // Comparison / boolean / IN / NOT: a narrow operand is read at 32 bits
        // (via getInt) unless it is itself I8-typed.
        markI64Widen(node.lhs, arithExprType(node.lhs) == I8_TYPE);
        markI64Widen(node.rhs, arithExprType(node.rhs) == I8_TYPE);
        for (int i = 0, n = node.args.size(); i < n; i++) {
            ExpressionNode arg = node.args.getQuick(i);
            markI64Widen(arg, arithExprType(arg) == I8_TYPE);
        }
    }

    /**
     * Marks the overflowing constant-arithmetic fold roots that the Java filter
     * reads at long width, so {@link #descend} emits a full I8 IMM for them
     * rather than a wrapped I4 (which would drop the high bits the getLong() path
     * reads). A fold root is long-width when it is genuinely LONG-typed (a LONG
     * leaf, e.g. {@code 2 * 5000000000L}) or when a LONG operand promotes an
     * enclosing arithmetic op (e.g. {@code c0_long + (258558 * -259815)}) or
     * comparison (e.g. {@code 1000000 * 1000000 > c0_long}) to long width.
     * <p>
     * The decision is derived per fold from its OWN context, not from a
     * predicate-global flag: a boolean equality of two comparisons --
     * {@code (cmp) = (cmp)} -- is a single predicate that can mix an INT-width
     * comparison with a LONG-width one, and each fold must take the width of the
     * comparison it actually feeds.
     * <p>
     * At a non-arithmetic boundary (comparison / boolean / IN / NOT) the walk
     * promotes the node's operand types: an integer comparison with a LONG
     * operand reads all its operands at long width, while a FLOAT/DOUBLE operand
     * promotes to floating point -- there a bare INT fold is read via getInt()
     * and wraps (cmpLong stays false), but a genuinely LONG-typed operand is
     * still read at long width and the arithmetic branch re-marks the folds
     * beneath it via {@link #genuineArithType}.
     * <p>
     * An IN / NOT IN list is an {@link ExpressionNode#FUNCTION} node, not an
     * OPERATION, so it is let through to the boundary handling as well. A value
     * list of two or more elements keeps its operands in {@code args} as
     * {@code [elements..., key]} (key last, {@code lhs} / {@code rhs} null); each
     * element compares against the key independently (OR of equals), so its width
     * is derived per element from key-vs-element via {@link #foldCmpType} -- a
     * coexisting genuine-LONG element must not promote (and widen) an overflowing
     * INT element that the key compares at INT width. The single-value form keeps
     * its key / element in {@code lhs} / {@code rhs} (args empty) and is paired by
     * the comparison handling, which treats a non-numeric operand as identity
     * rather than letting it absorb the width to UNDEFINED.
     */
    private void markI64WidenFoldRoots(ExpressionNode node, boolean underGenuineLong) {
        if (node == null) {
            return;
        }
        // An IN / NOT IN list is a FUNCTION node rather than an OPERATION; let it
        // through so the boundary handling below can mark an overflowing fold in
        // its value list. Every other FUNCTION (e.g. a scalar call) stops here --
        // descend() does not fold across it.
        if (node.type != ExpressionNode.OPERATION
                && !(node.type == ExpressionNode.FUNCTION && SqlKeywords.isInKeyword(node.token))) {
            return;
        }
        if (isArithmeticOperation(node) || (node.paramCount == 1 && Chars.equals(node.token, '-'))) {
            boolean nodeGenuineLong = underGenuineLong || genuineArithType(node) == I8_TYPE;
            if (nodeGenuineLong && isFoldableOverflowConst(node)) {
                i64WidenFoldRoots.add(node);
                return; // folds to one IMM; no deeper fold root to find
            }
            markI64WidenFoldRoots(node.lhs, nodeGenuineLong);
            markI64WidenFoldRoots(node.rhs, nodeGenuineLong);
            return;
        }
        // An IN value list (args = [elements..., key], the key last) is an OR of
        // independent equality checks. Derive each element's fold width from
        // key-vs-that-element, NOT from one width folded across the whole list: a
        // narrow / INT key compares (and wraps, via getInt) an overflowing INT
        // element even when a coexisting genuine-LONG element would, on its own, be
        // read at long width. A single list-wide width would let that LONG element
        // promote the INT element to I8 and widen it, diverging from the Java InLong
        // path that wraps it. The single-value IN keeps its key / element in lhs /
        // rhs (args empty) and falls through to the comparison handling below, which
        // already pairs exactly those two.
        if (node.type == ExpressionNode.FUNCTION && SqlKeywords.isInKeyword(node.token) && node.args.size() > 0) {
            final ExpressionNode key = node.args.getLast();
            final int keyType = genuineArithType(key);
            for (int i = 0, n = node.args.size() - 1; i < n; i++) {
                final ExpressionNode element = node.args.getQuick(i);
                markI64WidenFoldRoots(element, foldCmpType(keyType, element) == I8_TYPE);
            }
            // The key reads at its own genuine width (a narrow / INT key wraps, a
            // LONG key widens); the elements never promote it.
            markI64WidenFoldRoots(key, false);
            return;
        }
        int cmpType = foldCmpType(UNDEFINED_CODE, node.lhs);
        cmpType = foldCmpType(cmpType, node.rhs);
        for (int i = 0, n = node.args.size(); i < n; i++) {
            cmpType = foldCmpType(cmpType, node.args.getQuick(i));
        }
        boolean cmpLong = cmpType == I8_TYPE;
        markI64WidenFoldRoots(node.lhs, cmpLong);
        markI64WidenFoldRoots(node.rhs, cmpLong);
        for (int i = 0, n = node.args.size(); i < n; i++) {
            markI64WidenFoldRoots(node.args.getQuick(i), cmpLong);
        }
    }

    private void markI64WidenOperand(ExpressionNode child, int parentType, boolean parentLong) {
        if (parentLong && parentType != I8_TYPE && isWidenableLeaf(child)) {
            addNarrowLeaf(child);
        } else {
            markI64Widen(child, parentLong);
        }
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
            maybeEmitI64Widening(node, typeCode);
        } else {
            throw SqlException.position(node.position)
                    .put("bind variable outside of predicate: ")
                    .put(node.token);
        }
    }

    private void serializeColumn(ExpressionNode node, int position, final CharSequence token) throws SqlException {
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
            maybeEmitI64Widening(node, typeCode);
        } else {
            throw SqlException.position(position)
                    .put("non-boolean column outside of predicate: ")
                    .put(token);
        }
    }

    private void serializeConstant(long offset, int position, final CharSequence token, boolean negated, boolean widenToI64) throws SqlException {
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
            serializeUntypedNumber(offset, position, token, negated, widenToI64);
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
                case I1_TYPE: {
                    // Range-check before narrowing; an out-of-range int literal would
                    // silently fold to the column's low byte and admit rows that the
                    // scalar Java filter (which widens column to int) correctly rejects.
                    // Throwing SqlException here makes SqlCodeGenerator fall back to
                    // the Java filter, which evaluates the comparison at int width.
                    final long bImm = sign * Numbers.parseInt(token);
                    if (bImm < Byte.MIN_VALUE || bImm > Byte.MAX_VALUE) {
                        throw SqlException.position(position)
                                .put("byte literal out of range: ").put(token);
                    }
                    putOperand(offset, IMM, I1_TYPE, bImm);
                    break;
                }
                case I2_TYPE: {
                    final long sImm = sign * Numbers.parseInt(token);
                    if (sImm < Short.MIN_VALUE || sImm > Short.MAX_VALUE) {
                        throw SqlException.position(position)
                                .put("short literal out of range: ").put(token);
                    }
                    putOperand(offset, IMM, I2_TYPE, sImm);
                    break;
                }
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

    private void serializeUntypedNumber(long offset, int position, final CharSequence token, boolean negated, boolean widenToI64) throws SqlException {
        long sign = negated ? -1 : 1;

        // Emit the constant as I8 when the predicate computes at long width and
        // has no float (SubLong / AddLong reach into MulInt.getLong), or when
        // markFloatI64WidenLeaves tagged this constant as living under a
        // LONG-width subtree despite a float elsewhere. Otherwise keep it I4 so
        // int32_mul wraps mod 2^32 on both the JIT and Java sides.
        boolean keepI4 = (!predicateContext.localTypesObserver.hasI8() || predicateContext.hasFloatInPredicate)
                && !widenToI64;
        if (keepI4) {
            try {
                final int i = Numbers.parseInt(token);
                putOperand(offset, IMM, I4_TYPE, sign * i);
                return;
            } catch (NumericException ignore) {
            }
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

    /**
     * Evaluates node as a pure-constant integer arithmetic subtree at long
     * precision and returns the result; throws {@link NumericException} if
     * any descendant is non-constant, not an integer literal, or the subtree
     * uses an operator other than {@code + - * /}. Mirrors the int-vs-long
     * check that {@code FunctionParser.functionToConstant0} uses to decide
     * whether to fold an INT-typed function to a LongConstant; callers that
     * want the Java filter's fold behavior compare {@code (int) longVal}
     * against {@code longVal} and treat a mismatch as a fold root.
     */
    private long tryFoldConstantArith(ExpressionNode node) throws NumericException {
        if (node == null) {
            throw NumericException.INSTANCE;
        }
        if (node.type == ExpressionNode.CONSTANT) {
            return Numbers.parseLong(node.token);
        }
        if (node.type != ExpressionNode.OPERATION) {
            throw NumericException.INSTANCE;
        }
        // Unary minus: parser builds OPERATION "-" with rhs only.
        if (Chars.equals(node.token, '-') && node.lhs == null) {
            return -tryFoldConstantArith(node.rhs);
        }
        long left = tryFoldConstantArith(node.lhs);
        long right = tryFoldConstantArith(node.rhs);
        if (Chars.equals(node.token, '+')) {
            return left + right;
        }
        if (Chars.equals(node.token, '-')) {
            return left - right;
        }
        if (Chars.equals(node.token, '*')) {
            return left * right;
        }
        if (Chars.equals(node.token, '/')) {
            if (right == 0L) {
                throw NumericException.INSTANCE;
            }
            return left / right;
        }
        throw NumericException.INSTANCE;
    }

    /**
     * INT-width counterpart of {@link #tryFoldConstantArith}: evaluates the
     * pure-constant integer arithmetic subtree with per-operation {@code int}
     * wrapping, matching the Java filter's getInt() recursion (each MulInt /
     * DivInt / AddInt computes at i32 and wraps mod 2^32). Used for the I4 IMM a
     * fold root emits at an INT-width comparison, where {@code (int) longVal}
     * from the long fold would diverge for a non-modular operator such as
     * division. Throws {@link NumericException} on the same conditions as
     * {@link #tryFoldConstantArith} (plus an int-width division by zero), so the
     * caller cleanly falls back to descending the subtree as per-op IR.
     */
    private int tryFoldConstantArithI4(ExpressionNode node) throws NumericException {
        if (node == null) {
            throw NumericException.INSTANCE;
        }
        if (node.type == ExpressionNode.CONSTANT) {
            return (int) Numbers.parseLong(node.token);
        }
        if (node.type != ExpressionNode.OPERATION) {
            throw NumericException.INSTANCE;
        }
        // Unary minus: parser builds OPERATION "-" with rhs only. NegInt#getInt
        // propagates INT_NULL instead of negating the sentinel.
        if (Chars.equals(node.token, '-') && node.lhs == null) {
            int operand = tryFoldConstantArithI4(node.rhs);
            return operand == Numbers.INT_NULL ? Numbers.INT_NULL : -operand;
        }
        int left = tryFoldConstantArithI4(node.lhs);
        int right = tryFoldConstantArithI4(node.rhs);
        // MulInt / AddInt / SubInt / DivInt#getInt return INT_NULL when either
        // operand is INT_NULL, so an inner product that wraps exactly onto the
        // -2^31 sentinel poisons the rest of the fold to NULL instead of feeding
        // a wrapped value. Without this the JIT kept computing modular arithmetic
        // (e.g. (65536 * 32768) * 2 = 0) while the Java filter collapsed to NULL.
        if (left == Numbers.INT_NULL || right == Numbers.INT_NULL) {
            return Numbers.INT_NULL;
        }
        if (Chars.equals(node.token, '+')) {
            return left + right;
        }
        if (Chars.equals(node.token, '-')) {
            return left - right;
        }
        if (Chars.equals(node.token, '*')) {
            return left * right;
        }
        if (Chars.equals(node.token, '/')) {
            if (right == 0) {
                throw NumericException.INSTANCE;
            }
            return left / right;
        }
        throw NumericException.INSTANCE;
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

        public boolean hasFloat() {
            return sizes[F4_INDEX] != 0 || sizes[F8_INDEX] != 0;
        }

        public boolean hasI4() {
            return sizes[I4_INDEX] != 0;
        }

        public boolean hasI8() {
            return sizes[I8_INDEX] != 0;
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

        public boolean hasNarrowInt() {
            return sizes[I1_INDEX] != 0 || sizes[I2_INDEX] != 0;
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
     * Per-predicate pre-pass that decides whether the JIT IR emitter should
     * widen narrow integer operands to i64 before arithmetic ops. Triggers when
     * the predicate has integer arithmetic AND an I8 operand AND an I4 operand
     * (BYTE / SHORT alone never overflow int32 and stay correct under int32
     * arithmetic; INT can overflow). When triggered, the IR emitter wraps each
     * narrow column / bind variable with `IMM I8 0 + ADD`, which makes the C++
     * convert() promote the value to i64 before mul / add / sub / div
     * dispatches to int64_*. This matches the Java filter's
     * MulInt.getLong / AddInt.getLong, which compute via ((long) l) OP r.
     * <p>
     * A FLOAT or DOUBLE operand anywhere in the predicate suppresses the
     * widening: in that case the int-arithmetic subtree gets consumed by
     * IntFunction#getDouble / IntFunction#getFloat, which call
     * {@link io.questdb.cairo.sql.Function#getInt}, so the Java filter
     * computes at int32 width and wraps modulo 2^32 on overflow. Widening
     * here would let the JIT preserve the full long product and diverge
     * from the Java filter -- the inverse of the bug the pre-pass was
     * introduced to fix. Note that {@link #serializeUntypedNumber} also
     * keeps integer constants at I4 in that case, otherwise the constant
     * widening alone would re-introduce the divergence via convert().
     */
    private class NarrowI64WidenDetector implements PostOrderTreeTraversalAlgo.Visitor, Mutable {
        private final TypesObserver typesObserver = new TypesObserver();
        private boolean hasArithmetic;

        @Override
        public void clear() {
            typesObserver.clear();
            hasArithmetic = false;
        }

        @Override
        public boolean descend(ExpressionNode node) {
            return true;
        }

        @Override
        public void visit(ExpressionNode node) {
            switch (node.type) {
                case ExpressionNode.LITERAL: {
                    int columnIndex = metadata.getColumnIndexQuiet(node.token);
                    if (columnIndex != -1) {
                        int typeCode = columnTypeCode(ColumnType.tagOf(metadata.getColumnType(columnIndex)));
                        typesObserver.observe(typeCode);
                    }
                    break;
                }
                case ExpressionNode.BIND_VARIABLE: {
                    // An unbound or UNDEFINED-typed bind variable is safe to
                    // skip here because serializeBindVariable consults the
                    // same BindVariableService in the same serialize() call
                    // and throws on either condition, aborting JIT compile
                    // and falling back to the Java filter -- so the IR is
                    // never emitted with a missing widening signal.
                    Function bindFunction = lookupBindVariable(node.token);
                    if (bindFunction != null) {
                        int typeCode = bindVariableTypeCode(ColumnType.tagOf(bindFunction.getType()));
                        if (typeCode != UNDEFINED_CODE) {
                            typesObserver.observe(typeCode);
                        }
                    }
                    break;
                }
                case ExpressionNode.CONSTANT: {
                    // Observe FLOAT / DOUBLE numeric constants so a predicate
                    // whose only float source is a literal (e.g. c7 + 0.5)
                    // suppresses narrow-int widening, matching the Java
                    // filter's IntFunction#getDouble path that wraps at int32.
                    // Also observe LONG integer constants (L/l suffix or
                    // magnitude that overflows int) so a predicate whose only
                    // long source is a literal (e.g. c4 * c8 >= -432577L) is
                    // enough to pull narrow operands up to i64, matching the
                    // Java filter's MulInt.getLong long-width arithmetic.
                    int typeCode = floatConstantTypeCode(node.token);
                    if (typeCode == UNDEFINED_CODE) {
                        typeCode = longConstantTypeCode(node.token);
                    }
                    if (typeCode != UNDEFINED_CODE) {
                        typesObserver.observe(typeCode);
                    }
                    break;
                }
                case ExpressionNode.OPERATION:
                    hasArithmetic |= isArithmeticOperation(node);
                    // Overflowing pure-constant subtree folds to an IMM in
                    // descend(). Observe it as I4 (it keeps INT type), so
                    // shouldWiden() fires only on a real LONG operand -- then
                    // descend() wraps it to I4, or widens to I8 alongside it.
                    try {
                        long longVal = tryFoldConstantArith(node);
                        if ((int) longVal != longVal) {
                            typesObserver.observe(I4_TYPE);
                        }
                    } catch (NumericException ignored) {
                        // Not a pure-constant integer arithmetic subtree; nothing to observe.
                    }
                    break;
            }
        }

        boolean hasFloat() {
            return typesObserver.hasFloat();
        }

        boolean shouldWiden() {
            return hasArithmetic && typesObserver.hasI4() && typesObserver.hasI8()
                    && !typesObserver.hasFloat();
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
        // True when the predicate has at least one FLOAT / DOUBLE column,
        // bind variable, or numeric constant. Captured up front by
        // NarrowI64WidenDetector so other code paths (e.g. constant
        // typing in serializeUntypedNumber) can keep INT operands at i32
        // when the int-arithmetic subtree is consumed by IntFunction's
        // getDouble / getFloat, which call getInt() and wrap mod 2^32.
        boolean hasFloatInPredicate;
        // True when the predicate has integer arithmetic mixed with a LONG
        // operand. The IR emitter widens narrow operands to i64 in this case
        // so the JIT computes at long width, matching MulInt.getLong /
        // AddInt.getLong (which promote via ((long) l) OP r).
        boolean needsNarrowI64Widening;
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
                    // Pre-pass: decide whether to widen narrow integer operands
                    // to i64 at IR emission time, and remember whether any
                    // FLOAT / DOUBLE source is present anywhere in the
                    // predicate. See NarrowI64WidenDetector.
                    i64WidenLeaves.clear();
                    i64WidenFoldRoots.clear();
                    try {
                        narrowI64WidenDetector.clear();
                        traverseAlgo.traverse(node, narrowI64WidenDetector);
                        needsNarrowI64Widening = narrowI64WidenDetector.shouldWiden();
                        hasFloatInPredicate = narrowI64WidenDetector.hasFloat();
                    } catch (SqlException ignore) {
                        // Detector does not throw; defensive only.
                        needsNarrowI64Widening = false;
                        hasFloatInPredicate = false;
                    }
                    // Tag the overflowing constant fold roots that the Java
                    // filter reads at long width, so descend() emits a full I8
                    // IMM for exactly those and a wrapped I4 for the rest. The
                    // tag is per-comparison, so it is correct whether or not a
                    // float is present and even when one predicate mixes INT- and
                    // LONG-width comparisons across a boolean equality.
                    markI64WidenFoldRoots(node, false);
                    // A float suppresses the global narrow-i64 widening, but a
                    // LONG-width arithmetic subtree still computes at 64 bits in
                    // the Java filter. Tag the narrow leaves under such subtrees
                    // so the IR sign-extends exactly them, keeping a nested INT
                    // product from wrapping mod 2^32.
                    if (hasFloatInPredicate) {
                        markFloatI64WidenLeaves(node);
                    }
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
            hasFloatInPredicate = false;
            needsNarrowI64Widening = false;
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

        /**
         * I4 counterpart of {@link #markFoldedI8Imm} for an overflowing constant
         * folded to a wrapped I4 IMM (INT-width comparison): flags arithmetic and
         * observes I4 so getExecHint() and the scalar-mode forcer see the operand.
         */
        void markFoldedI4Imm() {
            hasArithmeticOperations = true;
            localTypesObserver.observe(I4_TYPE);
            globalTypesObserver.observe(I4_TYPE);
        }

        /**
         * Records the side effects of emitting a single I8 IMM in place of a
         * folded integer-arithmetic subtree. Sets hasArithmeticOperations
         * unconditionally because the fold root may be a unary minus, which
         * {@link #isArithmeticOperation} would skip due to its paramCount
         * &gt;= 2 gate -- descend only reaches this call after evaluating an
         * integer arithmetic subtree, so the flag is always correct here.
         * Observes I8 in both type observers so getExecHint() sees the
         * predicate's true operand-size diversity. Without the observation,
         * a predicate like c5 (FLOAT) &lt; a_long_const_overflow - (708206 -
         * c5) looks single-size (only F4 from the column) and the backend
         * picks the AVX2 path, which has no convert from a 4-element i64
         * vector to an 8-element f32 vector and produces wrong results for
         * NULL c5.
         */
        void markFoldedI8Imm() {
            hasArithmeticOperations = true;
            localTypesObserver.observe(I8_TYPE);
            globalTypesObserver.observe(I8_TYPE);
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
