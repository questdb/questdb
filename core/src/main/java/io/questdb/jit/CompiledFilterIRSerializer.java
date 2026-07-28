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
import io.questdb.std.DoubleList;
import io.questdb.std.GenericLexer;
import io.questdb.std.IntList;
import io.questdb.std.IntStack;
import io.questdb.std.LongIntHashMap;
import io.questdb.std.LongList;
import io.questdb.std.LongObjHashMap;
import io.questdb.std.Mutable;
import io.questdb.std.Numbers;
import io.questdb.std.NumericException;
import io.questdb.std.ObjHashSet;
import io.questdb.std.ObjIntHashMap;
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
    private static final int EXEC_HINT_WIDE_LANE = 3;
    private static final int INSTRUCTION_SIZE = Integer.BYTES + Integer.BYTES + Long.BYTES + Long.BYTES;
    // Maximum number of labels supported by the backend (must match LabelArray::MAX_LABELS in x86.h)
    private static final int MAX_LABELS = 8;
    // Absent-key marker for the arithmetic type caches. UNDEFINED_CODE is a cacheable answer, so it
    // cannot double as the miss value.
    private static final int NOT_CACHED = Integer.MIN_VALUE;
    // Predicate priority for short-circuit evaluation. Every priority getPredicatePriority() returns
    // falls in [0, PRIORITY_COUNT), which is what lets sortPredicates() order a chain by counting
    // occurrences per priority instead of comparing.
    private static final int PRIORITY_COUNT = 11;
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
    // Node kinds hasWideLaneConversionSource() looks for. See hasWideLaneSourceNode().
    private static final int WIDE_LANE_SOURCE_FLOAT_LEAF = 0;
    private static final int WIDE_LANE_SOURCE_FLOAT_WIDENING_CONST = 1;
    private static final int WIDE_LANE_SOURCE_I8_OPERAND = 3;
    private static final int WIDE_LANE_SOURCE_NARROW_INT_LEAF = 2;
    // Memoizes arithExprType() for the current predicate, keyed by node identity. The classification
    // walks the whole subtree (and folds pure-constant arithmetic), and the marker passes ask for it
    // repeatedly at every level, so without the cache a deep chain costs O(depth^2) subtree walks and
    // re-parses the same constant tokens. The tree does not mutate during serialize(), so the answer
    // is stable, and the entry stays valid for every predicate of the same filter, so clear() is the
    // only reset point - onNodeDescended() deliberately does NOT clear it (see both).
    private final ObjIntHashMap<ExpressionNode> arithExprTypeCache = new ObjIntHashMap<>(16, 0.5, NOT_CACHED);
    // Memoizes pure-constant long arithmetic folds for the current predicate. Zero records a failed
    // fold; positive values are one-based indexes into constantArithFoldValues.
    private final ObjIntHashMap<ExpressionNode> constantArithFoldCache = new ObjIntHashMap<>(16, 0.5, NOT_CACHED);
    private final LongList constantArithFoldValues = new LongList();
    // Memoizes tryFoldConstantArithFloat() for the current predicate. 0 marks a subtree that is
    // not a pure-constant arithmetic one; positive values are one-based indexes into
    // constantFloatFoldValues. Without it descend() re-walks each subtree at every node it
    // contains, which is quadratic in the length of a constant chain. See arithExprTypeCache.
    private final ObjIntHashMap<ExpressionNode> constantFloatFoldCache = new ObjIntHashMap<>(16, 0.5, NOT_CACHED);
    private final DoubleList constantFloatFoldValues = new DoubleList();
    // contains <memory_offset, constant_node> pairs for backfilling purposes
    private final LongObjHashMap<ExpressionNode> backfillNodes = new LongObjHashMap<>();
    // List to collect predicates from AND chains for reordering
    private final ObjList<ExpressionNode> collectedPredicates = new ObjList<>();
    // Memoizes containsFloatExpression() for the current predicate. See arithExprTypeCache.
    private final ObjIntHashMap<ExpressionNode> containsFloatCache = new ObjIntHashMap<>(16, 0.5, NOT_CACHED);
    // Memoizes containsNarrowIntegerValue() for the current predicate. See arithExprTypeCache.
    private final ObjIntHashMap<ExpressionNode> containsNarrowIntCache = new ObjIntHashMap<>(16, 0.5, NOT_CACHED);
    // Leaf nodes (column / bind variable / constant) a comparison or IN pairing must sign-extend
    // to i64 for the current predicate. Holds node references, compared by identity.
    // See markWidthSemantics.
    private final ObjHashSet<ExpressionNode> i64WidenLeaves = new ObjHashSet<>();
    // Untyped NULL elements of an IN whose key reads at INT width. The type observer sees the
    // predicate's narrowest column, so a BYTE operand under an INT-width key (abyte + 1) would type
    // the NULL at I1 and serializeNull declines - BYTE has no sentinel - taking the whole filter
    // off the JIT. The key's own width is what the pairing compares at. Compared by identity.
    private final ObjHashSet<ExpressionNode> intWidthNullElements = new ObjHashSet<>();
    // Integer CONSTANTS a 64-bit comparison or IN pairing must emit at I8 because the peer is
    // 8 bytes wide. Separate from i64WidenLeaves on purpose: this only widens an IMM, it emits no
    // SX_I64, so it must not drag the predicate onto the scalar backend the way a leaf widening
    // does. Compared by identity.
    private final ObjHashSet<ExpressionNode> i64WidenConstants = new ObjHashSet<>();
    // Integer CONSTANT operands of a NARROW arithmetic node. The predicate-wide type observer types
    // every constant at the widest column it saw, so a coexisting LONG operand elsewhere in the
    // predicate would emit the 2 in `i32 * 2` as an I8 immediate and promote the product to
    // int64_mul, which does not wrap where MulInt#getInt does. Compared by identity.
    private final ObjHashSet<ExpressionNode> narrowKeptConstants = new ObjHashSet<>();
    private final NarrowI64WidenDetector narrowI64WidenDetector = new NarrowI64WidenDetector();
    private final PredicateContext predicateContext = new PredicateContext();
    // Memoizes requiresWideLaneArithmetic() for the current predicate. See arithExprTypeCache.
    private final ObjIntHashMap<ExpressionNode> requiresWideLaneArithCache = new ObjIntHashMap<>(16, 0.5, NOT_CACHED);
    // Scratch state for sortPredicates(), reused across filters so ordering a chain allocates nothing
    // on the compile path: the priority of each predicate (getPredicatePriority walks the subtree, so
    // it is computed once per predicate rather than once per comparison), the per-priority bucket
    // offsets, and the reordered chain the sort writes back.
    private final IntList predicatePriorities = new IntList();
    private final IntList predicatePriorityOffsets = new IntList();
    private final ScalarModeDetector scalarModeDetector = new ScalarModeDetector();
    private final StringSink sink = new StringSink();
    private final ObjList<ExpressionNode> sortedPredicates = new ObjList<>();
    private final PostOrderTreeTraversalAlgo traverseAlgo = new PostOrderTreeTraversalAlgo();
    private final IntStack typeStack = new IntStack();
    private ObjList<Function> bindVarFunctions;
    private final LongObjHashMap.LongObjConsumer<ExpressionNode> backfillNodeConsumer = this::backfillNode;
    private SqlExecutionContext executionContext;
    // internal flag used to forcefully enable scalar mode based on filter's contents
    private boolean forceScalarMode;
    private boolean hasEmittedWideLaneConversion;
    private boolean isWideLaneMode;
    private MemoryCARW memory;
    private RecordMetadata metadata;
    private PageFrameCursor pageFrameCursor;

    @Override
    public void clear() {
        memory = null;
        metadata = null;
        pageFrameCursor = null;
        forceScalarMode = false;
        hasEmittedWideLaneConversion = false;
        isWideLaneMode = false;
        predicateContext.clear();
        backfillNodes.clear();
        collectedPredicates.clear();
        // The memo caches below are keyed by ExpressionNode identity and live for a whole filter, so
        // this is their ONLY reset point - the node pool can hand the same objects to the next
        // filter, where the cached answers would no longer describe the same subtree. The mark sets
        // after them are per-predicate state that onNodeDescended also resets before use.
        arithExprTypeCache.clear();
        constantArithFoldCache.clear();
        constantArithFoldValues.clear();
        constantFloatFoldCache.clear();
        constantFloatFoldValues.clear();
        containsFloatCache.clear();
        containsNarrowIntCache.clear();
        requiresWideLaneArithCache.clear();
        i64WidenConstants.clear();
        intWidthNullElements.clear();
        i64WidenLeaves.clear();
        narrowKeptConstants.clear();
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

        // Check if we're at the start of an arithmetic expression.
        predicateContext.onNodeDescended(node);

        // Constant integer arithmetic subtree whose long-width fold does not fit INT. Its width
        // follows its DECLARED type, exactly as FunctionParser#functionToConstant0 folds it: a
        // pure-INT subtree is an IntConstant holding the wrap, and only a genuine LONG operand
        // (arithExprType == I8) makes the subtree LONG and keeps the full value.
        if (predicateContext.isActive() && node.type == ExpressionNode.OPERATION) {
            try {
                long longVal = tryFoldConstantArith(node);
                if ((int) longVal != longVal) {
                    // Children are skipped, so flag the fold root as arithmetic
                    // and observe the emitted IMM (markFoldedI4Imm/I8Imm) for the
                    // scalar-mode forcer and getExecHint() mixed-size detection.
                    if (arithExprType(node) == I8_TYPE) {
                        predicateContext.markFoldedI8Imm();
                        putOperand(IMM, I8_TYPE, longVal);
                    } else {
                        // Replicate the Java filter's per-op INT wrapping (getInt() recurses
                        // getInt()), which differs from (int) longVal for a non-modular operator
                        // such as division, e.g. (1000000 * 1000000) / 7.
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

        // Constant FLOAT/DOUBLE arithmetic subtree that is not finite. FunctionParser folds
        // every constant subtree bottom-up through functionToConstant0, whose FLOAT/DOUBLE
        // arms call FloatConstant#newInstance / DoubleConstant#newInstance - and both map
        // +/-Infinity and NaN onto the NULL sentinel. So the Java filter compares against
        // NULL. The IR carries the operations instead and the backend computes them, which
        // leaves the JIT comparing against a real infinity. The equality opcodes agree
        // either way (double_cmp_epsilon in jit/impl/x86.h calls any two non-finite values
        // equal, exactly as Numbers#equals does), but double_lt/le/gt/ge order an infinity
        // like an ordinary number while the Java filter orders NULL against nothing - so
        // every ordering operator selects a different row set. Decline and let the Java
        // filter evaluate the predicate.
        //
        // An INTEGER-typed subtree is left alone: the block above already mirrors the Java
        // filter's getInt()/getLong() recursion exactly, including its deliberate refusal to
        // fold a zero divisor (tryFoldConstantArith0) so that the native int32_div/int64_div
        // produces the same NULL sentinel DivInt/DivLong do. Judging "10 / 0" by float rules
        // would read it as an infinity and needlessly decline a filter that already agrees.
        //
        // Anything else is judged by the fold, and the fold is fail-CLOSED, because whether a
        // subtree even classifies as floating point depends on three token classifiers agreeing
        // - floatConstantTypeCode/longConstantTypeCode inside arithExprType, the numeric parsers
        // inside the fold, and createConstant inside the parser - and they do not: parseInt
        // takes the underscore separator CLAUDE.md mandates while parseDouble rejects it, and
        // floatConstantTypeCode does not know the 'd' suffix createConstant accepts. A leaf no
        // classifier recognises leaves the node UNDEFINED rather than F4/F8, so UNDEFINED has to
        // decline too: there is no evidence the backend would land on the value the parser folds.
        if (predicateContext.isActive() && node.type == ExpressionNode.OPERATION && isArithmeticOperation(node)) {
            final int arithType = arithExprType(node);
            if (arithType != I1_TYPE && arithType != I2_TYPE && arithType != I4_TYPE && arithType != I8_TYPE) {
                try {
                    if (!Numbers.isFinite(tryFoldConstantArithFloat(node))) {
                        throw SqlException.position(node.position)
                                .put("non-finite constant arithmetic: ").put(node.token);
                    }
                } catch (NumericException notConstant) {
                    // Not a pure-constant arithmetic subtree at all; descend normally.
                }
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

    private boolean isWideLaneEligible(ExpressionNode node) {
        if (node == null) {
            return false;
        }
        if (node.type == ExpressionNode.OPERATION
                && (SqlKeywords.isAndKeyword(node.token) || SqlKeywords.isOrKeyword(node.token))) {
            return isWideLaneEligible(node.lhs) && isWideLaneEligible(node.rhs);
        }
        if (node.type == ExpressionNode.OPERATION && SqlKeywords.isNotKeyword(node.token)) {
            return isWideLaneEligible(node.rhs != null ? node.rhs : node.lhs);
        }
        if (node.type == ExpressionNode.FUNCTION && SqlKeywords.isInKeyword(node.token)) {
            return isWideLaneInEligible(node);
        }
        if (node.type == ExpressionNode.OPERATION && node.paramCount == 2 && isComparisonToken(node.token)) {
            if (isWideLaneIntegerExpression(node.lhs) && isWideLaneIntegerExpression(node.rhs)) {
                return true;
            }
            return isWideLaneFloatComparisonOperand(node.lhs)
                    && isWideLaneFloatComparisonOperand(node.rhs)
                    && (containsFloatExpression(node.lhs) || containsFloatExpression(node.rhs));
        }
        return false;
    }

    private boolean isWideLaneFloatComparisonOperand(ExpressionNode node) {
        return isWideLaneFloatExpression(node) || isWideLaneNumericConstant(node);
    }

    private boolean isWideLaneFloatExpression(ExpressionNode node) {
        if (node == null) {
            return false;
        }
        if (node.type == ExpressionNode.LITERAL || node.type == ExpressionNode.BIND_VARIABLE) {
            final int type = arithExprType(node);
            return type == F4_TYPE || type == F8_TYPE;
        }
        if (node.type == ExpressionNode.OPERATION && node.paramCount == 1 && Chars.equals(node.token, '-')) {
            return isWideLaneFloatExpression(node.rhs != null ? node.rhs : node.lhs);
        }
        if (node.type == ExpressionNode.OPERATION && isArithmeticOperation(node)) {
            return isWideLaneFloatArithmeticOperand(node.lhs)
                    && isWideLaneFloatArithmeticOperand(node.rhs)
                    && (containsFloatExpression(node.lhs) || containsFloatExpression(node.rhs));
        }
        return false;
    }

    private boolean isWideLaneFloatArithmeticOperand(ExpressionNode node) {
        return isWideLaneFloatExpression(node) || isWideLaneNumericConstant(node);
    }

    private boolean isWideLaneInEligible(ExpressionNode node) {
        final ObjList<ExpressionNode> args = node.args;
        final ExpressionNode key = args.size() > 0 ? args.getLast() : node.lhs;
        if (isWideLaneIntegerExpression(key)) {
            if (args.size() > 0) {
                for (int i = 0, n = args.size() - 1; i < n; i++) {
                    if (!isWideLaneIntegerInElement(args.getQuick(i))) {
                        return false;
                    }
                }
                return true;
            }
            return isWideLaneIntegerInElement(node.rhs);
        }
        if (isWideLaneFloatExpression(key)) {
            if (args.size() > 0) {
                for (int i = 0, n = args.size() - 1; i < n; i++) {
                    if (!isWideLaneFloatInElement(args.getQuick(i))) {
                        return false;
                    }
                }
                return true;
            }
            return isWideLaneFloatInElement(node.rhs);
        }
        return false;
    }

    private boolean isWideLaneIntegerExpression(ExpressionNode node) {
        if (node == null) {
            return false;
        }
        if (node.type == ExpressionNode.LITERAL || node.type == ExpressionNode.BIND_VARIABLE) {
            final int type = arithExprType(node);
            return (type == I4_TYPE || type == I8_TYPE) && isGenuineIntegerLeaf(node);
        }
        if (node.type == ExpressionNode.CONSTANT) {
            return isIntegerConstant(node);
        }
        if (node.type == ExpressionNode.OPERATION && node.paramCount == 1 && Chars.equals(node.token, '-')) {
            return isWideLaneIntegerExpression(node.rhs != null ? node.rhs : node.lhs);
        }
        if (node.type == ExpressionNode.OPERATION && isArithmeticOperation(node)) {
            return isWideLaneIntegerExpression(node.lhs) && isWideLaneIntegerExpression(node.rhs);
        }
        return false;
    }

    // A genuine fixed-width integer column / bind-variable leaf (INT / LONG / DATE / TIMESTAMP).
    // SYMBOL, IPv4 and the GEO types share an I4 / I8 arithmetic code but do not compare as plain
    // integer lanes; SQL type checking already rejects such comparisons, so this only backstops
    // the wide-lane eligibility check defensively, mirroring isWidthSensitiveInKey.
    private boolean isGenuineIntegerLeaf(ExpressionNode node) {
        final int typeTag;
        if (node.type == ExpressionNode.LITERAL) {
            final int index = metadata.getColumnIndexQuiet(node.token);
            if (index == -1) {
                return false;
            }
            typeTag = ColumnType.tagOf(metadata.getColumnType(index));
        } else if (node.type == ExpressionNode.BIND_VARIABLE) {
            final Function fn = lookupBindVariable(node.token);
            if (fn == null) {
                return false;
            }
            typeTag = ColumnType.tagOf(fn.getType());
        } else {
            return false;
        }
        return typeTag == ColumnType.INT
                || typeTag == ColumnType.LONG
                || typeTag == ColumnType.DATE
                || typeTag == ColumnType.TIMESTAMP;
    }

    private boolean isWideLaneIntegerInElement(ExpressionNode node) {
        return isWideLaneIntegerExpression(node) || isNullConstant(node);
    }

    private boolean isWideLaneFloatInElement(ExpressionNode node) {
        return isWideLaneNumericConstant(node) || isNullConstant(node);
    }

    private boolean isWideLaneNumericConstant(ExpressionNode node) {
        if (node == null) {
            return false;
        }
        if (node.type == ExpressionNode.OPERATION && node.paramCount == 1 && Chars.equals(node.token, '-')) {
            return isWideLaneNumericConstant(node.rhs != null ? node.rhs : node.lhs);
        }
        final int type = arithExprType(node);
        return node.type == ExpressionNode.CONSTANT
                && (type == I4_TYPE || type == I8_TYPE || type == F4_TYPE || type == F8_TYPE);
    }

    private boolean isIntegerConstant(ExpressionNode node) {
        if (node != null && node.type == ExpressionNode.OPERATION
                && node.paramCount == 1 && Chars.equals(node.token, '-')) {
            return isIntegerConstant(node.rhs != null ? node.rhs : node.lhs);
        }
        return node != null
                && node.type == ExpressionNode.CONSTANT
                && (arithExprType(node) == I4_TYPE || arithExprType(node) == I8_TYPE);
    }

    private boolean containsFloatExpression(ExpressionNode node) {
        if (node == null) {
            return false;
        }
        final int cached = containsFloatCache.get(node);
        if (cached != NOT_CACHED) {
            return cached != 0;
        }
        final boolean result = containsFloatExpression0(node);
        containsFloatCache.put(node, result ? 1 : 0);
        return result;
    }

    private boolean containsFloatExpression0(ExpressionNode node) {
        if (node.type == ExpressionNode.LITERAL || node.type == ExpressionNode.BIND_VARIABLE) {
            final int type = arithExprType(node);
            return type == F4_TYPE || type == F8_TYPE;
        }
        return containsFloatExpression(node.lhs) || containsFloatExpression(node.rhs);
    }

    private boolean requiresWideLane(ExpressionNode node) {
        if (node == null) {
            return false;
        }
        if (SqlKeywords.isAndKeyword(node.token) || SqlKeywords.isOrKeyword(node.token)) {
            return requiresWideLane(node.lhs) || requiresWideLane(node.rhs);
        }
        if (SqlKeywords.isNotKeyword(node.token)) {
            return requiresWideLane(node.rhs != null ? node.rhs : node.lhs);
        }
        if (node.type == ExpressionNode.FUNCTION && SqlKeywords.isInKeyword(node.token)) {
            final ObjList<ExpressionNode> args = node.args;
            final ExpressionNode key = args.size() > 0 ? args.getLast() : node.lhs;
            if (args.size() > 0) {
                for (int i = 0, n = args.size() - 1; i < n; i++) {
                    if (requiresWideLanePair(key, args.getQuick(i))) {
                        return true;
                    }
                }
                return false;
            }
            return requiresWideLanePair(key, node.rhs);
        }
        return requiresWideLanePair(node.lhs, node.rhs)
                || requiresWideLaneArithmetic(node.lhs)
                || requiresWideLaneArithmetic(node.rhs);
    }

    private boolean requiresWideLaneArithmetic(ExpressionNode node) {
        if (node == null) {
            return false;
        }
        final int cached = requiresWideLaneArithCache.get(node);
        if (cached != NOT_CACHED) {
            return cached != 0;
        }
        final boolean result = requiresWideLaneArithmetic0(node);
        requiresWideLaneArithCache.put(node, result ? 1 : 0);
        return result;
    }

    private boolean requiresWideLaneArithmetic0(ExpressionNode node) {
        if (node.type == ExpressionNode.OPERATION && isArithmeticOperation(node)) {
            if (arithExprType(node) == I8_TYPE && containsNarrowIntegerValue(node)) {
                return true;
            }
            return requiresWideLaneArithmetic(node.lhs) || requiresWideLaneArithmetic(node.rhs);
        }
        if (node.type == ExpressionNode.OPERATION && node.paramCount == 1 && Chars.equals(node.token, '-')) {
            return requiresWideLaneArithmetic(node.rhs != null ? node.rhs : node.lhs);
        }
        return false;
    }

    private boolean requiresWideLanePair(ExpressionNode lhs, ExpressionNode rhs) {
        // NOTE: this accepts F8 as well as F4, while the widening is only ever EMITTED for F4
        // (isFloatLeaf). The asymmetry used to cost two separate things, and only the first of them
        // is settled here. It no longer costs AND_SC / OR_SC short-circuiting or predicate
        // reordering: serialize() runs the mixed-size detector whenever hasWideLaneConversionSource()
        // proves no conversion can be emitted, and that predicate reads the F4 rule this method
        // deliberately does not. It DOES still demote a filter such as
        // "adouble > 1.1 AND along > (2000000000 + 2000000000)" out of SINGLE_SIZE, which is the
        // cost the rest of this comment is about and which needs the visit() terms fixed first.
        //
        // Do NOT "fix" this by narrowing the clause to isFloatLeaf. Measured, that turns
        // "adouble > 1.1 AND along > (2000000000 + 2000000000)" from SINGLE_SIZE into SCALAR with
        // byte-identical IR, and the third case below from WIDE_LANE into SCALAR. Dropping the F8
        // trigger un-suppresses the two "!isWideLaneMode &&" terms in visit(), and those fire on
        // predicates where no widening is emitted at all. Tying those terms to an actually-emitted
        // widening is the real fix; until then the F8 trigger is load-bearing.
        if ((containsFloatExpression(lhs) && isFloatWideningConst(rhs))
                || (containsFloatExpression(rhs) && isFloatWideningConst(lhs))) {
            return true;
        }
        final int lhsType = arithExprType(lhs);
        final int rhsType = arithExprType(rhs);
        return (lhsType == I8_TYPE && rhsType == I4_TYPE && containsNarrowIntegerValue(rhs))
                || (rhsType == I8_TYPE && lhsType == I4_TYPE && containsNarrowIntegerValue(lhs))
                || requiresWideLaneArithmetic(lhs)
                || requiresWideLaneArithmetic(rhs);
    }

    private boolean containsNarrowIntegerValue(ExpressionNode node) {
        if (node == null) {
            return false;
        }
        final int cached = containsNarrowIntCache.get(node);
        if (cached != NOT_CACHED) {
            return cached != 0;
        }
        final boolean result = containsNarrowIntegerValue0(node);
        containsNarrowIntCache.put(node, result ? 1 : 0);
        return result;
    }

    private boolean containsNarrowIntegerValue0(ExpressionNode node) {
        if (node.type == ExpressionNode.LITERAL || node.type == ExpressionNode.BIND_VARIABLE) {
            return arithExprType(node) == I4_TYPE;
        }
        return containsNarrowIntegerValue(node.lhs) || containsNarrowIntegerValue(node.rhs);
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
     * <li>5-6 LSBs - execution hint: 0 - scalar, 1 - single size (SIMD-friendly),
     * 2 - mixed sizes, 3 - four-lane SIMD</li>
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
        // Reset the per-element IN-key width override: the serializer instance is reused across
        // filters, and a throw mid-IN (JIT fallback) could otherwise leave it stale for the next one.
        hasEmittedWideLaneConversion = false;
        isWideLaneMode = !forceScalar && isWideLaneEligible(node) && requiresWideLane(node);
        // Detect if scalar mode is guaranteed by checking for mixed column sizes.
        // Short-circuit optimizations (including IN() short-circuit) only work correctly
        // in scalar mode, so we only enable them when scalar mode is certain.
        boolean scalarModeDetected = forceScalar;
        // Wide-lane mode suppresses the short-circuit path because AND_SC / OR_SC cannot branch per
        // SIMD lane. Entering the mode is not the same as emitting a conversion, though, and
        // requiresWideLane() deliberately over-accepts: when the prediction misses, getExecHint()
        // falls back to a mixed / single size hint and the backend runs the very same scalar loop
        // (compiler.cpp takes avx2_loop only for the single-size and wide-lane hints). Suppressing
        // the short-circuit there buys nothing and costs an evaluation of every conjunct on every
        // row, so only suppress it once a conversion is actually possible.
        if (!scalarModeDetected && (!isWideLaneMode || !hasWideLaneConversionSource(node))) {
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
                    sortPredicates(collectedPredicates, false);
                    return serializePredicatesAndSc(collectedPredicates, forceScalar, debug, nullChecks);
                }
            } else if (isPureOrChain(node)) {
                collectedPredicates.clear();
                collectOrPredicates(node, collectedPredicates);
                if (collectedPredicates.size() > 1) {
                    sortPredicates(collectedPredicates, true);
                    return serializePredicatesOrSc(collectedPredicates, forceScalar, debug, nullChecks);
                }
            }
        }

        // Not a pure AND/OR chain or SIMD mode possible, use normal serialization
        traverseAlgo.traverse(node, this);
        putOperator(RET);

        ensureOnlyVarSizeHeaderChecks();
        final int options = getOptions(forceScalar, debug, nullChecks);
        assert areWideLaneWidthsHarmonised(options);
        return options;
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
            // Also force scalar when leaf promotion emits SX_I64 outside the conservatively
            // selected four-lane mode, which is the only one that implements it.
            forceScalarMode |= (predicateContext.hasArithmeticOperations
                    && (predicateContext.localTypesObserver.maxSize() <= 2
                    || predicateContext.localTypesObserver.hasNarrowInt()))
                    || (!isWideLaneMode && i64WidenLeaves.size() > 0);

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
     * constant.
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

    private static boolean isArithmeticOperation(ExpressionNode node) {
        final CharSequence token = node.token;
        if (node.paramCount < 2) {
            return false;
        }
        return Chars.equals(token, '+') || Chars.equals(token, '-')
                || Chars.equals(token, '*') || Chars.equals(token, '/');
    }

    // A binary numeric comparison operator: the shapes where a narrow-int leaf and
    // an out-of-INT-range constant read at long width in the Java filter.
    private static boolean isComparisonToken(CharSequence token) {
        return Chars.equals(token, "=")
                || Chars.equals(token, "<>") || Chars.equals(token, "!=")
                || Chars.equals(token, "<") || Chars.equals(token, "<=")
                || Chars.equals(token, ">") || Chars.equals(token, ">=");
    }

    private static boolean isGeoHash(int columnType) {
        return switch (ColumnType.tagOf(columnType)) {
            case ColumnType.GEOBYTE, ColumnType.GEOSHORT, ColumnType.GEOINT, ColumnType.GEOLONG -> true;
            default -> false;
        };
    }

    private static boolean isNullConstant(ExpressionNode node) {
        return node != null && node.type == ExpressionNode.CONSTANT && SqlKeywords.isNullKeyword(node.token);
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
     * {@code UNDEFINED_CODE}. {@link #arithExprType} reads it so a literal LONG operand
     * (e.g. {@code c4 * c8 >= -432577L}) types its enclosing arithmetic node I8.
     */
    private static int longConstantTypeCode(CharSequence token) {
        int len = token.length();
        if (len == 0) {
            return UNDEFINED_CODE;
        }
        // Reserved literal keywords (null / NULL, true, false) end in
        // 'l' / 'e' and would otherwise be folded into a bogus I8
        // observation by the suffix check below. They have their own
        // dedicated emission paths in serializeConstant.
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
     * Narrows a folded intermediate to the width the Java filter folds it at and maps a
     * non-finite result onto NaN, mirroring {@code FloatConstant#newInstance} /
     * {@code DoubleConstant#newInstance}, which both hand back the NULL constant for
     * anything {@link Numbers#isFinite(double)} rejects.
     */
    private static double normalizeConstantFold(double value, boolean isFloat) {
        final double v = isFloat ? (float) value : value;
        return Numbers.isFinite(v) ? v : Double.NaN;
    }

    /**
     * Reads a constant leaf of a folded arithmetic subtree as a double, mirroring the ladder
     * {@code FunctionParser#createConstant} walks: {@code null}/{@code nan} give the NULL
     * constant, then {@code parseInt}, {@code parseLong}, {@code parseDouble}, {@code parseFloat}
     * in that order. Going through the same ladder rather than {@code parseDouble} alone is what
     * lets the fold read the shapes the type classifiers admit but a single parser does not -
     * {@code parseInt} takes the underscore thousands separator that {@code parseDouble} rejects,
     * and {@code parseDouble} takes the {@code 'd'} suffix. Throws {@link NumericException} for
     * every remaining shape (quoted literals, {@code true}/{@code false}, geo hashes, type
     * constants), which the caller turns into a declined filter.
     */
    private static double parseFoldLeaf(CharSequence token) throws NumericException {
        if (SqlKeywords.isNullKeyword(token) || SqlKeywords.isNanKeyword(token)) {
            return Double.NaN;
        }
        // Skip the integer rungs for a token that is lexically floating point. They could only
        // throw, and NumericException#instance() allocates and fills in a stack trace under -ea,
        // which the surefire argLine enables - so two doomed parses per FLOAT/DOUBLE leaf are not
        // free on the compile path.
        if (floatConstantTypeCode(token) == UNDEFINED_CODE) {
            try {
                return Numbers.parseInt(token);
            } catch (NumericException notInt) {
                // fall through to the next width, as createConstant does
            }
            try {
                return Numbers.parseLong(token);
            } catch (NumericException notLong) {
                // fall through
            }
        }
        try {
            return Numbers.parseDouble(token);
        } catch (NumericException notDouble) {
            // fall through
        }
        return Numbers.parseFloat(token);
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

    // Adds a leaf / constant node to the i64-widen set. The set dedups by node identity: the same
    // node can be reached by more than one marker pass.
    private void addI64WidenLeaf(ExpressionNode node) {
        i64WidenLeaves.add(node);
    }

    /**
     * Classifies the arithmetic result type code of a numeric expression
     * subtree (column / bind variable / numeric constant / + - * / over them),
     * or {@link #UNDEFINED_CODE} for anything that is not pure numeric
     * arithmetic. Mirrors the implicit-promotion rules the Java filter applies,
     * so {@link #markWidthSemantics} can find the LONG-width subtrees whose
     * narrow operands the Java filter reads at 64 bits.
     */
    private int arithExprType(ExpressionNode node) {
        if (node == null) {
            return UNDEFINED_CODE;
        }
        final int cached = arithExprTypeCache.get(node);
        if (cached != NOT_CACHED) {
            return cached;
        }
        final int typeCode = arithExprType0(node);
        arithExprTypeCache.put(node, typeCode);
        return typeCode;
    }

    private int arithExprType0(ExpressionNode node) {
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
                // Promotion only: an INT arithmetic subtree stays I4 however large its
                // mathematical result, because it wraps mod 2^32 at runtime and its constant fold
                // is an IntConstant. Only a genuine 64-bit operand promotes it to I8.
                return promoteArithType(arithExprType(node.lhs), arithExprType(node.rhs));
            }
            default:
                return UNDEFINED_CODE;
        }
    }

    /**
     * Asserts that no binary operator in a finished wide-lane IR stream mixes a 4-byte and an
     * 8-byte integer operand.
     * <p>
     * The four-lane backend loads a 4-byte column as four packed i32 in the low half of the
     * register while an 8-byte column spans all four 64-bit lanes, so an unharmonised pairing
     * compares against adjacent rows. {@code avx2::convert()} widens the i32 side of an i32-with-i64
     * and an i32-with-f64 pairing, so a miss no longer reaches a user as wrong rows - but the
     * frontend still owns the answer, because only it knows which width the JAVA filter reads at,
     * and the two must agree. This assert stays the tripwire that turns a miss into a test failure
     * rather than a silent reliance on the backstop. Integer-to-float pairings are excluded:
     * {@code convert()} handles them outright.
     * <p>
     * Runs under {@code -ea} only, so it costs nothing in production.
     */
    private boolean areWideLaneWidthsHarmonised(int options) {
        if (((options >> 4) & 3) != EXEC_HINT_WIDE_LANE) {
            return true;
        }
        // Operator instructions pad their options field with zero, and I1_TYPE is zero, so the
        // stack has to carry widths derived from the opcode rather than the encoded field.
        // UNDEFINED_CODE marks a value whose width is not a lane width (a comparison mask, or a
        // type this check does not reason about); pairings involving one are skipped.
        typeStack.clear();
        for (long offset = 0; offset < memory.size(); offset += INSTRUCTION_SIZE) {
            final int opCode = memory.getInt(offset);
            switch (opCode) {
                case RET:
                    return true;
                case VAR:
                case MEM:
                case IMM:
                    typeStack.push(memory.getInt(offset + Integer.BYTES));
                    break;
                case SX_I64:
                    typeStack.pop();
                    typeStack.push(I8_TYPE);
                    break;
                case NEG:
                    // Value-preserving: keeps its operand's width.
                    break;
                case NOT:
                    typeStack.pop();
                    typeStack.push(UNDEFINED_CODE);
                    break;
                case AND:
                case OR:
                case AND_SC:
                case OR_SC:
                    typeStack.pop();
                    typeStack.pop();
                    typeStack.push(UNDEFINED_CODE);
                    break;
                case BEGIN_SC:
                case END_SC:
                    break;
                case EQ:
                case NE:
                case LT:
                case LE:
                case GT:
                case GE:
                case ADD:
                case SUB:
                case MUL:
                case DIV: {
                    final int lhsType = typeStack.pop();
                    final int rhsType = typeStack.pop();
                    if ((isNarrowIntTypeCode(lhsType) && rhsType == I8_TYPE)
                            || (isNarrowIntTypeCode(rhsType) && lhsType == I8_TYPE)) {
                        return false;
                    }
                    final boolean isComparison = opCode == EQ || opCode == NE || opCode == LT
                            || opCode == LE || opCode == GT || opCode == GE;
                    // A comparison yields a lane mask, not a value of either operand's width.
                    typeStack.push(isComparison ? UNDEFINED_CODE : Math.max(lhsType, rhsType));
                    break;
                }
                default:
                    // An opcode this check does not model: drop the width information rather
                    // than guess at its arity.
                    typeStack.clear();
            }
        }
        return true;
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

        serializeConstant(offset, position, token, negate,
                isI64WidenLeaf(node) || i64WidenConstants.contains(node),
                narrowKeptConstants.contains(node), intWidthNullElements.contains(node));
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

    /**
     * Computes the priority of each predicate once, into {@link #predicatePriorities}, so that
     * {@link #sortPredicates(ObjList, boolean)} reads a pre-computed int per predicate instead of
     * re-walking its subtree.
     */
    private void computePredicatePriorities(ObjList<ExpressionNode> predicates) {
        predicatePriorities.clear();
        for (int i = 0, n = predicates.size(); i < n; i++) {
            final int priority = getPredicatePriority(predicates.getQuick(i));
            // The counting sort indexes its buckets by priority, so a new PRIORITY_* constant outside
            // the range has to raise PRIORITY_COUNT with it.
            assert priority >= 0 && priority < PRIORITY_COUNT;
            predicatePriorities.add(priority);
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

    /**
     * The exact value of a numeric constant - bare, or under a unary minus - as the double the Java
     * filter compares against a FLOAT leaf, or {@link Double#NaN} when {@code node} is not one.
     * parseLong runs first: it accepts the underscore thousands separator (1_000_000) and
     * parseDouble does not, so a separated integer literal would otherwise read as non-numeric.
     */
    private double floatCmpConstValue(ExpressionNode node) {
        final ExpressionNode constNode;
        final boolean isNegated;
        if (node.type == ExpressionNode.CONSTANT) {
            constNode = node;
            isNegated = false;
        } else if (node.type == ExpressionNode.OPERATION && node.paramCount == 1 && Chars.equals(node.token, '-')) {
            constNode = node.rhs != null ? node.rhs : node.lhs;
            isNegated = true;
        } else {
            return Double.NaN;
        }
        if (constNode == null || constNode.type != ExpressionNode.CONSTANT
                || constNode.token == null || isReservedConstantKeyword(constNode.token)) {
            return Double.NaN;
        }
        double d;
        try {
            d = Numbers.parseLong(constNode.token);
        } catch (NumericException notLong) {
            try {
                d = Numbers.parseDouble(constNode.token);
            } catch (NumericException notNumeric) {
                return Double.NaN;
            }
        }
        return isNegated ? -d : d;
    }

    /**
     * Folds one operand's {@link #arithExprType} into a running
     * comparison-width accumulator. Unlike {@link #promoteArithType}, a
     * non-numeric ({@link #UNDEFINED_CODE}) operand is treated as identity
     * rather than absorbing the result: an IN list keeps its column operand last
     * in {@code args} (with {@code lhs} / {@code rhs} null in the multi-value
     * form), so a plain promote seeded from the null operands would stay
     * UNDEFINED and read a LONG-width fold as a wrapped I4.
     */
    private int foldCmpType(int cmpType, ExpressionNode operand) {
        int operandType = arithExprType(operand);
        if (operandType == UNDEFINED_CODE) {
            return cmpType;
        }
        if (cmpType == UNDEFINED_CODE) {
            return operandType;
        }
        return promoteArithType(cmpType, operandType);
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
            if (isWideLaneMode && hasEmittedWideLaneConversion) {
                return EXEC_HINT_WIDE_LANE;
            }
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

    /**
     * Reports whether the tree carries the ingredients a wide-lane conversion is built from, so
     * {@code false} proves that serializing it cannot set {@link #hasEmittedWideLaneConversion}.
     * <p>
     * {@link #requiresWideLane} answers a deliberately broader question - whether to ENTER wide-lane
     * mode - and over-accepts in two places the emission rules do not follow: it takes any operand
     * {@link #containsFloatExpression} admits, F8 included, where only an F4 leaf is ever widened
     * ({@link #isFloatLeaf} excludes F8 because DOUBLE already compares exactly), and it reads an
     * overflowing constant fold through {@link #arithExprType} where {@link #markWidthSemantics}
     * reads it through {@link #arithExprType}. A filter that enters the mode and then emits
     * nothing still gets a correct hint - {@link #getExecHint} needs both the mode and the emission
     * to return {@link #EXEC_HINT_WIDE_LANE} - but it used to lose the whole short-circuit path,
     * because {@link #serialize} suppressed the scalar-mode detector on the mode alone.
     * <p>
     * Answering per predicate matches the granularity the width machinery works at: AND / OR delimit
     * predicates ({@link #isTopLevelOperation} accepts NOT, IN and the comparison operators, never
     * AND / OR), and every mark set is recomputed per predicate.
     * A NOT subtree is one predicate, so this does not recurse into it.
     * <p>
     * Erring towards {@code true} only preserves the previous behaviour, so anything uncertain
     * belongs on the {@code true} side. Should this ever answer {@code false} for a filter that does
     * emit a conversion, the wide-lane guard in {@link #serializePredicatesAndSc} /
     * {@link #serializePredicatesOrSc} declines JIT compilation rather than letting a short-circuit
     * opcode reach the four-lane backend, which cannot branch per lane.
     */
    private boolean hasWideLaneConversionSource(ExpressionNode node) {
        if (node == null) {
            return false;
        }
        if (node.type == ExpressionNode.OPERATION
                && (SqlKeywords.isAndKeyword(node.token) || SqlKeywords.isOrKeyword(node.token))) {
            return hasWideLaneConversionSource(node.lhs) || hasWideLaneConversionSource(node.rhs);
        }
        // Within one predicate a conversion has exactly two sources. markFloatCmpConst fires for an
        // F4 leaf against a constant that no 32-bit float reproduces, and maybeEmitI64Widening
        // sign-extends a leaf but returns early unless that leaf is emitted at I1 / I2 / I4 width -
        // so it needs both a narrow leaf to widen and a 64-bit operand to widen it towards.
        return (hasWideLaneSourceNode(node, WIDE_LANE_SOURCE_FLOAT_LEAF)
                && hasWideLaneSourceNode(node, WIDE_LANE_SOURCE_FLOAT_WIDENING_CONST))
                || (hasWideLaneSourceNode(node, WIDE_LANE_SOURCE_NARROW_INT_LEAF)
                && hasWideLaneSourceNode(node, WIDE_LANE_SOURCE_I8_OPERAND));
    }

    /**
     * Reports whether a subtree holds a node of the given {@code WIDE_LANE_SOURCE_*} kind, walking
     * both operands and the {@code args} of an n-ary node such as IN.
     */
    private boolean hasWideLaneSourceNode(ExpressionNode node, int kind) {
        if (node == null) {
            return false;
        }
        final boolean isMatch = switch (kind) {
            case WIDE_LANE_SOURCE_FLOAT_LEAF -> isFloatLeaf(node);
            case WIDE_LANE_SOURCE_FLOAT_WIDENING_CONST -> isFloatWideningConst(node);
            case WIDE_LANE_SOURCE_NARROW_INT_LEAF -> isNarrowIntLeaf(node);
            case WIDE_LANE_SOURCE_I8_OPERAND -> arithExprType(node) == I8_TYPE;
            default -> {
                // Unreachable: kind is always one of the four constants above. Answer true rather
                // than throwing, because true is the conservative side - it only keeps the
                // short-circuit suppression this method exists to lift - whereas an error raised
                // here would reach SqlCodeGenerator's catch (Throwable) and fail the query outright,
                // instead of declining JIT the way every other failure in this serializer does.
                assert false : "unexpected wide-lane source kind: " + kind;
                yield true;
            }
        };
        if (isMatch) {
            return true;
        }
        if (hasWideLaneSourceNode(node.lhs, kind) || hasWideLaneSourceNode(node.rhs, kind)) {
            return true;
        }
        for (int i = 0, n = node.args.size(); i < n; i++) {
            if (hasWideLaneSourceNode(node.args.getQuick(i), kind)) {
                return true;
            }
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

    /**
     * Reports whether {@code node} is a numeric constant - bare, or under a unary minus - whose
     * value no 32-bit float carries exactly, i.e. one that {@link #serializeNumber}'s F4 arm
     * would round. A FLOAT column is always compared at DOUBLE width by the Java filter (there
     * is no (FLOAT, FLOAT) comparison factory - only the double ones, so both operands promote),
     * so such a constant has to reach the compiled filter as a double or the two paths diverge.
     * <p>
     * Round-to-nearest is wrong in both directions, which is why every op is affected rather
     * than just some: {@code f < 1.00000003} rounds the bound DOWN to 1.0f and drops a row
     * holding 1.0f that the Java filter keeps, {@code f > 0.99999998} rounds it UP to 1.0f and
     * drops the same row, and {@code f = 1.00000003} rounds it to 1.0f and MATCHES that row -
     * a row whose value is not the one asked for. An integer literal is no safer above 2^24
     * ({@code (float) 16777217} is 16777216).
     */
    private boolean isFloatInexactConst(ExpressionNode node) {
        if (node == null) {
            return false;
        }
        final double d = floatCmpConstValue(node);
        if (Numbers.isNull(d)) {
            return false; // not a numeric constant
        }
        return (double) (float) d != d;
    }

    // A FLOAT-typed operand: a column, a bind variable, or an arithmetic subtree over them. A
    // constant compared directly against one types down to F4 (INT and FLOAT are both 4 bytes, so
    // the observer sees no mixed size), and serializeNumber would then emit it as a lossy 32-bit
    // float, while the Java filter compares both operands at double width - there is no
    // (FLOAT, FLOAT) comparison factory. markFloatCmpConst puts that right. An arithmetic subtree
    // counts: "f + 0 < 1.00000003" reads the bound the same way a bare column does. A CONSTANT is
    // excluded - a constant-vs-constant comparison has no column side to bound. (A negated constant
    // is an OPERATION and still slips through, but such a predicate has no column at all, so
    // serializeConstant rejects it and the JIT declines the filter.) DOUBLE (F8) already compares
    // exactly, so it is intentionally excluded too.
    private boolean isFloatLeaf(ExpressionNode node) {
        if (node == null || node.type == ExpressionNode.CONSTANT) {
            return false;
        }
        return arithExprType(node) == F4_TYPE;
    }

    /**
     * Reports whether a constant compared against a FLOAT leaf needs the double-width treatment,
     * i.e. whether the 32-bit float {@link #serializeNumber} would emit for it differs from the
     * value the Java filter compares at double width. Two spellings reach this: an out-of-INT-range
     * integer literal (the original rule - {@code (float) 5000000001} is 5000000000), and any other
     * literal with no exact float, fractional or not (see {@link #isFloatInexactConst}).
     */
    private boolean isFloatWideningConst(ExpressionNode node) {
        return (isIntegerConst(node) && arithExprType(node) == I8_TYPE) || isFloatInexactConst(node);
    }

    // Reference (not value) membership: the same node objects are marked and serialized.
    private boolean isI64WidenLeaf(ExpressionNode node) {
        return i64WidenLeaves.contains(node);
    }

    // Reference (not value) membership: the same node objects are marked and serialized.
    private boolean isInTimestampPredicate() throws SqlException {
        // visit inOperationNode to get an expression type
        predicateContext.onNodeVisited(predicateContext.inOperationNode.rhs);
        predicateContext.onNodeVisited(predicateContext.inOperationNode.lhs);

        // check predicate type is timestamp
        return ColumnType.isTimestamp(predicateContext.columnType);
    }

    // A bare or unary-minus-wrapped integer CONSTANT node (I4- or I8-typed). Returns
    // false for float, keyword, and non-numeric constants, and for columns.
    private boolean isIntegerConst(ExpressionNode node) {
        if (node == null) {
            return false;
        }
        final ExpressionNode constNode;
        if (node.type == ExpressionNode.CONSTANT) {
            constNode = node;
        } else if (node.type == ExpressionNode.OPERATION && node.paramCount == 1 && Chars.equals(node.token, '-')) {
            constNode = node.rhs != null ? node.rhs : node.lhs;
        } else {
            return false;
        }
        if (constNode == null || constNode.type != ExpressionNode.CONSTANT) {
            return false;
        }
        final int t = arithExprType(node);
        return t == I4_TYPE || t == I8_TYPE;
    }

    // A narrow-int (BYTE / SHORT / INT) column or bind-variable leaf. Sign-extending
    // one to i64 is value-preserving (no arithmetic to wrap).
    private boolean isNarrowIntLeaf(ExpressionNode node) {
        if (node == null || (node.type != ExpressionNode.LITERAL && node.type != ExpressionNode.BIND_VARIABLE)) {
            return false;
        }
        final int t = arithExprType(node);
        return t == I1_TYPE || t == I2_TYPE || t == I4_TYPE;
    }

    private static boolean isNarrowIntTypeCode(int typeCode) {
        return typeCode == I1_TYPE || typeCode == I2_TYPE || typeCode == I4_TYPE;
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

    /**
     * Reports whether an IN key is a genuine narrow INTEGER one (BYTE / SHORT / INT) - a column, a
     * numeric bind variable, or an arithmetic subtree over them - which is what
     * {@link #neverMatchingInPairingWidth} needs to know before folding a NULL pairing.
     * <p>
     * A SYMBOL / CHAR / GEOHASH / IPv4 / BOOLEAN key maps to the same narrow type
     * code as a genuine integer (see {@link #columnTypeCode}) but routes through a
     * different Java IN function (InSymbol / InChar / InIPv4 / ...), not the InLong path, so it
     * keeps its own IN function's NULL semantics. A genuinely-LONG (I8) key has a real sentinel at
     * its own width, so its NULL pairing is meaningful.
     * <p>
     * An arithmetic OPERATION key is checked by {@link #arithExprType}: the
     * arithmetic operators (+ - * /) only ever yield a numeric result, so a narrow
     * genuine type there is a real narrow-int subtree, never a symbol / geo leaf. A
     * plain LITERAL / BIND_VARIABLE key is checked against its real column type tag,
     * since {@code columnTypeCode} alone cannot tell an INT column from a SYMBOL one.
     */
    private boolean isWidthSensitiveInKey(ExpressionNode inKey) {
        if (inKey == null) {
            return false;
        }
        if (inKey.type == ExpressionNode.OPERATION || inKey.type == ExpressionNode.CONSTANT) {
            // A numeric CONSTANT key needs the override for the same reason an arithmetic one does,
            // and for one more: serializeUntypedNumber emits it at I8 as soon as anything else in
            // the predicate is I8, so "0 IN (i32, i64)" put an i64 key against the i32 element while
            // the i64 element paired correctly. arithExprType is UNDEFINED for a non-numeric
            // constant - a quoted symbol, a char, NULL - so those keep their own IN function's
            // semantics and take no numeric override, exactly as for a LITERAL key below.
            final int t = arithExprType(inKey);
            return t == I1_TYPE || t == I2_TYPE || t == I4_TYPE;
        }
        final int typeTag;
        if (inKey.type == ExpressionNode.LITERAL) {
            final int index = metadata.getColumnIndexQuiet(inKey.token);
            if (index == -1) {
                return false;
            }
            typeTag = ColumnType.tagOf(metadata.getColumnType(index));
        } else if (inKey.type == ExpressionNode.BIND_VARIABLE) {
            final Function fn = lookupBindVariable(inKey.token);
            if (fn == null) {
                return false;
            }
            typeTag = ColumnType.tagOf(fn.getType());
        } else {
            return false;
        }
        return typeTag == ColumnType.BYTE || typeTag == ColumnType.SHORT || typeTag == ColumnType.INT;
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
     * Routes a constant with no exact 32-bit float that a comparison puts against a FLOAT leaf (see
     * {@link #isFloatInexactConst}) onto the double-width path: {@link #serializeConstant} emits it
     * through the 64-bit arm. The four-lane AVX2 mode promotes the float leaf to double. Both
     * filters then run the SAME comparison - QuestDB compares floating point
     * with a tolerance ({@code Numbers.DOUBLE_TOLERANCE}, 1e-10: {@code LtDoubleVVFunctionFactory}
     * is {@code !Numbers.equals(l, r) && l < r}, and the backend's cmp_lt / cmp_eq apply
     * DOUBLE_EPSILON the same way) - so this is exact agreement rather than an approximation of it.
     * <p>
     * Emitting a float bound instead, rounded in the direction the operator preserves, looks like it
     * should work and does not: it reproduces the comparison only under EXACT IEEE ordering. The
     * tolerance is 1e-10 while one float ulp near 1.0 is 1.2e-7, so shifting the bound by an ulp
     * steps clean over the band. A constant within the tolerance of a float then flips rows in both
     * directions - {@code f < 1.00000000005} against a row holding 1.0f is a match for the rounded
     * bound but a tolerance-EQUAL (so excluded) for the Java filter. The bound has to carry the
     * tolerance to be rounded safely, and at that point the double comparison is what is wanted
     * anyway. (The parquet pruner cannot widen its stats slot, so it folds the tolerance into the
     * bound instead - see ParquetRowGroupFilter#tryPutFloatFromDouble.)
     */
    private void markFloatCmpConst(ExpressionNode constNode) {
        addI64WidenLeaf(constNode);
        if (isWideLaneMode) {
            hasEmittedWideLaneConversion = true;
        }
    }

    private void markWidthSemantics(ExpressionNode node, WidthCtx w) {
        if (node == null) {
            return;
        }
        final boolean isFloatActive = w.isFloatActive;
        final boolean isFloatUnderLong = w.isFloatUnderLong;
        markNarrowConstCmpWidenNode(node);
        final boolean isIn = node.type == ExpressionNode.FUNCTION && SqlKeywords.isInKeyword(node.token);
        if (node.type != ExpressionNode.OPERATION && !isIn) {
            return;
        }

        final boolean isUnaryMinus = node.paramCount == 1 && Chars.equals(node.token, '-');
        if (isArithmeticOperation(node) || isUnaryMinus) {
            final boolean isFloatLong = isFloatActive
                    && (isFloatUnderLong || arithExprType(node) == I8_TYPE);
            final WidthCtx childCtx = new WidthCtx(isFloatActive, isFloatLong);
            final int exprType = arithExprType(node);
            if (isUnaryMinus) {
                markWidthSemanticsOperand(node.rhs != null ? node.rhs : node.lhs, exprType, childCtx);
            } else {
                markWidthSemanticsOperand(node.lhs, exprType, childCtx);
                markWidthSemanticsOperand(node.rhs, exprType, childCtx);
            }
            return;
        }

        if (isIn && node.args.size() > 0) {
            final ExpressionNode key = node.args.getLast();
            final int keyType = arithExprType(key);
            // `k IN (e0, e1, ...)` re-serializes the key once per element, but the key is ONE node
            // and carries one emitted width, so a single 64-bit pairing pulls the whole list to
            // 64 bits. Decide that first, then harmonise every operand to it: the Java InLong path
            // reads the key through getLong() and a narrow-int element through
            // Numbers.intToLong(getInt()), so sign-extending either is value preserving.
            boolean hasLongPairing = false;
            for (int i = 0, n = node.args.size() - 1; i < n; i++) {
                if (foldCmpType(keyType, node.args.getQuick(i)) == I8_TYPE) {
                    hasLongPairing = true;
                    break;
                }
            }
            if (hasLongPairing) {
                markCmpOperandWidenedToI64(key);
            }
            for (int i = 0, n = node.args.size() - 1; i < n; i++) {
                final ExpressionNode element = node.args.getQuick(i);
                // A NULL element follows the width the LIST settled on. While the list stays at
                // INT width that is the key's I4 (INT_NULL); once a 64-bit pairing lifts the key to
                // i64 it is LONG_NULL at I8 - which is what the Java filter compares against, since
                // Numbers.intToLong(INT_NULL) is LONG_NULL. The predicate-wide observer knows
                // neither, because it sees the narrowest column anywhere in the predicate.
                if (isNullConstant(element)) {
                    if (hasLongPairing) {
                        i64WidenConstants.add(element);
                    } else if (keyType == I4_TYPE) {
                        intWidthNullElements.add(element);
                    }
                }
                final boolean isPairLong = foldCmpType(keyType, element) == I8_TYPE;
                if (hasLongPairing) {
                    markCmpOperandWidenedToI64(element);
                }
                markWidthSemantics(element, new WidthCtx(isFloatActive, isPairLong));
            }
            markWidthSemantics(key, new WidthCtx(isFloatActive, false));
            return;
        }

        int cmpType = foldCmpType(UNDEFINED_CODE, node.lhs);
        cmpType = foldCmpType(cmpType, node.rhs);
        for (int i = 0, n = node.args.size(); i < n; i++) {
            cmpType = foldCmpType(cmpType, node.args.getQuick(i));
        }
        final boolean isCmpLong = cmpType == I8_TYPE;
        // The two-operand IN form (single element, key and element in lhs / rhs) reaches here
        // instead of the args loop above and needs the same harmonisation.
        if (isCmpLong && node.paramCount == 2 && (isComparisonToken(node.token) || isIn)) {
            // A bare narrow-int leaf (column / bind variable) compared at long width against
            // a LONG operand must sign-extend to i64, so the four-lane AVX2 path compares it
            // at 64-bit width: the LONG operand loads at full width and cmp_* dispatches on a
            // single dtype, so an un-widened narrow leaf produces a width-mismatched vector
            // compare (garbage lanes). markNarrowConstCmpWidenPair covers the narrow-vs-out-of-
            // range-constant case and the IN path covers IN keys, but neither covers a plain
            // column-vs-column / column-vs-bind comparison. Outside wide-lane mode the emitted
            // SX_I64 forces scalar via serialize()'s i64WidenLeaves gate, matching master's
            // mixed-size scalar path.
            // Both operands of a 64-bit comparison are read at 64 bits by the Java filter, so
            // widening either is exact. A narrow LEAF sign-extends (IntColumn#getLong), and an
            // integer CONSTANT has to follow it: the type observer sees columns only, so an
            // all-INT-column predicate would emit it at I4 against the peer's i64 lanes.
            markCmpOperandWidenedToI64(node.lhs);
            markCmpOperandWidenedToI64(node.rhs);
        }
        // Not gated on isFloatActive: an out-of-INT-range constant compared against a narrow
        // arithmetic subtree has to widen whatever else the predicate contains. markNarrowConstCmp-
        // WidenPair covers only a bare LITERAL / BIND_VARIABLE leaf, so a unary-minus-wrapped column
        // (-i < 2147483649) fell through both and the constant was emitted as a lossy F4 immediate.
        // The scalar and vectorized backends then disagreed with each other, which made the answer
        // depend on whether the host has AVX2. maybeWidenCmpConstOperand carries its own predicate -
        // an integer CONSTANT of I8 type against an OPERATION with a narrow arithExprType - so it
        // is safe to ask unconditionally.
        if (node.paramCount == 2 && isComparisonToken(node.token)) {
            maybeWidenCmpConstOperand(node.lhs, node.rhs);
            maybeWidenCmpConstOperand(node.rhs, node.lhs);
        }
        // The single-value IN form keeps its key / element in lhs / rhs, so it needs the same
        // key-width NULL rule the args loop above applies. See intWidthNullElements.
        if (isIn && node.paramCount == 2) {
            final ExpressionNode nullElement = isNullConstant(node.rhs) ? node.rhs
                    : isNullConstant(node.lhs) ? node.lhs : null;
            if (nullElement != null) {
                final int keyWidth = arithExprType(nullElement == node.rhs ? node.lhs : node.rhs);
                if (keyWidth == I8_TYPE) {
                    i64WidenConstants.add(nullElement);
                } else if (keyWidth == I4_TYPE) {
                    intWidthNullElements.add(nullElement);
                }
            }
        }
        final WidthCtx cmpCtx = new WidthCtx(isFloatActive, isCmpLong);
        markWidthSemantics(node.lhs, cmpCtx);
        markWidthSemantics(node.rhs, cmpCtx);
        for (int i = 0, n = node.args.size(); i < n; i++) {
            markWidthSemantics(node.args.getQuick(i), cmpCtx);
        }
    }

    /**
     * Harmonises one operand of a comparison or {@code IN} pairing that runs at 64 bits: a narrow
     * leaf and an integer constant both sign-extend, and a narrow arithmetic subtree - which cannot
     * be sign-extended from the frontend - drops the predicate to the scalar backend.
     */
    private void markCmpOperandWidenedToI64(ExpressionNode node) {
        if (isNarrowIntLeaf(node)) {
            addI64WidenLeaf(node);
            return;
        }
        if (isIntegerConst(node)) {
            // Widening an immediate needs no SX_I64 and costs no vectorization, so it goes in the
            // constant-only set: emitting `0` at I8 against an i64 peer is what the observer would
            // have done anyway whenever it saw an 8-byte column.
            i64WidenConstants.add(node);
            return;
        }
        forceScalarOnUnharmonisedNarrowArith(node);
    }

    /**
     * Forces the scalar backend when {@code node} is a NARROW-width arithmetic subtree that a
     * 64-bit comparison peer will read, and the four-lane mode is not selected.
     * <p>
     * An INT arithmetic subtree computes at i32 and wraps, exactly as {@code MulInt#getInt} does,
     * and the comparison sign-extends its RESULT - which is what the Java filter does too, since
     * {@code IntFunction.getLong()} is {@code Numbers.intToLong(getInt())}. But SX_I64 is emitted
     * per LEAF, so there is no way to sign-extend a subtree's result from the frontend, and the
     * pairing reaches the backend as i32-against-i64. Neither vectorized mode reproduces the Java
     * answer for it: the single-size loop compares a packed i32 half against i64 lanes, and the
     * four-lane one was measured returning zero rows for {@code -i32 < 2147483649} where Java
     * returns every row. Only the scalar backend's {@code convert()} carries the complete table,
     * INT_NULL to LONG_NULL included.
     * <p>
     * This costs vectorization on a common shape - {@code WHERE i * j > long_col} now runs scalar -
     * which is the tradeoff for computing the product at the width the Java filter computes it at.
     * Master widened the operands instead, so the product ran at 64 bits and vectorized; that is
     * exactly the behaviour this change removes. Teaching the IR to sign-extend a subtree RESULT
     * (an SX_I64 after the operator rather than after a leaf) would recover it, and is the obvious
     * follow-up.
     */
    private void forceScalarOnUnharmonisedNarrowArith(ExpressionNode node) {
        if (node == null || node.type != ExpressionNode.OPERATION) {
            return;
        }
        final boolean isUnaryMinus = node.paramCount == 1 && Chars.equals(node.token, '-');
        if (!isArithmeticOperation(node) && !isUnaryMinus) {
            return;
        }
        final int type = arithExprType(node);
        if (type == I1_TYPE || type == I2_TYPE || type == I4_TYPE) {
            forceScalarMode = true;
        }
    }

    private void markWidthSemanticsOperand(ExpressionNode child, int parentType, WidthCtx w) {
        final boolean isFloatActive = w.isFloatActive;
        final boolean isFloatUnderLong = w.isFloatUnderLong;
        if (parentType == I1_TYPE || parentType == I2_TYPE || parentType == I4_TYPE) {
            // A narrow arithmetic node computes at its own width and wraps, so its integer constant
            // operands must be emitted at that width whatever else the predicate contains.
            if (isIntegerConst(child)) {
                narrowKeptConstants.add(child);
            }
        } else if (parentType == I8_TYPE) {
            // A GENUINELY 64-bit arithmetic node - one with a real LONG operand - resolves the (LL)
            // factory in Java, which reads a narrow operand through IntColumn#getLong(), an exact
            // sign extension. Widening the LEAF here matches that and leaves the node's operands at
            // one width, so the four-lane backend can take it.
            //
            // This is leaf promotion, not the arithmetic widening this change removes: the node is
            // 64 bits because an operand IS 64 bits, never because a narrower one overflowed.
            //
            // A narrow arithmetic SUBTREE operand cannot be promoted the same way - it computes at
            // i32 and wraps, and SX_I64 is emitted per leaf - so the pairing stays mixed-width and
            // has to run scalar, exactly as at a comparison boundary.
            if (isNarrowIntLeaf(child)) {
                addI64WidenLeaf(child);
            } else {
                forceScalarOnUnharmonisedNarrowArith(child);
            }
        }
        boolean isFloatChildActive = isFloatActive;
        // An out-of-INT-range integer constant operand. The predicate-wide type observer sees
        // columns only, so an all-INT-column predicate types it at I4 and serializeNumber's
        // parseInt rejects it outright; a co-present FLOAT column types it at F4 and
        // serializeNumber rounds it. Neither can represent it - emit a full I8 IMM.
        if (isIntegerConst(child) && arithExprType(child) == I8_TYPE) {
            addI64WidenLeaf(child);
            isFloatChildActive = false;
        }
        markWidthSemantics(child, new WidthCtx(isFloatChildActive, isFloatUnderLong));
    }

    /**
     * Tags a narrow-int column / bind-variable leaf and the out-of-INT-range integer
     * constant it is compared against for i64 widening.
     * <p>
     * The type observer sees only columns, so a predicate whose widest observed type
     * is an INT column types an out-of-INT-range constant down to I4;
     * {@link #serializeNumber} then emits it as a 32-bit float on the int-parse
     * overflow, and floats near 2^31 are spaced 256 apart, so distinct INT rows
     * collapse onto one float and match spuriously (e.g. {@code i32 = 2147483648}
     * admits 2147483647 / 2147483646). The Java filter reads the comparison at long
     * width - the constant is a LONG literal, so overload resolution picks the (LL) comparison and
     * reads the column through {@code IntColumn#getLong} - so no INT value equals it. Mirror that:
     * sign-extend the leaf (value-preserving) and emit the constant as a full I8 IMM. The
     * mark is per node, so a sibling in-range comparison is unaffected; eligible integer
     * comparisons, arithmetic, and IN shapes use the four-lane AVX2 path.
     * <p>
     * For an IN whose key is a narrow-int leaf and any element is out-of-INT-range, the Java
     * InLong path reads the key at long width, so widen the key and every integer-constant element
     * (all value-preserving) to keep each pairing at one width. The single-value form keeps
     * key / element in lhs / rhs.
     * <p>
     * A FLOAT leaf compared directly against an out-of-INT-range integer constant
     * diverges the same way (INT and FLOAT are both 4 bytes, so the constant types
     * down to F4 and rounds to the nearest float, e.g. 3000000200 -> 3000000256f).
     * Here only the constant widens to I8; the FLOAT column is not sign-extended but
     * promoted to double by the scalar convert(), matching the Java filter which
     * compares both operands at double width. See {@link #isFloatLeaf}. DOUBLE columns
     * already compare exactly and are left vectorized.
     */
    private void markNarrowConstCmpWidenNode(ExpressionNode node) {
        if (node == null) {
            return;
        }
        if (node.type == ExpressionNode.FUNCTION && SqlKeywords.isInKeyword(node.token)) {
            if (node.args.size() > 0) {
                final ExpressionNode key = node.args.getLast();
                if (isNarrowIntLeaf(key)) {
                    boolean hasOutOfRange = false;
                    for (int i = 0, n = node.args.size() - 1; i < n; i++) {
                        if (isIntegerConst(node.args.getQuick(i)) && arithExprType(node.args.getQuick(i)) == I8_TYPE) {
                            hasOutOfRange = true;
                            break;
                        }
                    }
                    if (hasOutOfRange) {
                        addI64WidenLeaf(key);
                        for (int i = 0, n = node.args.size() - 1; i < n; i++) {
                            final ExpressionNode element = node.args.getQuick(i);
                            if (isIntegerConst(element)) {
                                addI64WidenLeaf(element);
                            }
                        }
                    }
                } else if (isFloatLeaf(key)) {
                    // IN over a FLOAT key is an OR of equalities, and equality against a constant
                    // with no exact float has no float bound that reproduces it: any float emitted
                    // would match the rows that round to it, so the JIT returned rows whose value is
                    // not the one asked for (and NOT IN dropped them). Widen every such element to
                    // double - four-lane AVX2 promotes the float key alongside it. The
                    // single-value form keeps key / element in lhs / rhs and takes the pair path
                    // below, which routes to the same rule via markFloatCmpConst.
                    for (int i = 0, n = node.args.size() - 1; i < n; i++) {
                        final ExpressionNode element = node.args.getQuick(i);
                        if (isFloatWideningConst(element)) {
                            markFloatCmpConst(element);
                        }
                    }
                }
                // An out-of-INT-range constant element against a narrow ARITHMETIC key needs the
                // same compensation the comparison arm gets, or it types down to a lossy F4.
                for (int i = 0, n = node.args.size() - 1; i < n; i++) {
                    maybeWidenCmpConstOperand(node.args.getQuick(i), key);
                }
            } else {
                markNarrowConstCmpWidenPair(node);
                // The single-value form keeps key / element in lhs / rhs, so it needs the
                // arithmetic-operand compensation the comparison arm gets: an out-of-INT-range
                // constant against a narrow arithmetic key types down to a lossy F4 otherwise.
                maybeWidenCmpConstOperand(node.lhs, node.rhs);
                maybeWidenCmpConstOperand(node.rhs, node.lhs);
            }
            return;
        }
        if (node.type == ExpressionNode.OPERATION
                && node.paramCount == 2
                && isComparisonToken(node.token)) {
            markNarrowConstCmpWidenPair(node);
        }
    }

    private void markNarrowConstCmpWidenPair(ExpressionNode cmp) {
        final ExpressionNode a = cmp.lhs;
        final ExpressionNode b = cmp.rhs;
        final ExpressionNode narrowLeaf;
        final ExpressionNode constNode;
        if (isNarrowIntLeaf(a) && isIntegerConst(b)) {
            narrowLeaf = a;
            constNode = b;
        } else if (isNarrowIntLeaf(b) && isIntegerConst(a)) {
            narrowLeaf = b;
            constNode = a;
        } else if (isFloatLeaf(a) && isFloatWideningConst(b)) {
            markFloatCmpConst(b);
            return;
        } else if (isFloatLeaf(b) && isFloatWideningConst(a)) {
            markFloatCmpConst(a);
            return;
        } else {
            return;
        }
        // Only an out-of-INT-range constant diverges against a narrow-int leaf; an in-range one
        // already compares at int width on both paths, and widening it would needlessly force
        // scalar mode.
        if (arithExprType(constNode) != I8_TYPE) {
            return;
        }
        // The narrow-int leaf sign-extends alongside the constant (value-preserving).
        addI64WidenLeaf(narrowLeaf);
        addI64WidenLeaf(constNode);
    }

    /**
     * Sign-extends a narrow integer leaf to i64, for the leaves the marker passes tagged: a narrow
     * column or bind variable that a comparison puts directly against a 64-bit peer, and the
     * out-of-INT-range constant on the other side of it. The Java filter reads such a leaf through
     * {@code IntColumn#getLong}, which is {@code Numbers.intToLong(getInt())}, so the widening is
     * exact. Arithmetic is NOT widened: an INT subtree computes at 32 bits and wraps, and the
     * backend's {@code convert()} promotes its i32 result at the comparison.
     */
    private void maybeEmitI64Widening(ExpressionNode node, int typeCode) {
        if (typeCode != I1_TYPE && typeCode != I2_TYPE && typeCode != I4_TYPE) {
            return;
        }
        if (isI64WidenLeaf(node)) {
            putOperator(SX_I64);
            // SX_I64 is supported only by the conservatively selected four-lane AVX2 mode, so every
            // other shape that emits it must retain the scalar correctness fallback. Tying
            // forceScalarMode to the emission itself keeps that a hard invariant rather than a
            // coincidence of the current width rules.
            hasEmittedWideLaneConversion = true;
            if (!isWideLaneMode) {
                forceScalarMode = true;
            }
        }
    }

    /**
     * Widens a bare out-of-INT-range integer constant that a comparison puts against a narrow-int
     * arithmetic operand (e.g. {@code (a*b) = 4999999999}). The type observer sees only INT and
     * FLOAT columns (both 4 bytes, so {@link TypesObserver#hasMixedSizes()} is false) and types the
     * constant down to a lossy F4. Left as F4, {@link #serializeNumber} rounds it to the nearest
     * float (4999999999 -> 5.0e9f, floats near 2^32 are 512 apart) and the JIT float-compares,
     * admitting rows the Java filter rejects. Adding it to {@code i64WidenLeaves} makes
     * {@link #serializeConstant} emit a full I8 IMM. Only the CONSTANT widens - the narrow product
     * itself keeps computing at i32, exactly as {@code MulInt#getInt} does, and the backend's
     * {@code convert()} promotes its result at the comparison.
     * <p>
     * A narrow-int LEAF compared against such a constant is already covered by
     * {@link #markNarrowConstCmpWidenPair}; this covers the arithmetic-operand
     * (product / sum) gap it misses, so {@code other} is restricted to an OPERATION.
     * An in-range (I4) constant keeps its narrow width.
     */
    private void maybeWidenCmpConstOperand(ExpressionNode constNode, ExpressionNode other) {
        if (constNode == null || constNode.type != ExpressionNode.CONSTANT
                || !isIntegerConst(constNode) || arithExprType(constNode) != I8_TYPE) {
            return;
        }
        if (other == null || other.type != ExpressionNode.OPERATION) {
            return;
        }
        final int otherType = arithExprType(other);
        if (otherType == I1_TYPE || otherType == I2_TYPE || otherType == I4_TYPE) {
            addI64WidenLeaf(constNode);
        }
    }

    /**
     * Returns the width to emit a never-matching IN pairing at when this pairing can never match, so
     * it must not reach the backend at all; {@link #UNDEFINED_CODE} when it is an ordinary pairing.
     * <p>
     * A NULL element only becomes a hazard once the key is width-sensitive, because that is what
     * drives the element to I4: {@link #serializeConstant} then
     * keeps the NULL at I4 and {@link #serializeNull}'s "byte/short type is not nullable" decline
     * never fires. The element reaches the backend as an I4 immediate while the key loads as a
     * size-1 or size-2 lane, and the comparison runs at the key's width against a register holding
     * the other one - three of every four BYTE lanes then compare the key against 0. So the fold is
     * gated on exactly {@link #isWidthSensitiveInKey}, and from there two independent reasons make
     * the pairing impossible:
     * <ul>
     * <li>a numeric CONSTANT key is never NULL, whatever width it ends up emitted at;</li>
     * <li>any other key read at BYTE or SHORT width has no NULL sentinel - every bit pattern is a
     * legal value - so no row can make it NULL. That covers a column, a bind variable and any
     * arithmetic over them, including a unary minus, which {@code arithExprType} deliberately
     * sees through.</li>
     * </ul>
     * A key that widens to INT keeps the ordinary pairing: INT has a real sentinel, so the pairing is
     * meaningful, and both sides are already I4 - except a narrow BINARY arithmetic key such as
     * {@code abyte + 1}, which reads at INT width but emits at the observer's narrow one. That shape
     * is safe for a different reason: {@code isArithmeticOperation} accepts it ({@code paramCount >= 2}),
     * so it sets {@code hasArithmeticOperations} and {@code forceScalarMode} routes it to the scalar
     * backend, whose {@code convert()} has the complete integer table. A UNARY minus does not set
     * that flag, which is why it needs the fold rather than the forcer.
     * <p>
     * GEOHASH, CHAR and BOOLEAN keys share the narrow type codes but {@code isWidthSensitiveInKey}
     * excludes them, which is what this needs: a GEOHASH has a NULL at every width, CHAR reads
     * {@code (char) 0} as NULL, and BOOLEAN keeps the decline {@code serializeNull} always gave it.
     * <p>
     * The width returned is I4 rather than the key's own. The key's emitted width is not knowable
     * here - for {@code 0 IN (null, abyte)} this pairing is serialized before {@code abyte} is
     * observed - and it does not need to be: the fold is an all-zero FULL register, which reads the
     * same at every lane width.
     */
    private int neverMatchingInPairingWidth(ExpressionNode element, ExpressionNode key, boolean isWidthSensitiveKey) {
        // isWidthSensitiveKey is serializeIn's cached isWidthSensitiveInKey(key), so a non-null key
        // is implied whenever it is true.
        if (!isNullConstant(element) || !isWidthSensitiveKey) {
            return UNDEFINED_CODE;
        }
        if (key.type == ExpressionNode.CONSTANT) {
            return I4_TYPE;
        }
        final int keyArithType = arithExprType(key);
        return keyArithType == I1_TYPE || keyArithType == I2_TYPE ? I4_TYPE : UNDEFINED_CODE;
    }

    private void putDoubleOperand(long offset, int type, double payload) {
        memory.putInt(offset, CompiledFilterIRSerializer.IMM);
        memory.putInt(offset + Integer.BYTES, type);
        memory.putDouble(offset + 2 * Integer.BYTES, payload);
        memory.putLong(offset + 2 * Integer.BYTES + Double.BYTES, 0L);
    }

    /**
     * Emits a pairing that is false on every row. The mask is an all-zero full register, so the
     * width only has to be one the backend can compare at, not the key's own. See
     * {@link #neverMatchingInPairingWidth}.
     */
    private void putNeverMatchingInPairing(int typeCode) {
        putOperand(IMM, typeCode, 0);
        putOperand(IMM, typeCode, 1);
        putOperator(EQ);
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

    private void serializeConstant(
            long offset,
            int position,
            final CharSequence token,
            boolean negated,
            boolean isWidenedToI64,
            boolean isNarrowKept,
            boolean isIntWidthNull
    ) throws SqlException {
        final int len = token.length();
        final int typeCode = predicateContext.localTypesObserver.constantTypeCode();
        if (typeCode == UNDEFINED_CODE) {
            throw SqlException.position(position).put("all constants expression: ").put(token);
        }

        if (SqlKeywords.isNullKeyword(token)) {
            // An IN pairing compares at its KEY's width, which the predicate-wide observer does not
            // report: it sees the narrowest column anywhere in the predicate. See
            // intWidthNullElements and i64WidenConstants.
            final int nullTypeCode;
            if (isIntWidthNull) {
                nullTypeCode = I4_TYPE;
            } else if (isWidenedToI64 && (typeCode == I1_TYPE || typeCode == I2_TYPE || typeCode == I4_TYPE)) {
                nullTypeCode = I8_TYPE;
            } else {
                nullTypeCode = typeCode;
            }
            serializeNull(offset, position, nullTypeCode, predicateContext.columnType);
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
            serializeUntypedNumber(offset, position, token, negated, isWidenedToI64, isNarrowKept);
        } else {
            // isWidenedToI64 covers a plain out-of-INT-range constant compared against a narrow-int
            // leaf: the observer typed it down to I4, so serializeNumber would emit a lossy float on
            // overflow. markNarrowConstCmpWidenNode tags both sides to widen to i64.
            int numberTypeCode = typeCode;
            if (isNarrowKept && typeCode == I8_TYPE) {
                // A narrow arithmetic operand: keep the constant's own INT width so int32_* wraps
                // with the narrow column, however wide the rest of the predicate is. Only the I8
                // observation is overridden - a FLOAT / DOUBLE one is the promotion the Java filter
                // performs too (IntFunction#getDouble), and forcing I4 there would turn a float
                // division into an integer one.
                numberTypeCode = I4_TYPE;
            } else if (isWidenedToI64
                    && (numberTypeCode == I1_TYPE || numberTypeCode == I2_TYPE || numberTypeCode == I4_TYPE)) {
                numberTypeCode = I8_TYPE;
            } else if (isWidenedToI64
                    && (numberTypeCode == F4_TYPE || numberTypeCode == F8_TYPE)
                    && longConstantTypeCode(token) == I8_TYPE) {
                // A FLOAT/DOUBLE column co-present with a narrow-int leaf types this
                // out-of-INT-range integer constant down to F4/F8 (both 4 bytes as INT, so
                // hasMixedSizes() is false), but a marker flagged it to widen: the Java filter
                // reads it at long width - as an operand of a genuinely 64-bit arithmetic node, or
                // as a direct comparison operand (markNarrowConstCmpWidenPair, getLong). Emitting a
                // lossy 32-bit float here would make the JIT do a float multiply/compare and drop
                // rows the Java filter keeps. Emit a full I8 IMM instead. Flagging forces scalar
                // mode, where the scalar convert() widens the narrow operand to i64 and
                // int32*int64 stays exact.
                numberTypeCode = I8_TYPE;
            } else if (isWidenedToI64 && numberTypeCode == F4_TYPE) {
                // A constant with no exact 32-bit float that a comparison puts against a FLOAT leaf
                // (markFloatCmpConst marks every operator, ordering and equality alike). The F4 arm
                // below would round it to the nearest float - which the Java filter never does,
                // since a FLOAT column always compares at double width - so the two paths would
                // select different rows, and '=' would even match a row holding a different value.
                // Route it through the 64-bit arm, which emits an exact I8 IMM for an integer
                // literal (above 2^24 a float cannot hold one) and a full F8 double otherwise.
                // The four-lane AVX2 convert() promotes the float leaf to double for both
                // (f32, i64) and (f32, f64), so both filters run the same tolerance-aware
                // double comparison.
                numberTypeCode = F8_TYPE;
            }
            serializeNumber(offset, position, token, numberTypeCode, negated);
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

        // A multi-value IN list keeps its operands as [elements..., key]; the single-value form
        // keeps its key / element in lhs / rhs (args empty). The key is read once per row at one
        // width - the Java InLong path reads it through getLong(), which for a narrow key is an
        // exact sign extension - so the only classification left is for a NULL element against a
        // BYTE / SHORT key, whose pairing can never match. See neverMatchingInPairingWidth.
        final ExpressionNode inKey = args.size() > 0 ? args.getLast() : predicateContext.inOperationNode.lhs;
        final boolean isWidthSensitiveKey = isWidthSensitiveInKey(inKey);

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
                final int scNeverMatchingWidth = neverMatchingInPairingWidth(predicateContext.inOperationNode.rhs, inKey, isWidthSensitiveKey);
                if (scNeverMatchingWidth != UNDEFINED_CODE) {
                    putNeverMatchingInPairing(scNeverMatchingWidth);
                } else {
                    traverseAlgo.traverse(predicateContext.inOperationNode.rhs, this);
                    traverseAlgo.traverse(predicateContext.inOperationNode.lhs, this);
                    putOperator(EQ);
                }
                putOperatorWithLabel(AND_SC, 0); // if false, jump to next_row
            } else {
                // Multiple values: BEGIN_SC(2), [EQ, OR_SC(2)]*, EQ, AND_SC(0), END_SC(2)
                // Label 0 = next_row (skip this row) - reserved by backend
                // Label 1 = store_row (accept row) - reserved by backend
                // Label 2 = success (at least one IN match)
                putOperatorWithLabel(BEGIN_SC, 2); // create success label
                for (int i = 0, n = predicateContext.inOperationNode.args.size() - 1; i < n; i++) {
                    final int neverMatchingWidth = neverMatchingInPairingWidth(args.get(i), inKey, isWidthSensitiveKey);
                    if (neverMatchingWidth != UNDEFINED_CODE) {
                        putNeverMatchingInPairing(neverMatchingWidth);
                    } else {
                        traverseAlgo.traverse(args.get(i), this);
                        traverseAlgo.traverse(args.getLast(), this);
                        putOperator(EQ);
                    }
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
            final int singleNeverMatchingWidth = neverMatchingInPairingWidth(predicateContext.inOperationNode.rhs, inKey, isWidthSensitiveKey);
            if (singleNeverMatchingWidth != UNDEFINED_CODE) {
                putNeverMatchingInPairing(singleNeverMatchingWidth);
            } else {
                traverseAlgo.traverse(predicateContext.inOperationNode.rhs, this);
                traverseAlgo.traverse(predicateContext.inOperationNode.lhs, this);
                putOperator(EQ);
            }
        }

        int orCount = -1;
        for (int i = 0, n = predicateContext.inOperationNode.args.size() - 1; i < n; i++) {
            final int neverMatchingWidth = neverMatchingInPairingWidth(args.get(i), inKey, isWidthSensitiveKey);
            if (neverMatchingWidth != UNDEFINED_CODE) {
                putNeverMatchingInPairing(neverMatchingWidth);
            } else {
                traverseAlgo.traverse(args.get(i), this);
                traverseAlgo.traverse(args.getLast(), this);
                putOperator(EQ);
            }
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
            if (execHint == EXEC_HINT_SINGLE_SIZE_TYPE || execHint == EXEC_HINT_WIDE_LANE) {
                // We could handle this via the non-short-circuit code path, but if we get here,
                // it means that scalarModeDetector did a false-positive scalar mode detection, or
                // that hasWideLaneConversionSource() cleared a filter that went on to emit a
                // conversion after all. In such case, it's a bug we should fix, so let's fail JIT
                // compilation to flag that - a short-circuit opcode cannot branch per SIMD lane and
                // must never reach the four-lane backend.
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
            if (execHint == EXEC_HINT_SINGLE_SIZE_TYPE || execHint == EXEC_HINT_WIDE_LANE) {
                // We could handle this via the non-short-circuit code path, but if we get here,
                // it means that scalarModeDetector did a false-positive scalar mode detection, or
                // that hasWideLaneConversionSource() cleared a filter that went on to emit a
                // conversion after all. In such case, it's a bug we should fix, so let's fail JIT
                // compilation to flag that - a short-circuit opcode cannot branch per SIMD lane and
                // must never reach the four-lane backend.
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

    private void serializeUntypedNumber(long offset, int position, final CharSequence token, boolean negated, boolean isWidenedToI64, boolean isNarrowKept) throws SqlException {
        long sign = negated ? -1 : 1;

        // Emit the constant as I8 when the predicate has a 64-bit operand and no float, or when
        // markWidthSemantics tagged this constant as living under a LONG-width subtree despite a
        // float elsewhere. Otherwise keep it I4 so int32_* wraps mod 2^32 on both the JIT and the
        // Java sides.
        boolean isI4Kept = isNarrowKept
                || (!predicateContext.localTypesObserver.hasI8() || predicateContext.hasFloatInPredicate)
                && !isWidenedToI64;
        if (isI4Kept) {
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
     * Orders the predicates of an AND ({@code isInverted == false}) or OR ({@code isInverted == true})
     * chain for short-circuit evaluation: the AND chain runs its cheapest, most selective predicates
     * first (ascending priority), the OR chain the other way round (descending), so the chain exits
     * as early as it can.
     * <p>
     * Every priority falls in the fixed {@code [0, PRIORITY_COUNT)} range, so the order comes out of a
     * counting sort in theta(k) rather than the theta(k^2) of a comparison sort. Walking the input
     * front to back and appending each predicate to its bucket keeps the sort stable, i.e. predicates
     * of equal priority keep the order the user wrote them in - a chain reordered by anything else
     * would change which predicate the backend short-circuits on. The scratch lists are fields, so
     * ordering a chain allocates nothing.
     */
    private void sortPredicates(ObjList<ExpressionNode> predicates, boolean isInverted) {
        final int n = predicates.size();
        computePredicatePriorities(predicates);

        // Count the predicates per priority, then turn the counts into the offset at which each
        // bucket starts. Buckets run in ascending priority, or descending when inverted.
        predicatePriorityOffsets.setAll(PRIORITY_COUNT, 0);
        for (int i = 0; i < n; i++) {
            final int priority = predicatePriorities.getQuick(i);
            predicatePriorityOffsets.increment(priority);
        }
        int offset = 0;
        if (isInverted) {
            for (int priority = PRIORITY_COUNT - 1; priority >= 0; priority--) {
                final int count = predicatePriorityOffsets.getQuick(priority);
                predicatePriorityOffsets.setQuick(priority, offset);
                offset += count;
            }
        } else {
            for (int priority = 0; priority < PRIORITY_COUNT; priority++) {
                final int count = predicatePriorityOffsets.getQuick(priority);
                predicatePriorityOffsets.setQuick(priority, offset);
                offset += count;
            }
        }

        sortedPredicates.setPos(n);
        for (int i = 0; i < n; i++) {
            final int priority = predicatePriorities.getQuick(i);
            final int slot = predicatePriorityOffsets.getQuick(priority);
            predicatePriorityOffsets.setQuick(priority, slot + 1);
            sortedPredicates.setQuick(slot, predicates.getQuick(i));
        }
        for (int i = 0; i < n; i++) {
            predicates.setQuick(i, sortedPredicates.getQuick(i));
        }
        sortedPredicates.clear();
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
        final int cached = constantArithFoldCache.get(node);
        if (cached == 0) {
            throw NumericException.INSTANCE;
        }
        if (cached != NOT_CACHED) {
            return constantArithFoldValues.getQuick(cached - 1);
        }
        try {
            final long value = tryFoldConstantArith0(node);
            constantArithFoldValues.add(value);
            constantArithFoldCache.put(node, constantArithFoldValues.size());
            return value;
        } catch (NumericException e) {
            constantArithFoldCache.put(node, 0);
            throw e;
        }
    }

    private long tryFoldConstantArith0(ExpressionNode node) throws NumericException {
        if (node == null) {
            throw NumericException.INSTANCE;
        }
        if (node.type == ExpressionNode.CONSTANT) {
            return Numbers.parseLong(node.token);
        }
        if (node.type != ExpressionNode.OPERATION) {
            throw NumericException.INSTANCE;
        }
        // Unary minus: parser builds OPERATION "-" with rhs only. NegLong#getLong
        // propagates LONG_NULL instead of negating the sentinel.
        if (Chars.equals(node.token, '-') && node.lhs == null) {
            long operand = tryFoldConstantArith(node.rhs);
            return operand == Numbers.LONG_NULL ? Numbers.LONG_NULL : -operand;
        }
        long left = tryFoldConstantArith(node.lhs);
        long right = tryFoldConstantArith(node.rhs);
        // MulLong / AddLong / SubLong / DivLong#getLong return LONG_NULL when
        // either operand is Long.MIN_VALUE (the LONG null sentinel), so an inner
        // product that lands exactly on -2^63 poisons the rest of the fold to
        // NULL instead of feeding a wrapped value. Without this the JIT kept
        // computing full-width arithmetic (e.g. (2^62 * -2) + 5 = Long.MIN + 5)
        // while the Java filter collapsed to NULL. Mirrors the INT_NULL guard in
        // tryFoldConstantArithI4.
        if (left == Numbers.LONG_NULL || right == Numbers.LONG_NULL) {
            return Numbers.LONG_NULL;
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
            if (right == 0L) {
                // Decline the fold and let descend() emit the division as IR: the native
                // int64_div (impl/x86.h) returns LONG_NULL for a zero divisor, which is what
                // the Java filter's DivLong#getLong produces. ExpressionNode#applyLongFold
                // models the same operator table and folds this case straight to LONG_NULL -
                // a different spelling of the same result, not a disagreement.
                throw NumericException.INSTANCE;
            }
            return left / right;
        }
        throw NumericException.INSTANCE;
    }

    /**
     * FLOAT/DOUBLE counterpart of {@link #tryFoldConstantArith}: evaluates a pure-constant
     * floating point arithmetic subtree the way {@code FunctionParser} folds it, and throws
     * {@link NumericException} if any descendant is non-constant, is not a numeric literal,
     * or the subtree uses an operator other than {@code + - * /}. Callers use it only to
     * find out whether the fold is finite, so a declined fold is always safe: it costs a
     * pass over a subtree the caller then serializes as IR.
     * <p>
     * The NaN normalisation runs after EVERY operation rather than once at the end because
     * that is what the function parser does - it folds bottom-up and runs each intermediate
     * through {@code DoubleConstant#newInstance}. {@code 1e308 * 10.0} is therefore already
     * NULL by the time an enclosing operator sees it, which makes
     * {@code 1.0 / (1e308 * 10.0)} NULL as well, where raw IEEE hands back a perfectly
     * finite {@code 0.0}. NaN is absorbing under all four operators, so a finite result
     * proves every intermediate was finite too - exactly the case where the backend
     * computing the subtree agrees with the Java filter folding it.
     *
     * @param isFloat fold at FLOAT width, mirroring the {@code FloatConstant} arm of
     *                {@code functionToConstant0}. Rounding each double result back to float
     *                is exact for {@code + - * /}: double carries more than twice the
     *                significand bits a float needs, so no double rounding error survives.
     */
    private double tryFoldConstantArithFloat(ExpressionNode node) throws NumericException {
        if (node == null) {
            throw NumericException.INSTANCE;
        }
        final int cached = constantFloatFoldCache.get(node);
        if (cached == 0) {
            throw NumericException.INSTANCE;
        }
        if (cached != NOT_CACHED) {
            return constantFloatFoldValues.getQuick(cached - 1);
        }
        try {
            final double value = tryFoldConstantArithFloat0(node);
            constantFloatFoldValues.add(value);
            constantFloatFoldCache.put(node, constantFloatFoldValues.size());
            return value;
        } catch (NumericException e) {
            constantFloatFoldCache.put(node, 0);
            throw e;
        }
    }

    private double tryFoldConstantArithFloat0(ExpressionNode node) throws NumericException {
        // Each node narrows at its OWN width, which is what the parser does: it builds a
        // FloatConstant for an all-FLOAT operation and a DoubleConstant as soon as one operand is
        // DOUBLE, so (3.4e38f + 3.4e38f) * 1.0 overflows to NULL inside the float add even though
        // the enclosing multiply is evaluated at double width.
        final boolean isFloat = arithExprType(node) == F4_TYPE;
        if (node.type == ExpressionNode.CONSTANT) {
            // A leaf no parser accepts (a quoted literal, true/false, a geo hash, a type
            // constant) folds to NULL rather than throwing: the subtree IS a constant one, so
            // declining the filter is the honest answer - see descend().
            double leaf;
            try {
                leaf = parseFoldLeaf(node.token);
            } catch (NumericException notNumeric) {
                leaf = Double.NaN;
            }
            return normalizeConstantFold(leaf, isFloat);
        }
        if (node.type != ExpressionNode.OPERATION) {
            throw NumericException.INSTANCE;
        }
        // Unary minus: parser builds OPERATION "-" with rhs only.
        if (Chars.equals(node.token, '-') && node.lhs == null) {
            return normalizeConstantFold(-tryFoldConstantArithFloat(node.rhs), isFloat);
        }
        if (!isArithmeticOperation(node)) {
            throw NumericException.INSTANCE;
        }
        final double left = tryFoldConstantArithFloat(node.lhs);
        final double right = tryFoldConstantArithFloat(node.rhs);
        if (Chars.equals(node.token, '+')) {
            return normalizeConstantFold(left + right, isFloat);
        }
        if (Chars.equals(node.token, '-')) {
            return normalizeConstantFold(left - right, isFloat);
        }
        if (Chars.equals(node.token, '*')) {
            return normalizeConstantFold(left * right, isFloat);
        }
        return normalizeConstantFold(left / right, isFloat);
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
            // A leaf constant here is always in INT range: an out-of-INT leaf routes
            // to the I8/widen branch instead of the I4 fold. parseInt throws on an
            // out-of-range token rather than silently truncating a LONG-range literal,
            // so if that invariant is ever violated the caller cleanly falls back to
            // descending the subtree as per-op IR.
            return Numbers.parseInt(node.token);
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
     * Per-predicate pre-pass that reports whether the predicate has a FLOAT or DOUBLE source
     * anywhere - column, bind variable or numeric literal.
     * <p>
     * {@link #serializeUntypedNumber} reads it to keep an integer constant at I4 in that case: the
     * int-arithmetic subtree is consumed by {@code IntFunction#getDouble} / {@code #getFloat},
     * which call {@link io.questdb.cairo.sql.Function#getInt}, so the Java filter computes it at
     * int32 width and wraps modulo 2^32 on overflow. Widening the constant alone would let the JIT
     * preserve the full long product and diverge.
     */
    private class NarrowI64WidenDetector implements PostOrderTreeTraversalAlgo.Visitor, Mutable {
        private final TypesObserver typesObserver = new TypesObserver();

        @Override
        public void clear() {
            typesObserver.clear();
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
                        typesObserver.observe(columnTypeCode(ColumnType.tagOf(metadata.getColumnType(columnIndex))));
                    }
                    break;
                }
                case ExpressionNode.BIND_VARIABLE: {
                    // An unbound or UNDEFINED-typed bind variable is safe to skip here because
                    // serializeBindVariable consults the same BindVariableService in the same
                    // serialize() call and throws on either condition, aborting JIT compile and
                    // falling back to the Java filter.
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
                    // Observe FLOAT / DOUBLE numeric constants so a predicate whose only float
                    // source is a literal (e.g. c7 + 0.5) reports one.
                    int typeCode = floatConstantTypeCode(node.token);
                    if (typeCode != UNDEFINED_CODE) {
                        typesObserver.observe(typeCode);
                    }
                    break;
                }
            }
        }

        boolean hasFloat() {
            return typesObserver.hasFloat();
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
                    // Pre-pass: remember whether any FLOAT / DOUBLE source is present anywhere in
                    // the predicate. See NarrowI64WidenDetector.
                    i64WidenConstants.clear();
                    intWidthNullElements.clear();
                    i64WidenLeaves.clear();
                    narrowKeptConstants.clear();
                    // The type and wide-lane memo caches are NOT cleared here. They are pure
                    // functions of a node's subtree keyed by node identity, so an entry stays valid
                    // for every predicate of the same filter, and serialize()'s whole-tree wide-lane
                    // pre-pass has already filled them. Dropping them per predicate re-ran the type
                    // and fold analysis from scratch for each one. The node pool can hand the same
                    // objects to a LATER filter, and clear() already covers that boundary.
                    try {
                        narrowI64WidenDetector.clear();
                        traverseAlgo.traverse(node, narrowI64WidenDetector);
                        hasFloatInPredicate = narrowI64WidenDetector.hasFloat();
                    } catch (SqlException ignore) {
                        // Detector does not throw; defensive only.
                        hasFloatInPredicate = false;
                    }
                    // One top-down pass installs every per-comparison leaf-promotion mark.
                    markWidthSemantics(node, new WidthCtx(hasFloatInPredicate, false));
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

    /**
     * Immutable bundle of the width-semantics flags {@link #markWidthSemantics} threads down the
     * predicate tree. Each of the three rewrite passes carries an {@code active} flag plus an
     * {@code underLong} flag (whether the subtree already sits under a LONG-width parent), and the
     * wrap pass additionally tracks whether a narrower width has already been resolved. Grouping them
     * in one value keeps the recursive signatures to two parameters instead of eight adjacent booleans.
     */
    private static final class WidthCtx {
        final boolean isFloatActive;
        final boolean isFloatUnderLong;

        WidthCtx(boolean isFloatActive, boolean isFloatUnderLong) {
            this.isFloatActive = isFloatActive;
            this.isFloatUnderLong = isFloatUnderLong;
        }
    }
}
