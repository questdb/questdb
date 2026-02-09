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

package io.questdb.griffin;

import io.questdb.cairo.ColumnType;
import io.questdb.griffin.model.ExpressionNode;
import io.questdb.griffin.model.QueryModel;
import io.questdb.griffin.model.WindowExpression;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.Chars;
import io.questdb.std.GenericLexer;
import io.questdb.std.IntHashSet;
import io.questdb.std.IntStack;
import io.questdb.std.LowerCaseAsciiCharSequenceIntHashMap;
import io.questdb.std.LowerCaseAsciiCharSequenceObjHashMap;
import io.questdb.std.LowerCaseCharSequenceObjHashMap;
import io.questdb.std.Numbers;
import io.questdb.std.ObjStack;
import io.questdb.std.ObjectPool;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static io.questdb.griffin.OperatorExpression.Operator.In;
import static io.questdb.griffin.OperatorExpression.UNARY;

public class ExpressionParser {
    private static final int BRANCH_ARRAY_TYPE_QUALIFIER_END = 21;
    private static final int BRANCH_ARRAY_TYPE_QUALIFIER_START = 20;
    private static final int BRANCH_BETWEEN_END = 14;
    private static final int BRANCH_BETWEEN_START = 13;
    private static final int BRANCH_CASE_CONTROL = 10;
    private static final int BRANCH_CASE_START = 9;
    private static final int BRANCH_CAST_AS = 11;
    private static final int BRANCH_COMMA = 1;
    private static final int BRANCH_CONSTANT = 4;
    private static final int BRANCH_DECIMAL = 22;
    private static final int BRANCH_DOT = 12;
    private static final int BRANCH_DOT_DEREFERENCE = 17;
    private static final int BRANCH_GEOHASH = 18;
    private static final int BRANCH_LAMBDA = 7;
    private static final int BRANCH_LEFT_BRACKET = 15;
    private static final int BRANCH_LEFT_PARENTHESIS = 2;
    private static final int BRANCH_LITERAL = 6;
    private static final int BRANCH_NONE = 0;
    private static final int BRANCH_OPERATOR = 5;
    private static final int BRANCH_RIGHT_BRACKET = 16;
    private static final int BRANCH_RIGHT_PARENTHESIS = 3;
    private static final int BRANCH_TIMESTAMP_ZONE = 19;
    private static final int IDX_ELSE = 2;
    private static final int IDX_THEN = 1;
    private static final int IDX_WHEN = 0;
    private static final Log LOG = LogFactory.getLog(ExpressionParser.class);
    private static final LowerCaseAsciiCharSequenceObjHashMap<CharSequence> allFunctions = new LowerCaseAsciiCharSequenceObjHashMap<>();
    private static final LowerCaseAsciiCharSequenceIntHashMap caseKeywords = new LowerCaseAsciiCharSequenceIntHashMap();
    // columnTypes that an expression can be cast into, in addition to the range BOOLEAN…LONG256
    private static final IntHashSet moreCastTargetTypes = new IntHashSet();
    private static final IntHashSet nonLiteralBranches = new IntHashSet(); // branches that can't be followed by constants
    private final OperatorRegistry activeRegistry;
    private final IntStack argStackDepthStack = new IntStack();
    private final CharacterStore characterStore;
    private final ObjectPool<ExpressionNode> expressionNodePool;
    private final ObjStack<ExpressionNode> opStack = new ObjStack<>();
    private final IntStack paramCountStack = new IntStack();
    private final ObjStack<Scope> scopeStack = new ObjStack<>();
    private final OperatorRegistry shadowRegistry;
    private final SqlParser sqlParser;
    private final WindowExprTreeBuilder windowExprTreeBuilder = new WindowExprTreeBuilder();
    private final ObjectPool<WindowExpression> windowExpressionPool;
    private boolean stopOnTopINOperator = false;

    ExpressionParser(
            OperatorRegistry activeRegistry,
            OperatorRegistry shadowRegistry,
            ObjectPool<ExpressionNode> expressionNodePool,
            SqlParser sqlParser,
            CharacterStore characterStore,
            ObjectPool<WindowExpression> windowExpressionPool
    ) {
        this.activeRegistry = activeRegistry;
        this.shadowRegistry = shadowRegistry;
        this.expressionNodePool = expressionNodePool;
        this.sqlParser = sqlParser;
        this.characterStore = characterStore;
        this.windowExpressionPool = windowExpressionPool;
    }

    public static int extractGeoHashSuffix(int position, CharSequence tok) throws SqlException {
        assert tok.charAt(0) == '#'; // ^ ^
        // EP has already checked that the 'd' in '/d', '/dd' are numeric [0..9]
        int tokLen = tok.length();
        if (tokLen > 1) {
            if (tokLen >= 3 && tok.charAt(tokLen - 3) == '/') { // '/dd'
                short bits = (short) (10 * tok.charAt(tokLen - 2) + tok.charAt(tokLen - 1) - 528); // 10 * 48 + 48
                if (bits >= 1 && bits <= ColumnType.GEOLONG_MAX_BITS) {
                    return Numbers.encodeLowHighShorts((short) 3, bits);
                }
                throw SqlException.$(position, "invalid bits size for GEOHASH constant: ").put(tok);
            }
            if (tok.charAt(tokLen - 2) == '/') { // '/d'
                char du = tok.charAt(tokLen - 1);
                if (du >= '1' && du <= '9') {
                    return Numbers.encodeLowHighShorts((short) 2, (short) (du - 48));
                }
                throw SqlException.$(position, "invalid bits size for GEOHASH constant: ").put(tok);
            }
        }
        return Numbers.encodeLowHighShorts((short) 0, (short) (5 * Math.max(tokLen - 1, 0))); // - 1 to exclude '#'
    }

    public static boolean isGeoHashBitsConstant(CharSequence tok) {
        assert tok.charAt(0) == '#'; // ^ ^, also suffix not allowed
        int len = tok.length();
        // 2nd '#'
        return len > 1 && tok.charAt(1) == '#';
    }

    public static boolean isGeoHashCharsConstant(CharSequence tok) {
        assert tok.charAt(0) == '#'; // called by ExpressionParser where this has been checked.
        // the EP will eagerly try to detect '/dd' following the geohash token, and if so
        // it will create a FloatingSequencePair with '/' as separator. At this point
        // however, '/dd' does not exist, tok is just the potential geohash chars constant, with leading '#'
        final int len = tok.length();
        return len <= 1 || tok.charAt(1) != '#';
    }

    public void setStopOnTopINOperator(boolean stopOnTop) {
        this.stopOnTopINOperator = stopOnTop;
    }

    private static boolean cannotCastTo(int targetTag, boolean isFromNull) {
        return (targetTag < ColumnType.BOOLEAN || targetTag > ColumnType.LONG256) &&
                (!isFromNull || (targetTag != ColumnType.BINARY && targetTag != ColumnType.INTERVAL)) &&
                !moreCastTargetTypes.contains(targetTag);
    }

    private static boolean hasOffset(WindowExpression windowExpr) {
        int loKind = windowExpr.getRowsLoKind();
        int hiKind = windowExpr.getRowsHiKind();
        boolean hasOffset = (loKind == WindowExpression.PRECEDING || loKind == WindowExpression.FOLLOWING)
                && windowExpr.getRowsLoExpr() != null;
        if (!hasOffset) {
            hasOffset = (hiKind == WindowExpression.PRECEDING || hiKind == WindowExpression.FOLLOWING)
                    && windowExpr.getRowsHiExpr() != null;
        }
        return hasOffset;
    }

    private static SqlException missingArgs(int position) {
        return SqlException.$(position, "missing arguments");
    }

    private static void processDereference(GenericLexer lexer, ExpressionNode en, int lastPos) throws SqlException {

        if (en != null && Chars.equals(en.token, "::")) {
            // first off, we are going to fail here, this is an attempt to improve
            // error message rather than avert the error. In that we will fetch tokens
            // from the lexer without trying to bother with the lexers state after that.
            // check if user is writing something like x::[]type by mistake
            CharSequence tok = SqlUtil.fetchNext(lexer);
            if (Chars.equalsNc(tok, ']') && lastPos + 1 == lexer.lastTokenPosition()) {
                tok = SqlUtil.fetchNext(lexer);
                if (tok != null && ColumnType.tagOf(tok) != -1) {
                    // valid type
                    throw SqlException.position(lastPos).put("did you mean '").put(tok).put("[]'?");
                }
            }
            throw SqlException.position(lastPos).put("type definition is expected");
        }
    }

    private static @NotNull CharSequence validateIsKeyword(GenericLexer lexer, CharSequence tok, int lastPos) throws SqlException {
        final CharSequence isTok = GenericLexer.immutableOf(tok);
        tok = SqlUtil.fetchNext(lexer);
        if (tok == null) {
            throw SqlException.$(lastPos, "IS must be followed by [NOT] NULL, TRUE or FALSE");
        }
        if (SqlKeywords.isNotKeyword(tok)) {
            final int notTokPosition = lexer.lastTokenPosition();
            final CharSequence notTok = GenericLexer.immutableOf(tok);
            tok = SqlUtil.fetchNext(lexer);
            if (tok != null && (SqlKeywords.isNullKeyword(tok) || SqlKeywords.isTrueKeyword(tok) || SqlKeywords.isFalseKeyword(tok))) {
                lexer.backTo(notTokPosition + 3, notTok);
                tok = "!=";
            } else {
                throw SqlException.$(lastPos, "IS NOT must be followed by NULL, TRUE or FALSE");
            }
        } else if (SqlKeywords.isNullKeyword(tok) || SqlKeywords.isTrueKeyword(tok) || SqlKeywords.isFalseKeyword(tok)) {
            lexer.backTo(lastPos + 2, isTok);
            tok = "=";
        } else {
            throw SqlException.$(lastPos, "IS must be followed by NULL, TRUE or FALSE");
        }
        return tok;
    }

    private static int validateWhitespace(GenericLexer lexer, int lastPos, ExpressionNode en) throws SqlException {
        // Array type declaration context - strict whitespace validation
        if (lastPos > 0 && Character.isWhitespace(lexer.getContent().charAt(lastPos - 1))) {
            int hi = Chars.indexOfNonWhitespace(lexer.getContent(), en.position, lastPos, -1);
            assert hi != -1;
            throw SqlException.position(lastPos)
                    .put("array type requires no whitespace: expected '")
                    // hi is a non-whitespace char index, hence + 1
                    .put(lexer.getContent(), en.position, hi + 1).put("[]' but found '")
                    .put(lexer.getContent(), en.position, lastPos).put(" []'");
        }
        ((GenericLexer.FloatingSequence) en.token).setHi(lastPos + 1);
        return BRANCH_ARRAY_TYPE_QUALIFIER_START;
    }

    private boolean createFloatingPointConstant(GenericLexer lexer, int lastPos, CharSequence tok) throws SqlException {
        final ExpressionNode en = opStack.peek();
        if (en != null && en.type != ExpressionNode.CONTROL && en.type != ExpressionNode.OPERATION) {
            // check if this is '1.2' or '1. 2'
            if (lastPos > 0 && lexer.getContent().charAt(lastPos - 1) == '.') {
                if (en.token instanceof GenericLexer.FloatingSequence) {
                    ((GenericLexer.FloatingSequence) en.token).setHi(lexer.getTokenHi());
                } else {
                    opStack.pop();
                    CharacterStoreEntry cse = characterStore.newEntry();
                    cse.put(en.token).put(GenericLexer.unquote(tok));
                    final CharSequence lit = cse.toImmutable();
                    SqlKeywords.assertNameIsQuotedOrNotAKeyword(lit, en.position);
                    opStack.push(expressionNodePool.next().of(ExpressionNode.LITERAL, lit, Integer.MIN_VALUE, en.position));
                }
                return true;
            }
        } else {
            opStack.push(
                    SqlUtil.nextConstant(
                            expressionNodePool,
                            lexer.immutableBetween(lastPos - 1, lexer.getTokenHi()),
                            lastPos
                    )
            );
            return true;
        }
        return false;
    }

    private @Nullable CharSequence createGeoHashConst(GenericLexer lexer, CharSequence tok, int lastPos) throws SqlException {
        CharSequence geohashTok = GenericLexer.immutableOf(tok);
        // optional / bits '/dd', '/d'
        CharSequence slash = SqlUtil.fetchNext(lexer);
        if (slash == null || slash.charAt(0) != '/') {
            lexer.unparseLast();
            opStack.push(expressionNodePool.next().of(
                    ExpressionNode.CONSTANT,
                    geohashTok, // standard token, no suffix '/d', '/dd'
                    Integer.MIN_VALUE,
                    lastPos
            ));
            return tok;
        }
        tok = SqlUtil.fetchNext(lexer);
        if (tok == null || !Chars.isOnlyDecimals(tok)) { // ranges are checked later by FunctionParser.createConstant
            throw SqlException.$(lexer.lastTokenPosition(), "missing bits size for GEOHASH constant");
        }
        opStack.push(expressionNodePool.next().of(
                ExpressionNode.CONSTANT,
                lexer.immutablePairOf(geohashTok, '/', tok), // token plus suffix '/d', '/dd', where d in [0..9]
                Integer.MIN_VALUE,
                lastPos
        ));
        return tok;
    }

    private @NotNull CharSequence createGeoHashNodeStd(GenericLexer lexer, CharSequence geohashTok, int lastPos) throws SqlException {
        CharSequence tok;
        tok = SqlUtil.fetchNext(lexer);
        if (tok != null && tok.charAt(0) != ')') {
            GeoHashUtil.parseGeoHashBits(lexer.lastTokenPosition(), 0, tok); // validate geohash size token
            opStack.push(expressionNodePool.next().of(
                    ExpressionNode.CONSTANT,
                    lexer.immutablePairOf(geohashTok, tok),
                    Integer.MIN_VALUE,
                    lastPos
            ));
            tok = SqlUtil.fetchNext(lexer);
            if (tok == null || tok.charAt(0) != ')') {
                throw SqlException.$(lexer.lastTokenPosition(), "invalid GEOHASH, missing ')'");
            }
        } else {
            throw SqlException.$(lexer.lastTokenPosition(), "invalid GEOHASH, invalid type precision");
        }
        return tok;
    }

    private void createOperation(OperatorExpression op, int lastPos, int operatorType) {
        ExpressionNode node;
        node = expressionNodePool.next().of(
                op.type == OperatorExpression.SET ? ExpressionNode.SET_OPERATION : ExpressionNode.OPERATION,
                op.operator.token,
                op.precedence,
                lastPos
        );
        if (operatorType == OperatorExpression.UNARY) {
            node.paramCount = 1;
        } else if (SqlKeywords.isBetweenKeyword(node.token)) {
            node.paramCount = 3;
        } else {
            node.paramCount = 2;
        }
        opStack.push(node);
    }

    private boolean isCompletedOperand(int branchTag) {
        return branchTag == BRANCH_LITERAL
                || branchTag == BRANCH_CONSTANT
                || branchTag == BRANCH_GEOHASH
                || branchTag == BRANCH_DECIMAL
                || branchTag == BRANCH_RIGHT_BRACKET
                || branchTag == BRANCH_RIGHT_PARENTHESIS;
    }

    private boolean isCount() {
        return opStack.size() == 2 && Chars.equals(opStack.peek().token, '(') && SqlKeywords.isCountKeyword(opStack.peek(1).token);
    }

    private boolean isExtractFunctionOnStack() {
        boolean found = false;
        for (int i = 0, n = opStack.size(); i < n; i++) {
            ExpressionNode peek = opStack.peek(i);
            if (Chars.equals(peek.token, '(')) {
                if ((i + 1) < n && SqlKeywords.isExtractKeyword(opStack.peek(i + 1).token)) {
                    found = true;
                    break;
                }
            }
        }
        return found;
    }

    private boolean isTypeQualifier() {
        return opStack.size() >= 2 && SqlKeywords.isColonColon(opStack.peek(1).token);
    }

    private int onNode(
            ExpressionParserListener listener, ExpressionNode node, int argStackDepth, int prevBranch
    ) throws SqlException {
        return onNode(listener, node, argStackDepth, prevBranch, false);
    }

    private int onNode(
            ExpressionParserListener listener,
            ExpressionNode node,
            int argStackDepth,
            int prevBranch,
            boolean exprStackUnwind
    ) throws SqlException {
        if (node.type == ExpressionNode.OPERATION && Chars.equals(node.token, ':') &&
                (argStackDepth == 1 || prevBranch == BRANCH_OPERATOR)
        ) {
            node.paramCount = 1;
        }
        if (argStackDepth < node.paramCount) {
            throw SqlException.position(node.position).put("too few arguments for '").put(node.token)
                    .put("' [found=").put(argStackDepth)
                    .put(",expected=").put(node.paramCount).put(']');
        }
        if (node.type == ExpressionNode.LITERAL) {
            // when stack unwinds, not every keyword is expected to be column or table name and may not
            // need to be validated as such
            if (exprStackUnwind) {
                SqlKeywords.assertNameIsQuotedOrNotAKeyword(node.token, node.position);
            }
            node.token = GenericLexer.unquote(node.token);
        }
        listener.onNode(node);
        return argStackDepth - node.paramCount + 1;
    }

    private int parseOverExpr(GenericLexer lexer, ExpressionParserListener listener, SqlParserCallback sqlParserCallback, LowerCaseCharSequenceObjHashMap<ExpressionNode> decls, ExpressionNode node, int localParamCount, int argStackDepth, int prevBranch) throws SqlException {
        CharSequence nextTok = SqlUtil.fetchNext(lexer);

        // Check for IGNORE NULLS or RESPECT NULLS before OVER
        boolean ignoreNulls = false;
        int nullsDescPos = 0;
        if (nextTok != null && (SqlKeywords.isIgnoreWord(nextTok) || SqlKeywords.isRespectWord(nextTok))) {
            boolean isIgnore = SqlKeywords.isIgnoreWord(nextTok);
            nullsDescPos = lexer.lastTokenPosition();
            CharSequence nullsWord = SqlUtil.fetchNext(lexer);
            if (nullsWord != null && SqlKeywords.isNullsWord(nullsWord)) {
                ignoreNulls = isIgnore;
                nextTok = SqlUtil.fetchNext(lexer);
            } else if (nullsWord != null && SqlKeywords.isOverKeyword(nullsWord)) {
                // User wrote "IGNORE OVER" or "RESPECT OVER" - missing NULLS
                throw SqlException.$(lexer.lastTokenPosition(), "'nulls' expected after '")
                        .put(isIgnore ? "ignore" : "respect").put('\'');
            } else if (nullsWord != null && SqlKeywords.isNullKeyword(nullsWord)) {
                // User wrote "IGNORE NULL" instead of "IGNORE NULLS" - common typo
                throw SqlException.$(lexer.lastTokenPosition(), "'nulls' expected, not 'null'");
            } else {
                // Not IGNORE/RESPECT NULLS pattern, restore tokens
                if (nullsWord != null) {
                    lexer.unparseLast();
                }
                lexer.unparseLast();
                nextTok = SqlUtil.fetchNext(lexer);
                nullsDescPos = 0;
            }
        }

        if (nextTok != null && SqlKeywords.isOverKeyword(nextTok)) {
            // This is a window function - parse the OVER clause
            // First, update node to be a function
            node.paramCount = localParamCount + Math.max(0, node.paramCount - 1);
            node.type = ExpressionNode.FUNCTION;
            // Pop the function node before parsing OVER clause
            // to prevent inner expression parsing from interfering with it
            opStack.pop();
            parseWindowClause(lexer, node, sqlParserCallback, decls, ignoreNulls, nullsDescPos);
            argStackDepth = onNode(listener, node, argStackDepth, prevBranch);
        } else if (nullsDescPos != 0) {
            // We parsed IGNORE/RESPECT NULLS but no OVER follows
            throw SqlException.$(lexer.lastTokenPosition(), "'over' expected after 'nulls'");
        } else if (nextTok != null) {
            lexer.unparseLast();
        }
        return argStackDepth;
    }

    private boolean parsePlus(GenericLexer lexer, int prevBranch, int lastPos) {
        boolean processDefaultBranch = true;
        if (prevBranch == BRANCH_CONSTANT && lastPos > 0) {
            char c = lexer.getContent().charAt(lastPos - 1);
            if (c == 'e' || c == 'E') { // Incomplete scientific floating-point literal
                ExpressionNode en = opStack.peek();
                ((GenericLexer.FloatingSequence) en.token).setHi(lastPos + 1);
                processDefaultBranch = false;
            }
        }
        return processDefaultBranch;
    }

    /**
     * Parses a time unit keyword (HOUR, MINUTE, SECOND, etc.) and returns the corresponding constant.
     * Returns 0 if the token is not a time unit.
     */
    private char parseTimeUnit(CharSequence tok) {
        if (tok == null) {
            return 0;
        }
        if (SqlKeywords.isHourKeyword(tok) || SqlKeywords.isHoursKeyword(tok)) {
            return WindowExpression.TIME_UNIT_HOUR;
        }
        if (SqlKeywords.isMinuteKeyword(tok) || SqlKeywords.isMinutesKeyword(tok)) {
            return WindowExpression.TIME_UNIT_MINUTE;
        }
        if (SqlKeywords.isSecondKeyword(tok) || SqlKeywords.isSecondsKeyword(tok)) {
            return WindowExpression.TIME_UNIT_SECOND;
        }
        if (SqlKeywords.isMillisecondKeyword(tok) || SqlKeywords.isMillisecondsKeyword(tok)) {
            return WindowExpression.TIME_UNIT_MILLISECOND;
        }
        if (SqlKeywords.isMicrosecondKeyword(tok) || SqlKeywords.isMicrosecondsKeyword(tok)) {
            return WindowExpression.TIME_UNIT_MICROSECOND;
        }
        if (SqlKeywords.isNanosecondKeyword(tok) || SqlKeywords.isNanosecondsKeyword(tok)) {
            return WindowExpression.TIME_UNIT_NANOSECOND;
        }
        if (SqlKeywords.isDayKeyword(tok) || SqlKeywords.isDaysKeyword(tok)) {
            return WindowExpression.TIME_UNIT_DAY;
        }
        return 0;
    }

    /**
     * Parses window function OVER clause and attaches the WindowColumn to the function node.
     * Expected to be called when we've just finished parsing a function call and peeked OVER keyword.
     *
     * @param lexer             the lexer positioned after OVER keyword
     * @param functionNode      the function node to attach window context to
     * @param sqlParserCallback callback for nested expression parsing
     * @param decls             declarations for expression parsing
     * @param ignoreNulls       true if IGNORE NULLS was specified before OVER
     * @param nullsDescPos      position of IGNORE/RESPECT keyword (0 if not specified)
     */
    private void parseWindowClause(
            GenericLexer lexer,
            ExpressionNode functionNode,
            SqlParserCallback sqlParserCallback,
            @Nullable LowerCaseCharSequenceObjHashMap<ExpressionNode> decls,
            boolean ignoreNulls,
            int nullsDescPos
    ) throws SqlException {
        // OVER keyword already consumed, expect '(' or window name
        CharSequence tok = SqlUtil.fetchNext(lexer);
        if (tok == null) {
            throw SqlException.$(lexer.lastTokenPosition(), "'(' or window name expected after OVER");
        }

        WindowExpression windowCol = windowExpressionPool.next().of(null, functionNode);
        windowCol.setIgnoreNulls(ignoreNulls);
        windowCol.setNullsDescPos(nullsDescPos);
        functionNode.windowExpression = windowCol;

        // Check if this is a named window reference (OVER w) or inline spec (OVER (...))
        if (tok.charAt(0) != '(') {
            // Named window reference - validate and store the name for later resolution
            SqlParser.validateIdentifier(lexer, tok);
            SqlKeywords.assertNameIsQuotedOrNotAKeyword(tok, lexer.lastTokenPosition());
            windowCol.setWindowName(Chars.toString(GenericLexer.unquote(tok)), lexer.lastTokenPosition());
            return;
        }

        // Inline window specification - delegate to shared parsing method
        parseWindowSpec(lexer, windowCol, sqlParserCallback, decls);
    }

    /**
     * Parses an expression within a window clause context.
     * Uses a separate tree builder to avoid state conflicts with the outer expression parsing.
     * Saves and restores parser state since we're calling parseExpr recursively.
     */
    private ExpressionNode parseWindowExpr(
            GenericLexer lexer,
            SqlParserCallback sqlParserCallback,
            @Nullable LowerCaseCharSequenceObjHashMap<ExpressionNode> decls
    ) throws SqlException {
        // Save stack bottoms - we need to isolate the inner parseExpr from the outer context
        // This prevents the inner parseExpr from seeing/popping items from the outer expression
        // (e.g., a CASE node that belongs to the outer expression)
        int savedOpStackBottom = opStack.getBottom();
        opStack.setBottom(opStack.sizeRaw());

        int savedParamCountStackBottom = paramCountStack.bottom();
        paramCountStack.setBottom(paramCountStack.sizeRaw());

        int savedArgStackDepthStackBottom = argStackDepthStack.bottom();
        argStackDepthStack.setBottom(argStackDepthStack.sizeRaw());

        try {
            // Reuse the tree builder to avoid allocations
            windowExprTreeBuilder.reset();
            parseExpr(lexer, windowExprTreeBuilder, sqlParserCallback, decls);
            return windowExprTreeBuilder.getResult();
        } finally {
            // Restore stack bottoms
            opStack.setBottom(savedOpStackBottom);
            paramCountStack.setBottom(savedParamCountStackBottom);
            argStackDepthStack.setBottom(savedArgStackDepthStackBottom);
        }
    }

    /**
     * Parses a single frame bound (UNBOUNDED PRECEDING, CURRENT ROW, or expr PRECEDING/FOLLOWING)
     */
    private void parseWindowFrameBound(
            GenericLexer lexer,
            WindowExpression windowCol,
            CharSequence tok,
            boolean isLowerBound,
            SqlParserCallback sqlParserCallback,
            @Nullable LowerCaseCharSequenceObjHashMap<ExpressionNode> decls
    ) throws SqlException {
        if (tok == null) {
            throw SqlException.$(lexer.lastTokenPosition(), "'unbounded', 'current' or expression expected");
        }

        int pos = lexer.lastTokenPosition();

        // Check for common error: PRECEDING/FOLLOWING without a value
        if (SqlKeywords.isPrecedingKeyword(tok) || SqlKeywords.isFollowingKeyword(tok)) {
            throw SqlException.$(pos, "frame bound value expected before '").put(tok).put('\'');
        }

        if (SqlKeywords.isUnboundedKeyword(tok)) {
            tok = SqlUtil.fetchNext(lexer);
            if (tok != null && SqlKeywords.isPrecedingKeyword(tok)) {
                if (isLowerBound) {
                    windowCol.setRowsLoKind(WindowExpression.PRECEDING, lexer.lastTokenPosition());
                } else {
                    throw SqlException.$(lexer.lastTokenPosition(), "frame end cannot be UNBOUNDED PRECEDING, use UNBOUNDED FOLLOWING");
                }
            } else if (tok != null && SqlKeywords.isFollowingKeyword(tok)) {
                if (!isLowerBound) {
                    windowCol.setRowsHiKind(WindowExpression.FOLLOWING, lexer.lastTokenPosition());
                } else {
                    throw SqlException.$(lexer.lastTokenPosition(), "frame start cannot be UNBOUNDED FOLLOWING, use UNBOUNDED PRECEDING");
                }
            } else {
                throw SqlException.$(lexer.lastTokenPosition(), "'preceding' or 'following' expected after 'unbounded'");
            }
        } else if (SqlKeywords.isCurrentKeyword(tok)) {
            tok = SqlUtil.fetchNext(lexer);
            if (tok == null || !SqlKeywords.isRowKeyword(tok)) {
                throw SqlException.$(lexer.lastTokenPosition(), "'row' expected after 'current'");
            }
            if (isLowerBound) {
                windowCol.setRowsLoKind(WindowExpression.CURRENT, lexer.lastTokenPosition());
            } else {
                windowCol.setRowsHiKind(WindowExpression.CURRENT, lexer.lastTokenPosition());
            }
        } else {
            // Expression followed by optional time unit and PRECEDING or FOLLOWING
            lexer.unparseLast();
            ExpressionNode boundExpr = parseWindowExpr(lexer, sqlParserCallback, decls);

            // Check for optional time unit (HOUR, MINUTE, SECOND, etc.) - only valid for RANGE mode
            tok = SqlUtil.fetchNext(lexer);
            char timeUnit = parseTimeUnit(tok);
            if (timeUnit != 0) {
                if (windowCol.getFramingMode() != WindowExpression.FRAMING_RANGE) {
                    throw SqlException.$(lexer.lastTokenPosition(), "time units are only valid with RANGE frames, not ROWS or GROUPS");
                }
                if (isLowerBound) {
                    windowCol.setRowsLoExprTimeUnit(timeUnit);
                } else {
                    windowCol.setRowsHiExprTimeUnit(timeUnit);
                }
                tok = SqlUtil.fetchNext(lexer);
            }

            if (tok != null && SqlKeywords.isPrecedingKeyword(tok)) {
                if (isLowerBound) {
                    windowCol.setRowsLoExpr(boundExpr, pos);
                    windowCol.setRowsLoKind(WindowExpression.PRECEDING, lexer.lastTokenPosition());
                } else {
                    windowCol.setRowsHiExpr(boundExpr, pos);
                    windowCol.setRowsHiKind(WindowExpression.PRECEDING, lexer.lastTokenPosition());
                }
            } else if (tok != null && SqlKeywords.isFollowingKeyword(tok)) {
                if (isLowerBound) {
                    windowCol.setRowsLoExpr(boundExpr, pos);
                    windowCol.setRowsLoKind(WindowExpression.FOLLOWING, lexer.lastTokenPosition());
                } else {
                    windowCol.setRowsHiExpr(boundExpr, pos);
                    windowCol.setRowsHiKind(WindowExpression.FOLLOWING, lexer.lastTokenPosition());
                }
            } else {
                throw SqlException.$(lexer.lastTokenPosition(), "'preceding' or 'following' expected");
            }
        }
    }

    /**
     * Parses frame clause (BETWEEN ... AND ... or single bound specification)
     */
    private CharSequence parseWindowFrameClause(
            GenericLexer lexer,
            WindowExpression windowCol,
            SqlParserCallback sqlParserCallback,
            @Nullable LowerCaseCharSequenceObjHashMap<ExpressionNode> decls
    ) throws SqlException {
        CharSequence tok = SqlUtil.fetchNext(lexer);
        if (tok == null) {
            throw SqlException.$(lexer.lastTokenPosition(), "'between', 'unbounded', 'current' or expression expected");
        }

        if (SqlKeywords.isBetweenKeyword(tok)) {
            // BETWEEN low AND high
            tok = SqlUtil.fetchNext(lexer);
            parseWindowFrameBound(lexer, windowCol, tok, true, sqlParserCallback, decls);

            tok = SqlUtil.fetchNext(lexer);
            if (tok == null || !SqlKeywords.isAndKeyword(tok)) {
                throw SqlException.$(lexer.lastTokenPosition(), "'and' expected");
            }

            tok = SqlUtil.fetchNext(lexer);
            parseWindowFrameBound(lexer, windowCol, tok, false, sqlParserCallback, decls);

            // Validate frame bounds order
            int loKind = windowCol.getRowsLoKind();
            int hiKind = windowCol.getRowsHiKind();
            if (hiKind == WindowExpression.PRECEDING) {
                if (loKind == WindowExpression.CURRENT) {
                    throw SqlException.$(windowCol.getRowsHiKindPos(), "frame starting from CURRENT ROW must end with CURRENT ROW or FOLLOWING");
                }
                if (loKind == WindowExpression.FOLLOWING) {
                    throw SqlException.$(windowCol.getRowsHiKindPos(), "frame starting from FOLLOWING must end with FOLLOWING");
                }
            }

        } else {
            // Single bound - defaults to CURRENT ROW for upper bound
            parseWindowFrameBound(lexer, windowCol, tok, true, sqlParserCallback, decls);

            // In single-bound mode, only PRECEDING is allowed (not FOLLOWING)
            if (windowCol.getRowsLoKind() == WindowExpression.FOLLOWING) {
                throw SqlException.$(windowCol.getRowsLoKindPos(), "single-bound frame specification requires PRECEDING, use BETWEEN for FOLLOWING");
            }

            windowCol.setRowsHiKind(WindowExpression.CURRENT, windowCol.getRowsLoKindPos());
        }

        // Check for EXCLUDE clause
        tok = SqlUtil.fetchNext(lexer);
        if (tok != null && SqlKeywords.isExcludeKeyword(tok)) {
            int excludePos = lexer.lastTokenPosition();
            tok = SqlUtil.fetchNext(lexer);
            if (tok == null) {
                throw SqlException.$(lexer.lastTokenPosition(), "'current row', 'group', 'ties' or 'no others' expected after 'exclude'");
            }

            if (SqlKeywords.isCurrentKeyword(tok)) {
                // EXCLUDE CURRENT ROW
                tok = SqlUtil.fetchNext(lexer);
                if (tok == null || !SqlKeywords.isRowKeyword(tok)) {
                    throw SqlException.$(lexer.lastTokenPosition(), "'row' expected after 'current'");
                }
                windowCol.setExclusionKind(WindowExpression.EXCLUDE_CURRENT_ROW, excludePos);
            } else if (SqlKeywords.isGroupKeyword(tok)) {
                // EXCLUDE GROUP
                windowCol.setExclusionKind(WindowExpression.EXCLUDE_GROUP, excludePos);
            } else if (SqlKeywords.isTiesKeyword(tok)) {
                // EXCLUDE TIES
                windowCol.setExclusionKind(WindowExpression.EXCLUDE_TIES, excludePos);
            } else if (SqlKeywords.isNoKeyword(tok)) {
                // EXCLUDE NO OTHERS
                tok = SqlUtil.fetchNext(lexer);
                if (tok == null || !SqlKeywords.isOthersKeyword(tok)) {
                    throw SqlException.$(lexer.lastTokenPosition(), "'others' expected after 'no'");
                }
                windowCol.setExclusionKind(WindowExpression.EXCLUDE_NO_OTHERS, excludePos);
            } else {
                throw SqlException.$(lexer.lastTokenPosition(), "'current row', 'group', 'ties' or 'no others' expected after 'exclude'");
            }
            tok = SqlUtil.fetchNext(lexer);
        }

        return tok;
    }

    /**
     * Parses a window specification for WINDOW clause definitions.
     * Expects the opening '(' to already be consumed.
     * Parses PARTITION BY, ORDER BY, and frame specification.
     * Consumes and verifies the closing ')'.
     *
     * @param lexer             the lexer positioned after the '('
     * @param windowCol         the WindowExpression to populate with the specification
     * @param sqlParserCallback callback for nested expression parsing
     * @param decls             declarations for expression parsing
     */
    void parseWindowSpec(
            GenericLexer lexer,
            WindowExpression windowCol,
            SqlParserCallback sqlParserCallback,
            @Nullable LowerCaseCharSequenceObjHashMap<ExpressionNode> decls
    ) throws SqlException {
        CharSequence tok = SqlUtil.fetchNext(lexer);
        if (tok == null) {
            throw SqlException.$(lexer.lastTokenPosition(), "')' or window specification expected");
        }

        // Detect window inheritance attempts like WINDOW w2 AS (w1 ORDER BY y) or (w1).
        // If the first token is an unquoted non-keyword identifier followed by ')' or a
        // window clause keyword, it's a reference to another named window.
        if (tok.charAt(0) != ')'
                && !SqlKeywords.isPartitionKeyword(tok)
                && !SqlKeywords.isOrderKeyword(tok)
                && !SqlKeywords.isRowsKeyword(tok)
                && !SqlKeywords.isRangeKeyword(tok)
                && !SqlKeywords.isGroupsKeyword(tok)
                && !SqlKeywords.isCumulativeKeyword(tok)
                && !Chars.isQuoted(tok)) {
            int inheritPos = lexer.lastTokenPosition();
            CharSequence nextTok = SqlUtil.fetchNext(lexer);
            // Look at what follows: if it's ')' or a window clause keyword, this is inheritance
            if (nextTok != null && (nextTok.charAt(0) == ')'
                    || SqlKeywords.isPartitionKeyword(nextTok)
                    || SqlKeywords.isOrderKeyword(nextTok)
                    || SqlKeywords.isRowsKeyword(nextTok)
                    || SqlKeywords.isRangeKeyword(nextTok)
                    || SqlKeywords.isGroupsKeyword(nextTok)
                    || SqlKeywords.isCumulativeKeyword(nextTok))) {
                throw SqlException.$(inheritPos, "window inheritance is not supported");
            }
            // Not inheritance - restore both tokens and let downstream parsing handle the error
            lexer.backTo(inheritPos, tok);
            tok = SqlUtil.fetchNext(lexer);
        }

        // Handle PARTITION BY
        if (SqlKeywords.isPartitionKeyword(tok)) {
            tok = SqlUtil.fetchNext(lexer);
            if (tok == null || !SqlKeywords.isByKeyword(tok)) {
                throw SqlException.$(lexer.lastTokenPosition(), "'by' expected after 'partition'");
            }

            boolean expectingExpression = true;
            do {
                tok = SqlUtil.fetchNext(lexer);
                if (tok == null) {
                    throw SqlException.$(lexer.lastTokenPosition(), "column name expected");
                }
                if (SqlKeywords.isOrderKeyword(tok) || tok.charAt(0) == ')') {
                    if (expectingExpression) {
                        throw SqlException.$(lexer.lastTokenPosition(), "column name expected");
                    }
                    break;
                }
                lexer.unparseLast();
                ExpressionNode partitionExpr = parseWindowExpr(lexer, sqlParserCallback, decls);
                windowCol.getPartitionBy().add(partitionExpr);
                expectingExpression = false;
                tok = SqlUtil.fetchNext(lexer);
                if (tok == null) {
                    throw SqlException.$(lexer.lastTokenPosition(), "'order', ',' or ')' expected");
                }
                if (tok.charAt(0) == ',') {
                    expectingExpression = true;
                }
            } while (tok.charAt(0) == ',');
        }

        // Handle ORDER BY
        if (SqlKeywords.isOrderKeyword(tok)) {
            tok = SqlUtil.fetchNext(lexer);
            if (tok == null || !SqlKeywords.isByKeyword(tok)) {
                throw SqlException.$(lexer.lastTokenPosition(), "'by' expected after 'order'");
            }

            do {
                ExpressionNode orderExpr = parseWindowExpr(lexer, sqlParserCallback, decls);
                if (orderExpr == null) {
                    throw SqlException.$(lexer.lastTokenPosition(), "Expression expected");
                }
                tok = SqlUtil.fetchNext(lexer);

                int direction = QueryModel.ORDER_DIRECTION_ASCENDING;
                if (tok != null && SqlKeywords.isDescKeyword(tok)) {
                    direction = QueryModel.ORDER_DIRECTION_DESCENDING;
                    tok = SqlUtil.fetchNext(lexer);
                } else if (tok != null && SqlKeywords.isAscKeyword(tok)) {
                    tok = SqlUtil.fetchNext(lexer);
                }
                windowCol.addOrderBy(orderExpr, direction);

                if (tok == null) {
                    throw SqlException.$(lexer.lastTokenPosition(), "')' expected to close window specification");
                }
            } while (tok.charAt(0) == ',');
        }

        // Handle ROWS/RANGE/GROUPS/CUMULATIVE frame specification
        if (!Chars.equals(tok, ')')) {
            int framingMode = -1;
            int frameModePos = lexer.lastTokenPosition();

            if (SqlKeywords.isCumulativeKeyword(tok)) {
                // CUMULATIVE is shorthand for ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                if (windowCol.getOrderBy().size() == 0) {
                    throw SqlException.$(frameModePos, "CUMULATIVE requires an ORDER BY clause");
                }
                windowCol.setFramingMode(WindowExpression.FRAMING_ROWS);
                windowCol.setRowsLoKind(WindowExpression.PRECEDING, frameModePos);
                windowCol.setRowsHiKind(WindowExpression.CURRENT, frameModePos);
                tok = SqlUtil.fetchNext(lexer);
            } else {
                if (SqlKeywords.isRowsKeyword(tok)) {
                    framingMode = WindowExpression.FRAMING_ROWS;
                } else if (SqlKeywords.isRangeKeyword(tok)) {
                    framingMode = WindowExpression.FRAMING_RANGE;
                } else if (SqlKeywords.isGroupsKeyword(tok)) {
                    framingMode = WindowExpression.FRAMING_GROUPS;
                }

                if (framingMode == -1) {
                    throw SqlException.$(frameModePos, "'rows', 'range', 'groups', 'cumulative' or ')' expected");
                }

                // GROUPS mode requires ORDER BY
                if (framingMode == WindowExpression.FRAMING_GROUPS && windowCol.getOrderBy().size() == 0) {
                    throw SqlException.$(frameModePos, "GROUPS mode requires an ORDER BY clause");
                }
                windowCol.setFramingMode(framingMode);
                tok = parseWindowFrameClause(lexer, windowCol, sqlParserCallback, decls);

                // RANGE with offset PRECEDING/FOLLOWING requires exactly one ORDER BY column
                if (framingMode == WindowExpression.FRAMING_RANGE && windowCol.getOrderBy().size() != 1 && hasOffset(windowCol)) {
                    throw SqlException.$(frameModePos, "RANGE with offset PRECEDING/FOLLOWING requires exactly one ORDER BY column");
                }
            }
        }

        if (tok == null || tok.charAt(0) != ')') {
            throw SqlException.$(lexer.lastTokenPosition(), "')' expected to close window specification");
        }
    }

    private int popAndOpStack(ExpressionParserListener listener, int argStackDepth, int prevBranch) throws SqlException {
        ExpressionNode node;
        while ((node = opStack.pop()) != null && !SqlKeywords.isBetweenKeyword(node.token)) {
            argStackDepth = onNode(listener, node, argStackDepth, prevBranch);
        }

        if (node != null) {
            opStack.push(node);
        }
        return argStackDepth;
    }

    private int popArrayOpStack(ExpressionParserListener listener, int argStackDepth, int prevBranch) throws SqlException {
        // pop left literal or . expression, e.g. "a.b[i]" and push to the output queue.
        // the precedence of '[' is fixed to 2
        ExpressionNode other;
        while ((other = opStack.peek()) != null && (other.type == ExpressionNode.LITERAL ||
                other.type == ExpressionNode.ARRAY_CONSTRUCTOR ||
                other.type == ExpressionNode.ARRAY_ACCESS)
        ) {
            argStackDepth = onNode(listener, other, argStackDepth, prevBranch);
            opStack.pop();
        }
        return argStackDepth;
    }

    private int popLiteralAndFunctionOpStack(ExpressionParserListener listener, ExpressionNode node, int localParamCount, int argStackDepth, int prevBranch) throws SqlException {
        if (node.type == ExpressionNode.LITERAL) {
            // the parenthesised expression is preceded by a literal => it's a function call
            node.paramCount = localParamCount + Math.max(0, node.paramCount - 1);
            node.type = ExpressionNode.FUNCTION;
            argStackDepth = onNode(listener, node, argStackDepth, prevBranch);
            opStack.pop();
        } else if (node.type == ExpressionNode.SET_OPERATION && !SqlKeywords.isBetweenKeyword(node.token)) {
            node.paramCount = localParamCount + Math.max(0, node.paramCount - 1);
            if (node.paramCount < 2) {
                throw SqlException.position(node.position).put("too few arguments for '").put(node.token).put('\'');
            }
            node.type = ExpressionNode.FUNCTION;
            argStackDepth = onNode(listener, node, argStackDepth, prevBranch);
            opStack.pop();
        }
        return argStackDepth;
    }

    private void processAliasedWildcard(GenericLexer lexer, int lastPos) {
        final ExpressionNode en = opStack.peek();
        if (en != null && en.type != ExpressionNode.CONTROL) {
            // leverage the fact '*' is dedicated token, and it returned from cache
            // therefore lexer.tokenHi does not move when * follows dot without whitespace
            // e.g. 'a.*'
            if (en.token instanceof GenericLexer.FloatingSequence fs) {
                fs.setHi(lastPos + 1);
            } else {
                // "foo".* or 'foo'.*
                // foo was unquoted, and we cannot simply move hi to include the *
                opStack.pop();
                CharacterStoreEntry cse = characterStore.newEntry();
                cse.put(en.token).put('*');
                opStack.push(expressionNodePool.next().of(ExpressionNode.LITERAL, cse.toImmutable(), Integer.MIN_VALUE, en.position));
            }
        } else {
            opStack.push(expressionNodePool.next().of(
                    ExpressionNode.CONSTANT,
                    lexer.immutableBetween(lastPos - 1, lexer.getTokenHi()),
                    0,
                    lastPos
            ));
        }
    }

    private @NotNull CharSequence processColon(GenericLexer lexer, CharSequence tok, int prevBranch, int lastPos) {
        if (tok.length() > 1 || isCompletedOperand(prevBranch)) {
            return tok;
        }
        CharSequence content = lexer.getContent();
        int posAfterColon = lastPos + 1;
        if (content.length() <= posAfterColon) {
            return tok;
        }
        if (!Character.isWhitespace(content.charAt(posAfterColon))) {
            CharSequence nextToken = lexer.next();
            tok = (String) tok + nextToken;
        }
        return tok;
    }

    private int processCommaAndPopOpStack(ExpressionParserListener listener, int lastPos, int argStackDepth, int prevBranch) throws SqlException {
        ExpressionNode node;
        Scope scope1 = scopeStack.peek(1);
        if (scope1 == Scope.CAST || scope1 == Scope.CAST_AS) {
            throw SqlException.$(lastPos, "',' is not expected here");
        }

        // The comma is a function argument separator:
        // Until the token at the top of the stack is a left paren/bracket,
        // pop operators off the stack onto the output queue. If no left
        // parens/brackets are encountered, either the separator was misplaced or
        // paren/bracket was mismatched.
        while ((node = opStack.pop()) != null && node.type != ExpressionNode.CONTROL) {
            argStackDepth = onNode(listener, node, argStackDepth, prevBranch);
        }
        assert node != null : "opStack is empty at ','";
        opStack.push(node);
        return argStackDepth;
    }

    private @NotNull CharSequence processDecimal(GenericLexer lexer, CharSequence decimalTok, CharSequence precisionTok, int lastPos) throws SqlException {
        CharSequence tok = SqlUtil.fetchNext(lexer);
        if (tok == null) {
            throw SqlException.$(lexer.lastTokenPosition(), "Invalid decimal type. Missing ')'");
        } else if (tok.charAt(0) == ')') {
            opStack.push(expressionNodePool.next().of(
                    ExpressionNode.CONSTANT,
                    lexer.immutablePairOf(decimalTok, '_', precisionTok),
                    Integer.MIN_VALUE,
                    lastPos
            ));
        } else if (tok.charAt(0) == ',') {
            tok = SqlUtil.fetchNext(lexer);
            if (tok == null) {
                throw SqlException.$(lexer.lastTokenPosition(), "Invalid decimal type. The scale is missing");
            }
            DecimalUtil.parseScale(lexer.lastTokenPosition(), tok, 0, tok.length());
            opStack.push(expressionNodePool.next().of(
                    ExpressionNode.CONSTANT,
                    lexer.immutableTripleOf('_', decimalTok, precisionTok, tok),
                    Integer.MIN_VALUE,
                    lastPos
            ));
            tok = SqlUtil.fetchNext(lexer);
            if (tok == null || tok.charAt(0) != ')') {
                throw SqlException.$(lexer.lastTokenPosition(), "Invalid decimal type. Missing ')'");
            }
        } else {
            throw SqlException.$(lexer.lastTokenPosition(), "Invalid decimal type. Expected ',' or ')' after precision");
        }
        return tok;
    }

    private boolean processDecimalRewrite(GenericLexer lexer, CharSequence tokenStash, int lastPos) throws SqlException {
        ExpressionNode en;
        CharSequence nextToken = SqlUtil.fetchNext(lexer);
        if (nextToken != null) {
            if (Chars.equals(nextToken, ')') || Chars.equals(nextToken, ',') || Chars.equals(nextToken, "::")) {
                // this means 'distinct' is meant to be used as a column name and not as a keyword.
                // at this point we also know 'distinct' is not in double-quotes since otherwise the CASE wouldn't match
                // we call assertTableNameIsQuotedOrNotAKeyword() to ensure a consistent error message
                SqlKeywords.assertNameIsQuotedOrNotAKeyword(tokenStash, lastPos);
            } else {
                en = opStack.peek(1);
                if (en.type == ExpressionNode.LITERAL) {
                    if (SqlKeywords.isCountKeyword(en.token)) {
                        if (Chars.equals(nextToken, '*')) {
                            throw SqlException.$(lastPos, "count(distinct *) is not supported");
                        }
                        en.token = "count_distinct";
                        lexer.unparseLast();
                        return true;
                    } else if (Chars.equalsIgnoreCase("string_agg", en.token)) {
                        en.token = "string_distinct_agg";
                        lexer.unparseLast();
                        return true;
                    }
                }
            }
            lexer.unparseLast();
        }
        return false;
    }

    private int processLambdaQuery(
            GenericLexer lexer,
            ExpressionParserListener listener,
            int argStackDepth,
            SqlParserCallback sqlParserCallback,
            @Nullable LowerCaseCharSequenceObjHashMap<ExpressionNode> decls
    ) throws SqlException {
        // It is highly likely this expression parser will be re-entered when
        // parsing sub-query. To prevent sub-query consuming operation stack we must add a
        // control node, which would prevent such consumption

        // precedence must be max value to make sure control node isn't
        // consumed as parameter to a greedy function
        opStack.push(expressionNodePool.next().of(ExpressionNode.CONTROL, "|", Integer.MAX_VALUE, lexer.lastTokenPosition()));

        final int savedParamCountStackBottom = paramCountStack.bottom();
        paramCountStack.setBottom(paramCountStack.sizeRaw());
        final int savedArgStackDepthStackBottom = argStackDepthStack.bottom();
        argStackDepthStack.setBottom(argStackDepthStack.sizeRaw());

        int pos = lexer.lastTokenPosition();
        // allow sub-query to parse "select" keyword
        lexer.unparseLast();

        ExpressionNode node = expressionNodePool.next().of(ExpressionNode.QUERY, null, 0, pos);
        // validate is Query is allowed
        onNode(listener, node, argStackDepth, BRANCH_NONE);
        // we can compile query if all is well
        node.queryModel = sqlParser.parseAsSubQuery(lexer, null, true, sqlParserCallback, decls, false);
        argStackDepth = onNode(listener, node, argStackDepth, BRANCH_NONE);

        // pop our control node if sub-query hasn't done it
        ExpressionNode control = opStack.peek();
        if (control != null && control.type == ExpressionNode.CONTROL && Chars.equals(control.token, '|')) {
            opStack.pop();
        }

        paramCountStack.setBottom(savedParamCountStackBottom);
        argStackDepthStack.setBottom(savedArgStackDepthStackBottom);
        return argStackDepth;
    }

    private int processPgTimestampCast(
            ExpressionParserListener listener,
            boolean isCastingNull,
            CharSequence tok,
            int lastPos,
            int prevBranch,
            int argStackDepth
    ) throws SqlException {
        ExpressionNode prevNode = opStack.pop();
        // This is postgres syntax to cast string literal to a type
        // timestamp '2005-04-02 12:00:00-07'
        // long '12321312'
        // timestamp with time zone '2005-04-02 12:00:00-07'

        // validate type
        final short columnType = ColumnType.tagOf(prevNode.token);
        if (cannotCastTo(columnType, isCastingNull)) {
            throw SqlException.$(prevNode.position, "impossible type cast, invalid type");
        }
        ExpressionNode stringLiteral = SqlUtil.nextConstant(expressionNodePool, GenericLexer.immutableOf(tok), lastPos);
        onNode(listener, stringLiteral, 0, prevBranch);

        prevNode.type = ExpressionNode.CONSTANT;
        onNode(listener, prevNode, 0, prevBranch);

        ExpressionNode cast = expressionNodePool.next().of(ExpressionNode.FUNCTION, "cast", 0, prevNode.position);
        cast.paramCount = 2;

        onNode(listener, cast, argStackDepth + 2, prevBranch);
        argStackDepth++;
        return argStackDepth;
    }

    private OperatorExpression processUnaryMinus(int prevBranch, char thisChar, OperatorExpression op) {
        // BRANCH_BETWEEN_START will be processed as default branch, so prevBranch must be BRANCH_OPERATOR in this case
        assert prevBranch != BRANCH_BETWEEN_START;
        switch (prevBranch) {
            case BRANCH_OPERATOR, BRANCH_LEFT_PARENTHESIS, BRANCH_LEFT_BRACKET, BRANCH_COMMA, BRANCH_NONE,
                 BRANCH_CASE_CONTROL, BRANCH_BETWEEN_END -> {
                // we have unary minus or unary complement, update op completely because it will change precedence
                if (thisChar == '-') {
                    op = activeRegistry.unaryMinus;
                }
                if (thisChar == '~') {
                    op = activeRegistry.unaryComplement;
                }
            }
            default -> {
            }
        }
        return op;
    }

    private void validateArrayOutliers(int lastPos) throws SqlException {
        ExpressionNode en = opStack.peek();
        if (en == null || !Chars.equals(en.token, ':')) {
            throw SqlException.$(lastPos, "syntax error");
        }
    }

    private int validateArrayQualifierStart(GenericLexer lexer, int lastPos, CharSequence tok) throws SqlException {
        int thisBranch;
        ExpressionNode en = opStack.peek();
        assert en != null && en.type == ExpressionNode.LITERAL;
        GenericLexer.FloatingSequence token = (GenericLexer.FloatingSequence) en.token;
        assert token.charAt(token.length() - 1) == '[';
        if (lastPos - (en.position + en.token.length()) == 0) {
            token.setHi(lastPos + 1);
            thisBranch = BRANCH_ARRAY_TYPE_QUALIFIER_END;
        } else {
            // we have spaces
            throw SqlException.position(lastPos)
                    .put("expected '").put(en.token)
                    .put("]' but found '").put(lexer.getContent(), en.position, lastPos + tok.length())
                    .put('\'');
        }
        return thisBranch;
    }

    private boolean withinArrayConstructor() {
        for (int i = 0, n = scopeStack.size(); i < n; i++) {
            if (scopeStack.peek(i) == Scope.ARRAY) {
                return true;
            }
        }
        return false;
    }

    void parseExpr(
            GenericLexer lexer,
            ExpressionParserListener listener,
            SqlParserCallback sqlParserCallback,
            @Nullable LowerCaseCharSequenceObjHashMap<ExpressionNode> decls
    ) throws SqlException {
        int savedScopeStackBottom = 0;
        try {
            int shadowParseMismatchFirstPosition = -1;
            int paramCount = 0;
            int betweenCount = 0;
            int betweenAndCount = 0;
            int caseCount = 0;
            int argStackDepth = 0;
            int betweenStartCaseCount = 0;
            savedScopeStackBottom = scopeStack.getBottom();
            scopeStack.setBottom(scopeStack.sizeRaw());
            boolean parsedDeclaration = false;

            ExpressionNode node;
            CharSequence tok;
            char thisChar;
            int prevBranch = BRANCH_NONE;
            int thisBranch = BRANCH_NONE;
            boolean isCastingNull = false;
            OUT:
            while ((tok = SqlUtil.fetchNext(lexer)) != null) {
                thisChar = tok.charAt(0);
                prevBranch = thisBranch;
                boolean processDefaultBranch = false;
                final int lastPos = lexer.lastTokenPosition();
                switch (thisChar) {
                    case '-':
                    case '+':
                        // floating-point literals in scientific notation (e.g. 1e-10, 1e+10) separated in several
                        // tokens by lexer ('1e', '-', '10') - so we need to glue them together
                        processDefaultBranch = parsePlus(lexer, prevBranch, lastPos);
                        break;
                    case '.':
                        // Check what is on stack. If we have 'a .b' we have to stop processing
                        if (thisBranch == BRANCH_LITERAL || thisBranch == BRANCH_CONSTANT) {
                            char c = lexer.getContent().charAt(lastPos - 1);
                            if (GenericLexer.WHITESPACE_CH.contains(c)) {
                                lexer.unparseLast();
                                break OUT;
                            }

                            if (Chars.isQuote(c)) {
                                // todo: check a.table reference (table is the keyword)
                                ExpressionNode en = opStack.pop();
                                // table prefix cannot be unquoted keywords
                                CharacterStoreEntry cse = characterStore.newEntry();
                                cse.put(GenericLexer.unquoteIfNoDots(en.token)).put('.');
                                opStack.push(expressionNodePool.next().of(ExpressionNode.LITERAL, cse.toImmutable(), Integer.MIN_VALUE, en.position));
                            } else {
                                // attach dot to existing literal or constant
                                ExpressionNode en = opStack.peek();
                                ((GenericLexer.FloatingSequence) en.token).setHi(lastPos + 1);
                            }
                        }
                        if (prevBranch == BRANCH_DOT || prevBranch == BRANCH_DOT_DEREFERENCE) {
                            throw SqlException.$(lastPos, "too many dots");
                        }
                        if (thisBranch == BRANCH_CASE_CONTROL) {
                            throw SqlException.$(lastPos, "unexpected dot");
                        }
                        if (prevBranch == BRANCH_RIGHT_PARENTHESIS) {
                            thisBranch = BRANCH_DOT_DEREFERENCE;
                        } else {
                            thisBranch = BRANCH_DOT;
                        }
                        break;
                    case ',': {
                        if (prevBranch == BRANCH_COMMA || prevBranch == BRANCH_LEFT_PARENTHESIS || prevBranch == BRANCH_LEFT_BRACKET) {
                            throw missingArgs(lastPos);
                        }
                        thisBranch = BRANCH_COMMA;

                        Scope scope0 = scopeStack.peek();
                        if (scope0 != Scope.PAREN && scope0 != Scope.BRACKET && scope0 != Scope.ARRAY) {
                            // comma outside of parens/brackets
                            lexer.unparseLast();
                            break OUT;
                        }

                        argStackDepth = processCommaAndPopOpStack(listener, lastPos, argStackDepth, prevBranch);
                        paramCount++;
                        break;
                    }
                    case ':': {
                        processDefaultBranch = true;
                        tok = processColon(lexer, tok, prevBranch, lastPos);
                        break;
                    }
                    case '[': {
                        ExpressionNode en = opStack.peek();
                        // en could be null if it follows parentheses
                        // non-literal en is not a valid type qualifier, and it would not
                        // successfully cast to FloatingSequence either
                        if (en != null && en.type == ExpressionNode.LITERAL && isTypeQualifier() || scopeStack.peek(1) == Scope.CAST_AS) {
                            thisBranch = validateWhitespace(lexer, lastPos, en);
                            continue;
                        }
                        // restrict what array can dereference
                        switch (prevBranch) {
                            case BRANCH_OPERATOR:
                                processDereference(lexer, en, lastPos);
                                // fall thru
                            case BRANCH_LEFT_PARENTHESIS:
                            case BRANCH_BETWEEN_END:
                            case BRANCH_DOT:
                            case BRANCH_CAST_AS:
                            case BRANCH_CONSTANT:
                            case BRANCH_CASE_START:
                            case BRANCH_CASE_CONTROL:
                            case BRANCH_GEOHASH:
                            case BRANCH_DECIMAL:
                            case BRANCH_LAMBDA:
                                throw SqlException.position(lastPos).put("'[' is unexpected here");
                            default:
                                break;
                        }
                        thisBranch = BRANCH_LEFT_BRACKET;
                        boolean isArrayConstructor = withinArrayConstructor() && !isCompletedOperand(prevBranch);

                        argStackDepth = popArrayOpStack(listener, argStackDepth, prevBranch);

                        // entering bracketed context, push stuff onto the stacks
                        paramCountStack.push(paramCount);
                        paramCount = 0;
                        argStackDepthStack.push(argStackDepth);
                        argStackDepth = 0;
                        scopeStack.push(Scope.BRACKET);

                        // precedence must be max value to make sure control node isn't
                        // consumed as parameter to a greedy function
                        opStack.push(expressionNodePool.next().of(ExpressionNode.CONTROL,
                                isArrayConstructor ? "[[" : "[", Integer.MAX_VALUE, lastPos));
                        break;
                    }
                    case ']': {
                        switch (prevBranch) {
                            case BRANCH_ARRAY_TYPE_QUALIFIER_START: {
                                // we are confident asserting this because the code that set the
                                // prevBranch value has to ensure this is the case.
                                thisBranch = validateArrayQualifierStart(lexer, lastPos, tok);
                                break;
                            }
                            case BRANCH_COMMA:
                                throw missingArgs(lastPos);
                            case BRANCH_LEFT_PARENTHESIS:
                                throw SqlException.$(lastPos, "syntax error");
                            case BRANCH_OPERATOR: {
                                // this would be a syntax error in regular cases, such as
                                // "1 + ]". However, there is an edge case in array slicing:
                                // [1:] - this is a valid syntax. The only known one. The best one.
                                validateArrayOutliers(lastPos);
                            }
                            // fall thru
                            default: {
                                Scope scope = scopeStack.peek();
                                if (scope != Scope.BRACKET && scope != Scope.ARRAY) {
                                    lexer.unparseLast();
                                    break OUT;
                                }
                                scopeStack.pop();
                                // paramCount tracks the number of preceding commas within the current brackets.
                                // So, if the brackets are empty, arg count is zero, otherwise it's paramCount + 1
                                int bracketArgCount = prevBranch == BRANCH_LEFT_BRACKET ? 0 : paramCount + 1;
                                thisBranch = BRANCH_RIGHT_BRACKET;

                                // Until the token at the top of the stack is a left bracket,
                                // pop operators off the stack onto the output queue.
                                // Pop the left bracket from the stack, but don't push it to the output queue.
                                // If the token at the top of the stack is a literal (indicating array access),
                                // push it to the output queue.
                                // If the stack runs out without finding a left bracket, then there are mismatched brackets.
                                while ((node = opStack.pop()) != null && (node.type != ExpressionNode.CONTROL || node.token.charAt(0) != '[')) {
                                    argStackDepth = onNode(listener, node, argStackDepth, prevBranch);
                                }
                                assert node != null : "opStack is empty at ']'";
                                if (Chars.equals(node.token, '[')) {
                                    if (bracketArgCount == 0) {
                                        throw SqlException.$(lastPos, "empty brackets");
                                    }
                                    node = expressionNodePool.next().of(
                                            ExpressionNode.ARRAY_ACCESS,
                                            "[]",
                                            2,
                                            node.position
                                    );
                                    // For array access, the 1st arg is the array, 2nd arg is the first index, etc.
                                    // So, we must add one to the number of args within the brackets.
                                    node.paramCount = bracketArgCount + 1;
                                    opStack.push(node);
                                } else {
                                    assert Chars.equals(node.token, "[[") : "token is neither '[' nor '[['";
                                    node = expressionNodePool.next().of(
                                            ExpressionNode.ARRAY_CONSTRUCTOR,
                                            "ARRAY",
                                            2,
                                            node.position
                                    );
                                    node.paramCount = bracketArgCount;
                                    argStackDepth = onNode(listener, node, argStackDepth, prevBranch);
                                }
                                if (argStackDepthStack.notEmpty()) {
                                    argStackDepth += argStackDepthStack.pop();
                                }
                                if (paramCountStack.notEmpty()) {
                                    paramCount = paramCountStack.pop();
                                }
                            }
                            break;
                        }
                        break;
                    }

                    case '(':
                        // check that we are handling a declare variable, and we have finished parsing it
                        if (parsedDeclaration && prevBranch != BRANCH_LEFT_PARENTHESIS && prevBranch != BRANCH_LITERAL
                                && !(prevBranch == BRANCH_OPERATOR && Chars.equals(opStack.peek().token, ":="))
                        ) {
                            lexer.unparseLast();
                            break OUT;
                        }

                        if (prevBranch == BRANCH_RIGHT_PARENTHESIS) {
                            throw SqlException.$(lastPos, "not a function call");
                        }
                        if (prevBranch == BRANCH_CONSTANT) {
                            throw SqlException.$(lastPos, "dangling expression");
                        }

                        thisBranch = BRANCH_LEFT_PARENTHESIS;
                        // entering parenthesised context, push stuff onto the stacks
                        paramCountStack.push(paramCount);
                        paramCount = 0;
                        argStackDepthStack.push(argStackDepth);
                        argStackDepth = 0;
                        scopeStack.push(Scope.PAREN);

                        // precedence must be max value to make sure control node isn't
                        // consumed as parameter to a greedy function
                        opStack.push(expressionNodePool.next().of(ExpressionNode.CONTROL, "(", Integer.MAX_VALUE, lastPos));

                        break;

                    case ')':
                        switch (prevBranch) {
                            case BRANCH_COMMA:
                                throw missingArgs(lastPos);
                            case BRANCH_ARRAY_TYPE_QUALIFIER_START:
                                throw SqlException.$(lastPos, "']' expected");
                            default:
                                if (scopeStack.peek() != Scope.PAREN) {
                                    lexer.unparseLast();
                                    break OUT;
                                }
                                scopeStack.pop();

                                thisBranch = BRANCH_RIGHT_PARENTHESIS;
                                int localParamCount = (prevBranch == BRANCH_LEFT_PARENTHESIS ? 0 : paramCount + 1);
                                final boolean thisWasCast;

                                if (scopeStack.peek() == Scope.CAST_AS) {
                                    scopeStack.pop();
                                    thisWasCast = true;
                                } else if (scopeStack.peek() == Scope.CAST) {
                                    throw SqlException.$(lastPos, "'as' missing");
                                } else {
                                    thisWasCast = false;
                                }

                                // Until the token at the top of the stack is a left paren,
                                // pop operators off the stack onto the output queue.
                                // Pop the left paren from the stack, but don't push it to the output queue.
                                // If the token at the top of the stack is a literal (indicating function call),
                                // push it to the output queue.
                                // If the stack runs out without finding a left paren, then there are mismatched parens.
                                while ((node = opStack.pop()) != null && (node.type != ExpressionNode.CONTROL || node.token.charAt(0) != '(')) {
                                    // special case - (*) expression
                                    if (Chars.equals(node.token, '*') && argStackDepth == 0 && isCount()) {
                                        argStackDepth = onNode(listener, node, 2, prevBranch);
                                        continue;
                                    }
                                    if (thisWasCast && prevBranch != BRANCH_GEOHASH && prevBranch != BRANCH_DECIMAL) {
                                        // validate type
                                        final short castAsTag = ColumnType.tagOf(node.token);
                                        if ((cannotCastTo(castAsTag, isCastingNull)) ||
                                                (castAsTag == ColumnType.GEOHASH && node.type == ExpressionNode.LITERAL)
                                        ) {
                                            throw SqlException.$(node.position, "unsupported cast");
                                        }
                                        node.type = ExpressionNode.CONSTANT;
                                    }
                                    argStackDepth = onNode(listener, node, argStackDepth, prevBranch);
                                }

                                if (argStackDepthStack.notEmpty()) {
                                    argStackDepth += argStackDepthStack.pop();
                                }
                                if (paramCountStack.notEmpty()) {
                                    paramCount = paramCountStack.pop();
                                }

                                node = opStack.peek();
                                if (node == null) {
                                    break;
                                }
                                if (localParamCount > 1 && node.token.charAt(0) == '(') {
                                    // sensible error for count(distinct(col1, col...)) case
                                    // this is supported by postgresql -> we want to give a clear error message QuestDB does not support it
                                    if (opStack.size() > 1) {
                                        ExpressionNode en = opStack.peek();
                                        if (en.type == ExpressionNode.CONTROL && Chars.equals(en.token, '(')) {
                                            en = opStack.peek(1);
                                            if (en.type == ExpressionNode.LITERAL && Chars.equals(en.token, "count_distinct")) {
                                                throw SqlException.$(lastPos, "count distinct aggregation supports a single column only");
                                            }
                                        }
                                    }
                                    throw SqlException.$(lastPos, "no function or operator?");
                                }

                                // Check for window function OVER clause (including IGNORE/RESPECT NULLS)
                                if (node.type == ExpressionNode.LITERAL) {
                                    argStackDepth = parseOverExpr(lexer, listener, sqlParserCallback, decls, node, localParamCount, argStackDepth, prevBranch);
                                }

                                argStackDepth = popLiteralAndFunctionOpStack(listener, node, localParamCount, argStackDepth, prevBranch);
                                break;
                        }
                        break;
                    case 'd':
                    case 'D':
                        // Merges a decimal type constant of the form decimal(p[, s]) to a single node decimal_p[_s]
                        if (SqlKeywords.isDecimalKeyword(tok)) {
                            CharSequence decimalTok = GenericLexer.immutableOf(tok);
                            tok = SqlUtil.fetchNext(lexer);
                            if (tok == null || tok.charAt(0) != '(') {
                                lexer.backTo(lastPos + SqlKeywords.DECIMAL_KEYWORD_LENGTH, decimalTok);
                                tok = decimalTok;
                                processDefaultBranch = true;
                                break;
                            }
                            tok = SqlUtil.fetchNext(lexer);
                            if (tok == null || tok.charAt(0) == ')') {
                                throw SqlException.$(lexer.lastTokenPosition(), "Invalid decimal type. The precision is missing");
                            }
                            DecimalUtil.parsePrecision(lexer.lastTokenPosition(), tok, 0, tok.length());
                            CharSequence precisionTok = GenericLexer.immutableOf(tok);

                            // The user is not mandated to provide a scale value (defaults to 0)
                            tok = processDecimal(lexer, decimalTok, precisionTok, lastPos);
                            thisBranch = BRANCH_DECIMAL;
                            break;
                        }

                        if (parsedDeclaration && prevBranch != BRANCH_LEFT_PARENTHESIS && SqlKeywords.isDeclareKeyword(tok)) {
                            lexer.unparseLast();
                            break OUT;
                        }

                        if (prevBranch != BRANCH_LITERAL && SqlKeywords.isDeclareKeyword(tok)) {
                            thisBranch = BRANCH_LAMBDA;
                            if (betweenCount > 0) {
                                throw SqlException.$(lastPos, "constant expected");
                            }
                            argStackDepth = processLambdaQuery(lexer, listener, argStackDepth, sqlParserCallback, decls);
                            processDefaultBranch = false;
                            break;
                        }

                        if (prevBranch == BRANCH_LEFT_PARENTHESIS && SqlKeywords.isDistinctKeyword(tok)) {
                            // rewrite count(distinct x) to count_distinct(x)
                            // and string_agg(distinct x) to string_distinct_agg(x)
                            if (opStack.size() > 1) {
                                ExpressionNode en = opStack.peek();
                                // we are in the BRANCH_LEFT_PARENTHESIS, so the previous token must be
                                // a control token '('
                                assert Chars.equals(en.token, '(') && en.type == ExpressionNode.CONTROL;
                                CharSequence tokenStash = GenericLexer.immutableOf(tok);
                                if (processDecimalRewrite(lexer, tokenStash, lastPos)) {
                                    continue;
                                }
                                // unparsing won't set `tok` to the previous token, so we need to do it manually
                                tok = tokenStash;
                            }
                        }
                        processDefaultBranch = true;
                        break;
                    case 'g':
                    case 'G':
                        // this code ensures that "geohash(6c)" type will be converted to single "geohash6c" node (not two nodes)
                        if (SqlKeywords.isGeoHashKeyword(tok)) {
                            CharSequence geohashTok = GenericLexer.immutableOf(tok);
                            tok = SqlUtil.fetchNext(lexer);
                            if (tok == null || tok.charAt(0) != '(') {
                                lexer.backTo(lastPos + SqlKeywords.GEOHASH_KEYWORD_LENGTH, geohashTok);
                                tok = geohashTok;
                                processDefaultBranch = true;
                                break;
                            }
                            tok = createGeoHashNodeStd(lexer, geohashTok, lastPos);
                            thisBranch = BRANCH_GEOHASH;
                        } else {
                            processDefaultBranch = true;
                        }
                        break;

                    case '#':
                        if (isGeoHashCharsConstant(tok)) { // e.g. #sp052w92p1p8
                            thisBranch = BRANCH_CONSTANT;
                            tok = createGeoHashConst(lexer, tok, lastPos);
                            break;
                        }

                        if (isGeoHashBitsConstant(tok)) { // e.g. ##01110001
                            thisBranch = BRANCH_CONSTANT;
                            opStack.push(expressionNodePool.next().of(
                                    ExpressionNode.CONSTANT,
                                    GenericLexer.immutableOf(tok), // geohash bit literals do not allow suffix syntax
                                    Integer.MIN_VALUE,
                                    lastPos
                            ));
                            break;
                        }

                        processDefaultBranch = true;
                        break;
                    case 'c':
                    case 'C':
                        if (SqlKeywords.isCastKeyword(tok)) {
                            CharSequence castTok = GenericLexer.immutableOf(tok);
                            tok = SqlUtil.fetchNext(lexer);
                            if (tok == null || tok.charAt(0) != '(') {
                                lexer.backTo(lastPos + SqlKeywords.CAST_KEYWORD_LENGTH, castTok);
                                tok = castTok;
                                processDefaultBranch = true;
                                break;
                            }

                            lexer.backTo(lastPos + SqlKeywords.CAST_KEYWORD_LENGTH, castTok);
                            tok = castTok;
                            if (prevBranch == BRANCH_DOT_DEREFERENCE || isCompletedOperand(prevBranch)) {
                                throw SqlException.$(lastPos, "'cast' is not allowed here");
                            }
                            scopeStack.push(Scope.CAST);
                            thisBranch = BRANCH_OPERATOR;
                            opStack.push(expressionNodePool.next().of(ExpressionNode.LITERAL, "cast", Integer.MIN_VALUE, lastPos));
                            break;
                        }
                        processDefaultBranch = true;
                        break;
                    case 'a':
                    case 'A':
                        if (SqlKeywords.isAsKeyword(tok)) {
                            if (scopeStack.peek(1) == Scope.CAST) {

                                thisBranch = BRANCH_CAST_AS;

                                // push existing args to the listener
                                int nodeCount = 0;
                                while ((node = opStack.pop()) != null && node.token.charAt(0) != '(') {
                                    nodeCount++;
                                    isCastingNull = nodeCount == 1 && SqlKeywords.isNullKeyword(node.token);
                                    argStackDepth = onNode(listener, node, argStackDepth, prevBranch);
                                }
                                if (node != null) {
                                    // push back '('
                                    opStack.push(node);
                                }
                                paramCount++;
                                scopeStack.update(1, Scope.CAST_AS);
                            } else {
                                processDefaultBranch = true;
                            }
                        } else if (SqlKeywords.isAndKeyword(tok)) {
                            if (caseCount == betweenStartCaseCount && betweenCount > betweenAndCount) {
                                betweenAndCount++;
                                thisBranch = BRANCH_BETWEEN_END;
                                argStackDepth = popAndOpStack(listener, argStackDepth, prevBranch);
                            } else {
                                processDefaultBranch = true;
                            }
                        } else if (SqlKeywords.isAllKeyword(tok)) {
                            ExpressionNode operator = opStack.peek();
                            if (operator == null || operator.type != ExpressionNode.OPERATION) {
                                throw SqlException.$(lastPos, "missing operator");
                            }
                            CharSequence funcName = allFunctions.get(operator.token);
                            if (funcName != null && operator.paramCount == 2) {
                                operator.type = ExpressionNode.FUNCTION;
                                operator.token = funcName;
                            } else {
                                throw SqlException.$(operator.position, "unexpected operator");
                            }
                        } else if (SqlKeywords.isArrayKeyword(tok)) {
                            CharSequence nextTok = SqlUtil.fetchNext(lexer);
                            if (nextTok == null || !Chars.equals(nextTok, "[")) {
                                throw SqlException.$(lexer.lastTokenPosition(), "ARRAY not followed by '['");
                            }
                            thisBranch = BRANCH_LEFT_BRACKET;
                            // entering bracketed context, push stuff onto the stacks
                            paramCountStack.push(paramCount);
                            paramCount = 0;
                            argStackDepthStack.push(argStackDepth);
                            argStackDepth = 0;
                            scopeStack.push(Scope.ARRAY);
                            opStack.push(expressionNodePool.next().of(ExpressionNode.CONTROL,
                                    "[[", Integer.MAX_VALUE, lexer.lastTokenPosition()));
                        } else if (isCompletedOperand(prevBranch) && SqlKeywords.isAtKeyword(tok)) {
                            int pos = lexer.getPosition();
                            // '.' processing expects floating char sequence
                            CharSequence atTok = GenericLexer.immutableOf(tok);
                            tok = SqlUtil.fetchNext(lexer);
                            if (tok != null && SqlKeywords.isTimeKeyword(tok)) {
                                tok = SqlUtil.fetchNext(lexer);
                                if (tok != null && SqlKeywords.isZoneKeyword(tok)) {
                                    // do the zone thing
                                    thisBranch = BRANCH_TIMESTAMP_ZONE;
                                } else {
                                    throw SqlException.$(
                                            tok == null ? lexer.getPosition() : lexer.lastTokenPosition(),
                                            "did you mean 'at time zone <tz>'?"
                                    );
                                }
                            } else {
                                tok = atTok;
                                // non-literal branches use 'tok' to create expressions
                                // however literal branches exit, pushing literal back into lexer (unparse)
                                // so for non-literal branches we have to preserve the very last token we
                                // had to peek at
                                if (caseCount > 0 || nonLiteralBranches.excludes(thisBranch)) {
                                    lexer.unparseLast();
                                } else {
                                    lexer.unparse(tok, lastPos, pos);
                                }
                                processDefaultBranch = true;
                            }
                        } else {
                            processDefaultBranch = true;
                        }
                        break;
                    case 'b':
                    case 'B':
                        if (SqlKeywords.isBetweenKeyword(tok)) {
                            thisBranch = BRANCH_BETWEEN_START;
                            if (betweenCount > betweenAndCount) {
                                // Nested between are not supported
                                throw SqlException.$(lastPos, "between statements cannot be nested");
                            }
                            betweenCount++;
                            betweenStartCaseCount = caseCount;
                        }
                        processDefaultBranch = true;
                        break;
                    case 's':
                    case 'S':
                        if (parsedDeclaration && prevBranch != BRANCH_LEFT_PARENTHESIS && SqlKeywords.isSelectKeyword(tok)) {
                            lexer.unparseLast();
                            break OUT;
                        }

                        if (prevBranch != BRANCH_LITERAL && SqlKeywords.isSelectKeyword(tok)) {
                            thisBranch = BRANCH_LAMBDA;
                            if (betweenCount > 0) {
                                throw SqlException.$(lastPos, "constant expected");
                            }
                            argStackDepth = processLambdaQuery(lexer, listener, argStackDepth, sqlParserCallback, decls);
                        } else {
                            processDefaultBranch = true;
                        }
                        break;
                    case '0':
                    case '1':
                    case '2':
                    case '3':
                    case '4':
                    case '5':
                    case '6':
                    case '7':
                    case '8':
                    case '9':
                    case '\'':
                    case 'E':

                        switch (prevBranch) {
                            case BRANCH_DOT_DEREFERENCE:
                                throw SqlException.$(lastPos, "constant is not allowed here");
                            case BRANCH_ARRAY_TYPE_QUALIFIER_START:
                                throw SqlException.$(lastPos, "']' expected");
                            default:
                                // check if this is E'str'
                                if (thisChar == 'E' && (tok.length() < 3 || tok.charAt(1) != '\'')) {
                                    processDefaultBranch = true;
                                    break;
                                }

                                thisBranch = BRANCH_CONSTANT;
                                if (prevBranch == BRANCH_CONSTANT && lastPos > 0) {
                                    char prevChar = lexer.getContent().charAt(lastPos - 1);
                                    if (prevChar == '-' || prevChar == '+') {
                                        final ExpressionNode en = opStack.peek();
                                        if (en.token instanceof GenericLexer.FloatingSequence) {
                                            ((GenericLexer.FloatingSequence) en.token).setHi(lexer.getTokenHi());
                                            break;
                                        } else {
                                            assert false;
                                        }
                                    }
                                }
                                if (prevBranch == BRANCH_DOT) {
                                    if (createFloatingPointConstant(lexer, lastPos, tok)) {
                                        break;
                                    }
                                }
                                if (prevBranch != BRANCH_DOT && nonLiteralBranches.excludes(prevBranch)) {
                                    if (SqlKeywords.isQuote(tok)) {
                                        throw SqlException.$(lastPos, "unclosed quoted string?");
                                    }

                                    ExpressionNode constNode = SqlUtil.nextConstant(
                                            expressionNodePool,
                                            GenericLexer.immutableOf(tok),
                                            lastPos
                                    );

                                    if (prevBranch == BRANCH_TIMESTAMP_ZONE) {
                                        argStackDepth = onNode(
                                                listener,
                                                constNode,
                                                argStackDepth,
                                                prevBranch);

                                        // replace const node with 'to_timezone' function node
                                        constNode = expressionNodePool.next().of(
                                                ExpressionNode.FUNCTION,
                                                "to_timezone",
                                                Integer.MIN_VALUE,
                                                lastPos
                                        );
                                        constNode.paramCount = 2;
                                        // fall through
                                    }
                                    opStack.push(constNode);
                                    break;
                                } else {
                                    if (opStack.size() > 0 && prevBranch == BRANCH_LITERAL && thisChar == '\'') {
                                        argStackDepth = processPgTimestampCast(listener, isCastingNull, tok, lastPos, prevBranch, argStackDepth);
                                        break;
                                    }

                                    // there is one case for valid dangling expression - when we create an alias for column (`'value' 'x'` equivalent to `'value' as 'x'`)
                                    // this helper works for simple cases leaving unparsed last token and gives a chance for the caller to analyze aliasing
                                    // although for complex cases it will not work (`'a' || 'b' 'x'` will not be parsed as `'a' || 'b' as 'x'` without explicit parens)
                                    if (opStack.size() > 1) {
                                        throw SqlException.$(lastPos, "dangling expression");
                                    }
                                    lexer.unparseLast();
                                    break OUT;
                                }
                        }
                        break;
                    case 'N':
                    case 'n':
                    case 't':
                    case 'T':
                    case 'f':
                    case 'F':
                        if (prevBranch == BRANCH_ARRAY_TYPE_QUALIFIER_START) {
                            // must not go down default branch with unfinished array type qualifier
                            throw SqlException.$(lastPos, "']' expected");
                        }

                        if (SqlKeywords.isNanKeyword(tok)
                                || SqlKeywords.isNullKeyword(tok)
                                || SqlKeywords.isTrueKeyword(tok)
                                || SqlKeywords.isFalseKeyword(tok)
                        ) {
                            if (prevBranch != BRANCH_DOT_DEREFERENCE) {
                                if (nonLiteralBranches.excludes(prevBranch)) {
                                    thisBranch = BRANCH_CONSTANT;
                                    // If the token is a number, then add it to the output queue.
                                    opStack.push(SqlUtil.nextConstant(expressionNodePool, GenericLexer.immutableOf(tok), lastPos));
                                } else {
                                    throw SqlException.$(lastPos, "dangling expression");
                                }
                            } else {
                                throw SqlException.$(lastPos, "constant is not allowed here");
                            }
                            break;
                        }
                        processDefaultBranch = true;
                        break;
                    case 'i':
                    case 'I':
                        if (SqlKeywords.isIsKeyword(tok)) {
                            // replace:
                            // <literal or constant> IS NULL     -> <literal or constant> = NULL
                            // <literal or constant> IS NOT NULL -> <literal or constant> != NULL
                            if (prevBranch == BRANCH_LITERAL || prevBranch == BRANCH_CONSTANT || prevBranch == BRANCH_RIGHT_PARENTHESIS) {
                                tok = validateIsKeyword(lexer, tok, lastPos);
                            } else {
                                throw SqlException.$(lastPos, "IS [NOT] not allowed here");
                            }
                        }
                        processDefaultBranch = true;
                        break;
                    case '*':
                        // special case for tab.*
                        if (prevBranch == BRANCH_DOT) {
                            thisBranch = BRANCH_LITERAL;
                            processAliasedWildcard(lexer, lastPos);
                            break;
                        }
                    default:
                        if (prevBranch == BRANCH_ARRAY_TYPE_QUALIFIER_START) {
                            // must not go down default branch with unfinished array type qualifier
                            throw SqlException.$(lastPos, "']' expected");
                        }
                        processDefaultBranch = true;
                        break;
                }

                if (processDefaultBranch) {
                    OperatorExpression op;
                    if ((op = activeRegistry.map.get(tok)) != null && !(stopOnTopINOperator && op.operator == In && scopeStack.size() == 0)) {

                        thisBranch = BRANCH_OPERATOR;

                        if (Chars.equals(tok, ":=")) {
                            parsedDeclaration = true;
                        }

                        if (thisChar == '-' || thisChar == '~') {
                            op = processUnaryMinus(prevBranch, thisChar, op);
                        }

                        int operatorType = op.type;

                        boolean unaryOperator = op.type == UNARY;
                        // negation of set operators (NOT IN / NOT BETWEEN) changes precedence of NOT part
                        if (SqlKeywords.isNotKeyword(tok)) {
                            final int lastTokenPosition = lexer.lastTokenPosition();
                            final CharSequence lastToken = GenericLexer.immutableOf(tok);
                            final CharSequence nextToken = SqlUtil.fetchNext(lexer);
                            OperatorExpression nextOp;
                            if (nextToken != null && (nextOp = activeRegistry.map.get(nextToken)) != null && nextOp.type == OperatorExpression.SET) {
                                op = activeRegistry.unarySetNegation;
                                unaryOperator = false; // NOT is part of multi-ary set operation negation
                            }
                            lexer.backTo(lastTokenPosition + lastToken.length(), lastToken);
                        }

                        ExpressionNode other;
                        // If the token is an operator, o1, then:
                        // while there is an operator token, o2, at the top of the operator stack, and either
                        // o1 is left-associative and its precedence is less than or equal to that of o2, or
                        // o1 is right associative, and has precedence less than that of o2,
                        //        then pop o2 off the operator stack, onto the output queue;
                        // push o1 onto the operator stack.
                        while ((other = opStack.peek()) != null) {
                            boolean greaterPrecedence = op.greaterPrecedence(other.precedence);
                            // the unary prefix NOT operator can't pop binary operator from the left,
                            // although it has very high precedence (that's to allow usage of subexpressions
                            // like `y = FALSE AND NOT x = TRUE`)
                            if (unaryOperator && other.paramCount > 0) {
                                break;
                            }

                            /*
                                Validate consistency of query parsing with active & shadow precedence tables
                                On the high level, process looks like following:
                                1. Find the "op" operator in the shadow registry: shadowOp
                                2. Find the "other" operator in the shadow registry: shadowOther
                                3. Compare precedence of shadowOp & shadowOther (taking into account associativity rules)
                                4. If comparison differs from greaterPrecedence result from above, there is a mismatch in parsing

                                The procedure is not that straightforward because we don't have exact operator type for
                                the other op. This is only a problem for ambiguous operators like minus('-'),
                                complement('~') and set negation(e.g. 'not within'). In order to partially resolve this
                                issue, we "guess" operator by its token and precedence - this helps to distinguish
                                between minus and complement, but not for set negation.
                             */
                            if (shadowRegistry != null) {
                                OperatorExpression activeOtherGuess = activeRegistry.tryGuessOperator(other.token, other.precedence);
                                OperatorExpression shadowOp = shadowRegistry.tryGetOperator(op.operator);
                                if (shadowOp != null && activeOtherGuess != null) {
                                    OperatorExpression shadowOther = shadowRegistry.tryGetOperator(activeOtherGuess.operator);
                                    if (shadowOther != null && greaterPrecedence != shadowOp.greaterPrecedence(shadowOther.precedence)) {
                                        shadowParseMismatchFirstPosition = lexer.lastTokenPosition();
                                    }
                                }
                            }

                            if (greaterPrecedence) {
                                argStackDepth = onNode(listener, other, argStackDepth, prevBranch);
                                opStack.pop();
                            } else {
                                break;
                            }
                        }
                        createOperation(op, lastPos, operatorType);
                    } else if (caseCount > 0 || nonLiteralBranches.excludes(thisBranch)) {
                        // here we handle literals, in case of "case" statement some of these literals
                        // are going to flush operation stack
                        if (Chars.toLowerCaseAscii(thisChar) == 'c' && SqlKeywords.isCaseKeyword(tok)) {
                            if (prevBranch == BRANCH_DOT_DEREFERENCE) {
                                throw SqlException.$(lastPos, "'case' is not allowed here");
                            }
                            caseCount++;

                            // entering CASE context, push stuff onto the stacks
                            paramCountStack.push(paramCount);
                            paramCount = 0;
                            argStackDepthStack.push(argStackDepth);
                            argStackDepth = 0;
                            scopeStack.push(Scope.CASE);

                            opStack.push(expressionNodePool.next().of(ExpressionNode.FUNCTION, "case", Integer.MAX_VALUE, lastPos));
                            thisBranch = BRANCH_CASE_START;
                            continue;
                        }

                        thisBranch = BRANCH_LITERAL;

                        if (caseCount > 0) {
                            switch (Chars.toLowerCaseAscii(thisChar)) {
                                case 'e':
                                    if (SqlKeywords.isEndKeyword(tok)) {
                                        if (prevBranch == BRANCH_CASE_CONTROL) {
                                            throw missingArgs(lastPos);
                                        }
                                        if (paramCount == 0) {
                                            throw SqlException.$(lastPos, "'when' expected");
                                        }
                                        if (paramCount <= 2) {
                                            throw SqlException.$(lastPos, "'then' expected");
                                        }

                                        // If the token is a right parenthesis:
                                        // Until the token at the top of the stack is a left parenthesis, pop operators
                                        // off the stack onto the output queue.
                                        // Pop the left parenthesis from the stack, but not onto the output queue.
                                        //   - If the token at the top of the stack is a function token, pop it onto the
                                        //     output queue.
                                        //   - If the stack runs out without finding a left parenthesis, then there are
                                        //     mismatched parentheses.
                                        while ((node = opStack.pop()) != null && !SqlKeywords.isCaseKeyword(node.token)) {
                                            argStackDepth = onNode(listener, node, argStackDepth, prevBranch);
                                        }

                                        // 'when/else' have been clearing argStackDepth to ensure expressions between
                                        // 'when' and 'when' do not pick up arguments outside of scope now we need to
                                        // restore stack depth before 'case' entry
                                        if (argStackDepthStack.notEmpty()) {
                                            argStackDepth += argStackDepthStack.pop();
                                        }

                                        // exiting CASE context, pop stuff off the stacks
                                        Scope scope = scopeStack.pop();
                                        assert scope == Scope.CASE : "Should have popped CASE, but got " + scope;
                                        node.paramCount = paramCount;
                                        // add the number of 'case' arguments to the original stack depth
                                        argStackDepth = onNode(listener, node, argStackDepth + paramCount, prevBranch);
                                        if (paramCountStack.notEmpty()) {
                                            paramCount = paramCountStack.pop();
                                        }

                                        caseCount--;
                                        continue;
                                    }
                                    // fall through
                                case 'w':
                                case 't':
                                    int keywordIndex = caseKeywords.get(tok);
                                    if (keywordIndex > -1) {

                                        if (prevBranch == BRANCH_CASE_CONTROL) {
                                            throw missingArgs(lastPos);
                                        }
                                        if (keywordIndex == IDX_ELSE && paramCount == 0) {
                                            throw SqlException.$(lastPos, "'when' expected");
                                        }

                                        // we need to track argument consumption so that operators and functions
                                        // do no steal parameters outside of local 'case' scope
                                        int argCount = 0;
                                        while ((node = opStack.pop()) != null && !SqlKeywords.isCaseKeyword(node.token)) {
                                            argStackDepth = onNode(listener, node, argStackDepth, prevBranch);
                                            argCount++;
                                        }

                                        if (paramCount == 0) {
                                            if (argCount == 0) {
                                                // this is 'case when', we will indicate that this is regular 'case'
                                                // to the rewrite logic
                                                onNode(listener, expressionNodePool.next().of(ExpressionNode.LITERAL,
                                                        null, Integer.MIN_VALUE, -1), argStackDepth, prevBranch);
                                            }
                                            paramCount++;
                                        }

                                        switch (keywordIndex) {
                                            case IDX_WHEN:
                                            case IDX_ELSE:
                                                if ((paramCount % 2) == 0) {
                                                    throw SqlException.$(lastPos, "'then' expected");
                                                }
                                                break;
                                            default: // then
                                                if ((paramCount % 2) != 0) {
                                                    throw SqlException.$(lastPos, "'when' expected");
                                                }
                                                break;
                                        }

                                        if (node != null) {
                                            opStack.push(node);
                                        }

                                        argStackDepth = 0;
                                        paramCount++;
                                        thisBranch = BRANCH_CASE_CONTROL;
                                        continue;
                                    }
                                    break;
                                default:
                                    break;
                            }
                        }

                        if (prevBranch == BRANCH_DOT) {
                            // this deals with 'table.column' situations
                            ExpressionNode en = opStack.peek();
                            if (en == null) {
                                throw SqlException.$(lastPos, "qualifier expected");
                            }
                            // two possibilities here:
                            // 1. 'a.b'
                            // 2. 'a. b'

                            if (GenericLexer.WHITESPACE_CH.contains(lexer.getContent().charAt(lastPos - 1))) {
                                // 'a. b'
                                lexer.unparseLast();
                                break;
                            }

                            SqlKeywords.assertNameIsQuotedOrNotAKeyword(tok, lastPos);
                            if (Chars.isQuoted(tok) || en.token instanceof CharacterStore.NameAssemblerCharSequence) {
                                // replacing node, must remove old one from stack
                                opStack.pop();
                                // this was more analogous to 'a."b"'
                                CharacterStoreEntry cse = characterStore.newEntry();
                                SqlKeywords.assertNameIsQuotedOrNotAKeyword(tok, en.position);
                                cse.put(en.token).put(GenericLexer.unquoteIfNoDots(tok));
                                opStack.push(expressionNodePool.next().of(
                                        ExpressionNode.LITERAL, cse.toImmutable(), Integer.MIN_VALUE, en.position));
                            } else {
                                final GenericLexer.FloatingSequence fsA = (GenericLexer.FloatingSequence) en.token;
                                // vanilla 'a.b', just concat tokens efficiently
                                fsA.setHi(lexer.getTokenHi());
                            }
                        } else if (prevBranch == BRANCH_DOT_DEREFERENCE) {
                            argStackDepth++;
                            final ExpressionNode dotDereference = expressionNodePool.next().of(
                                    ExpressionNode.OPERATION, activeRegistry.dot.operator.token,
                                    activeRegistry.dot.precedence, lastPos);
                            dotDereference.paramCount = 2;
                            opStack.push(dotDereference);
                            opStack.push(expressionNodePool.next().of(
                                    ExpressionNode.MEMBER_ACCESS, GenericLexer.immutableOf(tok), Integer.MIN_VALUE, lastPos));
                        } else {
                            // this also could be syntax error such as extract(from x), when it should have been
                            // extract(something from x)
                            if (SqlKeywords.isFromKeyword(tok) && opStack.size() > 1 && SqlKeywords.isExtractKeyword(opStack.peek(1).token)) {
                                if (paramCount == 0) {
                                    throw SqlException.$(lastPos, "Huh? What would you like to extract?");
                                }
                                throw SqlException.$(lastPos, "Unnecessary `from`. Typo?");
                            } else if (SqlKeywords.isOverKeyword(tok)) {
                                if (Chars.equals(SqlUtil.fetchNext(lexer), '(')) {
                                    throw SqlException.$(lastPos, "Nested window functions are not currently supported.");
                                }
                                lexer.unparseLast();
                            }

                            // this is a function or array name, push it onto the stack
                            opStack.push(expressionNodePool.next().of(ExpressionNode.LITERAL,
                                    GenericLexer.immutableOf(tok), Integer.MIN_VALUE, lastPos));
                        }
                    } else {
                        ExpressionNode last = this.opStack.peek();
                        // Handle `timestamp with time zone`
                        if (last != null) {
                            if ((SqlKeywords.isTimestampKeyword(last.token) || SqlKeywords.isTimestampNsKeyword(last.token)) && SqlKeywords.isWithKeyword(tok)) {
                                CharSequence withTok = GenericLexer.immutableOf(tok);
                                int withTokPosition = lexer.getPosition();
                                tok = SqlUtil.fetchNext(lexer);
                                if (tok != null && SqlKeywords.isTimeKeyword(tok)) {
                                    tok = SqlUtil.fetchNext(lexer);
                                    if (tok != null && SqlKeywords.isZoneKeyword(tok)) {
                                        CharSequence zoneTok = GenericLexer.immutableOf(tok);
                                        int zoneTokPosition = lexer.getTokenHi();
                                        tok = SqlUtil.fetchNext(lexer);
                                        // Next token is string literal, or we are in 'as' part of cast function
                                        if (tok != null && (scopeStack.peek(1) == Scope.CAST_AS || tok.charAt(0) == '\'')) {
                                            lexer.backTo(zoneTokPosition, zoneTok);
                                            continue;
                                        }
                                        if (opStack.size() > 1) {
                                            ExpressionNode en = opStack.peek(1);
                                            if (SqlKeywords.isColonColon(en.token)) {
                                                // '1970-01-01 00:08:20.023+00'::timestamp with time zone
                                                lexer.backTo(zoneTokPosition, zoneTok);
                                                continue;
                                            }
                                        }
                                        throw SqlException.$(
                                                zoneTokPosition,
                                                "String literal expected after 'timestamp with time zone'"
                                        );
                                    }
                                }
                                lexer.backTo(withTokPosition, withTok);
                            } else if (SqlKeywords.isFromKeyword(tok)) {
                                // check if this is "extract(something from ...)"
                                // we can do this by analyzing opStack
                                if (opStack.size() > 2) {
                                    boolean extractError = true;
                                    ExpressionNode member = opStack.peek(0);
                                    if (member.type == ExpressionNode.LITERAL || (member.type == ExpressionNode.CONSTANT)) {
                                        if (Chars.equals(opStack.peek(1).token, '(')) {
                                            if (SqlKeywords.isExtractKeyword(opStack.peek(2).token)) {
                                                // validate part
                                                // todo: validate that token is quoted when it is a keyword
                                                if (SqlKeywords.validateExtractPart(GenericLexer.unquote(member.token))) {
                                                    // in this case "from" keyword acts as ',' in function call
                                                    member.type = ExpressionNode.MEMBER_ACCESS;
                                                    argStackDepth = onNode(
                                                            listener,
                                                            member,
                                                            argStackDepth,
                                                            prevBranch);
                                                    opStack.pop();
                                                    paramCount++;
                                                    thisBranch = BRANCH_COMMA;
                                                    continue;
                                                } else {
                                                    throw SqlException.$(member.position,
                                                            "unsupported timestamp part: ").put(member.token);
                                                }
                                            } else {
                                                extractError = false;
                                            }
                                        }
                                    }

                                    // report error on extract
                                    if (extractError && isExtractFunctionOnStack()) {
                                        throw SqlException.$(member.position, "we expect timestamp part here");
                                    }

                                }
                            } else if (SqlKeywords.isOrderKeyword(tok) && opStack.size() > 2) {
                                // PostgreSQL supports an optional ORDER BY for string_distinct_agg(), e.g.:
                                // string_distinct_agg('a', ',' ORDER BY 'b'). We do not support it and this branch
                                // exists to give a meaningful error message in this case
                                ExpressionNode en = opStack.peek();
                                if (en.type == ExpressionNode.CONSTANT) {
                                    en = opStack.peek(1);
                                    if (en.type == ExpressionNode.CONTROL && Chars.equals(en.token, '(')) {
                                        en = opStack.peek(2);
                                        if (en.type == ExpressionNode.LITERAL && Chars.equalsIgnoreCase(en.token, "string_distinct_agg")) {
                                            throw SqlException.$(lastPos, "ORDER BY not supported for string_distinct_agg");
                                        }
                                    }
                                }
                            } else if (SqlKeywords.isDoubleKeyword(last.token) && SqlKeywords.isPrecisionKeyword(tok)) {
                                // ignore 'precision' keyword after 'double'
                                continue;
                            } else if (SqlKeywords.isOverKeyword(tok)) {
                                if (Chars.equals(SqlUtil.fetchNext(lexer), '(')) {
                                    throw SqlException.$(lastPos, "Nested window functions' context are not currently supported.");
                                }
                                lexer.unparseLast();
                            }
                        }
                        // literal can be at start of input, after a bracket or part of an operator
                        // all other cases are illegal and will be considered end-of-input
                        if (scopeStack.notEmpty()) {
                            throw SqlException.$(lastPos, "dangling literal");
                        }
                        lexer.unparseLast();
                        break;
                    }
                }
            }

            if (thisBranch == BRANCH_TIMESTAMP_ZONE) {
                throw SqlException.$(lexer.lastTokenPosition(), "did you mean 'at time zone <tz>'?");
            }

            while ((node = opStack.pop()) != null) {
                if (!node.token.isEmpty() && node.token.charAt(0) == '(') {
                    throw SqlException.$(node.position, "unbalanced (");
                }

                // our array dereference is dangling
                if (node.type == ExpressionNode.CONTROL && node.token.charAt(0) == '[') {
                    throw SqlException.$(node.position, "unbalanced [");
                }

                if (SqlKeywords.isCaseKeyword(node.token)) {
                    throw SqlException.$(node.position, "unbalanced 'case'");
                }

                if (node.type == ExpressionNode.CONTROL) {
                    // break on any other control node to allow parser to be re-enterable
                    // put control node back on stack because we don't own it
                    opStack.push(node);
                    break;
                }

                argStackDepth = onNode(listener, node, argStackDepth, prevBranch, caseCount == 0);
            }

            if (shadowParseMismatchFirstPosition != -1) {
                LOG.advisory()
                        .$("operator precedence compat mode: detected expression parsing behaviour change for query=[")
                        .$(lexer.getContent())
                        .$("] at position=")
                        .$(shadowParseMismatchFirstPosition)
                        .$();
            }
        } catch (SqlException e) {
            opStack.clear();
            scopeStack.clear();
            argStackDepthStack.clear();
            paramCountStack.clear();
            throw e;
        } finally {
            scopeStack.setBottom(savedScopeStackBottom);
            argStackDepthStack.popAll();
            paramCountStack.popAll();
        }
    }

    private enum Scope {
        CASE, CAST, CAST_AS, PAREN, BRACKET, ARRAY
    }

    /**
     * Tree builder for window clause expressions.
     * Builds expression trees matching ExpressionTreeBuilder's behavior.
     * Uses lhs/rhs for paramCount < 3, args for paramCount >= 3.
     */
    private static class WindowExprTreeBuilder implements ExpressionParserListener {
        private final ObjStack<ExpressionNode> stack = new ObjStack<>();

        public ExpressionNode getResult() {
            return stack.peek();
        }

        @Override
        public void onNode(ExpressionNode node) {
            // Match ExpressionTreeBuilder's behavior exactly
            switch (node.paramCount) {
                case 0:
                    break;
                case 1:
                    node.rhs = stack.pop();
                    break;
                case 2:
                    node.rhs = stack.pop();
                    node.lhs = stack.pop();
                    break;
                default:
                    for (int i = 0; i < node.paramCount; i++) {
                        node.args.add(stack.pop());
                    }
                    break;
            }
            stack.push(node);
        }

        public void reset() {
            stack.clear();
        }
    }

    static {
        nonLiteralBranches.add(BRANCH_RIGHT_PARENTHESIS);
        nonLiteralBranches.add(BRANCH_RIGHT_BRACKET);
        nonLiteralBranches.add(BRANCH_CONSTANT);
        nonLiteralBranches.add(BRANCH_LITERAL);
        nonLiteralBranches.add(BRANCH_LAMBDA);
        nonLiteralBranches.add(BRANCH_ARRAY_TYPE_QUALIFIER_END);
        nonLiteralBranches.add(BRANCH_DECIMAL);

        caseKeywords.put("when", IDX_WHEN);
        caseKeywords.put("then", IDX_THEN);
        caseKeywords.put("else", IDX_ELSE);

        moreCastTargetTypes.add(ColumnType.UUID);
        moreCastTargetTypes.add(ColumnType.IPv4);
        moreCastTargetTypes.add(ColumnType.VARCHAR);
        moreCastTargetTypes.add(ColumnType.ARRAY);
        moreCastTargetTypes.add(ColumnType.DECIMAL);

        allFunctions.put("<>", "<>all");
        allFunctions.put("!=", "<>all");
    }
}
