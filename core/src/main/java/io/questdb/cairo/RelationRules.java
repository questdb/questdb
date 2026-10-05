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

import io.questdb.std.IntList;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

import static io.questdb.cairo.ColumnType.*;

/**
 * The pairwise relations between column types, derived by written rules from the facts each type
 * driver declares: its relation kind, its value width in bits and its one implicit-cast list. A
 * type's own relations, from it to every other type, derive from its facts. The relations of the
 * existing types into a type come from the existing types' implicit-cast lists and from the
 * exception cells below, which name tags, except that W into a type of a width-ordered kind
 * (INT, FLOAT, TEMPORAL) also derives from its facts, so a newcomer of such a kind needs no
 * existing list to name it unless it differs in signedness; a new kind of value needs a rule
 * clause. W, C, N and CASE's number rule are answered for one pair at a time, over the two types'
 * tags and type drivers, so a type no tag resolves to yet can be asked too; the tag-keyed rows
 * below call them.
 * <p>
 * The exception cells hold the pairs where the existing types' relations depart from the rules,
 * and {@code TypeRelationGoldenTest} pins every table. The rules read the type drivers, so
 * {@link ColumnType} fills its tables from here on first use, never in its static initializer.
 * <ul>
 * <li>W, built-in widening: the implicit list minus the type itself, where the target converts
 * through its own getter (integer, CHAR, float, temporal targets).</li>
 * <li>C, widening cast: the rest of the implicit list (C1); integers and CHAR of at most 16 bits
 * into DATE and TIMESTAMP (C2); a geohash into a narrower geohash (C3); text into every geohash,
 * TIMESTAMP, LONG256, IPv4 and the other texts (C4).</li>
 * <li>N, narrowing: the numeric and temporal targets that are neither the type nor W or C;
 * integers, CHAR and text into the decimals; text into UUID and arrays.</li>
 * <li>E, CASE: numbers meet at the first type of one's implicit list the other reaches; CHAR,
 * SYMBOL, STRING and VARCHAR meet at the later of the two; text meets UUID or IPv4 at the parsed
 * type; timestamps meet at the finer unit; any other value type meets only itself.</li>
 * <li>A, ALTER COLUMN TYPE: the conversion group follows the kind.</li>
 * <li>K, the copier's arms: numbers and temporals into numbers and temporals, integers also into
 * the decimals; CHAR and text into the text, geohash and parsed types; a geohash into one no
 * wider; UUID and LONG128 into each other and text; SYMBOL into text.</li>
 * <li>G, the cast groups of CREATE TABLE AS SELECT: numbers, CHAR and the temporals; BOOLEAN; the
 * column texts and SYMBOL; BINARY.</li>
 * </ul>
 */
public final class RelationRules {
    // A: cells no kind rule gives: SYMBOL's conversions, and text into IPv4
    private static final short[][] ALTER_ADD = {
            {STRING, IPv4}, {VARCHAR, IPv4},
            {SYMBOL, BOOLEAN}, {SYMBOL, BYTE}, {SYMBOL, CHAR}, {SYMBOL, DATE}, {SYMBOL, DOUBLE}, {SYMBOL, FLOAT},
            {SYMBOL, INT}, {SYMBOL, IPv4}, {SYMBOL, LONG}, {SYMBOL, SHORT}, {SYMBOL, STRING}, {SYMBOL, SYMBOL},
            {SYMBOL, TIMESTAMP}, {SYMBOL, UUID}, {SYMBOL, VARCHAR}
    };
    // W: cells the rule does not derive but the relation has (ADD), and cells it derives that the
    // relation lacks (REMOVE)
    private static final short[][] BUILT_IN_ADD = {{LONG, FLOAT}, {DATE, FLOAT}, {TIMESTAMP, FLOAT}};
    private static final short[][] BUILT_IN_REMOVE = {{TIMESTAMP, DATE}, {SYMBOL, CHAR}, {SYMBOL, INT}, {SYMBOL, TIMESTAMP}, {LONG256, LONG}};
    // C
    private static final short[][] CAST_ADD = {{BYTE, CHAR}, {CHAR, GEOBYTE}, {CHAR, SYMBOL}, {SYMBOL, TIMESTAMP}, {UUID, VARCHAR}};
    private static final short[][] CAST_REMOVE = {{IPv4, STRING}, {IPv4, VARCHAR}, {INTERVAL, STRING}};
    // E: {from, other, result} cells that override the rule: LONG with FLOAT is FLOAT (the rule
    // gives DOUBLE), both ways; SYMBOL then CHAR is STRING (the rule gives SYMBOL)
    private static final int[][] CASE_REPLACE = {{LONG, FLOAT, FLOAT}, {FLOAT, LONG, FLOAT}, {SYMBOL, CHAR, STRING}};
    // K: BYTE has a BOOLEAN arm
    private static final short[][] COPIER_ADD = {{BYTE, BOOLEAN}};
    private static final short[][] NARROWING_REMOVE = {{INT, CHAR}, {DOUBLE, DATE}, {DOUBLE, TIMESTAMP}};
    private static final short[][] NO_CELLS = {};
    // CASE's text order: two of them meet at the later one
    private static final short[] TEXT_ORDER = {CHAR, SYMBOL, STRING, VARCHAR};

    private RelationRules() {
    }

    /**
     * A: the column types ALTER TABLE ... ALTER COLUMN ... TYPE converts a column of {@code fromTag}
     * to. The rows are not symmetrical: integers and floats convert to the decimals, decimals do not
     * convert back to integers (no kernel for it); CHAR converts to the texts only, the texts convert
     * to CHAR. {@code TypeRelationGoldenTest.testColumnConversionSupport} pins the matrix.
     */
    public static short[] alter(short fromTag) {
        final IntList out = new IntList();
        final RelationKind k = kind(fromTag);
        switch (k) {
            case BOOL, TEMPORAL -> addNumbersAndTemporalsAndColumnTexts(out);
            case INT, FLOAT -> {
                addNumbersAndTemporalsAndColumnTexts(out);
                addKind(out, RelationKind.DECIMAL);
            }
            case CHAR, UUID, IPV4 -> addColumnTexts(out);
            case DECIMAL -> {
                addKind(out, RelationKind.FLOAT);
                addPersistedText(out);
                addKind(out, RelationKind.DECIMAL);
            }
            case TEXT -> {
                if (isPersisted(fromTag)) {
                    addNumbersAndTemporalsAndColumnTexts(out);
                    addKind(out, RelationKind.CHAR);
                    addKind(out, RelationKind.UUID);
                    addKind(out, RelationKind.DECIMAL);
                }
            }
            case SYMBOL, LONG256, LONG128, BINARY, GEO, ARRAY, INTERVAL, UNDEF, PSEUDO, NULL -> {
            }
        }
        return apply(fromTag, out, ALTER_ADD, NO_CELLS, true);
    }

    /**
     * W: the types a value of {@code fromTag} widens to without a cast wrapper: the target type's
     * own getter does the conversion.
     */
    public static short[] builtInWidening(short fromTag) {
        final TypeDriver from = findTypeDriver(fromTag);
        final IntList out = new IntList();
        for (short t = 0; t <= MAX_TAG; t++) {
            if (isBuiltInWidening(fromTag, from, t, findTypeDriver(t))) {
                out.add(t);
            }
        }
        return toRow(out);
    }

    /**
     * E: the type escalation of CASE, SWITCH and COALESCE. The row of {@code fromType}, the type the
     * branches so far agree on, lists as (valueType, resultType) pairs the types the next branch may
     * have and the type the expression then takes; a pair missing from the row is inconvertible.
     * Keyed by encoded type: the two TIMESTAMP precisions have their own rows and escalate to
     * TIMESTAMP_NANO. NULL, undefined, array and decimal types are resolved before the table
     * ({@code CaseCommon.getCommonType}). The rows are not all symmetric: SYMBOL then CHAR is
     * STRING, CHAR then SYMBOL is SYMBOL. {@code TypeRelationGoldenTest.testCaseCommonType} pins them.
     */
    public static int[] caseEscalation(int fromType) {
        final short fromTag = tagOf(fromType);
        final IntList out = new IntList();
        switch (kind(fromTag)) {
            case INT, FLOAT -> {
                final TypeDriver from = findTypeDriver(fromTag);
                for (short o = 0; o <= MAX_TAG; o++) {
                    final short common = caseCommonNumber(fromTag, from, o, findTypeDriver(o));
                    if (common != -1) {
                        out.add(o);
                        out.add(common);
                    }
                }
            }
            case CHAR, SYMBOL, TEXT -> {
                final int rank = textRank(fromTag);
                if (rank != -1) {
                    for (int i = 0; i < TEXT_ORDER.length; i++) {
                        out.add(TEXT_ORDER[i]);
                        out.add(TEXT_ORDER[Math.max(rank, i)]);
                    }
                    // text meets UUID and IPv4 at the parsed type
                    if (kind(fromTag) == RelationKind.TEXT) {
                        addSelfPairs(out, RelationKind.UUID);
                        addSelfPairs(out, RelationKind.IPV4);
                    }
                }
            }
            case UUID, IPV4 -> {
                // the parsed type, from the texts a column can have
                for (short t = 0; t <= MAX_TAG; t++) {
                    if (kind(t) == RelationKind.TEXT && isPersisted(t)) {
                        out.add(t);
                        out.add(fromTag);
                    }
                }
                out.add(fromTag);
                out.add(fromTag);
            }
            case TEMPORAL -> {
                if (fromTag == TIMESTAMP) {
                    // timestamps meet at the finer unit
                    out.add(TIMESTAMP_MICRO);
                    out.add(fromType == TIMESTAMP_NANO ? TIMESTAMP_NANO : TIMESTAMP_MICRO);
                    out.add(TIMESTAMP_NANO);
                    out.add(TIMESTAMP_NANO);
                } else {
                    out.add(fromTag);
                    out.add(fromTag);
                }
            }
            case BOOL, LONG256, BINARY -> {
                out.add(fromTag);
                out.add(fromTag);
            }
            case LONG128, GEO, DECIMAL, ARRAY, INTERVAL, UNDEF, PSEUDO, NULL -> {
            }
        }
        for (int[] cell : CASE_REPLACE) {
            if (cell[0] == fromTag) {
                for (int i = 0, n = out.size(); i < n; i += 2) {
                    if (out.getQuick(i) == cell[1]) {
                        out.setQuick(i + 1, cell[2]);
                    }
                }
            }
        }
        return out.toArray();
    }

    /**
     * K: the types the copiers (single-method, chunked, looping) have an arm to copy a value of
     * {@code fromTag} into. {@code TypeRelationGoldenTest.testCopierArms} pins the relation.
     */
    public static short[] copier(short fromTag) {
        final IntList out = new IntList();
        final RelationKind k = kind(fromTag);
        switch (k) {
            case INT, FLOAT, TEMPORAL -> {
                addNumbersAndTemporals(out);
                if (k == RelationKind.INT) {
                    addKind(out, RelationKind.DECIMAL);
                }
            }
            case CHAR -> {
                addNumbersAndTemporals(out);
                addKind(out, RelationKind.CHAR);
                addColumnTexts(out);
                // the one-character geohash
                addGeoUpTo(out, 8, 8);
                addKind(out, RelationKind.DECIMAL);
            }
            case TEXT -> {
                if (isPersisted(fromTag)) {
                    addNumbersAndTemporals(out);
                    addKind(out, RelationKind.CHAR);
                    addColumnTexts(out);
                    addKind(out, RelationKind.ARRAY);
                    addKind(out, RelationKind.IPV4);
                    addKind(out, RelationKind.UUID);
                    addKind(out, RelationKind.LONG256);
                    addGeoUpTo(out, 0, Integer.MAX_VALUE);
                    addKind(out, RelationKind.DECIMAL);
                }
            }
            case GEO -> addGeoUpTo(out, 0, bits(fromTag));
            case DECIMAL -> addKind(out, RelationKind.DECIMAL);
            case UUID, LONG128 -> {
                addKind(out, RelationKind.UUID);
                addKind(out, RelationKind.LONG128);
                addPersistedText(out);
            }
            case SYMBOL -> addColumnTexts(out);
            case BOOL, LONG256, BINARY, IPV4, ARRAY -> out.add(fromTag);
            case INTERVAL, UNDEF, PSEUDO, NULL -> {
            }
        }
        return apply(fromTag, out, COPIER_ADD, NO_CELLS, true);
    }

    /**
     * G: the cast group of {@code tag} for CREATE TABLE ... AS (SELECT ...) with a column CAST, which
     * admits a cast between two types of one group when the record copier has an arm for the pair;
     * -1 for a type no group covers, whose casts the caller admits by the conversion relations.
     * {@code TypeRelationGoldenTest.testIsCompatibleCast} pins the relation.
     */
    public static int ctasCastGroup(short tag) {
        return switch (kind(tag)) {
            case INT, CHAR, FLOAT, TEMPORAL -> 1;
            case BOOL -> 2;
            case TEXT -> isPersisted(tag) ? 3 : -1;
            case SYMBOL -> 3;
            case BINARY -> 4;
            case LONG256, GEO, UUID, LONG128, IPV4, DECIMAL, ARRAY, INTERVAL, UNDEF, PSEUDO, NULL -> -1;
        };
    }

    /**
     * E for two numbers: the first type of {@code a}'s implicit-cast list that {@code b} is or
     * reaches, the type CASE escalates the two to; -1 when either is no number (INT or FLOAT
     * kind), when there is no such type or when it is no number. Over the two types' tags and type
     * drivers; the exception cells apply to the row ({@link #caseEscalation(int)}).
     */
    public static short caseCommonNumber(short aTag, @Nullable TypeDriver a, short bTag, @Nullable TypeDriver b) {
        if (!isNumber(a) || !isNumber(b)) {
            return -1;
        }
        final short[] reach = b.getImplicitCasts();
        for (short t : a.getImplicitCasts()) {
            if (t == bTag || contains(reach, t)) {
                return isNumber(findTypeDriver(t)) ? t : -1;
            }
        }
        return -1;
    }

    /**
     * The implicit-cast list of {@code tag}, its overload row: the type driver's {@link
     * TypeDriver#getImplicitCasts()} for a real type. Of the pseudo tags, UNDEFINED (an unbound
     * bind variable) overloads to the types it can be defined as, and CURSOR to itself; the rest
     * overload to nothing.
     */
    public static short[] implicitCasts(short tag) {
        final TypeDriver driver = findTypeDriver(tag);
        if (driver != null) {
            return driver.getImplicitCasts();
        }
        if (tag == UNDEFINED) {
            return new short[]{DOUBLE, FLOAT, STRING, VARCHAR, LONG, TIMESTAMP, DATE, INT, CHAR, SHORT, BYTE, BOOLEAN};
        }
        if (tag == CURSOR) {
            return new short[]{CURSOR};
        }
        return new short[0];
    }

    /**
     * W for one pair: whether a value of {@code fromTag} widens to {@code toTag} without a cast
     * wrapper. The source's implicit-cast list names the target and the target converts through
     * its own getter (integer, CHAR, float and temporal targets); or the target is of a
     * width-ordered kind and the source, or a type its list widens it to, is of that kind, no wider
     * and of the same signedness, and the target can hold the source's NULL. The exception cells
     * apply to both. Over the two types' tags and type drivers; a null type driver is a pseudo
     * type.
     */
    public static boolean isBuiltInWidening(short fromTag, @Nullable TypeDriver from, short toTag, @Nullable TypeDriver to) {
        if (from == null || to == null || fromTag == toTag || isCell(BUILT_IN_REMOVE, fromTag, toTag)) {
            return false;
        }
        return isListedWidening(fromTag, from, toTag, to) || isDerivedWidening(fromTag, from, to);
    }

    /**
     * Whether an exception cell or a rule clause that names a tag decides the relation
     * {@code relation} ('W', 'C', 'N' or 'E') between the two tags, in either direction. Two types
     * of one kind and width relate alike to a third type except at such a cell.
     */
    @TestOnly
    public static boolean isNamedCell(char relation, short a, short b) {
        return switch (relation) {
            case 'W' -> isCellEitherWay(BUILT_IN_ADD, a, b) || isCellEitherWay(BUILT_IN_REMOVE, a, b);
            // C4 names TIMESTAMP as the one temporal type text parses into
            case 'C' -> isCellEitherWay(CAST_ADD, a, b) || isCellEitherWay(CAST_REMOVE, a, b)
                    || (a == TIMESTAMP && kind(b) == RelationKind.TEXT) || (b == TIMESTAMP && kind(a) == RelationKind.TEXT);
            // N is the complement of W and C
            case 'N' -> isCellEitherWay(NARROWING_REMOVE, a, b) || isNamedCell('W', a, b) || isNamedCell('C', a, b);
            // CASE orders the texts by TEXT_ORDER and meets timestamps at the finer unit
            case 'E' -> {
                for (int[] cell : CASE_REPLACE) {
                    if ((cell[0] == a && cell[1] == b) || (cell[0] == b && cell[1] == a)) {
                        yield true;
                    }
                }
                yield textRank(a) != -1 || textRank(b) != -1 || a == TIMESTAMP || b == TIMESTAMP;
            }
            default -> throw new IllegalArgumentException("no relation " + relation);
        };
    }

    /**
     * N for one pair: whether a value of {@code fromTag} narrows to {@code toTag} with an explicit
     * cast, which may lose precision or range: a numeric or temporal target that is neither W nor
     * C, every numeric and temporal target for a text, the decimals for an integer, CHAR or text,
     * and UUID and the arrays for a text; with the exception cells applied. Over the two types'
     * tags and type drivers; a null type driver is a pseudo type.
     */
    public static boolean isNarrowing(short fromTag, @Nullable TypeDriver from, short toTag, @Nullable TypeDriver to) {
        if (from == null || fromTag == toTag || isCell(NARROWING_REMOVE, fromTag, toTag)) {
            return false;
        }
        final RelationKind k = from.getRelationKind();
        if (k != RelationKind.INT && k != RelationKind.CHAR && k != RelationKind.FLOAT && k != RelationKind.TEMPORAL && k != RelationKind.TEXT) {
            return false;
        }
        final boolean isIntoDecimal = k != RelationKind.FLOAT && k != RelationKind.TEMPORAL;
        if (to == null) {
            // of the pseudo types, only DECIMAL
            return isIntoDecimal && toTag == DECIMAL;
        }
        final RelationKind tk = to.getRelationKind();
        return (isGetterKind(tk) && (k == RelationKind.TEXT
                || (!isBuiltInWidening(fromTag, from, toTag, to) && !isWideningCast(fromTag, from, toTag, to))))
                || (isIntoDecimal && tk == RelationKind.DECIMAL)
                || (k == RelationKind.TEXT && (tk == RelationKind.UUID || tk == RelationKind.ARRAY));
    }

    /**
     * C for one pair: whether a value of {@code fromTag} converts to {@code toTag}, same or wider,
     * through a cast wrapper: the rest of the implicit-cast list (C1), integers and CHAR of at most
     * 16 bits into the temporal types (C2), a geohash into a narrower geohash (C3), text into every
     * geohash, TIMESTAMP, LONG256, IPv4, SYMBOL and the column texts (C4); with the exception cells
     * applied. Over the two types' tags and type drivers; a null type driver is a pseudo type.
     */
    public static boolean isWideningCast(short fromTag, @Nullable TypeDriver from, short toTag, @Nullable TypeDriver to) {
        if (fromTag == toTag || isCell(CAST_REMOVE, fromTag, toTag)) {
            return false;
        }
        if (isCell(CAST_ADD, fromTag, toTag)) {
            return true;
        }
        if (from == null || to == null) {
            return false;
        }
        final RelationKind k = from.getRelationKind();
        final RelationKind tk = to.getRelationKind();
        // C1
        if (contains(from.getImplicitCasts(), toTag) && !isGetterKind(tk) && tk != RelationKind.DECIMAL && tk != RelationKind.GEO) {
            return true;
        }
        return ((k == RelationKind.INT || k == RelationKind.CHAR) && from.getRelationBits() <= 16 && tk == RelationKind.TEMPORAL)
                || (k == RelationKind.GEO && tk == RelationKind.GEO && to.getRelationBits() < from.getRelationBits())
                || (k == RelationKind.TEXT && (tk == RelationKind.GEO || toTag == TIMESTAMP || tk == RelationKind.LONG256
                || tk == RelationKind.IPV4 || tk == RelationKind.SYMBOL || (tk == RelationKind.TEXT && isPersisted(toTag))));
    }

    /**
     * N: the types a value of {@code fromTag} narrows to with an explicit cast, which may lose
     * precision or range.
     */
    public static short[] narrowing(short fromTag) {
        final TypeDriver from = findTypeDriver(fromTag);
        final IntList out = new IntList();
        for (short t = 0; t <= MAX_TAG; t++) {
            if (isNarrowing(fromTag, from, t, findTypeDriver(t))) {
                out.add(t);
            }
        }
        return toRow(out);
    }

    /**
     * C: the other same-or-wider conversions, the ones that need a cast wrapper.
     */
    public static short[] wideningCast(short fromTag) {
        final TypeDriver from = findTypeDriver(fromTag);
        final IntList out = new IntList();
        for (short t = 0; t <= MAX_TAG; t++) {
            if (isWideningCast(fromTag, from, t, findTypeDriver(t))) {
                out.add(t);
            }
        }
        return toRow(out);
    }

    /**
     * The relation kind of {@code tag}: its type driver's {@link TypeDriver#getRelationKind()}, or
     * the rules' own kind for a pseudo tag. {@link ColumnType#isIntegral(int)} and {@link
     * ColumnType#isIntegralOrFloat(int)} read it too.
     */
    static RelationKind kind(short tag) {
        final TypeDriver driver = findTypeDriver(tag);
        if (driver != null) {
            return driver.getRelationKind();
        }
        if (tag == UNDEFINED) {
            return RelationKind.UNDEF;
        }
        return tag == ColumnType.NULL ? RelationKind.NULL : RelationKind.PSEUDO;
    }

    private static void addColumnTexts(IntList out) {
        addKind(out, RelationKind.SYMBOL);
        addPersistedText(out);
    }

    private static void addGeoUpTo(IntList out, int loBits, int hiBits) {
        for (short t = 0; t <= MAX_TAG; t++) {
            if (kind(t) == RelationKind.GEO && bits(t) >= loBits && bits(t) <= hiBits) {
                out.add(t);
            }
        }
    }

    private static void addKind(IntList out, RelationKind kind) {
        for (short t = 0; t <= MAX_TAG; t++) {
            if (kind(t) == kind) {
                out.add(t);
            }
        }
    }

    private static void addNumbersAndTemporals(IntList out) {
        addKind(out, RelationKind.INT);
        addKind(out, RelationKind.FLOAT);
        addKind(out, RelationKind.TEMPORAL);
    }

    private static void addNumbersAndTemporalsAndColumnTexts(IntList out) {
        addKind(out, RelationKind.BOOL);
        addNumbersAndTemporals(out);
        addColumnTexts(out);
    }

    private static void addPersistedText(IntList out) {
        for (short t = 0; t <= MAX_TAG; t++) {
            if (kind(t) == RelationKind.TEXT && isPersisted(t)) {
                out.add(t);
            }
        }
    }

    private static void addSelfPairs(IntList out, RelationKind kind) {
        for (short t = 0; t <= MAX_TAG; t++) {
            if (kind(t) == kind) {
                out.add(t);
                out.add(t);
            }
        }
    }

    private static short[] apply(short fromTag, IntList out, short[][] add, short[][] remove, boolean isSelfKept) {
        for (short[] cell : add) {
            if (cell[0] == fromTag && indexOf(out, cell[1]) < 0) {
                out.add(cell[1]);
            }
        }
        for (short[] cell : remove) {
            if (cell[0] == fromTag) {
                final int i = indexOf(out, cell[1]);
                if (i > -1) {
                    out.removeIndex(i);
                }
            }
        }
        final short[] row = new short[out.size()];
        int n = 0;
        for (int i = 0, m = out.size(); i < m; i++) {
            final short t = (short) out.getQuick(i);
            if ((isSelfKept || t != fromTag) && !contains(row, n, t)) {
                row[n++] = t;
            }
        }
        final short[] result = new short[n];
        System.arraycopy(row, 0, result, 0, n);
        return result;
    }

    private static int bits(short tag) {
        final TypeDriver driver = findTypeDriver(tag);
        return driver != null ? driver.getRelationBits() : 0;
    }

    private static boolean contains(short[] row, short t) {
        return contains(row, row.length, t);
    }

    private static boolean contains(short[] row, int n, short t) {
        for (int i = 0; i < n; i++) {
            if (row[i] == t) {
                return true;
            }
        }
        return false;
    }

    // whether a value of the target can stand for the source's NULL: the source has none, or the
    // target has a sentinel of its own
    private static boolean holdsNullOf(TypeDriver to, TypeDriver from) {
        return switch (from.getNullPolicy()) {
            case NONE -> true;
            case SENTINEL -> switch (to.getNullPolicy()) {
                case SENTINEL -> true;
                case NONE -> false;
            };
        };
    }

    private static int indexOf(IntList list, int v) {
        for (int i = 0, n = list.size(); i < n; i++) {
            if (list.getQuick(i) == v) {
                return i;
            }
        }
        return -1;
    }

    private static boolean isCell(short[][] cells, short fromTag, short toTag) {
        for (short[] cell : cells) {
            if (cell[0] == fromTag && cell[1] == toTag) {
                return true;
            }
        }
        return false;
    }

    private static boolean isCellEitherWay(short[][] cells, short a, short b) {
        return isCell(cells, a, b) || isCell(cells, b, a);
    }

    // W into a type of a width-ordered kind, which the source's list need not name: the source, or
    // a type the source's list widens it to, is of the target's kind, no wider and of the same
    // signedness, and the target can hold the source's NULL
    private static boolean isDerivedWidening(short fromTag, TypeDriver from, TypeDriver to) {
        final RelationKind toKind = to.getRelationKind();
        if ((toKind != RelationKind.INT && toKind != RelationKind.FLOAT && toKind != RelationKind.TEMPORAL)
                || !holdsNullOf(to, from)) {
            return false;
        }
        if (isNoWiderOfKind(from, to)) {
            return true;
        }
        for (short x : from.getImplicitCasts()) {
            if (isListedWideningOfKind(fromTag, from, x, to)) {
                return true;
            }
        }
        for (short[] cell : BUILT_IN_ADD) {
            if (cell[0] == fromTag && isListedWideningOfKind(fromTag, from, cell[1], to)) {
                return true;
            }
        }
        return false;
    }

    private static boolean isGetterKind(RelationKind k) {
        return k == RelationKind.INT || k == RelationKind.CHAR || k == RelationKind.FLOAT || k == RelationKind.TEMPORAL;
    }

    // W by the source's list and the exception cells that add a target
    private static boolean isListedWidening(short fromTag, TypeDriver from, short toTag, TypeDriver to) {
        return (isGetterKind(to.getRelationKind()) && contains(from.getImplicitCasts(), toTag)) || isCell(BUILT_IN_ADD, fromTag, toTag);
    }

    // the source's list widens it to x, and x is of the target's kind, no wider, of its signedness
    private static boolean isListedWideningOfKind(short fromTag, TypeDriver from, short x, TypeDriver to) {
        final TypeDriver xDriver = findTypeDriver(x);
        return xDriver != null
                && x != fromTag
                && !isCell(BUILT_IN_REMOVE, fromTag, x)
                && isListedWidening(fromTag, from, x, xDriver)
                && isNoWiderOfKind(xDriver, to);
    }

    private static boolean isNoWiderOfKind(TypeDriver type, TypeDriver to) {
        return type.getRelationKind() == to.getRelationKind()
                && type.getRelationBits() <= to.getRelationBits()
                && isUnsigned(type.getArithmetic()) == isUnsigned(to.getArithmetic());
    }

    private static boolean isNumber(@Nullable TypeDriver driver) {
        return driver != null && (driver.getRelationKind() == RelationKind.INT || driver.getRelationKind() == RelationKind.FLOAT);
    }

    private static boolean isUnsigned(PhysicalDescriptor.Arithmetic arithmetic) {
        return switch (arithmetic) {
            case U8, U16, U32 -> true;
            case I8, I16, I32, I64, F32, F64, WIDE, NONE -> false;
        };
    }

    // CASE's order of CHAR, SYMBOL and the two column texts; -1 for another type
    private static int textRank(short tag) {
        for (int i = 0; i < TEXT_ORDER.length; i++) {
            if (TEXT_ORDER[i] == tag) {
                return i;
            }
        }
        return -1;
    }

    private static short[] toRow(IntList out) {
        final short[] row = new short[out.size()];
        for (int i = 0, n = out.size(); i < n; i++) {
            row[i] = (short) out.getQuick(i);
        }
        return row;
    }
}
