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

package io.questdb.test.cairo.types;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.ColumnTypeTag;
import io.questdb.cairo.PhysicalDescriptor;
import io.questdb.cairo.RelationRules;
import io.questdb.cairo.TypeDriver;
import io.questdb.test.cairo.LookAlikeTypeDriver;
import org.jetbrains.annotations.Nullable;
import org.junit.Assert;
import org.junit.Test;

import java.util.TreeSet;

/**
 * The relation rules read three facts per type (its relation kind, its width in bits and its
 * implicit-cast list), so two types that agree on them relate alike to every third type, except
 * where an exception cell or a rule clause names one of them. These tests compare the relations
 * that read the lists, W, C, N and CASE's number rule, pair by pair through the rules' functions
 * over two type drivers, which a type no tag resolves to can be handed to as well; A, K and G read
 * the kind alone, so a type joins them by its kind.
 */
public class RelationRulesTest {
    // a tag code that no implicit-cast list and no exception cell names: the stub type drivers'
    private static final short NEWCOMER = ColumnType.MAX_TAG + 1;

    @Test
    public void testByteAndNeverNullIntHaveACommonNumberType() {
        // BYTE widens to a never-null INT, which holds every BYTE value and has no NULL for BYTE to
        // lose, so UNION takes the never-null INT; CASE takes INT, the first type of BYTE's list
        // the never-null INT reaches through its own list, which is INT's. Before W into a
        // newcomer derived from its facts, UNION fell back to STRING
        final TypeDriver neverNull = LookAlikeTypeDriver.neverNullInt();
        final TypeDriver byteDriver = ColumnType.getTypeDriver(ColumnType.BYTE);
        Assert.assertEquals(ColumnType.INT, RelationRules.caseCommonNumber(ColumnType.BYTE, byteDriver, NEWCOMER, neverNull));
        Assert.assertEquals(ColumnType.INT, RelationRules.caseCommonNumber(NEWCOMER, neverNull, ColumnType.BYTE, byteDriver));
        Assert.assertEquals(NEWCOMER, unionType(ColumnType.BYTE, byteDriver, NEWCOMER, neverNull));
        Assert.assertEquals(NEWCOMER, unionType(NEWCOMER, neverNull, ColumnType.BYTE, byteDriver));
    }

    @Test
    public void testLookAlikesRelateAlike() {
        // every two stored types of one relation kind and width relate alike to every third type
        // (among the existing types, DATE with TIMESTAMP and STRING with VARCHAR); kind and width as
        // the type drivers answer them
        int pairs = 0;
        for (short a = 0; a <= ColumnType.MAX_TAG; a++) {
            final TypeDriver driverA = PhysicalDescriptor.storedTypeDriverOf(a);
            if (driverA == null) {
                continue;
            }
            for (short b = (short) (a + 1); b <= ColumnType.MAX_TAG; b++) {
                final TypeDriver driverB = PhysicalDescriptor.storedTypeDriverOf(b);
                if (driverB != null
                        && driverA.getRelationKind() == driverB.getRelationKind()
                        && driverA.getRelationBits() == driverB.getRelationBits()) {
                    Assert.assertEquals(ColumnType.nameOf(a) + " and " + ColumnType.nameOf(b), "[]", tagsRelatingOtherwise(a, driverA, b, driverB, false).toString());
                    pairs++;
                }
            }
        }
        Assert.assertEquals(2, pairs);
    }

    @Test
    public void testSignedIntKindLookAlikeNeedsNoEditToByteShortChar() {
        // a signed 32-bit INT-kind type relates as INT does with no edit to the lists of BYTE,
        // SHORT and CHAR, which name INT but not the newcomer
        final TypeDriver intDriver = ColumnType.getTypeDriver(ColumnType.INT);
        Assert.assertEquals("[]", tagsRelatingOtherwise(ColumnType.INT, intDriver, NEWCOMER, LookAlikeTypeDriver.sentinelInt(), true).toString());
        // a never-null one too; only the texts, whose NULL it cannot hold, do not widen to it
        // as they widen to INT, by parsing through INT's getter
        Assert.assertEquals("[STRING, VARCHAR, VARCHAR_SLICE]", tagsRelatingOtherwise(ColumnType.INT, intDriver, NEWCOMER, LookAlikeTypeDriver.neverNullInt(), true).toString());
    }

    @Test
    public void testUnsignedIntKindLookAlikeNamesItsSignedSources() {
        // an unsigned 32-bit INT-kind type does not take the widening into INT of the types that
        // widen through a signed integer: BYTE, SHORT and CHAR by their lists, and the texts,
        // which parse through INT's signed getter; the rules report them until the type's author
        // names their cells
        Assert.assertEquals("[BYTE, CHAR, SHORT, STRING, VARCHAR, VARCHAR_SLICE]", tagsRelatingOtherwise(ColumnType.INT, ColumnType.getTypeDriver(ColumnType.INT), NEWCOMER, LookAlikeTypeDriver.unsignedInt(), true).toString());
    }

    private static boolean isAlike(short aTag, short bTag, short resultWithA, short resultWithB) {
        return resultWithA == resultWithB || (resultWithA == aTag && resultWithB == bTag);
    }

    // the same-or-wider test of ColumnType.isToSameOrWider for two numbers
    private static boolean isToSameOrWider(short fromTag, TypeDriver from, short toTag, TypeDriver to) {
        return fromTag == toTag
                || RelationRules.isBuiltInWidening(fromTag, from, toTag, to)
                || RelationRules.isWideningCast(fromTag, from, toTag, to);
    }

    // the third types that relate to a and b differently, under W, C, N or CASE's number rule, in
    // either direction, outside the cells an exception or a rule clause names; the lists the stub
    // type drivers' tests pin hold the existing types, so those tests skip a type registered later
    private static TreeSet<String> tagsRelatingOtherwise(short aTag, TypeDriver a, short bTag, TypeDriver b, boolean isLaterTypeSkipped) {
        final TreeSet<String> tags = new TreeSet<>();
        for (short t = 0; t <= ColumnType.MAX_TAG; t++) {
            if (t == aTag || t == bTag || (isLaterTypeSkipped && TypeConformanceTypes.isLaterTag(t))) {
                continue;
            }
            final TypeDriver third = ColumnType.findTypeDriver(t);
            boolean isAlike = isNamed('W', t, aTag, bTag) || (
                    RelationRules.isBuiltInWidening(t, third, aTag, a) == RelationRules.isBuiltInWidening(t, third, bTag, b)
                            && RelationRules.isBuiltInWidening(aTag, a, t, third) == RelationRules.isBuiltInWidening(bTag, b, t, third));
            isAlike &= isNamed('C', t, aTag, bTag) || (
                    RelationRules.isWideningCast(t, third, aTag, a) == RelationRules.isWideningCast(t, third, bTag, b)
                            && RelationRules.isWideningCast(aTag, a, t, third) == RelationRules.isWideningCast(bTag, b, t, third));
            isAlike &= isNamed('N', t, aTag, bTag) || (
                    RelationRules.isNarrowing(t, third, aTag, a) == RelationRules.isNarrowing(t, third, bTag, b)
                            && RelationRules.isNarrowing(aTag, a, t, third) == RelationRules.isNarrowing(bTag, b, t, third));
            isAlike &= isNamed('E', t, aTag, bTag) || (
                    isAlike(aTag, bTag, RelationRules.caseCommonNumber(t, third, aTag, a), RelationRules.caseCommonNumber(t, third, bTag, b))
                            && isAlike(aTag, bTag, RelationRules.caseCommonNumber(aTag, a, t, third), RelationRules.caseCommonNumber(bTag, b, t, third)));
            if (!isAlike) {
                final ColumnTypeTag tag = ColumnTypeTag.of(t);
                tags.add(tag != null ? tag.name() : String.valueOf(t));
            }
        }
        return tags;
    }

    private static boolean isNamed(char relation, short third, short aTag, short bTag) {
        return RelationRules.isNamedCell(relation, third, aTag) || RelationRules.isNamedCell(relation, third, bTag);
    }

    // the union type ColumnType.commonWideningType gives two numbers of different types: the one
    // the other widens to, else STRING
    private static short unionType(short aTag, @Nullable TypeDriver a, short bTag, @Nullable TypeDriver b) {
        if (isToSameOrWider(bTag, b, aTag, a)) {
            return aTag;
        }
        if (isToSameOrWider(aTag, a, bTag, b)) {
            return bTag;
        }
        return ColumnType.STRING;
    }
}
