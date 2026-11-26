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
import org.junit.Assert;
import org.junit.Test;

import java.util.TreeSet;

/**
 * The relation rules read three facts per type (its relation kind, its width in bits and its
 * implicit-cast list), so two types that agree on them relate alike to every third type, except
 * where an exception cell or a rule clause names one of them. The test compares the relations that
 * read the lists, W, C, N and CASE's number rule, pair by pair through the rules' functions over two
 * type drivers; A, K and G read the kind alone, so a type joins them by its kind.
 */
public class RelationRulesTest {
    @Test
    public void testLookAlikesRelateAlike() {
        // every two stored types of one relation kind and width relate alike to every third type
        // (among the existing types, DATE with TIMESTAMP and STRING with VARCHAR); kind and width as
        // the type drivers answer them. A type registered later is left out: a look-alike that
        // stores no NULL or orders unsigned may relate otherwise, which its own PR decides
        int pairs = 0;
        for (short a = 0; a <= ColumnType.MAX_TAG; a++) {
            final TypeDriver driverA = PhysicalDescriptor.storedTypeDriverOf(a);
            if (driverA == null || TypeConformanceTypes.isLaterTag(a)) {
                continue;
            }
            for (short b = (short) (a + 1); b <= ColumnType.MAX_TAG; b++) {
                final TypeDriver driverB = PhysicalDescriptor.storedTypeDriverOf(b);
                if (driverB != null
                        && !TypeConformanceTypes.isLaterTag(b)
                        && driverA.getRelationKind() == driverB.getRelationKind()
                        && driverA.getRelationBits() == driverB.getRelationBits()) {
                    Assert.assertEquals(ColumnType.nameOf(a) + " and " + ColumnType.nameOf(b), "[]", tagsRelatingOtherwise(a, driverA, b, driverB).toString());
                    pairs++;
                }
            }
        }
        Assert.assertEquals(2, pairs);
    }

    private static boolean isAlike(short aTag, short bTag, short resultWithA, short resultWithB) {
        return resultWithA == resultWithB || (resultWithA == aTag && resultWithB == bTag);
    }

    // the third types that relate to a and b differently, under W, C, N or CASE's number rule, in
    // either direction, outside the cells an exception or a rule clause names
    private static TreeSet<String> tagsRelatingOtherwise(short aTag, TypeDriver a, short bTag, TypeDriver b) {
        final TreeSet<String> tags = new TreeSet<>();
        for (short t = 0; t <= ColumnType.MAX_TAG; t++) {
            if (t == aTag || t == bTag) {
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
}
