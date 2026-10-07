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

package io.questdb.test.cairo.types;

import io.questdb.std.ObjList;
import org.junit.Assert;
import org.junit.Test;

import java.util.Set;

/**
 * The declaration lines a type registered later joins the kit by, and the guarded sites a line
 * declares the type refused at.
 */
public class TypeConformanceTypesTest {
    private static final Set<String> DECLARABLE = Set.of("SAMPLE BY FILL(value)", "memoized virtual column", "ILP column kind");

    @Test
    public void testGuardedSitesAndTheirRefusals() {
        // the add-a-type tool's decisions name the twelve guarded sites; each raises the guard's
        // refusal, except ILP, which keeps the cast error it raised before the guard
        Assert.assertEquals(Set.of("memoized virtual column", "SAMPLE BY FILL(PREV)", "SAMPLE BY FILL(LINEAR)", "SAMPLE BY FILL(value)",
                "COPY bind snapshot", "ILP column kind", "WAL columnar append", "QWP WAL append", "Parquet conversion", "between", "= NULL",
                "copier conversion"), TypeConformanceInvariants.declarableSites());
        final TypeConformanceTypes.Entry type = TypeConformanceTypes.byLabel("INT");
        Assert.assertEquals("no family arm for INT at SAMPLE BY FILL(value)", TypeConformanceInvariants.refusalOf(type, "SAMPLE BY FILL(value)"));
        Assert.assertEquals("cast error from protocol type", TypeConformanceInvariants.refusalOf(type, "ILP column kind"));
    }

    @Test
    public void testLaterTypeLineWithDeclaredRefusals() {
        // the sixth field lists the guarded sites the type is refused at on purpose
        final String[] six = TypeConformanceTypes.parseLaterTypeLine(
                "NN_INT | NN_INT | NONE | sql.* ingest.* | I32 | SAMPLE BY FILL(value), memoized virtual column",
                DECLARABLE
        );
        Assert.assertEquals(6, six.length);
        Assert.assertEquals("NN_INT", six[0]);
        Assert.assertEquals("NONE", six[2]);
        Assert.assertEquals("sql.* ingest.*", six[3]);
        Assert.assertEquals("I32", six[4]);
        final ObjList<String> sites = TypeConformanceTypes.splitSites(six[5]);
        Assert.assertEquals(2, sites.size());
        Assert.assertEquals("SAMPLE BY FILL(value)", sites.getQuick(0));
        Assert.assertEquals("memoized virtual column", sites.getQuick(1));

        // an empty sixth field declares no site
        Assert.assertEquals(0, TypeConformanceTypes.splitSites(TypeConformanceTypes.parseLaterTypeLine("NN_INT | NN_INT | NONE | sql.* | I32 |", DECLARABLE)[5]).size());

        // five and four fields stay valid and declare no site
        final String[] five = TypeConformanceTypes.parseLaterTypeLine("UINT32 | UINT32 | SENTINEL | sql.order_* | U32", DECLARABLE);
        Assert.assertEquals("U32", five[4]);
        Assert.assertEquals(0, TypeConformanceTypes.splitSites(five[5]).size());
        final String[] four = TypeConformanceTypes.parseLaterTypeLine("UINT32 | UINT32 | SENTINEL | -", DECLARABLE);
        Assert.assertEquals("-", four[3]);
        Assert.assertEquals("", four[4]);
        Assert.assertEquals(0, TypeConformanceTypes.splitSites(four[5]).size());

        // a label that names no guarded site fails, naming the label and the line
        final String unknownLine = "NN_INT | NN_INT | NONE | sql.* | I32 | SAMPLE BY FILL(value), SAMPLE BY FILL(VALUE)";
        try {
            TypeConformanceTypes.parseLaterTypeLine(unknownLine, DECLARABLE);
            Assert.fail("an unknown site label must fail");
        } catch (IllegalStateException e) {
            Assert.assertTrue(e.getMessage(), e.getMessage().contains("SAMPLE BY FILL(VALUE)"));
            Assert.assertTrue(e.getMessage(), e.getMessage().contains(unknownLine));
        }

        // so do three and seven fields
        for (String bad : new String[]{"NN_INT | NN_INT | NONE", "NN_INT | NN_INT | NONE | sql.* | I32 | | x"}) {
            try {
                TypeConformanceTypes.parseLaterTypeLine(bad, DECLARABLE);
                Assert.fail("a line of the wrong field count must fail: " + bad);
            } catch (IllegalStateException e) {
                Assert.assertTrue(e.getMessage(), e.getMessage().contains(bad));
            }
        }
    }
}
