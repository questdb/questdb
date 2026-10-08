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

import org.junit.Assert;
import org.junit.Test;

import java.util.Set;

/**
 * The guarded sites a type registered later can be declared refused at, and their refusals.
 */
public class TypeConformanceTypesTest {
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
}
