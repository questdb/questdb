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

package io.questdb.test.cutlass.qwp;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.ColumnType;
import io.questdb.cutlass.qwp.codec.QwpEgressColumnDef;
import io.questdb.cutlass.qwp.protocol.QwpConstants;
import io.questdb.cutlass.qwp.server.egress.QwpEgressProcessorState;
import io.questdb.std.ObjList;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Unit coverage for {@link QwpEgressProcessorState#findOrAllocateSchemaId} and
 * its interaction with {@link QwpEgressProcessorState#wasLastSchemaIdReuse} and
 * {@link QwpEgressProcessorState#clear}. These are the mechanics behind the M1
 * fix (see PR 6991 review): identical schemas reuse their id, distinct ones
 * allocate fresh ids, and the per-connection cap is hit with a sentinel the
 * caller translates into a controlled error.
 */
public class QwpEgressProcessorStateTest extends AbstractCairoTest {

    private CairoConfiguration cfg;

    @Before
    public void setUpState() {
        cfg = configuration;
    }

    @Test
    public void testClearResetsIdCounterAndCache() {
        try (QwpEgressProcessorState state = new QwpEgressProcessorState(cfg)) {
            ObjList<QwpEgressColumnDef> cols = schemaOf("id", ColumnType.LONG);
            int firstId = state.findOrAllocateSchemaId(cols);
            Assert.assertEquals(0, firstId);

            state.clear();

            // After clear the counter starts from 0 and the cache is empty, so
            // the same schema gets a fresh id rather than its cached one.
            int afterClear = state.findOrAllocateSchemaId(cols);
            Assert.assertEquals("counter must reset", 0, afterClear);
            Assert.assertFalse("cache must be empty after clear", state.wasLastSchemaIdReuse());
        }
    }

    @Test
    public void testDifferentColumnCountYieldsNewId() {
        try (QwpEgressProcessorState state = new QwpEgressProcessorState(cfg)) {
            ObjList<QwpEgressColumnDef> a = schemaOf("id", ColumnType.LONG);
            ObjList<QwpEgressColumnDef> b = schemaOf("id", ColumnType.LONG, "ts", ColumnType.TIMESTAMP);
            Assert.assertEquals(0, state.findOrAllocateSchemaId(a));
            Assert.assertEquals(1, state.findOrAllocateSchemaId(b));
            Assert.assertFalse(state.wasLastSchemaIdReuse());
        }
    }

    @Test
    public void testDifferentNameYieldsNewId() {
        try (QwpEgressProcessorState state = new QwpEgressProcessorState(cfg)) {
            Assert.assertEquals(0, state.findOrAllocateSchemaId(schemaOf("a", ColumnType.LONG)));
            Assert.assertEquals(1, state.findOrAllocateSchemaId(schemaOf("b", ColumnType.LONG)));
            Assert.assertFalse(state.wasLastSchemaIdReuse());
        }
    }

    @Test
    public void testDifferentTypeYieldsNewId() {
        try (QwpEgressProcessorState state = new QwpEgressProcessorState(cfg)) {
            Assert.assertEquals(0, state.findOrAllocateSchemaId(schemaOf("id", ColumnType.LONG)));
            Assert.assertEquals(1, state.findOrAllocateSchemaId(schemaOf("id", ColumnType.INT)));
            Assert.assertFalse(state.wasLastSchemaIdReuse());
        }
    }

    @Test
    public void testExhaustionReturnsSentinelForNewShapeButReusesExisting() {
        try (QwpEgressProcessorState state = new QwpEgressProcessorState(cfg)) {
            // Fill the map to the cap with distinct single-column schemas. Using
            // unique column names is the cheapest way to force a unique fingerprint
            // for each iteration. Runs in well under a second because nothing here
            // hits native memory or SQL compilation.
            int cap = QwpConstants.DEFAULT_MAX_SCHEMAS_PER_CONNECTION;
            int firstCachedId = -1;
            for (int i = 0; i < cap; i++) {
                int id = state.findOrAllocateSchemaId(schemaOf("c" + i, ColumnType.LONG));
                Assert.assertEquals("monotonic allocation up to the cap", i, id);
                Assert.assertFalse(state.wasLastSchemaIdReuse());
                if (i == 0) {
                    firstCachedId = id;
                }
            }

            // A new shape at the cap must return the sentinel, not advance the
            // counter past the client's rejection threshold, and must report the
            // most-recent call as NOT a reuse.
            int exhausted = state.findOrAllocateSchemaId(schemaOf("new", ColumnType.DOUBLE));
            Assert.assertEquals(QwpEgressProcessorState.SCHEMA_ID_EXHAUSTED, exhausted);
            Assert.assertFalse(state.wasLastSchemaIdReuse());

            // A shape already in the cache must still return its cached id --
            // queries against previously-seen schemas are not penalised by
            // exhaustion. This is the behaviour that keeps the connection usable
            // for long-lived dashboards after the cap is reached.
            int reusedFirst = state.findOrAllocateSchemaId(schemaOf("c0", ColumnType.LONG));
            Assert.assertEquals(firstCachedId, reusedFirst);
            Assert.assertTrue(state.wasLastSchemaIdReuse());
        }
    }

    @Test
    public void testFirstCallAllocatesIdZero() {
        try (QwpEgressProcessorState state = new QwpEgressProcessorState(cfg)) {
            int id = state.findOrAllocateSchemaId(schemaOf("id", ColumnType.LONG));
            Assert.assertEquals(0, id);
            Assert.assertFalse("first alloc is not a reuse", state.wasLastSchemaIdReuse());
        }
    }

    @Test
    public void testSameSchemaReturnsSameIdAndReuseFlag() {
        try (QwpEgressProcessorState state = new QwpEgressProcessorState(cfg)) {
            ObjList<QwpEgressColumnDef> cols = schemaOf("id", ColumnType.LONG, "ts", ColumnType.TIMESTAMP);
            int first = state.findOrAllocateSchemaId(cols);
            Assert.assertFalse(state.wasLastSchemaIdReuse());
            for (int i = 0; i < 3; i++) {
                int hit = state.findOrAllocateSchemaId(cols);
                Assert.assertEquals("id must be stable for identical shape", first, hit);
                Assert.assertTrue("repeat lookup must flag reuse", state.wasLastSchemaIdReuse());
            }
        }
    }

    private static ObjList<QwpEgressColumnDef> schemaOf(Object... nameTypePairs) {
        if ((nameTypePairs.length & 1) != 0) {
            throw new IllegalArgumentException("expected alternating name, type pairs");
        }
        ObjList<QwpEgressColumnDef> out = new ObjList<>();
        for (int i = 0; i < nameTypePairs.length; i += 2) {
            QwpEgressColumnDef def = new QwpEgressColumnDef();
            // ColumnType constants are declared as `short`; unbox via Number so
            // callers can pass them straight in (autoboxing gives us Short, not
            // Integer, so a direct (Integer) cast would ClassCastException).
            int type = ((Number) nameTypePairs[i + 1]).intValue();
            def.of((String) nameTypePairs[i], type);
            out.add(def);
        }
        return out;
    }
}
