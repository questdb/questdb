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

package io.questdb.test.griffin.engine.join;

import io.questdb.cairo.AbstractRecordCursorFactory;
import io.questdb.cairo.GenericRecordMetadata;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.EmptyTableRandomRecordCursor;
import io.questdb.griffin.engine.functions.BooleanFunction;
import io.questdb.griffin.engine.join.SharedPlaces;
import io.questdb.griffin.engine.join.SharedPrimary;
import io.questdb.griffin.engine.join.SharedRecordCursorFactory;
import io.questdb.griffin.engine.table.RuntimeConstGateRecordCursorFactory;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

/**
 * Pins which runtime constant gates open their base while false, see {@link SharedPlaces}: a gate does when its
 * sub-tree holds the place that reads the cursor of a shared primary, its home, while a reader outside the sub-tree
 * reads the state of the primary. Each test records the places of a tree in build order, the way the code generator
 * does, hands the primaries over and resolves the gates, then opens each gate while its constant is false and counts
 * the opens of its base.
 */
public class SharedPlacesTest extends AbstractCairoTest {

    @Test
    public void testGateOverDroppedHomeOpensNothing() throws Exception {
        // the code generator drops the home, so handOver() promotes the first reader, which is outside the gate
        final SharedPlaces places = new SharedPlaces();
        final SharedPrimary primary = new SharedPrimary(new PlaceFactory());
        final SharedRecordCursorFactory outsideReader = addReader(places, primary, 1);
        addHome(places, primary);
        final SharedRecordCursorFactory insideReader = addReader(places, primary, 2);
        final Gate gate = addGate(places, 1);
        primary.getFactory().close();
        primary.handOver();
        places.resolveGates(0, 0);
        assertOpens(gate, false);
        insideReader.close();
        outsideReader.close();
    }

    @Test
    public void testGateOverEveryPlaceOpensNothing() throws Exception {
        // a WHERE condition over the join level that holds the GROUP BY and the LATERAL that reads it
        final SharedPlaces places = new SharedPlaces();
        final SharedPrimary primary = new SharedPrimary(new PlaceFactory());
        addHome(places, primary);
        final SharedRecordCursorFactory reader = addReader(places, primary, 1);
        final Gate gate = addGate(places, 0);
        primary.handOver();
        places.resolveGates(0, 0);
        assertOpens(gate, false);
        reader.close();
        primary.getFactory().close();
    }

    @Test
    public void testGateOverHomeOfDroppedReaderOpensNothing() throws Exception {
        final SharedPlaces places = new SharedPlaces();
        final SharedPrimary primary = new SharedPrimary(new PlaceFactory());
        addHome(places, primary);
        final Gate gate = addGate(places, 0);
        addReader(places, primary, 1).close();
        primary.handOver();
        places.resolveGates(0, 0);
        assertOpens(gate, false);
        primary.getFactory().close();
    }

    @Test
    public void testGateOverHomeOfOtherPrimaryOpensNothing() throws Exception {
        // the reader outside the gate reads another primary
        final SharedPlaces places = new SharedPlaces();
        final SharedPrimary primary = new SharedPrimary(new PlaceFactory());
        final SharedPrimary otherPrimary = new SharedPrimary(new PlaceFactory());
        addHome(places, otherPrimary);
        addHome(places, primary);
        final Gate gate = addGate(places, 1);
        final SharedRecordCursorFactory reader = addReader(places, otherPrimary, 1);
        primary.handOver();
        otherPrimary.handOver();
        places.resolveGates(0, 0);
        assertOpens(gate, false);
        reader.close();
        primary.getFactory().close();
        otherPrimary.getFactory().close();
    }

    @Test
    public void testGateOverHomeOpensForReaderBuiltAfter() throws Exception {
        // a gate in the ON clause of the GROUP BY that a LATERAL written after it reads
        final SharedPlaces places = new SharedPlaces();
        final SharedPrimary primary = new SharedPrimary(new PlaceFactory());
        addHome(places, primary);
        final Gate gate = addGate(places, 0);
        final SharedRecordCursorFactory reader = addReader(places, primary, 1);
        primary.handOver();
        places.resolveGates(0, 0);
        assertOpens(gate, true);
        reader.close();
        primary.getFactory().close();
    }

    @Test
    public void testGateOverHomeOpensForReaderBuiltBefore() throws Exception {
        // the join planner put the LATERAL before the GROUP BY that it reads
        final SharedPlaces places = new SharedPlaces();
        final SharedPrimary primary = new SharedPrimary(new PlaceFactory());
        final SharedRecordCursorFactory reader = addReader(places, primary, 1);
        addHome(places, primary);
        final Gate gate = addGate(places, 1);
        primary.handOver();
        places.resolveGates(0, 0);
        assertOpens(gate, true);
        reader.close();
        primary.getFactory().close();
    }

    @Test
    public void testGateOverPrimaryOfDroppedPlacesOpensNothing() throws Exception {
        // the code generator dropped every place of the primary, which closed it
        final SharedPlaces places = new SharedPlaces();
        final SharedPrimary primary = new SharedPrimary(new PlaceFactory());
        addHome(places, primary);
        final SharedRecordCursorFactory reader = addReader(places, primary, 1);
        final Gate gate = addGate(places, 0);
        reader.close();
        primary.getFactory().close();
        Assert.assertTrue(primary.isClosed());
        primary.handOver();
        places.resolveGates(0, 0);
        assertOpens(gate, false);
    }

    @Test
    public void testGateOverPromotedReaderOpensForReaderOutside() throws Exception {
        // the code generator drops the home, so handOver() promotes the reader under the gate, the first one, and the
        // reader after the gate reads its state
        final SharedPlaces places = new SharedPlaces();
        final SharedPrimary primary = new SharedPrimary(new PlaceFactory());
        addHome(places, primary);
        final SharedRecordCursorFactory promotedReader = addReader(places, primary, 1);
        final Gate gate = addGate(places, 1);
        final SharedRecordCursorFactory reader = addReader(places, primary, 2);
        primary.getFactory().close();
        primary.handOver();
        places.resolveGates(0, 0);
        assertOpens(gate, true);
        reader.close();
        promotedReader.close();
    }

    @Test
    public void testGateOverReaderOpensNothing() throws Exception {
        // the home outside the gate opens the cursor of the primary on its own
        final SharedPlaces places = new SharedPlaces();
        final SharedPrimary primary = new SharedPrimary(new PlaceFactory());
        addHome(places, primary);
        final SharedRecordCursorFactory reader = addReader(places, primary, 1);
        final Gate gate = addGate(places, 1);
        primary.handOver();
        places.resolveGates(0, 0);
        assertOpens(gate, false);
        reader.close();
        primary.getFactory().close();
    }

    @Test
    public void testGateWithoutPlaceIsNotRecorded() throws Exception {
        final SharedPlaces places = new SharedPlaces();
        final SharedPrimary primary = new SharedPrimary(new PlaceFactory());
        addHome(places, primary);
        final Gate gate = addGate(places, 1);
        Assert.assertEquals(0, places.getGateCount());
        primary.handOver();
        places.resolveGates(0, 0);
        assertOpens(gate, false);
        primary.getFactory().close();
    }

    @Test
    public void testNestedGenerationResolvesItsOwnGates() throws Exception {
        // The generation of a scalar sub-query, nested in the generation of the tree, resolves and forgets its own
        // places and gates when it ends. The gates of the tree then see only the places of the tree.
        final SharedPlaces places = new SharedPlaces();
        final SharedPrimary primary = new SharedPrimary(new PlaceFactory());
        addHome(places, primary);
        final Gate gate = addGate(places, 0);

        final int nestedPlaceMark = places.getPlaceCount();
        final int nestedGateMark = places.getGateCount();
        final SharedPrimary nestedPrimary = new SharedPrimary(new PlaceFactory());
        addHome(places, nestedPrimary);
        final Gate nestedGate = addGate(places, nestedPlaceMark);
        final SharedRecordCursorFactory nestedReader = addReader(places, nestedPrimary, 1);
        nestedPrimary.handOver();
        places.resolveGates(nestedPlaceMark, nestedGateMark);
        places.truncate(nestedPlaceMark, nestedGateMark);
        Assert.assertEquals(1, places.getPlaceCount());
        Assert.assertEquals(1, places.getGateCount());
        assertOpens(nestedGate, true);

        final SharedRecordCursorFactory reader = addReader(places, primary, 1);
        primary.handOver();
        places.resolveGates(0, 0);
        places.truncate(0, 0);
        Assert.assertEquals(0, places.getPlaceCount());
        Assert.assertEquals(0, places.getGateCount());
        assertOpens(gate, true);

        nestedReader.close();
        nestedPrimary.getFactory().close();
        reader.close();
        primary.getFactory().close();
    }

    private static Gate addGate(SharedPlaces places, int placeMark) {
        final PlaceFactory base = new PlaceFactory();
        return new Gate(places.addGate(new RuntimeConstGateRecordCursorFactory(base, new FalseFilter(), null), placeMark), base);
    }

    private static void addHome(SharedPlaces places, SharedPrimary primary) {
        primary.addHome();
        places.addHome(primary);
    }

    private static SharedRecordCursorFactory addReader(SharedPlaces places, SharedPrimary primary, int sharedId) {
        final SharedRecordCursorFactory reader = new SharedRecordCursorFactory(primary, sharedId);
        places.addReader(primary, reader);
        return reader;
    }

    private static void assertOpens(Gate gate, boolean isBaseOpened) throws Exception {
        try (RecordCursor cursor = gate.factory.getCursor(sqlExecutionContext)) {
            Assert.assertFalse(cursor.hasNext());
        }
        Assert.assertEquals("the gate opens its base while false", isBaseOpened ? 1 : 0, gate.base.openCount);
        gate.factory.close();
    }

    private static final class FalseFilter extends BooleanFunction {
        @Override
        public boolean getBool(Record rec) {
            return false;
        }

        @Override
        public boolean isRuntimeConstant() {
            return true;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(false);
        }
    }

    private record Gate(RuntimeConstGateRecordCursorFactory factory, PlaceFactory base) {
    }

    // a factory whose cursor is empty, which counts its opens
    private static final class PlaceFactory extends AbstractRecordCursorFactory {
        int openCount;

        private PlaceFactory() {
            super(new GenericRecordMetadata());
        }

        @Override
        public RecordCursor getCursor(SqlExecutionContext executionContext) {
            openCount++;
            return EmptyTableRandomRecordCursor.INSTANCE;
        }

        @Override
        public boolean recordCursorSupportsRandomAccess() {
            return true;
        }
    }
}
