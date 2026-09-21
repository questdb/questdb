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

package io.questdb.test.griffin.engine.join;

import io.questdb.PropertyKey;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.engine.join.SharedRecordCursorFactory;
import io.questdb.griffin.engine.table.parquet.PartitionDescriptor;
import io.questdb.griffin.engine.table.parquet.PartitionEncoder;
import io.questdb.std.Files;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Direct contract test for {@link SharedRecordCursorFactory#usesExternalDataSource()}.
 * <p>
 * This override cannot be pinned end-to-end through the materialized-view rejection guard.
 * A shared factory never owns its primary - as its class javadoc states, "ownership belongs to
 * the node in the main factory tree that holds the primary factory directly" - so the very same
 * primary is always reachable by a second, direct route (the lateral join's outer branch), and
 * that route propagates the property on its own. Any {@code CREATE MATERIALIZED VIEW} probe is
 * therefore rejected with or without this override, and would be a passenger test.
 * <p>
 * The override is still required: {@code usesExternalDataSource()} is a public contract on
 * {@link RecordCursorFactory}, and a wrapper that reports {@code false} while its primary reads
 * an external file is simply lying about itself. The lie is invisible to today's only consumer
 * purely by accident of tree shape; the next consumer that walks a sub-tree in isolation would
 * read it. This test asserts the contract on the factory itself, so the override cannot be
 * deleted silently.
 */
public class SharedRecordCursorFactoryExternalSourceTest extends AbstractCairoTest {

    @Before
    public void setUp() {
        super.setUp();
        inputRoot = root;
        setProperty(PropertyKey.DEV_MODE_ENABLED, "true");
    }

    @Test
    public void testSharedFactoryReportsPrimaryExternalSourceProperty() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table xs as (select rnd_symbol('a','b') sym, x::double v2, (x * 1_000_000)::timestamp ts from long_sequence(10))");
            encodeTable("xs", "xs.parquet");

            // positive: primary reads an external file -> the shared wrapper must say so too
            assertSharedMirrorsPrimary("select sym, sum(v2) as total from read_parquet('xs.parquet') group by sym", true);
            // negative: a database-only primary must not be reported as external
            assertSharedMirrorsPrimary("select sym, sum(v2) as total from xs group by sym", false);
        });
    }

    private static void assertSharedMirrorsPrimary(String sql, boolean expected) throws Exception {
        // SharedRecordCursorFactory does not own its primary, so ownership stays with the
        // compiled tree here, exactly as it does in production.
        try (RecordCursorFactory compiled = select(sql)) {
            // select() wraps the plan in QueryProgress; production shares the group-by underneath
            // it, so unwrap one level to build the same pairing the code generator builds.
            final RecordCursorFactory primary = compiled.getBaseFactory();
            Assert.assertNotNull("precondition: expected a QueryProgress wrapper to unwrap", primary);
            Assert.assertEquals(
                    "precondition: the primary factory itself must report the external-source property",
                    expected,
                    primary.usesExternalDataSource()
            );
            Assert.assertTrue(
                    "precondition: this shape must be shareable, otherwise no SharedRecordCursorFactory is built",
                    primary.supportsSharedCursors()
            );
            final SharedRecordCursorFactory shared = new SharedRecordCursorFactory(primary, 0);
            Assert.assertEquals(
                    "SharedRecordCursorFactory must propagate the external-source property of its "
                            + "primary; it holds the primary without exposing it through getBaseFactory(), "
                            + "so the fail-open default would silently report false",
                    expected,
                    shared.usesExternalDataSource()
            );
        }
    }

    private static void encodeTable(CharSequence tableName, CharSequence fileName) {
        try (
                Path path = new Path();
                PartitionDescriptor descriptor = new PartitionDescriptor();
                TableReader reader = engine.getReader(tableName)
        ) {
            path.of(root).concat(fileName);
            engine.getConfiguration().getFilesFacade().remove(path.$());
            PartitionEncoder.populateFromTableReader(reader, descriptor, 0);
            PartitionEncoder.encode(descriptor, path);
            Assert.assertTrue(Files.exists(path.$()));
        }
    }
}
