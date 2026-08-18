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

package io.questdb.test.griffin;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.DefaultCairoConfiguration;
import io.questdb.griffin.CompiledQuery;
import io.questdb.griffin.ReadOnlyStatementGate;
import io.questdb.griffin.engine.ops.GenericDropOperation;
import io.questdb.griffin.engine.ops.Operation;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.HashSet;
import java.util.Set;
import java.util.TreeMap;

/**
 * Exhaustive matrix for ReadOnlyStatementGate.isRefusedOnReadOnly, generated from the gate's own type
 * list: the test enumerates every {@code short} constant declared on the {@link CompiledQuery}
 * interface via reflection (so a newly-added statement type is automatically pulled into the matrix
 * and forces an explicit refused/allowed classification rather than silently defaulting) and asserts
 * the gate's verdict for each.
 * <p>
 * The gate is the single source of truth the pg-wire and HTTP /exec read-only fences share; a type
 * omitted from it falls through to the protocol default and mutates a demoting node. The matrix pins
 * every type, plus the one DROP shape exempt from the gate -- the HTTP parquet-export temp-table DROP,
 * the single DROP a read-only replica still runs -- exercised both ways.
 * <p>
 * EXPECTED_REFUSED below is the assertion side of the matrix; it is NOT a second copy of the gate's
 * disjunction used as the implementation. The reflection sweep guarantees the matrix and the gate's
 * type universe cannot drift: if CompiledQuery gains a constant, this test fails until the constant is
 * classified here.
 */
public class ReadOnlyStatementGateMatrixTest extends AbstractCairoTest {

    /**
     * The statement types the gate must refuse on a read-only node. Every other declared CompiledQuery
     * type must be allowed. Keyed by the constant name so a reflection mismatch reports which type.
     */
    private static final Set<String> EXPECTED_REFUSED = new HashSet<>();

    static {
        EXPECTED_REFUSED.add("INSERT");
        EXPECTED_REFUSED.add("INSERT_AS_SELECT");
        EXPECTED_REFUSED.add("UPDATE");
        EXPECTED_REFUSED.add("ALTER");
        EXPECTED_REFUSED.add("TRUNCATE");
        EXPECTED_REFUSED.add("RENAME_TABLE");
        EXPECTED_REFUSED.add("CREATE_TABLE");
        EXPECTED_REFUSED.add("CREATE_TABLE_AS_SELECT");
        EXPECTED_REFUSED.add("CREATE_MAT_VIEW");
        EXPECTED_REFUSED.add("REFRESH_MAT_VIEW");
        EXPECTED_REFUSED.add("CREATE_VIEW");
        EXPECTED_REFUSED.add("CREATE_LIVE_VIEW");
        EXPECTED_REFUSED.add("ALTER_VIEW");
        EXPECTED_REFUSED.add("ALTER_STORAGE_POLICY");
        EXPECTED_REFUSED.add("DROP");
    }

    /**
     * The WAL DROP (DROP TABLE / DROP VIEW / DROP MATERIALIZED VIEW / DROP ALL TABLES) is classified
     * engine-fenced, NOT gate-fenced: the gate the pg-wire and HTTP /exec fences share only sees a DROP on
     * those two channels, but the QWP egress channel executes the compiled DROP directly and never consults
     * ReadOnlyStatementGate. All four DROP shapes converge on the one engine entry point
     * CairoEngine.dropTableOrViewOrMatView, whose WAL branch mints the replicated drop, so the engine fence
     * on that method (the enterprise override) is the only barrier that covers every DROP shape across all
     * three channels. The gate still refuses a genuine client DROP on the pg-wire / HTTP channel (asserted
     * by testGateRefusesExactlyTheWriteTypes via EXPECTED_REFUSED), but that gate refusal is one channel
     * only -- it does not, and cannot, fence the QWP egress DROP. This test records the classification and
     * the gate-independence so a future change that mistakes the gate for the DROP's authoritative fence
     * reds the matrix.
     */
    @Test
    public void testDropIsEngineFencedNotGateFenced() throws Exception {
        CairoConfiguration cfg = newConfiguration();

        // The four WAL DROP shapes, enumerated individually so the matrix records that all four converge on
        // the one engine entry point (including the multi-table DROP ALL TABLES loop).
        String[] dropShapes = {"DROP TABLE", "DROP VIEW", "DROP MATERIALIZED VIEW", "DROP ALL TABLES"};
        Assert.assertEquals(
                "all four WAL DROP shapes must be enumerated -- DROP TABLE / VIEW / MATERIALIZED VIEW /"
                        + " ALL TABLES",
                4, dropShapes.length
        );

        // The single engine entry point all four shapes converge on -- the engine-fence chokepoint the
        // enterprise override wraps in the role-switch read lock. The QWP egress channel reaches this mint
        // without consulting the gate, so the engine fence here is the only catch-all.
        Assert.assertNotNull(
                "CairoEngine.dropTableOrViewOrMatView must exist -- the single engine entry point the WAL"
                        + " DROP shapes converge on, fenced by the enterprise override (the QWP egress DROP"
                        + " never consults this gate)",
                io.questdb.cairo.CairoEngine.class.getDeclaredMethod(
                        "dropTableOrViewOrMatView",
                        io.questdb.std.str.Path.class,
                        io.questdb.cairo.TableToken.class
                )
        );

        // On the pg-wire / HTTP channel the gate still refuses a genuine client DROP (a null operation) on a
        // read-only node, for every DROP shape. This is the channel the gate covers; the QWP egress DROP is
        // engine-fenced, not gate-fenced.
        for (String dropShape : dropShapes) {
            Assert.assertTrue(
                    "the " + dropShape + " shape mints a replicated WAL drop, so the live gate must refuse"
                            + " DROP on the pg-wire / HTTP channel on a read-only node",
                    ReadOnlyStatementGate.isRefusedOnReadOnly(CompiledQuery.DROP, null, cfg)
            );
        }
    }

    /**
     * The export-temp-table DROP exemption, exercised both ways: a DROP whose operation is the parquet
     * exporter's temp-table cleanup must be allowed on a read-only node (the one DROP a replica runs),
     * while every genuine client DROP (a non-export operation, or a null operation) stays refused.
     */
    @Test
    public void testExportTempTableDropExemption() throws Exception {
        CairoConfiguration cfg = newConfiguration();
        String prefix = cfg.getParquetExportTableNamePrefix().toString();

        // A genuine client DROP (null operation) stays refused.
        Assert.assertTrue(
                "a genuine client DROP must be refused on a read-only node",
                ReadOnlyStatementGate.isRefusedOnReadOnly(CompiledQuery.DROP, null, cfg)
        );

        // A DROP targeting an ordinary client table (whose name does NOT carry the export prefix)
        // stays refused.
        Assert.assertFalse(
                "fixture sanity: the client table name must not carry the export prefix",
                ("users_orders").startsWith(prefix)
        );
        Operation nonExportDrop = dropOperation("users_orders");
        Assert.assertTrue(
                "a DROP of a non-export table must be refused on a read-only node",
                ReadOnlyStatementGate.isRefusedOnReadOnly(CompiledQuery.DROP, nonExportDrop, cfg)
        );

        // A DROP of the parquet-export temp table is the one exempt DROP: allowed on a read-only node.
        Operation exportTempDrop = dropOperation(prefix + "1234");
        Assert.assertFalse(
                "the parquet-export temp-table DROP is exempt and must be allowed on a read-only node",
                ReadOnlyStatementGate.isRefusedOnReadOnly(CompiledQuery.DROP, exportTempDrop, cfg)
        );
    }

    /**
     * Iterates every declared CompiledQuery type and asserts the gate refuses exactly the types in
     * EXPECTED_REFUSED and allows every other. For the DROP type a null operation is passed (a genuine
     * client DROP, which is refused); the export-temp-table exemption has its own test above.
     */
    @Test
    public void testGateRefusesExactlyTheWriteTypes() throws Exception {
        CairoConfiguration cfg = newConfiguration();
        TreeMap<String, Short> declaredTypes = declaredCompiledQueryTypes();

        Assert.assertFalse("reflection must find the CompiledQuery type constants", declaredTypes.isEmpty());

        Set<String> seenRefused = new HashSet<>();
        for (var entry : declaredTypes.entrySet()) {
            String name = entry.getKey();
            short sqlType = entry.getValue();
            boolean refused = ReadOnlyStatementGate.isRefusedOnReadOnly(sqlType, null, cfg);
            boolean expectRefused = EXPECTED_REFUSED.contains(name);
            Assert.assertEquals(
                    "gate verdict mismatch for CompiledQuery." + name + " (=" + sqlType + ")"
                            + "; if this is a new statement type, classify it in EXPECTED_REFUSED and, if it"
                            + " writes, add it to ReadOnlyStatementGate.isRefusedOnReadOnly",
                    expectRefused,
                    refused
            );
            if (refused) {
                seenRefused.add(name);
            }
        }

        // Every name in EXPECTED_REFUSED must correspond to an actually-declared, actually-refused type
        // (catches a stale expectation referencing a removed/renamed constant).
        Assert.assertEquals(
                "EXPECTED_REFUSED must match the set the gate actually refuses",
                EXPECTED_REFUSED,
                seenRefused
        );
    }

    private static TreeMap<String, Short> declaredCompiledQueryTypes() throws IllegalAccessException {
        TreeMap<String, Short> out = new TreeMap<>();
        for (Field f : CompiledQuery.class.getDeclaredFields()) {
            if (f.getType() == short.class && Modifier.isStatic(f.getModifiers())) {
                String name = f.getName();
                // TYPES_COUNT and EMPTY are bookkeeping sentinels, not real statement types.
                if (name.equals("TYPES_COUNT") || name.equals("EMPTY")) {
                    continue;
                }
                out.put(name, f.getShort(null));
            }
        }
        return out;
    }

    private static Operation dropOperation(String tableName) {
        // GenericDropOperation.isExportTempTableDrop checks the entity name against the export prefix.
        return new GenericDropOperation(
                io.questdb.cairo.OperationCodes.DROP_TABLE,
                null,
                tableName,
                0,
                false
        );
    }

    private static CairoConfiguration newConfiguration() {
        return new DefaultCairoConfiguration(root);
    }
}
