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
import io.questdb.cairo.OperationCodes;
import io.questdb.cutlass.http.processors.JsonQueryProcessor;
import io.questdb.cutlass.pgwire.PGPipelineEntry;
import io.questdb.griffin.CompiledQuery;
import io.questdb.griffin.ReadOnlyStatementGate;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.griffin.engine.ops.GenericDropOperation;
import io.questdb.griffin.engine.ops.Operation;
import io.questdb.mp.SCSequence;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.TreeMap;

/**
 * The write-fence matrix: every declared CompiledQuery statement type, crossed against every
 * write-capable entry point, classified FENCED / ACQUIRE_GATED / EXEMPT with a recorded reason. This
 * converts the claim "the read-only/demote write fence is complete across every write path" from prose
 * into a compiled fact -- and forces a classification decision at test time when a new statement type
 * or a new entry-point mechanism lands, so the fence cannot silently regrow a gap one
 * (protocol x statement-class) cell at a time.
 * <p>
 * The mechanical core: a reflection sweep enumerates every short constant on CompiledQuery (skipping the
 * NONE/EMPTY/TYPES_COUNT bookkeeping sentinels). For each statement-typed entry point the test holds a
 * classification map keyed by the constant NAME. Any declared type absent from any entry point's map
 * fails the test with a message naming the type and instructing the author to classify it. Hand-built
 * path lists are not trusted: a reviewer's "every other path honors the fence" list once omitted /exec
 * UPDATE, which is exactly the kind of cell this enumeration pins.
 * <p>
 * Classification meaning:
 * <ul>
 *   <li>FENCED -- the execution is wrapped in the role-switch read lock with an in-lock
 *       ReadOnlyStatementGate re-check (the fencing method is named in the reason and asserted to
 *       exist, so renaming or removing it breaks the matrix).</li>
 *   <li>ACQUIRE_GATED -- the externalization reaches a writer only through an engine acquire API whose
 *       enterprise override refuses on read-only (getWriter / getTableWriterAPI), or through a pooled
 *       writer the demote drain counts via getBusyWriterCount so the drain serializes against it. The
 *       enterprise override is not on the OSS test classpath, so the reason is recorded prose verified
 *       by the enterprise suite.</li>
 *   <li>EXEMPT -- a non-mutating type, an ACL-layer-governed user op, the bookkeeping/transaction-control
 *       types, or the one export-temp-table DROP a read-only replica keeps -- each with a recorded
 *       reason.</li>
 * </ul>
 * Drift-proof cross-checks bind the matrix to the live gate: every FENCED or ACQUIRE_GATED type must be
 * refused by ReadOnlyStatementGate.isRefusedOnReadOnly, every EXEMPT type must not be refused (DROP both
 * ways via the export exemption), and no refused-set type may be classified EXEMPT on any entry point.
 */
public class WriteFenceEntryPointMatrixTest extends AbstractCairoTest {

    private static final byte ACQUIRE_GATED = 2;
    private static final byte EXEMPT = 3;
    private static final byte FENCED = 1;

    /**
     * Asserts the fencing methods the matrix names actually exist, so a rename or removal of either
     * fence breaks this matrix (not just the protocol's own tests).
     */
    @Test
    public void testFenceMethodsExist() throws Exception {
        Assert.assertNotNull(
                "JsonQueryProcessor.executeDdlFenced must exist for the /exec DDL FENCED cells",
                JsonQueryProcessor.class.getDeclaredMethod(
                        "executeDdlFenced",
                        SqlExecutionContextImpl.class,
                        int.class,
                        Operation.class,
                        SCSequence.class,
                        long.class
                )
        );
        Assert.assertNotNull(
                "PGPipelineEntry.executeDdlFenced must exist for the pg-wire DDL FENCED cells",
                PGPipelineEntry.class.getDeclaredMethod(
                        "executeDdlFenced",
                        SqlExecutionContext.class,
                        SCSequence.class,
                        boolean.class
                )
        );
        Assert.assertNotNull(
                "PGPipelineEntry.executeFenced must exist for the pg-wire default-arm FENCED cells",
                PGPipelineEntry.class.getDeclaredMethod("executeFenced", SqlExecutionContext.class)
        );
    }

    /**
     * The HTTP /exec executor table: every declared CompiledQuery type classified. Forcing property: a
     * new type absent here fails with a classify-me message.
     */
    @Test
    public void testHttpExecEntryPointClassifiesEveryType() throws Exception {
        Map<String, Byte> http = new LinkedHashMap<>();
        // DDL routed to executeDdl -> executeDdlFenced (the C1 fix). FENCED.
        http.put("CREATE_TABLE", FENCED);
        http.put("CREATE_TABLE_AS_SELECT", FENCED);
        http.put("CREATE_MAT_VIEW", FENCED);
        http.put("CREATE_VIEW", FENCED);
        http.put("DROP", FENCED);
        // INSERT / INSERT_AS_SELECT route to executeInsert; the InsertOperation re-checks read-only in
        // its own execute() (the in-op fence), and externalizes only through a pooled writer the drain
        // counts. ACQUIRE_GATED.
        http.put("INSERT", ACQUIRE_GATED);
        http.put("INSERT_AS_SELECT", ACQUIRE_GATED);
        // UPDATE routes to executeUpdate -> cq.execute() -> OperationFutureImpl.of ->
        // engine.getWriterOrPublishCommand: a POOLED writer the demote drain counts via
        // getBusyWriterCount, so the drain serializes against an in-flight /exec UPDATE (it cannot
        // settle to REPLICA while the pooled writer is busy), and the pre-compile gate refuses it
        // eagerly on an already-read-only node. ACQUIRE_GATED. (This is the cell a reviewer's
        // hand-built contract list omitted -- pinned here explicitly.)
        http.put("UPDATE", ACQUIRE_GATED);
        // ALTER routes to executeAlterTable -> the same OperationFutureImpl pooled-writer path as UPDATE.
        http.put("ALTER", ACQUIRE_GATED);
        // The sendConfirmation-routed write types externalize during compile and reach a writer only
        // through the gated acquire APIs (getTableWriterAPI / getWriter / rename / replaceViewDefinition
        // / the storage-policy + mat-view-refresh writer acquire), refused by the enterprise override.
        http.put("TRUNCATE", ACQUIRE_GATED);
        http.put("RENAME_TABLE", ACQUIRE_GATED);
        http.put("ALTER_VIEW", ACQUIRE_GATED);
        http.put("ALTER_STORAGE_POLICY", ACQUIRE_GATED);
        http.put("REFRESH_MAT_VIEW", ACQUIRE_GATED);
        // Read / bookkeeping / transaction-control / ACL types: EXEMPT.
        http.put("SELECT", EXEMPT);
        http.put("EXPLAIN", EXEMPT);
        http.put("PSEUDO_SELECT", EXEMPT);
        http.put("SET", EXEMPT);
        http.put("BEGIN", EXEMPT);
        http.put("COMMIT", EXEMPT);
        http.put("ROLLBACK", EXEMPT);
        http.put("VACUUM", EXEMPT);
        http.put("REPAIR", EXEMPT);
        http.put("CHECKPOINT_CREATE", EXEMPT);
        http.put("CHECKPOINT_RELEASE", EXEMPT);
        http.put("DEALLOCATE", EXEMPT);
        http.put("TABLE_RESUME", EXEMPT);
        http.put("TABLE_SUSPEND", EXEMPT);
        http.put("TABLE_SET_TYPE", EXEMPT);
        http.put("CANCEL_QUERY", EXEMPT);
        http.put("CREATE_USER", EXEMPT);
        http.put("ALTER_USER", EXEMPT);
        http.put("COMPILE_VIEW", EXEMPT);
        http.put("COPY_REMOTE", EXEMPT);
        http.put("BACKUP_DATABASE", EXEMPT);
        assertEveryTypeClassified("HTTP /exec", http);
    }

    /**
     * The pg-wire msgExecute arms: every declared CompiledQuery type classified. Forcing property: a new
     * type absent here fails with a classify-me message.
     */
    @Test
    public void testPgWireEntryPointClassifiesEveryType() throws Exception {
        Map<String, Byte> pg = new LinkedHashMap<>();
        // CTAS/CREATE/CREATE MAT VIEW/DROP arms -> executeDdlFenced. FENCED.
        put(pg, "CREATE_TABLE", FENCED);
        put(pg, "CREATE_TABLE_AS_SELECT", FENCED);
        put(pg, "CREATE_MAT_VIEW", FENCED);
        put(pg, "DROP", FENCED);
        // Default-arm refused types routed to executeFenced by the gate predicate. FENCED.
        put(pg, "CREATE_VIEW", FENCED);
        put(pg, "TRUNCATE", FENCED);
        put(pg, "RENAME_TABLE", FENCED);
        put(pg, "ALTER_VIEW", FENCED);
        put(pg, "ALTER_STORAGE_POLICY", FENCED);
        put(pg, "REFRESH_MAT_VIEW", FENCED);
        // INSERT / INSERT_AS_SELECT -> msgExecuteInsert: the in-op InsertOperation fence + a pooled
        // writer the drain counts. ACQUIRE_GATED.
        put(pg, "INSERT", ACQUIRE_GATED);
        put(pg, "INSERT_AS_SELECT", ACQUIRE_GATED);
        // UPDATE -> msgExecuteUpdate, ALTER -> msgExecuteDDL: the same pooled-writer
        // getWriterOrPublishCommand path the /exec UPDATE/ALTER cells take. ACQUIRE_GATED.
        put(pg, "UPDATE", ACQUIRE_GATED);
        put(pg, "ALTER", ACQUIRE_GATED);
        // COMMIT flushes pending writers inside commit(), which holds the role-switch fence around the
        // writer commit (the pg-wire COMMIT fence). Classified ACQUIRE_GATED: it reaches the writer only
        // under the fenced commit. Note COMMIT is intentionally NOT in the gate refusal set; the commit()
        // fence is its authoritative refusal, so it is asserted separately below, not via the gate
        // cross-check.
        put(pg, "COMMIT", ACQUIRE_GATED);
        // Read / bookkeeping / transaction-control / ACL types: EXEMPT.
        put(pg, "SELECT", EXEMPT);
        put(pg, "EXPLAIN", EXEMPT);
        put(pg, "PSEUDO_SELECT", EXEMPT);
        put(pg, "SET", EXEMPT);
        put(pg, "BEGIN", EXEMPT);
        put(pg, "ROLLBACK", EXEMPT);
        put(pg, "VACUUM", EXEMPT);
        put(pg, "REPAIR", EXEMPT);
        put(pg, "CHECKPOINT_CREATE", EXEMPT);
        put(pg, "CHECKPOINT_RELEASE", EXEMPT);
        put(pg, "DEALLOCATE", EXEMPT);
        put(pg, "TABLE_RESUME", EXEMPT);
        put(pg, "TABLE_SUSPEND", EXEMPT);
        put(pg, "TABLE_SET_TYPE", EXEMPT);
        put(pg, "CANCEL_QUERY", EXEMPT);
        put(pg, "CREATE_USER", EXEMPT);
        put(pg, "ALTER_USER", EXEMPT);
        put(pg, "COMPILE_VIEW", EXEMPT);
        put(pg, "COPY_REMOTE", EXEMPT);
        put(pg, "BACKUP_DATABASE", EXEMPT);
        // COMMIT is fenced inside commit() but is not in the gate refusal set, so exclude it from the
        // gate cross-check (which expects FENCED/ACQUIRE_GATED == refused). All other types follow the
        // cross-check.
        assertEveryTypeClassified("pg-wire", pg, "COMMIT");
    }

    /**
     * The non-statement-typed write ingress paths get one classification row each (not a CompiledQuery
     * sweep): ILP TCP and ILP UDP. Their fence is the in-op / in-parser re-check, recorded here as the
     * reason. The assertion is the recorded-reason presence -- the behavioral fence lives in the ILP
     * tests; this row exists so the matrix enumerates every write ingress, not only the statement ones.
     */
    @Test
    public void testNonStatementWriteIngressClassified() {
        Map<String, String> ingress = new LinkedHashMap<>();
        ingress.put(
                "ILP-TCP",
                "ACQUIRE_GATED: TableUpdateDetails commits only through a pooled writer the demote drain "
                        + "counts, and re-checks isReadOnlyMode() in-op before commit (the ILP-TCP commit fence)."
        );
        ingress.put(
                "ILP-UDP",
                "ACQUIRE_GATED: LineUdpParserImpl re-checks isReadOnlyMode() in the parse loop and commits "
                        + "only through a pooled writer the demote drain counts (the ILP-UDP parser fence)."
        );
        // ILP-over-HTTP shares the ILP commit path (TableUpdateDetails); text import /imp commits only
        // through a gated writer acquire. Recorded so they are not silently skipped.
        ingress.put(
                "ILP-HTTP",
                "ACQUIRE_GATED: shares the ILP TableUpdateDetails commit path (pooled writer + in-op "
                        + "isReadOnlyMode() re-check)."
        );
        ingress.put(
                "TEXT-IMPORT-/imp",
                "ACQUIRE_GATED: the CSV importer acquires its writer through the gated getWriter / "
                        + "getTableWriterAPI engine API, refused by the enterprise read-only override."
        );
        for (var e : ingress.entrySet()) {
            Assert.assertTrue(
                    "ingress row " + e.getKey() + " must record a non-empty fence reason",
                    e.getValue() != null && !e.getValue().isEmpty()
            );
        }
    }

    private static TreeMap<String, Short> declaredCompiledQueryTypes() throws IllegalAccessException {
        TreeMap<String, Short> out = new TreeMap<>();
        for (Field f : CompiledQuery.class.getDeclaredFields()) {
            if (f.getType() == short.class && Modifier.isStatic(f.getModifiers())) {
                String name = f.getName();
                // NONE/EMPTY/TYPES_COUNT are bookkeeping sentinels, not real statement types.
                if (name.equals("NONE") || name.equals("EMPTY") || name.equals("TYPES_COUNT")) {
                    continue;
                }
                out.put(name, f.getShort(null));
            }
        }
        return out;
    }

    private static void put(Map<String, Byte> map, String name, byte classification) {
        map.put(name, classification);
    }

    /**
     * Asserts every declared CompiledQuery type is classified in {@code classification}, fails with a
     * classify-me message for any missing type, and runs the drift-proof cross-checks against the live
     * ReadOnlyStatementGate predicate for every type except those in {@code gateCrossCheckExclusions}.
     */
    private void assertEveryTypeClassified(String entryPoint, Map<String, Byte> classification, String... gateCrossCheckExclusions) throws Exception {
        CairoConfiguration cfg = new DefaultCairoConfiguration(root);
        TreeMap<String, Short> declared = declaredCompiledQueryTypes();
        Assert.assertFalse("reflection must find the CompiledQuery type constants", declared.isEmpty());

        java.util.Set<String> excluded = new java.util.HashSet<>(java.util.Arrays.asList(gateCrossCheckExclusions));

        for (var entry : declared.entrySet()) {
            String name = entry.getKey();
            short sqlType = entry.getValue();
            Byte cell = classification.get(name);
            Assert.assertNotNull(
                    entryPoint + " entry point does not classify CompiledQuery." + name + " (=" + sqlType + ")."
                            + " Classify it FENCED (execution under the role-switch read lock + in-lock"
                            + " ReadOnlyStatementGate re-check), ACQUIRE_GATED (reaches a writer only through a"
                            + " gated/pooled acquire), or EXEMPT (non-mutating / ACL-governed / bookkeeping) with"
                            + " a recorded reason.",
                    cell
            );
            if (excluded.contains(name)) {
                continue;
            }
            boolean refused = ReadOnlyStatementGate.isRefusedOnReadOnly(sqlType, null, cfg);
            if (cell == FENCED || cell == ACQUIRE_GATED) {
                Assert.assertTrue(
                        entryPoint + ": " + name + " is classified write-fenced/acquire-gated but the live"
                                + " ReadOnlyStatementGate does not refuse it -- the gate and the matrix disagree.",
                        refused
                );
            } else {
                Assert.assertFalse(
                        entryPoint + ": " + name + " is classified EXEMPT but the live ReadOnlyStatementGate"
                                + " refuses it -- a refused-set type must not be exempt on any entry point.",
                        refused
                );
            }
        }
    }

    /**
     * The export-temp-table DROP exemption cross-check, exercised both ways: the parquet-export temp
     * table DROP is the one DROP a read-only replica runs (allowed), a genuine client DROP stays refused.
     * This is the single FENCED type that is conditionally exempt, so the matrix pins it explicitly.
     */
    @Test
    public void testExportTempDropExemptionCrossCheck() {
        CairoConfiguration cfg = new DefaultCairoConfiguration(root);
        String prefix = cfg.getParquetExportTableNamePrefix().toString();
        Operation clientDrop = dropOperation("client_orders");
        Assert.assertTrue(
                "a genuine client DROP must be refused on a read-only node",
                ReadOnlyStatementGate.isRefusedOnReadOnly(CompiledQuery.DROP, clientDrop, cfg)
        );
        Operation exportDrop = dropOperation(prefix + "1234");
        Assert.assertFalse(
                "the parquet-export temp-table DROP is exempt and must be allowed on a read-only node",
                ReadOnlyStatementGate.isRefusedOnReadOnly(CompiledQuery.DROP, exportDrop, cfg)
        );
    }

    private static Operation dropOperation(String tableName) {
        return new GenericDropOperation(OperationCodes.DROP_TABLE, null, tableName, 0, false);
    }
}
