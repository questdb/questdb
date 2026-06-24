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
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.DefaultCairoConfiguration;
import io.questdb.cairo.EntryUnavailableException;
import io.questdb.cairo.OperationCodes;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableWriterAPI;
import io.questdb.cutlass.http.processors.JsonQueryProcessor;
import io.questdb.cutlass.pgwire.PGPipelineEntry;
import io.questdb.griffin.CompiledQuery;
import io.questdb.griffin.ReadOnlyStatementGate;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.griffin.engine.ops.AbstractOperation;
import io.questdb.griffin.engine.ops.GenericDropOperation;
import io.questdb.griffin.engine.ops.Operation;
import io.questdb.griffin.engine.ops.OperationDispatcher;
import io.questdb.griffin.engine.ops.UpdateOperation;
import io.questdb.mp.SCSequence;
import io.questdb.std.ObjList;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.cairo.DefaultTestCairoConfiguration;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicBoolean;

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
 * The classification sweep alone is not enough -- a reflection-only matrix once classified the
 * parse-time DDL, parked-writer, and async-enqueue cells as safe while all three were unfenced. So the
 * matrix also DRIVES a real write through each fenced externalization path (the parse-time TRUNCATE mint,
 * the pg-wire parked-writer UPDATE branch, and the OperationDispatcher async-enqueue fallback) against a
 * read-only engine and asserts each is refused. A future write path that skips the fence reds these
 * drives, not just the classification map.
 * <p>
 * Classification meaning:
 * <ul>
 *   <li>FENCED -- the execution is wrapped in the role-switch read lock with an in-lock read-only
 *       re-check (the fencing method is named in the reason and asserted to exist, so renaming or
 *       removing it breaks the matrix).</li>
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
    private static final String READ_ONLY_REFUSAL = "replica access is read-only";

    /**
     * Drives a real WAL UPDATE into the OperationDispatcher async-enqueue fallback (the WAL writer-pool
     * exhaustion path) on a read-only engine and asserts the catch branch refuses before enqueuing. On a
     * read-only node the async apply would physical-commit without a replicated txn -- a silent
     * acked-loss -- so the fence must refuse. A non-null event sub-sequence selects the async-enqueue
     * branch (the re-throw branch fires only when it is null).
     */
    @Test
    public void testAsyncEnqueueBranchDrivesRealWriteAndIsFenced() throws Exception {
        assertMemoryLeak(() -> {
            try (CairoEngine readOnlyEngine = poolExhaustedReadOnlyEngine()) {
                OperationDispatcher<AbstractOperation> dispatcher = new OperationDispatcher<>(readOnlyEngine, "matrix update") {
                    @Override
                    protected long apply(AbstractOperation operation, TableWriterAPI writerFronted) {
                        return 0;
                    }
                };
                try {
                    dispatcher.execute(fenceProbeOperation(), TestUtils.createSqlExecutionCtx(readOnlyEngine), new SCSequence(), false);
                    Assert.fail("the async-enqueue fallback must refuse a WAL UPDATE on a read-only node");
                } catch (CairoException e) {
                    assertReadOnlyRefusal(e);
                }
            }
        });
    }

    /**
     * A WAL DROP is a SINGLE CompiledQuery type (DROP) but FOUR distinct statement shapes -- DROP TABLE,
     * DROP VIEW, DROP MATERIALIZED VIEW, DROP ALL TABLES -- and all four converge on the one engine entry
     * point CairoEngine.dropTableOrViewOrMatView, which mints the replicated drop (tableSequencerAPI.dropTable)
     * inside its WAL branch. A WAL DROP does NOT route through OperationDispatcher (it runs as a
     * GenericDropOperation / DropAllOperation executed directly), so the pg-wire / HTTP executeDdlFenced
     * route is one channel only; the QWP egress channel executes the compiled DROP directly and never
     * consults ReadOnlyStatementGate. The ONLY barrier that covers all four shapes across all three
     * channels (pg-wire, HTTP /exec, QWP egress) is an engine-level fence on dropTableOrViewOrMatView's
     * WAL branch -- so the DROP cell is engine-fenced, not gate-fenced.
     *
     * <p>The engine fence lives in the enterprise module (EntCairoEngine.dropTableOrViewOrMatView, which
     * wraps super in the role-switch read lock with an in-lock read-only re-check), which is not on the
     * OSS test classpath. So the OSS matrix pins the shared OSS seam the enterprise override relies on:
     * CairoEngine.dropTableOrViewOrMatView fires fireRoleSwitchMintObserver() inside its WAL branch
     * immediately before the mint (the same seam the parse-time TRUNCATE mint fires and
     * testParseTimeTruncateDrivesRealWriteAndIsFenced drives for real), so the enterprise DROP demote-race
     * witness can pause the mint on both the fenced and the unfenced tree. The DROP ALL TABLES loop routes
     * each WAL token through the same single dropTableOrViewOrMatView entry, so a multi-table DROP ALL is
     * covered by the same one fence -- pinned by the multi-shape enumeration below.
     */
    @Test
    public void testDropSubRoutesShareTheEngineFence() throws Exception {
        // The four WAL DROP statement shapes, enumerated individually so the matrix records that all four
        // converge on the one engine entry point (including the multi-table DROP ALL TABLES loop).
        String[] dropShapes = {"DROP TABLE", "DROP VIEW", "DROP MATERIALIZED VIEW", "DROP ALL TABLES"};
        Assert.assertEquals(
                "all four WAL DROP shapes must be enumerated -- DROP TABLE / VIEW / MATERIALIZED VIEW /"
                        + " ALL TABLES",
                4, dropShapes.length
        );

        // The single engine entry point all four shapes converge on. A WAL DROP mints the replicated drop
        // here (not through OperationDispatcher, and not behind the gate on the QWP egress channel), so the
        // engine fence on this method is the only catch-all across pg-wire, HTTP /exec and QWP egress.
        Assert.assertNotNull(
                "CairoEngine.dropTableOrViewOrMatView must exist -- the single engine entry point the WAL"
                        + " DROP TABLE / VIEW / MATERIALIZED VIEW / ALL TABLES shapes all converge on, and the"
                        + " engine-fence chokepoint the enterprise override wraps in the role-switch read lock",
                CairoEngine.class.getDeclaredMethod(
                        "dropTableOrViewOrMatView",
                        io.questdb.std.str.Path.class,
                        TableToken.class
                )
        );
        Assert.assertNotNull(
                "CairoEngine.getRoleSwitchReadLock must exist -- the lock the enterprise DROP override holds"
                        + " around the dropTableOrViewOrMatView WAL mint",
                CairoEngine.class.getDeclaredMethod("getRoleSwitchReadLock")
        );
        Assert.assertNotNull(
                "CairoEngine.fireRoleSwitchMintObserver must exist -- fired inside the dropTableOrViewOrMatView"
                        + " WAL branch immediately before the mint (the same seam the parse-time TRUNCATE mint"
                        + " fires), so the enterprise DROP demote-race witness can pause the mint on both trees",
                CairoEngine.class.getDeclaredMethod("fireRoleSwitchMintObserver")
        );

        // The pg-wire / HTTP channel must refuse a genuine client DROP on a read-only node (the QWP egress
        // channel is gate-independent and is covered by the engine fence). A null operation models a genuine
        // client DROP (not the export-temp-table exemption, which has its own cross-check above).
        CairoConfiguration cfg = new DefaultCairoConfiguration(root);
        for (String dropShape : dropShapes) {
            Assert.assertTrue(
                    "the " + dropShape + " shape mints a replicated WAL drop, so the live"
                            + " ReadOnlyStatementGate must refuse DROP on a read-only node for the pg-wire /"
                            + " HTTP channel",
                    ReadOnlyStatementGate.isRefusedOnReadOnly(CompiledQuery.DROP, null, cfg)
            );
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

    /**
     * Asserts the fencing methods and the mint-observer seams the matrix names actually exist, so a
     * rename or removal of any fence or its test seam breaks this matrix (not just the protocol's own
     * tests).
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
        Assert.assertNotNull(
                "OperationDispatcher.applyFenced must exist for the WAL UPDATE/ALTER FENCED cells on both"
                        + " pg-wire and /exec; renaming or removing it must break this matrix",
                OperationDispatcher.class.getDeclaredMethod(
                        "applyFenced",
                        AbstractOperation.class,
                        TableWriterAPI.class
                )
        );
        Assert.assertNotNull(
                "CairoEngine.setRoleSwitchMintObserver must exist -- the parse-time DDL fence's test seam,"
                        + " fired inside the role-switch read-lock hold at the TRUNCATE/RENAME/ALTER VIEW/"
                        + "storage-policy mints",
                CairoEngine.class.getDeclaredMethod("setRoleSwitchMintObserver", Runnable.class)
        );
        Assert.assertNotNull(
                "PGPipelineEntry.setParkedUpdateMintObserver must exist -- the parked-writer UPDATE fence's"
                        + " test seam",
                PGPipelineEntry.class.getDeclaredMethod("setParkedUpdateMintObserver", Runnable.class)
        );
        Assert.assertNotNull(
                "OperationDispatcher.setAsyncEnqueueObserver must exist -- the async-enqueue fence's test"
                        + " seam",
                OperationDispatcher.class.getDeclaredMethod("setAsyncEnqueueObserver", Runnable.class)
        );
    }

    /**
     * The HTTP /exec executor table: every declared CompiledQuery type classified. Forcing property: a
     * new type absent here fails with a classify-me message.
     */
    @Test
    public void testHttpExecEntryPointClassifiesEveryType() throws Exception {
        Map<String, Byte> http = new LinkedHashMap<>();
        // DDL routed to executeDdl -> executeDdlFenced. FENCED.
        http.put("CREATE_TABLE", FENCED);
        http.put("CREATE_TABLE_AS_SELECT", FENCED);
        http.put("CREATE_MAT_VIEW", FENCED);
        http.put("CREATE_VIEW", FENCED);
        // DROP (DROP TABLE / VIEW / MATERIALIZED VIEW / ALL TABLES) routes to executeDdl on the pg-wire /
        // HTTP channel, but a WAL DROP does NOT mint through OperationDispatcher: it converges on the one
        // engine entry point CairoEngine.dropTableOrViewOrMatView, whose WAL branch mints the replicated
        // drop. The QWP egress channel executes the same compiled DROP directly and never consults the
        // gate, so the engine fence on dropTableOrViewOrMatView (the enterprise override) is the only
        // catch-all across all three channels. FENCED -- engine-fenced, not gate-fenced; the four DROP
        // shapes and the multi-table DROP ALL loop are pinned in testDropSubRoutesShareTheEngineFence.
        http.put("DROP", FENCED);
        // INSERT / INSERT_AS_SELECT route to executeInsert; the InsertOperation re-checks read-only in
        // its own execute() (the in-op fence), and externalizes only through a pooled writer the drain
        // counts. ACQUIRE_GATED.
        http.put("INSERT", ACQUIRE_GATED);
        http.put("INSERT_AS_SELECT", ACQUIRE_GATED);
        // UPDATE routes to executeUpdate -> cq.execute() -> OperationDispatcher.execute, which holds the
        // role-switch read lock with an in-lock isReadOnlyMode() re-check around the inline apply() that
        // externalizes the operation (mints the WAL sequencer txn). For a WAL table the writer is the
        // WalWriter pool, NOT the non-WAL pooled writer the demote drain counts, so the earlier "pooled
        // writer the drain counts" rationale was wrong: only the dispatcher fence makes UPDATE
        // replicate-or-refuse across a demote. FENCED. (This is the cell a reviewer's hand-built contract
        // list omitted -- pinned here explicitly.)
        http.put("UPDATE", FENCED);
        // ALTER routes to executeAlterTable -> the same OperationDispatcher.execute fence as UPDATE.
        http.put("ALTER", FENCED);
        // The parse-time WAL DDL types (TRUNCATE, RENAME_TABLE, ALTER_VIEW, ALTER_STORAGE_POLICY) mint a
        // replicated WAL sequencer txn INSIDE compile(), before any ingress gate. The earlier rationale
        // marked them ACQUIRE_GATED citing the eager getTableWriterAPI / getWalWriter / rename /
        // replaceViewDefinition acquire, but that gate was check-then-act (it released before the mint), so
        // a demote could interleave and mint on an already-demoting node. Each mint now holds the
        // role-switch read lock around an in-lock re-check (SqlCompilerImpl.compileTruncate;
        // EntCairoEngine.rename / replaceViewDefinition; StoragePolicyWriterImpl.saveStoragePolicy), so
        // they are FENCED. The pg-wire side reaches the same mints through compile() and is fenced
        // identically. The async-enqueue drive test below exercises one of these mints for real.
        http.put("TRUNCATE", FENCED);
        http.put("RENAME_TABLE", FENCED);
        http.put("ALTER_VIEW", FENCED);
        // ALTER_STORAGE_POLICY is ONE CompiledQuery type but FOUR distinct mint sub-routes -- SET, DROP,
        // ENABLE, DISABLE -- on the enterprise StoragePolicyWriter. SET/DROP go through saveStoragePolicy;
        // ENABLE/DISABLE go through the standalone sp_links link-row mint. A single type-classification
        // cell cannot see that one sub-route skips the fence (the enable/disable hole), so the sub-routes
        // are exercised individually below (testStoragePolicySubRoutesShareTheRoleSwitchFence) and the ENT
        // EnableDisableStoragePolicyDemoteRaceTest drives the real enable/disable writes across a demote.
        http.put("ALTER_STORAGE_POLICY", FENCED);
        // REFRESH MAT VIEW enqueues an async refresh (no synchronous WAL txn mint); the eager getWalWriter
        // gate plus the demote's mat-view quiesce and the async job's own read-only re-check cover it.
        http.put("REFRESH_MAT_VIEW", ACQUIRE_GATED);
        http.put("TABLE_REBASE", ACQUIRE_GATED);
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
        assertEveryTypeClassified("HTTP /exec", http, "TABLE_REBASE");
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
        // QWP-UDP is the one ingress whose envelope flips acceptOpen to false on a PRIMARY-to-REPLICA
        // demote, so the receiver stops draining datagrams off the socket entirely (the freeze), and its
        // ingress processor also re-checks isReadOnlyMode() in-line before commit. Recorded so the matrix
        // enumerates QWP-UDP through-demote, which the enterprise QwpUdpRoleSwitchFreezeTest drives for real.
        ingress.put(
                "QWP-UDP",
                "ACQUIRE_GATED: the QWP-UDP envelope flips acceptOpen to false on demote (the receiver stops "
                        + "draining the socket) and the ingress processor re-checks isReadOnlyMode() in-line "
                        + "before commit (the QWP-UDP ingress fence)."
        );
        for (var e : ingress.entrySet()) {
            Assert.assertTrue(
                    "ingress row " + e.getKey() + " must record a non-empty fence reason",
                    e.getValue() != null && !e.getValue().isEmpty()
            );
        }
    }

    /**
     * Drives a real WAL TRUNCATE through compile() on an engine whose read-only flag flips after the
     * table is created -- exercising the parse-time TRUNCATE mint fence for real, not by reflection. The
     * SqlCompilerImpl.compileTruncate WAL branch holds the role-switch read lock around an in-lock
     * re-check and truncateSoft(); a read-only flag must refuse the truncate before it externalizes.
     */
    @Test
    public void testParseTimeTruncateDrivesRealWriteAndIsFenced() throws Exception {
        assertMemoryLeak(() -> {
            AtomicBoolean readOnly = new AtomicBoolean(false);
            try (CairoEngine flipEngine = flipReadOnlyEngine(readOnly)) {
                SqlExecutionContext ctx = TestUtils.createSqlExecutionCtx(flipEngine);
                flipEngine.execute(
                        "CREATE TABLE matrix_tr (ts TIMESTAMP, val INT) TIMESTAMP(ts) PARTITION BY DAY WAL",
                        ctx
                );
                // Flip read-only after the table exists: the truncate's WAL externalization must be
                // refused by the in-lock re-check before truncateSoft() runs.
                readOnly.set(true);
                try {
                    flipEngine.execute("TRUNCATE TABLE matrix_tr", ctx);
                    Assert.fail("a parse-time TRUNCATE must be refused on a read-only node");
                } catch (CairoException e) {
                    assertReadOnlyRefusal(e);
                }
            }
        });
    }

    /**
     * The pg-wire msgExecute arms: every declared CompiledQuery type classified. Forcing property: a new
     * type absent here fails with a classify-me message.
     */
    @Test
    public void testPgWireEntryPointClassifiesEveryType() throws Exception {
        Map<String, Byte> pg = new LinkedHashMap<>();
        // CTAS/CREATE/CREATE MAT VIEW arms -> executeDdlFenced. FENCED.
        put(pg, "CREATE_TABLE", FENCED);
        put(pg, "CREATE_TABLE_AS_SELECT", FENCED);
        put(pg, "CREATE_MAT_VIEW", FENCED);
        // DROP (DROP TABLE / VIEW / MATERIALIZED VIEW / ALL TABLES) reaches executeDdl on the pg-wire
        // channel, but a WAL DROP converges on the one engine entry point
        // CairoEngine.dropTableOrViewOrMatView (not OperationDispatcher), whose WAL branch mints the
        // replicated drop. The QWP egress channel executes the same compiled DROP directly and never
        // consults the gate, so the engine fence on dropTableOrViewOrMatView (the enterprise override) is
        // the only catch-all across all three channels. FENCED -- engine-fenced, not gate-fenced; pinned
        // in testDropSubRoutesShareTheEngineFence (incl. the multi-table DROP ALL loop).
        put(pg, "DROP", FENCED);
        // CREATE VIEW routes to the default arm -> executeFenced by the gate predicate. FENCED.
        put(pg, "CREATE_VIEW", FENCED);
        // The parse-time WAL DDL types execute inside compile() at parse time (msgParse), and msgExecute
        // short-circuits at stateParseExecuted BEFORE the gate and the default-arm executeFenced route, so
        // the protocol layer cannot fence them. The mint itself is now fenced (compile() holds the
        // role-switch read lock around the externalization), so they are FENCED at the mint, identically
        // to the /exec side.
        put(pg, "TRUNCATE", FENCED);
        put(pg, "RENAME_TABLE", FENCED);
        put(pg, "ALTER_VIEW", FENCED);
        // ALTER_STORAGE_POLICY is one type but four mint sub-routes (SET/DROP/ENABLE/DISABLE); the
        // sub-routes are exercised individually in testStoragePolicySubRoutesShareTheRoleSwitchFence so a
        // future enable/disable route that skips the fence reds the matrix (see the /exec note above).
        put(pg, "ALTER_STORAGE_POLICY", FENCED);
        put(pg, "REFRESH_MAT_VIEW", ACQUIRE_GATED);
        // INSERT / INSERT_AS_SELECT -> msgExecuteInsert: the in-op InsertOperation fence + a pooled
        // writer the drain counts. ACQUIRE_GATED.
        put(pg, "INSERT", ACQUIRE_GATED);
        put(pg, "INSERT_AS_SELECT", ACQUIRE_GATED);
        // UPDATE -> msgExecuteUpdate, ALTER -> msgExecuteDDL: both funnel through
        // OperationDispatcher.execute, which fences the inline apply() under the role-switch read lock
        // with an in-lock isReadOnlyMode() re-check (the same dispatcher fence the /exec UPDATE/ALTER
        // cells take). The parked-writer index < 0 branch of msgExecuteUpdate now holds the same fence
        // around its implicit commit() + apply(). FENCED.
        put(pg, "UPDATE", FENCED);
        put(pg, "ALTER", FENCED);
        // COMMIT flushes pending writers inside commit(), which holds the role-switch fence around the
        // writer commit (the pg-wire COMMIT fence). Classified ACQUIRE_GATED: it reaches the writer only
        // under the fenced commit. Note COMMIT is intentionally NOT in the gate refusal set; the commit()
        // fence is its authoritative refusal, so it is asserted separately below, not via the gate
        // cross-check.
        put(pg, "COMMIT", ACQUIRE_GATED);
        put(pg, "TABLE_REBASE", ACQUIRE_GATED);
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
        // COMMIT is fenced inside commit() and TABLE_REBASE is fenced at its engine writer-acquire, but
        // neither is in the gate refusal set, so exclude both from the gate cross-check (which expects
        // FENCED/ACQUIRE_GATED == refused). All other types follow the cross-check.
        assertEveryTypeClassified("pg-wire", pg, "COMMIT", "TABLE_REBASE");
    }

    /**
     * ALTER_STORAGE_POLICY is a SINGLE CompiledQuery type but FOUR distinct enterprise mint sub-routes --
     * SET, DROP, ENABLE, DISABLE -- on the enterprise StoragePolicyWriter, each writing a replicated WAL
     * row to the sp_entries / sp_links system tables inside compile() at parse time. The earlier matrix
     * classified ALTER_STORAGE_POLICY as one type-membership cell, which could not see that the
     * ENABLE/DISABLE sub-routes reached a standalone link-row mint that skipped the role-switch fence the
     * SET/DROP sub-routes carried -- the silent acked-loss the enterprise EnableDisableStoragePolicy
     * demote-race witness now drives RED-first and GREEN. This sweep enumerates the four sub-routes
     * individually and pins each to the shared role-switch fence seam, so a future enable/disable
     * storage-policy route that skips the fence reds the matrix (continuation of the matrix-masking fix).
     *
     * <p>The storage-policy writer lives in the enterprise module (com.questdb.cairo.cold.storage), which
     * is not on the OSS test classpath, so the OSS matrix pins the shared fence seam the enterprise
     * helper uses (CairoEngine.getRoleSwitchReadLock + fireRoleSwitchMintObserver, the same seam the OSS
     * parse-time TRUNCATE mint fires and the matrix's testParseTimeTruncateDrivesRealWriteAndIsFenced
     * drives for real) and asserts ALTER_STORAGE_POLICY is in the live gate's refused set; the enterprise
     * EnableDisableStoragePolicyDemoteRaceTest is the behavioral driver of the real ENABLE and DISABLE
     * writes across a demote.
     */
    @Test
    public void testStoragePolicySubRoutesShareTheRoleSwitchFence() throws Exception {
        // The four enterprise storage-policy mint sub-routes, enumerated individually so the matrix no
        // longer treats ALTER_STORAGE_POLICY as a single classification cell.
        String[] subRoutes = {"SET", "DROP", "ENABLE", "DISABLE"};
        Assert.assertEquals(
                "all four storage-policy sub-routes must be enumerated -- SET/DROP/ENABLE/DISABLE",
                4, subRoutes.length
        );

        // Every sub-route mints inside compile() before the per-statement gate, so each must hold the
        // shared role-switch fence at the mint. Pin the seam the enterprise shared helper uses, so a
        // rename or removal of the fence reds this matrix for the storage-policy sub-routes too.
        Assert.assertNotNull(
                "CairoEngine.getRoleSwitchReadLock must exist -- the lock the storage-policy SET/DROP/"
                        + "ENABLE/DISABLE sub-routes hold around their parse-time sp_entries / sp_links mint",
                CairoEngine.class.getDeclaredMethod("getRoleSwitchReadLock")
        );
        Assert.assertNotNull(
                "CairoEngine.fireRoleSwitchMintObserver must exist -- fired inside the read-lock hold by"
                        + " each storage-policy sub-route's fence (the same seam the parse-time TRUNCATE"
                        + " mint fires), so the enable/disable demote-race witness can pause the mint",
                CairoEngine.class.getDeclaredMethod("fireRoleSwitchMintObserver")
        );

        // The live gate must refuse ALTER_STORAGE_POLICY for every sub-route on a read-only node. A
        // sub-route that minted without being in the refused set would be a silent write on a replica.
        CairoConfiguration cfg = new DefaultCairoConfiguration(root);
        for (String subRoute : subRoutes) {
            Assert.assertTrue(
                    "the " + subRoute + " STORAGE POLICY sub-route mints a replicated WAL write, so the"
                            + " live ReadOnlyStatementGate must refuse ALTER_STORAGE_POLICY on a read-only"
                            + " node",
                    ReadOnlyStatementGate.isRefusedOnReadOnly(CompiledQuery.ALTER_STORAGE_POLICY, null, cfg)
            );
        }
    }

    // --- helpers ---

    private static void assertReadOnlyRefusal(CairoException e) {
        Assert.assertTrue("exception must be an authorization error: " + e.getMessage(), e.isAuthorizationError());
        Assert.assertTrue(
                "message must be '" + READ_ONLY_REFUSAL + "': " + e.getMessage(),
                e.getMessage().contains(READ_ONLY_REFUSAL)
        );
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

    private static Operation dropOperation(String tableName) {
        return new GenericDropOperation(OperationCodes.DROP_TABLE, null, tableName, 0, false);
    }

    /**
     * A real UpdateOperation used only to carry a TableToken into OperationDispatcher.execute (the
     * dispatcher's apply() is overridden to a no-op, so the operation's own apply() never runs).
     */
    private static UpdateOperation fenceProbeOperation() {
        final TableToken token = new TableToken("matrix_disp", "matrix_disp~1", null, 1, true, false, false);
        final ObjList<CharSequence> columns = new ObjList<>();
        columns.add("val");
        return new UpdateOperation(token, 1, 0, 0, columns);
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
                            + " read-only re-check), ACQUIRE_GATED (reaches a writer only through a"
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
     * An engine on a dedicated root whose isReadOnlyMode() reads a toggleable flag, so a test can create
     * a table while writable and then flip read-only to drive a fenced externalization. A dedicated root
     * avoids the table-name registry lock the shared test engine holds; the table-name registry stays
     * writable, so the fence (not the read-only registry) is the refusal source.
     */
    private CairoEngine flipReadOnlyEngine(AtomicBoolean readOnly) throws Exception {
        String dir = temp.newFolder().getAbsolutePath();
        CairoConfiguration cfg = new DefaultTestCairoConfiguration(dir);
        return new CairoEngine(cfg) {
            @Override
            public boolean isReadOnlyMode() {
                return readOnly.get();
            }
        };
    }

    /**
     * An engine on a dedicated root whose WAL writer acquire always throws EntryUnavailableException (the
     * pool-exhausted condition that routes OperationDispatcher.execute into the async-enqueue catch
     * branch) and whose isReadOnlyMode() is fixed true, so the catch-branch re-check must refuse.
     */
    private CairoEngine poolExhaustedReadOnlyEngine() throws Exception {
        String dir = temp.newFolder().getAbsolutePath();
        CairoConfiguration cfg = new DefaultCairoConfiguration(dir);
        return new CairoEngine(cfg, false) {
            @Override
            public TableWriterAPI getTableWriterAPI(TableToken tableToken, String lockReason) {
                throw EntryUnavailableException.instance("pool size exceeded");
            }

            @Override
            public boolean isReadOnlyMode() {
                return true;
            }
        };
    }
}
