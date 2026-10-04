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

import io.questdb.PropertyKey;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.CursorPrinter;
import io.questdb.cairo.PhysicalDescriptor;
import io.questdb.cairo.RelationKind;
import io.questdb.cairo.RelationRules;
import io.questdb.cairo.SqlJitMode;
import io.questdb.cairo.TypeDriver;
import io.questdb.cairo.arr.ArrayView;
import io.questdb.cairo.arr.DirectArray;
import io.questdb.cairo.lv.LiveViewRefreshJob;
import io.questdb.cairo.sql.BindVariableService;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cutlass.parquet.CopyExportRequestJob;
import io.questdb.griffin.SqlCodeGenerator;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.WorkerPool;
import io.questdb.std.Files;
import io.questdb.std.NumericException;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.str.StringSink;
import io.questdb.std.str.Utf8String;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.QueryAssertion;
import io.questdb.test.mp.TestWorkerPool;
import io.questdb.test.tools.TestUtils;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * The SQL part of the conformance kit (User Story 4): every kit type through filters,
 * ORDER BY, GROUP BY, inner and outer JOIN, UNION ALL, CASE, CAST, {@code lag}, SAMPLE BY and
 * LATEST ON, and the "not computed yet" versus NULL cases: {@code first}, {@code last},
 * {@code first_not_null} and {@code last_not_null} over groups whose first row is NULL,
 * SAMPLE BY FILL(PREV) over a leading NULL, and SAMPLE BY FILL(NULL), FILL(value) and
 * FILL(LINEAR) over gaps between two values and before a NULL.
 * <p>
 * Some paths run one query per value row and record one line per row: the labels of the rows
 * the query selects, or its error. The row's value goes into the query as a constant of the type
 * (its literal, the NULL row as a NULL of the type), as the value as it prints (quoted unless the
 * type is a number) or as a bind variable of the type set from that text; a row that reads as
 * NULL goes in as NULL in the last two forms. SUBSAMPLE
 * takes a value of the type as the stride of {@code cadence(...)} and as the target point count
 * of {@code uniform(...)}; a WHERE bound over the designated timestamp cast to LONG takes it as a
 * constant and as a bind variable; a key column (LATEST ON ... PARTITION BY the column, with
 * {@code WHERE v = <value>}) takes the plain constant.
 * <p>
 * {@code sql.copy_bind} exports {@code SELECT k, v FROM t WHERE v = $1} with COPY to a Parquet
 * file, {@code $1} a bind variable of the type set to the high value: COPY snapshots the bind
 * variables before it queues the export, which runs on a copy export job driven by the test
 * thread. It runs single-threaded only: the export runs on its own job in every mode.
 * <p>
 * {@code sql.bind_value} defines a bind variable with the type, as its type driver defines one,
 * sets it through each value setter of the bind variable service in turn and reads it back with
 * {@code SELECT $1}. {@code lv.window_anchor} makes the column the ANCHOR EXPRESSION of a live
 * view's window over a WAL table and refreshes the view with a refresh job driven by the test
 * thread. Both run single-threaded: neither has a parallel factory.
 * <p>
 * {@code sql.between_timestamp} compares the column with two TIMESTAMP bounds, which takes the
 * timestamp {@code between} for the types that widen to TIMESTAMP and reads a non-timestamp
 * operand through its timestamp getter; {@code sql.eq_null_double} tests {@code v = NULL}, which
 * the double equality answers with its family's NULL test.
 * <p>
 * The memoized path reads a projected function of the column, its identity cast, three times,
 * which makes the projection cache its value per row; the test base turns that caching off, so
 * this path turns it on for its own query and records whether the plan memoizes the function.
 * <p>
 * Every query runs in three modes: single-threaded with interpreted filters, and parallel (a
 * worker pool of four, the parallel factories on) with compiled and with interpreted filters. A
 * filter compiles only on the parallel path ({@code SqlCodeGenerator} gives the JIT to the async
 * filter alone), so a single-threaded compiled mode would repeat the interpreted one and is not
 * run. Every mode must give the one recording made at S12 ({@link TypeConformanceRecording}). A section holds the query's
 * output and the factory properties it pins: random access, whether the cursor knows its size,
 * and the designated timestamp with its order. A query that runs is also asserted with
 * {@code assertQuery(sql).returns(...)} under those properties, which reads the cursor twice
 * and checks its size. A query that fails records today's error. Casts to every kit type run
 * single-threaded with interpreted filters only: a projection has no filter and no parallel
 * factory, so the other modes cannot change it.
 * <p>
 * Tables per type: {@code t} holds every value row; {@code t2} holds them twice (GROUP BY,
 * LATEST ON); {@code u} holds the even rows (joins, UNION ALL); {@code n} holds groups whose
 * first, last or every row is NULL; {@code f} holds a NULL, the {@code max} row and a NULL two
 * seconds apart (SAMPLE BY FILL(PREV)); {@code g} holds a low value, the high value and a NULL
 * two seconds apart (the other FILL paths). Queries that need a literal use the {@code max}
 * row's; FILL(value) fills with the high value as it prints.
 * <p>
 * A type registered later runs the queries whose value survives unchanged (filters on NULL,
 * ORDER BY, UNION ALL) and the FILL paths over {@code g} where its resource line enables them,
 * checked by {@link TypeConformanceInvariants}; the design-proof mixing cases of the resource run once, in
 * the first later type's instance. Other queries need literals or relations a later type does
 * not have yet, and fail when enabled.
 */
@RunWith(Parameterized.class)
public class TypeConformanceSqlTest extends AbstractCairoTest {
    private static final Log LOG = LogFactory.getLog(TypeConformanceSqlTest.class);
    private static final Pattern NO_CAST = Pattern.compile("error: \\[(\\d+)] there is no matching function `cast` with the argument types: \\((.*)\\)");
    // the value setters of the bind variable service, each with one value (setBindValue)
    private static final String[] BIND_SETTERS = {
            "setBoolean", "setByte", "setShort", "setChar", "setInt", "setLong", "setFloat", "setDouble", "setDate",
            "setTimestamp", "setStr", "setVarchar", "setLong256", "setUuid", "setArray"
    };
    private static final String FORM_BIND = "bind";
    private static final String FORM_CONST = "const";
    private static final String FORM_TEXT = "text";
    private static final String[] MODES = {"single-nojit", "parallel-nojit", "parallel-jit"};
    private static final Map<String, String> RECORDINGS = new HashMap<>();
    private final ObjList<TypeConformanceValues.Row> rows;
    private final TypeConformanceTypes.Entry type;
    // the FILL(value) token: the high row of g as it prints, quoted unless it is a number
    private String fillValue;

    public TypeConformanceSqlTest(String label) {
        this.type = TypeConformanceTypes.byLabel(label);
        this.rows = TypeConformanceValues.rowsOf(type);
    }

    @Parameterized.Parameters(name = "{0}")
    public static Collection<Object[]> data() {
        final Collection<Object[]> data = new ArrayList<>();
        for (int i = 0, n = TypeConformanceTypes.ALL.size(); i < n; i++) {
            data.add(new Object[]{TypeConformanceTypes.ALL.getQuick(i).label});
        }
        return data;
    }

    @Test
    public void testBindValue() throws Exception {
        assertMemoryLeak(() -> {
            final String path = "sql.bind_value";
            final String mode = MODES[0];
            if (!TypeConformanceInvariants.isEnabled(type, path, mode)) {
                return;
            }
            configure(sqlExecutionContext, mode);
            final BindVariableService service = sqlExecutionContext.getBindVariableService();
            final StringSink section = new StringSink();
            try (DirectArray array = new DirectArray(configuration)) {
                array.setType(ColumnType.encodeArrayType(ColumnType.DOUBLE, 1));
                array.setDimLen(0, 1);
                array.applyShape();
                array.startMemoryA().putDouble(1.5);
                for (String setter : BIND_SETTERS) {
                    service.clear();
                    section.put(setter).put('\t');
                    try {
                        ColumnType.getTypeDriver(type.columnType).defineBindVariable(service, 0, type.columnType, 0);
                    } catch (Throwable e) {
                        section.put("define error: ").put(oneLineOf(e)).put('\n');
                        continue;
                    }
                    try {
                        setBindValue(service, setter, array);
                    } catch (Throwable e) {
                        section.put("error: ").put(oneLineOf(e)).put('\n');
                        continue;
                    }
                    final Observation observation = observe(engine, sqlExecutionContext, "SELECT $1 v");
                    if (observation.error != null) {
                        section.put(observation.error.replace('\n', ' '));
                    } else {
                        final String[] lines = observation.output.split("\n");
                        section.put(lines.length > 1 ? lines[1] : "");
                    }
                    section.put('\n');
                }
            } finally {
                service.clear();
            }
            if (!type.isLater()) {
                assertSection("bind_value", mode, section);
                return;
            }
            // a later type: each setter is refused with an error naming the type, or the value reads back
            final String name = ColumnType.nameOf(type.columnType);
            for (String line : section.toString().split("\n")) {
                final String outcome = line.substring(line.indexOf('\t') + 1);
                if ((outcome.startsWith("error: ") || outcome.startsWith("define error: ")) && !outcome.contains(name)) {
                    throw new AssertionError(TypeConformanceInvariants.context(type, line.substring(0, line.indexOf('\t')), path, mode)
                            + ": a refused bind value must name the type, but: " + outcome);
                }
            }
        });
    }

    @Test
    public void testCast() throws Exception {
        assertMemoryLeak(() -> {
            final String mode = MODES[0];
            if (!TypeConformanceInvariants.isEnabled(type, "sql.cast", mode)) {
                return;
            }
            configure(sqlExecutionContext, mode);
            if (type.isLater()) {
                checkLaterCasts(mode);
                return;
            }
            final StringSink steps = new StringSink();
            if (!createTables(engine, sqlExecutionContext, steps)) {
                assertSection("cast", mode, steps);
                return;
            }
            // one header line of row labels, then one line of values per target
            final StringSink section = new StringSink();
            section.put("target");
            for (int i = 0, n = rows.size(); i < n; i++) {
                section.put(i == 0 ? '\t' : '|').put(rows.getQuick(i).label);
            }
            section.put('\n');
            for (int t = 0, n = TypeConformanceTypes.ALL.size(); t < n; t++) {
                final TypeConformanceTypes.Entry target = TypeConformanceTypes.ALL.getQuick(t);
                if (target.isLater()) {
                    continue;
                }
                // CAST(... AS ...): an alias after a ::GEOHASH(...) cast parses as a column
                final String sql = "SELECT k, CAST(v AS " + target.ddl + ") c FROM t";
                final Observation observation = observe(engine, sqlExecutionContext, sql);
                section.put(target.label).put('\t');
                if (observation.error != null) {
                    section.put(abbreviate(observation.error.replace('\n', ' '), target)).put('\n');
                    continue;
                }
                final String battery = assertReturns(engine, sqlExecutionContext, "sql.cast", mode, sql, observation, true);
                final String[] lines = observation.output.split("\n");
                for (int i = 1; i < lines.length; i++) {
                    if (i > 1) {
                        section.put('|');
                    }
                    final int tab = lines[i].indexOf('\t');
                    section.put(lines[i], tab + 1, lines[i].length());
                }
                if (battery != null) {
                    section.put('\t').put(battery);
                }
                section.put('\n');
            }
            assertSection("cast", mode, section);
            dropTables(engine, sqlExecutionContext);
        });
    }

    @Test
    public void testCopyBind() throws Exception {
        assertMemoryLeak(() -> {
            final String path = "sql.copy_bind";
            final String mode = MODES[0];
            if (!TypeConformanceInvariants.isEnabled(type, path, mode)) {
                return;
            }
            configure(sqlExecutionContext, mode);
            final String exportRoot = temp.newFolder().getAbsolutePath();
            node1.setProperty(PropertyKey.CAIRO_SQL_COPY_ROOT, exportRoot);
            node1.setProperty(PropertyKey.CAIRO_SQL_COPY_EXPORT_ROOT, exportRoot);
            // read_parquet reads from the input root
            inputRoot = exportRoot;
            final StringSink steps = new StringSink();
            if (!createTables(engine, sqlExecutionContext, steps)) {
                assertSection("copy_bind", mode, steps);
                return;
            }
            try {
                final TypeConformanceValues.Row high = fillRows() != null ? fillRows().getQuick(2) : null;
                final String highText = high == null ? null : readTexts(engine, sqlExecutionContext, "SELECT k, v FROM g WHERE k = 'r2'").get("r2");
                final StringSink section = new StringSink();
                final String error = copyBind(highText, exportRoot, section);
                if (type.isLater()) {
                    if (!TypeConformanceInvariants.assertDeclaredRefusal(type, "-", path, mode, error, "COPY bind snapshot")) {
                        if (error != null || !section.toString().contains("\n" + high.label + "\n")) {
                            throw new AssertionError(TypeConformanceInvariants.context(type, high.label, path, mode)
                                    + ": the export of the row bound by its value must hold that row: " + (error != null ? error : section));
                        }
                    }
                    return;
                }
                assertSection("copy_bind", mode, mask(error != null ? section + error + '\n' : section).replace(exportRoot, "<exportRoot>"));
            } finally {
                inputRoot = null;
                sqlExecutionContext.getBindVariableService().clear();
                dropTables(engine, sqlExecutionContext);
            }
        });
    }

    @Test
    public void testDesignProofMixingCases() throws Exception {
        final ObjList<String[]> cases = TypeConformanceInvariants.mixingCases();
        TypeConformanceTypes.Entry firstLater = null;
        for (int i = 0, n = TypeConformanceTypes.ALL.size(); i < n && firstLater == null; i++) {
            if (TypeConformanceTypes.ALL.getQuick(i).isLater()) {
                firstLater = TypeConformanceTypes.ALL.getQuick(i);
            }
        }
        if (cases.size() == 0 || firstLater != type) {
            // the cases run once, in the first later type's instance; no later type, no cases
            return;
        }
        assertMemoryLeak(() -> runModes((eng, ctx, mode) -> {
            for (int i = 0, n = cases.size(); i < n; i++) {
                final String[] mixingCase = cases.getQuick(i);
                try {
                    assertQuery(mixingCase[1])
                            .withEngine(eng)
                            .withContext(ctx)
                            .noLeakCheck()
                            .inferRandomAccess()
                            .inferTimestamp()
                            .sizeMayVary()
                            .returns(mixingCase[2]);
                } catch (AssertionError e) {
                    throw new AssertionError("mixing case " + mixingCase[0] + " mode=" + mode + ": " + e.getMessage(), e);
                }
            }
        }));
    }

    @Test
    public void testLiveViewAnchor() throws Exception {
        assertMemoryLeak(() -> {
            final String path = "lv.window_anchor";
            final String mode = MODES[0];
            if (!TypeConformanceInvariants.isEnabled(type, path, mode)) {
                return;
            }
            configure(sqlExecutionContext, mode);
            final StringSink section = new StringSink();
            // g stays NULL in every row: one window partition, so only the anchor resets the count
            execute(engine, sqlExecutionContext, "CREATE TABLE lvb (k VARCHAR, g SYMBOL, v " + type.ddl + ", ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL", "create", section);
            if (section.length() > 0) {
                if (type.isLater()) {
                    throw new AssertionError(TypeConformanceInvariants.context(type, "-", path, mode) + ": " + section);
                }
                assertSection("window_anchor", mode, section);
                return;
            }
            try {
                // the view starts from the test clock's now, which the refresh below advances
                setCurrentMicros(0L);
                // the anchor resets the window's count each time the column's value changes; the
                // anchor and the window's keys resolve against the projection
                execute(engine, sqlExecutionContext, "CREATE LIVE VIEW lva FLUSH EVERY 100ms START FROM NOW AS "
                        + "SELECT ts, k, g, v, count(*) OVER w AS c FROM lvb WINDOW w AS (PARTITION BY g ORDER BY ts ANCHOR EXPRESSION v)", "create view", section);
                final boolean isCreated = section.length() == 0;
                if (isCreated) {
                    try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                        TypeConformanceValues.writeRows(engine, sqlExecutionContext, "lvb", rows, "", 0, 0, rows.size(), 1, true, section);
                        // past each flush deadline until the refresh finds no more work
                        boolean isProgressing = true;
                        for (int pass = 0; pass < 512 && isProgressing; pass++) {
                            setCurrentMicros(currentMicros + 250_000L);
                            drainWalQueue();
                            isProgressing = false;
                            for (int i = 0; i < 64 && job.run(); i++) {
                                isProgressing = true;
                            }
                            drainWalQueue();
                        }
                    }
                    section.put(TypeConformanceRecording.escape(printQuietly("SELECT k, c FROM lva ORDER BY ts")));
                }
                if (!type.isLater()) {
                    assertSection("window_anchor", mode, section);
                    return;
                }
                // a later type is an anchor when its family is TIMESTAMP, LONG or INT and it orders as the family does
                final TypeDriver driver = ColumnType.getTypeDriver(type.columnType);
                final PhysicalDescriptor.Accessor family = driver.getAccessor();
                final boolean isAnchorFamily = family == PhysicalDescriptor.Accessor.TIMESTAMP
                        || family == PhysicalDescriptor.Accessor.LONG || family == PhysicalDescriptor.Accessor.INT;
                final int namesake = family == PhysicalDescriptor.Accessor.INT ? ColumnType.INT
                        : family == PhysicalDescriptor.Accessor.LONG ? ColumnType.LONG : ColumnType.TIMESTAMP;
                final boolean isAnchor = isAnchorFamily && driver.getArithmetic() == ColumnType.getTypeDriver(namesake).getArithmetic();
                if (isCreated != isAnchor) {
                    throw new AssertionError(TypeConformanceInvariants.context(type, "-", path, mode)
                            + ": the anchor must be " + (isAnchor ? "accepted" : "refused") + ": " + section);
                }
                if (isCreated && (section.toString().contains("error: ") || section.toString().split("\n").length != rows.size() + 1)) {
                    throw new AssertionError(TypeConformanceInvariants.context(type, "-", path, mode)
                            + ": the view must hold every row of the base table: " + section);
                }
            } finally {
                setCurrentMicros(-1);
                final StringSink ignored = new StringSink();
                execute(engine, sqlExecutionContext, "DROP LIVE VIEW IF EXISTS lva", "drop", ignored);
                execute(engine, sqlExecutionContext, "DROP TABLE IF EXISTS lvb", "drop", ignored);
            }
        });
    }

    @Test
    public void testQueries() throws Exception {
        assertMemoryLeak(() -> runModes((eng, ctx, mode) -> {
            final StringSink steps = new StringSink();
            if (!createTables(eng, ctx, steps)) {
                for (String[] query : queries()) {
                    if (TypeConformanceInvariants.isEnabled(type, "sql." + query[0], mode)) {
                        assertSection(query[0], mode, steps);
                    }
                }
                for (String[] query : rowQueries()) {
                    if (TypeConformanceInvariants.isEnabled(type, "sql." + query[0], mode)) {
                        assertSection(query[0], mode, steps);
                    }
                }
                return;
            }
            if (type.isLater()) {
                // every value row is written; under NOT_NULL the NULL rows are refused
                TypeConformanceInvariants.nullRowWriteError(type, "sql.setup", mode, steps);
            }
            for (String[] query : queries()) {
                final String path = "sql." + query[0];
                if (!TypeConformanceInvariants.isEnabled(type, path, mode)) {
                    continue;
                }
                final boolean isMemoized = "memoized".equals(query[0]);
                SqlCodeGenerator.ALLOW_FUNCTION_MEMOIZATION = isMemoized;
                try {
                    if (type.isLater()) {
                        checkLater(eng, ctx, query[0], query[1], mode);
                        continue;
                    }
                    final Observation observation = observe(eng, ctx, query[1]);
                    final String battery = observation.error == null
                            ? assertReturns(eng, ctx, path, mode, query[1], observation, eng == engine)
                            : null;
                    String section = battery == null ? observation.section() : observation.section() + battery + '\n';
                    if (isMemoized && observation.error == null) {
                        section += "plan memoizes: " + observe(eng, ctx, "EXPLAIN " + query[1]).output.contains("memoize(") + '\n';
                    }
                    assertSection(query[0], mode, section);
                } finally {
                    SqlCodeGenerator.ALLOW_FUNCTION_MEMOIZATION = false;
                }
            }
            for (String[] query : rowQueries()) {
                final String path = "sql." + query[0];
                if (!TypeConformanceInvariants.isEnabled(type, path, mode)) {
                    continue;
                }
                if (type.isLater()) {
                    checkLaterRowQuery(eng, ctx, query[0], query[1], query[2], mode);
                    continue;
                }
                assertSection(query[0], mode, rowSection(eng, ctx, path, mode, query[1], query[2]));
            }
            dropTables(eng, ctx);
        }));
    }

    /**
     * Shortens the refusal every pair without a cast function gives,
     * {@code error: [p] there is no matching function `cast` with the argument types: (S, T)}
     * with this line's S and T, to {@code no cast [p]}; any other text stays as it is, so the
     * recording loses nothing.
     */
    private String abbreviate(String error, TypeConformanceTypes.Entry target) {
        final Matcher matcher = NO_CAST.matcher(error);
        if (matcher.matches() && matcher.group(2).equals(ColumnType.nameOf(type.columnType) + ", " + ColumnType.nameOf(target.columnType))) {
            return "no cast [" + matcher.group(1) + "]";
        }
        return error;
    }

    private static void configure(SqlExecutionContext ctx, String mode) {
        final boolean isParallel = mode.startsWith("parallel");
        ctx.setJitMode(mode.endsWith("-jit") ? SqlJitMode.JIT_MODE_ENABLED : SqlJitMode.JIT_MODE_DISABLED);
        ctx.setParallelFilterEnabled(isParallel);
        ctx.setParallelGroupByEnabled(isParallel);
        ctx.setParallelTopKEnabled(isParallel);
    }

    private static void execute(CairoEngine eng, SqlExecutionContext ctx, String sql, String step, StringSink steps) {
        try {
            eng.execute(sql, ctx);
        } catch (Throwable e) {
            steps.put("error: ").put(step).put(": ").put(e.getMessage()).put('\n');
        }
    }

    private static String mask(CharSequence text) {
        // mask: the database root of the test run
        return text.toString().replace(root, "<dbRoot>");
    }

    // the first column of every row of the output, comma-separated: the labels of the rows selected
    private static String labelsOf(Observation observation) {
        final String[] lines = observation.output.split("\n");
        final StringSink labels = new StringSink();
        for (int l = 1; l < lines.length; l++) {
            final int tab = lines[l].indexOf('\t');
            labels.put(l > 1 ? "," : "").put(tab > -1 ? lines[l].substring(0, tab) : lines[l]);
        }
        return labels.toString();
    }

    /**
     * The paths that run one query per value row, {name, sql, form}: {@code <value>} in the SQL
     * stands for the row's value in the form named: {@code const} the value as a constant of the
     * type ({@link #constantOf}), {@code text} as the value as it prints ({@link #keyConstantOf}),
     * {@code bind} as the bind variable {@code $1}, defined with the type and set from the value's
     * text. A row that reads as NULL goes in as NULL in the last two forms. The path of each is
     * {@code sql.<name>}.
     */
    private static String[][] rowQueries() {
        return new String[][]{
                {"subsample_stride", "SELECT k, ts FROM t SUBSAMPLE cadence(<value>)", FORM_CONST},
                {"subsample_target", "SELECT k, ts FROM t SUBSAMPLE uniform(<value>)", FORM_CONST},
                {"where_bound_const", "SELECT k FROM t WHERE ts::LONG >= <value>", FORM_CONST},
                {"where_bound_bind", "SELECT k FROM t WHERE ts::LONG >= <value>", FORM_BIND},
                {"where_key", "SELECT k FROM t2 WHERE v = <value> LATEST ON ts PARTITION BY v", FORM_TEXT},
        };
    }

    // a row that reads as NULL: the NULL row, and under SENTINEL the sentinel-pattern rows
    private static boolean isNullRow(TypeConformanceValues.Row row, String nullRows) {
        return row.isNull() || nullRows.contains("," + row.label + ",");
    }

    private static String oneLineOf(Throwable e) {
        return TypeConformanceRecording.escape(String.valueOf(e.getMessage())).replace('\n', ' ');
    }

    // sets $1 through one value setter of the service, with that setter's value
    private static void setBindValue(BindVariableService service, String setter, ArrayView array) throws SqlException {
        switch (setter) {
            case "setBoolean" -> service.setBoolean(0, true);
            case "setByte" -> service.setByte(0, (byte) 1);
            case "setShort" -> service.setShort(0, (short) 1);
            case "setChar" -> service.setChar(0, '1');
            case "setInt" -> service.setInt(0, 1);
            case "setLong" -> service.setLong(0, 1L);
            case "setFloat" -> service.setFloat(0, 1.5f);
            case "setDouble" -> service.setDouble(0, 1.5);
            case "setDate" -> service.setDate(0, 1L);
            case "setTimestamp" -> service.setTimestamp(0, 1L);
            case "setStr" -> service.setStr(0, "1");
            case "setVarchar" -> service.setVarchar(0, new Utf8String("1"));
            case "setLong256" -> service.setLong256(0, 1L, 0L, 0L, 0L);
            case "setUuid" -> service.setUuid(0, 1L, 0L);
            default -> service.setArray(0, array);
        }
    }

    // a value as SQL text: a number as it is, any other text quoted, a NULL as NULL
    private static String plainConstantOf(@Nullable String text) {
        if (text == null) {
            return "NULL";
        }
        try {
            Numbers.parseDouble(text);
            return text;
        } catch (NumericException e) {
            return "'" + text.replace("'", "''") + "'";
        }
    }


    // the value a widening of the type gives for each row, by its declared tier (F89 invariant 4)
    private void addWideningGaps(String pair, TypeConformanceTypes.Entry target, Map<String, long[]> actual, ObjList<String> gaps) {
        if (type.laterTier == null) {
            return;
        }
        final RelationKind targetKind = TypeConformanceInvariants.kindOf(target.columnType);
        final int targetWidth = TypeConformanceInvariants.widthOf(target.columnType);
        for (int i = 0, n = rows.size(); i < n; i++) {
            final TypeConformanceValues.Row row = rows.getQuick(i);
            if (row.isNull() || !actual.containsKey(row.label) || isSentinelUnderSentinel(row)) {
                continue;
            }
            final long[] expected = TypeConformanceInvariants.widened(type, row.bits, targetKind, targetWidth);
            if (expected != null && !TypeConformanceInvariants.isSameValue(targetKind, targetWidth, expected, actual.get(row.label))) {
                gaps.add(pair + ": row " + row.label + " converts to " + Arrays.toString(actual.get(row.label))
                        + ", tier " + type.laterTier + " gives " + Arrays.toString(expected));
            }
        }
    }

    private void assertNoGaps(String path, String mode, String what, ObjList<String> gaps) {
        if (gaps.size() == 0) {
            return;
        }
        final StringBuilder message = new StringBuilder(TypeConformanceInvariants.context(type, "-", path, mode))
                .append(": ").append(gaps.size()).append(' ').append(what).append(" break an invariant:");
        for (int i = 0, n = gaps.size(); i < n; i++) {
            message.append("\n  ").append(gaps.getQuick(i));
        }
        throw new AssertionError(message.toString());
    }

    /**
     * Asserts the query with the full {@code returns} battery, under the factory properties the
     * recording pins. The base engine keeps the assertion's own leak check; the worker pool's
     * engine is checked by the enclosing {@code assertMemoryLeak}. An exception the query raises
     * on one of the battery's read paths (not an assertion) is today's behaviour: it returns as
     * a line for the recording, {@code returns: <exception>: <message>}; null when the battery
     * passes.
     */
    @Nullable
    private String assertReturns(
            CairoEngine eng,
            SqlExecutionContext ctx,
            String path,
            String mode,
            String sql,
            Observation observation,
            boolean isLeakChecked
    ) throws Exception {
        final QueryAssertion assertion = assertQuery(sql)
                .withEngine(eng)
                .withContext(ctx)
                .supportsRandomAccess(observation.isRandomAccess)
                .expectSize(observation.isSizeKnown);
        if (!isLeakChecked) {
            assertion.noLeakCheck();
        }
        if (observation.timestamp != null) {
            switch (observation.scanDirection) {
                case RecordCursorFactory.SCAN_DIRECTION_FORWARD -> assertion.timestamp(observation.timestamp);
                case RecordCursorFactory.SCAN_DIRECTION_BACKWARD -> assertion.timestampDesc(observation.timestamp);
                default -> assertion.inferTimestamp();
            }
        }
        try {
            assertion.returns(observation.rawOutput);
        } catch (AssertionError e) {
            throw new AssertionError(TypeConformanceInvariants.context(type, "-", path, mode) + ": " + e.getMessage(), e);
        } catch (Throwable e) {
            return "returns: " + e.getClass().getSimpleName() + ": " + TypeConformanceRecording.escape(String.valueOf(e.getMessage())).replace('\n', ' ');
        }
        return null;
    }

    private void assertSection(String path, String mode, CharSequence actual) {
        TypeConformanceRecording.assertSection(type, path, mode, RECORDINGS.get(type.label), mask(actual));
    }

    /**
     * A type registered later: the queries whose value survives unchanged read back as written,
     * and the NULL row and the sentinel-pattern row behave as the NULL policy says.
     */
    private void checkLater(CairoEngine eng, SqlExecutionContext ctx, String name, String sql, String mode) throws Exception {
        final String path = "sql." + name;
        switch (name) {
            case "filter_null", "filter_not_null", "order_asc", "order_desc", "union_all" -> {
            }
            case "case_no_else" -> {
                checkLaterCaseNoElse(eng, ctx, mode);
                return;
            }
            case "case_else" -> {
                checkLaterCaseElse(eng, ctx, mode);
                return;
            }
            case "fill_null", "fill_value", "fill_prev", "fill_linear" -> {
                checkLaterFill(eng, ctx, name, mode);
                return;
            }
            case "memoized" -> {
                checkLaterMemoized(eng, ctx, sql, mode);
                return;
            }
            case "between_timestamp", "eq_null_double" -> {
                checkLaterNullTest(eng, ctx, name, sql, mode);
                return;
            }
            default -> throw new AssertionError(TypeConformanceInvariants.context(type, "-", path, mode)
                    + ": no invariant for this query before the stage that converts it");
        }
        final Map<String, long[]> bits = new HashMap<>();
        final Map<String, String> texts = new HashMap<>();
        final ObjList<String> order = new ObjList<>();
        final ObjList<long[]> orderBits = new ObjList<>();
        try (
                SqlCompiler compiler = eng.getSqlCompiler();
                RecordCursorFactory factory = compiler.compile(sql, ctx).getRecordCursorFactory();
                RecordCursor cursor = factory.getCursor(ctx)
        ) {
            final RecordMetadata metadata = factory.getMetadata();
            final Record record = cursor.getRecord();
            final StringSink sink = new StringSink();
            while (cursor.hasNext()) {
                final String label = record.getVarcharA(0).toString();
                bits.put(label, TypeConformanceValues.readValue(record, 1, type));
                order.add(label);
                orderBits.add(bits.get(label));
                sink.clear();
                CursorPrinter.printColumn(record, metadata, 1, sink);
                texts.put(label, sink.toString());
            }
        }
        final String policy = TypeConformanceInvariants.policyOf(type);
        for (int i = 0, n = rows.size(); i < n; i++) {
            final TypeConformanceValues.Row row = rows.getQuick(i);
            if (!row.isNull() && bits.containsKey(row.label)) {
                TypeConformanceInvariants.assertReadsBackAsWritten(type, row.label, path, mode, row.bits, bits.get(row.label));
            }
        }
        if ("order_asc".equals(name) || "order_desc".equals(name)) {
            TypeConformanceInvariants.assertOrdered(type, path, mode, order, orderBits, "order_asc".equals(name));
        }
        // IS NULL selects the NULL row unless the policy stores none; the sentinel-pattern row
        // only under SENTINEL, where it is NULL
        if ("filter_null".equals(name) || "filter_not_null".equals(name)) {
            final boolean isNullQuery = "filter_null".equals(name);
            final boolean isNullStored = TypeConformanceInvariants.POLICY_SENTINEL.equals(policy) || TypeConformanceInvariants.POLICY_BITMAP.equals(policy);
            final boolean isSentinelNull = TypeConformanceInvariants.POLICY_SENTINEL.equals(policy);
            if (bits.containsKey("null") != (isNullQuery == isNullStored) && !TypeConformanceInvariants.POLICY_NOT_NULL.equals(policy)) {
                throw new AssertionError(TypeConformanceInvariants.context(type, "null", path, mode) + ": " + policy + " row presence is wrong");
            }
            // a var-size type has no sentinel-pattern row: its NULL lives in the length
            if (hasRow("sentinel") && bits.containsKey("sentinel") != (isNullQuery == isSentinelNull)) {
                throw new AssertionError(TypeConformanceInvariants.context(type, "sentinel", path, mode) + ": " + policy + " row presence is wrong");
            }
        }
        if (texts.containsKey("null") && texts.containsKey("sentinel")) {
            TypeConformanceInvariants.assertNullPolicy(type, path, mode, texts.get("null"), bits.get("null"), null,
                    texts.get("sentinel"), bits.get("sentinel"), sentinelBits());
        }
        // another type's sentinel pattern is a value (#6921): IS NULL never selects it, and it
        // reads differently from the NULL row, except under SENTINEL
        final boolean isOtherSentinelValue = !TypeConformanceInvariants.POLICY_SENTINEL.equals(policy);
        for (int i = 0, n = rows.size(); i < n; i++) {
            final String label = rows.getQuick(i).label;
            if (!label.startsWith("sentinel_")) {
                continue;
            }
            if (isOtherSentinelValue && ("filter_null".equals(name) || "filter_not_null".equals(name))
                    && bits.containsKey(label) != "filter_not_null".equals(name)) {
                throw new AssertionError(TypeConformanceInvariants.context(type, label, path, mode)
                        + ": " + policy + ", another type's sentinel is a value, but IS NULL treats it as NULL");
            }
            if (texts.containsKey(label)) {
                TypeConformanceInvariants.assertOtherSentinel(type, label, path, mode, texts.get("null"), texts.get(label));
            }
        }
    }

    /**
     * {@code sql.case_else} for a type registered later, from its declared relations (F89): for
     * every kit type rule E pairs it with, {@code CASE WHEN ... THEN v ELSE <NULL of that type>}
     * compiles, takes the common type the rule names, and gives the selected row as {@code v}
     * converted to that type (as written when the common type is the type itself). With the type
     * itself in the ELSE branch, every row reads back as written.
     */
    private void checkLaterCaseElse(CairoEngine eng, SqlExecutionContext ctx, String mode) throws Exception {
        final String path = "sql.case_else";
        final TypeConformanceValues.Row selected = firstValueRow();
        if (selected == null) {
            return;
        }
        final ObjList<String> gaps = new ObjList<>();
        final String when = "SELECT k, CASE WHEN k = '" + selected.label + "' THEN v ELSE ";
        final String selfPair = type.label + " with " + type.label;
        try {
            final Map<String, long[]> self = readLaterValues(eng, ctx, when + "v END c FROM t");
            for (int i = 0, n = rows.size(); i < n; i++) {
                final TypeConformanceValues.Row row = rows.getQuick(i);
                if (!row.isNull() && self.containsKey(row.label)) {
                    TypeConformanceInvariants.assertReadsBackAsWritten(type, row.label, path, mode, row.bits, self.get(row.label));
                }
            }
        } catch (AssertionError e) {
            gaps.add(selfPair + ": " + e.getMessage());
        } catch (Throwable e) {
            gaps.add(selfPair + ": no implementation: " + e.getMessage());
        }
        final int[] escalation = RelationRules.caseEscalation(type.columnType);
        for (int k = 0; k < escalation.length; k += 2) {
            final TypeConformanceTypes.Entry other = kitTypeOf(escalation[k]);
            if (other == null || other == type) {
                continue;
            }
            final int common = escalation[k + 1];
            final String pair = type.label + " with " + other.label + " -> " + ColumnType.nameOf(common) + " (rule E)";
            final String sql = when + "CAST(NULL AS " + other.ddl + ") END c FROM t";
            final int resultType;
            try (
                    SqlCompiler compiler = eng.getSqlCompiler();
                    RecordCursorFactory factory = compiler.compile(sql, ctx).getRecordCursorFactory()
            ) {
                resultType = factory.getMetadata().getColumnType(1);
            } catch (Throwable e) {
                gaps.add(pair + ": no implementation: " + e.getMessage());
                continue;
            }
            final Map<String, String> texts;
            try {
                texts = readTexts(eng, ctx, sql);
            } catch (Throwable e) {
                gaps.add(pair + ": fails at run time: " + e.getMessage());
                continue;
            }
            if (resultType != common) {
                gaps.add(pair + ": CASE takes " + ColumnType.nameOf(resultType));
                continue;
            }
            if (common == type.columnType) {
                final long[] value = readLaterValues(eng, ctx, sql).get(selected.label);
                try {
                    TypeConformanceInvariants.assertReadsBackAsWritten(type, selected.label, path, mode, selected.bits, value);
                } catch (AssertionError e) {
                    gaps.add(pair + ": " + e.getMessage());
                }
                continue;
            }
            final TypeConformanceTypes.Entry commonEntry = kitTypeOf(common);
            final String castSql = "SELECT k, CAST(v AS " + (commonEntry != null ? commonEntry.ddl : ColumnType.nameOf(common)) + ") c FROM t";
            final Map<String, String> casts;
            try {
                casts = readTexts(eng, ctx, castSql);
            } catch (Throwable e) {
                // no explicit cast to compare with: sql.cast lists admitted pairs without one
                continue;
            }
            if (!casts.get(selected.label).equals(texts.get(selected.label))) {
                gaps.add(pair + ": CASE gives " + texts.get(selected.label) + ", the cast " + casts.get(selected.label));
            }
        }
        assertNoGaps(path, mode, "CASE pairs rule E admits", gaps);
    }

    /**
     * {@code sql.case_no_else} for a type registered later: {@code CASE WHEN ... THEN v END} has
     * the type itself, gives the selected row as written, and every other row as the NULL row
     * reads. A type without NULL (NOT_NULL) is excepted from the NULL rows: CASE without ELSE
     * introduces NULL (F31).
     */
    private void checkLaterCaseNoElse(CairoEngine eng, SqlExecutionContext ctx, String mode) throws Exception {
        final String path = "sql.case_no_else";
        final TypeConformanceValues.Row selected = firstValueRow();
        if (selected == null) {
            return;
        }
        final String sql = "SELECT k, CASE WHEN k = '" + selected.label + "' THEN v END c FROM t";
        try (
                SqlCompiler compiler = eng.getSqlCompiler();
                RecordCursorFactory factory = compiler.compile(sql, ctx).getRecordCursorFactory()
        ) {
            final int resultType = factory.getMetadata().getColumnType(1);
            if (resultType != type.columnType) {
                throw new AssertionError(TypeConformanceInvariants.context(type, "-", path, mode) + ": CASE WHEN ... THEN v END takes "
                        + ColumnType.nameOf(resultType) + ", not the type itself: no CASE function returns the type");
            }
        }
        final Map<String, long[]> values = readLaterValues(eng, ctx, sql);
        TypeConformanceInvariants.assertReadsBackAsWritten(type, selected.label, path, mode, selected.bits, values.get(selected.label));
        if (TypeConformanceInvariants.POLICY_NOT_NULL.equals(TypeConformanceInvariants.policyOf(type))) {
            return;
        }
        final Map<String, String> texts = readTexts(eng, ctx, sql);
        final String nullText = readTexts(eng, ctx, "SELECT k, v c FROM t").get("null");
        for (int i = 0, n = rows.size(); i < n; i++) {
            final String label = rows.getQuick(i).label;
            if (!label.equals(selected.label) && texts.containsKey(label) && nullText != null && !nullText.equals(texts.get(label))) {
                throw new AssertionError(TypeConformanceInvariants.context(type, label, path, mode)
                        + ": a row CASE does not select reads " + texts.get(label) + ", the NULL row " + nullText);
            }
        }
    }

    /**
     * Casts of a type registered later, from its declared relations (F89):
     * <ol>
     * <li>a cast to the type itself reads every row back as written;</li>
     * <li>a cast rule W, C or N admits resolves and runs, so a pair the rules admit without a
     * cast function fails here, naming the pair and the rule;</li>
     * <li>the NULL row converts as a NULL literal does;</li>
     * <li>a widening (rule W) of a type with a declared tier into an integer, temporal or float
     * target gives the row's value by that tier.</li>
     * </ol>
     * A cast the rules do not admit may still resolve (casts into text are no relation), so it is
     * not checked. The test fails once, listing every cast that breaks an invariant, so a type
     * registered without its cast functions sees the whole gap.
     */
    private void checkLaterCasts(String mode) throws Exception {
        final String path = "sql.cast";
        final StringSink steps = new StringSink();
        if (!createTables(engine, sqlExecutionContext, steps)) {
            throw new AssertionError(TypeConformanceInvariants.context(type, "-", path, mode) + ": " + steps.toString().trim().replace('\n', ' '));
        }
        TypeConformanceInvariants.nullRowWriteError(type, path, mode, steps);
        final ObjList<String> gaps = new ObjList<>();
        try {
            final String identityPair = type.label + " -> " + type.label + " (identity)";
            final String identitySql = "SELECT k, CAST(v AS " + type.ddl + ") c FROM t";
            final String identityError = compileError(engine, sqlExecutionContext, identitySql);
            if (identityError != null) {
                gaps.add(identityPair + ": no implementation: " + identityError);
            } else {
                try {
                    final Map<String, long[]> identity = readLaterValues(engine, sqlExecutionContext, identitySql);
                    for (int i = 0, n = rows.size(); i < n; i++) {
                        final TypeConformanceValues.Row row = rows.getQuick(i);
                        if (!row.isNull() && identity.containsKey(row.label)) {
                            TypeConformanceInvariants.assertReadsBackAsWritten(type, row.label, path, mode, row.bits, identity.get(row.label));
                        }
                    }
                } catch (AssertionError e) {
                    gaps.add(identityPair + ": " + e.getMessage());
                } catch (Throwable e) {
                    gaps.add(identityPair + ": fails at run time: " + e.getMessage());
                }
            }
            for (int t = 0, n = TypeConformanceTypes.ALL.size(); t < n; t++) {
                final TypeConformanceTypes.Entry target = TypeConformanceTypes.ALL.getQuick(t);
                if (target.isLater() || ColumnType.tagOf(target.columnType) == ColumnType.tagOf(type.columnType)) {
                    continue;
                }
                final String rule = TypeConformanceInvariants.castRule(type.columnType, target.columnType);
                if (rule == null) {
                    continue;
                }
                final String pair = type.label + " -> " + target.label + " (rule " + rule + ")";
                final String sql = "SELECT k, CAST(v AS " + target.ddl + ") c FROM t";
                final String error = compileError(engine, sqlExecutionContext, sql);
                if (error != null) {
                    gaps.add(pair + ": no implementation: " + error);
                    continue;
                }
                final Map<String, String> texts;
                try {
                    texts = readTexts(engine, sqlExecutionContext, sql);
                } catch (Throwable e) {
                    // a conversion may refuse a value at run time (out of range, not parsable); the
                    // value checks below need every row, so they do not run for this pair
                    continue;
                }
                if (texts.containsKey("null")) {
                    final String nullLiteral = readTexts(engine, sqlExecutionContext, "SELECT 'null' k, CAST(NULL AS " + target.ddl + ") c FROM long_sequence(1)").get("null");
                    if (nullLiteral != null && !nullLiteral.equals(texts.get("null"))) {
                        gaps.add(pair + ": the NULL row converts to " + texts.get("null") + ", a NULL literal to " + nullLiteral);
                    }
                }
                if ("W".equals(rule)) {
                    addWideningGaps(pair, target, readTargetValues(sql, target), gaps);
                }
            }
        } finally {
            dropTables(engine, sqlExecutionContext);
        }
        assertNoGaps(path, mode, "casts the relations admit", gaps);
    }

    /**
     * The FILL paths for a type registered later, over {@code g}: the low row at 0s, the high
     * row at 2s and the NULL row at 4s, sampled by the second. A path that reaches a guarded
     * site the type declares it is refused at must fail there with the site's refusal.
     * Otherwise the sampled rows read back as written, and the gaps at 1s and 3s read as the
     * fill implies: FILL(NULL) as the NULL row, FILL(PREV) as the row before, FILL(value) as
     * the high row whose printed form fills, and FILL(LINEAR) between its two neighbours by the
     * arithmetic tier, and as the NULL row next to a NULL under SENTINEL. FILL(LINEAR) applies
     * to the numeric tiers: a type without one must fail with an error naming the type.
     */
    private void checkLaterFill(CairoEngine eng, SqlExecutionContext ctx, String name, String mode) throws Exception {
        final String path = "sql." + name;
        final String fill;
        final String site;
        switch (name) {
            case "fill_null" -> {
                fill = "NULL";
                site = null;
            }
            case "fill_prev" -> {
                fill = "PREV";
                site = "SAMPLE BY FILL(PREV)";
            }
            case "fill_value" -> {
                fill = fillValue;
                site = "SAMPLE BY FILL(value)";
            }
            default -> {
                fill = "LINEAR";
                site = "SAMPLE BY FILL(LINEAR)";
            }
        }
        final ObjList<TypeConformanceValues.Row> gRows = fillRows();
        if (gRows == null) {
            throw new AssertionError(TypeConformanceInvariants.context(type, "-", path, mode) + ": the type has no low and high value rows");
        }
        final TypeConformanceValues.Row low = gRows.getQuick(0);
        final TypeConformanceValues.Row high = gRows.getQuick(2);
        final ObjList<long[]> bits = new ObjList<>();
        final ObjList<String> texts = new ObjList<>();
        String error = null;
        try (
                SqlCompiler compiler = eng.getSqlCompiler();
                RecordCursorFactory factory = compiler.compile("SELECT ts, last(v) v FROM g SAMPLE BY 1s FILL(" + fill + ")", ctx).getRecordCursorFactory();
                RecordCursor cursor = factory.getCursor(ctx)
        ) {
            final RecordMetadata metadata = factory.getMetadata();
            final Record record = cursor.getRecord();
            final StringSink sink = new StringSink();
            while (cursor.hasNext()) {
                bits.add(TypeConformanceValues.readValue(record, 1, type));
                sink.clear();
                CursorPrinter.printColumn(record, metadata, 1, sink);
                texts.add(sink.toString());
            }
        } catch (Throwable e) {
            error = String.valueOf(e.getMessage());
        }
        if (site != null && TypeConformanceInvariants.assertDeclaredRefusal(type, "-", path, mode, error, site)) {
            return;
        }
        if ("fill_linear".equals(name) && type.laterTier == null) {
            if (error == null || !error.contains(ColumnType.nameOf(type.columnType))) {
                throw new AssertionError(TypeConformanceInvariants.context(type, "-", path, mode)
                        + ": FILL(LINEAR) applies to the numeric tiers, so without one the query must fail naming the type, but "
                        + (error == null ? "it ran" : "it failed with: " + error));
            }
            return;
        }
        if (error != null) {
            throw new AssertionError(TypeConformanceInvariants.context(type, "-", path, mode) + ": " + error);
        }
        // the NULL row at 4s is refused under NOT_NULL, which leaves three seconds to sample
        if (bits.size() < 3) {
            throw new AssertionError(TypeConformanceInvariants.context(type, "-", path, mode) + ": " + bits.size() + " sampled rows, expected 3 or 5");
        }
        TypeConformanceInvariants.assertReadsBackAsWritten(type, low.label, path, mode, low.bits, bits.getQuick(0));
        TypeConformanceInvariants.assertReadsBackAsWritten(type, high.label, path, mode, high.bits, bits.getQuick(2));
        final boolean hasNullRow = bits.size() > 4;
        switch (name) {
            case "fill_null" -> {
                for (int gap = 1; gap < bits.size() && hasNullRow; gap += 2) {
                    if (!texts.getQuick(4).equals(texts.getQuick(gap))) {
                        throw new AssertionError(TypeConformanceInvariants.context(type, "gap" + gap, path, mode)
                                + ": the gap reads " + texts.getQuick(gap) + ", the NULL row " + texts.getQuick(4));
                    }
                }
            }
            case "fill_prev" -> {
                TypeConformanceInvariants.assertReadsBackAsWritten(type, "gap1", path, mode, low.bits, bits.getQuick(1));
                if (hasNullRow) {
                    TypeConformanceInvariants.assertReadsBackAsWritten(type, "gap3", path, mode, high.bits, bits.getQuick(3));
                }
            }
            case "fill_value" -> {
                TypeConformanceInvariants.assertReadsBackAsWritten(type, "gap1", path, mode, high.bits, bits.getQuick(1));
                if (hasNullRow) {
                    TypeConformanceInvariants.assertReadsBackAsWritten(type, "gap3", path, mode, high.bits, bits.getQuick(3));
                }
            }
            default -> {
                TypeConformanceInvariants.assertBetween(type, "gap1", path, mode, low.bits, bits.getQuick(1), high.bits);
                if (hasNullRow && TypeConformanceInvariants.POLICY_SENTINEL.equals(TypeConformanceInvariants.policyOf(type))
                        && !texts.getQuick(4).equals(texts.getQuick(3))) {
                    throw new AssertionError(TypeConformanceInvariants.context(type, "gap3", path, mode)
                            + ": next to a NULL the gap must read as NULL, " + texts.getQuick(4) + ", but reads " + texts.getQuick(3));
                }
            }
        }
    }

    /**
     * {@code sql.memoized} for a type registered later: unless the type declares it is refused at
     * the memoized virtual column, the projection caches the column and every read of it gives the
     * row's value as written.
     */
    private void checkLaterMemoized(CairoEngine eng, SqlExecutionContext ctx, String sql, String mode) {
        final String path = "sql.memoized";
        final Map<String, long[]> first = new HashMap<>();
        final Map<String, long[]> second = new HashMap<>();
        String error = null;
        try (
                SqlCompiler compiler = eng.getSqlCompiler();
                RecordCursorFactory factory = compiler.compile(sql, ctx).getRecordCursorFactory();
                RecordCursor cursor = factory.getCursor(ctx)
        ) {
            final Record record = cursor.getRecord();
            while (cursor.hasNext()) {
                final String label = record.getVarcharA(0).toString();
                first.put(label, TypeConformanceValues.readValue(record, 1, type));
                second.put(label, TypeConformanceValues.readValue(record, 3, type));
            }
        } catch (Throwable e) {
            error = String.valueOf(e.getMessage());
        }
        if (TypeConformanceInvariants.assertDeclaredRefusal(type, "-", path, mode, error, "memoized virtual column")) {
            return;
        }
        if (error != null) {
            throw new AssertionError(TypeConformanceInvariants.context(type, "-", path, mode) + ": " + error);
        }
        final String explain = observe(eng, ctx, "EXPLAIN " + sql).output;
        if (explain == null || !explain.contains("memoize(")) {
            throw new AssertionError(TypeConformanceInvariants.context(type, "-", path, mode) + ": the projection does not memoize the column: " + explain);
        }
        for (int i = 0, n = rows.size(); i < n; i++) {
            final TypeConformanceValues.Row row = rows.getQuick(i);
            if (!row.isNull() && first.containsKey(row.label)) {
                TypeConformanceInvariants.assertReadsBackAsWritten(type, row.label, path, mode, row.bits, first.get(row.label));
                TypeConformanceInvariants.assertReadsBackAsWritten(type, row.label, path, mode, row.bits, second.get(row.label));
            }
        }
    }

    /**
     * {@code sql.between_timestamp} and {@code sql.eq_null_double} for a type registered later.
     * Each must fail at its guarded site with the refusal when the type declares that site
     * refused. Otherwise {@code v = NULL} selects the rows {@code v IS NULL} selects, and
     * {@code between} selects, for a type with an integer tier, the rows that are not NULL and
     * whose value, read at the tier, lies between the two bounds.
     */
    private void checkLaterNullTest(CairoEngine eng, SqlExecutionContext ctx, String name, String sql, String mode) {
        final String path = "sql." + name;
        final boolean isBetween = "between_timestamp".equals(name);
        final String selected = selection(eng, ctx, sql);
        final String error = selected.startsWith("error: ") ? selected : null;
        if (TypeConformanceInvariants.assertDeclaredRefusal(type, "-", path, mode, error, isBetween ? "between" : "= NULL")) {
            return;
        }
        if (error != null) {
            throw new AssertionError(TypeConformanceInvariants.context(type, "-", path, mode) + ": " + error);
        }
        final String nullRows = selection(eng, ctx, "SELECT k FROM t WHERE v IS NULL");
        if (!isBetween) {
            if (!nullRows.equals(selected)) {
                throw new AssertionError(TypeConformanceInvariants.context(type, "-", path, mode)
                        + ": v = NULL selects " + selected + ", v IS NULL " + nullRows);
            }
            return;
        }
        if (type.laterTier == null || type.laterTier.startsWith("F")) {
            return;
        }
        final String nulls = "," + nullRows + ",";
        final String chosen = "," + selected + ",";
        final boolean isSigned = type.laterTier.startsWith("I");
        final int bits = Integer.parseInt(type.laterTier.substring(1));
        for (int i = 0, n = rows.size(); i < n; i++) {
            final TypeConformanceValues.Row row = rows.getQuick(i);
            if (row.isNull() || nulls.contains("," + row.label + ",") || bits > 64) {
                continue;
            }
            long value = row.bits[0];
            if (bits < 64) {
                value = isSigned ? value << (64 - bits) >> (64 - bits) : value & ((1L << bits) - 1);
            }
            final boolean isInRange = (isSigned || bits < 64 || value >= 0) && value >= Integer.MIN_VALUE && value <= Integer.MAX_VALUE;
            if (isInRange != chosen.contains("," + row.label + ",")) {
                throw new AssertionError(TypeConformanceInvariants.context(type, row.label, path, mode)
                        + ": between the bounds by tier " + type.laterTier + ": " + isInRange + ", selected: " + selected);
            }
        }
    }

    /**
     * The per-row paths for a type registered later, which has no literal: the value is a cast of
     * an INT constant or of NULL to the type, a bind variable of the type set from text, or, for
     * the key column, each value row as it prints.
     * <ul>
     * <li>SUBSAMPLE applies to the integral types: for a type of the INT relation kind, the value 2
     * selects the rows the INT 2 selects, and a NULL of the type is refused as not set under
     * SENTINEL and as less than the minimum under NONE, where it reads as 0; a type of any other
     * kind must be refused as no integer.</li>
     * <li>A WHERE bound of an integral type reads at its tier: 2 of the type, as a constant or a
     * bind variable, selects what the INT 2 selects, and a NULL of the type selects nothing under
     * SENTINEL and what 0 selects under NONE. A bound of another kind stays a filter and is not
     * checked.</li>
     * <li>The key column selects, for each value row, one row of the same value, whether the type
     * is a key column or, unlike its family's namesake, stays a filter.</li>
     * </ul>
     */
    private void checkLaterRowQuery(CairoEngine eng, SqlExecutionContext ctx, String name, String sql, String form, String mode) throws Exception {
        final String path = "sql." + name;
        if ("where_key".equals(name)) {
            checkLaterKey(eng, ctx, sql, mode);
            return;
        }
        final boolean isBind = FORM_BIND.equals(form);
        final String two = isBind ? boundSelection(eng, ctx, sql, "2") : selection(eng, ctx, sql.replace("<value>", "CAST(2 AS " + type.ddl + ")"));
        final String nullValue = isBind ? boundSelection(eng, ctx, sql, null) : selection(eng, ctx, sql.replace("<value>", "CAST(NULL AS " + type.ddl + ")"));
        final boolean isSubsample = name.startsWith("subsample_");
        if (!ColumnType.isIntegral(type.columnType)) {
            if (isSubsample && (!two.startsWith("error: ") || !two.contains("integer expected for"))) {
                throw new AssertionError(TypeConformanceInvariants.context(type, "two", path, mode)
                        + ": SUBSAMPLE takes integers, so a value of a type of another kind must be refused, but gives: " + two);
            }
            return;
        }
        final String expected = selection(eng, ctx, sql.replace("<value>", "2"));
        if (!expected.equals(two)) {
            throw new AssertionError(TypeConformanceInvariants.context(type, "two", path, mode)
                    + ": 2 of the type gives " + two + ", the INT 2 " + expected);
        }
        final String policy = TypeConformanceInvariants.policyOf(type);
        if (isSubsample) {
            final String refusal = switch (policy) {
                case TypeConformanceInvariants.POLICY_SENTINEL, TypeConformanceInvariants.POLICY_BITMAP ->
                        "must be set";
                case TypeConformanceInvariants.POLICY_NONE -> "must be at least";
                default -> "";
            };
            if (!nullValue.startsWith("error: ") || !nullValue.contains(refusal)) {
                throw new AssertionError(TypeConformanceInvariants.context(type, "null", path, mode)
                        + ": " + policy + ", a NULL of the type must be refused (" + refusal + "), but gives: " + nullValue);
            }
            return;
        }
        final String expectedNull = switch (policy) {
            case TypeConformanceInvariants.POLICY_NONE -> selection(eng, ctx, sql.replace("<value>", "0"));
            case TypeConformanceInvariants.POLICY_NOT_NULL -> nullValue;
            default -> "";
        };
        if (!expectedNull.equals(nullValue)) {
            throw new AssertionError(TypeConformanceInvariants.context(type, "null", path, mode)
                    + ": " + policy + ", a NULL bound of the type gives " + nullValue + ", expected " + expectedNull);
        }
    }

    // sql.where_key for a type registered later: each value row, as it prints, selects one row of that value
    private void checkLaterKey(CairoEngine eng, SqlExecutionContext ctx, String sql, String mode) throws Exception {
        final String path = "sql.where_key";
        final Map<String, String> texts = readTexts(eng, ctx, "SELECT k, v FROM t");
        final String nullRows = "," + selection(eng, ctx, "SELECT k FROM t WHERE v IS NULL") + ",";
        for (int i = 0, n = rows.size(); i < n; i++) {
            final TypeConformanceValues.Row row = rows.getQuick(i);
            final String text = texts.get(row.label);
            if (isNullRow(row, nullRows) || text == null) {
                continue;
            }
            final String selected = selection(eng, ctx, sql.replace("<value>", keyConstantOf(text)));
            final String label = selected.startsWith("b:") ? selected.substring(2) : null;
            if (label == null || selected.contains(",") || !text.equals(texts.get(label))) {
                throw new AssertionError(TypeConformanceInvariants.context(type, row.label, path, mode)
                        + ": the value " + text + " selects " + selected + ", expected the latest row of that value");
            }
        }
    }

    /**
     * Binds {@code $1} to the value as it prints, exports the rows of t equal to it to a Parquet
     * file with COPY, runs the export on a copy export job in this thread, and puts the export's
     * status and the labels read back from the file into {@code section}. Returns the error of the
     * binding or of the COPY statement itself, null when COPY queued the export.
     */
    @Nullable
    private String copyBind(@Nullable String text, String exportRoot, StringSink section) throws Exception {
        final String bindError = bind(sqlExecutionContext, text);
        if (bindError != null) {
            return "bind error: " + bindError;
        }
        try (CopyExportRequestJob job = new CopyExportRequestJob(engine)) {
            try (
                    RecordCursorFactory factory = select("COPY (SELECT k, v FROM t WHERE v = $1) TO 'copy_bind' WITH FORMAT parquet");
                    RecordCursor cursor = factory.getCursor(sqlExecutionContext)
            ) {
                // the cursor answers the export's id, which differs per run
                cursor.hasNext();
            } catch (Throwable e) {
                return "error: " + TypeConformanceRecording.escape(String.valueOf(e.getMessage())).replace('\n', ' ');
            }
            // the bound value now lives in the export's snapshot only
            sqlExecutionContext.getBindVariableService().clear();
            while (job.run()) {
                // one export per run
            }
        }
        section.put(TypeConformanceRecording.escape(printQuietly("SELECT status, message FROM \"" + configuration.getSystemTableNamePrefix() + "copy_export_log\" LIMIT -1")));
        section.put(TypeConformanceRecording.escape(printQuietly("SELECT k FROM read_parquet('" + exportRoot + Files.SEPARATOR + "copy_bind.parquet')")));
        return null;
    }

    // a query's output, or its error as one line
    private String printQuietly(String sql) {
        try {
            final StringSink sink = new StringSink();
            TestUtils.printSql(engine, sqlExecutionContext, sql, sink);
            return sink.toString();
        } catch (Throwable e) {
            return "error: " + String.valueOf(e.getMessage()).replace('\n', ' ') + '\n';
        }
    }

    // the error compiling the query raises, null when it compiles
    @Nullable
    private String compileError(CairoEngine eng, SqlExecutionContext ctx, String sql) {
        try (
                SqlCompiler compiler = eng.getSqlCompiler();
                RecordCursorFactory ignore = compiler.compile(sql, ctx).getRecordCursorFactory()
        ) {
            return null;
        } catch (Throwable e) {
            return String.valueOf(e.getMessage());
        }
    }

    /**
     * Defines the bind variable {@code $1} with the type, as its type driver defines one, and sets
     * it from the value's text, NULL for null; returns the error either step raised, null when
     * both succeed.
     */
    @Nullable
    private String bind(SqlExecutionContext ctx, @Nullable String text) {
        final BindVariableService service = ctx.getBindVariableService();
        service.clear();
        try {
            ColumnType.getTypeDriver(type.columnType).defineBindVariable(service, 0, type.columnType, 0);
            service.setStr(0, text);
            return null;
        } catch (Throwable e) {
            return TypeConformanceRecording.escape(String.valueOf(e.getMessage())).replace('\n', ' ');
        }
    }

    // a printed value as a constant to compare the column with: unquoted for a number type, else quoted
    private String keyConstantOf(@Nullable String text) {
        final RelationKind kind = TypeConformanceInvariants.kindOf(type.columnType);
        if (text != null && kind != RelationKind.INT && kind != RelationKind.FLOAT) {
            return "'" + text.replace("'", "''") + "'";
        }
        return plainConstantOf(text);
    }

    // a value row as a constant of the type: its literal, and for the NULL row a NULL of the type
    private String constantOf(TypeConformanceValues.Row row) {
        return row.isNull() ? "CAST(NULL AS " + type.ddl + ")" : row.literal;
    }

    /**
     * Creates and fills the tables of one mode; on a failure (a type that cannot be stored)
     * returns false with the error in {@code steps}.
     */
    private boolean createTables(CairoEngine eng, SqlExecutionContext ctx, StringSink steps) {
        final String columns = " (k VARCHAR, v " + type.ddl + ", ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL";
        for (String table : new String[]{"t", "t2", "u", "n", "f", "g"}) {
            execute(eng, ctx, "CREATE TABLE " + table + columns, "create", steps);
            if (steps.length() > 0) {
                return false;
            }
        }
        final int n = rows.size();
        TypeConformanceValues.writeRows(eng, ctx, "t", rows, "", 0, 0, n, 1, true, steps);
        TypeConformanceValues.writeRows(eng, ctx, "t2", rows, "a:", 0, 0, n, 1, true, steps);
        TypeConformanceValues.writeRows(eng, ctx, "t2", rows, "b:", n * TypeConformanceValues.SECOND, 0, n, 1, true, steps);
        TypeConformanceValues.writeRows(eng, ctx, "u", rows, "", 0, 0, n, 2, true, steps);
        final TypeConformanceValues.Row nullRow = new TypeConformanceValues.Row("-", "NULL");
        final TypeConformanceValues.Row valueRow = valueRow();
        if (valueRow != null) {
            // groups: A starts with NULL, B ends with NULL, C is NULL only
            final ObjList<TypeConformanceValues.Row> groups = new ObjList<>();
            groups.add(relabel(nullRow, "A"));
            groups.add(relabel(valueRow, "A"));
            groups.add(relabel(valueRow, "B"));
            groups.add(relabel(nullRow, "B"));
            groups.add(relabel(nullRow, "C"));
            groups.add(relabel(nullRow, "C"));
            TypeConformanceValues.writeRows(eng, ctx, "n", groups, "", 0, 0, groups.size(), 1, true, steps);
            // a leading NULL, the value two seconds later and a NULL after it, with gaps between
            final ObjList<TypeConformanceValues.Row> gaps = new ObjList<>();
            gaps.add(relabel(nullRow, "r0"));
            gaps.add(null);
            gaps.add(relabel(valueRow, "r2"));
            gaps.add(null);
            gaps.add(relabel(nullRow, "r4"));
            TypeConformanceValues.writeRows(eng, ctx, "f", gaps, "", 0, 0, gaps.size(), 2, true, steps);
        }
        final ObjList<TypeConformanceValues.Row> gRows = fillRows();
        fillValue = "NULL";
        if (gRows != null) {
            TypeConformanceValues.writeRows(eng, ctx, "g", gRows, "", 0, 0, gRows.size(), 2, true, steps);
            fillValue = fillToken(eng, ctx);
        }
        return true;
    }

    private void dropTables(CairoEngine eng, SqlExecutionContext ctx) {
        final StringSink ignored = new StringSink();
        for (String table : new String[]{"t", "t2", "u", "n", "f", "g"}) {
            execute(eng, ctx, "DROP TABLE IF EXISTS " + table, "drop", ignored);
        }
    }

    /**
     * The rows of {@code g}: a low value, a gap, the high value, a gap and the NULL row, written
     * at 0, 2 and 4 seconds. High is {@code max} or, without one, the literal row the queries use
     * ({@code one} for a type registered later); low is {@code min} or, without one, the first
     * other value row ({@code zero} for a type registered later). Null when the type has no two
     * such rows.
     */
    @Nullable
    private ObjList<TypeConformanceValues.Row> fillRows() {
        TypeConformanceValues.Row high = rowLabelled("max");
        if (high == null) {
            high = type.isLater() ? rowLabelled("one") : valueRow();
        }
        TypeConformanceValues.Row low = rowLabelled("min");
        if (low == null) {
            low = type.isLater() ? rowLabelled("zero") : null;
            for (int i = 0, n = rows.size(); i < n && low == null; i++) {
                final TypeConformanceValues.Row row = rows.getQuick(i);
                if (!row.isNull() && row.literal != null && row != high) {
                    low = row;
                }
            }
        }
        final TypeConformanceValues.Row nullRow = rowLabelled("null");
        if (high == null || low == null || nullRow == null) {
            return null;
        }
        final ObjList<TypeConformanceValues.Row> fillRows = new ObjList<>();
        fillRows.add(TypeConformanceValues.Row.relabel(low, "r0"));
        fillRows.add(null);
        fillRows.add(TypeConformanceValues.Row.relabel(high, "r2"));
        fillRows.add(null);
        fillRows.add(TypeConformanceValues.Row.relabel(nullRow, "r4"));
        return fillRows;
    }

    // the FILL(value) token: the high row of g as it prints, a number as it is, any other text quoted
    private String fillToken(CairoEngine eng, SqlExecutionContext ctx) {
        final String text;
        try {
            text = readTexts(eng, ctx, "SELECT k, v FROM g WHERE k = 'r2'").get("r2");
        } catch (Throwable e) {
            return "NULL";
        }
        return plainConstantOf(text);
    }

    // the first row the kit writes as a value (not NULL)
    @Nullable
    private TypeConformanceValues.Row firstValueRow() {
        for (int i = 0, n = rows.size(); i < n; i++) {
            if (!rows.getQuick(i).isNull()) {
                return rows.getQuick(i);
            }
        }
        return null;
    }

    private boolean hasRow(String label) {
        for (int i = 0, n = rows.size(); i < n; i++) {
            if (label.equals(rows.getQuick(i).label)) {
                return true;
            }
        }
        return false;
    }

    // under SENTINEL the sentinel-pattern row is NULL, so it converts as NULL, not as its bits
    private boolean isSentinelUnderSentinel(TypeConformanceValues.Row row) {
        return "sentinel".equals(row.label) && TypeConformanceInvariants.POLICY_SENTINEL.equals(TypeConformanceInvariants.policyOf(type));
    }

    @Nullable
    private TypeConformanceTypes.Entry kitTypeOf(int columnType) {
        for (int i = 0, n = TypeConformanceTypes.ALL.size(); i < n; i++) {
            final TypeConformanceTypes.Entry entry = TypeConformanceTypes.ALL.getQuick(i);
            if (entry.columnType == columnType && ColumnType.isPersisted(ColumnType.tagOf(columnType))) {
                return entry;
            }
        }
        return null;
    }

    private Observation observe(CairoEngine eng, SqlExecutionContext ctx, String sql) {
        final Observation observation = new Observation();
        final StringSink sink = new StringSink();
        try (
                SqlCompiler compiler = eng.getSqlCompiler();
                RecordCursorFactory factory = compiler.compile(sql, ctx).getRecordCursorFactory()
        ) {
            final RecordMetadata metadata = factory.getMetadata();
            observation.isRandomAccess = factory.recordCursorSupportsRandomAccess();
            final int timestampIndex = metadata.getTimestampIndex();
            observation.timestamp = timestampIndex > -1 ? metadata.getColumnName(timestampIndex) : null;
            observation.scanDirection = factory.getScanDirection();
            try (RecordCursor cursor = factory.getCursor(ctx)) {
                CursorPrinter.println(metadata, sink);
                final Record record = cursor.getRecord();
                while (cursor.hasNext()) {
                    TestUtils.println(record, metadata, sink);
                }
                cursor.toTop();
                observation.isSizeKnown = cursor.size() != -1;
            }
            observation.rawOutput = sink.toString();
            observation.output = TypeConformanceRecording.escape(sink);
        } catch (Throwable e) {
            observation.error = "error: " + TypeConformanceRecording.escape(String.valueOf(e.getMessage()));
        }
        return observation;
    }

    /**
     * The queries of this class, {name, sql}; the path of each is {@code sql.<name>}.
     */
    private String[][] queries() {
        final TypeConformanceValues.Row valueRow = valueRow();
        final String literal = valueRow != null ? valueRow.literal : "NULL";
        return new String[][]{
                {"filter_eq", "SELECT k, v FROM t WHERE v = " + literal},
                {"filter_ne", "SELECT k, v FROM t WHERE v != " + literal},
                {"filter_null", "SELECT k, v FROM t WHERE v IS NULL"},
                {"filter_not_null", "SELECT k, v FROM t WHERE v IS NOT NULL"},
                {"filter_lt", "SELECT k, v FROM t WHERE v < " + literal},
                {"filter_ge", "SELECT k, v FROM t WHERE v >= " + literal},
                {"order_asc", "SELECT k, v FROM t ORDER BY v, k"},
                {"order_desc", "SELECT k, v FROM t ORDER BY v DESC, k"},
                {"group_by", "SELECT v, count() c, min(ts) f FROM t2 GROUP BY v ORDER BY f"},
                {"join_inner", "SELECT t.k tk, u.k uk, t.v FROM t JOIN u ON t.v = u.v ORDER BY tk, uk"},
                {"join_left", "SELECT t.k tk, u.k uk, u.v uv FROM t LEFT JOIN u ON t.v = u.v ORDER BY tk, uk"},
                {"join_left_null", "SELECT t.k tk, u.v uv FROM t LEFT JOIN u ON t.k = u.k ORDER BY tk"},
                {"union_all", "SELECT k, v FROM t UNION ALL SELECT k, v FROM u"},
                {"union_null", "SELECT k, v FROM u UNION ALL SELECT 'null_branch' k, NULL v FROM long_sequence(1)"},
                {"case_no_else", "SELECT k, CASE WHEN k = 'max' THEN v END c FROM t"},
                {"case_else", "SELECT k, CASE WHEN v IS NULL THEN " + literal + " ELSE v END c FROM t"},
                {"lag", "SELECT k, v, lag(v) OVER (ORDER BY ts) p FROM t"},
                {"sample_by", "SELECT ts, first(v) f, last(v) l, count() c FROM t SAMPLE BY 2s"},
                {"first_last", "SELECT k, first(v) f, last(v) l FROM n GROUP BY k ORDER BY k"},
                {"first_not_null", "SELECT k, first_not_null(v) f, last_not_null(v) l FROM n GROUP BY k ORDER BY k"},
                {"fill_prev", "SELECT ts, last(v) v FROM f SAMPLE BY 1s FILL(PREV)"},
                {"fill_null", "SELECT ts, last(v) v FROM g SAMPLE BY 1s FILL(NULL)"},
                {"fill_value", "SELECT ts, last(v) v FROM g SAMPLE BY 1s FILL(" + (fillValue != null ? fillValue : "NULL") + ")"},
                {"fill_linear", "SELECT ts, last(v) v FROM g SAMPLE BY 1s FILL(LINEAR)"},
                // an alias of a plain column is a column selection; the identity cast is a function
                {"memoized", "SELECT k, CAST(v AS " + type.ddl + ") a, a a2, a a3 FROM t"},
                {"between_timestamp", "SELECT k, v FROM t WHERE v BETWEEN (-2147483648)::TIMESTAMP AND 2147483647::TIMESTAMP"},
                {"eq_null_double", "SELECT k, v FROM t WHERE v = NULL"},
                {"latest_on", "SELECT k, v FROM (t2 LATEST ON ts PARTITION BY v) ORDER BY k"},
        };
    }

    // label -> the value of column 1 as the type's rows hold it (TypeConformanceValues.readValue)
    private Map<String, long[]> readLaterValues(CairoEngine eng, SqlExecutionContext ctx, String sql) throws Exception {
        final Map<String, long[]> values = new HashMap<>();
        try (
                SqlCompiler compiler = eng.getSqlCompiler();
                RecordCursorFactory factory = compiler.compile(sql, ctx).getRecordCursorFactory();
                RecordCursor cursor = factory.getCursor(ctx)
        ) {
            final Record record = cursor.getRecord();
            while (cursor.hasNext()) {
                values.put(record.getVarcharA(0).toString(), TypeConformanceValues.readValue(record, 1, type));
            }
        }
        return values;
    }

    // label -> the value of column 1 as raw bits of the target's width
    private Map<String, long[]> readTargetValues(String sql, TypeConformanceTypes.Entry target) throws Exception {
        final Map<String, long[]> values = new HashMap<>();
        final int width = TypeConformanceInvariants.widthOf(target.columnType);
        if (width <= 0) {
            return values;
        }
        try (
                RecordCursorFactory factory = select(sql);
                RecordCursor cursor = factory.getCursor(sqlExecutionContext)
        ) {
            final Record record = cursor.getRecord();
            while (cursor.hasNext()) {
                values.put(record.getVarcharA(0).toString(), TypeConformanceValues.readBits(record, 1, width));
            }
        } catch (Throwable e) {
            // a conversion that refuses a value at run time gives no values to compare
            values.clear();
        }
        return values;
    }

    // label -> column 1 printed
    private Map<String, String> readTexts(CairoEngine eng, SqlExecutionContext ctx, String sql) throws Exception {
        final Map<String, String> texts = new HashMap<>();
        try (
                SqlCompiler compiler = eng.getSqlCompiler();
                RecordCursorFactory factory = compiler.compile(sql, ctx).getRecordCursorFactory();
                RecordCursor cursor = factory.getCursor(ctx)
        ) {
            final RecordMetadata metadata = factory.getMetadata();
            final Record record = cursor.getRecord();
            final StringSink sink = new StringSink();
            while (cursor.hasNext()) {
                sink.clear();
                CursorPrinter.printColumn(record, metadata, 1, sink);
                texts.put(record.getVarcharA(0).toString(), sink.toString());
            }
        }
        return texts;
    }

    /**
     * One line per value row with a literal: the label, then the labels of the rows the query
     * selects with the row's value in place of {@code <value>}, comma-separated, or the query's
     * error. A query that runs is also asserted with the {@code returns} battery.
     */
    private String rowSection(CairoEngine eng, SqlExecutionContext ctx, String path, String mode, String template, String form) throws Exception {
        final StringSink section = new StringSink();
        final Map<String, String> texts = FORM_CONST.equals(form) ? null : readTexts(eng, ctx, "SELECT k, v FROM t");
        final String nullRows = FORM_CONST.equals(form) ? null : "," + selection(eng, ctx, "SELECT k FROM t WHERE v IS NULL") + ",";
        for (int i = 0, n = rows.size(); i < n; i++) {
            final TypeConformanceValues.Row row = rows.getQuick(i);
            if (row.literal == null) {
                // a row written raw has no literal to put in a query
                continue;
            }
            section.put(row.label).put('\t');
            final String sql;
            switch (form) {
                case FORM_CONST -> sql = template.replace("<value>", constantOf(row));
                case FORM_TEXT ->
                        sql = template.replace("<value>", isNullRow(row, nullRows) ? "NULL" : keyConstantOf(texts.get(row.label)));
                default -> {
                    sql = template.replace("<value>", "$1");
                    final String bindError = bind(ctx, isNullRow(row, nullRows) ? null : texts.get(row.label));
                    if (bindError != null) {
                        section.put("bind error: ").put(bindError).put('\n');
                        continue;
                    }
                }
            }
            final Observation observation = observe(eng, ctx, sql);
            if (observation.error != null) {
                section.put(observation.error.replace('\n', ' '));
            } else {
                final String battery = assertReturns(eng, ctx, path, mode, sql, observation, eng == engine);
                section.put(labelsOf(observation));
                if (battery != null) {
                    section.put('\t').put(battery);
                }
            }
            section.put('\n');
        }
        ctx.getBindVariableService().clear();
        return section.toString();
    }

    private TypeConformanceValues.Row relabel(TypeConformanceValues.Row row, String label) {
        return new TypeConformanceValues.Row(label, row.literal);
    }

    @Nullable
    private TypeConformanceValues.Row rowLabelled(String label) {
        for (int i = 0, n = rows.size(); i < n; i++) {
            if (label.equals(rows.getQuick(i).label)) {
                return rows.getQuick(i);
            }
        }
        return null;
    }

    /**
     * Runs {@code body} in every mode: single-threaded on the test's engine with interpreted
     * filters, then parallel on a worker pool's engine with interpreted and compiled filters.
     */
    private void runModes(ModeBody body) throws Exception {
        configure(sqlExecutionContext, MODES[0]);
        body.run(engine, sqlExecutionContext, MODES[0]);
        final WorkerPool pool = new TestWorkerPool(4, TestUtils.getWorkerPoolMode(TestUtils.generateRandom(LOG)));
        TestUtils.execute(
                pool,
                (eng, compiler, ctx) -> {
                    for (int m = 1; m < MODES.length; m++) {
                        configure(ctx, MODES[m]);
                        body.run(eng, ctx, MODES[m]);
                    }
                },
                configuration,
                LOG
        );
    }

    // the selection of a query with $1 defined as the type and set from text, or the binding's error
    private String boundSelection(CairoEngine eng, SqlExecutionContext ctx, String template, @Nullable String text) {
        final String bindError = bind(ctx, text);
        try {
            return bindError != null ? "bind error: " + bindError : selection(eng, ctx, template.replace("<value>", "$1"));
        } finally {
            ctx.getBindVariableService().clear();
        }
    }

    // the labels of the rows a query selects (its first column), comma-separated, or its error
    private String selection(CairoEngine eng, SqlExecutionContext ctx, String sql) {
        final Observation observation = observe(eng, ctx, sql);
        if (observation.error != null) {
            return observation.error.replace('\n', ' ');
        }
        return labelsOf(observation);
    }

    private long[] sentinelBits() {
        for (int i = 0, n = rows.size(); i < n; i++) {
            if ("sentinel".equals(rows.getQuick(i).label)) {
                return rows.getQuick(i).bits;
            }
        }
        return null;
    }

    /**
     * The row whose literal the queries use: {@code max}, else the first non-NULL row with a
     * literal; null for a type registered later, which has no literal.
     */
    @Nullable
    private TypeConformanceValues.Row valueRow() {
        TypeConformanceValues.Row first = null;
        for (int i = 0, n = rows.size(); i < n; i++) {
            final TypeConformanceValues.Row row = rows.getQuick(i);
            if (row.literal == null || row.isNull()) {
                continue;
            }
            if ("max".equals(row.label)) {
                return row;
            }
            if (first == null) {
                first = row;
            }
        }
        return first;
    }

    @FunctionalInterface
    private interface ModeBody {
        void run(CairoEngine eng, SqlExecutionContext ctx, String mode) throws Exception;
    }

    private static class Observation {
        String error;
        boolean isRandomAccess;
        boolean isSizeKnown;
        String output;
        String rawOutput;
        int scanDirection;
        String timestamp;

        String section() {
            if (error != null) {
                return error + '\n';
            }
            return "props: random_access=" + isRandomAccess
                    + " size=" + (isSizeKnown ? "known" : "unknown")
                    + " timestamp=" + (timestamp == null ? "none" : timestamp + ':' + direction())
                    + '\n' + output;
        }

        private String direction() {
            return switch (scanDirection) {
                case RecordCursorFactory.SCAN_DIRECTION_FORWARD -> "asc";
                case RecordCursorFactory.SCAN_DIRECTION_BACKWARD -> "desc";
                default -> "other";
            };
        }
    }

    // Masks applied to the output before it is compared: the database root of the test run
    // becomes <dbRoot>. Casts abbreviate one refusal (see abbreviate()).
    // recordings: start
    static {
        rec("BOOLEAN", """
                ## cast
                target\tmin|max|null
                BOOLEAN\tfalse|true|false
                BYTE\t0|1|0
                SHORT\t0|1|0
                CHAR\tF|T|F
                INT\t0|1|0
                LONG\t0|1|0
                DATE\t1970-01-01T00:00:00.000Z|1970-01-01T00:00:00.001Z|1970-01-01T00:00:00.000Z
                TIMESTAMP\t1970-01-01T00:00:00.000000Z|1970-01-01T00:00:00.000001Z|1970-01-01T00:00:00.000000Z
                FLOAT\t0.0|1.0|0.0
                DOUBLE\t0.0|1.0|0.0
                STRING\tfalse|true|false
                SYMBOL\tfalse|true|false
                LONG256\t0x00|0x01|0x00
                GEOBYTE\tno cast [10]
                GEOSHORT\tno cast [10]
                GEOINT\tno cast [10]
                GEOLONG\tno cast [10]
                BINARY\terror: [20] unsupported cast
                UUID\tno cast [10]
                LONG128\terror: [20] unsupported cast
                IPv4\tno cast [10]
                VARCHAR\tfalse|true|false
                DOUBLE[]\tno cast [10]
                DECIMAL8\tno cast [10]
                DECIMAL16\tno cast [10]
                DECIMAL32\tno cast [10]
                DECIMAL64\tno cast [10]
                DECIMAL128\tno cast [10]
                DECIMAL256\tno cast [10]
                INTERVAL\terror: [20] unsupported cast
                VARCHAR_SLICE\terror: [20] unsupported cast
                TIMESTAMP_NS\t1970-01-01T00:00:00.000000000Z|1970-01-01T00:00:00.000000001Z|1970-01-01T00:00:00.000000000Z
                GEOHASH(1c)\tno cast [10]
                GEOHASH(8b)\tno cast [10]
                GEOHASH(31b)\tno cast [10]
                GEOHASH(12c)\tno cast [10]
                DECIMAL(5,2)\tno cast [10]
                DECIMAL(18,3)\tno cast [10]
                DOUBLE[][]\tno cast [10]
                INTERVAL(us)\terror: [20] unsupported cast
                INTERVAL(ns)\terror: [20] unsupported cast
                ## filter_eq
                props: random_access=true size=unknown timestamp=none
                k\tv
                max\ttrue
                ## filter_ne
                props: random_access=true size=unknown timestamp=none
                k\tv
                min\tfalse
                null\tfalse
                ## filter_null
                props: random_access=true size=known timestamp=none
                k\tv
                ## filter_not_null
                props: random_access=true size=known timestamp=none
                k\tv
                min\tfalse
                max\ttrue
                null\tfalse
                ## filter_lt
                error: [27] there is no matching operator `<` with the argument types: BOOLEAN < BOOLEAN
                ## filter_ge
                error: [27] there is no matching operator `>=` with the argument types: BOOLEAN >= BOOLEAN
                ## order_asc
                props: random_access=true size=known timestamp=none
                k\tv
                min\tfalse
                null\tfalse
                max\ttrue
                ## order_desc
                props: random_access=true size=known timestamp=none
                k\tv
                max\ttrue
                min\tfalse
                null\tfalse
                ## group_by
                props: random_access=true size=known timestamp=f:asc
                v\tc\tf
                false\t4\t1970-01-01T00:00:00.000000Z
                true\t2\t1970-01-01T00:00:01.000000Z
                ## join_inner
                props: random_access=true size=unknown timestamp=none
                tk\tuk\tv
                min\tmin\tfalse
                min\tnull\tfalse
                null\tmin\tfalse
                null\tnull\tfalse
                ## join_left
                props: random_access=true size=unknown timestamp=none
                tk\tuk\tuv
                max\t\tfalse
                min\tmin\tfalse
                min\tnull\tfalse
                null\tmin\tfalse
                null\tnull\tfalse
                ## join_left_null
                props: random_access=true size=unknown timestamp=none
                tk\tuv
                max\tfalse
                min\tfalse
                null\tfalse
                ## union_all
                props: random_access=false size=known timestamp=none
                k\tv
                min\tfalse
                max\ttrue
                null\tfalse
                min\tfalse
                null\tfalse
                ## union_null
                props: random_access=false size=known timestamp=none
                k\tv
                min\tfalse
                null\tfalse
                null_branch\tfalse
                ## case_no_else
                props: random_access=true size=known timestamp=none
                k\tc
                min\tfalse
                max\ttrue
                null\tfalse
                ## case_else
                props: random_access=true size=known timestamp=none
                k\tc
                min\ttrue
                max\ttrue
                null\ttrue
                ## lag
                error: [13] there is no matching function `lag` with the argument types: (BOOLEAN)
                ## sample_by
                props: random_access=true size=known timestamp=ts:asc
                ts\tf\tl\tc
                1970-01-01T00:00:00.000000Z\tfalse\ttrue\t2
                1970-01-01T00:00:02.000000Z\tfalse\tfalse\t1
                ## first_last
                props: random_access=true size=known timestamp=none
                k\tf\tl
                A\tfalse\ttrue
                B\ttrue\tfalse
                C\tfalse\tfalse
                ## first_not_null
                error: [10] there is no matching function `first_not_null` with the argument types: (BOOLEAN)
                ## fill_prev
                props: random_access=false size=unknown timestamp=ts:asc
                ts\tv
                1970-01-01T00:00:00.000000Z\tfalse
                1970-01-01T00:00:01.000000Z\tfalse
                1970-01-01T00:00:02.000000Z\ttrue
                1970-01-01T00:00:03.000000Z\ttrue
                1970-01-01T00:00:04.000000Z\tfalse
                ## latest_on
                props: random_access=true size=known timestamp=none
                k\tv
                b:max\ttrue
                b:null\tfalse
                ## fill_null
                error: [46] fill value of type NULL cannot fill column of type BOOLEAN
                ## fill_value
                error: [46] fill value of type STRING cannot fill column of type BOOLEAN
                ## fill_linear
                error: [11] Unsupported interpolation type: BOOLEAN
                ## subsample_stride
                min\terror: [38] integer expected for stride
                max\terror: [38] integer expected for stride
                null\terror: [38] integer expected for stride
                ## subsample_target
                min\terror: [38] integer expected for target point count
                max\terror: [38] integer expected for target point count
                null\terror: [38] integer expected for target point count
                ## memoized
                props: random_access=true size=known timestamp=none
                k\ta\ta2\ta3
                min\tfalse\tfalse\tfalse
                max\ttrue\ttrue\ttrue
                null\tfalse\tfalse\tfalse
                plan memoizes: true
                ## where_bound_const
                min\terror: [31] there is no matching operator `>=` with the argument types: LONG >= BOOLEAN
                max\terror: [31] there is no matching operator `>=` with the argument types: LONG >= BOOLEAN
                null\terror: [31] there is no matching operator `>=` with the argument types: LONG >= BOOLEAN
                ## where_bound_bind
                min\terror: [31] there is no matching operator `>=` with the argument types: LONG >= BOOLEAN
                max\terror: [31] there is no matching operator `>=` with the argument types: LONG >= BOOLEAN
                null\terror: [31] there is no matching operator `>=` with the argument types: LONG >= BOOLEAN
                ## where_key
                min\terror: [25] there is no matching operator `=` with the argument types: BOOLEAN = STRING
                max\terror: [25] there is no matching operator `=` with the argument types: BOOLEAN = STRING
                null\t
                ## copy_bind
                status\tmessage
                finished\t
                k
                max
                ## between_timestamp
                error: [27] there is no matching operator `between` with the argument type: BOOLEAN
                ## eq_null_double
                props: random_access=true size=known timestamp=none
                k\tv
                ## bind_value
                setBoolean\ttrue
                setByte\terror: [0] bind variable at 0 is defined as BOOLEAN and cannot accept BYTE
                setShort\terror: [0] bind variable at 0 is defined as BOOLEAN and cannot accept SHORT
                setChar\terror: [0] bind variable at 0 is defined as BOOLEAN and cannot accept CHAR
                setInt\terror: [0] bind variable at 0 is defined as BOOLEAN and cannot accept INT
                setLong\terror: [0] bind variable at 0 is defined as BOOLEAN and cannot accept LONG
                setFloat\terror: [0] bind variable at 0 is defined as BOOLEAN and cannot accept FLOAT
                setDouble\terror: [0] bind variable at 0 is defined as BOOLEAN and cannot accept DOUBLE
                setDate\terror: [0] bind variable at 0 is defined as BOOLEAN and cannot accept DATE
                setTimestamp\terror: [0] bind variable at 0 is defined as BOOLEAN and cannot accept TIMESTAMP
                setStr\tfalse
                setVarchar\tfalse
                setLong256\terror: [0] bind variable at 0 is defined as BOOLEAN and cannot accept LONG256
                setUuid\terror: [0] bind variable at 0 is defined as BOOLEAN and cannot accept UUID
                setArray\terror: [0] bind variable at 0 is defined as BOOLEAN and cannot accept ARRAY
                ## window_anchor
                error: create view: [147] ANCHOR EXPRESSION must return TIMESTAMP, LONG, or INT; got BOOLEAN
                """);
        rec("BYTE", """
                ## filter_eq
                props: random_access=true size=unknown timestamp=none
                k\tv
                max\t127
                ## filter_ne
                props: random_access=true size=unknown timestamp=none
                k\tv
                min\t-128
                other_null\t-1
                null\t0
                ## filter_null
                props: random_access=true size=known timestamp=none
                k\tv
                ## filter_not_null
                props: random_access=true size=known timestamp=none
                k\tv
                min\t-128
                max\t127
                other_null\t-1
                null\t0
                ## filter_lt
                props: random_access=true size=unknown timestamp=none
                k\tv
                min\t-128
                other_null\t-1
                null\t0
                ## filter_ge
                props: random_access=true size=unknown timestamp=none
                k\tv
                max\t127
                ## order_asc
                props: random_access=true size=known timestamp=none
                k\tv
                min\t-128
                other_null\t-1
                null\t0
                max\t127
                ## order_desc
                props: random_access=true size=known timestamp=none
                k\tv
                max\t127
                null\t0
                other_null\t-1
                min\t-128
                ## group_by
                props: random_access=true size=known timestamp=f:asc
                v\tc\tf
                -128\t2\t1970-01-01T00:00:00.000000Z
                127\t2\t1970-01-01T00:00:01.000000Z
                -1\t2\t1970-01-01T00:00:02.000000Z
                0\t2\t1970-01-01T00:00:03.000000Z
                ## join_inner
                props: random_access=true size=unknown timestamp=none
                tk\tuk\tv
                min\tmin\t-128
                other_null\tother_null\t-1
                ## join_left
                props: random_access=true size=unknown timestamp=none
                tk\tuk\tuv
                max\t\t0
                min\tmin\t-128
                null\t\t0
                other_null\tother_null\t-1
                ## join_left_null
                props: random_access=true size=unknown timestamp=none
                tk\tuv
                max\t0
                min\t-128
                null\t0
                other_null\t-1
                ## union_all
                props: random_access=false size=known timestamp=none
                k\tv
                min\t-128
                max\t127
                other_null\t-1
                null\t0
                min\t-128
                other_null\t-1
                ## union_null
                props: random_access=false size=known timestamp=none
                k\tv
                min\t-128
                other_null\t-1
                null_branch\t0
                ## case_no_else
                props: random_access=true size=known timestamp=none
                k\tc
                min\t0
                max\t127
                other_null\t0
                null\t0
                ## case_else
                props: random_access=true size=known timestamp=none
                k\tc
                min\t-128
                max\t127
                other_null\t-1
                null\t127
                ## lag
                props: random_access=false size=known timestamp=none
                k\tv\tp
                min\t-128\tnull
                max\t127\t-128
                other_null\t-1\t127
                null\t0\t-1
                ## sample_by
                props: random_access=true size=known timestamp=ts:asc
                ts\tf\tl\tc
                1970-01-01T00:00:00.000000Z\t-128\t127\t2
                1970-01-01T00:00:02.000000Z\t-1\t0\t2
                ## first_last
                props: random_access=true size=known timestamp=none
                k\tf\tl
                A\t0\t127
                B\t127\t0
                C\t0\t0
                ## first_not_null
                props: random_access=true size=known timestamp=none
                k\tf\tl
                A\t0\t127
                B\t127\t0
                C\t0\t0
                ## fill_prev
                props: random_access=false size=unknown timestamp=ts:asc
                ts\tv
                1970-01-01T00:00:00.000000Z\t0
                1970-01-01T00:00:01.000000Z\t0
                1970-01-01T00:00:02.000000Z\t127
                1970-01-01T00:00:03.000000Z\t127
                1970-01-01T00:00:04.000000Z\t0
                ## latest_on
                props: random_access=true size=known timestamp=none
                k\tv
                b:max\t127
                b:min\t-128
                b:null\t0
                b:other_null\t-1
                ## cast
                target\tmin|max|other_null|null
                BOOLEAN\ttrue|true|true|false
                BYTE\t-128|127|-1|0
                SHORT\t-128|127|-1|0
                CHAR\tﾀ|\\u007f|\\uffff|
                INT\t-128|127|-1|0
                LONG\t-128|127|-1|0
                DATE\t1969-12-31T23:59:59.872Z|1970-01-01T00:00:00.127Z|1969-12-31T23:59:59.999Z|1970-01-01T00:00:00.000Z
                TIMESTAMP\t1969-12-31T23:59:59.999872Z|1970-01-01T00:00:00.000127Z|1969-12-31T23:59:59.999999Z|1970-01-01T00:00:00.000000Z
                FLOAT\t-128.0|127.0|-1.0|0.0
                DOUBLE\t-128.0|127.0|-1.0|0.0
                STRING\t-128|127|-1|0
                SYMBOL\t-128|127|-1|0
                LONG256\t0xffffffffffffff80|0x7f|0xffffffffffffffff|0x00
                GEOBYTE\t0000000|1111111||0000000
                GEOSHORT\tzw0|03z||000
                GEOINT\tzzzzw0|00003z||000000
                GEOLONG\tzzzzzzw0|0000003z||00000000
                BINARY\terror: [20] unsupported cast
                UUID\tno cast [10]
                LONG128\terror: [20] unsupported cast
                IPv4\t255.255.255.128|0.0.0.127|255.255.255.255|
                VARCHAR\t-128|127|-1|0
                DOUBLE[]\t[-128.0]|[127.0]|[-1.0]|[0.0]
                DECIMAL8\terror: inconvertible value: -128 [BYTE -> DECIMAL(2,1)]
                DECIMAL16\terror: inconvertible value: -128 [BYTE -> DECIMAL(4,2)]
                DECIMAL32\t-128|127|-1|0
                DECIMAL64\t-128.0000|127.0000|-1.0000|0.0000
                DECIMAL128\t-128.0000000000|127.0000000000|-1.0000000000|0.0000000000
                DECIMAL256\t-128.00000000000000000000|127.00000000000000000000|-1.00000000000000000000|0.00000000000000000000
                INTERVAL\terror: [20] unsupported cast
                VARCHAR_SLICE\terror: [20] unsupported cast
                TIMESTAMP_NS\t1969-12-31T23:59:59.999999872Z|1970-01-01T00:00:00.000000127Z|1969-12-31T23:59:59.999999999Z|1970-01-01T00:00:00.000000000Z
                GEOHASH(1c)\t0|z||0
                GEOHASH(8b)\t10000000|01111111||00000000
                GEOHASH(31b)\t1111111111111111111111110000000|0000000000000000000000001111111||0000000000000000000000000000000
                GEOHASH(12c)\tzzzzzzzzzzw0|00000000003z||000000000000
                DECIMAL(5,2)\t-128.00|127.00|-1.00|0.00
                DECIMAL(18,3)\t-128.000|127.000|-1.000|0.000
                DOUBLE[][]\t[[-128.0]]|[[127.0]]|[[-1.0]]|[[0.0]]
                INTERVAL(us)\terror: [20] unsupported cast
                INTERVAL(ns)\terror: [20] unsupported cast
                ## fill_null
                props: random_access=false size=unknown timestamp=ts:asc
                ts\tv
                1970-01-01T00:00:00.000000Z\t-128
                1970-01-01T00:00:01.000000Z\t0
                1970-01-01T00:00:02.000000Z\t127
                1970-01-01T00:00:03.000000Z\t0
                1970-01-01T00:00:04.000000Z\t0
                ## fill_value
                props: random_access=false size=unknown timestamp=ts:asc
                ts\tv
                1970-01-01T00:00:00.000000Z\t-128
                1970-01-01T00:00:01.000000Z\t127
                1970-01-01T00:00:02.000000Z\t127
                1970-01-01T00:00:03.000000Z\t127
                1970-01-01T00:00:04.000000Z\t0
                ## fill_linear
                props: random_access=true size=known timestamp=ts:asc
                ts\tv
                1970-01-01T00:00:00.000000Z\t-128
                1970-01-01T00:00:01.000000Z\t0
                1970-01-01T00:00:02.000000Z\t127
                1970-01-01T00:00:03.000000Z\t63
                1970-01-01T00:00:04.000000Z\t0
                ## subsample_stride
                min\terror: [44] stride must be at least 1
                max\tmin
                other_null\terror: [42] stride must be at least 1
                null\terror: [38] stride must be at least 1
                ## subsample_target
                min\terror: [44] target points must be at least 2
                max\tmin,max,other_null,null
                other_null\terror: [42] target points must be at least 2
                null\terror: [38] target points must be at least 2
                ## memoized
                props: random_access=true size=known timestamp=none
                k\ta\ta2\ta3
                min\t-128\t-128\t-128
                max\t127\t127\t127
                other_null\t-1\t-1\t-1
                null\t0\t0\t0
                plan memoizes: true
                ## where_bound_const
                min\tmin,max,other_null,null
                max\tmax,other_null,null
                other_null\tmin,max,other_null,null
                null\tmin,max,other_null,null
                ## where_bound_bind
                min\tmin,max,other_null,null
                max\tmax,other_null,null
                other_null\tmin,max,other_null,null
                null\tmin,max,other_null,null
                ## where_key
                min\tb:min
                max\tb:max
                other_null\tb:other_null
                null\t
                ## copy_bind
                status\tmessage
                finished\t
                k
                max
                ## between_timestamp
                error: [27] there is no matching operator `between` with the argument type: BYTE
                ## eq_null_double
                props: random_access=true size=known timestamp=none
                k\tv
                ## bind_value
                setBoolean\terror: [0] bind variable at 0 is defined as BYTE and cannot accept BOOLEAN
                setByte\t1
                setShort\t1
                setChar\t1
                setInt\t1
                setLong\t1
                setFloat\terror: [0] bind variable at 0 is defined as BYTE and cannot accept FLOAT
                setDouble\terror: [0] bind variable at 0 is defined as BYTE and cannot accept DOUBLE
                setDate\t1
                setTimestamp\t1
                setStr\t1
                setVarchar\t1
                setLong256\terror: [0] bind variable at 0 is defined as BYTE and cannot accept LONG256
                setUuid\terror: [0] bind variable at 0 is defined as BYTE and cannot accept UUID
                setArray\terror: [0] bind variable at 0 is defined as BYTE and cannot accept ARRAY
                ## window_anchor
                error: create view: [147] ANCHOR EXPRESSION must return TIMESTAMP, LONG, or INT; got BYTE
                """);
        rec("SHORT", """
                ## cast
                target\tmin|max|other_null|null
                BOOLEAN\ttrue|true|true|false
                BYTE\t0|-1|-1|0
                SHORT\t-32768|32767|-1|0
                CHAR\t耀|翿|\\uffff|
                INT\t-32768|32767|-1|0
                LONG\t-32768|32767|-1|0
                DATE\t1969-12-31T23:59:27.232Z|1970-01-01T00:00:32.767Z|1969-12-31T23:59:59.999Z|1970-01-01T00:00:00.000Z
                TIMESTAMP\t1969-12-31T23:59:59.967232Z|1970-01-01T00:00:00.032767Z|1969-12-31T23:59:59.999999Z|1970-01-01T00:00:00.000000Z
                FLOAT\t-32768.0|32767.0|-1.0|0.0
                DOUBLE\t-32768.0|32767.0|-1.0|0.0
                STRING\t-32768|32767|-1|0
                SYMBOL\t-32768|32767|-1|0
                LONG256\t0xffffffffffff8000|0x7fff|0xffffffffffffffff|0x00
                GEOBYTE\t0000000|||0000000
                GEOSHORT\t000|zzz||000
                GEOINT\tzzz000|000zzz||000000
                GEOLONG\tzzzzz000|00000zzz||00000000
                BINARY\terror: [20] unsupported cast
                UUID\tno cast [10]
                LONG128\terror: [20] unsupported cast
                IPv4\t255.255.128.0|0.0.127.255|255.255.255.255|
                VARCHAR\t-32768|32767|-1|0
                DOUBLE[]\t[-32768.0]|[32767.0]|[-1.0]|[0.0]
                DECIMAL8\terror: inconvertible value: -32768 [SHORT -> DECIMAL(2,1)]
                DECIMAL16\terror: inconvertible value: -32768 [SHORT -> DECIMAL(4,2)]
                DECIMAL32\t-32768|32767|-1|0
                DECIMAL64\t-32768.0000|32767.0000|-1.0000|0.0000
                DECIMAL128\t-32768.0000000000|32767.0000000000|-1.0000000000|0.0000000000
                DECIMAL256\t-32768.00000000000000000000|32767.00000000000000000000|-1.00000000000000000000|0.00000000000000000000
                INTERVAL\terror: [20] unsupported cast
                VARCHAR_SLICE\terror: [20] unsupported cast
                TIMESTAMP_NS\t1969-12-31T23:59:59.999967232Z|1970-01-01T00:00:00.000032767Z|1969-12-31T23:59:59.999999999Z|1970-01-01T00:00:00.000000000Z
                GEOHASH(1c)\t0|||0
                GEOHASH(8b)\t00000000|11111111||00000000
                GEOHASH(31b)\t1111111111111111000000000000000|0000000000000000111111111111111||0000000000000000000000000000000
                GEOHASH(12c)\tzzzzzzzzz000|000000000zzz||000000000000
                DECIMAL(5,2)\terror: inconvertible value: -32768 [SHORT -> DECIMAL(5,2)]
                DECIMAL(18,3)\t-32768.000|32767.000|-1.000|0.000
                DOUBLE[][]\t[[-32768.0]]|[[32767.0]]|[[-1.0]]|[[0.0]]
                INTERVAL(us)\terror: [20] unsupported cast
                INTERVAL(ns)\terror: [20] unsupported cast
                ## filter_eq
                props: random_access=true size=unknown timestamp=none
                k\tv
                max\t32767
                ## filter_ne
                props: random_access=true size=unknown timestamp=none
                k\tv
                min\t-32768
                other_null\t-1
                null\t0
                ## filter_null
                props: random_access=true size=known timestamp=none
                k\tv
                ## filter_not_null
                props: random_access=true size=known timestamp=none
                k\tv
                min\t-32768
                max\t32767
                other_null\t-1
                null\t0
                ## filter_lt
                props: random_access=true size=unknown timestamp=none
                k\tv
                min\t-32768
                other_null\t-1
                null\t0
                ## filter_ge
                props: random_access=true size=unknown timestamp=none
                k\tv
                max\t32767
                ## order_asc
                props: random_access=true size=known timestamp=none
                k\tv
                min\t-32768
                other_null\t-1
                null\t0
                max\t32767
                ## order_desc
                props: random_access=true size=known timestamp=none
                k\tv
                max\t32767
                null\t0
                other_null\t-1
                min\t-32768
                ## group_by
                props: random_access=true size=known timestamp=f:asc
                v\tc\tf
                -32768\t2\t1970-01-01T00:00:00.000000Z
                32767\t2\t1970-01-01T00:00:01.000000Z
                -1\t2\t1970-01-01T00:00:02.000000Z
                0\t2\t1970-01-01T00:00:03.000000Z
                ## join_inner
                props: random_access=true size=unknown timestamp=none
                tk\tuk\tv
                min\tmin\t-32768
                other_null\tother_null\t-1
                ## join_left
                props: random_access=true size=unknown timestamp=none
                tk\tuk\tuv
                max\t\t0
                min\tmin\t-32768
                null\t\t0
                other_null\tother_null\t-1
                ## join_left_null
                props: random_access=true size=unknown timestamp=none
                tk\tuv
                max\t0
                min\t-32768
                null\t0
                other_null\t-1
                ## union_all
                props: random_access=false size=known timestamp=none
                k\tv
                min\t-32768
                max\t32767
                other_null\t-1
                null\t0
                min\t-32768
                other_null\t-1
                ## union_null
                props: random_access=false size=known timestamp=none
                k\tv
                min\t-32768
                other_null\t-1
                null_branch\t0
                ## case_no_else
                props: random_access=true size=known timestamp=none
                k\tc
                min\t0
                max\t32767
                other_null\t0
                null\t0
                ## case_else
                props: random_access=true size=known timestamp=none
                k\tc
                min\t-32768
                max\t32767
                other_null\t-1
                null\t32767
                ## lag
                props: random_access=false size=known timestamp=none
                k\tv\tp
                min\t-32768\tnull
                max\t32767\t-32768
                other_null\t-1\t32767
                null\t0\t-1
                ## sample_by
                props: random_access=true size=known timestamp=ts:asc
                ts\tf\tl\tc
                1970-01-01T00:00:00.000000Z\t-32768\t32767\t2
                1970-01-01T00:00:02.000000Z\t-1\t0\t2
                ## first_last
                props: random_access=true size=known timestamp=none
                k\tf\tl
                A\t0\t32767
                B\t32767\t0
                C\t0\t0
                ## first_not_null
                props: random_access=true size=known timestamp=none
                k\tf\tl
                A\t0\t32767
                B\t32767\t0
                C\t0\t0
                ## fill_prev
                props: random_access=false size=unknown timestamp=ts:asc
                ts\tv
                1970-01-01T00:00:00.000000Z\t0
                1970-01-01T00:00:01.000000Z\t0
                1970-01-01T00:00:02.000000Z\t32767
                1970-01-01T00:00:03.000000Z\t32767
                1970-01-01T00:00:04.000000Z\t0
                ## latest_on
                props: random_access=true size=known timestamp=none
                k\tv
                b:max\t32767
                b:min\t-32768
                b:null\t0
                b:other_null\t-1
                ## fill_null
                props: random_access=false size=unknown timestamp=ts:asc
                ts\tv
                1970-01-01T00:00:00.000000Z\t-32768
                1970-01-01T00:00:01.000000Z\t0
                1970-01-01T00:00:02.000000Z\t32767
                1970-01-01T00:00:03.000000Z\t0
                1970-01-01T00:00:04.000000Z\t0
                ## fill_value
                props: random_access=false size=unknown timestamp=ts:asc
                ts\tv
                1970-01-01T00:00:00.000000Z\t-32768
                1970-01-01T00:00:01.000000Z\t32767
                1970-01-01T00:00:02.000000Z\t32767
                1970-01-01T00:00:03.000000Z\t32767
                1970-01-01T00:00:04.000000Z\t0
                ## fill_linear
                props: random_access=true size=known timestamp=ts:asc
                ts\tv
                1970-01-01T00:00:00.000000Z\t-32768
                1970-01-01T00:00:01.000000Z\t0
                1970-01-01T00:00:02.000000Z\t32767
                1970-01-01T00:00:03.000000Z\t16383
                1970-01-01T00:00:04.000000Z\t0
                ## subsample_stride
                min\terror: [46] stride must be at least 1
                max\tmin
                other_null\terror: [42] stride must be at least 1
                null\terror: [38] stride must be at least 1
                ## subsample_target
                min\terror: [46] target points must be at least 2
                max\tmin,max,other_null,null
                other_null\terror: [42] target points must be at least 2
                null\terror: [38] target points must be at least 2
                ## memoized
                props: random_access=true size=known timestamp=none
                k\ta\ta2\ta3
                min\t-32768\t-32768\t-32768
                max\t32767\t32767\t32767
                other_null\t-1\t-1\t-1
                null\t0\t0\t0
                plan memoizes: true
                ## where_bound_const
                min\tmin,max,other_null,null
                max\tmax,other_null,null
                other_null\tmin,max,other_null,null
                null\tmin,max,other_null,null
                ## where_bound_bind
                min\tmin,max,other_null,null
                max\tmax,other_null,null
                other_null\tmin,max,other_null,null
                null\tmin,max,other_null,null
                ## where_key
                min\tb:min
                max\tb:max
                other_null\tb:other_null
                null\t
                ## copy_bind
                status\tmessage
                finished\t
                k
                max
                ## between_timestamp
                error: [27] there is no matching operator `between` with the argument type: SHORT
                ## eq_null_double
                props: random_access=true size=known timestamp=none
                k\tv
                ## bind_value
                setBoolean\terror: [0] bind variable at 0 is defined as SHORT and cannot accept BOOLEAN
                setByte\t1
                setShort\t1
                setChar\t1
                setInt\t1
                setLong\t1
                setFloat\terror: [0] bind variable at 0 is defined as SHORT and cannot accept FLOAT
                setDouble\terror: [0] bind variable at 0 is defined as SHORT and cannot accept DOUBLE
                setDate\t1
                setTimestamp\t1
                setStr\t1
                setVarchar\t1
                setLong256\terror: [0] bind variable at 0 is defined as SHORT and cannot accept LONG256
                setUuid\terror: [0] bind variable at 0 is defined as SHORT and cannot accept UUID
                setArray\terror: [0] bind variable at 0 is defined as SHORT and cannot accept ARRAY
                ## window_anchor
                error: create view: [147] ANCHOR EXPRESSION must return TIMESTAMP, LONG, or INT; got SHORT
                """);
        rec("CHAR", """
                ## cast
                target\tmin|max|other_null|null
                BOOLEAN\terror: inconvertible value: \\u0000 [CHAR -> BOOLEAN]
                BYTE\terror: inconvertible value: \\u0000 [CHAR -> BYTE]
                SHORT\terror: inconvertible value: \\u0000 [CHAR -> SHORT]
                CHAR\t|\\uffff|\\uffff|
                INT\terror: inconvertible value: \\u0000 [CHAR -> INT]
                LONG\terror: inconvertible value: \\u0000 [CHAR -> LONG]
                DATE\terror: inconvertible value: \\u0000 [CHAR -> DATE]
                TIMESTAMP\terror: inconvertible value: \\u0000 [CHAR -> TIMESTAMP]
                FLOAT\terror: inconvertible value: \\u0000 [CHAR -> FLOAT]
                DOUBLE\terror: inconvertible value: \\u0000 [CHAR -> DOUBLE]
                STRING\t|\\uffff|\\uffff|
                SYMBOL\t|\\uffff|\\uffff|
                LONG256\terror: inconvertible value: \\u0000 [CHAR -> LONG]
                GEOBYTE\t|||
                GEOSHORT\t|||
                GEOINT\t|||
                GEOLONG\t|||
                BINARY\terror: [20] unsupported cast
                UUID\t|||
                LONG128\terror: [20] unsupported cast
                IPv4\t|||
                VARCHAR\t|\\uffff|\\uffff|
                DOUBLE[]\tnull|null|null|null
                DECIMAL8\terror: inconvertible value: `\\uffff` [CHAR -> DECIMAL(2,1)]
                DECIMAL16\terror: inconvertible value: `\\uffff` [CHAR -> DECIMAL(4,2)]
                DECIMAL32\terror: inconvertible value: `\\uffff` [CHAR -> DECIMAL(9,0)]
                DECIMAL64\terror: inconvertible value: `\\uffff` [CHAR -> DECIMAL(16,4)]
                DECIMAL128\terror: inconvertible value: `\\uffff` [CHAR -> DECIMAL(38,10)]
                DECIMAL256\terror: inconvertible value: `\\uffff` [CHAR -> DECIMAL(76,20)]
                INTERVAL\terror: [20] unsupported cast
                VARCHAR_SLICE\terror: [20] unsupported cast
                TIMESTAMP_NS\terror: inconvertible value: \\u0000 [CHAR -> TIMESTAMP_NS]
                GEOHASH(1c)\t|||
                GEOHASH(8b)\t|||
                GEOHASH(31b)\t|||
                GEOHASH(12c)\t|||
                DECIMAL(5,2)\terror: inconvertible value: `\\uffff` [CHAR -> DECIMAL(5,2)]
                DECIMAL(18,3)\terror: inconvertible value: `\\uffff` [CHAR -> DECIMAL(18,3)]
                DOUBLE[][]\tnull|null|null|null
                INTERVAL(us)\terror: [20] unsupported cast
                INTERVAL(ns)\terror: [20] unsupported cast
                ## filter_eq
                props: random_access=true size=unknown timestamp=none
                k\tv
                max\t\\uffff
                other_null\t\\uffff
                ## filter_ne
                props: random_access=true size=unknown timestamp=none
                k\tv
                min\t
                null\t
                ## filter_null
                props: random_access=true size=unknown timestamp=none
                k\tv
                min\t
                null\t
                ## filter_not_null
                props: random_access=true size=unknown timestamp=none
                k\tv
                max\t\\uffff
                other_null\t\\uffff
                ## filter_lt
                props: random_access=true size=unknown timestamp=none
                k\tv
                ## filter_ge
                props: random_access=true size=unknown timestamp=none
                k\tv
                max\t\\uffff
                other_null\t\\uffff
                ## order_asc
                props: random_access=true size=known timestamp=none
                k\tv
                min\t
                null\t
                max\t\\uffff
                other_null\t\\uffff
                ## order_desc
                props: random_access=true size=known timestamp=none
                k\tv
                max\t\\uffff
                other_null\t\\uffff
                min\t
                null\t
                ## group_by
                props: random_access=true size=known timestamp=f:asc
                v\tc\tf
                \t4\t1970-01-01T00:00:00.000000Z
                \\uffff\t4\t1970-01-01T00:00:01.000000Z
                ## join_inner
                props: random_access=true size=unknown timestamp=none
                tk\tuk\tv
                max\tother_null\t\\uffff
                min\tmin\t
                null\tmin\t
                other_null\tother_null\t\\uffff
                ## join_left
                props: random_access=true size=unknown timestamp=none
                tk\tuk\tuv
                max\tother_null\t\\uffff
                min\tmin\t
                null\tmin\t
                other_null\tother_null\t\\uffff
                ## join_left_null
                props: random_access=true size=unknown timestamp=none
                tk\tuv
                max\t
                min\t
                null\t
                other_null\t\\uffff
                ## union_all
                props: random_access=false size=known timestamp=none
                k\tv
                min\t
                max\t\\uffff
                other_null\t\\uffff
                null\t
                min\t
                other_null\t\\uffff
                ## union_null
                props: random_access=false size=known timestamp=none
                k\tv
                min\t
                other_null\t\\uffff
                null_branch\t
                ## case_no_else
                props: random_access=true size=known timestamp=none
                k\tc
                min\t
                max\t\\uffff
                other_null\t
                null\t
                ## case_else
                props: random_access=true size=known timestamp=none
                k\tc
                min\t\\uffff
                max\t\\uffff
                other_null\t\\uffff
                null\t\\uffff
                ## lag
                error: inconvertible value: \\u0000 [CHAR -> LONG]
                ## sample_by
                props: random_access=true size=known timestamp=ts:asc
                ts\tf\tl\tc
                1970-01-01T00:00:00.000000Z\t\t\\uffff\t2
                1970-01-01T00:00:02.000000Z\t\\uffff\t\t2
                ## first_last
                props: random_access=true size=known timestamp=none
                k\tf\tl
                A\t\t\\uffff
                B\t\\uffff\t
                C\t\t
                ## first_not_null
                props: random_access=true size=known timestamp=none
                k\tf\tl
                A\t\\uffff\t\\uffff
                B\t\\uffff\t\\uffff
                C\t\t
                ## fill_prev
                props: random_access=false size=unknown timestamp=ts:asc
                ts\tv
                1970-01-01T00:00:00.000000Z\t
                1970-01-01T00:00:01.000000Z\t
                1970-01-01T00:00:02.000000Z\t\\uffff
                1970-01-01T00:00:03.000000Z\t\\uffff
                1970-01-01T00:00:04.000000Z\t
                ## latest_on
                props: random_access=true size=known timestamp=none
                k\tv
                b:null\t
                b:other_null\t\\uffff
                ## fill_null
                error: [46] fill value of type NULL cannot fill column of type CHAR
                ## fill_value
                props: random_access=false size=unknown timestamp=ts:asc
                ts\tv
                1970-01-01T00:00:00.000000Z\t
                1970-01-01T00:00:01.000000Z\t\\uffff
                1970-01-01T00:00:02.000000Z\t\\uffff
                1970-01-01T00:00:03.000000Z\t\\uffff
                1970-01-01T00:00:04.000000Z\t
                ## fill_linear
                error: [11] Unsupported interpolation type: CHAR
                ## subsample_stride
                min\terror: [39] integer expected for stride
                max\terror: [43] integer expected for stride
                other_null\terror: [42] integer expected for stride
                null\terror: [38] integer expected for stride
                ## subsample_target
                min\terror: [39] integer expected for target point count
                max\terror: [43] integer expected for target point count
                other_null\terror: [42] integer expected for target point count
                null\terror: [38] integer expected for target point count
                ## memoized
                props: random_access=true size=known timestamp=none
                k\ta\ta2\ta3
                min\t\t\t
                max\t\\uffff\t\\uffff\t\\uffff
                other_null\t\\uffff\t\\uffff\t\\uffff
                null\t\t\t
                plan memoizes: true
                ## where_bound_const
                min\terror: inconvertible value: \\u0000 [CHAR -> LONG]
                max\terror: inconvertible value: \\uffff [CHAR -> LONG]
                other_null\terror: inconvertible value: \\uffff [CHAR -> LONG]
                null\terror: inconvertible value: \\u0000 [CHAR -> LONG]
                ## where_bound_bind
                min\terror: inconvertible value: \\u0000 [CHAR -> LONG]
                max\terror: inconvertible value: \\uffff [CHAR -> LONG]
                other_null\terror: inconvertible value: \\uffff [CHAR -> LONG]
                null\terror: inconvertible value: \\u0000 [CHAR -> LONG]
                ## where_key
                min\tb:null
                max\tb:other_null
                other_null\tb:other_null
                null\tb:null
                ## copy_bind
                status\tmessage
                finished\t
                k
                max
                other_null
                ## between_timestamp
                error: [27] there is no matching operator `between` with the argument type: CHAR
                ## eq_null_double
                props: random_access=true size=unknown timestamp=none
                k\tv
                min\t
                null\t
                ## bind_value
                setBoolean\terror: [0] bind variable at 0 is defined as CHAR and cannot accept BOOLEAN
                setByte\t1
                setShort\t1
                setChar\t1
                setInt\t1
                setLong\t1
                setFloat\terror: [0] bind variable at 0 is defined as CHAR and cannot accept FLOAT
                setDouble\terror: [0] bind variable at 0 is defined as CHAR and cannot accept DOUBLE
                setDate\t1
                setTimestamp\terror: [0] bind variable at 0 is defined as CHAR and cannot accept TIMESTAMP
                setStr\t1
                setVarchar\t1
                setLong256\terror: [0] bind variable at 0 is defined as CHAR and cannot accept LONG256
                setUuid\terror: [0] bind variable at 0 is defined as CHAR and cannot accept UUID
                setArray\terror: [0] bind variable at 0 is defined as CHAR and cannot accept ARRAY
                ## window_anchor
                error: create view: [147] ANCHOR EXPRESSION must return TIMESTAMP, LONG, or INT; got CHAR
                """);
        rec("INT", """
                ## cast
                target\tmin|max|sentinel|null
                BOOLEAN\ttrue|true|false|false
                BYTE\t1|-1|0|0
                SHORT\t1|-1|0|0
                CHAR\t\\u0001|\\uffff||
                INT\t-2147483647|2147483647|null|null
                LONG\t-2147483647|2147483647|null|null
                DATE\t1969-12-07T03:28:36.353Z|1970-01-25T20:31:23.647Z||
                TIMESTAMP\t1969-12-31T23:24:12.516353Z|1970-01-01T00:35:47.483647Z||
                FLOAT\t-2.1474836E9|2.1474836E9|null|null
                DOUBLE\t-2.147483647E9|2.147483647E9|null|null
                STRING\t-2147483647|2147483647||
                SYMBOL\t-2147483647|2147483647||
                LONG256\t0xffffffff80000001|0x7fffffff||
                GEOBYTE\t0000001|||
                GEOSHORT\t001|||
                GEOINT\t000001|zzzzzz||
                GEOLONG\tzy000001|01zzzzzz||
                BINARY\terror: [20] unsupported cast
                UUID\tno cast [10]
                LONG128\terror: [20] unsupported cast
                IPv4\t128.0.0.1|127.255.255.255||
                VARCHAR\t-2147483647|2147483647||
                DOUBLE[]\t[-2.147483647E9]|[2.147483647E9]|null|null
                DECIMAL8\terror: inconvertible value: -2147483647 [INT -> DECIMAL(2,1)]
                DECIMAL16\terror: inconvertible value: -2147483647 [INT -> DECIMAL(4,2)]
                DECIMAL32\terror: inconvertible value: -2147483647 [INT -> DECIMAL(9,0)]
                DECIMAL64\t-2147483647.0000|2147483647.0000||
                DECIMAL128\t-2147483647.0000000000|2147483647.0000000000||
                DECIMAL256\t-2147483647.00000000000000000000|2147483647.00000000000000000000||
                INTERVAL\terror: [20] unsupported cast
                VARCHAR_SLICE\terror: [20] unsupported cast
                TIMESTAMP_NS\t1969-12-31T23:59:57.852516353Z|1970-01-01T00:00:02.147483647Z||
                GEOHASH(1c)\t1|||
                GEOHASH(8b)\t00000001|||
                GEOHASH(31b)\t0000000000000000000000000000001|1111111111111111111111111111111||
                GEOHASH(12c)\tzzzzzy000001|000001zzzzzz||
                DECIMAL(5,2)\terror: inconvertible value: -2147483647 [INT -> DECIMAL(5,2)]
                DECIMAL(18,3)\t-2147483647.000|2147483647.000||
                DOUBLE[][]\t[[-2.147483647E9]]|[[2.147483647E9]]|null|null
                INTERVAL(us)\terror: [20] unsupported cast
                INTERVAL(ns)\terror: [20] unsupported cast
                ## filter_eq
                props: random_access=true size=unknown timestamp=none
                k\tv
                max\t2147483647
                ## filter_ne
                props: random_access=true size=unknown timestamp=none
                k\tv
                min\t-2147483647
                sentinel\tnull
                null\tnull
                ## filter_null
                props: random_access=true size=unknown timestamp=none
                k\tv
                sentinel\tnull
                null\tnull
                ## filter_not_null
                props: random_access=true size=unknown timestamp=none
                k\tv
                min\t-2147483647
                max\t2147483647
                ## filter_lt
                props: random_access=true size=unknown timestamp=none
                k\tv
                min\t-2147483647
                ## filter_ge
                props: random_access=true size=unknown timestamp=none
                k\tv
                max\t2147483647
                ## order_asc
                props: random_access=true size=known timestamp=none
                k\tv
                null\tnull
                sentinel\tnull
                min\t-2147483647
                max\t2147483647
                ## order_desc
                props: random_access=true size=known timestamp=none
                k\tv
                max\t2147483647
                min\t-2147483647
                null\tnull
                sentinel\tnull
                ## group_by
                props: random_access=true size=known timestamp=f:asc
                v\tc\tf
                -2147483647\t2\t1970-01-01T00:00:00.000000Z
                2147483647\t2\t1970-01-01T00:00:01.000000Z
                null\t4\t1970-01-01T00:00:02.000000Z
                ## join_inner
                props: random_access=true size=unknown timestamp=none
                tk\tuk\tv
                min\tmin\t-2147483647
                null\tsentinel\tnull
                sentinel\tsentinel\tnull
                ## join_left
                props: random_access=true size=unknown timestamp=none
                tk\tuk\tuv
                max\t\tnull
                min\tmin\t-2147483647
                null\tsentinel\tnull
                sentinel\tsentinel\tnull
                ## join_left_null
                props: random_access=true size=unknown timestamp=none
                tk\tuv
                max\tnull
                min\t-2147483647
                null\tnull
                sentinel\tnull
                ## union_all
                props: random_access=false size=known timestamp=none
                k\tv
                min\t-2147483647
                max\t2147483647
                sentinel\tnull
                null\tnull
                min\t-2147483647
                sentinel\tnull
                ## union_null
                props: random_access=false size=known timestamp=none
                k\tv
                min\t-2147483647
                sentinel\tnull
                null_branch\tnull
                ## case_no_else
                props: random_access=true size=known timestamp=none
                k\tc
                min\tnull
                max\t2147483647
                sentinel\tnull
                null\tnull
                ## case_else
                props: random_access=true size=known timestamp=none
                k\tc
                min\t-2147483647
                max\t2147483647
                sentinel\t2147483647
                null\t2147483647
                ## lag
                props: random_access=false size=known timestamp=none
                k\tv\tp
                min\t-2147483647\tnull
                max\t2147483647\t-2147483647
                sentinel\tnull\t2147483647
                null\tnull\tnull
                ## sample_by
                props: random_access=true size=known timestamp=ts:asc
                ts\tf\tl\tc
                1970-01-01T00:00:00.000000Z\t-2147483647\t2147483647\t2
                1970-01-01T00:00:02.000000Z\tnull\tnull\t2
                ## first_last
                props: random_access=true size=known timestamp=none
                k\tf\tl
                A\tnull\t2147483647
                B\t2147483647\tnull
                C\tnull\tnull
                ## first_not_null
                props: random_access=true size=known timestamp=none
                k\tf\tl
                A\t2147483647\t2147483647
                B\t2147483647\t2147483647
                C\tnull\tnull
                ## fill_prev
                props: random_access=false size=unknown timestamp=ts:asc
                ts\tv
                1970-01-01T00:00:00.000000Z\tnull
                1970-01-01T00:00:01.000000Z\tnull
                1970-01-01T00:00:02.000000Z\t2147483647
                1970-01-01T00:00:03.000000Z\t2147483647
                1970-01-01T00:00:04.000000Z\tnull
                ## latest_on
                props: random_access=true size=known timestamp=none
                k\tv
                b:max\t2147483647
                b:min\t-2147483647
                b:null\tnull
                ## fill_null
                props: random_access=false size=unknown timestamp=ts:asc
                ts\tv
                1970-01-01T00:00:00.000000Z\t-2147483647
                1970-01-01T00:00:01.000000Z\tnull
                1970-01-01T00:00:02.000000Z\t2147483647
                1970-01-01T00:00:03.000000Z\tnull
                1970-01-01T00:00:04.000000Z\tnull
                ## fill_value
                props: random_access=false size=unknown timestamp=ts:asc
                ts\tv
                1970-01-01T00:00:00.000000Z\t-2147483647
                1970-01-01T00:00:01.000000Z\t2147483647
                1970-01-01T00:00:02.000000Z\t2147483647
                1970-01-01T00:00:03.000000Z\t2147483647
                1970-01-01T00:00:04.000000Z\tnull
                ## fill_linear
                props: random_access=true size=known timestamp=ts:asc
                ts\tv
                1970-01-01T00:00:00.000000Z\t-2147483647
                1970-01-01T00:00:01.000000Z\t0
                1970-01-01T00:00:02.000000Z\t2147483647
                1970-01-01T00:00:03.000000Z\tnull
                1970-01-01T00:00:04.000000Z\tnull
                ## subsample_stride
                min\terror: [51] stride must be at least 1
                max\tmin
                sentinel\terror: [51] stride must be set
                null\terror: [38] stride must be set
                ## subsample_target
                min\terror: [51] target points must be at least 2
                max\tmin,max,sentinel,null
                sentinel\terror: [51] target point count must be set
                null\terror: [38] target point count must be set
                ## memoized
                props: random_access=true size=known timestamp=none
                k\ta\ta2\ta3
                min\t-2147483647\t-2147483647\t-2147483647
                max\t2147483647\t2147483647\t2147483647
                sentinel\tnull\tnull\tnull
                null\tnull\tnull\tnull
                plan memoizes: true
                ## where_bound_const
                min\tmin,max,sentinel,null
                max\t
                sentinel\t
                null\t
                ## where_bound_bind
                min\tmin,max,sentinel,null
                max\t
                sentinel\t
                null\t
                ## where_key
                min\tb:min
                max\tb:max
                sentinel\tb:null
                null\tb:null
                ## copy_bind
                status\tmessage
                finished\t
                k
                max
                ## between_timestamp
                props: random_access=true size=unknown timestamp=none
                k\tv
                min\t-2147483647
                max\t2147483647
                ## eq_null_double
                props: random_access=true size=unknown timestamp=none
                k\tv
                sentinel\tnull
                null\tnull
                ## bind_value
                setBoolean\terror: [0] bind variable at 0 is defined as INT and cannot accept BOOLEAN
                setByte\t1
                setShort\t1
                setChar\t1
                setInt\t1
                setLong\t1
                setFloat\terror: [0] bind variable at 0 is defined as INT and cannot accept FLOAT
                setDouble\terror: [0] bind variable at 0 is defined as INT and cannot accept DOUBLE
                setDate\t1
                setTimestamp\t1
                setStr\t1
                setVarchar\t1
                setLong256\terror: [0] bind variable at 0 is defined as INT and cannot accept LONG256
                setUuid\terror: [0] bind variable at 0 is defined as INT and cannot accept UUID
                setArray\terror: [0] bind variable at 0 is defined as INT and cannot accept ARRAY
                ## window_anchor
                k\tc
                min\t1
                max\t1
                sentinel\t1
                null\t2
                """);
        rec("LONG", """
                ## cast
                target\tmin|max|sentinel|null
                BOOLEAN\ttrue|true|false|false
                BYTE\t1|-1|0|0
                SHORT\t1|-1|0|0
                CHAR\t\\u0001|\\uffff||
                INT\t1|-1|null|null
                LONG\t-9223372036854775807|9223372036854775807|null|null
                DATE\t-292275055-05-16T16:47:04.193Z|292278994-08-17T07:12:55.807Z||
                TIMESTAMP\t-290308-01-01T19:59:05.224193Z|294247-01-10T04:00:54.775807Z||
                FLOAT\t-9.223372E18|9.223372E18|null|null
                DOUBLE\t-9.223372036854776E18|9.223372036854776E18|null|null
                STRING\t-9223372036854775807|9223372036854775807||
                SYMBOL\t-9223372036854775807|9223372036854775807||
                LONG256\t0x8000000000000001|0x7fffffffffffffff||
                GEOBYTE\t0000001|||
                GEOSHORT\t001|||
                GEOINT\t000001|||
                GEOLONG\t00000001|zzzzzzzz||
                BINARY\terror: [20] unsupported cast
                UUID\tno cast [10]
                LONG128\terror: [20] unsupported cast
                IPv4\tno cast [10]
                VARCHAR\t-9223372036854775807|9223372036854775807||
                DOUBLE[]\t[-9.223372036854776E18]|[9.223372036854776E18]|null|null
                DECIMAL8\terror: inconvertible value: -9223372036854775807 [LONG -> DECIMAL(2,1)]
                DECIMAL16\terror: inconvertible value: -9223372036854775807 [LONG -> DECIMAL(4,2)]
                DECIMAL32\terror: inconvertible value: -9223372036854775807 [LONG -> DECIMAL(9,0)]
                DECIMAL64\terror: inconvertible value: -9223372036854775807 [LONG -> DECIMAL(16,4)]
                DECIMAL128\t-9223372036854775807.0000000000|9223372036854775807.0000000000||
                DECIMAL256\t-9223372036854775807.00000000000000000000|9223372036854775807.00000000000000000000||
                INTERVAL\terror: [20] unsupported cast
                VARCHAR_SLICE\terror: [20] unsupported cast
                TIMESTAMP_NS\t1677-01-01T00:12:43.145224193Z|2262-04-11T23:47:16.854775807Z||
                GEOHASH(1c)\t1|||
                GEOHASH(8b)\t00000001|||
                GEOHASH(31b)\t0000000000000000000000000000001|||
                GEOHASH(12c)\t000000000001|zzzzzzzzzzzz||
                DECIMAL(5,2)\terror: inconvertible value: -9223372036854775807 [LONG -> DECIMAL(5,2)]
                DECIMAL(18,3)\terror: inconvertible value: -9223372036854775807 [LONG -> DECIMAL(18,3)]
                DOUBLE[][]\t[[-9.223372036854776E18]]|[[9.223372036854776E18]]|null|null
                INTERVAL(us)\terror: [20] unsupported cast
                INTERVAL(ns)\terror: [20] unsupported cast
                ## filter_eq
                props: random_access=true size=unknown timestamp=none
                k\tv
                max\t9223372036854775807
                ## filter_ne
                props: random_access=true size=unknown timestamp=none
                k\tv
                min\t-9223372036854775807
                sentinel\tnull
                null\tnull
                ## filter_null
                props: random_access=true size=unknown timestamp=none
                k\tv
                sentinel\tnull
                null\tnull
                ## filter_not_null
                props: random_access=true size=unknown timestamp=none
                k\tv
                min\t-9223372036854775807
                max\t9223372036854775807
                ## filter_lt
                props: random_access=true size=unknown timestamp=none
                k\tv
                min\t-9223372036854775807
                ## filter_ge
                props: random_access=true size=unknown timestamp=none
                k\tv
                max\t9223372036854775807
                ## order_asc
                props: random_access=true size=known timestamp=none
                k\tv
                null\tnull
                sentinel\tnull
                min\t-9223372036854775807
                max\t9223372036854775807
                ## order_desc
                props: random_access=true size=known timestamp=none
                k\tv
                max\t9223372036854775807
                min\t-9223372036854775807
                null\tnull
                sentinel\tnull
                ## group_by
                props: random_access=true size=known timestamp=f:asc
                v\tc\tf
                -9223372036854775807\t2\t1970-01-01T00:00:00.000000Z
                9223372036854775807\t2\t1970-01-01T00:00:01.000000Z
                null\t4\t1970-01-01T00:00:02.000000Z
                ## join_inner
                props: random_access=true size=unknown timestamp=none
                tk\tuk\tv
                min\tmin\t-9223372036854775807
                null\tsentinel\tnull
                sentinel\tsentinel\tnull
                ## join_left
                props: random_access=true size=unknown timestamp=none
                tk\tuk\tuv
                max\t\tnull
                min\tmin\t-9223372036854775807
                null\tsentinel\tnull
                sentinel\tsentinel\tnull
                ## join_left_null
                props: random_access=true size=unknown timestamp=none
                tk\tuv
                max\tnull
                min\t-9223372036854775807
                null\tnull
                sentinel\tnull
                ## union_all
                props: random_access=false size=known timestamp=none
                k\tv
                min\t-9223372036854775807
                max\t9223372036854775807
                sentinel\tnull
                null\tnull
                min\t-9223372036854775807
                sentinel\tnull
                ## union_null
                props: random_access=false size=known timestamp=none
                k\tv
                min\t-9223372036854775807
                sentinel\tnull
                null_branch\tnull
                ## case_no_else
                props: random_access=true size=known timestamp=none
                k\tc
                min\tnull
                max\t9223372036854775807
                sentinel\tnull
                null\tnull
                ## case_else
                props: random_access=true size=known timestamp=none
                k\tc
                min\t-9223372036854775807
                max\t9223372036854775807
                sentinel\t9223372036854775807
                null\t9223372036854775807
                ## lag
                props: random_access=false size=known timestamp=none
                k\tv\tp
                min\t-9223372036854775807\tnull
                max\t9223372036854775807\t-9223372036854775807
                sentinel\tnull\t9223372036854775807
                null\tnull\tnull
                ## sample_by
                props: random_access=true size=known timestamp=ts:asc
                ts\tf\tl\tc
                1970-01-01T00:00:00.000000Z\t-9223372036854775807\t9223372036854775807\t2
                1970-01-01T00:00:02.000000Z\tnull\tnull\t2
                ## first_last
                props: random_access=true size=known timestamp=none
                k\tf\tl
                A\tnull\t9223372036854775807
                B\t9223372036854775807\tnull
                C\tnull\tnull
                ## first_not_null
                props: random_access=true size=known timestamp=none
                k\tf\tl
                A\t9223372036854775807\t9223372036854775807
                B\t9223372036854775807\t9223372036854775807
                C\tnull\tnull
                ## fill_prev
                props: random_access=false size=unknown timestamp=ts:asc
                ts\tv
                1970-01-01T00:00:00.000000Z\tnull
                1970-01-01T00:00:01.000000Z\tnull
                1970-01-01T00:00:02.000000Z\t9223372036854775807
                1970-01-01T00:00:03.000000Z\t9223372036854775807
                1970-01-01T00:00:04.000000Z\tnull
                ## latest_on
                props: random_access=true size=known timestamp=none
                k\tv
                b:max\t9223372036854775807
                b:min\t-9223372036854775807
                b:null\tnull
                ## fill_null
                props: random_access=false size=unknown timestamp=ts:asc
                ts\tv
                1970-01-01T00:00:00.000000Z\t-9223372036854775807
                1970-01-01T00:00:01.000000Z\tnull
                1970-01-01T00:00:02.000000Z\t9223372036854775807
                1970-01-01T00:00:03.000000Z\tnull
                1970-01-01T00:00:04.000000Z\tnull
                ## fill_value
                props: random_access=false size=unknown timestamp=ts:asc
                ts\tv
                1970-01-01T00:00:00.000000Z\t-9223372036854775807
                1970-01-01T00:00:01.000000Z\t9223372036854775807
                1970-01-01T00:00:02.000000Z\t9223372036854775807
                1970-01-01T00:00:03.000000Z\t9223372036854775807
                1970-01-01T00:00:04.000000Z\tnull
                ## fill_linear
                props: random_access=true size=known timestamp=ts:asc
                ts\tv
                1970-01-01T00:00:00.000000Z\t-9223372036854775807
                1970-01-01T00:00:01.000000Z\t0
                1970-01-01T00:00:02.000000Z\t9223372036854775807
                1970-01-01T00:00:03.000000Z\tnull
                1970-01-01T00:00:04.000000Z\tnull
                ## subsample_stride
                min\terror: [60] stride must be at least 1
                max\terror: [57] stride exceeds maximum of 2147483647
                sentinel\terror: [64] stride must be set
                null\terror: [38] stride must be set
                ## subsample_target
                min\terror: [60] target points must be at least 2
                max\terror: [57] target points exceeds maximum of 2147483647
                sentinel\terror: [64] target point count must be set
                null\terror: [38] target point count must be set
                ## memoized
                props: random_access=true size=known timestamp=none
                k\ta\ta2\ta3
                min\t-9223372036854775807\t-9223372036854775807\t-9223372036854775807
                max\t9223372036854775807\t9223372036854775807\t9223372036854775807
                sentinel\tnull\tnull\tnull
                null\tnull\tnull\tnull
                plan memoizes: true
                ## where_bound_const
                min\tmin,max,sentinel,null
                max\t
                sentinel\t
                null\t
                ## where_bound_bind
                min\tmin,max,sentinel,null
                max\t
                sentinel\t
                null\t
                ## where_key
                min\tb:min
                max\tb:max
                sentinel\tb:null
                null\tb:null
                ## copy_bind
                status\tmessage
                finished\t
                k
                max
                ## between_timestamp
                props: random_access=true size=unknown timestamp=none
                k\tv
                ## eq_null_double
                props: random_access=true size=unknown timestamp=none
                k\tv
                sentinel\tnull
                null\tnull
                ## bind_value
                setBoolean\terror: [0] bind variable at 0 is defined as LONG and cannot accept BOOLEAN
                setByte\t1
                setShort\t1
                setChar\t1
                setInt\t1
                setLong\t1
                setFloat\terror: [0] bind variable at 0 is defined as LONG and cannot accept FLOAT
                setDouble\terror: [0] bind variable at 0 is defined as LONG and cannot accept DOUBLE
                setDate\t1
                setTimestamp\t1
                setStr\t1
                setVarchar\t1
                setLong256\terror: [0] bind variable at 0 is defined as LONG and cannot accept LONG256
                setUuid\terror: [0] bind variable at 0 is defined as LONG and cannot accept UUID
                setArray\terror: [0] bind variable at 0 is defined as LONG and cannot accept ARRAY
                ## window_anchor
                k\tc
                min\t1
                max\t1
                sentinel\t1
                null\t2
                """);
        rec("DATE", """
                ## filter_eq
                props: random_access=true size=unknown timestamp=none
                k\tv
                max\t292278994-08-17T07:12:55.807Z
                ## filter_ne
                props: random_access=true size=unknown timestamp=none
                k\tv
                min\t-292275055-05-16T16:47:04.193Z
                sentinel\t
                null\t
                ## filter_null
                props: random_access=true size=unknown timestamp=none
                k\tv
                sentinel\t
                null\t
                ## filter_not_null
                props: random_access=true size=unknown timestamp=none
                k\tv
                min\t-292275055-05-16T16:47:04.193Z
                max\t292278994-08-17T07:12:55.807Z
                ## filter_lt
                props: random_access=true size=unknown timestamp=none
                k\tv
                min\t-292275055-05-16T16:47:04.193Z
                ## filter_ge
                props: random_access=true size=unknown timestamp=none
                k\tv
                max\t292278994-08-17T07:12:55.807Z
                ## order_asc
                props: random_access=true size=known timestamp=none
                k\tv
                null\t
                sentinel\t
                min\t-292275055-05-16T16:47:04.193Z
                max\t292278994-08-17T07:12:55.807Z
                ## order_desc
                props: random_access=true size=known timestamp=none
                k\tv
                max\t292278994-08-17T07:12:55.807Z
                min\t-292275055-05-16T16:47:04.193Z
                null\t
                sentinel\t
                ## group_by
                props: random_access=true size=known timestamp=f:asc
                v\tc\tf
                -292275055-05-16T16:47:04.193Z\t2\t1970-01-01T00:00:00.000000Z
                292278994-08-17T07:12:55.807Z\t2\t1970-01-01T00:00:01.000000Z
                \t4\t1970-01-01T00:00:02.000000Z
                ## join_inner
                props: random_access=true size=unknown timestamp=none
                tk\tuk\tv
                min\tmin\t-292275055-05-16T16:47:04.193Z
                null\tsentinel\t
                sentinel\tsentinel\t
                ## join_left
                props: random_access=true size=unknown timestamp=none
                tk\tuk\tuv
                max\t\t
                min\tmin\t-292275055-05-16T16:47:04.193Z
                null\tsentinel\t
                sentinel\tsentinel\t
                ## join_left_null
                props: random_access=true size=unknown timestamp=none
                tk\tuv
                max\t
                min\t-292275055-05-16T16:47:04.193Z
                null\t
                sentinel\t
                ## union_all
                props: random_access=false size=known timestamp=none
                k\tv
                min\t-292275055-05-16T16:47:04.193Z
                max\t292278994-08-17T07:12:55.807Z
                sentinel\t
                null\t
                min\t-292275055-05-16T16:47:04.193Z
                sentinel\t
                ## union_null
                props: random_access=false size=known timestamp=none
                k\tv
                min\t-292275055-05-16T16:47:04.193Z
                sentinel\t
                null_branch\t
                ## case_no_else
                props: random_access=true size=known timestamp=none
                k\tc
                min\t
                max\t292278994-08-17T07:12:55.807Z
                sentinel\t
                null\t
                ## case_else
                props: random_access=true size=known timestamp=none
                k\tc
                min\t-292275055-05-16T16:47:04.193Z
                max\t292278994-08-17T07:12:55.807Z
                sentinel\t292278994-08-17T07:12:55.807Z
                null\t292278994-08-17T07:12:55.807Z
                ## lag
                props: random_access=false size=known timestamp=none
                k\tv\tp
                min\t-292275055-05-16T16:47:04.193Z\t
                max\t292278994-08-17T07:12:55.807Z\t-292275055-05-16T16:47:04.193Z
                sentinel\t\t292278994-08-17T07:12:55.807Z
                null\t\t
                ## sample_by
                props: random_access=true size=known timestamp=ts:asc
                ts\tf\tl\tc
                1970-01-01T00:00:00.000000Z\t-292275055-05-16T16:47:04.193Z\t292278994-08-17T07:12:55.807Z\t2
                1970-01-01T00:00:02.000000Z\t\t\t2
                ## first_last
                props: random_access=true size=known timestamp=none
                k\tf\tl
                A\t\t292278994-08-17T07:12:55.807Z
                B\t292278994-08-17T07:12:55.807Z\t
                C\t\t
                ## first_not_null
                props: random_access=true size=known timestamp=none
                k\tf\tl
                A\t292278994-08-17T07:12:55.807Z\t292278994-08-17T07:12:55.807Z
                B\t292278994-08-17T07:12:55.807Z\t292278994-08-17T07:12:55.807Z
                C\t\t
                ## fill_prev
                props: random_access=false size=unknown timestamp=ts:asc
                ts\tv
                1970-01-01T00:00:00.000000Z\t
                1970-01-01T00:00:01.000000Z\t
                1970-01-01T00:00:02.000000Z\t292278994-08-17T07:12:55.807Z
                1970-01-01T00:00:03.000000Z\t292278994-08-17T07:12:55.807Z
                1970-01-01T00:00:04.000000Z\t
                ## latest_on
                props: random_access=true size=known timestamp=none
                k\tv
                b:max\t292278994-08-17T07:12:55.807Z
                b:min\t-292275055-05-16T16:47:04.193Z
                b:null\t
                ## cast
                target\tmin|max|sentinel|null
                BOOLEAN\ttrue|true|false|false
                BYTE\t1|-1|0|0
                SHORT\t1|-1|0|0
                CHAR\t\\u0001|\\uffff||
                INT\t1|-1|null|null
                LONG\t-9223372036854775807|9223372036854775807|null|null
                DATE\t-292275055-05-16T16:47:04.193Z|292278994-08-17T07:12:55.807Z||
                TIMESTAMP\t1970-01-01T00:00:00.001000Z|1969-12-31T23:59:59.999000Z||
                FLOAT\t-9.223372E18|9.223372E18|null|null
                DOUBLE\t-9.223372036854776E18|9.223372036854776E18|null|null
                STRING\t-292275055-05-16T16:47:04.193Z|292278994-08-17T07:12:55.807Z||
                SYMBOL\t-9223372036854775807|9223372036854775807||
                LONG256\t0x8000000000000001|0x7fffffffffffffff||
                GEOBYTE\t0000001|||
                GEOSHORT\t001|||
                GEOINT\t000001|||
                GEOLONG\t00000001|zzzzzzzz||
                BINARY\terror: [20] unsupported cast
                UUID\tno cast [10]
                LONG128\terror: [20] unsupported cast
                IPv4\tno cast [10]
                VARCHAR\t-292275055-05-16T16:47:04.193Z|292278994-08-17T07:12:55.807Z||
                DOUBLE[]\t[-9.223372036854776E18]|[9.223372036854776E18]|null|null
                DECIMAL8\terror: inconvertible value: -9223372036854775807 [LONG -> DECIMAL(2,1)]
                DECIMAL16\terror: inconvertible value: -9223372036854775807 [LONG -> DECIMAL(4,2)]
                DECIMAL32\terror: inconvertible value: -9223372036854775807 [LONG -> DECIMAL(9,0)]
                DECIMAL64\terror: inconvertible value: -9223372036854775807 [LONG -> DECIMAL(16,4)]
                DECIMAL128\t-9223372036854775807.0000000000|9223372036854775807.0000000000||
                DECIMAL256\t-9223372036854775807.00000000000000000000|9223372036854775807.00000000000000000000||
                INTERVAL\terror: [20] unsupported cast
                VARCHAR_SLICE\terror: [20] unsupported cast
                TIMESTAMP_NS\t1970-01-01T00:00:00.001000000Z|1969-12-31T23:59:59.999000000Z||
                GEOHASH(1c)\t1|||
                GEOHASH(8b)\t00000001|||
                GEOHASH(31b)\t0000000000000000000000000000001|||
                GEOHASH(12c)\t000000000001|zzzzzzzzzzzz||
                DECIMAL(5,2)\terror: inconvertible value: -9223372036854775807 [LONG -> DECIMAL(5,2)]
                DECIMAL(18,3)\terror: inconvertible value: -9223372036854775807 [LONG -> DECIMAL(18,3)]
                DOUBLE[][]\t[[-9.223372036854776E18]]|[[9.223372036854776E18]]|null|null
                INTERVAL(us)\terror: [20] unsupported cast
                INTERVAL(ns)\terror: [20] unsupported cast
                ## fill_null
                props: random_access=false size=unknown timestamp=ts:asc
                ts\tv
                1970-01-01T00:00:00.000000Z\t-292275055-05-16T16:47:04.193Z
                1970-01-01T00:00:01.000000Z\t
                1970-01-01T00:00:02.000000Z\t292278994-08-17T07:12:55.807Z
                1970-01-01T00:00:03.000000Z\t
                1970-01-01T00:00:04.000000Z\t
                ## fill_value
                error: inconvertible value: `292278994-08-17T07:12:55.807Z` [STRING -> LONG]
                ## fill_linear
                error: [46] support for LINEAR fill is not yet implemented [function=last(v), class=io.questdb.griffin.engine.functions.groupby.LastDateGroupByFunction]
                ## subsample_stride
                min\terror: [60] integer expected for stride
                max\terror: [57] integer expected for stride
                sentinel\terror: [64] integer expected for stride
                null\terror: [38] integer expected for stride
                ## subsample_target
                min\terror: [60] integer expected for target point count
                max\terror: [57] integer expected for target point count
                sentinel\terror: [64] integer expected for target point count
                null\terror: [38] integer expected for target point count
                ## memoized
                props: random_access=true size=known timestamp=none
                k\ta\ta2\ta3
                min\t-292275055-05-16T16:47:04.193Z\t-292275055-05-16T16:47:04.193Z\t-292275055-05-16T16:47:04.193Z
                max\t292278994-08-17T07:12:55.807Z\t292278994-08-17T07:12:55.807Z\t292278994-08-17T07:12:55.807Z
                sentinel\t\t\t
                null\t\t\t
                plan memoizes: true
                ## where_bound_const
                min\tmin,max,sentinel,null
                max\t
                sentinel\t
                null\t
                ## where_bound_bind
                min\tbind error: inconvertible value: `-292275055-05-16T16:47:04.193Z` [STRING -> DATE]
                max\tbind error: inconvertible value: `292278994-08-17T07:12:55.807Z` [STRING -> DATE]
                sentinel\t
                null\t
                ## where_key
                min\terror: [27] Invalid date [str=-292275055-05-16T16:47:04.193Z]
                max\terror: [27] Invalid date [str=292278994-08-17T07:12:55.807Z]
                sentinel\tb:null
                null\tb:null
                ## copy_bind
                bind error: inconvertible value: `292278994-08-17T07:12:55.807Z` [STRING -> DATE]
                ## between_timestamp
                props: random_access=true size=unknown timestamp=none
                k\tv
                min\t-292275055-05-16T16:47:04.193Z
                max\t292278994-08-17T07:12:55.807Z
                ## eq_null_double
                props: random_access=true size=unknown timestamp=none
                k\tv
                sentinel\t
                null\t
                ## bind_value
                setBoolean\terror: [0] bind variable at 0 is defined as DATE and cannot accept BOOLEAN
                setByte\t1970-01-01T00:00:00.001Z
                setShort\t1970-01-01T00:00:00.001Z
                setChar\t1970-01-01T00:00:00.001Z
                setInt\t1970-01-01T00:00:00.001Z
                setLong\t1970-01-01T00:00:00.001Z
                setFloat\terror: [0] bind variable at 0 is defined as DATE and cannot accept FLOAT
                setDouble\terror: [0] bind variable at 0 is defined as DATE and cannot accept DOUBLE
                setDate\t1970-01-01T00:00:00.001Z
                setTimestamp\t1970-01-01T00:00:00.000Z
                setStr\t1970-01-01T00:00:00.001Z
                setVarchar\t1970-01-01T00:00:00.001Z
                setLong256\terror: [0] bind variable at 0 is defined as DATE and cannot accept LONG256
                setUuid\terror: [0] bind variable at 0 is defined as DATE and cannot accept UUID
                setArray\terror: [0] bind variable at 0 is defined as DATE and cannot accept ARRAY
                ## window_anchor
                error: create view: [147] ANCHOR EXPRESSION must return TIMESTAMP, LONG, or INT; got DATE
                """);
        rec("TIMESTAMP", """
                ## cast
                target\tmin|max|sentinel|null
                BOOLEAN\ttrue|true|false|false
                BYTE\t1|-1|0|0
                SHORT\t1|-1|0|0
                CHAR\t\\u0001|\\uffff||
                INT\t1|-1|null|null
                LONG\t-9223372036854775807|9223372036854775807|null|null
                DATE\t-290308-12-21T19:59:05.225Z|294247-01-10T04:00:54.775Z||
                TIMESTAMP\t-290308-01-01T19:59:05.224193Z|294247-01-10T04:00:54.775807Z||
                FLOAT\t-9.223372E18|9.223372E18|null|null
                DOUBLE\t-9.223372036854776E18|9.223372036854776E18|null|null
                STRING\t-290308-01-01T19:59:05.224193Z|294247-01-10T04:00:54.775807Z||
                SYMBOL\t-9223372036854775807|9223372036854775807||
                LONG256\t0x8000000000000001|0x7fffffffffffffff||
                GEOBYTE\t0000001|||
                GEOSHORT\t001|||
                GEOINT\t000001|||
                GEOLONG\t00000001|zzzzzzzz||
                BINARY\terror: [20] unsupported cast
                UUID\tno cast [10]
                LONG128\terror: [20] unsupported cast
                IPv4\tno cast [10]
                VARCHAR\t-290308-01-01T19:59:05.224193Z|294247-01-10T04:00:54.775807Z||
                DOUBLE[]\t[-9.223372036854776E18]|[9.223372036854776E18]|null|null
                DECIMAL8\terror: inconvertible value: -9223372036854775807 [LONG -> DECIMAL(2,1)]
                DECIMAL16\terror: inconvertible value: -9223372036854775807 [LONG -> DECIMAL(4,2)]
                DECIMAL32\terror: inconvertible value: -9223372036854775807 [LONG -> DECIMAL(9,0)]
                DECIMAL64\terror: inconvertible value: -9223372036854775807 [LONG -> DECIMAL(16,4)]
                DECIMAL128\t-9223372036854775807.0000000000|9223372036854775807.0000000000||
                DECIMAL256\t-9223372036854775807.00000000000000000000|9223372036854775807.00000000000000000000||
                INTERVAL\terror: [20] unsupported cast
                VARCHAR_SLICE\terror: [20] unsupported cast
                TIMESTAMP_NS\terror: inconvertible value: -9223372036854775807 [TIMESTAMP -> TIMESTAMP_NS]
                GEOHASH(1c)\t1|||
                GEOHASH(8b)\t00000001|||
                GEOHASH(31b)\t0000000000000000000000000000001|||
                GEOHASH(12c)\t000000000001|zzzzzzzzzzzz||
                DECIMAL(5,2)\terror: inconvertible value: -9223372036854775807 [LONG -> DECIMAL(5,2)]
                DECIMAL(18,3)\terror: inconvertible value: -9223372036854775807 [LONG -> DECIMAL(18,3)]
                DOUBLE[][]\t[[-9.223372036854776E18]]|[[9.223372036854776E18]]|null|null
                INTERVAL(us)\terror: [20] unsupported cast
                INTERVAL(ns)\terror: [20] unsupported cast
                ## filter_eq
                props: random_access=true size=unknown timestamp=none
                k\tv
                max\t294247-01-10T04:00:54.775807Z
                ## filter_ne
                props: random_access=true size=unknown timestamp=none
                k\tv
                min\t-290308-01-01T19:59:05.224193Z
                sentinel\t
                null\t
                ## filter_null
                props: random_access=true size=unknown timestamp=none
                k\tv
                sentinel\t
                null\t
                ## filter_not_null
                props: random_access=true size=unknown timestamp=none
                k\tv
                min\t-290308-01-01T19:59:05.224193Z
                max\t294247-01-10T04:00:54.775807Z
                ## filter_lt
                props: random_access=true size=unknown timestamp=none
                k\tv
                min\t-290308-01-01T19:59:05.224193Z
                ## filter_ge
                props: random_access=true size=unknown timestamp=none
                k\tv
                max\t294247-01-10T04:00:54.775807Z
                ## order_asc
                props: random_access=true size=known timestamp=v:asc
                k\tv
                null\t
                sentinel\t
                min\t-290308-01-01T19:59:05.224193Z
                max\t294247-01-10T04:00:54.775807Z
                ## order_desc
                props: random_access=true size=known timestamp=v:desc
                k\tv
                max\t294247-01-10T04:00:54.775807Z
                min\t-290308-01-01T19:59:05.224193Z
                null\t
                sentinel\t
                ## group_by
                props: random_access=true size=known timestamp=f:asc
                v\tc\tf
                -290308-01-01T19:59:05.224193Z\t2\t1970-01-01T00:00:00.000000Z
                294247-01-10T04:00:54.775807Z\t2\t1970-01-01T00:00:01.000000Z
                \t4\t1970-01-01T00:00:02.000000Z
                ## join_inner
                props: random_access=true size=unknown timestamp=none
                tk\tuk\tv
                min\tmin\t-290308-01-01T19:59:05.224193Z
                null\tsentinel\t
                sentinel\tsentinel\t
                ## join_left
                props: random_access=true size=unknown timestamp=none
                tk\tuk\tuv
                max\t\t
                min\tmin\t-290308-01-01T19:59:05.224193Z
                null\tsentinel\t
                sentinel\tsentinel\t
                ## join_left_null
                props: random_access=true size=unknown timestamp=none
                tk\tuv
                max\t
                min\t-290308-01-01T19:59:05.224193Z
                null\t
                sentinel\t
                ## union_all
                props: random_access=false size=known timestamp=none
                k\tv
                min\t-290308-01-01T19:59:05.224193Z
                max\t294247-01-10T04:00:54.775807Z
                sentinel\t
                null\t
                min\t-290308-01-01T19:59:05.224193Z
                sentinel\t
                ## union_null
                props: random_access=false size=known timestamp=none
                k\tv
                min\t-290308-01-01T19:59:05.224193Z
                sentinel\t
                null_branch\t
                ## case_no_else
                props: random_access=true size=known timestamp=none
                k\tc
                min\t
                max\t294247-01-10T04:00:54.775807Z
                sentinel\t
                null\t
                ## case_else
                props: random_access=true size=known timestamp=none
                k\tc
                min\t-290308-01-01T19:59:05.224193Z
                max\t294247-01-10T04:00:54.775807Z
                sentinel\t294247-01-10T04:00:54.775807Z
                null\t294247-01-10T04:00:54.775807Z
                ## lag
                props: random_access=false size=known timestamp=none
                k\tv\tp
                min\t-290308-01-01T19:59:05.224193Z\t
                max\t294247-01-10T04:00:54.775807Z\t-290308-01-01T19:59:05.224193Z
                sentinel\t\t294247-01-10T04:00:54.775807Z
                null\t\t
                ## sample_by
                props: random_access=true size=known timestamp=ts:asc
                ts\tf\tl\tc
                1970-01-01T00:00:00.000000Z\t-290308-01-01T19:59:05.224193Z\t294247-01-10T04:00:54.775807Z\t2
                1970-01-01T00:00:02.000000Z\t\t\t2
                ## first_last
                props: random_access=true size=known timestamp=none
                k\tf\tl
                A\t\t294247-01-10T04:00:54.775807Z
                B\t294247-01-10T04:00:54.775807Z\t
                C\t\t
                ## first_not_null
                props: random_access=true size=known timestamp=none
                k\tf\tl
                A\t294247-01-10T04:00:54.775807Z\t294247-01-10T04:00:54.775807Z
                B\t294247-01-10T04:00:54.775807Z\t294247-01-10T04:00:54.775807Z
                C\t\t
                ## fill_prev
                props: random_access=false size=unknown timestamp=ts:asc
                ts\tv
                1970-01-01T00:00:00.000000Z\t
                1970-01-01T00:00:01.000000Z\t
                1970-01-01T00:00:02.000000Z\t294247-01-10T04:00:54.775807Z
                1970-01-01T00:00:03.000000Z\t294247-01-10T04:00:54.775807Z
                1970-01-01T00:00:04.000000Z\t
                ## latest_on
                props: random_access=true size=known timestamp=none
                k\tv
                b:max\t294247-01-10T04:00:54.775807Z
                b:min\t-290308-01-01T19:59:05.224193Z
                b:null\t
                ## fill_null
                props: random_access=false size=unknown timestamp=ts:asc
                ts\tv
                1970-01-01T00:00:00.000000Z\t-290308-01-01T19:59:05.224193Z
                1970-01-01T00:00:01.000000Z\t
                1970-01-01T00:00:02.000000Z\t294247-01-10T04:00:54.775807Z
                1970-01-01T00:00:03.000000Z\t
                1970-01-01T00:00:04.000000Z\t
                ## fill_value
                error: [46] invalid fill value: '294247-01-10T04:00:54.775807Z'
                ## fill_linear
                error: [11] Unsupported interpolation type: TIMESTAMP
                ## subsample_stride
                min\terror: [60] integer expected for stride
                max\terror: [57] integer expected for stride
                sentinel\terror: [64] integer expected for stride
                null\terror: [38] integer expected for stride
                ## subsample_target
                min\terror: [60] integer expected for target point count
                max\terror: [57] integer expected for target point count
                sentinel\terror: [64] integer expected for target point count
                null\terror: [38] integer expected for target point count
                ## memoized
                props: random_access=true size=known timestamp=none
                k\ta\ta2\ta3
                min\t-290308-01-01T19:59:05.224193Z\t-290308-01-01T19:59:05.224193Z\t-290308-01-01T19:59:05.224193Z
                max\t294247-01-10T04:00:54.775807Z\t294247-01-10T04:00:54.775807Z\t294247-01-10T04:00:54.775807Z
                sentinel\t\t\t
                null\t\t\t
                plan memoizes: true
                ## where_bound_const
                min\tmin,max,sentinel,null
                max\t
                sentinel\t
                null\t
                ## where_bound_bind
                min\tbind error: inconvertible value: `-290308-01-01T19:59:05.224193Z` [STRING -> TIMESTAMP]
                max\tbind error: inconvertible value: `294247-01-10T04:00:54.775807Z` [STRING -> TIMESTAMP]
                sentinel\t
                null\t
                ## where_key
                min\terror: [27] Invalid date [str=-290308-01-01T19:59:05.224193Z]
                max\terror: [27] Invalid date [str=294247-01-10T04:00:54.775807Z]
                sentinel\tb:null
                null\tb:null
                ## copy_bind
                bind error: inconvertible value: `294247-01-10T04:00:54.775807Z` [STRING -> TIMESTAMP]
                ## between_timestamp
                props: random_access=true size=unknown timestamp=none
                k\tv
                ## eq_null_double
                props: random_access=true size=unknown timestamp=none
                k\tv
                sentinel\t
                null\t
                ## bind_value
                setBoolean\terror: [0] bind variable at 0 is defined as TIMESTAMP and cannot accept BOOLEAN
                setByte\t1970-01-01T00:00:00.000001Z
                setShort\t1970-01-01T00:00:00.000001Z
                setChar\t1970-01-01T00:00:00.000001Z
                setInt\t1970-01-01T00:00:00.000001Z
                setLong\t1970-01-01T00:00:00.000001Z
                setFloat\terror: [0] bind variable at 0 is defined as TIMESTAMP and cannot accept FLOAT
                setDouble\terror: [0] bind variable at 0 is defined as TIMESTAMP and cannot accept DOUBLE
                setDate\t1970-01-01T00:00:00.001000Z
                setTimestamp\t1970-01-01T00:00:00.000001Z
                setStr\t1970-01-01T00:00:00.000001Z
                setVarchar\t1970-01-01T00:00:00.000001Z
                setLong256\terror: [0] bind variable at 0 is defined as TIMESTAMP and cannot accept LONG256
                setUuid\terror: [0] bind variable at 0 is defined as TIMESTAMP and cannot accept UUID
                setArray\terror: [0] bind variable at 0 is defined as TIMESTAMP and cannot accept ARRAY
                ## window_anchor
                k\tc
                min\t1
                max\t1
                sentinel\t1
                null\t2
                """);
        rec("FLOAT", """
                ## filter_eq
                props: random_access=true size=unknown timestamp=none
                k\tv
                max\t3.4028235E38
                ## filter_ne
                props: random_access=true size=unknown timestamp=none
                k\tv
                min\t-3.4028235E38
                nan\tnull
                literal_inf\tnull
                negzero\t-0.0
                null\tnull
                inf\tnull
                ninf\tnull
                ## filter_null
                props: random_access=true size=unknown timestamp=none
                k\tv
                nan\tnull
                literal_inf\tnull
                null\tnull
                inf\tnull
                ninf\tnull
                ## filter_not_null
                props: random_access=true size=unknown timestamp=none
                k\tv
                min\t-3.4028235E38
                max\t3.4028235E38
                negzero\t-0.0
                ## filter_lt
                props: random_access=true size=unknown timestamp=none
                k\tv
                min\t-3.4028235E38
                negzero\t-0.0
                ninf\tnull
                ## filter_ge
                props: random_access=true size=unknown timestamp=none
                k\tv
                max\t3.4028235E38
                inf\tnull
                ## order_asc
                props: random_access=true size=known timestamp=none
                k\tv
                ninf\tnull
                min\t-3.4028235E38
                negzero\t-0.0
                max\t3.4028235E38
                inf\tnull
                literal_inf\tnull
                nan\tnull
                null\tnull
                ## order_desc
                props: random_access=true size=known timestamp=none
                k\tv
                literal_inf\tnull
                nan\tnull
                null\tnull
                inf\tnull
                max\t3.4028235E38
                negzero\t-0.0
                min\t-3.4028235E38
                ninf\tnull
                ## group_by
                props: random_access=true size=known timestamp=f:asc
                v\tc\tf
                -3.4028235E38\t2\t1970-01-01T00:00:00.000000Z
                3.4028235E38\t2\t1970-01-01T00:00:01.000000Z
                null\t6\t1970-01-01T00:00:02.000000Z
                -0.0\t2\t1970-01-01T00:00:04.000000Z
                null\t2\t1970-01-01T00:00:06.000000Z
                null\t2\t1970-01-01T00:00:07.000000Z
                ## join_inner
                props: random_access=true size=unknown timestamp=none
                tk\tuk\tv
                inf\tinf\tnull
                literal_inf\tnan\tnull
                min\tmin\t-3.4028235E38
                nan\tnan\tnull
                negzero\tnegzero\t-0.0
                null\tnan\tnull
                ## join_left
                props: random_access=true size=unknown timestamp=none
                tk\tuk\tuv
                inf\tinf\tnull
                literal_inf\tnan\tnull
                max\t\tnull
                min\tmin\t-3.4028235E38
                nan\tnan\tnull
                negzero\tnegzero\t-0.0
                ninf\t\tnull
                null\tnan\tnull
                ## join_left_null
                props: random_access=true size=unknown timestamp=none
                tk\tuv
                inf\tnull
                literal_inf\tnull
                max\tnull
                min\t-3.4028235E38
                nan\tnull
                negzero\t-0.0
                ninf\tnull
                null\tnull
                ## union_all
                props: random_access=false size=known timestamp=none
                k\tv
                min\t-3.4028235E38
                max\t3.4028235E38
                nan\tnull
                literal_inf\tnull
                negzero\t-0.0
                null\tnull
                inf\tnull
                ninf\tnull
                min\t-3.4028235E38
                nan\tnull
                negzero\t-0.0
                inf\tnull
                ## union_null
                props: random_access=false size=known timestamp=none
                k\tv
                min\t-3.4028235E38
                nan\tnull
                negzero\t-0.0
                inf\tnull
                null_branch\tnull
                ## case_no_else
                props: random_access=true size=known timestamp=none
                k\tc
                min\tnull
                max\t3.4028235E38
                nan\tnull
                literal_inf\tnull
                negzero\tnull
                null\tnull
                inf\tnull
                ninf\tnull
                ## case_else
                props: random_access=true size=known timestamp=none
                k\tc
                min\t-3.4028234663852886E38
                max\t3.4028234663852886E38
                nan\t3.4028234663852886E38
                literal_inf\t3.4028234663852886E38
                negzero\t-0.0
                null\t3.4028234663852886E38
                inf\tnull
                ninf\tnull
                ## lag
                props: random_access=false size=known timestamp=none
                k\tv\tp
                min\t-3.4028235E38\tnull
                max\t3.4028235E38\t-3.4028234663852886E38
                nan\tnull\t3.4028234663852886E38
                literal_inf\tnull\tnull
                negzero\t-0.0\tnull
                null\tnull\t-0.0
                inf\tnull\tnull
                ninf\tnull\tnull
                ## sample_by
                props: random_access=true size=known timestamp=ts:asc
                ts\tf\tl\tc
                1970-01-01T00:00:00.000000Z\t-3.4028235E38\t3.4028235E38\t2
                1970-01-01T00:00:02.000000Z\tnull\tnull\t2
                1970-01-01T00:00:04.000000Z\t-0.0\tnull\t2
                1970-01-01T00:00:06.000000Z\tnull\tnull\t2
                ## first_last
                props: random_access=true size=known timestamp=none
                k\tf\tl
                A\tnull\t3.4028235E38
                B\t3.4028235E38\tnull
                C\tnull\tnull
                ## first_not_null
                props: random_access=true size=known timestamp=none
                k\tf\tl
                A\t3.4028235E38\t3.4028235E38
                B\t3.4028235E38\t3.4028235E38
                C\tnull\tnull
                ## fill_prev
                props: random_access=false size=unknown timestamp=ts:asc
                ts\tv
                1970-01-01T00:00:00.000000Z\tnull
                1970-01-01T00:00:01.000000Z\tnull
                1970-01-01T00:00:02.000000Z\t3.4028235E38
                1970-01-01T00:00:03.000000Z\t3.4028235E38
                1970-01-01T00:00:04.000000Z\tnull
                ## latest_on
                props: random_access=true size=known timestamp=none
                k\tv
                b:inf\tnull
                b:max\t3.4028235E38
                b:min\t-3.4028235E38
                b:negzero\t-0.0
                b:ninf\tnull
                b:null\tnull
                ## cast
                target\tmin|max|nan|literal_inf|negzero|null|inf|ninf
                BOOLEAN\ttrue|true|false|false|false|false|false|false
                BYTE\t0|0|0|0|0|0|0|0
                SHORT\t0|0|0|0|0|0|0|0
                CHAR\t|\\uffff|||||\\uffff|
                INT\tnull|null|null|null|0|null|null|null
                LONG\tnull|null|null|null|0|null|null|null
                DATE\t||||1970-01-01T00:00:00.000Z|||
                TIMESTAMP\t||||1970-01-01T00:00:00.000000Z|||
                FLOAT\t-3.4028235E38|3.4028235E38|null|null|-0.0|null|null|null
                DOUBLE\t-3.4028234663852886E38|3.4028234663852886E38|null|null|-0.0|null|null|null
                STRING\t-3.4028235E38|3.4028235E38|||-0.0|||
                SYMBOL\t-3.4028235E38|3.4028235E38|||-0.0|||
                LONG256\t0x8000000000000000|0x7fffffffffffffff|||0x00|||
                GEOBYTE\tno cast [10]
                GEOSHORT\tno cast [10]
                GEOINT\tno cast [10]
                GEOLONG\tno cast [10]
                BINARY\terror: [20] unsupported cast
                UUID\tno cast [10]
                LONG128\terror: [20] unsupported cast
                IPv4\tno cast [10]
                VARCHAR\t-3.4028235E38|3.4028235E38|||-0.0|||
                DOUBLE[]\t[-3.4028234663852886E38]|[3.4028234663852886E38]|null|null|[-0.0]|null|null|null
                DECIMAL8\terror: inconvertible value: `-3.4028235E38` [FLOAT -> DECIMAL(2,1)]
                DECIMAL16\terror: inconvertible value: `-3.4028235E38` [FLOAT -> DECIMAL(4,2)]
                DECIMAL32\terror: inconvertible value: `-3.4028235E38` [FLOAT -> DECIMAL(9,0)]
                DECIMAL64\terror: inconvertible value: `-3.4028235E38` [FLOAT -> DECIMAL(16,4)]
                DECIMAL128\terror: inconvertible value: `-3.4028235E38` [FLOAT -> DECIMAL(38,10)]
                DECIMAL256\t-340282350000000000000000000000000000000.00000000000000000000|340282350000000000000000000000000000000.00000000000000000000|||0.00000000000000000000|||
                INTERVAL\terror: [20] unsupported cast
                VARCHAR_SLICE\terror: [20] unsupported cast
                TIMESTAMP_NS\t||||1970-01-01T00:00:00.000000000Z|||
                GEOHASH(1c)\tno cast [10]
                GEOHASH(8b)\tno cast [10]
                GEOHASH(31b)\tno cast [10]
                GEOHASH(12c)\tno cast [10]
                DECIMAL(5,2)\terror: inconvertible value: `-3.4028235E38` [FLOAT -> DECIMAL(5,2)]
                DECIMAL(18,3)\terror: inconvertible value: `-3.4028235E38` [FLOAT -> DECIMAL(18,3)]
                DOUBLE[][]\t[[-3.4028234663852886E38]]|[[3.4028234663852886E38]]|null|null|[[-0.0]]|null|null|null
                INTERVAL(us)\terror: [20] unsupported cast
                INTERVAL(ns)\terror: [20] unsupported cast
                ## fill_null
                props: random_access=false size=unknown timestamp=ts:asc
                ts\tv
                1970-01-01T00:00:00.000000Z\t-3.4028235E38
                1970-01-01T00:00:01.000000Z\tnull
                1970-01-01T00:00:02.000000Z\t3.4028235E38
                1970-01-01T00:00:03.000000Z\tnull
                1970-01-01T00:00:04.000000Z\tnull
                ## fill_value
                props: random_access=false size=unknown timestamp=ts:asc
                ts\tv
                1970-01-01T00:00:00.000000Z\t-3.4028235E38
                1970-01-01T00:00:01.000000Z\t3.4028235E38
                1970-01-01T00:00:02.000000Z\t3.4028235E38
                1970-01-01T00:00:03.000000Z\t3.4028235E38
                1970-01-01T00:00:04.000000Z\tnull
                ## fill_linear
                props: random_access=true size=known timestamp=ts:asc
                ts\tv
                1970-01-01T00:00:00.000000Z\t-3.4028235E38
                1970-01-01T00:00:01.000000Z\t0.0
                1970-01-01T00:00:02.000000Z\t3.4028235E38
                1970-01-01T00:00:03.000000Z\tnull
                1970-01-01T00:00:04.000000Z\tnull
                ## subsample_stride
                min\terror: [62] integer expected for stride
                max\terror: [59] integer expected for stride
                nan\terror: [43] integer expected for stride
                literal_inf\terror: [48] integer expected for stride
                negzero\terror: [44] integer expected for stride
                null\terror: [38] integer expected for stride
                ## subsample_target
                min\terror: [62] integer expected for target point count
                max\terror: [59] integer expected for target point count
                nan\terror: [43] integer expected for target point count
                literal_inf\terror: [48] integer expected for target point count
                negzero\terror: [44] integer expected for target point count
                null\terror: [38] integer expected for target point count
                ## memoized
                props: random_access=true size=known timestamp=none
                k\ta\ta2\ta3
                min\t-3.4028235E38\t-3.4028235E38\t-3.4028235E38
                max\t3.4028235E38\t3.4028235E38\t3.4028235E38
                nan\tnull\tnull\tnull
                literal_inf\tnull\tnull\tnull
                negzero\t-0.0\t-0.0\t-0.0
                null\tnull\tnull\tnull
                inf\tnull\tnull\tnull
                ninf\tnull\tnull\tnull
                plan memoizes: true
                ## where_bound_const
                min\tmin,max,nan,literal_inf,negzero,null,inf,ninf
                max\t
                nan\t
                literal_inf\t
                negzero\tmin,max,nan,literal_inf,negzero,null,inf,ninf
                null\t
                ## where_bound_bind
                min\tmin,max,nan,literal_inf,negzero,null,inf,ninf
                max\t
                nan\t
                literal_inf\t
                negzero\tmin,max,nan,literal_inf,negzero,null,inf,ninf
                null\t
                ## where_key
                min\t
                max\t
                nan\tb:null,b:inf,b:ninf
                literal_inf\tb:null,b:inf,b:ninf
                negzero\tb:negzero
                null\tb:null,b:inf,b:ninf
                ## copy_bind
                status\tmessage
                finished\t
                k
                max
                ## between_timestamp
                error: [27] there is no matching operator `between` with the argument type: FLOAT
                ## eq_null_double
                props: random_access=true size=unknown timestamp=none
                k\tv
                nan\tnull
                literal_inf\tnull
                null\tnull
                inf\tnull
                ninf\tnull
                ## bind_value
                setBoolean\terror: [0] bind variable at 0 is defined as FLOAT and cannot accept BOOLEAN
                setByte\t1.0
                setShort\t1.0
                setChar\t1.0
                setInt\t1.0
                setLong\t1.0
                setFloat\t1.5
                setDouble\t1.5
                setDate\t1.0
                setTimestamp\t1.0
                setStr\t1.0
                setVarchar\t1.0
                setLong256\terror: [0] bind variable at 0 is defined as FLOAT and cannot accept LONG256
                setUuid\terror: [0] bind variable at 0 is defined as FLOAT and cannot accept UUID
                setArray\terror: [0] bind variable at 0 is defined as FLOAT and cannot accept ARRAY
                ## window_anchor
                error: create view: [147] ANCHOR EXPRESSION must return TIMESTAMP, LONG, or INT; got FLOAT
                """);
        rec("DOUBLE", """
                ## cast
                target\tmin|max|nan|literal_inf|negzero|null|inf|ninf
                BOOLEAN\ttrue|true|false|false|false|false|false|false
                BYTE\t0|0|0|0|0|0|0|0
                SHORT\t0|0|0|0|0|0|0|0
                CHAR\t|\\uffff|||||\\uffff|
                INT\tnull|null|null|null|0|null|null|null
                LONG\tnull|null|null|null|0|null|null|null
                DATE\t||||1970-01-01T00:00:00.000Z|||
                TIMESTAMP\t||||1970-01-01T00:00:00.000000Z|||
                FLOAT\tnull|null|null|null|-0.0|null|null|null
                DOUBLE\t-1.7976931348623157E308|1.7976931348623157E308|null|null|-0.0|null|null|null
                STRING\t-1.7976931348623157E308|1.7976931348623157E308|||-0.0|||
                SYMBOL\t-1.7976931348623157E308|1.7976931348623157E308|||-0.0|||
                LONG256\t0x8000000000000000|0x7fffffffffffffff|||0x00|||
                GEOBYTE\tno cast [10]
                GEOSHORT\tno cast [10]
                GEOINT\tno cast [10]
                GEOLONG\tno cast [10]
                BINARY\terror: [20] unsupported cast
                UUID\tno cast [10]
                LONG128\terror: [20] unsupported cast
                IPv4\tno cast [10]
                VARCHAR\t-1.7976931348623157E308|1.7976931348623157E308|||-0.0|||
                DOUBLE[]\t[-1.7976931348623157E308]|[1.7976931348623157E308]|null|null|[-0.0]|null|null|null
                DECIMAL8\terror: inconvertible value: -1.7976931348623157E308 [DOUBLE -> DECIMAL(2,1)]
                DECIMAL16\terror: inconvertible value: -1.7976931348623157E308 [DOUBLE -> DECIMAL(4,2)]
                DECIMAL32\terror: inconvertible value: -1.7976931348623157E308 [DOUBLE -> DECIMAL(9,0)]
                DECIMAL64\terror: inconvertible value: -1.7976931348623157E308 [DOUBLE -> DECIMAL(16,4)]
                DECIMAL128\terror: inconvertible value: -1.7976931348623157E308 [DOUBLE -> DECIMAL(38,10)]
                DECIMAL256\terror: inconvertible value: -1.7976931348623157E308 [DOUBLE -> DECIMAL(76,20)]
                INTERVAL\terror: [20] unsupported cast
                VARCHAR_SLICE\terror: [20] unsupported cast
                TIMESTAMP_NS\t||||1970-01-01T00:00:00.000000000Z|||
                GEOHASH(1c)\tno cast [10]
                GEOHASH(8b)\tno cast [10]
                GEOHASH(31b)\tno cast [10]
                GEOHASH(12c)\tno cast [10]
                DECIMAL(5,2)\terror: inconvertible value: -1.7976931348623157E308 [DOUBLE -> DECIMAL(5,2)]
                DECIMAL(18,3)\terror: inconvertible value: -1.7976931348623157E308 [DOUBLE -> DECIMAL(18,3)]
                DOUBLE[][]\t[[-1.7976931348623157E308]]|[[1.7976931348623157E308]]|null|null|[[-0.0]]|null|null|null
                INTERVAL(us)\terror: [20] unsupported cast
                INTERVAL(ns)\terror: [20] unsupported cast
                ## filter_eq
                props: random_access=true size=unknown timestamp=none
                k\tv
                max\t1.7976931348623157E308
                ## filter_ne
                props: random_access=true size=unknown timestamp=none
                k\tv
                min\t-1.7976931348623157E308
                nan\tnull
                literal_inf\tnull
                negzero\t-0.0
                null\tnull
                inf\tnull
                ninf\tnull
                ## filter_null
                props: random_access=true size=unknown timestamp=none
                k\tv
                nan\tnull
                literal_inf\tnull
                null\tnull
                inf\tnull
                ninf\tnull
                ## filter_not_null
                props: random_access=true size=unknown timestamp=none
                k\tv
                min\t-1.7976931348623157E308
                max\t1.7976931348623157E308
                negzero\t-0.0
                ## filter_lt
                props: random_access=true size=unknown timestamp=none
                k\tv
                min\t-1.7976931348623157E308
                negzero\t-0.0
                ninf\tnull
                ## filter_ge
                props: random_access=true size=unknown timestamp=none
                k\tv
                max\t1.7976931348623157E308
                inf\tnull
                ## order_asc
                props: random_access=true size=known timestamp=none
                k\tv
                ninf\tnull
                min\t-1.7976931348623157E308
                negzero\t-0.0
                max\t1.7976931348623157E308
                inf\tnull
                literal_inf\tnull
                nan\tnull
                null\tnull
                ## order_desc
                props: random_access=true size=known timestamp=none
                k\tv
                literal_inf\tnull
                nan\tnull
                null\tnull
                inf\tnull
                max\t1.7976931348623157E308
                negzero\t-0.0
                min\t-1.7976931348623157E308
                ninf\tnull
                ## group_by
                props: random_access=true size=known timestamp=f:asc
                v\tc\tf
                -1.7976931348623157E308\t2\t1970-01-01T00:00:00.000000Z
                1.7976931348623157E308\t2\t1970-01-01T00:00:01.000000Z
                null\t6\t1970-01-01T00:00:02.000000Z
                -0.0\t2\t1970-01-01T00:00:04.000000Z
                null\t2\t1970-01-01T00:00:06.000000Z
                null\t2\t1970-01-01T00:00:07.000000Z
                ## join_inner
                props: random_access=true size=unknown timestamp=none
                tk\tuk\tv
                inf\tinf\tnull
                literal_inf\tnan\tnull
                min\tmin\t-1.7976931348623157E308
                nan\tnan\tnull
                negzero\tnegzero\t-0.0
                null\tnan\tnull
                ## join_left
                props: random_access=true size=unknown timestamp=none
                tk\tuk\tuv
                inf\tinf\tnull
                literal_inf\tnan\tnull
                max\t\tnull
                min\tmin\t-1.7976931348623157E308
                nan\tnan\tnull
                negzero\tnegzero\t-0.0
                ninf\t\tnull
                null\tnan\tnull
                ## join_left_null
                props: random_access=true size=unknown timestamp=none
                tk\tuv
                inf\tnull
                literal_inf\tnull
                max\tnull
                min\t-1.7976931348623157E308
                nan\tnull
                negzero\t-0.0
                ninf\tnull
                null\tnull
                ## union_all
                props: random_access=false size=known timestamp=none
                k\tv
                min\t-1.7976931348623157E308
                max\t1.7976931348623157E308
                nan\tnull
                literal_inf\tnull
                negzero\t-0.0
                null\tnull
                inf\tnull
                ninf\tnull
                min\t-1.7976931348623157E308
                nan\tnull
                negzero\t-0.0
                inf\tnull
                ## union_null
                props: random_access=false size=known timestamp=none
                k\tv
                min\t-1.7976931348623157E308
                nan\tnull
                negzero\t-0.0
                inf\tnull
                null_branch\tnull
                ## case_no_else
                props: random_access=true size=known timestamp=none
                k\tc
                min\tnull
                max\t1.7976931348623157E308
                nan\tnull
                literal_inf\tnull
                negzero\tnull
                null\tnull
                inf\tnull
                ninf\tnull
                ## case_else
                props: random_access=true size=known timestamp=none
                k\tc
                min\t-1.7976931348623157E308
                max\t1.7976931348623157E308
                nan\t1.7976931348623157E308
                literal_inf\t1.7976931348623157E308
                negzero\t-0.0
                null\t1.7976931348623157E308
                inf\tnull
                ninf\tnull
                ## lag
                props: random_access=false size=known timestamp=none
                k\tv\tp
                min\t-1.7976931348623157E308\tnull
                max\t1.7976931348623157E308\t-1.7976931348623157E308
                nan\tnull\t1.7976931348623157E308
                literal_inf\tnull\tnull
                negzero\t-0.0\tnull
                null\tnull\t-0.0
                inf\tnull\tnull
                ninf\tnull\tnull
                ## sample_by
                props: random_access=true size=known timestamp=ts:asc
                ts\tf\tl\tc
                1970-01-01T00:00:00.000000Z\t-1.7976931348623157E308\t1.7976931348623157E308\t2
                1970-01-01T00:00:02.000000Z\tnull\tnull\t2
                1970-01-01T00:00:04.000000Z\t-0.0\tnull\t2
                1970-01-01T00:00:06.000000Z\tnull\tnull\t2
                ## first_last
                props: random_access=true size=known timestamp=none
                k\tf\tl
                A\tnull\t1.7976931348623157E308
                B\t1.7976931348623157E308\tnull
                C\tnull\tnull
                ## first_not_null
                props: random_access=true size=known timestamp=none
                k\tf\tl
                A\t1.7976931348623157E308\t1.7976931348623157E308
                B\t1.7976931348623157E308\t1.7976931348623157E308
                C\tnull\tnull
                ## fill_prev
                props: random_access=false size=unknown timestamp=ts:asc
                ts\tv
                1970-01-01T00:00:00.000000Z\tnull
                1970-01-01T00:00:01.000000Z\tnull
                1970-01-01T00:00:02.000000Z\t1.7976931348623157E308
                1970-01-01T00:00:03.000000Z\t1.7976931348623157E308
                1970-01-01T00:00:04.000000Z\tnull
                ## latest_on
                props: random_access=true size=known timestamp=none
                k\tv
                b:inf\tnull
                b:max\t1.7976931348623157E308
                b:min\t-1.7976931348623157E308
                b:negzero\t-0.0
                b:ninf\tnull
                b:null\tnull
                ## fill_null
                props: random_access=false size=unknown timestamp=ts:asc
                ts\tv
                1970-01-01T00:00:00.000000Z\t-1.7976931348623157E308
                1970-01-01T00:00:01.000000Z\tnull
                1970-01-01T00:00:02.000000Z\t1.7976931348623157E308
                1970-01-01T00:00:03.000000Z\tnull
                1970-01-01T00:00:04.000000Z\tnull
                ## fill_value
                props: random_access=false size=unknown timestamp=ts:asc
                ts\tv
                1970-01-01T00:00:00.000000Z\t-1.7976931348623157E308
                1970-01-01T00:00:01.000000Z\t1.7976931348623157E308
                1970-01-01T00:00:02.000000Z\t1.7976931348623157E308
                1970-01-01T00:00:03.000000Z\t1.7976931348623157E308
                1970-01-01T00:00:04.000000Z\tnull
                ## fill_linear
                props: random_access=true size=known timestamp=ts:asc
                ts\tv
                1970-01-01T00:00:00.000000Z\t-1.7976931348623157E308
                1970-01-01T00:00:01.000000Z\tnull
                1970-01-01T00:00:02.000000Z\t1.7976931348623157E308
                1970-01-01T00:00:03.000000Z\tnull
                1970-01-01T00:00:04.000000Z\tnull
                ## subsample_stride
                min\terror: [63] integer expected for stride
                max\terror: [60] integer expected for stride
                nan\terror: [43] integer expected for stride
                literal_inf\terror: [48] integer expected for stride
                negzero\terror: [44] integer expected for stride
                null\terror: [38] integer expected for stride
                ## subsample_target
                min\terror: [63] integer expected for target point count
                max\terror: [60] integer expected for target point count
                nan\terror: [43] integer expected for target point count
                literal_inf\terror: [48] integer expected for target point count
                negzero\terror: [44] integer expected for target point count
                null\terror: [38] integer expected for target point count
                ## memoized
                props: random_access=true size=known timestamp=none
                k\ta\ta2\ta3
                min\t-1.7976931348623157E308\t-1.7976931348623157E308\t-1.7976931348623157E308
                max\t1.7976931348623157E308\t1.7976931348623157E308\t1.7976931348623157E308
                nan\tnull\tnull\tnull
                literal_inf\tnull\tnull\tnull
                negzero\t-0.0\t-0.0\t-0.0
                null\tnull\tnull\tnull
                inf\tnull\tnull\tnull
                ninf\tnull\tnull\tnull
                plan memoizes: true
                ## where_bound_const
                min\tmin,max,nan,literal_inf,negzero,null,inf,ninf
                max\t
                nan\t
                literal_inf\t
                negzero\tmin,max,nan,literal_inf,negzero,null,inf,ninf
                null\t
                ## where_bound_bind
                min\tmin,max,nan,literal_inf,negzero,null,inf,ninf
                max\t
                nan\t
                literal_inf\t
                negzero\tmin,max,nan,literal_inf,negzero,null,inf,ninf
                null\t
                ## where_key
                min\tb:min
                max\tb:max
                nan\tb:null,b:inf,b:ninf
                literal_inf\tb:null,b:inf,b:ninf
                negzero\tb:negzero
                null\tb:null,b:inf,b:ninf
                ## copy_bind
                status\tmessage
                finished\t
                k
                max
                ## between_timestamp
                error: [27] there is no matching operator `between` with the argument type: DOUBLE
                ## eq_null_double
                props: random_access=true size=unknown timestamp=none
                k\tv
                nan\tnull
                literal_inf\tnull
                null\tnull
                inf\tnull
                ninf\tnull
                ## bind_value
                setBoolean\terror: [0] bind variable at 0 is defined as DOUBLE and cannot accept BOOLEAN
                setByte\t1.0
                setShort\t1.0
                setChar\t1.0
                setInt\t1.0
                setLong\t1.0
                setFloat\t1.5
                setDouble\t1.5
                setDate\t1.0
                setTimestamp\t1.0
                setStr\t1.0
                setVarchar\t1.0
                setLong256\terror: [0] bind variable at 0 is defined as DOUBLE and cannot accept LONG256
                setUuid\terror: [0] bind variable at 0 is defined as DOUBLE and cannot accept UUID
                setArray\terror: [0] bind variable at 0 is defined as DOUBLE and cannot accept ARRAY
                ## window_anchor
                error: create view: [147] ANCHOR EXPRESSION must return TIMESTAMP, LONG, or INT; got DOUBLE
                """);
        rec("STRING", """
                ## filter_eq
                props: random_access=true size=unknown timestamp=none
                k\tv
                max\tü€😀�
                ## filter_ne
                props: random_access=true size=unknown timestamp=none
                k\tv
                empty\t
                min\t\s
                escape\ta"b,c\\d'e
                null\t
                ## filter_null
                props: random_access=true size=unknown timestamp=none
                k\tv
                null\t
                ## filter_not_null
                props: random_access=true size=unknown timestamp=none
                k\tv
                empty\t
                min\t\s
                max\tü€😀�
                escape\ta"b,c\\d'e
                ## filter_lt
                props: random_access=true size=unknown timestamp=none
                k\tv
                empty\t
                min\t\s
                escape\ta"b,c\\d'e
                ## filter_ge
                props: random_access=true size=unknown timestamp=none
                k\tv
                max\tü€😀�
                ## order_asc
                props: random_access=true size=known timestamp=none
                k\tv
                null\t
                empty\t
                min\t\s
                escape\ta"b,c\\d'e
                max\tü€😀�
                ## order_desc
                props: random_access=true size=known timestamp=none
                k\tv
                max\tü€😀�
                escape\ta"b,c\\d'e
                min\t\s
                empty\t
                null\t
                ## group_by
                props: random_access=true size=known timestamp=f:asc
                v\tc\tf
                \t2\t1970-01-01T00:00:00.000000Z
                 \t2\t1970-01-01T00:00:01.000000Z
                ü€😀�\t2\t1970-01-01T00:00:02.000000Z
                a"b,c\\d'e\t2\t1970-01-01T00:00:03.000000Z
                \t2\t1970-01-01T00:00:04.000000Z
                ## join_inner
                props: random_access=true size=unknown timestamp=none
                tk\tuk\tv
                empty\tempty\t
                max\tmax\tü€😀�
                null\tnull\t
                ## join_left
                props: random_access=true size=unknown timestamp=none
                tk\tuk\tuv
                empty\tempty\t
                escape\t\t
                max\tmax\tü€😀�
                min\t\t
                null\tnull\t
                ## join_left_null
                props: random_access=true size=unknown timestamp=none
                tk\tuv
                empty\t
                escape\t
                max\tü€😀�
                min\t
                null\t
                ## union_all
                props: random_access=false size=known timestamp=none
                k\tv
                empty\t
                min\t\s
                max\tü€😀�
                escape\ta"b,c\\d'e
                null\t
                empty\t
                max\tü€😀�
                null\t
                ## union_null
                props: random_access=false size=known timestamp=none
                k\tv
                empty\t
                max\tü€😀�
                null\t
                null_branch\t
                ## case_no_else
                props: random_access=true size=known timestamp=none
                k\tc
                empty\t
                min\t
                max\tü€😀�
                escape\t
                null\t
                ## case_else
                props: random_access=true size=known timestamp=none
                k\tc
                empty\t
                min\t\s
                max\tü€😀�
                escape\ta"b,c\\d'e
                null\tü€😀�
                ## lag
                error: inconvertible value: `` [STRING -> DOUBLE]
                ## sample_by
                props: random_access=true size=known timestamp=ts:asc
                ts\tf\tl\tc
                1970-01-01T00:00:00.000000Z\t\t \t2
                1970-01-01T00:00:02.000000Z\tü€😀�\ta"b,c\\d'e\t2
                1970-01-01T00:00:04.000000Z\t\t\t1
                ## first_last
                props: random_access=true size=known timestamp=none
                k\tf\tl
                A\t\tü€😀�
                B\tü€😀�\t
                C\t\t
                ## first_not_null
                props: random_access=true size=known timestamp=none
                k\tf\tl
                A\tü€😀�\tü€😀�
                B\tü€😀�\tü€😀�
                C\t\t
                ## fill_prev
                props: random_access=false size=unknown timestamp=ts:asc
                ts\tv
                1970-01-01T00:00:00.000000Z\t
                1970-01-01T00:00:01.000000Z\t
                1970-01-01T00:00:02.000000Z\tü€😀�
                1970-01-01T00:00:03.000000Z\tü€😀�
                1970-01-01T00:00:04.000000Z\t
                ## latest_on
                props: random_access=true size=known timestamp=none
                k\tv
                b:empty\t
                b:escape\ta"b,c\\d'e
                b:max\tü€😀�
                b:min\t\s
                b:null\t
                ## cast
                target\tempty|min|max|escape|null
                BOOLEAN\tfalse|false|false|false|false
                BYTE\t0|0|0|0|0
                SHORT\t0|0|0|0|0
                CHAR\t| |ü|a|
                INT\tnull|null|null|null|null
                LONG\tnull|null|null|null|null
                DATE\t||||
                TIMESTAMP\t||||
                FLOAT\tnull|null|null|null|null
                DOUBLE\tnull|null|null|null|null
                STRING\t| |ü€😀�|a"b,c\\d'e|
                SYMBOL\t| |ü€😀�|a"b,c\\d'e|
                LONG256\t||||
                GEOBYTE\t||||
                GEOSHORT\t||||
                GEOINT\t||||
                GEOLONG\t||||
                BINARY\terror: [20] unsupported cast
                UUID\t||||
                LONG128\terror: [20] unsupported cast
                IPv4\t||||
                VARCHAR\t| |ü€😀�|a"b,c\\d'e|
                DOUBLE[]\tnull|null|null|null|null
                DECIMAL8\terror: inconvertible value: `` [STRING -> DECIMAL(2,1)]
                DECIMAL16\terror: inconvertible value: `` [STRING -> DECIMAL(4,2)]
                DECIMAL32\terror: inconvertible value: `` [STRING -> DECIMAL(9,0)]
                DECIMAL64\terror: inconvertible value: `` [STRING -> DECIMAL(16,4)]
                DECIMAL128\terror: inconvertible value: `` [STRING -> DECIMAL(38,10)]
                DECIMAL256\terror: inconvertible value: `` [STRING -> DECIMAL(76,20)]
                INTERVAL\terror: [20] unsupported cast
                VARCHAR_SLICE\terror: [20] unsupported cast
                TIMESTAMP_NS\t||||
                GEOHASH(1c)\t||||
                GEOHASH(8b)\t||||
                GEOHASH(31b)\t||||
                GEOHASH(12c)\t||||
                DECIMAL(5,2)\terror: inconvertible value: `` [STRING -> DECIMAL(5,2)]
                DECIMAL(18,3)\terror: inconvertible value: `` [STRING -> DECIMAL(18,3)]
                DOUBLE[][]\tnull|null|null|null|null
                INTERVAL(us)\terror: [20] unsupported cast
                INTERVAL(ns)\terror: [20] unsupported cast
                ## fill_null
                props: random_access=false size=unknown timestamp=ts:asc
                ts\tv
                1970-01-01T00:00:00.000000Z\t\s
                1970-01-01T00:00:01.000000Z\t
                1970-01-01T00:00:02.000000Z\tü€😀�
                1970-01-01T00:00:03.000000Z\t
                1970-01-01T00:00:04.000000Z\t
                ## fill_value
                props: random_access=false size=unknown timestamp=ts:asc
                ts\tv
                1970-01-01T00:00:00.000000Z\t\s
                1970-01-01T00:00:01.000000Z\tü€😀�
                1970-01-01T00:00:02.000000Z\tü€😀�
                1970-01-01T00:00:03.000000Z\tü€😀�
                1970-01-01T00:00:04.000000Z\t
                ## fill_linear
                error: [46] support for LINEAR fill is not yet implemented [function=last(v), class=io.questdb.griffin.engine.functions.groupby.LastStrGroupByFunction]
                ## subsample_stride
                empty\terror: [40] integer expected for stride
                min\terror: [41] integer expected for stride
                max\terror: [45] integer expected for stride
                escape\terror: [50] integer expected for stride
                null\terror: [38] integer expected for stride
                ## subsample_target
                empty\terror: [40] integer expected for target point count
                min\terror: [41] integer expected for target point count
                max\terror: [45] integer expected for target point count
                escape\terror: [50] integer expected for target point count
                null\terror: [38] integer expected for target point count
                ## memoized
                props: random_access=true size=known timestamp=none
                k\ta\ta2\ta3
                empty\t\t\t
                min\t \t \t\s
                max\tü€😀�\tü€😀�\tü€😀�
                escape\ta"b,c\\d'e\ta"b,c\\d'e\ta"b,c\\d'e
                null\t\t\t
                plan memoizes: true
                ## where_bound_const
                empty\terror: inconvertible value: `` [STRING -> LONG]
                min\terror: inconvertible value: ` ` [STRING -> LONG]
                max\terror: inconvertible value: `ü€😀�` [STRING -> LONG]
                escape\terror: inconvertible value: `a"b,c\\d'e` [STRING -> LONG]
                null\t
                ## where_bound_bind
                empty\terror: inconvertible value: `` [STRING -> LONG]
                min\terror: inconvertible value: ` ` [STRING -> LONG]
                max\terror: inconvertible value: `ü€😀�` [STRING -> LONG]
                escape\terror: inconvertible value: `a"b,c\\d'e` [STRING -> LONG]
                null\t
                ## where_key
                empty\tb:empty
                min\tb:min
                max\tb:max
                escape\tb:escape
                null\tb:null
                ## copy_bind
                status\tmessage
                finished\t
                k
                max
                ## between_timestamp
                error: inconvertible value: `` [STRING -> TIMESTAMP_NS]
                ## eq_null_double
                props: random_access=true size=unknown timestamp=none
                k\tv
                null\t
                ## bind_value
                setBoolean\ttrue
                setByte\t1
                setShort\t1
                setChar\t1
                setInt\t1
                setLong\t1
                setFloat\t1.5
                setDouble\t1.5
                setDate\t1
                setTimestamp\t1970-01-01T00:00:00.000001Z
                setStr\t1
                setVarchar\t1
                setLong256\t0x01
                setUuid\t00000000-0000-0000-0000-000000000001
                setArray\terror: [0] bind variable at 0 is defined as STRING and cannot accept ARRAY
                ## window_anchor
                error: create view: [147] ANCHOR EXPRESSION must return TIMESTAMP, LONG, or INT; got STRING
                """);
        rec("SYMBOL", """
                ## filter_eq
                props: random_access=true size=unknown timestamp=none
                k\tv
                max\tü€😀�
                ## filter_ne
                props: random_access=true size=unknown timestamp=none
                k\tv
                empty\t
                min\t\s
                escape\ta"b,c\\d'e
                null\t
                ## filter_null
                props: random_access=true size=unknown timestamp=none
                k\tv
                null\t
                ## filter_not_null
                props: random_access=true size=unknown timestamp=none
                k\tv
                empty\t
                min\t\s
                max\tü€😀�
                escape\ta"b,c\\d'e
                ## filter_lt
                props: random_access=true size=unknown timestamp=none
                k\tv
                empty\t
                min\t\s
                escape\ta"b,c\\d'e
                ## filter_ge
                props: random_access=true size=unknown timestamp=none
                k\tv
                max\tü€😀�
                ## order_asc
                props: random_access=true size=known timestamp=none
                k\tv
                null\t
                empty\t
                min\t\s
                escape\ta"b,c\\d'e
                max\tü€😀�
                ## order_desc
                props: random_access=true size=known timestamp=none
                k\tv
                max\tü€😀�
                escape\ta"b,c\\d'e
                min\t\s
                empty\t
                null\t
                ## group_by
                props: random_access=true size=known timestamp=f:asc
                v\tc\tf
                \t2\t1970-01-01T00:00:00.000000Z
                 \t2\t1970-01-01T00:00:01.000000Z
                ü€😀�\t2\t1970-01-01T00:00:02.000000Z
                a"b,c\\d'e\t2\t1970-01-01T00:00:03.000000Z
                \t2\t1970-01-01T00:00:04.000000Z
                ## join_inner
                props: random_access=true size=unknown timestamp=none
                tk\tuk\tv
                empty\tempty\t
                max\tmax\tü€😀�
                null\tnull\t
                ## join_left
                props: random_access=true size=unknown timestamp=none
                tk\tuk\tuv
                empty\tempty\t
                escape\t\t
                max\tmax\tü€😀�
                min\t\t
                null\tnull\t
                ## join_left_null
                props: random_access=true size=unknown timestamp=none
                tk\tuv
                empty\t
                escape\t
                max\tü€😀�
                min\t
                null\t
                ## union_all
                props: random_access=false size=known timestamp=none
                k\tv
                empty\t
                min\t\s
                max\tü€😀�
                escape\ta"b,c\\d'e
                null\t
                empty\t
                max\tü€😀�
                null\t
                ## union_null
                props: random_access=false size=known timestamp=none
                k\tv
                empty\t
                max\tü€😀�
                null\t
                null_branch\t
                ## case_no_else
                props: random_access=true size=known timestamp=none
                k\tc
                empty\t
                min\t
                max\tü€😀�
                escape\t
                null\t
                ## case_else
                props: random_access=true size=known timestamp=none
                k\tc
                empty\t
                min\t\s
                max\tü€😀�
                escape\ta"b,c\\d'e
                null\tü€😀�
                ## lag
                props: random_access=false size=known timestamp=none
                k\tv\tp
                empty\t\t
                min\t \t
                max\tü€😀�\t\s
                escape\ta"b,c\\d'e\tü€😀�
                null\t\ta"b,c\\d'e
                ## sample_by
                props: random_access=true size=known timestamp=ts:asc
                ts\tf\tl\tc
                1970-01-01T00:00:00.000000Z\t\t \t2
                1970-01-01T00:00:02.000000Z\tü€😀�\ta"b,c\\d'e\t2
                1970-01-01T00:00:04.000000Z\t\t\t1
                ## first_last
                props: random_access=true size=known timestamp=none
                k\tf\tl
                A\t\tü€😀�
                B\tü€😀�\t
                C\t\t
                ## first_not_null
                props: random_access=true size=known timestamp=none
                k\tf\tl
                A\tü€😀�\tü€😀�
                B\tü€😀�\tü€😀�
                C\t\t
                ## fill_prev
                props: random_access=false size=unknown timestamp=ts:asc
                ts\tv
                1970-01-01T00:00:00.000000Z\t
                1970-01-01T00:00:01.000000Z\t
                1970-01-01T00:00:02.000000Z\tü€😀�
                1970-01-01T00:00:03.000000Z\tü€😀�
                1970-01-01T00:00:04.000000Z\t
                ## latest_on
                props: random_access=true size=known timestamp=none
                k\tv
                b:empty\t
                b:escape\ta"b,c\\d'e
                b:max\tü€😀�
                b:min\t\s
                b:null\t
                ## cast
                target\tempty|min|max|escape|null
                BOOLEAN\tfalse|false|false|false|false
                BYTE\t0|0|0|0|0
                SHORT\t0|0|0|0|0
                CHAR\t| |ü|a|
                INT\tnull|null|null|null|null
                LONG\tnull|null|null|null|null
                DATE\t||||
                TIMESTAMP\t||||
                FLOAT\tnull|null|null|null|null
                DOUBLE\tnull|null|null|null|null
                STRING\t| |ü€😀�|a"b,c\\d'e|
                SYMBOL\t| |ü€😀�|a"b,c\\d'e|
                LONG256\t||||
                GEOBYTE\t||||
                GEOSHORT\t||||
                GEOINT\t||||
                GEOLONG\t||||
                BINARY\terror: [20] unsupported cast
                UUID\t||||
                LONG128\terror: [20] unsupported cast
                IPv4\t||||
                VARCHAR\t| |ü€😀�|a"b,c\\d'e|
                DOUBLE[]\tnull|null|null|null|null
                DECIMAL8\terror: inconvertible value: `` [SYMBOL -> DECIMAL(2,1)]
                DECIMAL16\terror: inconvertible value: `` [SYMBOL -> DECIMAL(4,2)]
                DECIMAL32\terror: inconvertible value: `` [SYMBOL -> DECIMAL(9,0)]
                DECIMAL64\terror: inconvertible value: `` [SYMBOL -> DECIMAL(16,4)]
                DECIMAL128\terror: inconvertible value: `` [SYMBOL -> DECIMAL(38,10)]
                DECIMAL256\terror: inconvertible value: `` [SYMBOL -> DECIMAL(76,20)]
                INTERVAL\terror: [20] unsupported cast
                VARCHAR_SLICE\terror: [20] unsupported cast
                TIMESTAMP_NS\t||||
                GEOHASH(1c)\t||||
                GEOHASH(8b)\t||||
                GEOHASH(31b)\t||||
                GEOHASH(12c)\t||||
                DECIMAL(5,2)\terror: inconvertible value: `` [SYMBOL -> DECIMAL(5,2)]
                DECIMAL(18,3)\terror: inconvertible value: `` [SYMBOL -> DECIMAL(18,3)]
                DOUBLE[][]\tnull|null|null|null|null
                INTERVAL(us)\terror: [20] unsupported cast
                INTERVAL(ns)\terror: [20] unsupported cast
                ## fill_null
                props: random_access=false size=unknown timestamp=ts:asc
                ts\tv
                1970-01-01T00:00:00.000000Z\t\s
                1970-01-01T00:00:01.000000Z\t
                1970-01-01T00:00:02.000000Z\tü€😀�
                1970-01-01T00:00:03.000000Z\t
                1970-01-01T00:00:04.000000Z\t
                ## fill_value
                props: random_access=false size=unknown timestamp=ts:asc
                ts\tv
                1970-01-01T00:00:00.000000Z\t\s
                1970-01-01T00:00:01.000000Z\tü€😀�
                1970-01-01T00:00:02.000000Z\tü€😀�
                1970-01-01T00:00:03.000000Z\tü€😀�
                1970-01-01T00:00:04.000000Z\t
                returns: ImplicitCastException: inconvertible value: `ü€😀�` [STRING -> INT]
                ## fill_linear
                error: [46] support for LINEAR fill is not yet implemented [function=last(v), class=io.questdb.griffin.engine.functions.groupby.LastSymbolGroupByFunction]
                ## subsample_stride
                empty\terror: [40] integer expected for stride
                min\terror: [41] integer expected for stride
                max\terror: [45] integer expected for stride
                escape\terror: [50] integer expected for stride
                null\terror: [38] integer expected for stride
                ## subsample_target
                empty\terror: [40] integer expected for target point count
                min\terror: [41] integer expected for target point count
                max\terror: [45] integer expected for target point count
                escape\terror: [50] integer expected for target point count
                null\terror: [38] integer expected for target point count
                ## memoized
                props: random_access=true size=known timestamp=none
                k\ta\ta2\ta3
                empty\t\t\t
                min\t \t \t\s
                max\tü€😀�\tü€😀�\tü€😀�
                escape\ta"b,c\\d'e\ta"b,c\\d'e\ta"b,c\\d'e
                null\t\t\t
                plan memoizes: true
                ## where_bound_const
                empty\terror: [36] Invalid date [str=]
                min\terror: [37] Invalid date [str= ]
                max\terror: [41] Invalid date [str=ü€😀�]
                escape\terror: [46] Invalid date [str=a"b,c\\d'e]
                null\t
                ## where_bound_bind
                empty\terror: inconvertible value: `` [STRING -> LONG]
                min\terror: inconvertible value: ` ` [STRING -> LONG]
                max\terror: inconvertible value: `ü€😀�` [STRING -> LONG]
                escape\terror: inconvertible value: `a"b,c\\d'e` [STRING -> LONG]
                null\t
                ## where_key
                empty\tb:empty
                min\tb:min
                max\tb:max
                escape\t
                null\tb:null
                ## copy_bind
                status\tmessage
                finished\t
                k
                max
                ## between_timestamp
                error: inconvertible value: `` [SYMBOL -> TIMESTAMP_NS]
                ## eq_null_double
                props: random_access=true size=unknown timestamp=none
                k\tv
                null\t
                ## bind_value
                setBoolean\ttrue
                setByte\t1
                setShort\t1
                setChar\t1
                setInt\t1
                setLong\t1
                setFloat\t1.5
                setDouble\t1.5
                setDate\t1
                setTimestamp\t1970-01-01T00:00:00.000001Z
                setStr\t1
                setVarchar\t1
                setLong256\t0x01
                setUuid\t00000000-0000-0000-0000-000000000001
                setArray\terror: [0] bind variable at 0 is defined as STRING and cannot accept ARRAY
                ## window_anchor
                error: create view: [147] ANCHOR EXPRESSION must return TIMESTAMP, LONG, or INT; got SYMBOL
                """);
        rec("LONG256", """
                ## filter_eq
                props: random_access=true size=unknown timestamp=none
                k\tv
                max\t0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff
                ## filter_ne
                props: random_access=true size=unknown timestamp=none
                k\tv
                min\t0x00
                sentinel\t
                null\t
                ## filter_null
                props: random_access=true size=unknown timestamp=none
                k\tv
                sentinel\t
                null\t
                ## filter_not_null
                props: random_access=true size=unknown timestamp=none
                k\tv
                min\t0x00
                max\t0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff
                ## filter_lt
                props: random_access=true size=unknown timestamp=none
                k\tv
                min\t0x00
                ## filter_ge
                props: random_access=true size=unknown timestamp=none
                k\tv
                max\t0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff
                ## order_asc
                props: random_access=true size=known timestamp=none
                k\tv
                null\t
                sentinel\t
                min\t0x00
                max\t0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff
                ## order_desc
                props: random_access=true size=known timestamp=none
                k\tv
                max\t0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff
                min\t0x00
                null\t
                sentinel\t
                ## group_by
                props: random_access=true size=known timestamp=f:asc
                v\tc\tf
                0x00\t2\t1970-01-01T00:00:00.000000Z
                0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff\t2\t1970-01-01T00:00:01.000000Z
                \t4\t1970-01-01T00:00:02.000000Z
                ## join_inner
                props: random_access=true size=unknown timestamp=none
                tk\tuk\tv
                min\tmin\t0x00
                null\tsentinel\t
                sentinel\tsentinel\t
                ## join_left
                props: random_access=true size=unknown timestamp=none
                tk\tuk\tuv
                max\t\t
                min\tmin\t0x00
                null\tsentinel\t
                sentinel\tsentinel\t
                ## join_left_null
                props: random_access=true size=unknown timestamp=none
                tk\tuv
                max\t
                min\t0x00
                null\t
                sentinel\t
                ## union_all
                props: random_access=false size=known timestamp=none
                k\tv
                min\t0x00
                max\t0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff
                sentinel\t
                null\t
                min\t0x00
                sentinel\t
                ## union_null
                props: random_access=false size=known timestamp=none
                k\tv
                min\t0x00
                sentinel\t
                null_branch\t
                ## case_no_else
                props: random_access=true size=known timestamp=none
                k\tc
                min\t
                max\t0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff
                sentinel\t
                null\t
                ## case_else
                error: [20] type LONG256 is not supported in 'switch' type of 'case' statement
                ## lag
                props: random_access=false size=known timestamp=none
                k\tv\tp
                min\t0x00\tnull
                max\t0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff\t0
                sentinel\t\t-1
                null\t\tnull
                ## sample_by
                props: random_access=true size=known timestamp=ts:asc
                ts\tf\tl\tc
                1970-01-01T00:00:00.000000Z\t0\t-1\t2
                1970-01-01T00:00:02.000000Z\tnull\tnull\t2
                ## first_last
                props: random_access=true size=known timestamp=none
                k\tf\tl
                A\tnull\t-1
                B\t-1\tnull
                C\tnull\tnull
                ## first_not_null
                props: random_access=true size=known timestamp=none
                k\tf\tl
                A\t-1\t-1
                B\t-1\t-1
                C\tnull\tnull
                ## fill_prev
                props: random_access=false size=unknown timestamp=ts:asc
                ts\tv
                1970-01-01T00:00:00.000000Z\tnull
                1970-01-01T00:00:01.000000Z\tnull
                1970-01-01T00:00:02.000000Z\t-1
                1970-01-01T00:00:03.000000Z\t-1
                1970-01-01T00:00:04.000000Z\tnull
                ## latest_on
                props: random_access=true size=known timestamp=none
                k\tv
                b:max\t0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff
                b:min\t0x00
                b:null\t
                ## cast
                target\tmin|max|sentinel|null
                BOOLEAN\tfalse|true|false|false
                BYTE\t0|-1|0|0
                SHORT\t0|-1|0|0
                CHAR\t|\\uffff||
                INT\t0|-1|null|null
                LONG\t0|-1|null|null
                DATE\t1970-01-01T00:00:00.000Z|1969-12-31T23:59:59.999Z||
                TIMESTAMP\t1970-01-01T00:00:00.000000Z|1969-12-31T23:59:59.999999Z||
                FLOAT\terror: null
                DOUBLE\terror: null
                STRING\t0x00|0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff||
                SYMBOL\t0|-1||
                LONG256\t0x00|0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff||
                GEOBYTE\t0000000|||
                GEOSHORT\t000|||
                GEOINT\t000000|||
                GEOLONG\t00000000|||
                BINARY\terror: [20] unsupported cast
                UUID\tno cast [10]
                LONG128\terror: [20] unsupported cast
                IPv4\tno cast [10]
                VARCHAR\t0x00|0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff||
                DOUBLE[]\tno cast [10]
                DECIMAL8\t0.0|-1.0||
                DECIMAL16\t0.00|-1.00||
                DECIMAL32\t0|-1||
                DECIMAL64\t0.0000|-1.0000||
                DECIMAL128\t0.0000000000|-1.0000000000||
                DECIMAL256\t0.00000000000000000000|-1.00000000000000000000||
                INTERVAL\terror: [20] unsupported cast
                VARCHAR_SLICE\terror: [20] unsupported cast
                TIMESTAMP_NS\t1970-01-01T00:00:00.000000000Z|1969-12-31T23:59:59.999999999Z||
                GEOHASH(1c)\t0|||
                GEOHASH(8b)\t00000000|||
                GEOHASH(31b)\t0000000000000000000000000000000|||
                GEOHASH(12c)\t000000000000|||
                DECIMAL(5,2)\t0.00|-1.00||
                DECIMAL(18,3)\t0.000|-1.000||
                DOUBLE[][]\tno cast [10]
                INTERVAL(us)\terror: [20] unsupported cast
                INTERVAL(ns)\terror: [20] unsupported cast
                ## fill_null
                props: random_access=false size=unknown timestamp=ts:asc
                ts\tv
                1970-01-01T00:00:00.000000Z\t0
                1970-01-01T00:00:01.000000Z\tnull
                1970-01-01T00:00:02.000000Z\t-1
                1970-01-01T00:00:03.000000Z\tnull
                1970-01-01T00:00:04.000000Z\tnull
                ## fill_value
                error: inconvertible value: `0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff` [STRING -> LONG]
                ## fill_linear
                props: random_access=true size=known timestamp=ts:asc
                ts\tv
                1970-01-01T00:00:00.000000Z\t0
                1970-01-01T00:00:01.000000Z\t0
                1970-01-01T00:00:02.000000Z\t-1
                1970-01-01T00:00:03.000000Z\tnull
                1970-01-01T00:00:04.000000Z\tnull
                ## subsample_stride
                min\terror: [104] integer expected for stride
                max\terror: [104] integer expected for stride
                sentinel\terror: [104] integer expected for stride
                null\terror: [38] integer expected for stride
                ## subsample_target
                min\terror: [104] integer expected for target point count
                max\terror: [104] integer expected for target point count
                sentinel\terror: [104] integer expected for target point count
                null\terror: [38] integer expected for target point count
                ## memoized
                props: random_access=true size=known timestamp=none
                k\ta\ta2\ta3
                min\t0x00\t0x00\t0x00
                max\t0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff\t0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff\t0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff
                sentinel\t\t\t
                null\t\t\t
                plan memoizes: true
                ## where_bound_const
                min\tmin,max,sentinel,null
                max\tmin,max,sentinel,null
                sentinel\t
                null\t
                ## where_bound_bind
                min\tbind error: inconvertible value: `0x00` [STRING -> LONG256]
                max\tbind error: inconvertible value: `0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff` [STRING -> LONG256]
                sentinel\t
                null\t
                ## where_key
                min\tb:min
                max\tb:max
                sentinel\tb:null
                null\tb:null
                ## copy_bind
                bind error: inconvertible value: `0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff` [STRING -> LONG256]
                ## between_timestamp
                error: [27] there is no matching operator `between` with the argument type: LONG256
                ## eq_null_double
                props: random_access=true size=unknown timestamp=none
                k\tv
                sentinel\t
                null\t
                ## bind_value
                setBoolean\terror: [0] bind variable at 0 is defined as LONG256 and cannot accept BOOLEAN
                setByte\terror: [0] bind variable at 0 is defined as LONG256 and cannot accept BYTE
                setShort\terror: [0] bind variable at 0 is defined as LONG256 and cannot accept SHORT
                setChar\terror: [0] bind variable at 0 is defined as LONG256 and cannot accept CHAR
                setInt\terror: [0] bind variable at 0 is defined as LONG256 and cannot accept INT
                setLong\terror: [0] bind variable at 0 is defined as LONG256 and cannot accept LONG
                setFloat\terror: [0] bind variable at 0 is defined as LONG256 and cannot accept FLOAT
                setDouble\terror: [0] bind variable at 0 is defined as LONG256 and cannot accept DOUBLE
                setDate\terror: [0] bind variable at 0 is defined as LONG256 and cannot accept DATE
                setTimestamp\terror: [0] bind variable at 0 is defined as LONG256 and cannot accept TIMESTAMP
                setStr\t0x01
                setVarchar\t0x01
                setLong256\t0x01
                setUuid\terror: [0] bind variable at 0 is defined as LONG256 and cannot accept UUID
                setArray\terror: [0] bind variable at 0 is defined as LONG256 and cannot accept ARRAY
                ## window_anchor
                error: create view: [147] ANCHOR EXPRESSION must return TIMESTAMP, LONG, or INT; got LONG256
                """);
        rec("GEOBYTE", """
                ## filter_eq
                props: random_access=true size=unknown timestamp=none
                k\tv
                max\t1111111
                ## filter_ne
                props: random_access=true size=unknown timestamp=none
                k\tv
                min\t0000000
                null\t
                ## filter_null
                props: random_access=true size=unknown timestamp=none
                k\tv
                null\t
                ## filter_not_null
                props: random_access=true size=unknown timestamp=none
                k\tv
                min\t0000000
                max\t1111111
                ## filter_lt
                error: [27] there is no matching operator `<` with the argument types: GEOHASH(7b) < GEOHASH(7b)
                ## filter_ge
                error: [27] there is no matching operator `>=` with the argument types: GEOHASH(7b) >= GEOHASH(7b)
                ## order_asc
                props: random_access=true size=known timestamp=none
                k\tv
                null\t
                min\t0000000
                max\t1111111
                ## order_desc
                props: random_access=true size=known timestamp=none
                k\tv
                max\t1111111
                min\t0000000
                null\t
                ## group_by
                props: random_access=true size=known timestamp=f:asc
                v\tc\tf
                0000000\t2\t1970-01-01T00:00:00.000000Z
                1111111\t2\t1970-01-01T00:00:01.000000Z
                \t2\t1970-01-01T00:00:02.000000Z
                ## join_inner
                props: random_access=true size=unknown timestamp=none
                tk\tuk\tv
                min\tmin\t0000000
                null\tnull\t
                ## join_left
                props: random_access=true size=unknown timestamp=none
                tk\tuk\tuv
                max\t\t
                min\tmin\t0000000
                null\tnull\t
                ## join_left_null
                props: random_access=true size=unknown timestamp=none
                tk\tuv
                max\t
                min\t0000000
                null\t
                ## union_all
                props: random_access=false size=known timestamp=none
                k\tv
                min\t0000000
                max\t1111111
                null\t
                min\t0000000
                null\t
                ## union_null
                props: random_access=false size=known timestamp=none
                k\tv
                min\t0000000
                null\tnull
                null_branch\t
                ## case_no_else
                props: random_access=true size=known timestamp=none
                k\tc
                min\t
                max\t1111111
                null\t
                ## case_else
                error: [35] inconvertible types: GEOHASH(7b) -> GEOHASH(7b) [from=GEOHASH(7b), to=GEOHASH(7b)]
                ## lag
                error: [13] there is no matching function `lag` with the argument types: (GEOHASH(7b))
                ## sample_by
                props: random_access=true size=known timestamp=ts:asc
                ts\tf\tl\tc
                1970-01-01T00:00:00.000000Z\t0000000\t1111111\t2
                1970-01-01T00:00:02.000000Z\t\t\t1
                ## first_last
                props: random_access=true size=known timestamp=none
                k\tf\tl
                A\t\t1111111
                B\t1111111\t
                C\t\t
                ## first_not_null
                props: random_access=true size=known timestamp=none
                k\tf\tl
                A\t1111111\t1111111
                B\t1111111\t1111111
                C\t\t
                ## fill_prev
                props: random_access=false size=unknown timestamp=ts:asc
                ts\tv
                1970-01-01T00:00:00.000000Z\t
                1970-01-01T00:00:01.000000Z\t
                1970-01-01T00:00:02.000000Z\t1111111
                1970-01-01T00:00:03.000000Z\t1111111
                1970-01-01T00:00:04.000000Z\t
                ## latest_on
                props: random_access=true size=known timestamp=none
                k\tv
                b:max\t1111111
                b:min\t0000000
                b:null\t
                ## cast
                target\tmin|max|null
                BOOLEAN\tno cast [10]
                BYTE\tno cast [10]
                SHORT\tno cast [10]
                CHAR\tno cast [10]
                INT\tno cast [10]
                LONG\tno cast [10]
                DATE\tno cast [10]
                TIMESTAMP\tno cast [10]
                FLOAT\tno cast [10]
                DOUBLE\tno cast [10]
                STRING\t0000000|1111111|
                SYMBOL\tno cast [10]
                LONG256\tno cast [10]
                GEOBYTE\t0000000|1111111|
                GEOSHORT\terror: [10] CAST cannot narrow values from GEOHASH(7b) to GEOHASH(15b)
                GEOINT\terror: [10] CAST cannot narrow values from GEOHASH(7b) to GEOHASH(30b)
                GEOLONG\terror: [10] CAST cannot narrow values from GEOHASH(7b) to GEOHASH(40b)
                BINARY\terror: [20] unsupported cast
                UUID\tno cast [10]
                LONG128\terror: [20] unsupported cast
                IPv4\tno cast [10]
                VARCHAR\t0000000|1111111|
                DOUBLE[]\tno cast [10]
                DECIMAL8\tno cast [10]
                DECIMAL16\tno cast [10]
                DECIMAL32\tno cast [10]
                DECIMAL64\tno cast [10]
                DECIMAL128\tno cast [10]
                DECIMAL256\tno cast [10]
                INTERVAL\terror: [20] unsupported cast
                VARCHAR_SLICE\terror: [20] unsupported cast
                TIMESTAMP_NS\tno cast [10]
                GEOHASH(1c)\t0|z|
                GEOHASH(8b)\terror: [10] CAST cannot narrow values from GEOHASH(7b) to GEOHASH(8b)
                GEOHASH(31b)\terror: [10] CAST cannot narrow values from GEOHASH(7b) to GEOHASH(31b)
                GEOHASH(12c)\terror: [10] CAST cannot narrow values from GEOHASH(7b) to GEOHASH(60b)
                DECIMAL(5,2)\tno cast [10]
                DECIMAL(18,3)\tno cast [10]
                DOUBLE[][]\tno cast [10]
                INTERVAL(us)\terror: [20] unsupported cast
                INTERVAL(ns)\terror: [20] unsupported cast
                ## fill_null
                props: random_access=false size=unknown timestamp=ts:asc
                ts\tv
                1970-01-01T00:00:00.000000Z\t0000000
                1970-01-01T00:00:01.000000Z\t
                1970-01-01T00:00:02.000000Z\t1111111
                1970-01-01T00:00:03.000000Z\t
                1970-01-01T00:00:04.000000Z\t
                ## fill_value
                error: [46] fill value of type INT cannot fill column of type GEOHASH(7b)
                ## fill_linear
                error: [11] Unsupported interpolation type: GEOHASH(7b)
                ## subsample_stride
                min\terror: [38] integer expected for stride
                max\terror: [38] integer expected for stride
                null\terror: [38] integer expected for stride
                ## subsample_target
                min\terror: [38] integer expected for target point count
                max\terror: [38] integer expected for target point count
                null\terror: [38] integer expected for target point count
                ## memoized
                props: random_access=true size=known timestamp=none
                k\ta\ta2\ta3
                min\t0000000\t0000000\t0000000
                max\t1111111\t1111111\t1111111
                null\t\t\t
                plan memoizes: false
                ## where_bound_const
                min\terror: [31] there is no matching operator `>=` with the argument types: LONG >= GEOHASH(7b)
                max\terror: [31] there is no matching operator `>=` with the argument types: LONG >= GEOHASH(7b)
                null\terror: [31] there is no matching operator `>=` with the argument types: LONG >= GEOHASH(7b)
                ## where_bound_bind
                min\tbind error: [0] bind variable at 0 is defined as GEOHASH(7b) and cannot accept STRING
                max\tbind error: [0] bind variable at 0 is defined as GEOHASH(7b) and cannot accept STRING
                null\tbind error: [0] bind variable at 0 is defined as GEOHASH(7b) and cannot accept STRING
                ## where_key
                min\tb:min
                max\t
                null\tb:null
                ## copy_bind
                bind error: [0] bind variable at 0 is defined as GEOHASH(7b) and cannot accept STRING
                ## between_timestamp
                error: [27] there is no matching operator `between` with the argument type: GEOHASH(7b)
                ## eq_null_double
                props: random_access=true size=unknown timestamp=none
                k\tv
                null\t
                ## bind_value
                setBoolean\terror: [0] bind variable at 0 is defined as GEOHASH(7b) and cannot accept BOOLEAN
                setByte\terror: [0] bind variable at 0 is defined as GEOHASH(7b) and cannot accept BYTE
                setShort\terror: [0] bind variable at 0 is defined as GEOHASH(7b) and cannot accept SHORT
                setChar\terror: inconvertible value: 1 [CHAR -> GEOHASH(7b)]
                setInt\terror: [0] bind variable at 0 is defined as GEOHASH(7b) and cannot accept INT
                setLong\terror: [0] bind variable at 0 is defined as GEOHASH(7b) and cannot accept LONG
                setFloat\terror: [0] bind variable at 0 is defined as GEOHASH(7b) and cannot accept FLOAT
                setDouble\terror: [0] bind variable at 0 is defined as GEOHASH(7b) and cannot accept DOUBLE
                setDate\terror: [0] bind variable at 0 is defined as GEOHASH(7b) and cannot accept DATE
                setTimestamp\terror: [0] bind variable at 0 is defined as GEOHASH(7b) and cannot accept TIMESTAMP
                setStr\terror: [0] bind variable at 0 is defined as GEOHASH(7b) and cannot accept STRING
                setVarchar\terror: [0] bind variable at 0 is defined as GEOHASH(7b) and cannot accept VARCHAR
                setLong256\terror: [0] bind variable at 0 is defined as GEOHASH(7b) and cannot accept LONG256
                setUuid\terror: [0] bind variable at 0 is defined as GEOHASH(7b) and cannot accept UUID
                setArray\terror: [0] bind variable at 0 is defined as GEOHASH(7b) and cannot accept ARRAY
                ## window_anchor
                error: create view: [147] ANCHOR EXPRESSION must return TIMESTAMP, LONG, or INT; got GEOHASH(7b)
                """);
        rec("GEOSHORT", """
                ## cast
                target\tmin|max|null
                BOOLEAN\tno cast [10]
                BYTE\tno cast [10]
                SHORT\tno cast [10]
                CHAR\tno cast [10]
                INT\tno cast [10]
                LONG\tno cast [10]
                DATE\tno cast [10]
                TIMESTAMP\tno cast [10]
                FLOAT\tno cast [10]
                DOUBLE\tno cast [10]
                STRING\t000|zzz|
                SYMBOL\tno cast [10]
                LONG256\tno cast [10]
                GEOBYTE\t0000000|1111111|
                GEOSHORT\t000|zzz|
                GEOINT\terror: [10] CAST cannot narrow values from GEOHASH(15b) to GEOHASH(30b)
                GEOLONG\terror: [10] CAST cannot narrow values from GEOHASH(15b) to GEOHASH(40b)
                BINARY\terror: [20] unsupported cast
                UUID\tno cast [10]
                LONG128\terror: [20] unsupported cast
                IPv4\tno cast [10]
                VARCHAR\t000|zzz|
                DOUBLE[]\tno cast [10]
                DECIMAL8\tno cast [10]
                DECIMAL16\tno cast [10]
                DECIMAL32\tno cast [10]
                DECIMAL64\tno cast [10]
                DECIMAL128\tno cast [10]
                DECIMAL256\tno cast [10]
                INTERVAL\terror: [20] unsupported cast
                VARCHAR_SLICE\terror: [20] unsupported cast
                TIMESTAMP_NS\tno cast [10]
                GEOHASH(1c)\t0|z|
                GEOHASH(8b)\t00000000|11111111|
                GEOHASH(31b)\terror: [10] CAST cannot narrow values from GEOHASH(15b) to GEOHASH(31b)
                GEOHASH(12c)\terror: [10] CAST cannot narrow values from GEOHASH(15b) to GEOHASH(60b)
                DECIMAL(5,2)\tno cast [10]
                DECIMAL(18,3)\tno cast [10]
                DOUBLE[][]\tno cast [10]
                INTERVAL(us)\terror: [20] unsupported cast
                INTERVAL(ns)\terror: [20] unsupported cast
                ## filter_eq
                props: random_access=true size=unknown timestamp=none
                k\tv
                max\tzzz
                ## filter_ne
                props: random_access=true size=unknown timestamp=none
                k\tv
                min\t000
                null\t
                ## filter_null
                props: random_access=true size=unknown timestamp=none
                k\tv
                null\t
                ## filter_not_null
                props: random_access=true size=unknown timestamp=none
                k\tv
                min\t000
                max\tzzz
                ## filter_lt
                error: [27] there is no matching operator `<` with the argument types: GEOHASH(3c) < GEOHASH(3c)
                ## filter_ge
                error: [27] there is no matching operator `>=` with the argument types: GEOHASH(3c) >= GEOHASH(3c)
                ## order_asc
                props: random_access=true size=known timestamp=none
                k\tv
                null\t
                min\t000
                max\tzzz
                ## order_desc
                props: random_access=true size=known timestamp=none
                k\tv
                max\tzzz
                min\t000
                null\t
                ## group_by
                props: random_access=true size=known timestamp=f:asc
                v\tc\tf
                000\t2\t1970-01-01T00:00:00.000000Z
                zzz\t2\t1970-01-01T00:00:01.000000Z
                \t2\t1970-01-01T00:00:02.000000Z
                ## join_inner
                props: random_access=true size=unknown timestamp=none
                tk\tuk\tv
                min\tmin\t000
                null\tnull\t
                ## join_left
                props: random_access=true size=unknown timestamp=none
                tk\tuk\tuv
                max\t\t
                min\tmin\t000
                null\tnull\t
                ## join_left_null
                props: random_access=true size=unknown timestamp=none
                tk\tuv
                max\t
                min\t000
                null\t
                ## union_all
                props: random_access=false size=known timestamp=none
                k\tv
                min\t000
                max\tzzz
                null\t
                min\t000
                null\t
                ## union_null
                props: random_access=false size=known timestamp=none
                k\tv
                min\t000
                null\tnull
                null_branch\t
                ## case_no_else
                props: random_access=true size=known timestamp=none
                k\tc
                min\t
                max\tzzz
                null\t
                ## case_else
                error: [35] inconvertible types: GEOHASH(3c) -> GEOHASH(3c) [from=GEOHASH(3c), to=GEOHASH(3c)]
                ## lag
                error: [13] there is no matching function `lag` with the argument types: (GEOHASH(3c))
                ## sample_by
                props: random_access=true size=known timestamp=ts:asc
                ts\tf\tl\tc
                1970-01-01T00:00:00.000000Z\t000\tzzz\t2
                1970-01-01T00:00:02.000000Z\t\t\t1
                ## first_last
                props: random_access=true size=known timestamp=none
                k\tf\tl
                A\t\tzzz
                B\tzzz\t
                C\t\t
                ## first_not_null
                props: random_access=true size=known timestamp=none
                k\tf\tl
                A\tzzz\tzzz
                B\tzzz\tzzz
                C\t\t
                ## fill_prev
                props: random_access=false size=unknown timestamp=ts:asc
                ts\tv
                1970-01-01T00:00:00.000000Z\t
                1970-01-01T00:00:01.000000Z\t
                1970-01-01T00:00:02.000000Z\tzzz
                1970-01-01T00:00:03.000000Z\tzzz
                1970-01-01T00:00:04.000000Z\t
                ## latest_on
                props: random_access=true size=known timestamp=none
                k\tv
                b:max\tzzz
                b:min\t000
                b:null\t
                ## fill_null
                props: random_access=false size=unknown timestamp=ts:asc
                ts\tv
                1970-01-01T00:00:00.000000Z\t000
                1970-01-01T00:00:01.000000Z\t
                1970-01-01T00:00:02.000000Z\tzzz
                1970-01-01T00:00:03.000000Z\t
                1970-01-01T00:00:04.000000Z\t
                ## fill_value
                props: random_access=false size=unknown timestamp=ts:asc
                ts\tv
                1970-01-01T00:00:00.000000Z\t000
                1970-01-01T00:00:01.000000Z\tzzz
                1970-01-01T00:00:02.000000Z\tzzz
                1970-01-01T00:00:03.000000Z\tzzz
                1970-01-01T00:00:04.000000Z\t
                ## fill_linear
                error: [46] support for LINEAR fill is not yet implemented [function=last(v), class=io.questdb.griffin.engine.functions.groupby.LastGeoHashGroupByFunctionFactory$2]
                ## subsample_stride
                min\terror: [38] integer expected for stride
                max\terror: [38] integer expected for stride
                null\terror: [38] integer expected for stride
                ## subsample_target
                min\terror: [38] integer expected for target point count
                max\terror: [38] integer expected for target point count
                null\terror: [38] integer expected for target point count
                ## memoized
                props: random_access=true size=known timestamp=none
                k\ta\ta2\ta3
                min\t000\t000\t000
                max\tzzz\tzzz\tzzz
                null\t\t\t
                plan memoizes: false
                ## where_bound_const
                min\terror: [31] there is no matching operator `>=` with the argument types: LONG >= GEOHASH(3c)
                max\terror: [31] there is no matching operator `>=` with the argument types: LONG >= GEOHASH(3c)
                null\terror: [31] there is no matching operator `>=` with the argument types: LONG >= GEOHASH(3c)
                ## where_bound_bind
                min\tbind error: [0] bind variable at 0 is defined as GEOHASH(3c) and cannot accept STRING
                max\tbind error: [0] bind variable at 0 is defined as GEOHASH(3c) and cannot accept STRING
                null\tbind error: [0] bind variable at 0 is defined as GEOHASH(3c) and cannot accept STRING
                ## where_key
                min\tb:min
                max\tb:max
                null\tb:null
                ## copy_bind
                bind error: [0] bind variable at 0 is defined as GEOHASH(3c) and cannot accept STRING
                ## between_timestamp
                error: [27] there is no matching operator `between` with the argument type: GEOHASH(3c)
                ## eq_null_double
                props: random_access=true size=unknown timestamp=none
                k\tv
                null\t
                ## bind_value
                setBoolean\terror: [0] bind variable at 0 is defined as GEOHASH(3c) and cannot accept BOOLEAN
                setByte\terror: [0] bind variable at 0 is defined as GEOHASH(3c) and cannot accept BYTE
                setShort\terror: [0] bind variable at 0 is defined as GEOHASH(3c) and cannot accept SHORT
                setChar\terror: [0] bind variable at 0 is defined as GEOHASH(3c) and cannot accept CHAR
                setInt\terror: [0] bind variable at 0 is defined as GEOHASH(3c) and cannot accept INT
                setLong\terror: [0] bind variable at 0 is defined as GEOHASH(3c) and cannot accept LONG
                setFloat\terror: [0] bind variable at 0 is defined as GEOHASH(3c) and cannot accept FLOAT
                setDouble\terror: [0] bind variable at 0 is defined as GEOHASH(3c) and cannot accept DOUBLE
                setDate\terror: [0] bind variable at 0 is defined as GEOHASH(3c) and cannot accept DATE
                setTimestamp\terror: [0] bind variable at 0 is defined as GEOHASH(3c) and cannot accept TIMESTAMP
                setStr\terror: [0] bind variable at 0 is defined as GEOHASH(3c) and cannot accept STRING
                setVarchar\terror: [0] bind variable at 0 is defined as GEOHASH(3c) and cannot accept VARCHAR
                setLong256\terror: [0] bind variable at 0 is defined as GEOHASH(3c) and cannot accept LONG256
                setUuid\terror: [0] bind variable at 0 is defined as GEOHASH(3c) and cannot accept UUID
                setArray\terror: [0] bind variable at 0 is defined as GEOHASH(3c) and cannot accept ARRAY
                ## window_anchor
                error: create view: [147] ANCHOR EXPRESSION must return TIMESTAMP, LONG, or INT; got GEOHASH(3c)
                """);
        rec("GEOINT", """
                ## filter_eq
                props: random_access=true size=unknown timestamp=none
                k\tv
                max\tzzzzzz
                ## filter_ne
                props: random_access=true size=unknown timestamp=none
                k\tv
                min\t000000
                null\t
                ## filter_null
                props: random_access=true size=unknown timestamp=none
                k\tv
                null\t
                ## filter_not_null
                props: random_access=true size=unknown timestamp=none
                k\tv
                min\t000000
                max\tzzzzzz
                ## filter_lt
                error: [27] there is no matching operator `<` with the argument types: GEOHASH(6c) < GEOHASH(6c)
                ## filter_ge
                error: [27] there is no matching operator `>=` with the argument types: GEOHASH(6c) >= GEOHASH(6c)
                ## order_asc
                props: random_access=true size=known timestamp=none
                k\tv
                null\t
                min\t000000
                max\tzzzzzz
                ## order_desc
                props: random_access=true size=known timestamp=none
                k\tv
                max\tzzzzzz
                min\t000000
                null\t
                ## group_by
                props: random_access=true size=known timestamp=f:asc
                v\tc\tf
                000000\t2\t1970-01-01T00:00:00.000000Z
                zzzzzz\t2\t1970-01-01T00:00:01.000000Z
                \t2\t1970-01-01T00:00:02.000000Z
                ## join_inner
                props: random_access=true size=unknown timestamp=none
                tk\tuk\tv
                min\tmin\t000000
                null\tnull\t
                ## join_left
                props: random_access=true size=unknown timestamp=none
                tk\tuk\tuv
                max\t\t
                min\tmin\t000000
                null\tnull\t
                ## join_left_null
                props: random_access=true size=unknown timestamp=none
                tk\tuv
                max\t
                min\t000000
                null\t
                ## union_all
                props: random_access=false size=known timestamp=none
                k\tv
                min\t000000
                max\tzzzzzz
                null\t
                min\t000000
                null\t
                ## union_null
                props: random_access=false size=known timestamp=none
                k\tv
                min\t000000
                null\tnull
                null_branch\t
                ## case_no_else
                props: random_access=true size=known timestamp=none
                k\tc
                min\t
                max\tzzzzzz
                null\t
                ## case_else
                error: [35] inconvertible types: GEOHASH(6c) -> GEOHASH(6c) [from=GEOHASH(6c), to=GEOHASH(6c)]
                ## lag
                error: [13] there is no matching function `lag` with the argument types: (GEOHASH(6c))
                ## sample_by
                props: random_access=true size=known timestamp=ts:asc
                ts\tf\tl\tc
                1970-01-01T00:00:00.000000Z\t000000\tzzzzzz\t2
                1970-01-01T00:00:02.000000Z\t\t\t1
                ## first_last
                props: random_access=true size=known timestamp=none
                k\tf\tl
                A\t\tzzzzzz
                B\tzzzzzz\t
                C\t\t
                ## first_not_null
                props: random_access=true size=known timestamp=none
                k\tf\tl
                A\tzzzzzz\tzzzzzz
                B\tzzzzzz\tzzzzzz
                C\t\t
                ## fill_prev
                props: random_access=false size=unknown timestamp=ts:asc
                ts\tv
                1970-01-01T00:00:00.000000Z\t
                1970-01-01T00:00:01.000000Z\t
                1970-01-01T00:00:02.000000Z\tzzzzzz
                1970-01-01T00:00:03.000000Z\tzzzzzz
                1970-01-01T00:00:04.000000Z\t
                ## latest_on
                props: random_access=true size=known timestamp=none
                k\tv
                b:max\tzzzzzz
                b:min\t000000
                b:null\t
                ## cast
                target\tmin|max|null
                BOOLEAN\tno cast [10]
                BYTE\tno cast [10]
                SHORT\tno cast [10]
                CHAR\tno cast [10]
                INT\tno cast [10]
                LONG\tno cast [10]
                DATE\tno cast [10]
                TIMESTAMP\tno cast [10]
                FLOAT\tno cast [10]
                DOUBLE\tno cast [10]
                STRING\t000000|zzzzzz|
                SYMBOL\tno cast [10]
                LONG256\tno cast [10]
                GEOBYTE\t0000000|1111111|
                GEOSHORT\t000|zzz|
                GEOINT\t000000|zzzzzz|
                GEOLONG\terror: [10] CAST cannot narrow values from GEOHASH(30b) to GEOHASH(40b)
                BINARY\terror: [20] unsupported cast
                UUID\tno cast [10]
                LONG128\terror: [20] unsupported cast
                IPv4\tno cast [10]
                VARCHAR\t000000|zzzzzz|
                DOUBLE[]\tno cast [10]
                DECIMAL8\tno cast [10]
                DECIMAL16\tno cast [10]
                DECIMAL32\tno cast [10]
                DECIMAL64\tno cast [10]
                DECIMAL128\tno cast [10]
                DECIMAL256\tno cast [10]
                INTERVAL\terror: [20] unsupported cast
                VARCHAR_SLICE\terror: [20] unsupported cast
                TIMESTAMP_NS\tno cast [10]
                GEOHASH(1c)\t0|z|
                GEOHASH(8b)\t00000000|11111111|
                GEOHASH(31b)\terror: [10] CAST cannot narrow values from GEOHASH(30b) to GEOHASH(31b)
                GEOHASH(12c)\terror: [10] CAST cannot narrow values from GEOHASH(30b) to GEOHASH(60b)
                DECIMAL(5,2)\tno cast [10]
                DECIMAL(18,3)\tno cast [10]
                DOUBLE[][]\tno cast [10]
                INTERVAL(us)\terror: [20] unsupported cast
                INTERVAL(ns)\terror: [20] unsupported cast
                ## fill_null
                props: random_access=false size=unknown timestamp=ts:asc
                ts\tv
                1970-01-01T00:00:00.000000Z\t000000
                1970-01-01T00:00:01.000000Z\t
                1970-01-01T00:00:02.000000Z\tzzzzzz
                1970-01-01T00:00:03.000000Z\t
                1970-01-01T00:00:04.000000Z\t
                ## fill_value
                props: random_access=false size=unknown timestamp=ts:asc
                ts\tv
                1970-01-01T00:00:00.000000Z\t000000
                1970-01-01T00:00:01.000000Z\tzzzzzz
                1970-01-01T00:00:02.000000Z\tzzzzzz
                1970-01-01T00:00:03.000000Z\tzzzzzz
                1970-01-01T00:00:04.000000Z\t
                ## fill_linear
                error: [11] Unsupported interpolation type: GEOHASH(6c)
                ## subsample_stride
                min\terror: [38] integer expected for stride
                max\terror: [38] integer expected for stride
                null\terror: [38] integer expected for stride
                ## subsample_target
                min\terror: [38] integer expected for target point count
                max\terror: [38] integer expected for target point count
                null\terror: [38] integer expected for target point count
                ## memoized
                props: random_access=true size=known timestamp=none
                k\ta\ta2\ta3
                min\t000000\t000000\t000000
                max\tzzzzzz\tzzzzzz\tzzzzzz
                null\t\t\t
                plan memoizes: false
                ## where_bound_const
                min\terror: [31] there is no matching operator `>=` with the argument types: LONG >= GEOHASH(6c)
                max\terror: [31] there is no matching operator `>=` with the argument types: LONG >= GEOHASH(6c)
                null\terror: [31] there is no matching operator `>=` with the argument types: LONG >= GEOHASH(6c)
                ## where_bound_bind
                min\tbind error: [0] bind variable at 0 is defined as GEOHASH(6c) and cannot accept STRING
                max\tbind error: [0] bind variable at 0 is defined as GEOHASH(6c) and cannot accept STRING
                null\tbind error: [0] bind variable at 0 is defined as GEOHASH(6c) and cannot accept STRING
                ## where_key
                min\tb:min
                max\tb:max
                null\tb:null
                ## copy_bind
                bind error: [0] bind variable at 0 is defined as GEOHASH(6c) and cannot accept STRING
                ## between_timestamp
                error: [27] there is no matching operator `between` with the argument type: GEOHASH(6c)
                ## eq_null_double
                props: random_access=true size=unknown timestamp=none
                k\tv
                null\t
                ## bind_value
                setBoolean\terror: [0] bind variable at 0 is defined as GEOHASH(6c) and cannot accept BOOLEAN
                setByte\terror: [0] bind variable at 0 is defined as GEOHASH(6c) and cannot accept BYTE
                setShort\terror: [0] bind variable at 0 is defined as GEOHASH(6c) and cannot accept SHORT
                setChar\terror: [0] bind variable at 0 is defined as GEOHASH(6c) and cannot accept CHAR
                setInt\terror: [0] bind variable at 0 is defined as GEOHASH(6c) and cannot accept INT
                setLong\terror: [0] bind variable at 0 is defined as GEOHASH(6c) and cannot accept LONG
                setFloat\terror: [0] bind variable at 0 is defined as GEOHASH(6c) and cannot accept FLOAT
                setDouble\terror: [0] bind variable at 0 is defined as GEOHASH(6c) and cannot accept DOUBLE
                setDate\terror: [0] bind variable at 0 is defined as GEOHASH(6c) and cannot accept DATE
                setTimestamp\terror: [0] bind variable at 0 is defined as GEOHASH(6c) and cannot accept TIMESTAMP
                setStr\terror: [0] bind variable at 0 is defined as GEOHASH(6c) and cannot accept STRING
                setVarchar\terror: [0] bind variable at 0 is defined as GEOHASH(6c) and cannot accept VARCHAR
                setLong256\terror: [0] bind variable at 0 is defined as GEOHASH(6c) and cannot accept LONG256
                setUuid\terror: [0] bind variable at 0 is defined as GEOHASH(6c) and cannot accept UUID
                setArray\terror: [0] bind variable at 0 is defined as GEOHASH(6c) and cannot accept ARRAY
                ## window_anchor
                error: create view: [147] ANCHOR EXPRESSION must return TIMESTAMP, LONG, or INT; got GEOHASH(6c)
                """);
        rec("GEOLONG", """
                ## cast
                target\tmin|max|null
                BOOLEAN\tno cast [10]
                BYTE\tno cast [10]
                SHORT\tno cast [10]
                CHAR\tno cast [10]
                INT\tno cast [10]
                LONG\tno cast [10]
                DATE\tno cast [10]
                TIMESTAMP\tno cast [10]
                FLOAT\tno cast [10]
                DOUBLE\tno cast [10]
                STRING\t00000000|zzzzzzzz|
                SYMBOL\tno cast [10]
                LONG256\tno cast [10]
                GEOBYTE\t0000000|1111111|
                GEOSHORT\t000|zzz|
                GEOINT\t000000|zzzzzz|
                GEOLONG\t00000000|zzzzzzzz|
                BINARY\terror: [20] unsupported cast
                UUID\tno cast [10]
                LONG128\terror: [20] unsupported cast
                IPv4\tno cast [10]
                VARCHAR\t00000000|zzzzzzzz|
                DOUBLE[]\tno cast [10]
                DECIMAL8\tno cast [10]
                DECIMAL16\tno cast [10]
                DECIMAL32\tno cast [10]
                DECIMAL64\tno cast [10]
                DECIMAL128\tno cast [10]
                DECIMAL256\tno cast [10]
                INTERVAL\terror: [20] unsupported cast
                VARCHAR_SLICE\terror: [20] unsupported cast
                TIMESTAMP_NS\tno cast [10]
                GEOHASH(1c)\t0|z|
                GEOHASH(8b)\t00000000|11111111|
                GEOHASH(31b)\t0000000000000000000000000000000|1111111111111111111111111111111|
                GEOHASH(12c)\terror: [10] CAST cannot narrow values from GEOHASH(40b) to GEOHASH(60b)
                DECIMAL(5,2)\tno cast [10]
                DECIMAL(18,3)\tno cast [10]
                DOUBLE[][]\tno cast [10]
                INTERVAL(us)\terror: [20] unsupported cast
                INTERVAL(ns)\terror: [20] unsupported cast
                ## filter_eq
                props: random_access=true size=unknown timestamp=none
                k\tv
                max\tzzzzzzzz
                ## filter_ne
                props: random_access=true size=unknown timestamp=none
                k\tv
                min\t00000000
                null\t
                ## filter_null
                props: random_access=true size=unknown timestamp=none
                k\tv
                null\t
                ## filter_not_null
                props: random_access=true size=unknown timestamp=none
                k\tv
                min\t00000000
                max\tzzzzzzzz
                ## filter_lt
                error: [27] there is no matching operator `<` with the argument types: GEOHASH(8c) < GEOHASH(8c)
                ## filter_ge
                error: [27] there is no matching operator `>=` with the argument types: GEOHASH(8c) >= GEOHASH(8c)
                ## order_asc
                props: random_access=true size=known timestamp=none
                k\tv
                null\t
                min\t00000000
                max\tzzzzzzzz
                ## order_desc
                props: random_access=true size=known timestamp=none
                k\tv
                max\tzzzzzzzz
                min\t00000000
                null\t
                ## group_by
                props: random_access=true size=known timestamp=f:asc
                v\tc\tf
                00000000\t2\t1970-01-01T00:00:00.000000Z
                zzzzzzzz\t2\t1970-01-01T00:00:01.000000Z
                \t2\t1970-01-01T00:00:02.000000Z
                ## join_inner
                props: random_access=true size=unknown timestamp=none
                tk\tuk\tv
                min\tmin\t00000000
                null\tnull\t
                ## join_left
                props: random_access=true size=unknown timestamp=none
                tk\tuk\tuv
                max\t\t
                min\tmin\t00000000
                null\tnull\t
                ## join_left_null
                props: random_access=true size=unknown timestamp=none
                tk\tuv
                max\t
                min\t00000000
                null\t
                ## union_all
                props: random_access=false size=known timestamp=none
                k\tv
                min\t00000000
                max\tzzzzzzzz
                null\t
                min\t00000000
                null\t
                ## union_null
                props: random_access=false size=known timestamp=none
                k\tv
                min\t00000000
                null\tnull
                null_branch\t
                ## case_no_else
                props: random_access=true size=known timestamp=none
                k\tc
                min\t
                max\tzzzzzzzz
                null\t
                ## case_else
                error: [35] inconvertible types: GEOHASH(8c) -> GEOHASH(8c) [from=GEOHASH(8c), to=GEOHASH(8c)]
                ## lag
                error: [13] there is no matching function `lag` with the argument types: (GEOHASH(8c))
                ## sample_by
                props: random_access=true size=known timestamp=ts:asc
                ts\tf\tl\tc
                1970-01-01T00:00:00.000000Z\t00000000\tzzzzzzzz\t2
                1970-01-01T00:00:02.000000Z\t\t\t1
                ## first_last
                props: random_access=true size=known timestamp=none
                k\tf\tl
                A\t\tzzzzzzzz
                B\tzzzzzzzz\t
                C\t\t
                ## first_not_null
                props: random_access=true size=known timestamp=none
                k\tf\tl
                A\tzzzzzzzz\tzzzzzzzz
                B\tzzzzzzzz\tzzzzzzzz
                C\t\t
                ## fill_prev
                props: random_access=false size=unknown timestamp=ts:asc
                ts\tv
                1970-01-01T00:00:00.000000Z\t
                1970-01-01T00:00:01.000000Z\t
                1970-01-01T00:00:02.000000Z\tzzzzzzzz
                1970-01-01T00:00:03.000000Z\tzzzzzzzz
                1970-01-01T00:00:04.000000Z\t
                ## latest_on
                props: random_access=true size=known timestamp=none
                k\tv
                b:max\tzzzzzzzz
                b:min\t00000000
                b:null\t
                ## fill_null
                props: random_access=false size=unknown timestamp=ts:asc
                ts\tv
                1970-01-01T00:00:00.000000Z\t00000000
                1970-01-01T00:00:01.000000Z\t
                1970-01-01T00:00:02.000000Z\tzzzzzzzz
                1970-01-01T00:00:03.000000Z\t
                1970-01-01T00:00:04.000000Z\t
                ## fill_value
                props: random_access=false size=unknown timestamp=ts:asc
                ts\tv
                1970-01-01T00:00:00.000000Z\t00000000
                1970-01-01T00:00:01.000000Z\tzzzzzzzz
                1970-01-01T00:00:02.000000Z\tzzzzzzzz
                1970-01-01T00:00:03.000000Z\tzzzzzzzz
                1970-01-01T00:00:04.000000Z\t
                ## fill_linear
                error: [11] Unsupported interpolation type: GEOHASH(8c)
                ## subsample_stride
                min\terror: [38] integer expected for stride
                max\terror: [38] integer expected for stride
                null\terror: [38] integer expected for stride
                ## subsample_target
                min\terror: [38] integer expected for target point count
                max\terror: [38] integer expected for target point count
                null\terror: [38] integer expected for target point count
                ## memoized
                props: random_access=true size=known timestamp=none
                k\ta\ta2\ta3
                min\t00000000\t00000000\t00000000
                max\tzzzzzzzz\tzzzzzzzz\tzzzzzzzz
                null\t\t\t
                plan memoizes: false
                ## where_bound_const
                min\terror: [31] there is no matching operator `>=` with the argument types: LONG >= GEOHASH(8c)
                max\terror: [31] there is no matching operator `>=` with the argument types: LONG >= GEOHASH(8c)
                null\terror: [31] there is no matching operator `>=` with the argument types: LONG >= GEOHASH(8c)
                ## where_bound_bind
                min\tbind error: [0] bind variable at 0 is defined as GEOHASH(8c) and cannot accept STRING
                max\tbind error: [0] bind variable at 0 is defined as GEOHASH(8c) and cannot accept STRING
                null\tbind error: [0] bind variable at 0 is defined as GEOHASH(8c) and cannot accept STRING
                ## where_key
                min\tb:min
                max\tb:max
                null\tb:null
                ## copy_bind
                bind error: [0] bind variable at 0 is defined as GEOHASH(8c) and cannot accept STRING
                ## between_timestamp
                error: [27] there is no matching operator `between` with the argument type: GEOHASH(8c)
                ## eq_null_double
                props: random_access=true size=unknown timestamp=none
                k\tv
                null\t
                ## bind_value
                setBoolean\terror: [0] bind variable at 0 is defined as GEOHASH(8c) and cannot accept BOOLEAN
                setByte\terror: [0] bind variable at 0 is defined as GEOHASH(8c) and cannot accept BYTE
                setShort\terror: [0] bind variable at 0 is defined as GEOHASH(8c) and cannot accept SHORT
                setChar\terror: [0] bind variable at 0 is defined as GEOHASH(8c) and cannot accept CHAR
                setInt\terror: [0] bind variable at 0 is defined as GEOHASH(8c) and cannot accept INT
                setLong\terror: [0] bind variable at 0 is defined as GEOHASH(8c) and cannot accept LONG
                setFloat\terror: [0] bind variable at 0 is defined as GEOHASH(8c) and cannot accept FLOAT
                setDouble\terror: [0] bind variable at 0 is defined as GEOHASH(8c) and cannot accept DOUBLE
                setDate\terror: [0] bind variable at 0 is defined as GEOHASH(8c) and cannot accept DATE
                setTimestamp\terror: [0] bind variable at 0 is defined as GEOHASH(8c) and cannot accept TIMESTAMP
                setStr\terror: [0] bind variable at 0 is defined as GEOHASH(8c) and cannot accept STRING
                setVarchar\terror: [0] bind variable at 0 is defined as GEOHASH(8c) and cannot accept VARCHAR
                setLong256\terror: [0] bind variable at 0 is defined as GEOHASH(8c) and cannot accept LONG256
                setUuid\terror: [0] bind variable at 0 is defined as GEOHASH(8c) and cannot accept UUID
                setArray\terror: [0] bind variable at 0 is defined as GEOHASH(8c) and cannot accept ARRAY
                ## window_anchor
                error: create view: [147] ANCHOR EXPRESSION must return TIMESTAMP, LONG, or INT; got GEOHASH(8c)
                """);
        rec("BINARY", """
                ## cast
                target\tempty|max|null
                BOOLEAN\tno cast [10]
                BYTE\tno cast [10]
                SHORT\tno cast [10]
                CHAR\tno cast [10]
                INT\tno cast [10]
                LONG\tno cast [10]
                DATE\tno cast [10]
                TIMESTAMP\tno cast [10]
                FLOAT\tno cast [10]
                DOUBLE\tno cast [10]
                STRING\tno cast [10]
                SYMBOL\tno cast [10]
                LONG256\tno cast [10]
                GEOBYTE\tno cast [10]
                GEOSHORT\tno cast [10]
                GEOINT\tno cast [10]
                GEOLONG\tno cast [10]
                BINARY\terror: [20] unsupported cast
                UUID\tno cast [10]
                LONG128\terror: [20] unsupported cast
                IPv4\tno cast [10]
                VARCHAR\tno cast [10]
                DOUBLE[]\tno cast [10]
                DECIMAL8\tno cast [10]
                DECIMAL16\tno cast [10]
                DECIMAL32\tno cast [10]
                DECIMAL64\tno cast [10]
                DECIMAL128\tno cast [10]
                DECIMAL256\tno cast [10]
                INTERVAL\terror: [20] unsupported cast
                VARCHAR_SLICE\terror: [20] unsupported cast
                TIMESTAMP_NS\tno cast [10]
                GEOHASH(1c)\tno cast [10]
                GEOHASH(8b)\tno cast [10]
                GEOHASH(31b)\tno cast [10]
                GEOHASH(12c)\tno cast [10]
                DECIMAL(5,2)\tno cast [10]
                DECIMAL(18,3)\tno cast [10]
                DOUBLE[][]\tno cast [10]
                INTERVAL(us)\terror: [20] unsupported cast
                INTERVAL(ns)\terror: [20] unsupported cast
                ## filter_eq
                props: random_access=true size=unknown timestamp=none
                k\tv
                max\t00000000 00 01 02 fd fe ff
                ## filter_ne
                props: random_access=true size=unknown timestamp=none
                k\tv
                empty\t
                null\t
                ## filter_null
                props: random_access=true size=unknown timestamp=none
                k\tv
                null\t
                ## filter_not_null
                props: random_access=true size=unknown timestamp=none
                k\tv
                empty\t
                max\t00000000 00 01 02 fd fe ff
                ## filter_lt
                error: [27] there is no matching operator `<` with the argument types: BINARY < BINARY
                ## filter_ge
                error: [27] there is no matching operator `>=` with the argument types: BINARY >= BINARY
                ## order_asc
                error: [28] BINARY is not a supported type in ORDER BY clause
                ## order_desc
                error: [28] BINARY is not a supported type in ORDER BY clause
                ## group_by
                props: random_access=true size=known timestamp=f:asc
                v\tc\tf
                \t2\t1970-01-01T00:00:00.000000Z
                00000000 00 01 02 fd fe ff\t2\t1970-01-01T00:00:01.000000Z
                \t2\t1970-01-01T00:00:02.000000Z
                ## join_inner
                props: random_access=true size=unknown timestamp=none
                tk\tuk\tv
                empty\tempty\t
                null\tnull\t
                ## join_left
                props: random_access=true size=unknown timestamp=none
                tk\tuk\tuv
                empty\tempty\t
                max\t\t
                null\tnull\t
                ## join_left_null
                props: random_access=true size=unknown timestamp=none
                tk\tuv
                empty\t
                max\t
                null\t
                ## union_all
                props: random_access=false size=known timestamp=none
                k\tv
                empty\t
                max\t00000000 00 01 02 fd fe ff
                null\t
                empty\t
                null\t
                ## union_null
                props: random_access=false size=known timestamp=none
                k\tv
                empty\t
                null\t
                null_branch\t
                ## case_no_else
                props: random_access=true size=known timestamp=none
                k\tc
                empty\t
                max\t00000000 00 01 02 fd fe ff
                null\t
                ## case_else
                error: [20] type BINARY is not supported in 'switch' type of 'case' statement
                ## lag
                error: [13] there is no matching function `lag` with the argument types: (BINARY)
                ## sample_by
                error: [11] there is no matching function `first` with the argument types: (BINARY)
                ## first_last
                error: [10] there is no matching function `first` with the argument types: (BINARY)
                ## first_not_null
                error: [10] there is no matching function `first_not_null` with the argument types: (BINARY)
                ## fill_prev
                error: [11] there is no matching function `last` with the argument types: (BINARY)
                ## latest_on
                error: [47] v (BINARY): invalid type, only [BOOLEAN, BYTE, SHORT, INT, LONG, DATE, TIMESTAMP, FLOAT, DOUBLE, LONG128, LONG256, CHAR, STRING, VARCHAR, SYMBOL, UUID, GEOHASH, IPv4] are supported in LATEST ON
                ## fill_null
                error: [11] there is no matching function `last` with the argument types: (BINARY)
                ## fill_value
                error: [11] there is no matching function `last` with the argument types: (BINARY)
                ## fill_linear
                error: [11] there is no matching function `last` with the argument types: (BINARY)
                ## subsample_stride
                empty\terror: [38] integer expected for stride
                max\terror: [38] integer expected for stride
                null\terror: [38] integer expected for stride
                ## subsample_target
                empty\terror: [38] integer expected for target point count
                max\terror: [38] integer expected for target point count
                null\terror: [38] integer expected for target point count
                ## memoized
                error: [20] unsupported cast
                ## where_bound_const
                empty\terror: [31] there is no matching operator `>=` with the argument types: LONG >= BINARY
                max\terror: [31] there is no matching operator `>=` with the argument types: LONG >= BINARY
                null\terror: [31] there is no matching operator `>=` with the argument types: LONG >= BINARY
                ## where_bound_bind
                empty\tbind error: [0] bind variable at 0 is defined as BINARY and cannot accept STRING
                max\tbind error: [0] bind variable at 0 is defined as BINARY and cannot accept STRING
                null\tbind error: [0] bind variable at 0 is defined as BINARY and cannot accept STRING
                ## where_key
                empty\terror: [56] v (BINARY): invalid type, only [BOOLEAN, BYTE, SHORT, INT, LONG, DATE, TIMESTAMP, FLOAT, DOUBLE, LONG128, LONG256, CHAR, STRING, VARCHAR, SYMBOL, UUID, GEOHASH, IPv4] are supported in LATEST ON
                max\terror: [82] v (BINARY): invalid type, only [BOOLEAN, BYTE, SHORT, INT, LONG, DATE, TIMESTAMP, FLOAT, DOUBLE, LONG128, LONG256, CHAR, STRING, VARCHAR, SYMBOL, UUID, GEOHASH, IPv4] are supported in LATEST ON
                null\terror: [58] v (BINARY): invalid type, only [BOOLEAN, BYTE, SHORT, INT, LONG, DATE, TIMESTAMP, FLOAT, DOUBLE, LONG128, LONG256, CHAR, STRING, VARCHAR, SYMBOL, UUID, GEOHASH, IPv4] are supported in LATEST ON
                ## copy_bind
                bind error: [0] bind variable at 0 is defined as BINARY and cannot accept STRING
                ## between_timestamp
                error: [27] there is no matching operator `between` with the argument type: BINARY
                ## eq_null_double
                props: random_access=true size=unknown timestamp=none
                k\tv
                null\t
                ## bind_value
                setBoolean\terror: [0] bind variable at 0 is defined as BINARY and cannot accept BOOLEAN
                setByte\terror: [0] bind variable at 0 is defined as BINARY and cannot accept BYTE
                setShort\terror: [0] bind variable at 0 is defined as BINARY and cannot accept SHORT
                setChar\terror: [0] bind variable at 0 is defined as BINARY and cannot accept CHAR
                setInt\terror: [0] bind variable at 0 is defined as BINARY and cannot accept INT
                setLong\terror: [0] bind variable at 0 is defined as BINARY and cannot accept LONG
                setFloat\terror: [0] bind variable at 0 is defined as BINARY and cannot accept FLOAT
                setDouble\terror: [0] bind variable at 0 is defined as BINARY and cannot accept DOUBLE
                setDate\terror: [0] bind variable at 0 is defined as BINARY and cannot accept DATE
                setTimestamp\terror: [0] bind variable at 0 is defined as BINARY and cannot accept TIMESTAMP
                setStr\terror: [0] bind variable at 0 is defined as BINARY and cannot accept STRING
                setVarchar\terror: [0] bind variable at 0 is defined as BINARY and cannot accept VARCHAR
                setLong256\terror: [0] bind variable at 0 is defined as BINARY and cannot accept LONG256
                setUuid\terror: [0] bind variable at 0 is defined as BINARY and cannot accept UUID
                setArray\terror: [0] bind variable at 0 is defined as BINARY and cannot accept ARRAY
                ## window_anchor
                error: create view: [147] ANCHOR EXPRESSION must return TIMESTAMP, LONG, or INT; got BINARY
                """);
        rec("UUID", """
                ## cast
                target\tmin|max|sentinel|null
                BOOLEAN\tfalse|false|false|false
                BYTE\t0|0|0|0
                SHORT\t0|0|0|0
                CHAR\t0|f||
                INT\tnull|null|null|null
                LONG\tnull|null|null|null
                DATE\t|||
                TIMESTAMP\t|||
                FLOAT\tnull|null|null|null
                DOUBLE\tnull|null|null|null
                STRING\t00000000-0000-0000-0000-000000000000|ffffffff-ffff-ffff-ffff-ffffffffffff||
                SYMBOL\t00000000-0000-0000-0000-000000000000|ffffffff-ffff-ffff-ffff-ffffffffffff||
                LONG256\t|||
                GEOBYTE\t0000000|0111001||
                GEOSHORT\t000|fff||
                GEOINT\t000000|ffffff||
                GEOLONG\t00000000|ffffffff||
                BINARY\terror: [20] unsupported cast
                UUID\t00000000-0000-0000-0000-000000000000|ffffffff-ffff-ffff-ffff-ffffffffffff||
                LONG128\terror: [20] unsupported cast
                IPv4\t|||
                VARCHAR\t00000000-0000-0000-0000-000000000000|ffffffff-ffff-ffff-ffff-ffffffffffff||
                DOUBLE[]\tnull|null|null|null
                DECIMAL8\terror: inconvertible value: `00000000-0000-0000-0000-000000000000` [STRING -> DECIMAL(2,1)]
                DECIMAL16\terror: inconvertible value: `00000000-0000-0000-0000-000000000000` [STRING -> DECIMAL(4,2)]
                DECIMAL32\terror: inconvertible value: `00000000-0000-0000-0000-000000000000` [STRING -> DECIMAL(9,0)]
                DECIMAL64\terror: inconvertible value: `00000000-0000-0000-0000-000000000000` [STRING -> DECIMAL(16,4)]
                DECIMAL128\terror: inconvertible value: `00000000-0000-0000-0000-000000000000` [STRING -> DECIMAL(38,10)]
                DECIMAL256\terror: inconvertible value: `00000000-0000-0000-0000-000000000000` [STRING -> DECIMAL(76,20)]
                INTERVAL\terror: [20] unsupported cast
                VARCHAR_SLICE\terror: [20] unsupported cast
                TIMESTAMP_NS\t|||
                GEOHASH(1c)\t0|f||
                GEOHASH(8b)\t00000000|01110011||
                GEOHASH(31b)\t0000000000000000000000000000000|0111001110011100111001110011100||
                GEOHASH(12c)\t|||
                DECIMAL(5,2)\terror: inconvertible value: `00000000-0000-0000-0000-000000000000` [STRING -> DECIMAL(5,2)]
                DECIMAL(18,3)\terror: inconvertible value: `00000000-0000-0000-0000-000000000000` [STRING -> DECIMAL(18,3)]
                DOUBLE[][]\tnull|null|null|null
                INTERVAL(us)\terror: [20] unsupported cast
                INTERVAL(ns)\terror: [20] unsupported cast
                ## filter_eq
                props: random_access=true size=unknown timestamp=none
                k\tv
                max\tffffffff-ffff-ffff-ffff-ffffffffffff
                ## filter_ne
                props: random_access=true size=unknown timestamp=none
                k\tv
                min\t00000000-0000-0000-0000-000000000000
                sentinel\t
                null\t
                ## filter_null
                props: random_access=true size=unknown timestamp=none
                k\tv
                sentinel\t
                null\t
                ## filter_not_null
                props: random_access=true size=unknown timestamp=none
                k\tv
                min\t00000000-0000-0000-0000-000000000000
                max\tffffffff-ffff-ffff-ffff-ffffffffffff
                ## filter_lt
                props: random_access=true size=unknown timestamp=none
                k\tv
                min\t00000000-0000-0000-0000-000000000000
                ## filter_ge
                props: random_access=true size=unknown timestamp=none
                k\tv
                max\tffffffff-ffff-ffff-ffff-ffffffffffff
                ## order_asc
                props: random_access=true size=known timestamp=none
                k\tv
                null\t
                sentinel\t
                min\t00000000-0000-0000-0000-000000000000
                max\tffffffff-ffff-ffff-ffff-ffffffffffff
                ## order_desc
                props: random_access=true size=known timestamp=none
                k\tv
                max\tffffffff-ffff-ffff-ffff-ffffffffffff
                min\t00000000-0000-0000-0000-000000000000
                null\t
                sentinel\t
                ## group_by
                props: random_access=true size=known timestamp=f:asc
                v\tc\tf
                00000000-0000-0000-0000-000000000000\t2\t1970-01-01T00:00:00.000000Z
                ffffffff-ffff-ffff-ffff-ffffffffffff\t2\t1970-01-01T00:00:01.000000Z
                \t4\t1970-01-01T00:00:02.000000Z
                ## join_inner
                props: random_access=true size=unknown timestamp=none
                tk\tuk\tv
                min\tmin\t00000000-0000-0000-0000-000000000000
                null\tsentinel\t
                sentinel\tsentinel\t
                ## join_left
                props: random_access=true size=unknown timestamp=none
                tk\tuk\tuv
                max\t\t
                min\tmin\t00000000-0000-0000-0000-000000000000
                null\tsentinel\t
                sentinel\tsentinel\t
                ## join_left_null
                props: random_access=true size=unknown timestamp=none
                tk\tuv
                max\t
                min\t00000000-0000-0000-0000-000000000000
                null\t
                sentinel\t
                ## union_all
                props: random_access=false size=known timestamp=none
                k\tv
                min\t00000000-0000-0000-0000-000000000000
                max\tffffffff-ffff-ffff-ffff-ffffffffffff
                sentinel\t
                null\t
                min\t00000000-0000-0000-0000-000000000000
                sentinel\t
                ## union_null
                props: random_access=false size=known timestamp=none
                k\tv
                min\t00000000-0000-0000-0000-000000000000
                sentinel\t
                null_branch\t
                ## case_no_else
                props: random_access=true size=known timestamp=none
                k\tc
                min\t
                max\tffffffff-ffff-ffff-ffff-ffffffffffff
                sentinel\t
                null\t
                ## case_else
                error: [20] type UUID is not supported in 'switch' type of 'case' statement
                ## lag
                error: [13] there is no matching function `lag` with the argument types: (UUID)
                ## sample_by
                props: random_access=true size=known timestamp=ts:asc
                ts\tf\tl\tc
                1970-01-01T00:00:00.000000Z\t00000000-0000-0000-0000-000000000000\tffffffff-ffff-ffff-ffff-ffffffffffff\t2
                1970-01-01T00:00:02.000000Z\t\t\t2
                ## first_last
                props: random_access=true size=known timestamp=none
                k\tf\tl
                A\t\tffffffff-ffff-ffff-ffff-ffffffffffff
                B\tffffffff-ffff-ffff-ffff-ffffffffffff\t
                C\t\t
                ## first_not_null
                props: random_access=true size=known timestamp=none
                k\tf\tl
                A\tffffffff-ffff-ffff-ffff-ffffffffffff\tffffffff-ffff-ffff-ffff-ffffffffffff
                B\tffffffff-ffff-ffff-ffff-ffffffffffff\tffffffff-ffff-ffff-ffff-ffffffffffff
                C\t\t
                ## fill_prev
                props: random_access=false size=unknown timestamp=ts:asc
                ts\tv
                1970-01-01T00:00:00.000000Z\t
                1970-01-01T00:00:01.000000Z\t
                1970-01-01T00:00:02.000000Z\tffffffff-ffff-ffff-ffff-ffffffffffff
                1970-01-01T00:00:03.000000Z\tffffffff-ffff-ffff-ffff-ffffffffffff
                1970-01-01T00:00:04.000000Z\t
                ## latest_on
                props: random_access=true size=known timestamp=none
                k\tv
                b:max\tffffffff-ffff-ffff-ffff-ffffffffffff
                b:min\t00000000-0000-0000-0000-000000000000
                b:null\t
                ## fill_null
                props: random_access=false size=unknown timestamp=ts:asc
                ts\tv
                1970-01-01T00:00:00.000000Z\t00000000-0000-0000-0000-000000000000
                1970-01-01T00:00:01.000000Z\t
                1970-01-01T00:00:02.000000Z\tffffffff-ffff-ffff-ffff-ffffffffffff
                1970-01-01T00:00:03.000000Z\t
                1970-01-01T00:00:04.000000Z\t
                ## fill_value
                props: random_access=false size=unknown timestamp=ts:asc
                ts\tv
                1970-01-01T00:00:00.000000Z\t00000000-0000-0000-0000-000000000000
                1970-01-01T00:00:01.000000Z\tffffffff-ffff-ffff-ffff-ffffffffffff
                1970-01-01T00:00:02.000000Z\tffffffff-ffff-ffff-ffff-ffffffffffff
                1970-01-01T00:00:03.000000Z\tffffffff-ffff-ffff-ffff-ffffffffffff
                1970-01-01T00:00:04.000000Z\t
                ## fill_linear
                error: [46] support for LINEAR fill is not yet implemented [function=last(v), class=io.questdb.griffin.engine.functions.groupby.LastUuidGroupByFunction]
                ## subsample_stride
                min\terror: [76] integer expected for stride
                max\terror: [76] integer expected for stride
                sentinel\terror: [76] integer expected for stride
                null\terror: [38] integer expected for stride
                ## subsample_target
                min\terror: [76] integer expected for target point count
                max\terror: [76] integer expected for target point count
                sentinel\terror: [76] integer expected for target point count
                null\terror: [38] integer expected for target point count
                ## memoized
                props: random_access=true size=known timestamp=none
                k\ta\ta2\ta3
                min\t00000000-0000-0000-0000-000000000000\t00000000-0000-0000-0000-000000000000\t00000000-0000-0000-0000-000000000000
                max\tffffffff-ffff-ffff-ffff-ffffffffffff\tffffffff-ffff-ffff-ffff-ffffffffffff\tffffffff-ffff-ffff-ffff-ffffffffffff
                sentinel\t\t\t
                null\t\t\t
                plan memoizes: true
                ## where_bound_const
                min\terror: [31] there is no matching operator `>=` with the argument types: LONG >= UUID
                max\terror: [31] there is no matching operator `>=` with the argument types: LONG >= UUID
                sentinel\terror: [31] there is no matching operator `>=` with the argument types: LONG >= UUID
                null\terror: [31] there is no matching operator `>=` with the argument types: LONG >= UUID
                ## where_bound_bind
                min\terror: [31] there is no matching operator `>=` with the argument types: LONG >= UUID
                max\terror: [31] there is no matching operator `>=` with the argument types: LONG >= UUID
                sentinel\terror: [31] there is no matching operator `>=` with the argument types: LONG >= UUID
                null\terror: [31] there is no matching operator `>=` with the argument types: LONG >= UUID
                ## where_key
                min\tb:min
                max\tb:max
                sentinel\tb:null
                null\tb:null
                ## copy_bind
                status\tmessage
                finished\t
                k
                max
                ## between_timestamp
                error: [27] there is no matching operator `between` with the argument type: UUID
                ## eq_null_double
                props: random_access=true size=unknown timestamp=none
                k\tv
                sentinel\t
                null\t
                ## bind_value
                setBoolean\terror: [0] bind variable at 0 is defined as UUID and cannot accept BOOLEAN
                setByte\terror: [0] bind variable at 0 is defined as UUID and cannot accept BYTE
                setShort\terror: [0] bind variable at 0 is defined as UUID and cannot accept SHORT
                setChar\terror: [0] bind variable at 0 is defined as UUID and cannot accept CHAR
                setInt\terror: [0] bind variable at 0 is defined as UUID and cannot accept INT
                setLong\terror: [0] bind variable at 0 is defined as UUID and cannot accept LONG
                setFloat\terror: [0] bind variable at 0 is defined as UUID and cannot accept FLOAT
                setDouble\terror: [0] bind variable at 0 is defined as UUID and cannot accept DOUBLE
                setDate\terror: [0] bind variable at 0 is defined as UUID and cannot accept DATE
                setTimestamp\terror: [0] bind variable at 0 is defined as UUID and cannot accept TIMESTAMP
                setStr\terror: inconvertible value: `1` [STRING -> UUID]
                setVarchar\terror: inconvertible value: `1` [STRING -> UUID]
                setLong256\terror: [0] bind variable at 0 is defined as UUID and cannot accept LONG256
                setUuid\t00000000-0000-0000-0000-000000000001
                setArray\terror: [0] bind variable at 0 is defined as UUID and cannot accept ARRAY
                ## window_anchor
                error: create view: [147] ANCHOR EXPRESSION must return TIMESTAMP, LONG, or INT; got UUID
                """);
        rec("LONG128", """
                ## cast
                target\tmin|max|sentinel|null
                BOOLEAN\tno cast [10]
                BYTE\tno cast [10]
                SHORT\tno cast [10]
                CHAR\tno cast [10]
                INT\tno cast [10]
                LONG\tno cast [10]
                DATE\tno cast [10]
                TIMESTAMP\tno cast [10]
                FLOAT\tno cast [10]
                DOUBLE\tno cast [10]
                STRING\tno cast [10]
                SYMBOL\tno cast [10]
                LONG256\tno cast [10]
                GEOBYTE\tno cast [10]
                GEOSHORT\tno cast [10]
                GEOINT\tno cast [10]
                GEOLONG\tno cast [10]
                BINARY\terror: [20] unsupported cast
                UUID\tno cast [10]
                LONG128\terror: [20] unsupported cast
                IPv4\tno cast [10]
                VARCHAR\tno cast [10]
                DOUBLE[]\tno cast [10]
                DECIMAL8\tno cast [10]
                DECIMAL16\tno cast [10]
                DECIMAL32\tno cast [10]
                DECIMAL64\tno cast [10]
                DECIMAL128\tno cast [10]
                DECIMAL256\tno cast [10]
                INTERVAL\terror: [20] unsupported cast
                VARCHAR_SLICE\terror: [20] unsupported cast
                TIMESTAMP_NS\tno cast [10]
                GEOHASH(1c)\tno cast [10]
                GEOHASH(8b)\tno cast [10]
                GEOHASH(31b)\tno cast [10]
                GEOHASH(12c)\tno cast [10]
                DECIMAL(5,2)\tno cast [10]
                DECIMAL(18,3)\tno cast [10]
                DOUBLE[][]\tno cast [10]
                INTERVAL(us)\terror: [20] unsupported cast
                INTERVAL(ns)\terror: [20] unsupported cast
                ## filter_eq
                props: random_access=true size=unknown timestamp=none
                k\tv
                max\tffffffff-ffff-ffff-ffff-ffffffffffff
                ## filter_ne
                props: random_access=true size=unknown timestamp=none
                k\tv
                min\t00000000-0000-0000-0000-000000000000
                sentinel\t
                null\t
                ## filter_null
                props: random_access=true size=unknown timestamp=none
                k\tv
                sentinel\t
                null\t
                ## filter_not_null
                props: random_access=true size=unknown timestamp=none
                k\tv
                min\t00000000-0000-0000-0000-000000000000
                max\tffffffff-ffff-ffff-ffff-ffffffffffff
                ## filter_lt
                error: [27] there is no matching operator `<` with the argument types: LONG128 < LONG128
                ## filter_ge
                error: [27] there is no matching operator `>=` with the argument types: LONG128 >= LONG128
                ## order_asc
                props: random_access=true size=known timestamp=none
                k\tv
                null\t
                sentinel\t
                max\tffffffff-ffff-ffff-ffff-ffffffffffff
                min\t00000000-0000-0000-0000-000000000000
                ## order_desc
                props: random_access=true size=known timestamp=none
                k\tv
                min\t00000000-0000-0000-0000-000000000000
                max\tffffffff-ffff-ffff-ffff-ffffffffffff
                null\t
                sentinel\t
                ## group_by
                props: random_access=true size=known timestamp=f:asc
                v\tc\tf
                00000000-0000-0000-0000-000000000000\t2\t1970-01-01T00:00:00.000000Z
                ffffffff-ffff-ffff-ffff-ffffffffffff\t2\t1970-01-01T00:00:01.000000Z
                \t4\t1970-01-01T00:00:02.000000Z
                ## join_inner
                props: random_access=true size=unknown timestamp=none
                tk\tuk\tv
                min\tmin\t00000000-0000-0000-0000-000000000000
                null\tsentinel\t
                sentinel\tsentinel\t
                ## join_left
                props: random_access=true size=unknown timestamp=none
                tk\tuk\tuv
                max\t\t
                min\tmin\t00000000-0000-0000-0000-000000000000
                null\tsentinel\t
                sentinel\tsentinel\t
                ## join_left_null
                props: random_access=true size=unknown timestamp=none
                tk\tuv
                max\t
                min\t00000000-0000-0000-0000-000000000000
                null\t
                sentinel\t
                ## union_all
                props: random_access=false size=known timestamp=none
                k\tv
                min\t00000000-0000-0000-0000-000000000000
                max\tffffffff-ffff-ffff-ffff-ffffffffffff
                sentinel\t
                null\t
                min\t00000000-0000-0000-0000-000000000000
                sentinel\t
                ## union_null
                props: random_access=false size=known timestamp=none
                k\tv
                min\t00000000-0000-0000-0000-000000000000
                sentinel\t
                null_branch\t
                ## case_no_else
                props: random_access=true size=known timestamp=none
                k\tc
                min\t
                max\tffffffff-ffff-ffff-ffff-ffffffffffff
                sentinel\t
                null\t
                ## case_else
                error: [35] inconvertible types: LONG128 -> LONG128 [from=LONG128, to=LONG128]
                ## lag
                error: [13] there is no matching function `lag` with the argument types: (LONG128)
                ## sample_by
                error: [11] there is no matching function `first` with the argument types: (LONG128)
                ## first_last
                error: [10] there is no matching function `first` with the argument types: (LONG128)
                ## first_not_null
                error: [10] there is no matching function `first_not_null` with the argument types: (LONG128)
                ## fill_prev
                error: [11] there is no matching function `last` with the argument types: (LONG128)
                ## latest_on
                props: random_access=true size=known timestamp=none
                k\tv
                b:max\tffffffff-ffff-ffff-ffff-ffffffffffff
                b:min\t00000000-0000-0000-0000-000000000000
                b:null\t
                ## fill_null
                error: [11] there is no matching function `last` with the argument types: (LONG128)
                ## fill_value
                error: [11] there is no matching function `last` with the argument types: (LONG128)
                ## fill_linear
                error: [11] there is no matching function `last` with the argument types: (LONG128)
                ## subsample_stride
                min\terror: [38] integer expected for stride
                max\terror: [38] integer expected for stride
                sentinel\terror: [38] integer expected for stride
                null\terror: [51] unsupported cast
                ## subsample_target
                min\terror: [38] integer expected for target point count
                max\terror: [38] integer expected for target point count
                sentinel\terror: [38] integer expected for target point count
                null\terror: [51] unsupported cast
                ## memoized
                error: [20] unsupported cast
                ## where_bound_const
                min\terror: [31] there is no matching operator `>=` with the argument types: LONG >= LONG128
                max\terror: [31] there is no matching operator `>=` with the argument types: LONG >= LONG128
                sentinel\terror: [31] there is no matching operator `>=` with the argument types: LONG >= LONG128
                null\terror: [47] unsupported cast
                ## where_bound_bind
                min\tbind error: [0] bind variable cannot be used [contextType=24, index=0]
                max\tbind error: [0] bind variable cannot be used [contextType=24, index=0]
                sentinel\tbind error: [0] bind variable cannot be used [contextType=24, index=0]
                null\tbind error: [0] bind variable cannot be used [contextType=24, index=0]
                ## where_key
                min\terror: [25] there is no matching operator `=` with the argument types: LONG128 = STRING
                max\terror: [25] there is no matching operator `=` with the argument types: LONG128 = STRING
                sentinel\tb:null
                null\tb:null
                ## copy_bind
                bind error: [0] bind variable cannot be used [contextType=24, index=0]
                ## between_timestamp
                error: [27] there is no matching operator `between` with the argument type: LONG128
                ## eq_null_double
                props: random_access=true size=unknown timestamp=none
                k\tv
                sentinel\t
                null\t
                ## bind_value
                setBoolean\tdefine error: [0] bind variable cannot be used [contextType=24, index=0]
                setByte\tdefine error: [0] bind variable cannot be used [contextType=24, index=0]
                setShort\tdefine error: [0] bind variable cannot be used [contextType=24, index=0]
                setChar\tdefine error: [0] bind variable cannot be used [contextType=24, index=0]
                setInt\tdefine error: [0] bind variable cannot be used [contextType=24, index=0]
                setLong\tdefine error: [0] bind variable cannot be used [contextType=24, index=0]
                setFloat\tdefine error: [0] bind variable cannot be used [contextType=24, index=0]
                setDouble\tdefine error: [0] bind variable cannot be used [contextType=24, index=0]
                setDate\tdefine error: [0] bind variable cannot be used [contextType=24, index=0]
                setTimestamp\tdefine error: [0] bind variable cannot be used [contextType=24, index=0]
                setStr\tdefine error: [0] bind variable cannot be used [contextType=24, index=0]
                setVarchar\tdefine error: [0] bind variable cannot be used [contextType=24, index=0]
                setLong256\tdefine error: [0] bind variable cannot be used [contextType=24, index=0]
                setUuid\tdefine error: [0] bind variable cannot be used [contextType=24, index=0]
                setArray\tdefine error: [0] bind variable cannot be used [contextType=24, index=0]
                ## window_anchor
                error: create view: [147] ANCHOR EXPRESSION must return TIMESTAMP, LONG, or INT; got LONG128
                """);
        rec("IPv4", """
                ## cast
                target\tmin|max|sentinel|null
                BOOLEAN\tfalse|false|false|false
                BYTE\t0|0|0|0
                SHORT\t0|0|0|0
                CHAR\t0|2||
                INT\t1|-1|null|null
                LONG\tnull|null|null|null
                DATE\t|||
                TIMESTAMP\t|||
                FLOAT\tnull|null|null|null
                DOUBLE\tnull|null|null|null
                STRING\t0.0.0.1|255.255.255.255||
                SYMBOL\t0.0.0.1|255.255.255.255||
                LONG256\t|||
                GEOBYTE\t|0001000||
                GEOSHORT\t|255||
                GEOINT\t|||
                GEOLONG\t|||
                BINARY\terror: [20] unsupported cast
                UUID\t|||
                LONG128\terror: [20] unsupported cast
                IPv4\t0.0.0.1|255.255.255.255||
                VARCHAR\t0.0.0.1|255.255.255.255||
                DOUBLE[]\tnull|null|null|null
                DECIMAL8\terror: inconvertible value: `0.0.0.1` [STRING -> DECIMAL(2,1)]
                DECIMAL16\terror: inconvertible value: `0.0.0.1` [STRING -> DECIMAL(4,2)]
                DECIMAL32\terror: inconvertible value: `0.0.0.1` [STRING -> DECIMAL(9,0)]
                DECIMAL64\terror: inconvertible value: `0.0.0.1` [STRING -> DECIMAL(16,4)]
                DECIMAL128\terror: inconvertible value: `0.0.0.1` [STRING -> DECIMAL(38,10)]
                DECIMAL256\terror: inconvertible value: `0.0.0.1` [STRING -> DECIMAL(76,20)]
                INTERVAL\terror: [20] unsupported cast
                VARCHAR_SLICE\terror: [20] unsupported cast
                TIMESTAMP_NS\t|||
                GEOHASH(1c)\t0|2||
                GEOHASH(8b)\t|00010001||
                GEOHASH(31b)\t|||
                GEOHASH(12c)\t|||
                DECIMAL(5,2)\terror: inconvertible value: `0.0.0.1` [STRING -> DECIMAL(5,2)]
                DECIMAL(18,3)\terror: inconvertible value: `0.0.0.1` [STRING -> DECIMAL(18,3)]
                DOUBLE[][]\tnull|null|null|null
                INTERVAL(us)\terror: [20] unsupported cast
                INTERVAL(ns)\terror: [20] unsupported cast
                ## filter_eq
                props: random_access=true size=unknown timestamp=none
                k\tv
                max\t255.255.255.255
                ## filter_ne
                props: random_access=true size=unknown timestamp=none
                k\tv
                min\t0.0.0.1
                sentinel\t
                null\t
                ## filter_null
                props: random_access=true size=unknown timestamp=none
                k\tv
                sentinel\t
                null\t
                ## filter_not_null
                props: random_access=true size=unknown timestamp=none
                k\tv
                min\t0.0.0.1
                max\t255.255.255.255
                ## filter_lt
                props: random_access=true size=unknown timestamp=none
                k\tv
                min\t0.0.0.1
                ## filter_ge
                props: random_access=true size=unknown timestamp=none
                k\tv
                max\t255.255.255.255
                ## order_asc
                props: random_access=true size=known timestamp=none
                k\tv
                null\t
                sentinel\t
                min\t0.0.0.1
                max\t255.255.255.255
                ## order_desc
                props: random_access=true size=known timestamp=none
                k\tv
                max\t255.255.255.255
                min\t0.0.0.1
                null\t
                sentinel\t
                ## group_by
                props: random_access=true size=known timestamp=f:asc
                v\tc\tf
                0.0.0.1\t2\t1970-01-01T00:00:00.000000Z
                255.255.255.255\t2\t1970-01-01T00:00:01.000000Z
                \t4\t1970-01-01T00:00:02.000000Z
                ## join_inner
                props: random_access=true size=unknown timestamp=none
                tk\tuk\tv
                min\tmin\t0.0.0.1
                null\tsentinel\t
                sentinel\tsentinel\t
                ## join_left
                props: random_access=true size=unknown timestamp=none
                tk\tuk\tuv
                max\t\t
                min\tmin\t0.0.0.1
                null\tsentinel\t
                sentinel\tsentinel\t
                ## join_left_null
                props: random_access=true size=unknown timestamp=none
                tk\tuv
                max\t
                min\t0.0.0.1
                null\t
                sentinel\t
                ## union_all
                props: random_access=false size=known timestamp=none
                k\tv
                min\t0.0.0.1
                max\t255.255.255.255
                sentinel\t
                null\t
                min\t0.0.0.1
                sentinel\t
                ## union_null
                props: random_access=false size=known timestamp=none
                k\tv
                min\t0.0.0.1
                sentinel\t
                null_branch\t
                ## case_no_else
                props: random_access=true size=known timestamp=none
                k\tc
                min\t
                max\t255.255.255.255
                sentinel\t
                null\t
                ## case_else
                props: random_access=true size=known timestamp=none
                k\tc
                min\t0.0.0.1
                max\t255.255.255.255
                sentinel\t255.255.255.255
                null\t255.255.255.255
                ## lag
                error: [13] there is no matching function `lag` with the argument types: (IPv4)
                ## sample_by
                props: random_access=true size=known timestamp=ts:asc
                ts\tf\tl\tc
                1970-01-01T00:00:00.000000Z\t0.0.0.1\t255.255.255.255\t2
                1970-01-01T00:00:02.000000Z\t\t\t2
                ## first_last
                props: random_access=true size=known timestamp=none
                k\tf\tl
                A\t\t255.255.255.255
                B\t255.255.255.255\t
                C\t\t
                ## first_not_null
                props: random_access=true size=known timestamp=none
                k\tf\tl
                A\t255.255.255.255\t255.255.255.255
                B\t255.255.255.255\t255.255.255.255
                C\t\t
                ## fill_prev
                props: random_access=false size=unknown timestamp=ts:asc
                ts\tv
                1970-01-01T00:00:00.000000Z\t
                1970-01-01T00:00:01.000000Z\t
                1970-01-01T00:00:02.000000Z\t255.255.255.255
                1970-01-01T00:00:03.000000Z\t255.255.255.255
                1970-01-01T00:00:04.000000Z\t
                ## latest_on
                props: random_access=true size=known timestamp=none
                k\tv
                b:max\t255.255.255.255
                b:min\t0.0.0.1
                b:null\t
                ## fill_null
                props: random_access=false size=unknown timestamp=ts:asc
                ts\tv
                1970-01-01T00:00:00.000000Z\t0.0.0.1
                1970-01-01T00:00:01.000000Z\t
                1970-01-01T00:00:02.000000Z\t255.255.255.255
                1970-01-01T00:00:03.000000Z\t
                1970-01-01T00:00:04.000000Z\t
                ## fill_value
                props: random_access=false size=unknown timestamp=ts:asc
                ts\tv
                1970-01-01T00:00:00.000000Z\t0.0.0.1
                1970-01-01T00:00:01.000000Z\t255.255.255.255
                1970-01-01T00:00:02.000000Z\t255.255.255.255
                1970-01-01T00:00:03.000000Z\t255.255.255.255
                1970-01-01T00:00:04.000000Z\t
                ## fill_linear
                error: [46] support for LINEAR fill is not yet implemented [function=last(v), class=io.questdb.griffin.engine.functions.groupby.LastIPv4GroupByFunction]
                ## subsample_stride
                min\terror: [47] integer expected for stride
                max\terror: [55] integer expected for stride
                sentinel\terror: [47] integer expected for stride
                null\terror: [38] integer expected for stride
                ## subsample_target
                min\terror: [47] integer expected for target point count
                max\terror: [55] integer expected for target point count
                sentinel\terror: [47] integer expected for target point count
                null\terror: [38] integer expected for target point count
                ## memoized
                props: random_access=true size=known timestamp=none
                k\ta\ta2\ta3
                min\t0.0.0.1\t0.0.0.1\t0.0.0.1
                max\t255.255.255.255\t255.255.255.255\t255.255.255.255
                sentinel\t\t\t
                null\t\t\t
                plan memoizes: true
                ## where_bound_const
                min\terror: [31] there is no matching operator `>=` with the argument types: LONG >= IPv4
                max\terror: [31] there is no matching operator `>=` with the argument types: LONG >= IPv4
                sentinel\terror: [31] there is no matching operator `>=` with the argument types: LONG >= IPv4
                null\terror: [31] there is no matching operator `>=` with the argument types: LONG >= IPv4
                ## where_bound_bind
                min\terror: [31] there is no matching operator `>=` with the argument types: LONG >= IPv4
                max\terror: [31] there is no matching operator `>=` with the argument types: LONG >= IPv4
                sentinel\terror: [31] there is no matching operator `>=` with the argument types: LONG >= IPv4
                null\terror: [31] there is no matching operator `>=` with the argument types: LONG >= IPv4
                ## where_key
                min\tb:min
                max\tb:max
                sentinel\tb:null
                null\tb:null
                ## copy_bind
                status\tmessage
                finished\t
                k
                max
                ## between_timestamp
                error: [27] there is no matching operator `between` with the argument type: IPv4
                ## eq_null_double
                props: random_access=true size=unknown timestamp=none
                k\tv
                sentinel\t
                null\t
                ## bind_value
                setBoolean\terror: [0] bind variable at 0 is defined as IPv4 and cannot accept BOOLEAN
                setByte\terror: [0] bind variable at 0 is defined as IPv4 and cannot accept BYTE
                setShort\terror: [0] bind variable at 0 is defined as IPv4 and cannot accept SHORT
                setChar\terror: [0] bind variable at 0 is defined as IPv4 and cannot accept CHAR
                setInt\t0.0.0.1
                setLong\terror: [0] bind variable at 0 is defined as IPv4 and cannot accept LONG
                setFloat\terror: [0] bind variable at 0 is defined as IPv4 and cannot accept FLOAT
                setDouble\terror: [0] bind variable at 0 is defined as IPv4 and cannot accept DOUBLE
                setDate\terror: [0] bind variable at 0 is defined as IPv4 and cannot accept DATE
                setTimestamp\terror: [0] bind variable at 0 is defined as IPv4 and cannot accept TIMESTAMP
                setStr\terror: invalid IPv4 format: 1
                setVarchar\terror: invalid IPv4 format: 1
                setLong256\terror: [0] bind variable at 0 is defined as IPv4 and cannot accept LONG256
                setUuid\terror: [0] bind variable at 0 is defined as IPv4 and cannot accept UUID
                setArray\terror: [0] bind variable at 0 is defined as IPv4 and cannot accept ARRAY
                ## window_anchor
                error: create view: [147] ANCHOR EXPRESSION must return TIMESTAMP, LONG, or INT; got IPv4
                """);
        rec("VARCHAR", """
                ## filter_eq
                props: random_access=true size=unknown timestamp=none
                k\tv
                max\tü€😀�
                ## filter_ne
                props: random_access=true size=unknown timestamp=none
                k\tv
                empty\t
                min\t\s
                escape\ta"b,c\\d'e
                null\t
                ## filter_null
                props: random_access=true size=unknown timestamp=none
                k\tv
                null\t
                ## filter_not_null
                props: random_access=true size=unknown timestamp=none
                k\tv
                empty\t
                min\t\s
                max\tü€😀�
                escape\ta"b,c\\d'e
                ## filter_lt
                props: random_access=true size=unknown timestamp=none
                k\tv
                empty\t
                min\t\s
                escape\ta"b,c\\d'e
                ## filter_ge
                props: random_access=true size=unknown timestamp=none
                k\tv
                max\tü€😀�
                ## order_asc
                props: random_access=true size=known timestamp=none
                k\tv
                null\t
                empty\t
                min\t\s
                escape\ta"b,c\\d'e
                max\tü€😀�
                ## order_desc
                props: random_access=true size=known timestamp=none
                k\tv
                max\tü€😀�
                escape\ta"b,c\\d'e
                min\t\s
                empty\t
                null\t
                ## group_by
                props: random_access=true size=known timestamp=f:asc
                v\tc\tf
                \t2\t1970-01-01T00:00:00.000000Z
                 \t2\t1970-01-01T00:00:01.000000Z
                ü€😀�\t2\t1970-01-01T00:00:02.000000Z
                a"b,c\\d'e\t2\t1970-01-01T00:00:03.000000Z
                \t2\t1970-01-01T00:00:04.000000Z
                ## join_inner
                props: random_access=true size=unknown timestamp=none
                tk\tuk\tv
                empty\tempty\t
                max\tmax\tü€😀�
                null\tnull\t
                ## join_left
                props: random_access=true size=unknown timestamp=none
                tk\tuk\tuv
                empty\tempty\t
                escape\t\t
                max\tmax\tü€😀�
                min\t\t
                null\tnull\t
                ## join_left_null
                props: random_access=true size=unknown timestamp=none
                tk\tuv
                empty\t
                escape\t
                max\tü€😀�
                min\t
                null\t
                ## union_all
                props: random_access=false size=known timestamp=none
                k\tv
                empty\t
                min\t\s
                max\tü€😀�
                escape\ta"b,c\\d'e
                null\t
                empty\t
                max\tü€😀�
                null\t
                ## union_null
                props: random_access=false size=known timestamp=none
                k\tv
                empty\t
                max\tü€😀�
                null\t
                null_branch\t
                ## case_no_else
                props: random_access=true size=known timestamp=none
                k\tc
                empty\t
                min\t
                max\tü€😀�
                escape\t
                null\t
                ## case_else
                props: random_access=true size=known timestamp=none
                k\tc
                empty\t
                min\t\s
                max\tü€😀�
                escape\ta"b,c\\d'e
                null\tü€😀�
                ## lag
                error: inconvertible value: `` [VARCHAR -> DOUBLE]
                ## sample_by
                props: random_access=true size=known timestamp=ts:asc
                ts\tf\tl\tc
                1970-01-01T00:00:00.000000Z\t\t \t2
                1970-01-01T00:00:02.000000Z\tü€😀�\ta"b,c\\d'e\t2
                1970-01-01T00:00:04.000000Z\t\t\t1
                ## first_last
                props: random_access=true size=known timestamp=none
                k\tf\tl
                A\t\tü€😀�
                B\tü€😀�\t
                C\t\t
                ## first_not_null
                props: random_access=true size=known timestamp=none
                k\tf\tl
                A\tü€😀�\tü€😀�
                B\tü€😀�\tü€😀�
                C\t\t
                ## fill_prev
                props: random_access=false size=unknown timestamp=ts:asc
                ts\tv
                1970-01-01T00:00:00.000000Z\t
                1970-01-01T00:00:01.000000Z\t
                1970-01-01T00:00:02.000000Z\tü€😀�
                1970-01-01T00:00:03.000000Z\tü€😀�
                1970-01-01T00:00:04.000000Z\t
                ## latest_on
                props: random_access=true size=known timestamp=none
                k\tv
                b:empty\t
                b:escape\ta"b,c\\d'e
                b:max\tü€😀�
                b:min\t\s
                b:null\t
                ## cast
                target\tempty|min|max|escape|null
                BOOLEAN\tfalse|false|false|false|false
                BYTE\t0|0|0|0|0
                SHORT\t0|0|0|0|0
                CHAR\t| |ü|a|
                INT\tnull|null|null|null|null
                LONG\tnull|null|null|null|null
                DATE\t||||
                TIMESTAMP\t||||
                FLOAT\tnull|null|null|null|null
                DOUBLE\tnull|null|null|null|null
                STRING\t| |ü€😀�|a"b,c\\d'e|
                SYMBOL\t| |ü€😀�|a"b,c\\d'e|
                LONG256\t||||
                GEOBYTE\t||||
                GEOSHORT\t||||
                GEOINT\t||||
                GEOLONG\t||||
                BINARY\terror: [20] unsupported cast
                UUID\t||||
                LONG128\terror: [20] unsupported cast
                IPv4\t||||
                VARCHAR\t| |ü€😀�|a"b,c\\d'e|
                DOUBLE[]\tnull|null|null|null|null
                DECIMAL8\terror: inconvertible value: `` [VARCHAR -> DECIMAL(2,1)]
                DECIMAL16\terror: inconvertible value: `` [VARCHAR -> DECIMAL(4,2)]
                DECIMAL32\terror: inconvertible value: `` [VARCHAR -> DECIMAL(9,0)]
                DECIMAL64\terror: inconvertible value: `` [VARCHAR -> DECIMAL(16,4)]
                DECIMAL128\terror: inconvertible value: `` [VARCHAR -> DECIMAL(38,10)]
                DECIMAL256\terror: inconvertible value: `` [VARCHAR -> DECIMAL(76,20)]
                INTERVAL\terror: [20] unsupported cast
                VARCHAR_SLICE\terror: [20] unsupported cast
                TIMESTAMP_NS\t||||
                GEOHASH(1c)\t||||
                GEOHASH(8b)\t||||
                GEOHASH(31b)\t||||
                GEOHASH(12c)\t||||
                DECIMAL(5,2)\terror: inconvertible value: `` [VARCHAR -> DECIMAL(5,2)]
                DECIMAL(18,3)\terror: inconvertible value: `` [VARCHAR -> DECIMAL(18,3)]
                DOUBLE[][]\tnull|null|null|null|null
                INTERVAL(us)\terror: [20] unsupported cast
                INTERVAL(ns)\terror: [20] unsupported cast
                ## fill_null
                props: random_access=false size=unknown timestamp=ts:asc
                ts\tv
                1970-01-01T00:00:00.000000Z\t\s
                1970-01-01T00:00:01.000000Z\t
                1970-01-01T00:00:02.000000Z\tü€😀�
                1970-01-01T00:00:03.000000Z\t
                1970-01-01T00:00:04.000000Z\t
                ## fill_value
                props: random_access=false size=unknown timestamp=ts:asc
                ts\tv
                1970-01-01T00:00:00.000000Z\t\s
                1970-01-01T00:00:01.000000Z\tü€😀�
                1970-01-01T00:00:02.000000Z\tü€😀�
                1970-01-01T00:00:03.000000Z\tü€😀�
                1970-01-01T00:00:04.000000Z\t
                ## fill_linear
                error: [46] support for LINEAR fill is not yet implemented [function=last(v), class=io.questdb.griffin.engine.functions.groupby.LastVarcharGroupByFunction]
                ## subsample_stride
                empty\terror: [40] integer expected for stride
                min\terror: [41] integer expected for stride
                max\terror: [45] integer expected for stride
                escape\terror: [50] integer expected for stride
                null\terror: [38] integer expected for stride
                ## subsample_target
                empty\terror: [40] integer expected for target point count
                min\terror: [41] integer expected for target point count
                max\terror: [45] integer expected for target point count
                escape\terror: [50] integer expected for target point count
                null\terror: [38] integer expected for target point count
                ## memoized
                props: random_access=true size=known timestamp=none
                k\ta\ta2\ta3
                empty\t\t\t
                min\t \t \t\s
                max\tü€😀�\tü€😀�\tü€😀�
                escape\ta"b,c\\d'e\ta"b,c\\d'e\ta"b,c\\d'e
                null\t\t\t
                plan memoizes: true
                ## where_bound_const
                empty\terror: inconvertible value: `` [VARCHAR -> LONG]
                min\terror: inconvertible value: ` ` [VARCHAR -> LONG]
                max\terror: inconvertible value: `ü€😀�` [VARCHAR -> LONG]
                escape\terror: inconvertible value: `a"b,c\\d'e` [VARCHAR -> LONG]
                null\t
                ## where_bound_bind
                empty\terror: inconvertible value: `` [VARCHAR -> LONG]
                min\terror: inconvertible value: ` ` [VARCHAR -> LONG]
                max\terror: inconvertible value: `ü€😀�` [VARCHAR -> LONG]
                escape\terror: inconvertible value: `a"b,c\\d'e` [VARCHAR -> LONG]
                null\t
                ## where_key
                empty\tb:empty
                min\tb:min
                max\tb:max
                escape\tb:escape
                null\tb:null
                ## copy_bind
                status\tmessage
                finished\t
                k
                max
                ## between_timestamp
                error: inconvertible value: `` [VARCHAR -> TIMESTAMP_NS]
                ## eq_null_double
                props: random_access=true size=unknown timestamp=none
                k\tv
                null\t
                ## bind_value
                setBoolean\ttrue
                setByte\t1
                setShort\t1
                setChar\t1
                setInt\t1
                setLong\t1
                setFloat\t1.5
                setDouble\t1.5
                setDate\t1
                setTimestamp\t1970-01-01T00:00:00.000001Z
                setStr\t1
                setVarchar\t1
                setLong256\t0x01
                setUuid\t00000000-0000-0000-0000-000000000001
                setArray\terror: [0] bind variable at 0 is defined as VARCHAR and cannot accept ARRAY
                ## window_anchor
                error: create view: [147] ANCHOR EXPRESSION must return TIMESTAMP, LONG, or INT; got VARCHAR
                """);
        rec("DOUBLE[]", """
                ## filter_eq
                props: random_access=true size=unknown timestamp=none
                k\tv
                max\t[1.7976931348623157E308]
                ## filter_ne
                props: random_access=true size=unknown timestamp=none
                k\tv
                min\t[-1.7976931348623157E308]
                empty\t[]
                specials\t[null,null,null,-0.0]
                null\tnull
                ## filter_null
                error: [27] there is no matching operator `=` with the argument types: DOUBLE[] = NULL
                ## filter_not_null
                error: [27] there is no matching operator `!=` with the argument types: DOUBLE[] != NULL
                ## filter_lt
                error: [27] there is no matching operator `<` with the argument types: DOUBLE[] < DOUBLE[]
                ## filter_ge
                error: [27] there is no matching operator `>=` with the argument types: DOUBLE[] >= DOUBLE[]
                ## order_asc
                error: [28] DOUBLE[] is not a supported type in ORDER BY clause
                ## order_desc
                error: [28] DOUBLE[] is not a supported type in ORDER BY clause
                ## group_by
                props: random_access=true size=known timestamp=f:asc
                v\tc\tf
                [-1.7976931348623157E308]\t2\t1970-01-01T00:00:00.000000Z
                [1.7976931348623157E308]\t2\t1970-01-01T00:00:01.000000Z
                []\t2\t1970-01-01T00:00:02.000000Z
                [null,null,null,-0.0]\t2\t1970-01-01T00:00:03.000000Z
                null\t2\t1970-01-01T00:00:04.000000Z
                ## join_inner
                props: random_access=true size=unknown timestamp=none
                tk\tuk\tv
                empty\tempty\t[]
                min\tmin\t[-1.7976931348623157E308]
                null\tnull\tnull
                ## join_left
                props: random_access=true size=unknown timestamp=none
                tk\tuk\tuv
                empty\tempty\t[]
                max\t\tnull
                min\tmin\t[-1.7976931348623157E308]
                null\tnull\tnull
                specials\t\tnull
                ## join_left_null
                props: random_access=true size=unknown timestamp=none
                tk\tuv
                empty\t[]
                max\tnull
                min\t[-1.7976931348623157E308]
                null\tnull
                specials\tnull
                ## union_all
                props: random_access=false size=known timestamp=none
                k\tv
                min\t[-1.7976931348623157E308]
                max\t[1.7976931348623157E308]
                empty\t[]
                specials\t[null,null,null,-0.0]
                null\tnull
                min\t[-1.7976931348623157E308]
                empty\t[]
                null\tnull
                ## union_null
                props: random_access=false size=known timestamp=none
                k\tv
                min\t[-1.7976931348623157E308]
                empty\t[]
                null\t
                null_branch\t
                ## case_no_else
                props: random_access=true size=known timestamp=none
                k\tc
                min\tnull
                max\t[1.7976931348623157E308]
                empty\tnull
                specials\tnull
                null\tnull
                ## case_else
                error: [20] type DOUBLE[] is not supported in 'switch' type of 'case' statement
                ## lag
                error: [13] there is no matching function `lag` with the argument types: (DOUBLE[])
                ## sample_by
                props: random_access=true size=known timestamp=ts:asc
                ts\tf\tl\tc
                1970-01-01T00:00:00.000000Z\t[-1.7976931348623157E308]\t[1.7976931348623157E308]\t2
                1970-01-01T00:00:02.000000Z\t[]\t[null,null,null,-0.0]\t2
                1970-01-01T00:00:04.000000Z\tnull\tnull\t1
                ## first_last
                props: random_access=true size=known timestamp=none
                k\tf\tl
                A\tnull\t[1.7976931348623157E308]
                B\t[1.7976931348623157E308]\tnull
                C\tnull\tnull
                ## first_not_null
                props: random_access=true size=known timestamp=none
                k\tf\tl
                A\t[1.7976931348623157E308]\t[1.7976931348623157E308]
                B\t[1.7976931348623157E308]\t[1.7976931348623157E308]
                C\tnull\tnull
                ## fill_prev
                props: random_access=false size=unknown timestamp=ts:asc
                ts\tv
                1970-01-01T00:00:00.000000Z\tnull
                1970-01-01T00:00:01.000000Z\tnull
                1970-01-01T00:00:02.000000Z\t[1.7976931348623157E308]
                1970-01-01T00:00:03.000000Z\t[1.7976931348623157E308]
                1970-01-01T00:00:04.000000Z\tnull
                ## latest_on
                error: [47] v (DOUBLE[]): invalid type, only [BOOLEAN, BYTE, SHORT, INT, LONG, DATE, TIMESTAMP, FLOAT, DOUBLE, LONG128, LONG256, CHAR, STRING, VARCHAR, SYMBOL, UUID, GEOHASH, IPv4] are supported in LATEST ON
                ## cast
                target\tmin|max|empty|specials|null
                BOOLEAN\tno cast [10]
                BYTE\tno cast [10]
                SHORT\tno cast [10]
                CHAR\tno cast [10]
                INT\tno cast [10]
                LONG\tno cast [10]
                DATE\tno cast [10]
                TIMESTAMP\tno cast [10]
                FLOAT\tno cast [10]
                DOUBLE\tno cast [10]
                STRING\t[-1.7976931348623157E308]|[1.7976931348623157E308]|[]|[null,null,null,-0.0]|
                SYMBOL\tno cast [10]
                LONG256\tno cast [10]
                GEOBYTE\tno cast [10]
                GEOSHORT\tno cast [10]
                GEOINT\tno cast [10]
                GEOLONG\tno cast [10]
                BINARY\terror: [20] unsupported cast
                UUID\tno cast [10]
                LONG128\terror: [20] unsupported cast
                IPv4\tno cast [10]
                VARCHAR\t[-1.7976931348623157E308]|[1.7976931348623157E308]|[]|[null,null,null,-0.0]|
                DOUBLE[]\t[-1.7976931348623157E308]|[1.7976931348623157E308]|[]|[null,null,null,-0.0]|null
                DECIMAL8\tno cast [10]
                DECIMAL16\tno cast [10]
                DECIMAL32\tno cast [10]
                DECIMAL64\tno cast [10]
                DECIMAL128\tno cast [10]
                DECIMAL256\tno cast [10]
                INTERVAL\terror: [20] unsupported cast
                VARCHAR_SLICE\terror: [20] unsupported cast
                TIMESTAMP_NS\tno cast [10]
                GEOHASH(1c)\tno cast [10]
                GEOHASH(8b)\tno cast [10]
                GEOHASH(31b)\tno cast [10]
                GEOHASH(12c)\tno cast [10]
                DECIMAL(5,2)\tno cast [10]
                DECIMAL(18,3)\tno cast [10]
                DOUBLE[][]\terror: not supported as array element type: NULL
                INTERVAL(us)\terror: [20] unsupported cast
                INTERVAL(ns)\terror: [20] unsupported cast
                ## fill_null
                props: random_access=false size=unknown timestamp=ts:asc
                ts\tv
                1970-01-01T00:00:00.000000Z\t[-1.7976931348623157E308]
                1970-01-01T00:00:01.000000Z\tnull
                1970-01-01T00:00:02.000000Z\t[1.7976931348623157E308]
                1970-01-01T00:00:03.000000Z\tnull
                1970-01-01T00:00:04.000000Z\tnull
                ## fill_value
                error: [46] support for VALUE fill is not yet implemented [function=last(v), class=io.questdb.griffin.engine.functions.groupby.LastArrayGroupByFunction]
                ## fill_linear
                error: [46] support for LINEAR fill is not yet implemented [function=last(v), class=io.questdb.griffin.engine.functions.groupby.LastArrayGroupByFunction]
                ## subsample_stride
                min\terror: [43] integer expected for stride
                max\terror: [43] integer expected for stride
                empty\terror: [45] integer expected for stride
                specials\terror: [43] integer expected for stride
                null\terror: [38] integer expected for stride
                ## subsample_target
                min\terror: [43] integer expected for target point count
                max\terror: [43] integer expected for target point count
                empty\terror: [45] integer expected for target point count
                specials\terror: [43] integer expected for target point count
                null\terror: [38] integer expected for target point count
                ## memoized
                props: random_access=true size=known timestamp=none
                k\ta\ta2\ta3
                min\t[-1.7976931348623157E308]\t[-1.7976931348623157E308]\t[-1.7976931348623157E308]
                max\t[1.7976931348623157E308]\t[1.7976931348623157E308]\t[1.7976931348623157E308]
                empty\t[]\t[]\t[]
                specials\t[null,null,null,-0.0]\t[null,null,null,-0.0]\t[null,null,null,-0.0]
                null\tnull\tnull\tnull
                plan memoizes: true
                ## where_bound_const
                min\terror: [31] there is no matching operator `>=` with the argument types: LONG >= DOUBLE[]
                max\terror: [31] there is no matching operator `>=` with the argument types: LONG >= DOUBLE[]
                empty\terror: [31] there is no matching operator `>=` with the argument types: LONG >= DOUBLE[]
                specials\terror: [31] there is no matching operator `>=` with the argument types: LONG >= DOUBLE[]
                null\terror: [31] there is no matching operator `>=` with the argument types: LONG >= DOUBLE[]
                ## where_bound_bind
                min\terror: [31] there is no matching operator `>=` with the argument types: LONG >= DOUBLE[]
                max\terror: [31] there is no matching operator `>=` with the argument types: LONG >= DOUBLE[]
                empty\terror: [31] there is no matching operator `>=` with the argument types: LONG >= DOUBLE[]
                specials\tbind error: inconvertible value: `[null,null,null,-0.0]` [STRING -> DOUBLE[]]
                null\tbind error: [-1] array type mismatch [expected=DOUBLE[], actual=NULL]
                ## where_key
                min\terror: [81] v (DOUBLE[]): invalid type, only [BOOLEAN, BYTE, SHORT, INT, LONG, DATE, TIMESTAMP, FLOAT, DOUBLE, LONG128, LONG256, CHAR, STRING, VARCHAR, SYMBOL, UUID, GEOHASH, IPv4] are supported in LATEST ON
                max\terror: [80] v (DOUBLE[]): invalid type, only [BOOLEAN, BYTE, SHORT, INT, LONG, DATE, TIMESTAMP, FLOAT, DOUBLE, LONG128, LONG256, CHAR, STRING, VARCHAR, SYMBOL, UUID, GEOHASH, IPv4] are supported in LATEST ON
                empty\terror: [58] v (DOUBLE[]): invalid type, only [BOOLEAN, BYTE, SHORT, INT, LONG, DATE, TIMESTAMP, FLOAT, DOUBLE, LONG128, LONG256, CHAR, STRING, VARCHAR, SYMBOL, UUID, GEOHASH, IPv4] are supported in LATEST ON
                specials\terror: [77] v (DOUBLE[]): invalid type, only [BOOLEAN, BYTE, SHORT, INT, LONG, DATE, TIMESTAMP, FLOAT, DOUBLE, LONG128, LONG256, CHAR, STRING, VARCHAR, SYMBOL, UUID, GEOHASH, IPv4] are supported in LATEST ON
                null\terror: [58] v (DOUBLE[]): invalid type, only [BOOLEAN, BYTE, SHORT, INT, LONG, DATE, TIMESTAMP, FLOAT, DOUBLE, LONG128, LONG256, CHAR, STRING, VARCHAR, SYMBOL, UUID, GEOHASH, IPv4] are supported in LATEST ON
                ## copy_bind
                status\tmessage
                finished\t
                k
                ## between_timestamp
                error: [27] there is no matching operator `between` with the argument type: DOUBLE[]
                ## eq_null_double
                error: [27] there is no matching operator `=` with the argument types: DOUBLE[] = NULL
                ## bind_value
                setBoolean\terror: [0] bind variable at 0 is defined as DOUBLE[] and cannot accept BOOLEAN
                setByte\terror: [0] bind variable at 0 is defined as DOUBLE[] and cannot accept BYTE
                setShort\terror: [0] bind variable at 0 is defined as DOUBLE[] and cannot accept SHORT
                setChar\terror: [0] bind variable at 0 is defined as DOUBLE[] and cannot accept CHAR
                setInt\terror: [0] bind variable at 0 is defined as DOUBLE[] and cannot accept INT
                setLong\terror: [0] bind variable at 0 is defined as DOUBLE[] and cannot accept LONG
                setFloat\terror: [0] bind variable at 0 is defined as DOUBLE[] and cannot accept FLOAT
                setDouble\terror: [0] bind variable at 0 is defined as DOUBLE[] and cannot accept DOUBLE
                setDate\terror: [0] bind variable at 0 is defined as DOUBLE[] and cannot accept DATE
                setTimestamp\terror: [0] bind variable at 0 is defined as DOUBLE[] and cannot accept TIMESTAMP
                setStr\terror: inconvertible value: `1` [STRING -> DOUBLE[]]
                setVarchar\terror: [0] bind variable at 0 is defined as DOUBLE[] and cannot accept VARCHAR
                setLong256\terror: [0] bind variable at 0 is defined as DOUBLE[] and cannot accept LONG256
                setUuid\terror: [0] bind variable at 0 is defined as DOUBLE[] and cannot accept UUID
                setArray\t[1.5]
                ## window_anchor
                error: create view: [147] ANCHOR EXPRESSION must return TIMESTAMP, LONG, or INT; got DOUBLE[]
                """);
        rec("DECIMAL8", """
                ## cast
                target\tmin|max|null
                BOOLEAN\tno cast [10]
                BYTE\t-9|9|0
                SHORT\t-9|9|0
                CHAR\tno cast [10]
                INT\t-9|9|null
                LONG\t-9|9|null
                DATE\tno cast [10]
                TIMESTAMP\tno cast [10]
                FLOAT\t-9.9|9.9|null
                DOUBLE\t-9.9|9.9|null
                STRING\t-9.9|9.9|
                SYMBOL\tno cast [10]
                LONG256\tno cast [10]
                GEOBYTE\tno cast [10]
                GEOSHORT\tno cast [10]
                GEOINT\tno cast [10]
                GEOLONG\tno cast [10]
                BINARY\terror: [20] unsupported cast
                UUID\tno cast [10]
                LONG128\terror: [20] unsupported cast
                IPv4\tno cast [10]
                VARCHAR\t-9.9|9.9|
                DOUBLE[]\tno cast [10]
                DECIMAL8\t-9.9|9.9|
                DECIMAL16\t-9.90|9.90|
                DECIMAL32\terror: inconvertible value: -9.9 [DECIMAL(2,1) -> DECIMAL(9,0)]
                DECIMAL64\t-9.9000|9.9000|
                DECIMAL128\t-9.9000000000|9.9000000000|
                DECIMAL256\t-9.90000000000000000000|9.90000000000000000000|
                INTERVAL\terror: [20] unsupported cast
                VARCHAR_SLICE\terror: [20] unsupported cast
                TIMESTAMP_NS\tno cast [10]
                GEOHASH(1c)\tno cast [10]
                GEOHASH(8b)\tno cast [10]
                GEOHASH(31b)\tno cast [10]
                GEOHASH(12c)\tno cast [10]
                DECIMAL(5,2)\t-9.90|9.90|
                DECIMAL(18,3)\t-9.900|9.900|
                DOUBLE[][]\tno cast [10]
                INTERVAL(us)\terror: [20] unsupported cast
                INTERVAL(ns)\terror: [20] unsupported cast
                ## filter_eq
                props: random_access=true size=unknown timestamp=none
                k\tv
                max\t9.9
                ## filter_ne
                props: random_access=true size=unknown timestamp=none
                k\tv
                min\t-9.9
                null\t
                ## filter_null
                props: random_access=true size=unknown timestamp=none
                k\tv
                null\t
                ## filter_not_null
                props: random_access=true size=unknown timestamp=none
                k\tv
                min\t-9.9
                max\t9.9
                ## filter_lt
                props: random_access=true size=unknown timestamp=none
                k\tv
                min\t-9.9
                ## filter_ge
                props: random_access=true size=unknown timestamp=none
                k\tv
                max\t9.9
                ## order_asc
                props: random_access=true size=known timestamp=none
                k\tv
                null\t
                min\t-9.9
                max\t9.9
                ## order_desc
                props: random_access=true size=known timestamp=none
                k\tv
                max\t9.9
                min\t-9.9
                null\t
                ## group_by
                props: random_access=true size=known timestamp=f:asc
                v\tc\tf
                -9.9\t2\t1970-01-01T00:00:00.000000Z
                9.9\t2\t1970-01-01T00:00:01.000000Z
                \t2\t1970-01-01T00:00:02.000000Z
                ## join_inner
                props: random_access=true size=unknown timestamp=none
                tk\tuk\tv
                min\tmin\t-9.9
                null\tnull\t
                ## join_left
                props: random_access=true size=unknown timestamp=none
                tk\tuk\tuv
                max\t\t
                min\tmin\t-9.9
                null\tnull\t
                ## join_left_null
                props: random_access=true size=unknown timestamp=none
                tk\tuv
                max\t
                min\t-9.9
                null\t
                ## union_all
                props: random_access=false size=known timestamp=none
                k\tv
                min\t-9.9
                max\t9.9
                null\t
                min\t-9.9
                null\t
                ## union_null
                props: random_access=false size=known timestamp=none
                k\tv
                min\t-9.9
                null\t
                null_branch\t
                ## case_no_else
                props: random_access=true size=known timestamp=none
                k\tc
                min\t
                max\t9.9
                null\t
                ## case_else
                error: [20] type DECIMAL(2,1) is not supported in 'switch' type of 'case' statement
                ## lag
                props: random_access=false size=known timestamp=none
                k\tv\tp
                min\t-9.9\t
                max\t9.9\t-9.9
                null\t\t9.9
                ## sample_by
                props: random_access=true size=known timestamp=ts:asc
                ts\tf\tl\tc
                1970-01-01T00:00:00.000000Z\t-9.9\t9.9\t2
                1970-01-01T00:00:02.000000Z\t\t\t1
                ## first_last
                props: random_access=true size=known timestamp=none
                k\tf\tl
                A\t\t9.9
                B\t9.9\t
                C\t\t
                ## first_not_null
                props: random_access=true size=known timestamp=none
                k\tf\tl
                A\t9.9\t9.9
                B\t9.9\t9.9
                C\t\t
                ## fill_prev
                props: random_access=false size=unknown timestamp=ts:asc
                ts\tv
                1970-01-01T00:00:00.000000Z\t
                1970-01-01T00:00:01.000000Z\t
                1970-01-01T00:00:02.000000Z\t9.9
                1970-01-01T00:00:03.000000Z\t9.9
                1970-01-01T00:00:04.000000Z\t
                ## latest_on
                error: [47] v (DECIMAL(2,1)): invalid type, only [BOOLEAN, BYTE, SHORT, INT, LONG, DATE, TIMESTAMP, FLOAT, DOUBLE, LONG128, LONG256, CHAR, STRING, VARCHAR, SYMBOL, UUID, GEOHASH, IPv4] are supported in LATEST ON
                ## fill_null
                props: random_access=false size=unknown timestamp=ts:asc
                ts\tv
                1970-01-01T00:00:00.000000Z\t-9.9
                1970-01-01T00:00:01.000000Z\t
                1970-01-01T00:00:02.000000Z\t9.9
                1970-01-01T00:00:03.000000Z\t
                1970-01-01T00:00:04.000000Z\t
                ## fill_value
                error: [46] fill value of type DOUBLE cannot fill column of type DECIMAL(2,1)
                ## fill_linear
                error: [46] support for LINEAR fill is not yet implemented [function=last(v), class=io.questdb.griffin.engine.functions.groupby.LastDecimalGroupByFunctionFactory$Decimal8Func]
                ## subsample_stride
                min\terror: [44] integer expected for stride
                max\terror: [43] integer expected for stride
                null\terror: [38] integer expected for stride
                ## subsample_target
                min\terror: [44] integer expected for target point count
                max\terror: [43] integer expected for target point count
                null\terror: [38] integer expected for target point count
                ## memoized
                props: random_access=true size=known timestamp=none
                k\ta\ta2\ta3
                min\t-9.9\t-9.9\t-9.9
                max\t9.9\t9.9\t9.9
                null\t\t\t
                plan memoizes: true
                ## where_bound_const
                min\tmin,max,null
                max\tmax,null
                null\t
                ## where_bound_bind
                min\tmin,max,null
                max\tmax,null
                null\t
                ## where_key
                min\terror: [60] v (DECIMAL(2,1)): invalid type, only [BOOLEAN, BYTE, SHORT, INT, LONG, DATE, TIMESTAMP, FLOAT, DOUBLE, LONG128, LONG256, CHAR, STRING, VARCHAR, SYMBOL, UUID, GEOHASH, IPv4] are supported in LATEST ON
                max\terror: [59] v (DECIMAL(2,1)): invalid type, only [BOOLEAN, BYTE, SHORT, INT, LONG, DATE, TIMESTAMP, FLOAT, DOUBLE, LONG128, LONG256, CHAR, STRING, VARCHAR, SYMBOL, UUID, GEOHASH, IPv4] are supported in LATEST ON
                null\terror: [58] v (DECIMAL(2,1)): invalid type, only [BOOLEAN, BYTE, SHORT, INT, LONG, DATE, TIMESTAMP, FLOAT, DOUBLE, LONG128, LONG256, CHAR, STRING, VARCHAR, SYMBOL, UUID, GEOHASH, IPv4] are supported in LATEST ON
                ## copy_bind
                status\tmessage
                finished\t
                k
                max
                ## between_timestamp
                error: [27] there is no matching operator `between` with the argument type: DECIMAL(2,1)
                ## eq_null_double
                props: random_access=true size=unknown timestamp=none
                k\tv
                null\t
                ## bind_value
                setBoolean\terror: [0] bind variable at 0 is defined as DECIMAL(2,1) and cannot accept BOOLEAN
                setByte\terror: [0] bind variable at 0 is defined as DECIMAL(2,1) and cannot accept BYTE
                setShort\terror: [0] bind variable at 0 is defined as DECIMAL(2,1) and cannot accept SHORT
                setChar\terror: [0] bind variable at 0 is defined as DECIMAL(2,1) and cannot accept CHAR
                setInt\terror: [0] bind variable at 0 is defined as DECIMAL(2,1) and cannot accept INT
                setLong\terror: [0] bind variable at 0 is defined as DECIMAL(2,1) and cannot accept LONG
                setFloat\terror: [0] bind variable at 0 is defined as DECIMAL(2,1) and cannot accept FLOAT
                setDouble\terror: [0] bind variable at 0 is defined as DECIMAL(2,1) and cannot accept DOUBLE
                setDate\terror: [0] bind variable at 0 is defined as DECIMAL(2,1) and cannot accept DATE
                setTimestamp\terror: [0] bind variable at 0 is defined as DECIMAL(2,1) and cannot accept TIMESTAMP
                setStr\t1.0
                setVarchar\t1.0
                setLong256\terror: [0] bind variable at 0 is defined as DECIMAL(2,1) and cannot accept LONG256
                setUuid\terror: [0] bind variable at 0 is defined as DECIMAL(2,1) and cannot accept UUID
                setArray\terror: [0] bind variable at 0 is defined as DECIMAL(2,1) and cannot accept ARRAY
                ## window_anchor
                error: create view: [147] ANCHOR EXPRESSION must return TIMESTAMP, LONG, or INT; got DECIMAL(2,1)
                """);
        rec("DECIMAL16", """
                ## cast
                target\tmin|max|null
                BOOLEAN\tno cast [10]
                BYTE\t-99|99|0
                SHORT\t-99|99|0
                CHAR\tno cast [10]
                INT\t-99|99|null
                LONG\t-99|99|null
                DATE\tno cast [10]
                TIMESTAMP\tno cast [10]
                FLOAT\t-99.99|99.99|null
                DOUBLE\t-99.99|99.99|null
                STRING\t-99.99|99.99|
                SYMBOL\tno cast [10]
                LONG256\tno cast [10]
                GEOBYTE\tno cast [10]
                GEOSHORT\tno cast [10]
                GEOINT\tno cast [10]
                GEOLONG\tno cast [10]
                BINARY\terror: [20] unsupported cast
                UUID\tno cast [10]
                LONG128\terror: [20] unsupported cast
                IPv4\tno cast [10]
                VARCHAR\t-99.99|99.99|
                DOUBLE[]\tno cast [10]
                DECIMAL8\terror: inconvertible value: -99.99 [DECIMAL(4,2) -> DECIMAL(2,1)]
                DECIMAL16\t-99.99|99.99|
                DECIMAL32\terror: inconvertible value: -99.99 [DECIMAL(4,2) -> DECIMAL(9,0)]
                DECIMAL64\t-99.9900|99.9900|
                DECIMAL128\t-99.9900000000|99.9900000000|
                DECIMAL256\t-99.99000000000000000000|99.99000000000000000000|
                INTERVAL\terror: [20] unsupported cast
                VARCHAR_SLICE\terror: [20] unsupported cast
                TIMESTAMP_NS\tno cast [10]
                GEOHASH(1c)\tno cast [10]
                GEOHASH(8b)\tno cast [10]
                GEOHASH(31b)\tno cast [10]
                GEOHASH(12c)\tno cast [10]
                DECIMAL(5,2)\t-99.99|99.99|
                DECIMAL(18,3)\t-99.990|99.990|
                DOUBLE[][]\tno cast [10]
                INTERVAL(us)\terror: [20] unsupported cast
                INTERVAL(ns)\terror: [20] unsupported cast
                ## filter_eq
                props: random_access=true size=unknown timestamp=none
                k\tv
                max\t99.99
                ## filter_ne
                props: random_access=true size=unknown timestamp=none
                k\tv
                min\t-99.99
                null\t
                ## filter_null
                props: random_access=true size=unknown timestamp=none
                k\tv
                null\t
                ## filter_not_null
                props: random_access=true size=unknown timestamp=none
                k\tv
                min\t-99.99
                max\t99.99
                ## filter_lt
                props: random_access=true size=unknown timestamp=none
                k\tv
                min\t-99.99
                ## filter_ge
                props: random_access=true size=unknown timestamp=none
                k\tv
                max\t99.99
                ## order_asc
                props: random_access=true size=known timestamp=none
                k\tv
                null\t
                min\t-99.99
                max\t99.99
                ## order_desc
                props: random_access=true size=known timestamp=none
                k\tv
                max\t99.99
                min\t-99.99
                null\t
                ## group_by
                props: random_access=true size=known timestamp=f:asc
                v\tc\tf
                -99.99\t2\t1970-01-01T00:00:00.000000Z
                99.99\t2\t1970-01-01T00:00:01.000000Z
                \t2\t1970-01-01T00:00:02.000000Z
                ## join_inner
                props: random_access=true size=unknown timestamp=none
                tk\tuk\tv
                min\tmin\t-99.99
                null\tnull\t
                ## join_left
                props: random_access=true size=unknown timestamp=none
                tk\tuk\tuv
                max\t\t
                min\tmin\t-99.99
                null\tnull\t
                ## join_left_null
                props: random_access=true size=unknown timestamp=none
                tk\tuv
                max\t
                min\t-99.99
                null\t
                ## union_all
                props: random_access=false size=known timestamp=none
                k\tv
                min\t-99.99
                max\t99.99
                null\t
                min\t-99.99
                null\t
                ## union_null
                props: random_access=false size=known timestamp=none
                k\tv
                min\t-99.99
                null\t
                null_branch\t
                ## case_no_else
                props: random_access=true size=known timestamp=none
                k\tc
                min\t
                max\t99.99
                null\t
                ## case_else
                error: [20] type DECIMAL(4,2) is not supported in 'switch' type of 'case' statement
                ## lag
                props: random_access=false size=known timestamp=none
                k\tv\tp
                min\t-99.99\t
                max\t99.99\t-99.99
                null\t\t99.99
                ## sample_by
                props: random_access=true size=known timestamp=ts:asc
                ts\tf\tl\tc
                1970-01-01T00:00:00.000000Z\t-99.99\t99.99\t2
                1970-01-01T00:00:02.000000Z\t\t\t1
                ## first_last
                props: random_access=true size=known timestamp=none
                k\tf\tl
                A\t\t99.99
                B\t99.99\t
                C\t\t
                ## first_not_null
                props: random_access=true size=known timestamp=none
                k\tf\tl
                A\t99.99\t99.99
                B\t99.99\t99.99
                C\t\t
                ## fill_prev
                props: random_access=false size=unknown timestamp=ts:asc
                ts\tv
                1970-01-01T00:00:00.000000Z\t
                1970-01-01T00:00:01.000000Z\t
                1970-01-01T00:00:02.000000Z\t99.99
                1970-01-01T00:00:03.000000Z\t99.99
                1970-01-01T00:00:04.000000Z\t
                ## latest_on
                error: [47] v (DECIMAL(4,2)): invalid type, only [BOOLEAN, BYTE, SHORT, INT, LONG, DATE, TIMESTAMP, FLOAT, DOUBLE, LONG128, LONG256, CHAR, STRING, VARCHAR, SYMBOL, UUID, GEOHASH, IPv4] are supported in LATEST ON
                ## fill_null
                props: random_access=false size=unknown timestamp=ts:asc
                ts\tv
                1970-01-01T00:00:00.000000Z\t-99.99
                1970-01-01T00:00:01.000000Z\t
                1970-01-01T00:00:02.000000Z\t99.99
                1970-01-01T00:00:03.000000Z\t
                1970-01-01T00:00:04.000000Z\t
                ## fill_value
                error: [46] fill value of type DOUBLE cannot fill column of type DECIMAL(4,2)
                ## fill_linear
                error: [46] support for LINEAR fill is not yet implemented [function=last(v), class=io.questdb.griffin.engine.functions.groupby.LastDecimalGroupByFunctionFactory$Decimal16Func]
                ## subsample_stride
                min\terror: [46] integer expected for stride
                max\terror: [45] integer expected for stride
                null\terror: [38] integer expected for stride
                ## subsample_target
                min\terror: [46] integer expected for target point count
                max\terror: [45] integer expected for target point count
                null\terror: [38] integer expected for target point count
                ## memoized
                props: random_access=true size=known timestamp=none
                k\ta\ta2\ta3
                min\t-99.99\t-99.99\t-99.99
                max\t99.99\t99.99\t99.99
                null\t\t\t
                plan memoizes: true
                ## where_bound_const
                min\tmin,max,null
                max\tmax,null
                null\t
                ## where_bound_bind
                min\tmin,max,null
                max\tmax,null
                null\t
                ## where_key
                min\terror: [62] v (DECIMAL(4,2)): invalid type, only [BOOLEAN, BYTE, SHORT, INT, LONG, DATE, TIMESTAMP, FLOAT, DOUBLE, LONG128, LONG256, CHAR, STRING, VARCHAR, SYMBOL, UUID, GEOHASH, IPv4] are supported in LATEST ON
                max\terror: [61] v (DECIMAL(4,2)): invalid type, only [BOOLEAN, BYTE, SHORT, INT, LONG, DATE, TIMESTAMP, FLOAT, DOUBLE, LONG128, LONG256, CHAR, STRING, VARCHAR, SYMBOL, UUID, GEOHASH, IPv4] are supported in LATEST ON
                null\terror: [58] v (DECIMAL(4,2)): invalid type, only [BOOLEAN, BYTE, SHORT, INT, LONG, DATE, TIMESTAMP, FLOAT, DOUBLE, LONG128, LONG256, CHAR, STRING, VARCHAR, SYMBOL, UUID, GEOHASH, IPv4] are supported in LATEST ON
                ## copy_bind
                status\tmessage
                finished\t
                k
                max
                ## between_timestamp
                error: [27] there is no matching operator `between` with the argument type: DECIMAL(4,2)
                ## eq_null_double
                props: random_access=true size=unknown timestamp=none
                k\tv
                null\t
                ## bind_value
                setBoolean\terror: [0] bind variable at 0 is defined as DECIMAL(4,2) and cannot accept BOOLEAN
                setByte\terror: [0] bind variable at 0 is defined as DECIMAL(4,2) and cannot accept BYTE
                setShort\terror: [0] bind variable at 0 is defined as DECIMAL(4,2) and cannot accept SHORT
                setChar\terror: [0] bind variable at 0 is defined as DECIMAL(4,2) and cannot accept CHAR
                setInt\terror: [0] bind variable at 0 is defined as DECIMAL(4,2) and cannot accept INT
                setLong\terror: [0] bind variable at 0 is defined as DECIMAL(4,2) and cannot accept LONG
                setFloat\terror: [0] bind variable at 0 is defined as DECIMAL(4,2) and cannot accept FLOAT
                setDouble\terror: [0] bind variable at 0 is defined as DECIMAL(4,2) and cannot accept DOUBLE
                setDate\terror: [0] bind variable at 0 is defined as DECIMAL(4,2) and cannot accept DATE
                setTimestamp\terror: [0] bind variable at 0 is defined as DECIMAL(4,2) and cannot accept TIMESTAMP
                setStr\t1.00
                setVarchar\t1.00
                setLong256\terror: [0] bind variable at 0 is defined as DECIMAL(4,2) and cannot accept LONG256
                setUuid\terror: [0] bind variable at 0 is defined as DECIMAL(4,2) and cannot accept UUID
                setArray\terror: [0] bind variable at 0 is defined as DECIMAL(4,2) and cannot accept ARRAY
                ## window_anchor
                error: create view: [147] ANCHOR EXPRESSION must return TIMESTAMP, LONG, or INT; got DECIMAL(4,2)
                """);
        rec("DECIMAL32", """
                ## cast
                target\tmin|max|null
                BOOLEAN\tno cast [10]
                BYTE\terror: inconvertible value: -999999999 [DECIMAL(9,0) -> BYTE]
                SHORT\terror: inconvertible value: -999999999 [DECIMAL(9,0) -> SHORT]
                CHAR\tno cast [10]
                INT\t-999999999|999999999|null
                LONG\t-999999999|999999999|null
                DATE\tno cast [10]
                TIMESTAMP\tno cast [10]
                FLOAT\t-1.0E9|1.0E9|null
                DOUBLE\t-9.99999999E8|9.99999999E8|null
                STRING\t-999999999|999999999|
                SYMBOL\tno cast [10]
                LONG256\tno cast [10]
                GEOBYTE\tno cast [10]
                GEOSHORT\tno cast [10]
                GEOINT\tno cast [10]
                GEOLONG\tno cast [10]
                BINARY\terror: [20] unsupported cast
                UUID\tno cast [10]
                LONG128\terror: [20] unsupported cast
                IPv4\tno cast [10]
                VARCHAR\t-999999999|999999999|
                DOUBLE[]\tno cast [10]
                DECIMAL8\terror: inconvertible value: -999999999 [DECIMAL(9,0) -> DECIMAL(2,1)]
                DECIMAL16\terror: inconvertible value: -999999999 [DECIMAL(9,0) -> DECIMAL(4,2)]
                DECIMAL32\t-999999999|999999999|
                DECIMAL64\t-999999999.0000|999999999.0000|
                DECIMAL128\t-999999999.0000000000|999999999.0000000000|
                DECIMAL256\t-999999999.00000000000000000000|999999999.00000000000000000000|
                INTERVAL\terror: [20] unsupported cast
                VARCHAR_SLICE\terror: [20] unsupported cast
                TIMESTAMP_NS\tno cast [10]
                GEOHASH(1c)\tno cast [10]
                GEOHASH(8b)\tno cast [10]
                GEOHASH(31b)\tno cast [10]
                GEOHASH(12c)\tno cast [10]
                DECIMAL(5,2)\terror: inconvertible value: -999999999 [DECIMAL(9,0) -> DECIMAL(5,2)]
                DECIMAL(18,3)\t-999999999.000|999999999.000|
                DOUBLE[][]\tno cast [10]
                INTERVAL(us)\terror: [20] unsupported cast
                INTERVAL(ns)\terror: [20] unsupported cast
                ## filter_eq
                props: random_access=true size=unknown timestamp=none
                k\tv
                max\t999999999
                ## filter_ne
                props: random_access=true size=unknown timestamp=none
                k\tv
                min\t-999999999
                null\t
                ## filter_null
                props: random_access=true size=unknown timestamp=none
                k\tv
                null\t
                ## filter_not_null
                props: random_access=true size=unknown timestamp=none
                k\tv
                min\t-999999999
                max\t999999999
                ## filter_lt
                props: random_access=true size=unknown timestamp=none
                k\tv
                min\t-999999999
                ## filter_ge
                props: random_access=true size=unknown timestamp=none
                k\tv
                max\t999999999
                ## order_asc
                props: random_access=true size=known timestamp=none
                k\tv
                null\t
                min\t-999999999
                max\t999999999
                ## order_desc
                props: random_access=true size=known timestamp=none
                k\tv
                max\t999999999
                min\t-999999999
                null\t
                ## group_by
                props: random_access=true size=known timestamp=f:asc
                v\tc\tf
                -999999999\t2\t1970-01-01T00:00:00.000000Z
                999999999\t2\t1970-01-01T00:00:01.000000Z
                \t2\t1970-01-01T00:00:02.000000Z
                ## join_inner
                props: random_access=true size=unknown timestamp=none
                tk\tuk\tv
                min\tmin\t-999999999
                null\tnull\t
                ## join_left
                props: random_access=true size=unknown timestamp=none
                tk\tuk\tuv
                max\t\t
                min\tmin\t-999999999
                null\tnull\t
                ## join_left_null
                props: random_access=true size=unknown timestamp=none
                tk\tuv
                max\t
                min\t-999999999
                null\t
                ## union_all
                props: random_access=false size=known timestamp=none
                k\tv
                min\t-999999999
                max\t999999999
                null\t
                min\t-999999999
                null\t
                ## union_null
                props: random_access=false size=known timestamp=none
                k\tv
                min\t-999999999
                null\t
                null_branch\t
                ## case_no_else
                props: random_access=true size=known timestamp=none
                k\tc
                min\t
                max\t999999999
                null\t
                ## case_else
                error: [20] type DECIMAL(9,0) is not supported in 'switch' type of 'case' statement
                ## lag
                props: random_access=false size=known timestamp=none
                k\tv\tp
                min\t-999999999\t
                max\t999999999\t-999999999
                null\t\t999999999
                ## sample_by
                props: random_access=true size=known timestamp=ts:asc
                ts\tf\tl\tc
                1970-01-01T00:00:00.000000Z\t-999999999\t999999999\t2
                1970-01-01T00:00:02.000000Z\t\t\t1
                ## first_last
                props: random_access=true size=known timestamp=none
                k\tf\tl
                A\t\t999999999
                B\t999999999\t
                C\t\t
                ## first_not_null
                props: random_access=true size=known timestamp=none
                k\tf\tl
                A\t999999999\t999999999
                B\t999999999\t999999999
                C\t\t
                ## fill_prev
                props: random_access=false size=unknown timestamp=ts:asc
                ts\tv
                1970-01-01T00:00:00.000000Z\t
                1970-01-01T00:00:01.000000Z\t
                1970-01-01T00:00:02.000000Z\t999999999
                1970-01-01T00:00:03.000000Z\t999999999
                1970-01-01T00:00:04.000000Z\t
                ## latest_on
                error: [47] v (DECIMAL(9,0)): invalid type, only [BOOLEAN, BYTE, SHORT, INT, LONG, DATE, TIMESTAMP, FLOAT, DOUBLE, LONG128, LONG256, CHAR, STRING, VARCHAR, SYMBOL, UUID, GEOHASH, IPv4] are supported in LATEST ON
                ## fill_null
                props: random_access=false size=unknown timestamp=ts:asc
                ts\tv
                1970-01-01T00:00:00.000000Z\t-999999999
                1970-01-01T00:00:01.000000Z\t
                1970-01-01T00:00:02.000000Z\t999999999
                1970-01-01T00:00:03.000000Z\t
                1970-01-01T00:00:04.000000Z\t
                ## fill_value
                props: random_access=false size=unknown timestamp=ts:asc
                ts\tv
                1970-01-01T00:00:00.000000Z\t-999999999
                1970-01-01T00:00:01.000000Z\t999999999
                1970-01-01T00:00:02.000000Z\t999999999
                1970-01-01T00:00:03.000000Z\t999999999
                1970-01-01T00:00:04.000000Z\t
                ## fill_linear
                error: [46] support for LINEAR fill is not yet implemented [function=last(v), class=io.questdb.griffin.engine.functions.groupby.LastDecimalGroupByFunctionFactory$Decimal32Func]
                ## subsample_stride
                min\terror: [50] integer expected for stride
                max\terror: [49] integer expected for stride
                null\terror: [38] integer expected for stride
                ## subsample_target
                min\terror: [50] integer expected for target point count
                max\terror: [49] integer expected for target point count
                null\terror: [38] integer expected for target point count
                ## memoized
                props: random_access=true size=known timestamp=none
                k\ta\ta2\ta3
                min\t-999999999\t-999999999\t-999999999
                max\t999999999\t999999999\t999999999
                null\t\t\t
                plan memoizes: true
                ## where_bound_const
                min\tmin,max,null
                max\t
                null\t
                ## where_bound_bind
                min\tmin,max,null
                max\t
                null\t
                ## where_key
                min\terror: [66] v (DECIMAL(9,0)): invalid type, only [BOOLEAN, BYTE, SHORT, INT, LONG, DATE, TIMESTAMP, FLOAT, DOUBLE, LONG128, LONG256, CHAR, STRING, VARCHAR, SYMBOL, UUID, GEOHASH, IPv4] are supported in LATEST ON
                max\terror: [65] v (DECIMAL(9,0)): invalid type, only [BOOLEAN, BYTE, SHORT, INT, LONG, DATE, TIMESTAMP, FLOAT, DOUBLE, LONG128, LONG256, CHAR, STRING, VARCHAR, SYMBOL, UUID, GEOHASH, IPv4] are supported in LATEST ON
                null\terror: [58] v (DECIMAL(9,0)): invalid type, only [BOOLEAN, BYTE, SHORT, INT, LONG, DATE, TIMESTAMP, FLOAT, DOUBLE, LONG128, LONG256, CHAR, STRING, VARCHAR, SYMBOL, UUID, GEOHASH, IPv4] are supported in LATEST ON
                ## copy_bind
                status\tmessage
                finished\t
                k
                max
                ## between_timestamp
                error: [27] there is no matching operator `between` with the argument type: DECIMAL(9,0)
                ## eq_null_double
                props: random_access=true size=unknown timestamp=none
                k\tv
                null\t
                ## bind_value
                setBoolean\terror: [0] bind variable at 0 is defined as DECIMAL(9,0) and cannot accept BOOLEAN
                setByte\terror: [0] bind variable at 0 is defined as DECIMAL(9,0) and cannot accept BYTE
                setShort\terror: [0] bind variable at 0 is defined as DECIMAL(9,0) and cannot accept SHORT
                setChar\terror: [0] bind variable at 0 is defined as DECIMAL(9,0) and cannot accept CHAR
                setInt\terror: [0] bind variable at 0 is defined as DECIMAL(9,0) and cannot accept INT
                setLong\terror: [0] bind variable at 0 is defined as DECIMAL(9,0) and cannot accept LONG
                setFloat\terror: [0] bind variable at 0 is defined as DECIMAL(9,0) and cannot accept FLOAT
                setDouble\terror: [0] bind variable at 0 is defined as DECIMAL(9,0) and cannot accept DOUBLE
                setDate\terror: [0] bind variable at 0 is defined as DECIMAL(9,0) and cannot accept DATE
                setTimestamp\terror: [0] bind variable at 0 is defined as DECIMAL(9,0) and cannot accept TIMESTAMP
                setStr\t1
                setVarchar\t1
                setLong256\terror: [0] bind variable at 0 is defined as DECIMAL(9,0) and cannot accept LONG256
                setUuid\terror: [0] bind variable at 0 is defined as DECIMAL(9,0) and cannot accept UUID
                setArray\terror: [0] bind variable at 0 is defined as DECIMAL(9,0) and cannot accept ARRAY
                ## window_anchor
                error: create view: [147] ANCHOR EXPRESSION must return TIMESTAMP, LONG, or INT; got DECIMAL(9,0)
                """);
        rec("DECIMAL64", """
                ## filter_eq
                props: random_access=true size=unknown timestamp=none
                k\tv
                max\t999999999999.9999
                ## filter_ne
                props: random_access=true size=unknown timestamp=none
                k\tv
                min\t-999999999999.9999
                null\t
                ## filter_null
                props: random_access=true size=unknown timestamp=none
                k\tv
                null\t
                ## filter_not_null
                props: random_access=true size=unknown timestamp=none
                k\tv
                min\t-999999999999.9999
                max\t999999999999.9999
                ## filter_lt
                props: random_access=true size=unknown timestamp=none
                k\tv
                min\t-999999999999.9999
                ## filter_ge
                props: random_access=true size=unknown timestamp=none
                k\tv
                max\t999999999999.9999
                ## order_asc
                props: random_access=true size=known timestamp=none
                k\tv
                null\t
                min\t-999999999999.9999
                max\t999999999999.9999
                ## order_desc
                props: random_access=true size=known timestamp=none
                k\tv
                max\t999999999999.9999
                min\t-999999999999.9999
                null\t
                ## group_by
                props: random_access=true size=known timestamp=f:asc
                v\tc\tf
                -999999999999.9999\t2\t1970-01-01T00:00:00.000000Z
                999999999999.9999\t2\t1970-01-01T00:00:01.000000Z
                \t2\t1970-01-01T00:00:02.000000Z
                ## join_inner
                props: random_access=true size=unknown timestamp=none
                tk\tuk\tv
                min\tmin\t-999999999999.9999
                null\tnull\t
                ## join_left
                props: random_access=true size=unknown timestamp=none
                tk\tuk\tuv
                max\t\t
                min\tmin\t-999999999999.9999
                null\tnull\t
                ## join_left_null
                props: random_access=true size=unknown timestamp=none
                tk\tuv
                max\t
                min\t-999999999999.9999
                null\t
                ## union_all
                props: random_access=false size=known timestamp=none
                k\tv
                min\t-999999999999.9999
                max\t999999999999.9999
                null\t
                min\t-999999999999.9999
                null\t
                ## union_null
                props: random_access=false size=known timestamp=none
                k\tv
                min\t-999999999999.9999
                null\t
                null_branch\t
                ## case_no_else
                props: random_access=true size=known timestamp=none
                k\tc
                min\t
                max\t999999999999.9999
                null\t
                ## case_else
                error: [20] type DECIMAL(16,4) is not supported in 'switch' type of 'case' statement
                ## lag
                props: random_access=false size=known timestamp=none
                k\tv\tp
                min\t-999999999999.9999\t
                max\t999999999999.9999\t-999999999999.9999
                null\t\t999999999999.9999
                ## sample_by
                props: random_access=true size=known timestamp=ts:asc
                ts\tf\tl\tc
                1970-01-01T00:00:00.000000Z\t-999999999999.9999\t999999999999.9999\t2
                1970-01-01T00:00:02.000000Z\t\t\t1
                ## first_last
                props: random_access=true size=known timestamp=none
                k\tf\tl
                A\t\t999999999999.9999
                B\t999999999999.9999\t
                C\t\t
                ## first_not_null
                props: random_access=true size=known timestamp=none
                k\tf\tl
                A\t999999999999.9999\t999999999999.9999
                B\t999999999999.9999\t999999999999.9999
                C\t\t
                ## fill_prev
                props: random_access=false size=unknown timestamp=ts:asc
                ts\tv
                1970-01-01T00:00:00.000000Z\t
                1970-01-01T00:00:01.000000Z\t
                1970-01-01T00:00:02.000000Z\t999999999999.9999
                1970-01-01T00:00:03.000000Z\t999999999999.9999
                1970-01-01T00:00:04.000000Z\t
                ## latest_on
                error: [47] v (DECIMAL(16,4)): invalid type, only [BOOLEAN, BYTE, SHORT, INT, LONG, DATE, TIMESTAMP, FLOAT, DOUBLE, LONG128, LONG256, CHAR, STRING, VARCHAR, SYMBOL, UUID, GEOHASH, IPv4] are supported in LATEST ON
                ## cast
                target\tmin|max|null
                BOOLEAN\tno cast [10]
                BYTE\terror: inconvertible value: -999999999999.9999 [DECIMAL(16,4) -> BYTE]
                SHORT\terror: inconvertible value: -999999999999.9999 [DECIMAL(16,4) -> SHORT]
                CHAR\tno cast [10]
                INT\terror: inconvertible value: -999999999999.9999 [DECIMAL(16,4) -> INT]
                LONG\t-999999999999|999999999999|null
                DATE\tno cast [10]
                TIMESTAMP\tno cast [10]
                FLOAT\t-1.0E12|1.0E12|null
                DOUBLE\t-9.999999999999999E11|9.999999999999999E11|null
                STRING\t-999999999999.9999|999999999999.9999|
                SYMBOL\tno cast [10]
                LONG256\tno cast [10]
                GEOBYTE\tno cast [10]
                GEOSHORT\tno cast [10]
                GEOINT\tno cast [10]
                GEOLONG\tno cast [10]
                BINARY\terror: [20] unsupported cast
                UUID\tno cast [10]
                LONG128\terror: [20] unsupported cast
                IPv4\tno cast [10]
                VARCHAR\t-999999999999.9999|999999999999.9999|
                DOUBLE[]\tno cast [10]
                DECIMAL8\terror: inconvertible value: -999999999999.9999 [DECIMAL(16,4) -> DECIMAL(2,1)]
                DECIMAL16\terror: inconvertible value: -999999999999.9999 [DECIMAL(16,4) -> DECIMAL(4,2)]
                DECIMAL32\terror: inconvertible value: -999999999999.9999 [DECIMAL(16,4) -> DECIMAL(9,0)]
                DECIMAL64\t-999999999999.9999|999999999999.9999|
                DECIMAL128\t-999999999999.9999000000|999999999999.9999000000|
                DECIMAL256\t-999999999999.99990000000000000000|999999999999.99990000000000000000|
                INTERVAL\terror: [20] unsupported cast
                VARCHAR_SLICE\terror: [20] unsupported cast
                TIMESTAMP_NS\tno cast [10]
                GEOHASH(1c)\tno cast [10]
                GEOHASH(8b)\tno cast [10]
                GEOHASH(31b)\tno cast [10]
                GEOHASH(12c)\tno cast [10]
                DECIMAL(5,2)\terror: inconvertible value: -999999999999.9999 [DECIMAL(16,4) -> DECIMAL(5,2)]
                DECIMAL(18,3)\terror: inconvertible value: -999999999999.9999 [DECIMAL(16,4) -> DECIMAL(18,3)]
                DOUBLE[][]\tno cast [10]
                INTERVAL(us)\terror: [20] unsupported cast
                INTERVAL(ns)\terror: [20] unsupported cast
                ## fill_null
                props: random_access=false size=unknown timestamp=ts:asc
                ts\tv
                1970-01-01T00:00:00.000000Z\t-999999999999.9999
                1970-01-01T00:00:01.000000Z\t
                1970-01-01T00:00:02.000000Z\t999999999999.9999
                1970-01-01T00:00:03.000000Z\t
                1970-01-01T00:00:04.000000Z\t
                ## fill_value
                error: [46] fill value of type DOUBLE cannot fill column of type DECIMAL(16,4)
                ## fill_linear
                error: [46] support for LINEAR fill is not yet implemented [function=last(v), class=io.questdb.griffin.engine.functions.groupby.LastDecimalGroupByFunctionFactory$Decimal64Func]
                ## subsample_stride
                min\terror: [58] integer expected for stride
                max\terror: [57] integer expected for stride
                null\terror: [38] integer expected for stride
                ## subsample_target
                min\terror: [58] integer expected for target point count
                max\terror: [57] integer expected for target point count
                null\terror: [38] integer expected for target point count
                ## memoized
                props: random_access=true size=known timestamp=none
                k\ta\ta2\ta3
                min\t-999999999999.9999\t-999999999999.9999\t-999999999999.9999
                max\t999999999999.9999\t999999999999.9999\t999999999999.9999
                null\t\t\t
                plan memoizes: true
                ## where_bound_const
                min\tmin,max,null
                max\t
                null\t
                ## where_bound_bind
                min\tmin,max,null
                max\t
                null\t
                ## where_key
                min\terror: [74] v (DECIMAL(16,4)): invalid type, only [BOOLEAN, BYTE, SHORT, INT, LONG, DATE, TIMESTAMP, FLOAT, DOUBLE, LONG128, LONG256, CHAR, STRING, VARCHAR, SYMBOL, UUID, GEOHASH, IPv4] are supported in LATEST ON
                max\terror: [73] v (DECIMAL(16,4)): invalid type, only [BOOLEAN, BYTE, SHORT, INT, LONG, DATE, TIMESTAMP, FLOAT, DOUBLE, LONG128, LONG256, CHAR, STRING, VARCHAR, SYMBOL, UUID, GEOHASH, IPv4] are supported in LATEST ON
                null\terror: [58] v (DECIMAL(16,4)): invalid type, only [BOOLEAN, BYTE, SHORT, INT, LONG, DATE, TIMESTAMP, FLOAT, DOUBLE, LONG128, LONG256, CHAR, STRING, VARCHAR, SYMBOL, UUID, GEOHASH, IPv4] are supported in LATEST ON
                ## copy_bind
                status\tmessage
                finished\t
                k
                max
                ## between_timestamp
                error: [27] there is no matching operator `between` with the argument type: DECIMAL(16,4)
                ## eq_null_double
                props: random_access=true size=unknown timestamp=none
                k\tv
                null\t
                ## bind_value
                setBoolean\terror: [0] bind variable at 0 is defined as DECIMAL(16,4) and cannot accept BOOLEAN
                setByte\terror: [0] bind variable at 0 is defined as DECIMAL(16,4) and cannot accept BYTE
                setShort\terror: [0] bind variable at 0 is defined as DECIMAL(16,4) and cannot accept SHORT
                setChar\terror: [0] bind variable at 0 is defined as DECIMAL(16,4) and cannot accept CHAR
                setInt\terror: [0] bind variable at 0 is defined as DECIMAL(16,4) and cannot accept INT
                setLong\terror: [0] bind variable at 0 is defined as DECIMAL(16,4) and cannot accept LONG
                setFloat\terror: [0] bind variable at 0 is defined as DECIMAL(16,4) and cannot accept FLOAT
                setDouble\terror: [0] bind variable at 0 is defined as DECIMAL(16,4) and cannot accept DOUBLE
                setDate\terror: [0] bind variable at 0 is defined as DECIMAL(16,4) and cannot accept DATE
                setTimestamp\terror: [0] bind variable at 0 is defined as DECIMAL(16,4) and cannot accept TIMESTAMP
                setStr\t1.0000
                setVarchar\t1.0000
                setLong256\terror: [0] bind variable at 0 is defined as DECIMAL(16,4) and cannot accept LONG256
                setUuid\terror: [0] bind variable at 0 is defined as DECIMAL(16,4) and cannot accept UUID
                setArray\terror: [0] bind variable at 0 is defined as DECIMAL(16,4) and cannot accept ARRAY
                ## window_anchor
                error: create view: [147] ANCHOR EXPRESSION must return TIMESTAMP, LONG, or INT; got DECIMAL(16,4)
                """);
        rec("DECIMAL128", """
                ## filter_eq
                props: random_access=true size=unknown timestamp=none
                k\tv
                max\t9999999999999999999999999999.9999999999
                ## filter_ne
                props: random_access=true size=unknown timestamp=none
                k\tv
                min\t-9999999999999999999999999999.9999999999
                null\t
                ## filter_null
                props: random_access=true size=unknown timestamp=none
                k\tv
                null\t
                ## filter_not_null
                props: random_access=true size=unknown timestamp=none
                k\tv
                min\t-9999999999999999999999999999.9999999999
                max\t9999999999999999999999999999.9999999999
                ## filter_lt
                props: random_access=true size=unknown timestamp=none
                k\tv
                min\t-9999999999999999999999999999.9999999999
                ## filter_ge
                props: random_access=true size=unknown timestamp=none
                k\tv
                max\t9999999999999999999999999999.9999999999
                ## order_asc
                props: random_access=true size=known timestamp=none
                k\tv
                null\t
                min\t-9999999999999999999999999999.9999999999
                max\t9999999999999999999999999999.9999999999
                ## order_desc
                props: random_access=true size=known timestamp=none
                k\tv
                max\t9999999999999999999999999999.9999999999
                min\t-9999999999999999999999999999.9999999999
                null\t
                ## group_by
                props: random_access=true size=known timestamp=f:asc
                v\tc\tf
                -9999999999999999999999999999.9999999999\t2\t1970-01-01T00:00:00.000000Z
                9999999999999999999999999999.9999999999\t2\t1970-01-01T00:00:01.000000Z
                \t2\t1970-01-01T00:00:02.000000Z
                ## join_inner
                props: random_access=true size=unknown timestamp=none
                tk\tuk\tv
                min\tmin\t-9999999999999999999999999999.9999999999
                null\tnull\t
                ## join_left
                props: random_access=true size=unknown timestamp=none
                tk\tuk\tuv
                max\t\t
                min\tmin\t-9999999999999999999999999999.9999999999
                null\tnull\t
                ## join_left_null
                props: random_access=true size=unknown timestamp=none
                tk\tuv
                max\t
                min\t-9999999999999999999999999999.9999999999
                null\t
                ## union_all
                props: random_access=false size=known timestamp=none
                k\tv
                min\t-9999999999999999999999999999.9999999999
                max\t9999999999999999999999999999.9999999999
                null\t
                min\t-9999999999999999999999999999.9999999999
                null\t
                ## union_null
                props: random_access=false size=known timestamp=none
                k\tv
                min\t-9999999999999999999999999999.9999999999
                null\t
                null_branch\t
                ## case_no_else
                props: random_access=true size=known timestamp=none
                k\tc
                min\t
                max\t9999999999999999999999999999.9999999999
                null\t
                ## case_else
                error: [20] type DECIMAL(38,10) is not supported in 'switch' type of 'case' statement
                ## lag
                props: random_access=false size=known timestamp=none
                k\tv\tp
                min\t-9999999999999999999999999999.9999999999\t
                max\t9999999999999999999999999999.9999999999\t-9999999999999999999999999999.9999999999
                null\t\t9999999999999999999999999999.9999999999
                ## sample_by
                props: random_access=true size=known timestamp=ts:asc
                ts\tf\tl\tc
                1970-01-01T00:00:00.000000Z\t-9999999999999999999999999999.9999999999\t9999999999999999999999999999.9999999999\t2
                1970-01-01T00:00:02.000000Z\t\t\t1
                ## first_last
                props: random_access=true size=known timestamp=none
                k\tf\tl
                A\t\t9999999999999999999999999999.9999999999
                B\t9999999999999999999999999999.9999999999\t
                C\t\t
                ## first_not_null
                props: random_access=true size=known timestamp=none
                k\tf\tl
                A\t9999999999999999999999999999.9999999999\t9999999999999999999999999999.9999999999
                B\t9999999999999999999999999999.9999999999\t9999999999999999999999999999.9999999999
                C\t\t
                ## fill_prev
                props: random_access=false size=unknown timestamp=ts:asc
                ts\tv
                1970-01-01T00:00:00.000000Z\t
                1970-01-01T00:00:01.000000Z\t
                1970-01-01T00:00:02.000000Z\t9999999999999999999999999999.9999999999
                1970-01-01T00:00:03.000000Z\t9999999999999999999999999999.9999999999
                1970-01-01T00:00:04.000000Z\t
                ## latest_on
                error: [47] v (DECIMAL(38,10)): invalid type, only [BOOLEAN, BYTE, SHORT, INT, LONG, DATE, TIMESTAMP, FLOAT, DOUBLE, LONG128, LONG256, CHAR, STRING, VARCHAR, SYMBOL, UUID, GEOHASH, IPv4] are supported in LATEST ON
                ## cast
                target\tmin|max|null
                BOOLEAN\tno cast [10]
                BYTE\terror: inconvertible value: -9999999999999999999999999999.9999999999 [DECIMAL(38,10) -> BYTE]
                SHORT\terror: inconvertible value: -9999999999999999999999999999.9999999999 [DECIMAL(38,10) -> SHORT]
                CHAR\tno cast [10]
                INT\terror: inconvertible value: -9999999999999999999999999999.9999999999 [DECIMAL(38,10) -> INT]
                LONG\terror: inconvertible value: -9999999999999999999999999999.9999999999 [DECIMAL(38,10) -> LONG]
                DATE\tno cast [10]
                TIMESTAMP\tno cast [10]
                FLOAT\t-1.0E28|1.0E28|null
                DOUBLE\t-1.0E28|1.0E28|null
                STRING\t-9999999999999999999999999999.9999999999|9999999999999999999999999999.9999999999|
                SYMBOL\tno cast [10]
                LONG256\tno cast [10]
                GEOBYTE\tno cast [10]
                GEOSHORT\tno cast [10]
                GEOINT\tno cast [10]
                GEOLONG\tno cast [10]
                BINARY\terror: [20] unsupported cast
                UUID\tno cast [10]
                LONG128\terror: [20] unsupported cast
                IPv4\tno cast [10]
                VARCHAR\t-9999999999999999999999999999.9999999999|9999999999999999999999999999.9999999999|
                DOUBLE[]\tno cast [10]
                DECIMAL8\terror: inconvertible value: -9999999999999999999999999999.9999999999 [DECIMAL(38,10) -> DECIMAL(2,1)]
                DECIMAL16\terror: inconvertible value: -9999999999999999999999999999.9999999999 [DECIMAL(38,10) -> DECIMAL(4,2)]
                DECIMAL32\terror: inconvertible value: -9999999999999999999999999999.9999999999 [DECIMAL(38,10) -> DECIMAL(9,0)]
                DECIMAL64\terror: inconvertible value: -9999999999999999999999999999.9999999999 [DECIMAL(38,10) -> DECIMAL(16,4)]
                DECIMAL128\t-9999999999999999999999999999.9999999999|9999999999999999999999999999.9999999999|
                DECIMAL256\t-9999999999999999999999999999.99999999990000000000|9999999999999999999999999999.99999999990000000000|
                INTERVAL\terror: [20] unsupported cast
                VARCHAR_SLICE\terror: [20] unsupported cast
                TIMESTAMP_NS\tno cast [10]
                GEOHASH(1c)\tno cast [10]
                GEOHASH(8b)\tno cast [10]
                GEOHASH(31b)\tno cast [10]
                GEOHASH(12c)\tno cast [10]
                DECIMAL(5,2)\terror: inconvertible value: -9999999999999999999999999999.9999999999 [DECIMAL(38,10) -> DECIMAL(5,2)]
                DECIMAL(18,3)\terror: inconvertible value: -9999999999999999999999999999.9999999999 [DECIMAL(38,10) -> DECIMAL(18,3)]
                DOUBLE[][]\tno cast [10]
                INTERVAL(us)\terror: [20] unsupported cast
                INTERVAL(ns)\terror: [20] unsupported cast
                ## fill_null
                props: random_access=false size=unknown timestamp=ts:asc
                ts\tv
                1970-01-01T00:00:00.000000Z\t-9999999999999999999999999999.9999999999
                1970-01-01T00:00:01.000000Z\t
                1970-01-01T00:00:02.000000Z\t9999999999999999999999999999.9999999999
                1970-01-01T00:00:03.000000Z\t
                1970-01-01T00:00:04.000000Z\t
                ## fill_value
                error: [46] fill value of type DOUBLE cannot fill column of type DECIMAL(38,10)
                ## fill_linear
                error: [46] support for LINEAR fill is not yet implemented [function=last(v), class=io.questdb.griffin.engine.functions.groupby.LastDecimalGroupByFunctionFactory$Decimal128Func]
                ## subsample_stride
                min\terror: [80] integer expected for stride
                max\terror: [79] integer expected for stride
                null\terror: [38] integer expected for stride
                ## subsample_target
                min\terror: [80] integer expected for target point count
                max\terror: [79] integer expected for target point count
                null\terror: [38] integer expected for target point count
                ## memoized
                props: random_access=true size=known timestamp=none
                k\ta\ta2\ta3
                min\t-9999999999999999999999999999.9999999999\t-9999999999999999999999999999.9999999999\t-9999999999999999999999999999.9999999999
                max\t9999999999999999999999999999.9999999999\t9999999999999999999999999999.9999999999\t9999999999999999999999999999.9999999999
                null\t\t\t
                plan memoizes: true
                ## where_bound_const
                min\tmin,max,null
                max\t
                null\t
                ## where_bound_bind
                min\tmin,max,null
                max\t
                null\t
                ## where_key
                min\terror: [96] v (DECIMAL(38,10)): invalid type, only [BOOLEAN, BYTE, SHORT, INT, LONG, DATE, TIMESTAMP, FLOAT, DOUBLE, LONG128, LONG256, CHAR, STRING, VARCHAR, SYMBOL, UUID, GEOHASH, IPv4] are supported in LATEST ON
                max\terror: [95] v (DECIMAL(38,10)): invalid type, only [BOOLEAN, BYTE, SHORT, INT, LONG, DATE, TIMESTAMP, FLOAT, DOUBLE, LONG128, LONG256, CHAR, STRING, VARCHAR, SYMBOL, UUID, GEOHASH, IPv4] are supported in LATEST ON
                null\terror: [58] v (DECIMAL(38,10)): invalid type, only [BOOLEAN, BYTE, SHORT, INT, LONG, DATE, TIMESTAMP, FLOAT, DOUBLE, LONG128, LONG256, CHAR, STRING, VARCHAR, SYMBOL, UUID, GEOHASH, IPv4] are supported in LATEST ON
                ## copy_bind
                status\tmessage
                finished\t
                k
                max
                ## between_timestamp
                error: [27] there is no matching operator `between` with the argument type: DECIMAL(38,10)
                ## eq_null_double
                props: random_access=true size=unknown timestamp=none
                k\tv
                null\t
                ## bind_value
                setBoolean\terror: [0] bind variable at 0 is defined as DECIMAL(38,10) and cannot accept BOOLEAN
                setByte\terror: [0] bind variable at 0 is defined as DECIMAL(38,10) and cannot accept BYTE
                setShort\terror: [0] bind variable at 0 is defined as DECIMAL(38,10) and cannot accept SHORT
                setChar\terror: [0] bind variable at 0 is defined as DECIMAL(38,10) and cannot accept CHAR
                setInt\terror: [0] bind variable at 0 is defined as DECIMAL(38,10) and cannot accept INT
                setLong\terror: [0] bind variable at 0 is defined as DECIMAL(38,10) and cannot accept LONG
                setFloat\terror: [0] bind variable at 0 is defined as DECIMAL(38,10) and cannot accept FLOAT
                setDouble\terror: [0] bind variable at 0 is defined as DECIMAL(38,10) and cannot accept DOUBLE
                setDate\terror: [0] bind variable at 0 is defined as DECIMAL(38,10) and cannot accept DATE
                setTimestamp\terror: [0] bind variable at 0 is defined as DECIMAL(38,10) and cannot accept TIMESTAMP
                setStr\t1.0000000000
                setVarchar\t1.0000000000
                setLong256\terror: [0] bind variable at 0 is defined as DECIMAL(38,10) and cannot accept LONG256
                setUuid\terror: [0] bind variable at 0 is defined as DECIMAL(38,10) and cannot accept UUID
                setArray\terror: [0] bind variable at 0 is defined as DECIMAL(38,10) and cannot accept ARRAY
                ## window_anchor
                error: create view: [147] ANCHOR EXPRESSION must return TIMESTAMP, LONG, or INT; got DECIMAL(38,10)
                """);
        rec("DECIMAL256", """
                ## cast
                target\tmin|max|null
                BOOLEAN\tno cast [10]
                BYTE\terror: inconvertible value: -99999999999999999999999999999999999999999999999999999999.99999999999999999999 [DECIMAL(76,20) -> BYTE]
                SHORT\terror: inconvertible value: -99999999999999999999999999999999999999999999999999999999.99999999999999999999 [DECIMAL(76,20) -> SHORT]
                CHAR\tno cast [10]
                INT\terror: inconvertible value: -99999999999999999999999999999999999999999999999999999999.99999999999999999999 [DECIMAL(76,20) -> INT]
                LONG\terror: inconvertible value: -99999999999999999999999999999999999999999999999999999999.99999999999999999999 [DECIMAL(76,20) -> LONG]
                DATE\tno cast [10]
                TIMESTAMP\tno cast [10]
                FLOAT\tnull|null|null
                DOUBLE\t-1.0E56|1.0E56|null
                STRING\t-99999999999999999999999999999999999999999999999999999999.99999999999999999999|99999999999999999999999999999999999999999999999999999999.99999999999999999999|
                SYMBOL\tno cast [10]
                LONG256\tno cast [10]
                GEOBYTE\tno cast [10]
                GEOSHORT\tno cast [10]
                GEOINT\tno cast [10]
                GEOLONG\tno cast [10]
                BINARY\terror: [20] unsupported cast
                UUID\tno cast [10]
                LONG128\terror: [20] unsupported cast
                IPv4\tno cast [10]
                VARCHAR\t-99999999999999999999999999999999999999999999999999999999.99999999999999999999|99999999999999999999999999999999999999999999999999999999.99999999999999999999|
                DOUBLE[]\tno cast [10]
                DECIMAL8\terror: inconvertible value: -99999999999999999999999999999999999999999999999999999999.99999999999999999999 [DECIMAL(76,20) -> DECIMAL(2,1)]
                DECIMAL16\terror: inconvertible value: -99999999999999999999999999999999999999999999999999999999.99999999999999999999 [DECIMAL(76,20) -> DECIMAL(4,2)]
                DECIMAL32\terror: inconvertible value: -99999999999999999999999999999999999999999999999999999999.99999999999999999999 [DECIMAL(76,20) -> DECIMAL(9,0)]
                DECIMAL64\terror: inconvertible value: -99999999999999999999999999999999999999999999999999999999.99999999999999999999 [DECIMAL(76,20) -> DECIMAL(16,4)]
                DECIMAL128\terror: inconvertible value: -99999999999999999999999999999999999999999999999999999999.99999999999999999999 [DECIMAL(76,20) -> DECIMAL(38,10)]
                DECIMAL256\t-99999999999999999999999999999999999999999999999999999999.99999999999999999999|99999999999999999999999999999999999999999999999999999999.99999999999999999999|
                INTERVAL\terror: [20] unsupported cast
                VARCHAR_SLICE\terror: [20] unsupported cast
                TIMESTAMP_NS\tno cast [10]
                GEOHASH(1c)\tno cast [10]
                GEOHASH(8b)\tno cast [10]
                GEOHASH(31b)\tno cast [10]
                GEOHASH(12c)\tno cast [10]
                DECIMAL(5,2)\terror: inconvertible value: -99999999999999999999999999999999999999999999999999999999.99999999999999999999 [DECIMAL(76,20) -> DECIMAL(5,2)]
                DECIMAL(18,3)\terror: inconvertible value: -99999999999999999999999999999999999999999999999999999999.99999999999999999999 [DECIMAL(76,20) -> DECIMAL(18,3)]
                DOUBLE[][]\tno cast [10]
                INTERVAL(us)\terror: [20] unsupported cast
                INTERVAL(ns)\terror: [20] unsupported cast
                ## filter_eq
                props: random_access=true size=unknown timestamp=none
                k\tv
                max\t99999999999999999999999999999999999999999999999999999999.99999999999999999999
                ## filter_ne
                props: random_access=true size=unknown timestamp=none
                k\tv
                min\t-99999999999999999999999999999999999999999999999999999999.99999999999999999999
                null\t
                ## filter_null
                props: random_access=true size=unknown timestamp=none
                k\tv
                null\t
                ## filter_not_null
                props: random_access=true size=unknown timestamp=none
                k\tv
                min\t-99999999999999999999999999999999999999999999999999999999.99999999999999999999
                max\t99999999999999999999999999999999999999999999999999999999.99999999999999999999
                ## filter_lt
                props: random_access=true size=unknown timestamp=none
                k\tv
                min\t-99999999999999999999999999999999999999999999999999999999.99999999999999999999
                ## filter_ge
                props: random_access=true size=unknown timestamp=none
                k\tv
                max\t99999999999999999999999999999999999999999999999999999999.99999999999999999999
                ## order_asc
                props: random_access=true size=known timestamp=none
                k\tv
                null\t
                min\t-99999999999999999999999999999999999999999999999999999999.99999999999999999999
                max\t99999999999999999999999999999999999999999999999999999999.99999999999999999999
                ## order_desc
                props: random_access=true size=known timestamp=none
                k\tv
                max\t99999999999999999999999999999999999999999999999999999999.99999999999999999999
                min\t-99999999999999999999999999999999999999999999999999999999.99999999999999999999
                null\t
                ## group_by
                props: random_access=true size=known timestamp=f:asc
                v\tc\tf
                -99999999999999999999999999999999999999999999999999999999.99999999999999999999\t2\t1970-01-01T00:00:00.000000Z
                99999999999999999999999999999999999999999999999999999999.99999999999999999999\t2\t1970-01-01T00:00:01.000000Z
                \t2\t1970-01-01T00:00:02.000000Z
                ## join_inner
                props: random_access=true size=unknown timestamp=none
                tk\tuk\tv
                min\tmin\t-99999999999999999999999999999999999999999999999999999999.99999999999999999999
                null\tnull\t
                ## join_left
                props: random_access=true size=unknown timestamp=none
                tk\tuk\tuv
                max\t\t
                min\tmin\t-99999999999999999999999999999999999999999999999999999999.99999999999999999999
                null\tnull\t
                ## join_left_null
                props: random_access=true size=unknown timestamp=none
                tk\tuv
                max\t
                min\t-99999999999999999999999999999999999999999999999999999999.99999999999999999999
                null\t
                ## union_all
                props: random_access=false size=known timestamp=none
                k\tv
                min\t-99999999999999999999999999999999999999999999999999999999.99999999999999999999
                max\t99999999999999999999999999999999999999999999999999999999.99999999999999999999
                null\t
                min\t-99999999999999999999999999999999999999999999999999999999.99999999999999999999
                null\t
                ## union_null
                props: random_access=false size=known timestamp=none
                k\tv
                min\t-99999999999999999999999999999999999999999999999999999999.99999999999999999999
                null\t
                null_branch\t
                ## case_no_else
                props: random_access=true size=known timestamp=none
                k\tc
                min\t
                max\t99999999999999999999999999999999999999999999999999999999.99999999999999999999
                null\t
                ## case_else
                error: [20] type DECIMAL(76,20) is not supported in 'switch' type of 'case' statement
                ## lag
                props: random_access=false size=known timestamp=none
                k\tv\tp
                min\t-99999999999999999999999999999999999999999999999999999999.99999999999999999999\t
                max\t99999999999999999999999999999999999999999999999999999999.99999999999999999999\t-99999999999999999999999999999999999999999999999999999999.99999999999999999999
                null\t\t99999999999999999999999999999999999999999999999999999999.99999999999999999999
                ## sample_by
                props: random_access=true size=known timestamp=ts:asc
                ts\tf\tl\tc
                1970-01-01T00:00:00.000000Z\t-99999999999999999999999999999999999999999999999999999999.99999999999999999999\t99999999999999999999999999999999999999999999999999999999.99999999999999999999\t2
                1970-01-01T00:00:02.000000Z\t\t\t1
                ## first_last
                props: random_access=true size=known timestamp=none
                k\tf\tl
                A\t\t99999999999999999999999999999999999999999999999999999999.99999999999999999999
                B\t99999999999999999999999999999999999999999999999999999999.99999999999999999999\t
                C\t\t
                ## first_not_null
                props: random_access=true size=known timestamp=none
                k\tf\tl
                A\t99999999999999999999999999999999999999999999999999999999.99999999999999999999\t99999999999999999999999999999999999999999999999999999999.99999999999999999999
                B\t99999999999999999999999999999999999999999999999999999999.99999999999999999999\t99999999999999999999999999999999999999999999999999999999.99999999999999999999
                C\t\t
                ## fill_prev
                props: random_access=false size=unknown timestamp=ts:asc
                ts\tv
                1970-01-01T00:00:00.000000Z\t
                1970-01-01T00:00:01.000000Z\t
                1970-01-01T00:00:02.000000Z\t99999999999999999999999999999999999999999999999999999999.99999999999999999999
                1970-01-01T00:00:03.000000Z\t99999999999999999999999999999999999999999999999999999999.99999999999999999999
                1970-01-01T00:00:04.000000Z\t
                ## latest_on
                error: [47] v (DECIMAL(76,20)): invalid type, only [BOOLEAN, BYTE, SHORT, INT, LONG, DATE, TIMESTAMP, FLOAT, DOUBLE, LONG128, LONG256, CHAR, STRING, VARCHAR, SYMBOL, UUID, GEOHASH, IPv4] are supported in LATEST ON
                ## fill_null
                props: random_access=false size=unknown timestamp=ts:asc
                ts\tv
                1970-01-01T00:00:00.000000Z\t-99999999999999999999999999999999999999999999999999999999.99999999999999999999
                1970-01-01T00:00:01.000000Z\t
                1970-01-01T00:00:02.000000Z\t99999999999999999999999999999999999999999999999999999999.99999999999999999999
                1970-01-01T00:00:03.000000Z\t
                1970-01-01T00:00:04.000000Z\t
                ## fill_value
                error: [46] fill value of type DOUBLE cannot fill column of type DECIMAL(76,20)
                ## fill_linear
                error: [46] support for LINEAR fill is not yet implemented [function=last(v), class=io.questdb.griffin.engine.functions.groupby.LastDecimalGroupByFunctionFactory$Decimal256Func]
                ## subsample_stride
                min\terror: [118] integer expected for stride
                max\terror: [117] integer expected for stride
                null\terror: [38] integer expected for stride
                ## subsample_target
                min\terror: [118] integer expected for target point count
                max\terror: [117] integer expected for target point count
                null\terror: [38] integer expected for target point count
                ## memoized
                props: random_access=true size=known timestamp=none
                k\ta\ta2\ta3
                min\t-99999999999999999999999999999999999999999999999999999999.99999999999999999999\t-99999999999999999999999999999999999999999999999999999999.99999999999999999999\t-99999999999999999999999999999999999999999999999999999999.99999999999999999999
                max\t99999999999999999999999999999999999999999999999999999999.99999999999999999999\t99999999999999999999999999999999999999999999999999999999.99999999999999999999\t99999999999999999999999999999999999999999999999999999999.99999999999999999999
                null\t\t\t
                plan memoizes: true
                ## where_bound_const
                min\tmin,max,null
                max\t
                null\t
                ## where_bound_bind
                min\tmin,max,null
                max\t
                null\t
                ## where_key
                min\terror: [134] v (DECIMAL(76,20)): invalid type, only [BOOLEAN, BYTE, SHORT, INT, LONG, DATE, TIMESTAMP, FLOAT, DOUBLE, LONG128, LONG256, CHAR, STRING, VARCHAR, SYMBOL, UUID, GEOHASH, IPv4] are supported in LATEST ON
                max\terror: [133] v (DECIMAL(76,20)): invalid type, only [BOOLEAN, BYTE, SHORT, INT, LONG, DATE, TIMESTAMP, FLOAT, DOUBLE, LONG128, LONG256, CHAR, STRING, VARCHAR, SYMBOL, UUID, GEOHASH, IPv4] are supported in LATEST ON
                null\terror: [58] v (DECIMAL(76,20)): invalid type, only [BOOLEAN, BYTE, SHORT, INT, LONG, DATE, TIMESTAMP, FLOAT, DOUBLE, LONG128, LONG256, CHAR, STRING, VARCHAR, SYMBOL, UUID, GEOHASH, IPv4] are supported in LATEST ON
                ## copy_bind
                status\tmessage
                finished\t
                k
                max
                ## between_timestamp
                error: [27] there is no matching operator `between` with the argument type: DECIMAL(76,20)
                ## eq_null_double
                props: random_access=true size=unknown timestamp=none
                k\tv
                null\t
                ## bind_value
                setBoolean\terror: [0] bind variable at 0 is defined as DECIMAL(76,20) and cannot accept BOOLEAN
                setByte\terror: [0] bind variable at 0 is defined as DECIMAL(76,20) and cannot accept BYTE
                setShort\terror: [0] bind variable at 0 is defined as DECIMAL(76,20) and cannot accept SHORT
                setChar\terror: [0] bind variable at 0 is defined as DECIMAL(76,20) and cannot accept CHAR
                setInt\terror: [0] bind variable at 0 is defined as DECIMAL(76,20) and cannot accept INT
                setLong\terror: [0] bind variable at 0 is defined as DECIMAL(76,20) and cannot accept LONG
                setFloat\terror: [0] bind variable at 0 is defined as DECIMAL(76,20) and cannot accept FLOAT
                setDouble\terror: [0] bind variable at 0 is defined as DECIMAL(76,20) and cannot accept DOUBLE
                setDate\terror: [0] bind variable at 0 is defined as DECIMAL(76,20) and cannot accept DATE
                setTimestamp\terror: [0] bind variable at 0 is defined as DECIMAL(76,20) and cannot accept TIMESTAMP
                setStr\t1.00000000000000000000
                setVarchar\t1.00000000000000000000
                setLong256\terror: [0] bind variable at 0 is defined as DECIMAL(76,20) and cannot accept LONG256
                setUuid\terror: [0] bind variable at 0 is defined as DECIMAL(76,20) and cannot accept UUID
                setArray\terror: [0] bind variable at 0 is defined as DECIMAL(76,20) and cannot accept ARRAY
                ## window_anchor
                error: create view: [147] ANCHOR EXPRESSION must return TIMESTAMP, LONG, or INT; got DECIMAL(76,20)
                """);
        rec("INTERVAL", """
                ## filter_eq
                error: create: [29] non-persisted type: INTERVAL
                ## filter_ne
                error: create: [29] non-persisted type: INTERVAL
                ## filter_null
                error: create: [29] non-persisted type: INTERVAL
                ## filter_not_null
                error: create: [29] non-persisted type: INTERVAL
                ## filter_lt
                error: create: [29] non-persisted type: INTERVAL
                ## filter_ge
                error: create: [29] non-persisted type: INTERVAL
                ## order_asc
                error: create: [29] non-persisted type: INTERVAL
                ## order_desc
                error: create: [29] non-persisted type: INTERVAL
                ## group_by
                error: create: [29] non-persisted type: INTERVAL
                ## join_inner
                error: create: [29] non-persisted type: INTERVAL
                ## join_left
                error: create: [29] non-persisted type: INTERVAL
                ## join_left_null
                error: create: [29] non-persisted type: INTERVAL
                ## union_all
                error: create: [29] non-persisted type: INTERVAL
                ## union_null
                error: create: [29] non-persisted type: INTERVAL
                ## case_no_else
                error: create: [29] non-persisted type: INTERVAL
                ## case_else
                error: create: [29] non-persisted type: INTERVAL
                ## lag
                error: create: [29] non-persisted type: INTERVAL
                ## sample_by
                error: create: [29] non-persisted type: INTERVAL
                ## first_last
                error: create: [29] non-persisted type: INTERVAL
                ## first_not_null
                error: create: [29] non-persisted type: INTERVAL
                ## fill_prev
                error: create: [29] non-persisted type: INTERVAL
                ## latest_on
                error: create: [29] non-persisted type: INTERVAL
                ## cast
                error: create: [29] non-persisted type: INTERVAL
                ## fill_null
                error: create: [29] non-persisted type: INTERVAL
                ## fill_value
                error: create: [29] non-persisted type: INTERVAL
                ## fill_linear
                error: create: [29] non-persisted type: INTERVAL
                ## subsample_stride
                error: create: [29] non-persisted type: INTERVAL
                ## subsample_target
                error: create: [29] non-persisted type: INTERVAL
                ## memoized
                error: create: [29] non-persisted type: INTERVAL
                ## where_bound_const
                error: create: [29] non-persisted type: INTERVAL
                ## where_bound_bind
                error: create: [29] non-persisted type: INTERVAL
                ## where_key
                error: create: [29] non-persisted type: INTERVAL
                ## copy_bind
                error: create: [29] non-persisted type: INTERVAL
                ## between_timestamp
                error: create: [29] non-persisted type: INTERVAL
                ## eq_null_double
                error: create: [29] non-persisted type: INTERVAL
                ## bind_value
                setBoolean\tdefine error: [0] bind variable cannot be used [contextType=39, index=0]
                setByte\tdefine error: [0] bind variable cannot be used [contextType=39, index=0]
                setShort\tdefine error: [0] bind variable cannot be used [contextType=39, index=0]
                setChar\tdefine error: [0] bind variable cannot be used [contextType=39, index=0]
                setInt\tdefine error: [0] bind variable cannot be used [contextType=39, index=0]
                setLong\tdefine error: [0] bind variable cannot be used [contextType=39, index=0]
                setFloat\tdefine error: [0] bind variable cannot be used [contextType=39, index=0]
                setDouble\tdefine error: [0] bind variable cannot be used [contextType=39, index=0]
                setDate\tdefine error: [0] bind variable cannot be used [contextType=39, index=0]
                setTimestamp\tdefine error: [0] bind variable cannot be used [contextType=39, index=0]
                setStr\tdefine error: [0] bind variable cannot be used [contextType=39, index=0]
                setVarchar\tdefine error: [0] bind variable cannot be used [contextType=39, index=0]
                setLong256\tdefine error: [0] bind variable cannot be used [contextType=39, index=0]
                setUuid\tdefine error: [0] bind variable cannot be used [contextType=39, index=0]
                setArray\tdefine error: [0] bind variable cannot be used [contextType=39, index=0]
                ## window_anchor
                error: create: [41] non-persisted type: INTERVAL
                """);
        rec("VARCHAR_SLICE", """
                ## filter_eq
                error: create: [29] unsupported column type: VARCHAR_SLICE
                ## filter_ne
                error: create: [29] unsupported column type: VARCHAR_SLICE
                ## filter_null
                error: create: [29] unsupported column type: VARCHAR_SLICE
                ## filter_not_null
                error: create: [29] unsupported column type: VARCHAR_SLICE
                ## filter_lt
                error: create: [29] unsupported column type: VARCHAR_SLICE
                ## filter_ge
                error: create: [29] unsupported column type: VARCHAR_SLICE
                ## order_asc
                error: create: [29] unsupported column type: VARCHAR_SLICE
                ## order_desc
                error: create: [29] unsupported column type: VARCHAR_SLICE
                ## group_by
                error: create: [29] unsupported column type: VARCHAR_SLICE
                ## join_inner
                error: create: [29] unsupported column type: VARCHAR_SLICE
                ## join_left
                error: create: [29] unsupported column type: VARCHAR_SLICE
                ## join_left_null
                error: create: [29] unsupported column type: VARCHAR_SLICE
                ## union_all
                error: create: [29] unsupported column type: VARCHAR_SLICE
                ## union_null
                error: create: [29] unsupported column type: VARCHAR_SLICE
                ## case_no_else
                error: create: [29] unsupported column type: VARCHAR_SLICE
                ## case_else
                error: create: [29] unsupported column type: VARCHAR_SLICE
                ## lag
                error: create: [29] unsupported column type: VARCHAR_SLICE
                ## sample_by
                error: create: [29] unsupported column type: VARCHAR_SLICE
                ## first_last
                error: create: [29] unsupported column type: VARCHAR_SLICE
                ## first_not_null
                error: create: [29] unsupported column type: VARCHAR_SLICE
                ## fill_prev
                error: create: [29] unsupported column type: VARCHAR_SLICE
                ## latest_on
                error: create: [29] unsupported column type: VARCHAR_SLICE
                ## cast
                error: create: [29] unsupported column type: VARCHAR_SLICE
                ## fill_null
                error: create: [29] unsupported column type: VARCHAR_SLICE
                ## fill_value
                error: create: [29] unsupported column type: VARCHAR_SLICE
                ## fill_linear
                error: create: [29] unsupported column type: VARCHAR_SLICE
                ## subsample_stride
                error: create: [29] unsupported column type: VARCHAR_SLICE
                ## subsample_target
                error: create: [29] unsupported column type: VARCHAR_SLICE
                ## memoized
                error: create: [29] unsupported column type: VARCHAR_SLICE
                ## where_bound_const
                error: create: [29] unsupported column type: VARCHAR_SLICE
                ## where_bound_bind
                error: create: [29] unsupported column type: VARCHAR_SLICE
                ## where_key
                error: create: [29] unsupported column type: VARCHAR_SLICE
                ## copy_bind
                error: create: [29] unsupported column type: VARCHAR_SLICE
                ## between_timestamp
                error: create: [29] unsupported column type: VARCHAR_SLICE
                ## eq_null_double
                error: create: [29] unsupported column type: VARCHAR_SLICE
                ## bind_value
                setBoolean\ttrue
                setByte\t1
                setShort\t1
                setChar\t1
                setInt\t1
                setLong\t1
                setFloat\t1.5
                setDouble\t1.5
                setDate\t1
                setTimestamp\t1970-01-01T00:00:00.000001Z
                setStr\t1
                setVarchar\t1
                setLong256\t0x01
                setUuid\t00000000-0000-0000-0000-000000000001
                setArray\terror: [0] bind variable at 0 is defined as VARCHAR and cannot accept ARRAY
                ## window_anchor
                error: create: [41] unsupported column type: VARCHAR_SLICE
                """);
        rec("TIMESTAMP_NS", """
                ## filter_eq
                props: random_access=true size=unknown timestamp=none
                k\tv
                max\t2262-04-11T23:47:16.854775807Z
                ## filter_ne
                props: random_access=true size=unknown timestamp=none
                k\tv
                min\t1677-01-01T00:12:43.145224193Z
                sentinel\t
                null\t
                ## filter_null
                props: random_access=true size=unknown timestamp=none
                k\tv
                sentinel\t
                null\t
                ## filter_not_null
                props: random_access=true size=unknown timestamp=none
                k\tv
                min\t1677-01-01T00:12:43.145224193Z
                max\t2262-04-11T23:47:16.854775807Z
                ## filter_lt
                props: random_access=true size=unknown timestamp=none
                k\tv
                min\t1677-01-01T00:12:43.145224193Z
                ## filter_ge
                props: random_access=true size=unknown timestamp=none
                k\tv
                max\t2262-04-11T23:47:16.854775807Z
                ## order_asc
                props: random_access=true size=known timestamp=v:asc
                k\tv
                null\t
                sentinel\t
                min\t1677-01-01T00:12:43.145224193Z
                max\t2262-04-11T23:47:16.854775807Z
                ## order_desc
                props: random_access=true size=known timestamp=v:desc
                k\tv
                max\t2262-04-11T23:47:16.854775807Z
                min\t1677-01-01T00:12:43.145224193Z
                null\t
                sentinel\t
                ## group_by
                props: random_access=true size=known timestamp=f:asc
                v\tc\tf
                1677-01-01T00:12:43.145224193Z\t2\t1970-01-01T00:00:00.000000Z
                2262-04-11T23:47:16.854775807Z\t2\t1970-01-01T00:00:01.000000Z
                \t4\t1970-01-01T00:00:02.000000Z
                ## join_inner
                props: random_access=true size=unknown timestamp=none
                tk\tuk\tv
                min\tmin\t1677-01-01T00:12:43.145224193Z
                null\tsentinel\t
                sentinel\tsentinel\t
                ## join_left
                props: random_access=true size=unknown timestamp=none
                tk\tuk\tuv
                max\t\t
                min\tmin\t1677-01-01T00:12:43.145224193Z
                null\tsentinel\t
                sentinel\tsentinel\t
                ## join_left_null
                props: random_access=true size=unknown timestamp=none
                tk\tuv
                max\t
                min\t1677-01-01T00:12:43.145224193Z
                null\t
                sentinel\t
                ## union_all
                props: random_access=false size=known timestamp=none
                k\tv
                min\t1677-01-01T00:12:43.145224193Z
                max\t2262-04-11T23:47:16.854775807Z
                sentinel\t
                null\t
                min\t1677-01-01T00:12:43.145224193Z
                sentinel\t
                ## union_null
                props: random_access=false size=known timestamp=none
                k\tv
                min\t1677-01-01T00:12:43.145224193Z
                sentinel\t
                null_branch\t
                ## case_no_else
                props: random_access=true size=known timestamp=none
                k\tc
                min\t
                max\t2262-04-11T23:47:16.854775807Z
                sentinel\t
                null\t
                ## case_else
                props: random_access=true size=known timestamp=none
                k\tc
                min\t1677-01-01T00:12:43.145224193Z
                max\t2262-04-11T23:47:16.854775807Z
                sentinel\t2262-04-11T23:47:16.854775807Z
                null\t2262-04-11T23:47:16.854775807Z
                ## lag
                props: random_access=false size=known timestamp=none
                k\tv\tp
                min\t1677-01-01T00:12:43.145224193Z\t
                max\t2262-04-11T23:47:16.854775807Z\t1677-01-01T00:12:43.145224193Z
                sentinel\t\t2262-04-11T23:47:16.854775807Z
                null\t\t
                ## sample_by
                props: random_access=true size=known timestamp=ts:asc
                ts\tf\tl\tc
                1970-01-01T00:00:00.000000Z\t1677-01-01T00:12:43.145224193Z\t2262-04-11T23:47:16.854775807Z\t2
                1970-01-01T00:00:02.000000Z\t\t\t2
                ## first_last
                props: random_access=true size=known timestamp=none
                k\tf\tl
                A\t\t2262-04-11T23:47:16.854775807Z
                B\t2262-04-11T23:47:16.854775807Z\t
                C\t\t
                ## first_not_null
                props: random_access=true size=known timestamp=none
                k\tf\tl
                A\t2262-04-11T23:47:16.854775807Z\t2262-04-11T23:47:16.854775807Z
                B\t2262-04-11T23:47:16.854775807Z\t2262-04-11T23:47:16.854775807Z
                C\t\t
                ## fill_prev
                props: random_access=false size=unknown timestamp=ts:asc
                ts\tv
                1970-01-01T00:00:00.000000Z\t
                1970-01-01T00:00:01.000000Z\t
                1970-01-01T00:00:02.000000Z\t2262-04-11T23:47:16.854775807Z
                1970-01-01T00:00:03.000000Z\t2262-04-11T23:47:16.854775807Z
                1970-01-01T00:00:04.000000Z\t
                ## latest_on
                props: random_access=true size=known timestamp=none
                k\tv
                b:max\t2262-04-11T23:47:16.854775807Z
                b:min\t1677-01-01T00:12:43.145224193Z
                b:null\t
                ## cast
                target\tmin|max|sentinel|null
                BOOLEAN\ttrue|true|false|false
                BYTE\t1|-1|0|0
                SHORT\t1|-1|0|0
                CHAR\t\\u0001|\\uffff||
                INT\t1|-1|null|null
                LONG\t-9223372036854775807|9223372036854775807|null|null
                DATE\t1677-09-21T00:12:43.146Z|2262-04-11T23:47:16.854Z||
                TIMESTAMP\t1677-09-21T00:12:43.145225Z|2262-04-11T23:47:16.854775Z||
                FLOAT\t-9.223372E18|9.223372E18|null|null
                DOUBLE\t-9.223372036854776E18|9.223372036854776E18|null|null
                STRING\t1677-01-01T00:12:43.145224193Z|2262-04-11T23:47:16.854775807Z||
                SYMBOL\t-9223372036854775807|9223372036854775807||
                LONG256\t0x8000000000000001|0x7fffffffffffffff||
                GEOBYTE\t0000001|||
                GEOSHORT\t001|||
                GEOINT\t000001|||
                GEOLONG\t00000001|zzzzzzzz||
                BINARY\terror: [20] unsupported cast
                UUID\tno cast [10]
                LONG128\terror: [20] unsupported cast
                IPv4\tno cast [10]
                VARCHAR\t1677-01-01T00:12:43.145224193Z|2262-04-11T23:47:16.854775807Z||
                DOUBLE[]\t[-9.223372036854776E18]|[9.223372036854776E18]|null|null
                DECIMAL8\terror: inconvertible value: -9223372036854775807 [LONG -> DECIMAL(2,1)]
                DECIMAL16\terror: inconvertible value: -9223372036854775807 [LONG -> DECIMAL(4,2)]
                DECIMAL32\terror: inconvertible value: -9223372036854775807 [LONG -> DECIMAL(9,0)]
                DECIMAL64\terror: inconvertible value: -9223372036854775807 [LONG -> DECIMAL(16,4)]
                DECIMAL128\t-9223372036854775807.0000000000|9223372036854775807.0000000000||
                DECIMAL256\t-9223372036854775807.00000000000000000000|9223372036854775807.00000000000000000000||
                INTERVAL\terror: [20] unsupported cast
                VARCHAR_SLICE\terror: [20] unsupported cast
                TIMESTAMP_NS\t1677-01-01T00:12:43.145224193Z|2262-04-11T23:47:16.854775807Z||
                GEOHASH(1c)\t1|||
                GEOHASH(8b)\t00000001|||
                GEOHASH(31b)\t0000000000000000000000000000001|||
                GEOHASH(12c)\t000000000001|zzzzzzzzzzzz||
                DECIMAL(5,2)\terror: inconvertible value: -9223372036854775807 [LONG -> DECIMAL(5,2)]
                DECIMAL(18,3)\terror: inconvertible value: -9223372036854775807 [LONG -> DECIMAL(18,3)]
                DOUBLE[][]\t[[-9.223372036854776E18]]|[[9.223372036854776E18]]|null|null
                INTERVAL(us)\terror: [20] unsupported cast
                INTERVAL(ns)\terror: [20] unsupported cast
                ## fill_null
                props: random_access=false size=unknown timestamp=ts:asc
                ts\tv
                1970-01-01T00:00:00.000000Z\t1677-01-01T00:12:43.145224193Z
                1970-01-01T00:00:01.000000Z\t
                1970-01-01T00:00:02.000000Z\t2262-04-11T23:47:16.854775807Z
                1970-01-01T00:00:03.000000Z\t
                1970-01-01T00:00:04.000000Z\t
                ## fill_value
                props: random_access=false size=unknown timestamp=ts:asc
                ts\tv
                1970-01-01T00:00:00.000000Z\t1677-01-01T00:12:43.145224193Z
                1970-01-01T00:00:01.000000Z\t2262-04-11T23:47:16.854775807Z
                1970-01-01T00:00:02.000000Z\t2262-04-11T23:47:16.854775807Z
                1970-01-01T00:00:03.000000Z\t2262-04-11T23:47:16.854775807Z
                1970-01-01T00:00:04.000000Z\t
                ## fill_linear
                error: [11] Unsupported interpolation type: TIMESTAMP_NS
                ## subsample_stride
                min\terror: [60] integer expected for stride
                max\terror: [57] integer expected for stride
                sentinel\terror: [64] integer expected for stride
                null\terror: [38] integer expected for stride
                ## subsample_target
                min\terror: [60] integer expected for target point count
                max\terror: [57] integer expected for target point count
                sentinel\terror: [64] integer expected for target point count
                null\terror: [38] integer expected for target point count
                ## memoized
                props: random_access=true size=known timestamp=none
                k\ta\ta2\ta3
                min\t1677-01-01T00:12:43.145224193Z\t1677-01-01T00:12:43.145224193Z\t1677-01-01T00:12:43.145224193Z
                max\t2262-04-11T23:47:16.854775807Z\t2262-04-11T23:47:16.854775807Z\t2262-04-11T23:47:16.854775807Z
                sentinel\t\t\t
                null\t\t\t
                plan memoizes: true
                ## where_bound_const
                min\tmin,max,sentinel,null
                max\t
                sentinel\t
                null\t
                ## where_bound_bind
                min\tmin,max,sentinel,null
                max\t
                sentinel\t
                null\t
                ## where_key
                min\t
                max\t
                sentinel\tb:null
                null\tb:null
                ## copy_bind
                status\tmessage
                finished\t
                k
                max
                ## between_timestamp
                props: random_access=true size=unknown timestamp=none
                k\tv
                ## eq_null_double
                props: random_access=true size=unknown timestamp=none
                k\tv
                sentinel\t
                null\t
                ## bind_value
                setBoolean\terror: [0] bind variable at 0 is defined as TIMESTAMP_NS and cannot accept BOOLEAN
                setByte\t1970-01-01T00:00:00.000000001Z
                setShort\t1970-01-01T00:00:00.000000001Z
                setChar\t1970-01-01T00:00:00.000000001Z
                setInt\t1970-01-01T00:00:00.000000001Z
                setLong\t1970-01-01T00:00:00.000000001Z
                setFloat\terror: [0] bind variable at 0 is defined as TIMESTAMP_NS and cannot accept FLOAT
                setDouble\terror: [0] bind variable at 0 is defined as TIMESTAMP_NS and cannot accept DOUBLE
                setDate\t1970-01-01T00:00:00.001000000Z
                setTimestamp\t1970-01-01T00:00:00.000001000Z
                setStr\t1970-01-01T00:00:00.000000001Z
                setVarchar\t1970-01-01T00:00:00.000000001Z
                setLong256\terror: [0] bind variable at 0 is defined as TIMESTAMP_NS and cannot accept LONG256
                setUuid\terror: [0] bind variable at 0 is defined as TIMESTAMP_NS and cannot accept UUID
                setArray\terror: [0] bind variable at 0 is defined as TIMESTAMP_NS and cannot accept ARRAY
                ## window_anchor
                k\tc
                min\t1
                max\t1
                sentinel\t1
                null\t2
                """);
        rec("GEOHASH(1c)", """
                ## filter_eq
                props: random_access=true size=unknown timestamp=none
                k\tv
                max\tz
                ## filter_ne
                props: random_access=true size=unknown timestamp=none
                k\tv
                min\t0
                null\t
                ## filter_null
                props: random_access=true size=unknown timestamp=none
                k\tv
                null\t
                ## filter_not_null
                props: random_access=true size=unknown timestamp=none
                k\tv
                min\t0
                max\tz
                ## filter_lt
                error: [27] there is no matching operator `<` with the argument types: GEOHASH(1c) < GEOHASH(1c)
                ## filter_ge
                error: [27] there is no matching operator `>=` with the argument types: GEOHASH(1c) >= GEOHASH(1c)
                ## order_asc
                props: random_access=true size=known timestamp=none
                k\tv
                null\t
                min\t0
                max\tz
                ## order_desc
                props: random_access=true size=known timestamp=none
                k\tv
                max\tz
                min\t0
                null\t
                ## group_by
                props: random_access=true size=known timestamp=f:asc
                v\tc\tf
                0\t2\t1970-01-01T00:00:00.000000Z
                z\t2\t1970-01-01T00:00:01.000000Z
                \t2\t1970-01-01T00:00:02.000000Z
                ## join_inner
                props: random_access=true size=unknown timestamp=none
                tk\tuk\tv
                min\tmin\t0
                null\tnull\t
                ## join_left
                props: random_access=true size=unknown timestamp=none
                tk\tuk\tuv
                max\t\t
                min\tmin\t0
                null\tnull\t
                ## join_left_null
                props: random_access=true size=unknown timestamp=none
                tk\tuv
                max\t
                min\t0
                null\t
                ## union_all
                props: random_access=false size=known timestamp=none
                k\tv
                min\t0
                max\tz
                null\t
                min\t0
                null\t
                ## union_null
                props: random_access=false size=known timestamp=none
                k\tv
                min\t0
                null\tnull
                null_branch\t
                ## case_no_else
                props: random_access=true size=known timestamp=none
                k\tc
                min\t
                max\tz
                null\t
                ## case_else
                error: [35] inconvertible types: GEOHASH(1c) -> GEOHASH(1c) [from=GEOHASH(1c), to=GEOHASH(1c)]
                ## lag
                error: [13] there is no matching function `lag` with the argument types: (GEOHASH(1c))
                ## sample_by
                props: random_access=true size=known timestamp=ts:asc
                ts\tf\tl\tc
                1970-01-01T00:00:00.000000Z\t0\tz\t2
                1970-01-01T00:00:02.000000Z\t\t\t1
                ## first_last
                props: random_access=true size=known timestamp=none
                k\tf\tl
                A\t\tz
                B\tz\t
                C\t\t
                ## first_not_null
                props: random_access=true size=known timestamp=none
                k\tf\tl
                A\tz\tz
                B\tz\tz
                C\t\t
                ## fill_prev
                props: random_access=false size=unknown timestamp=ts:asc
                ts\tv
                1970-01-01T00:00:00.000000Z\t
                1970-01-01T00:00:01.000000Z\t
                1970-01-01T00:00:02.000000Z\tz
                1970-01-01T00:00:03.000000Z\tz
                1970-01-01T00:00:04.000000Z\t
                ## latest_on
                props: random_access=true size=known timestamp=none
                k\tv
                b:max\tz
                b:min\t0
                b:null\t
                ## cast
                target\tmin|max|null
                BOOLEAN\tno cast [10]
                BYTE\tno cast [10]
                SHORT\tno cast [10]
                CHAR\tno cast [10]
                INT\tno cast [10]
                LONG\tno cast [10]
                DATE\tno cast [10]
                TIMESTAMP\tno cast [10]
                FLOAT\tno cast [10]
                DOUBLE\tno cast [10]
                STRING\t0|z|
                SYMBOL\tno cast [10]
                LONG256\tno cast [10]
                GEOBYTE\terror: [10] CAST cannot narrow values from GEOHASH(5b) to GEOHASH(7b)
                GEOSHORT\terror: [10] CAST cannot narrow values from GEOHASH(5b) to GEOHASH(15b)
                GEOINT\terror: [10] CAST cannot narrow values from GEOHASH(5b) to GEOHASH(30b)
                GEOLONG\terror: [10] CAST cannot narrow values from GEOHASH(5b) to GEOHASH(40b)
                BINARY\terror: [20] unsupported cast
                UUID\tno cast [10]
                LONG128\terror: [20] unsupported cast
                IPv4\tno cast [10]
                VARCHAR\t0|z|
                DOUBLE[]\tno cast [10]
                DECIMAL8\tno cast [10]
                DECIMAL16\tno cast [10]
                DECIMAL32\tno cast [10]
                DECIMAL64\tno cast [10]
                DECIMAL128\tno cast [10]
                DECIMAL256\tno cast [10]
                INTERVAL\terror: [20] unsupported cast
                VARCHAR_SLICE\terror: [20] unsupported cast
                TIMESTAMP_NS\tno cast [10]
                GEOHASH(1c)\t0|z|
                GEOHASH(8b)\terror: [10] CAST cannot narrow values from GEOHASH(5b) to GEOHASH(8b)
                GEOHASH(31b)\terror: [10] CAST cannot narrow values from GEOHASH(5b) to GEOHASH(31b)
                GEOHASH(12c)\terror: [10] CAST cannot narrow values from GEOHASH(5b) to GEOHASH(60b)
                DECIMAL(5,2)\tno cast [10]
                DECIMAL(18,3)\tno cast [10]
                DOUBLE[][]\tno cast [10]
                INTERVAL(us)\terror: [20] unsupported cast
                INTERVAL(ns)\terror: [20] unsupported cast
                ## fill_null
                props: random_access=false size=unknown timestamp=ts:asc
                ts\tv
                1970-01-01T00:00:00.000000Z\t0
                1970-01-01T00:00:01.000000Z\t
                1970-01-01T00:00:02.000000Z\tz
                1970-01-01T00:00:03.000000Z\t
                1970-01-01T00:00:04.000000Z\t
                ## fill_value
                error: null
                ## fill_linear
                error: [11] Unsupported interpolation type: GEOHASH(1c)
                ## subsample_stride
                min\terror: [38] integer expected for stride
                max\terror: [38] integer expected for stride
                null\terror: [38] integer expected for stride
                ## subsample_target
                min\terror: [38] integer expected for target point count
                max\terror: [38] integer expected for target point count
                null\terror: [38] integer expected for target point count
                ## memoized
                props: random_access=true size=known timestamp=none
                k\ta\ta2\ta3
                min\t0\t0\t0
                max\tz\tz\tz
                null\t\t\t
                plan memoizes: false
                ## where_bound_const
                min\terror: [31] there is no matching operator `>=` with the argument types: LONG >= GEOHASH(1c)
                max\terror: [31] there is no matching operator `>=` with the argument types: LONG >= GEOHASH(1c)
                null\terror: [31] there is no matching operator `>=` with the argument types: LONG >= GEOHASH(1c)
                ## where_bound_bind
                min\tbind error: [0] bind variable at 0 is defined as GEOHASH(1c) and cannot accept STRING
                max\tbind error: [0] bind variable at 0 is defined as GEOHASH(1c) and cannot accept STRING
                null\tbind error: [0] bind variable at 0 is defined as GEOHASH(1c) and cannot accept STRING
                ## where_key
                min\tb:min
                max\tb:max
                null\tb:null
                ## copy_bind
                bind error: [0] bind variable at 0 is defined as GEOHASH(1c) and cannot accept STRING
                ## between_timestamp
                error: [27] there is no matching operator `between` with the argument type: GEOHASH(1c)
                ## eq_null_double
                props: random_access=true size=unknown timestamp=none
                k\tv
                null\t
                ## bind_value
                setBoolean\terror: [0] bind variable at 0 is defined as GEOHASH(1c) and cannot accept BOOLEAN
                setByte\terror: [0] bind variable at 0 is defined as GEOHASH(1c) and cannot accept BYTE
                setShort\terror: [0] bind variable at 0 is defined as GEOHASH(1c) and cannot accept SHORT
                setChar\t1
                setInt\terror: [0] bind variable at 0 is defined as GEOHASH(1c) and cannot accept INT
                setLong\terror: [0] bind variable at 0 is defined as GEOHASH(1c) and cannot accept LONG
                setFloat\terror: [0] bind variable at 0 is defined as GEOHASH(1c) and cannot accept FLOAT
                setDouble\terror: [0] bind variable at 0 is defined as GEOHASH(1c) and cannot accept DOUBLE
                setDate\terror: [0] bind variable at 0 is defined as GEOHASH(1c) and cannot accept DATE
                setTimestamp\terror: [0] bind variable at 0 is defined as GEOHASH(1c) and cannot accept TIMESTAMP
                setStr\terror: [0] bind variable at 0 is defined as GEOHASH(1c) and cannot accept STRING
                setVarchar\terror: [0] bind variable at 0 is defined as GEOHASH(1c) and cannot accept VARCHAR
                setLong256\terror: [0] bind variable at 0 is defined as GEOHASH(1c) and cannot accept LONG256
                setUuid\terror: [0] bind variable at 0 is defined as GEOHASH(1c) and cannot accept UUID
                setArray\terror: [0] bind variable at 0 is defined as GEOHASH(1c) and cannot accept ARRAY
                ## window_anchor
                error: create view: [147] ANCHOR EXPRESSION must return TIMESTAMP, LONG, or INT; got GEOHASH(1c)
                """);
        rec("GEOHASH(8b)", """
                ## filter_eq
                props: random_access=true size=unknown timestamp=none
                k\tv
                max\t11111111
                ## filter_ne
                props: random_access=true size=unknown timestamp=none
                k\tv
                min\t00000000
                null\t
                ## filter_null
                props: random_access=true size=unknown timestamp=none
                k\tv
                null\t
                ## filter_not_null
                props: random_access=true size=unknown timestamp=none
                k\tv
                min\t00000000
                max\t11111111
                ## filter_lt
                error: [27] there is no matching operator `<` with the argument types: GEOHASH(8b) < GEOHASH(8b)
                ## filter_ge
                error: [27] there is no matching operator `>=` with the argument types: GEOHASH(8b) >= GEOHASH(8b)
                ## order_asc
                props: random_access=true size=known timestamp=none
                k\tv
                null\t
                min\t00000000
                max\t11111111
                ## order_desc
                props: random_access=true size=known timestamp=none
                k\tv
                max\t11111111
                min\t00000000
                null\t
                ## group_by
                props: random_access=true size=known timestamp=f:asc
                v\tc\tf
                00000000\t2\t1970-01-01T00:00:00.000000Z
                11111111\t2\t1970-01-01T00:00:01.000000Z
                \t2\t1970-01-01T00:00:02.000000Z
                ## join_inner
                props: random_access=true size=unknown timestamp=none
                tk\tuk\tv
                min\tmin\t00000000
                null\tnull\t
                ## join_left
                props: random_access=true size=unknown timestamp=none
                tk\tuk\tuv
                max\t\t
                min\tmin\t00000000
                null\tnull\t
                ## join_left_null
                props: random_access=true size=unknown timestamp=none
                tk\tuv
                max\t
                min\t00000000
                null\t
                ## union_all
                props: random_access=false size=known timestamp=none
                k\tv
                min\t00000000
                max\t11111111
                null\t
                min\t00000000
                null\t
                ## union_null
                props: random_access=false size=known timestamp=none
                k\tv
                min\t00000000
                null\tnull
                null_branch\t
                ## case_no_else
                props: random_access=true size=known timestamp=none
                k\tc
                min\t
                max\t11111111
                null\t
                ## case_else
                error: [35] inconvertible types: GEOHASH(8b) -> GEOHASH(8b) [from=GEOHASH(8b), to=GEOHASH(8b)]
                ## lag
                error: [13] there is no matching function `lag` with the argument types: (GEOHASH(8b))
                ## sample_by
                props: random_access=true size=known timestamp=ts:asc
                ts\tf\tl\tc
                1970-01-01T00:00:00.000000Z\t00000000\t11111111\t2
                1970-01-01T00:00:02.000000Z\t\t\t1
                ## first_last
                props: random_access=true size=known timestamp=none
                k\tf\tl
                A\t\t11111111
                B\t11111111\t
                C\t\t
                ## first_not_null
                props: random_access=true size=known timestamp=none
                k\tf\tl
                A\t11111111\t11111111
                B\t11111111\t11111111
                C\t\t
                ## fill_prev
                props: random_access=false size=unknown timestamp=ts:asc
                ts\tv
                1970-01-01T00:00:00.000000Z\t
                1970-01-01T00:00:01.000000Z\t
                1970-01-01T00:00:02.000000Z\t11111111
                1970-01-01T00:00:03.000000Z\t11111111
                1970-01-01T00:00:04.000000Z\t
                ## latest_on
                props: random_access=true size=known timestamp=none
                k\tv
                b:max\t11111111
                b:min\t00000000
                b:null\t
                ## cast
                target\tmin|max|null
                BOOLEAN\tno cast [10]
                BYTE\tno cast [10]
                SHORT\tno cast [10]
                CHAR\tno cast [10]
                INT\tno cast [10]
                LONG\tno cast [10]
                DATE\tno cast [10]
                TIMESTAMP\tno cast [10]
                FLOAT\tno cast [10]
                DOUBLE\tno cast [10]
                STRING\t00000000|11111111|
                SYMBOL\tno cast [10]
                LONG256\tno cast [10]
                GEOBYTE\t0000000|1111111|
                GEOSHORT\terror: [10] CAST cannot narrow values from GEOHASH(8b) to GEOHASH(15b)
                GEOINT\terror: [10] CAST cannot narrow values from GEOHASH(8b) to GEOHASH(30b)
                GEOLONG\terror: [10] CAST cannot narrow values from GEOHASH(8b) to GEOHASH(40b)
                BINARY\terror: [20] unsupported cast
                UUID\tno cast [10]
                LONG128\terror: [20] unsupported cast
                IPv4\tno cast [10]
                VARCHAR\t00000000|11111111|
                DOUBLE[]\tno cast [10]
                DECIMAL8\tno cast [10]
                DECIMAL16\tno cast [10]
                DECIMAL32\tno cast [10]
                DECIMAL64\tno cast [10]
                DECIMAL128\tno cast [10]
                DECIMAL256\tno cast [10]
                INTERVAL\terror: [20] unsupported cast
                VARCHAR_SLICE\terror: [20] unsupported cast
                TIMESTAMP_NS\tno cast [10]
                GEOHASH(1c)\t0|z|
                GEOHASH(8b)\t00000000|11111111|
                GEOHASH(31b)\terror: [10] CAST cannot narrow values from GEOHASH(8b) to GEOHASH(31b)
                GEOHASH(12c)\terror: [10] CAST cannot narrow values from GEOHASH(8b) to GEOHASH(60b)
                DECIMAL(5,2)\tno cast [10]
                DECIMAL(18,3)\tno cast [10]
                DOUBLE[][]\tno cast [10]
                INTERVAL(us)\terror: [20] unsupported cast
                INTERVAL(ns)\terror: [20] unsupported cast
                ## fill_null
                props: random_access=false size=unknown timestamp=ts:asc
                ts\tv
                1970-01-01T00:00:00.000000Z\t00000000
                1970-01-01T00:00:01.000000Z\t
                1970-01-01T00:00:02.000000Z\t11111111
                1970-01-01T00:00:03.000000Z\t
                1970-01-01T00:00:04.000000Z\t
                ## fill_value
                error: [46] fill value of type INT cannot fill column of type GEOHASH(8b)
                ## fill_linear
                error: [46] support for LINEAR fill is not yet implemented [function=last(v), class=io.questdb.griffin.engine.functions.groupby.LastGeoHashGroupByFunctionFactory$2]
                ## subsample_stride
                min\terror: [38] integer expected for stride
                max\terror: [38] integer expected for stride
                null\terror: [38] integer expected for stride
                ## subsample_target
                min\terror: [38] integer expected for target point count
                max\terror: [38] integer expected for target point count
                null\terror: [38] integer expected for target point count
                ## memoized
                props: random_access=true size=known timestamp=none
                k\ta\ta2\ta3
                min\t00000000\t00000000\t00000000
                max\t11111111\t11111111\t11111111
                null\t\t\t
                plan memoizes: false
                ## where_bound_const
                min\terror: [31] there is no matching operator `>=` with the argument types: LONG >= GEOHASH(8b)
                max\terror: [31] there is no matching operator `>=` with the argument types: LONG >= GEOHASH(8b)
                null\terror: [31] there is no matching operator `>=` with the argument types: LONG >= GEOHASH(8b)
                ## where_bound_bind
                min\tbind error: [0] bind variable at 0 is defined as GEOHASH(8b) and cannot accept STRING
                max\tbind error: [0] bind variable at 0 is defined as GEOHASH(8b) and cannot accept STRING
                null\tbind error: [0] bind variable at 0 is defined as GEOHASH(8b) and cannot accept STRING
                ## where_key
                min\tb:min
                max\t
                null\tb:null
                ## copy_bind
                bind error: [0] bind variable at 0 is defined as GEOHASH(8b) and cannot accept STRING
                ## between_timestamp
                error: [27] there is no matching operator `between` with the argument type: GEOHASH(8b)
                ## eq_null_double
                props: random_access=true size=unknown timestamp=none
                k\tv
                null\t
                ## bind_value
                setBoolean\terror: [0] bind variable at 0 is defined as GEOHASH(8b) and cannot accept BOOLEAN
                setByte\terror: [0] bind variable at 0 is defined as GEOHASH(8b) and cannot accept BYTE
                setShort\terror: [0] bind variable at 0 is defined as GEOHASH(8b) and cannot accept SHORT
                setChar\terror: [0] bind variable at 0 is defined as GEOHASH(8b) and cannot accept CHAR
                setInt\terror: [0] bind variable at 0 is defined as GEOHASH(8b) and cannot accept INT
                setLong\terror: [0] bind variable at 0 is defined as GEOHASH(8b) and cannot accept LONG
                setFloat\terror: [0] bind variable at 0 is defined as GEOHASH(8b) and cannot accept FLOAT
                setDouble\terror: [0] bind variable at 0 is defined as GEOHASH(8b) and cannot accept DOUBLE
                setDate\terror: [0] bind variable at 0 is defined as GEOHASH(8b) and cannot accept DATE
                setTimestamp\terror: [0] bind variable at 0 is defined as GEOHASH(8b) and cannot accept TIMESTAMP
                setStr\terror: [0] bind variable at 0 is defined as GEOHASH(8b) and cannot accept STRING
                setVarchar\terror: [0] bind variable at 0 is defined as GEOHASH(8b) and cannot accept VARCHAR
                setLong256\terror: [0] bind variable at 0 is defined as GEOHASH(8b) and cannot accept LONG256
                setUuid\terror: [0] bind variable at 0 is defined as GEOHASH(8b) and cannot accept UUID
                setArray\terror: [0] bind variable at 0 is defined as GEOHASH(8b) and cannot accept ARRAY
                ## window_anchor
                error: create view: [147] ANCHOR EXPRESSION must return TIMESTAMP, LONG, or INT; got GEOHASH(8b)
                """);
        rec("GEOHASH(31b)", """
                ## filter_eq
                props: random_access=true size=unknown timestamp=none
                k\tv
                max\t1111111111111111111111111111111
                ## filter_ne
                props: random_access=true size=unknown timestamp=none
                k\tv
                min\t0000000000000000000000000000000
                null\t
                ## filter_null
                props: random_access=true size=unknown timestamp=none
                k\tv
                null\t
                ## filter_not_null
                props: random_access=true size=unknown timestamp=none
                k\tv
                min\t0000000000000000000000000000000
                max\t1111111111111111111111111111111
                ## filter_lt
                error: [27] there is no matching operator `<` with the argument types: GEOHASH(31b) < GEOHASH(31b)
                ## filter_ge
                error: [27] there is no matching operator `>=` with the argument types: GEOHASH(31b) >= GEOHASH(31b)
                ## order_asc
                props: random_access=true size=known timestamp=none
                k\tv
                null\t
                min\t0000000000000000000000000000000
                max\t1111111111111111111111111111111
                ## order_desc
                props: random_access=true size=known timestamp=none
                k\tv
                max\t1111111111111111111111111111111
                min\t0000000000000000000000000000000
                null\t
                ## group_by
                props: random_access=true size=known timestamp=f:asc
                v\tc\tf
                0000000000000000000000000000000\t2\t1970-01-01T00:00:00.000000Z
                1111111111111111111111111111111\t2\t1970-01-01T00:00:01.000000Z
                \t2\t1970-01-01T00:00:02.000000Z
                ## join_inner
                props: random_access=true size=unknown timestamp=none
                tk\tuk\tv
                min\tmin\t0000000000000000000000000000000
                null\tnull\t
                ## join_left
                props: random_access=true size=unknown timestamp=none
                tk\tuk\tuv
                max\t\t
                min\tmin\t0000000000000000000000000000000
                null\tnull\t
                ## join_left_null
                props: random_access=true size=unknown timestamp=none
                tk\tuv
                max\t
                min\t0000000000000000000000000000000
                null\t
                ## union_all
                props: random_access=false size=known timestamp=none
                k\tv
                min\t0000000000000000000000000000000
                max\t1111111111111111111111111111111
                null\t
                min\t0000000000000000000000000000000
                null\t
                ## union_null
                props: random_access=false size=known timestamp=none
                k\tv
                min\t0000000000000000000000000000000
                null\tnull
                null_branch\t
                ## case_no_else
                props: random_access=true size=known timestamp=none
                k\tc
                min\t
                max\t1111111111111111111111111111111
                null\t
                ## case_else
                error: [35] inconvertible types: GEOHASH(31b) -> GEOHASH(31b) [from=GEOHASH(31b), to=GEOHASH(31b)]
                ## lag
                error: [13] there is no matching function `lag` with the argument types: (GEOHASH(31b))
                ## sample_by
                props: random_access=true size=known timestamp=ts:asc
                ts\tf\tl\tc
                1970-01-01T00:00:00.000000Z\t0000000000000000000000000000000\t1111111111111111111111111111111\t2
                1970-01-01T00:00:02.000000Z\t\t\t1
                ## first_last
                props: random_access=true size=known timestamp=none
                k\tf\tl
                A\t\t1111111111111111111111111111111
                B\t1111111111111111111111111111111\t
                C\t\t
                ## first_not_null
                props: random_access=true size=known timestamp=none
                k\tf\tl
                A\t1111111111111111111111111111111\t1111111111111111111111111111111
                B\t1111111111111111111111111111111\t1111111111111111111111111111111
                C\t\t
                ## fill_prev
                props: random_access=false size=unknown timestamp=ts:asc
                ts\tv
                1970-01-01T00:00:00.000000Z\t
                1970-01-01T00:00:01.000000Z\t
                1970-01-01T00:00:02.000000Z\t1111111111111111111111111111111
                1970-01-01T00:00:03.000000Z\t1111111111111111111111111111111
                1970-01-01T00:00:04.000000Z\t
                ## latest_on
                props: random_access=true size=known timestamp=none
                k\tv
                b:max\t1111111111111111111111111111111
                b:min\t0000000000000000000000000000000
                b:null\t
                ## cast
                target\tmin|max|null
                BOOLEAN\tno cast [10]
                BYTE\tno cast [10]
                SHORT\tno cast [10]
                CHAR\tno cast [10]
                INT\tno cast [10]
                LONG\tno cast [10]
                DATE\tno cast [10]
                TIMESTAMP\tno cast [10]
                FLOAT\tno cast [10]
                DOUBLE\tno cast [10]
                STRING\t0000000000000000000000000000000|1111111111111111111111111111111|
                SYMBOL\tno cast [10]
                LONG256\tno cast [10]
                GEOBYTE\t0000000|1111111|
                GEOSHORT\t000|zzz|
                GEOINT\t000000|zzzzzz|
                GEOLONG\terror: [10] CAST cannot narrow values from GEOHASH(31b) to GEOHASH(40b)
                BINARY\terror: [20] unsupported cast
                UUID\tno cast [10]
                LONG128\terror: [20] unsupported cast
                IPv4\tno cast [10]
                VARCHAR\t0000000000000000000000000000000|1111111111111111111111111111111|
                DOUBLE[]\tno cast [10]
                DECIMAL8\tno cast [10]
                DECIMAL16\tno cast [10]
                DECIMAL32\tno cast [10]
                DECIMAL64\tno cast [10]
                DECIMAL128\tno cast [10]
                DECIMAL256\tno cast [10]
                INTERVAL\terror: [20] unsupported cast
                VARCHAR_SLICE\terror: [20] unsupported cast
                TIMESTAMP_NS\tno cast [10]
                GEOHASH(1c)\t0|z|
                GEOHASH(8b)\t00000000|11111111|
                GEOHASH(31b)\t0000000000000000000000000000000|1111111111111111111111111111111|
                GEOHASH(12c)\terror: [10] CAST cannot narrow values from GEOHASH(31b) to GEOHASH(60b)
                DECIMAL(5,2)\tno cast [10]
                DECIMAL(18,3)\tno cast [10]
                DOUBLE[][]\tno cast [10]
                INTERVAL(us)\terror: [20] unsupported cast
                INTERVAL(ns)\terror: [20] unsupported cast
                ## fill_null
                props: random_access=false size=unknown timestamp=ts:asc
                ts\tv
                1970-01-01T00:00:00.000000Z\t0000000000000000000000000000000
                1970-01-01T00:00:01.000000Z\t
                1970-01-01T00:00:02.000000Z\t1111111111111111111111111111111
                1970-01-01T00:00:03.000000Z\t
                1970-01-01T00:00:04.000000Z\t
                ## fill_value
                error: [46] fill value of type DOUBLE cannot fill column of type GEOHASH(31b)
                ## fill_linear
                error: [11] Unsupported interpolation type: GEOHASH(31b)
                ## subsample_stride
                min\terror: [38] integer expected for stride
                max\terror: [38] integer expected for stride
                null\terror: [38] integer expected for stride
                ## subsample_target
                min\terror: [38] integer expected for target point count
                max\terror: [38] integer expected for target point count
                null\terror: [38] integer expected for target point count
                ## memoized
                props: random_access=true size=known timestamp=none
                k\ta\ta2\ta3
                min\t0000000000000000000000000000000\t0000000000000000000000000000000\t0000000000000000000000000000000
                max\t1111111111111111111111111111111\t1111111111111111111111111111111\t1111111111111111111111111111111
                null\t\t\t
                plan memoizes: false
                ## where_bound_const
                min\terror: [31] there is no matching operator `>=` with the argument types: LONG >= GEOHASH(31b)
                max\terror: [31] there is no matching operator `>=` with the argument types: LONG >= GEOHASH(31b)
                null\terror: [31] there is no matching operator `>=` with the argument types: LONG >= GEOHASH(31b)
                ## where_bound_bind
                min\tbind error: [0] bind variable at 0 is defined as GEOHASH(31b) and cannot accept STRING
                max\tbind error: [0] bind variable at 0 is defined as GEOHASH(31b) and cannot accept STRING
                null\tbind error: [0] bind variable at 0 is defined as GEOHASH(31b) and cannot accept STRING
                ## where_key
                min\tb:min
                max\t
                null\tb:null
                ## copy_bind
                bind error: [0] bind variable at 0 is defined as GEOHASH(31b) and cannot accept STRING
                ## between_timestamp
                error: [27] there is no matching operator `between` with the argument type: GEOHASH(31b)
                ## eq_null_double
                props: random_access=true size=unknown timestamp=none
                k\tv
                null\t
                ## bind_value
                setBoolean\terror: [0] bind variable at 0 is defined as GEOHASH(31b) and cannot accept BOOLEAN
                setByte\terror: [0] bind variable at 0 is defined as GEOHASH(31b) and cannot accept BYTE
                setShort\terror: [0] bind variable at 0 is defined as GEOHASH(31b) and cannot accept SHORT
                setChar\terror: [0] bind variable at 0 is defined as GEOHASH(31b) and cannot accept CHAR
                setInt\terror: [0] bind variable at 0 is defined as GEOHASH(31b) and cannot accept INT
                setLong\terror: [0] bind variable at 0 is defined as GEOHASH(31b) and cannot accept LONG
                setFloat\terror: [0] bind variable at 0 is defined as GEOHASH(31b) and cannot accept FLOAT
                setDouble\terror: [0] bind variable at 0 is defined as GEOHASH(31b) and cannot accept DOUBLE
                setDate\terror: [0] bind variable at 0 is defined as GEOHASH(31b) and cannot accept DATE
                setTimestamp\terror: [0] bind variable at 0 is defined as GEOHASH(31b) and cannot accept TIMESTAMP
                setStr\terror: [0] bind variable at 0 is defined as GEOHASH(31b) and cannot accept STRING
                setVarchar\terror: [0] bind variable at 0 is defined as GEOHASH(31b) and cannot accept VARCHAR
                setLong256\terror: [0] bind variable at 0 is defined as GEOHASH(31b) and cannot accept LONG256
                setUuid\terror: [0] bind variable at 0 is defined as GEOHASH(31b) and cannot accept UUID
                setArray\terror: [0] bind variable at 0 is defined as GEOHASH(31b) and cannot accept ARRAY
                ## window_anchor
                error: create view: [147] ANCHOR EXPRESSION must return TIMESTAMP, LONG, or INT; got GEOHASH(31b)
                """);
        rec("GEOHASH(12c)", """
                ## cast
                target\tmin|max|null
                BOOLEAN\tno cast [10]
                BYTE\tno cast [10]
                SHORT\tno cast [10]
                CHAR\tno cast [10]
                INT\tno cast [10]
                LONG\tno cast [10]
                DATE\tno cast [10]
                TIMESTAMP\tno cast [10]
                FLOAT\tno cast [10]
                DOUBLE\tno cast [10]
                STRING\t000000000000|zzzzzzzzzzzz|
                SYMBOL\tno cast [10]
                LONG256\tno cast [10]
                GEOBYTE\t0000000|1111111|
                GEOSHORT\t000|zzz|
                GEOINT\t000000|zzzzzz|
                GEOLONG\t00000000|zzzzzzzz|
                BINARY\terror: [20] unsupported cast
                UUID\tno cast [10]
                LONG128\terror: [20] unsupported cast
                IPv4\tno cast [10]
                VARCHAR\t000000000000|zzzzzzzzzzzz|
                DOUBLE[]\tno cast [10]
                DECIMAL8\tno cast [10]
                DECIMAL16\tno cast [10]
                DECIMAL32\tno cast [10]
                DECIMAL64\tno cast [10]
                DECIMAL128\tno cast [10]
                DECIMAL256\tno cast [10]
                INTERVAL\terror: [20] unsupported cast
                VARCHAR_SLICE\terror: [20] unsupported cast
                TIMESTAMP_NS\tno cast [10]
                GEOHASH(1c)\t0|z|
                GEOHASH(8b)\t00000000|11111111|
                GEOHASH(31b)\t0000000000000000000000000000000|1111111111111111111111111111111|
                GEOHASH(12c)\t000000000000|zzzzzzzzzzzz|
                DECIMAL(5,2)\tno cast [10]
                DECIMAL(18,3)\tno cast [10]
                DOUBLE[][]\tno cast [10]
                INTERVAL(us)\terror: [20] unsupported cast
                INTERVAL(ns)\terror: [20] unsupported cast
                ## filter_eq
                props: random_access=true size=unknown timestamp=none
                k\tv
                max\tzzzzzzzzzzzz
                ## filter_ne
                props: random_access=true size=unknown timestamp=none
                k\tv
                min\t000000000000
                null\t
                ## filter_null
                props: random_access=true size=unknown timestamp=none
                k\tv
                null\t
                ## filter_not_null
                props: random_access=true size=unknown timestamp=none
                k\tv
                min\t000000000000
                max\tzzzzzzzzzzzz
                ## filter_lt
                error: [27] there is no matching operator `<` with the argument types: GEOHASH(12c) < GEOHASH(12c)
                ## filter_ge
                error: [27] there is no matching operator `>=` with the argument types: GEOHASH(12c) >= GEOHASH(12c)
                ## order_asc
                props: random_access=true size=known timestamp=none
                k\tv
                null\t
                min\t000000000000
                max\tzzzzzzzzzzzz
                ## order_desc
                props: random_access=true size=known timestamp=none
                k\tv
                max\tzzzzzzzzzzzz
                min\t000000000000
                null\t
                ## group_by
                props: random_access=true size=known timestamp=f:asc
                v\tc\tf
                000000000000\t2\t1970-01-01T00:00:00.000000Z
                zzzzzzzzzzzz\t2\t1970-01-01T00:00:01.000000Z
                \t2\t1970-01-01T00:00:02.000000Z
                ## join_inner
                props: random_access=true size=unknown timestamp=none
                tk\tuk\tv
                min\tmin\t000000000000
                null\tnull\t
                ## join_left
                props: random_access=true size=unknown timestamp=none
                tk\tuk\tuv
                max\t\t
                min\tmin\t000000000000
                null\tnull\t
                ## join_left_null
                props: random_access=true size=unknown timestamp=none
                tk\tuv
                max\t
                min\t000000000000
                null\t
                ## union_all
                props: random_access=false size=known timestamp=none
                k\tv
                min\t000000000000
                max\tzzzzzzzzzzzz
                null\t
                min\t000000000000
                null\t
                ## union_null
                props: random_access=false size=known timestamp=none
                k\tv
                min\t000000000000
                null\tnull
                null_branch\t
                ## case_no_else
                props: random_access=true size=known timestamp=none
                k\tc
                min\t
                max\tzzzzzzzzzzzz
                null\t
                ## case_else
                error: [35] inconvertible types: GEOHASH(12c) -> GEOHASH(12c) [from=GEOHASH(12c), to=GEOHASH(12c)]
                ## lag
                error: [13] there is no matching function `lag` with the argument types: (GEOHASH(12c))
                ## sample_by
                props: random_access=true size=known timestamp=ts:asc
                ts\tf\tl\tc
                1970-01-01T00:00:00.000000Z\t000000000000\tzzzzzzzzzzzz\t2
                1970-01-01T00:00:02.000000Z\t\t\t1
                ## first_last
                props: random_access=true size=known timestamp=none
                k\tf\tl
                A\t\tzzzzzzzzzzzz
                B\tzzzzzzzzzzzz\t
                C\t\t
                ## first_not_null
                props: random_access=true size=known timestamp=none
                k\tf\tl
                A\tzzzzzzzzzzzz\tzzzzzzzzzzzz
                B\tzzzzzzzzzzzz\tzzzzzzzzzzzz
                C\t\t
                ## fill_prev
                props: random_access=false size=unknown timestamp=ts:asc
                ts\tv
                1970-01-01T00:00:00.000000Z\t
                1970-01-01T00:00:01.000000Z\t
                1970-01-01T00:00:02.000000Z\tzzzzzzzzzzzz
                1970-01-01T00:00:03.000000Z\tzzzzzzzzzzzz
                1970-01-01T00:00:04.000000Z\t
                ## latest_on
                props: random_access=true size=known timestamp=none
                k\tv
                b:max\tzzzzzzzzzzzz
                b:min\t000000000000
                b:null\t
                ## fill_null
                props: random_access=false size=unknown timestamp=ts:asc
                ts\tv
                1970-01-01T00:00:00.000000Z\t000000000000
                1970-01-01T00:00:01.000000Z\t
                1970-01-01T00:00:02.000000Z\tzzzzzzzzzzzz
                1970-01-01T00:00:03.000000Z\t
                1970-01-01T00:00:04.000000Z\t
                ## fill_value
                props: random_access=false size=unknown timestamp=ts:asc
                ts\tv
                1970-01-01T00:00:00.000000Z\t000000000000
                1970-01-01T00:00:01.000000Z\tzzzzzzzzzzzz
                1970-01-01T00:00:02.000000Z\tzzzzzzzzzzzz
                1970-01-01T00:00:03.000000Z\tzzzzzzzzzzzz
                1970-01-01T00:00:04.000000Z\t
                ## fill_linear
                error: [11] Unsupported interpolation type: GEOHASH(12c)
                ## subsample_stride
                min\terror: [38] integer expected for stride
                max\terror: [38] integer expected for stride
                null\terror: [38] integer expected for stride
                ## subsample_target
                min\terror: [38] integer expected for target point count
                max\terror: [38] integer expected for target point count
                null\terror: [38] integer expected for target point count
                ## memoized
                props: random_access=true size=known timestamp=none
                k\ta\ta2\ta3
                min\t000000000000\t000000000000\t000000000000
                max\tzzzzzzzzzzzz\tzzzzzzzzzzzz\tzzzzzzzzzzzz
                null\t\t\t
                plan memoizes: false
                ## where_bound_const
                min\terror: [31] there is no matching operator `>=` with the argument types: LONG >= GEOHASH(12c)
                max\terror: [31] there is no matching operator `>=` with the argument types: LONG >= GEOHASH(12c)
                null\terror: [31] there is no matching operator `>=` with the argument types: LONG >= GEOHASH(12c)
                ## where_bound_bind
                min\tbind error: [0] bind variable at 0 is defined as GEOHASH(12c) and cannot accept STRING
                max\tbind error: [0] bind variable at 0 is defined as GEOHASH(12c) and cannot accept STRING
                null\tbind error: [0] bind variable at 0 is defined as GEOHASH(12c) and cannot accept STRING
                ## where_key
                min\tb:min
                max\tb:max
                null\tb:null
                ## copy_bind
                bind error: [0] bind variable at 0 is defined as GEOHASH(12c) and cannot accept STRING
                ## between_timestamp
                error: [27] there is no matching operator `between` with the argument type: GEOHASH(12c)
                ## eq_null_double
                props: random_access=true size=unknown timestamp=none
                k\tv
                null\t
                ## bind_value
                setBoolean\terror: [0] bind variable at 0 is defined as GEOHASH(12c) and cannot accept BOOLEAN
                setByte\terror: [0] bind variable at 0 is defined as GEOHASH(12c) and cannot accept BYTE
                setShort\terror: [0] bind variable at 0 is defined as GEOHASH(12c) and cannot accept SHORT
                setChar\terror: [0] bind variable at 0 is defined as GEOHASH(12c) and cannot accept CHAR
                setInt\terror: [0] bind variable at 0 is defined as GEOHASH(12c) and cannot accept INT
                setLong\terror: [0] bind variable at 0 is defined as GEOHASH(12c) and cannot accept LONG
                setFloat\terror: [0] bind variable at 0 is defined as GEOHASH(12c) and cannot accept FLOAT
                setDouble\terror: [0] bind variable at 0 is defined as GEOHASH(12c) and cannot accept DOUBLE
                setDate\terror: [0] bind variable at 0 is defined as GEOHASH(12c) and cannot accept DATE
                setTimestamp\terror: [0] bind variable at 0 is defined as GEOHASH(12c) and cannot accept TIMESTAMP
                setStr\terror: [0] bind variable at 0 is defined as GEOHASH(12c) and cannot accept STRING
                setVarchar\terror: [0] bind variable at 0 is defined as GEOHASH(12c) and cannot accept VARCHAR
                setLong256\terror: [0] bind variable at 0 is defined as GEOHASH(12c) and cannot accept LONG256
                setUuid\terror: [0] bind variable at 0 is defined as GEOHASH(12c) and cannot accept UUID
                setArray\terror: [0] bind variable at 0 is defined as GEOHASH(12c) and cannot accept ARRAY
                ## window_anchor
                error: create view: [147] ANCHOR EXPRESSION must return TIMESTAMP, LONG, or INT; got GEOHASH(12c)
                """);
        rec("DECIMAL(5,2)", """
                ## cast
                target\tmin|max|null
                BOOLEAN\tno cast [10]
                BYTE\terror: inconvertible value: -999.99 [DECIMAL(5,2) -> BYTE]
                SHORT\t-999|999|0
                CHAR\tno cast [10]
                INT\t-999|999|null
                LONG\t-999|999|null
                DATE\tno cast [10]
                TIMESTAMP\tno cast [10]
                FLOAT\t-999.99|999.99|null
                DOUBLE\t-999.99|999.99|null
                STRING\t-999.99|999.99|
                SYMBOL\tno cast [10]
                LONG256\tno cast [10]
                GEOBYTE\tno cast [10]
                GEOSHORT\tno cast [10]
                GEOINT\tno cast [10]
                GEOLONG\tno cast [10]
                BINARY\terror: [20] unsupported cast
                UUID\tno cast [10]
                LONG128\terror: [20] unsupported cast
                IPv4\tno cast [10]
                VARCHAR\t-999.99|999.99|
                DOUBLE[]\tno cast [10]
                DECIMAL8\terror: inconvertible value: -999.99 [DECIMAL(5,2) -> DECIMAL(2,1)]
                DECIMAL16\terror: inconvertible value: -99999 [DECIMAL(5,2) -> DECIMAL(4,2)]
                DECIMAL32\terror: inconvertible value: -999.99 [DECIMAL(5,2) -> DECIMAL(9,0)]
                DECIMAL64\t-999.9900|999.9900|
                DECIMAL128\t-999.9900000000|999.9900000000|
                DECIMAL256\t-999.99000000000000000000|999.99000000000000000000|
                INTERVAL\terror: [20] unsupported cast
                VARCHAR_SLICE\terror: [20] unsupported cast
                TIMESTAMP_NS\tno cast [10]
                GEOHASH(1c)\tno cast [10]
                GEOHASH(8b)\tno cast [10]
                GEOHASH(31b)\tno cast [10]
                GEOHASH(12c)\tno cast [10]
                DECIMAL(5,2)\t-999.99|999.99|
                DECIMAL(18,3)\t-999.990|999.990|
                DOUBLE[][]\tno cast [10]
                INTERVAL(us)\terror: [20] unsupported cast
                INTERVAL(ns)\terror: [20] unsupported cast
                ## filter_eq
                props: random_access=true size=unknown timestamp=none
                k\tv
                max\t999.99
                ## filter_ne
                props: random_access=true size=unknown timestamp=none
                k\tv
                min\t-999.99
                null\t
                ## filter_null
                props: random_access=true size=unknown timestamp=none
                k\tv
                null\t
                ## filter_not_null
                props: random_access=true size=unknown timestamp=none
                k\tv
                min\t-999.99
                max\t999.99
                ## filter_lt
                props: random_access=true size=unknown timestamp=none
                k\tv
                min\t-999.99
                ## filter_ge
                props: random_access=true size=unknown timestamp=none
                k\tv
                max\t999.99
                ## order_asc
                props: random_access=true size=known timestamp=none
                k\tv
                null\t
                min\t-999.99
                max\t999.99
                ## order_desc
                props: random_access=true size=known timestamp=none
                k\tv
                max\t999.99
                min\t-999.99
                null\t
                ## group_by
                props: random_access=true size=known timestamp=f:asc
                v\tc\tf
                -999.99\t2\t1970-01-01T00:00:00.000000Z
                999.99\t2\t1970-01-01T00:00:01.000000Z
                \t2\t1970-01-01T00:00:02.000000Z
                ## join_inner
                props: random_access=true size=unknown timestamp=none
                tk\tuk\tv
                min\tmin\t-999.99
                null\tnull\t
                ## join_left
                props: random_access=true size=unknown timestamp=none
                tk\tuk\tuv
                max\t\t
                min\tmin\t-999.99
                null\tnull\t
                ## join_left_null
                props: random_access=true size=unknown timestamp=none
                tk\tuv
                max\t
                min\t-999.99
                null\t
                ## union_all
                props: random_access=false size=known timestamp=none
                k\tv
                min\t-999.99
                max\t999.99
                null\t
                min\t-999.99
                null\t
                ## union_null
                props: random_access=false size=known timestamp=none
                k\tv
                min\t-999.99
                null\t
                null_branch\t
                ## case_no_else
                props: random_access=true size=known timestamp=none
                k\tc
                min\t
                max\t999.99
                null\t
                ## case_else
                error: [20] type DECIMAL(5,2) is not supported in 'switch' type of 'case' statement
                ## lag
                props: random_access=false size=known timestamp=none
                k\tv\tp
                min\t-999.99\t
                max\t999.99\t-999.99
                null\t\t999.99
                ## sample_by
                props: random_access=true size=known timestamp=ts:asc
                ts\tf\tl\tc
                1970-01-01T00:00:00.000000Z\t-999.99\t999.99\t2
                1970-01-01T00:00:02.000000Z\t\t\t1
                ## first_last
                props: random_access=true size=known timestamp=none
                k\tf\tl
                A\t\t999.99
                B\t999.99\t
                C\t\t
                ## first_not_null
                props: random_access=true size=known timestamp=none
                k\tf\tl
                A\t999.99\t999.99
                B\t999.99\t999.99
                C\t\t
                ## fill_prev
                props: random_access=false size=unknown timestamp=ts:asc
                ts\tv
                1970-01-01T00:00:00.000000Z\t
                1970-01-01T00:00:01.000000Z\t
                1970-01-01T00:00:02.000000Z\t999.99
                1970-01-01T00:00:03.000000Z\t999.99
                1970-01-01T00:00:04.000000Z\t
                ## latest_on
                error: [47] v (DECIMAL(5,2)): invalid type, only [BOOLEAN, BYTE, SHORT, INT, LONG, DATE, TIMESTAMP, FLOAT, DOUBLE, LONG128, LONG256, CHAR, STRING, VARCHAR, SYMBOL, UUID, GEOHASH, IPv4] are supported in LATEST ON
                ## fill_null
                props: random_access=false size=unknown timestamp=ts:asc
                ts\tv
                1970-01-01T00:00:00.000000Z\t-999.99
                1970-01-01T00:00:01.000000Z\t
                1970-01-01T00:00:02.000000Z\t999.99
                1970-01-01T00:00:03.000000Z\t
                1970-01-01T00:00:04.000000Z\t
                ## fill_value
                error: [46] fill value of type DOUBLE cannot fill column of type DECIMAL(5,2)
                ## fill_linear
                error: [46] support for LINEAR fill is not yet implemented [function=last(v), class=io.questdb.griffin.engine.functions.groupby.LastDecimalGroupByFunctionFactory$Decimal32Func]
                ## subsample_stride
                min\terror: [47] integer expected for stride
                max\terror: [46] integer expected for stride
                null\terror: [38] integer expected for stride
                ## subsample_target
                min\terror: [47] integer expected for target point count
                max\terror: [46] integer expected for target point count
                null\terror: [38] integer expected for target point count
                ## memoized
                props: random_access=true size=known timestamp=none
                k\ta\ta2\ta3
                min\t-999.99\t-999.99\t-999.99
                max\t999.99\t999.99\t999.99
                null\t\t\t
                plan memoizes: true
                ## where_bound_const
                min\tmin,max,null
                max\tmax,null
                null\t
                ## where_bound_bind
                min\tmin,max,null
                max\tmax,null
                null\t
                ## where_key
                min\terror: [63] v (DECIMAL(5,2)): invalid type, only [BOOLEAN, BYTE, SHORT, INT, LONG, DATE, TIMESTAMP, FLOAT, DOUBLE, LONG128, LONG256, CHAR, STRING, VARCHAR, SYMBOL, UUID, GEOHASH, IPv4] are supported in LATEST ON
                max\terror: [62] v (DECIMAL(5,2)): invalid type, only [BOOLEAN, BYTE, SHORT, INT, LONG, DATE, TIMESTAMP, FLOAT, DOUBLE, LONG128, LONG256, CHAR, STRING, VARCHAR, SYMBOL, UUID, GEOHASH, IPv4] are supported in LATEST ON
                null\terror: [58] v (DECIMAL(5,2)): invalid type, only [BOOLEAN, BYTE, SHORT, INT, LONG, DATE, TIMESTAMP, FLOAT, DOUBLE, LONG128, LONG256, CHAR, STRING, VARCHAR, SYMBOL, UUID, GEOHASH, IPv4] are supported in LATEST ON
                ## copy_bind
                status\tmessage
                finished\t
                k
                max
                ## between_timestamp
                error: [27] there is no matching operator `between` with the argument type: DECIMAL(5,2)
                ## eq_null_double
                props: random_access=true size=unknown timestamp=none
                k\tv
                null\t
                ## bind_value
                setBoolean\terror: [0] bind variable at 0 is defined as DECIMAL(5,2) and cannot accept BOOLEAN
                setByte\terror: [0] bind variable at 0 is defined as DECIMAL(5,2) and cannot accept BYTE
                setShort\terror: [0] bind variable at 0 is defined as DECIMAL(5,2) and cannot accept SHORT
                setChar\terror: [0] bind variable at 0 is defined as DECIMAL(5,2) and cannot accept CHAR
                setInt\terror: [0] bind variable at 0 is defined as DECIMAL(5,2) and cannot accept INT
                setLong\terror: [0] bind variable at 0 is defined as DECIMAL(5,2) and cannot accept LONG
                setFloat\terror: [0] bind variable at 0 is defined as DECIMAL(5,2) and cannot accept FLOAT
                setDouble\terror: [0] bind variable at 0 is defined as DECIMAL(5,2) and cannot accept DOUBLE
                setDate\terror: [0] bind variable at 0 is defined as DECIMAL(5,2) and cannot accept DATE
                setTimestamp\terror: [0] bind variable at 0 is defined as DECIMAL(5,2) and cannot accept TIMESTAMP
                setStr\t1.00
                setVarchar\t1.00
                setLong256\terror: [0] bind variable at 0 is defined as DECIMAL(5,2) and cannot accept LONG256
                setUuid\terror: [0] bind variable at 0 is defined as DECIMAL(5,2) and cannot accept UUID
                setArray\terror: [0] bind variable at 0 is defined as DECIMAL(5,2) and cannot accept ARRAY
                ## window_anchor
                error: create view: [147] ANCHOR EXPRESSION must return TIMESTAMP, LONG, or INT; got DECIMAL(5,2)
                """);
        rec("DECIMAL(18,3)", """
                ## filter_eq
                props: random_access=true size=unknown timestamp=none
                k\tv
                max\t999999999999999.999
                ## filter_ne
                props: random_access=true size=unknown timestamp=none
                k\tv
                min\t-999999999999999.999
                null\t
                ## filter_null
                props: random_access=true size=unknown timestamp=none
                k\tv
                null\t
                ## filter_not_null
                props: random_access=true size=unknown timestamp=none
                k\tv
                min\t-999999999999999.999
                max\t999999999999999.999
                ## filter_lt
                props: random_access=true size=unknown timestamp=none
                k\tv
                min\t-999999999999999.999
                ## filter_ge
                props: random_access=true size=unknown timestamp=none
                k\tv
                max\t999999999999999.999
                ## order_asc
                props: random_access=true size=known timestamp=none
                k\tv
                null\t
                min\t-999999999999999.999
                max\t999999999999999.999
                ## order_desc
                props: random_access=true size=known timestamp=none
                k\tv
                max\t999999999999999.999
                min\t-999999999999999.999
                null\t
                ## group_by
                props: random_access=true size=known timestamp=f:asc
                v\tc\tf
                -999999999999999.999\t2\t1970-01-01T00:00:00.000000Z
                999999999999999.999\t2\t1970-01-01T00:00:01.000000Z
                \t2\t1970-01-01T00:00:02.000000Z
                ## join_inner
                props: random_access=true size=unknown timestamp=none
                tk\tuk\tv
                min\tmin\t-999999999999999.999
                null\tnull\t
                ## join_left
                props: random_access=true size=unknown timestamp=none
                tk\tuk\tuv
                max\t\t
                min\tmin\t-999999999999999.999
                null\tnull\t
                ## join_left_null
                props: random_access=true size=unknown timestamp=none
                tk\tuv
                max\t
                min\t-999999999999999.999
                null\t
                ## union_all
                props: random_access=false size=known timestamp=none
                k\tv
                min\t-999999999999999.999
                max\t999999999999999.999
                null\t
                min\t-999999999999999.999
                null\t
                ## union_null
                props: random_access=false size=known timestamp=none
                k\tv
                min\t-999999999999999.999
                null\t
                null_branch\t
                ## case_no_else
                props: random_access=true size=known timestamp=none
                k\tc
                min\t
                max\t999999999999999.999
                null\t
                ## case_else
                error: [20] type DECIMAL(18,3) is not supported in 'switch' type of 'case' statement
                ## lag
                props: random_access=false size=known timestamp=none
                k\tv\tp
                min\t-999999999999999.999\t
                max\t999999999999999.999\t-999999999999999.999
                null\t\t999999999999999.999
                ## sample_by
                props: random_access=true size=known timestamp=ts:asc
                ts\tf\tl\tc
                1970-01-01T00:00:00.000000Z\t-999999999999999.999\t999999999999999.999\t2
                1970-01-01T00:00:02.000000Z\t\t\t1
                ## first_last
                props: random_access=true size=known timestamp=none
                k\tf\tl
                A\t\t999999999999999.999
                B\t999999999999999.999\t
                C\t\t
                ## first_not_null
                props: random_access=true size=known timestamp=none
                k\tf\tl
                A\t999999999999999.999\t999999999999999.999
                B\t999999999999999.999\t999999999999999.999
                C\t\t
                ## fill_prev
                props: random_access=false size=unknown timestamp=ts:asc
                ts\tv
                1970-01-01T00:00:00.000000Z\t
                1970-01-01T00:00:01.000000Z\t
                1970-01-01T00:00:02.000000Z\t999999999999999.999
                1970-01-01T00:00:03.000000Z\t999999999999999.999
                1970-01-01T00:00:04.000000Z\t
                ## latest_on
                error: [47] v (DECIMAL(18,3)): invalid type, only [BOOLEAN, BYTE, SHORT, INT, LONG, DATE, TIMESTAMP, FLOAT, DOUBLE, LONG128, LONG256, CHAR, STRING, VARCHAR, SYMBOL, UUID, GEOHASH, IPv4] are supported in LATEST ON
                ## cast
                target\tmin|max|null
                BOOLEAN\tno cast [10]
                BYTE\terror: inconvertible value: -999999999999999.999 [DECIMAL(18,3) -> BYTE]
                SHORT\terror: inconvertible value: -999999999999999.999 [DECIMAL(18,3) -> SHORT]
                CHAR\tno cast [10]
                INT\terror: inconvertible value: -999999999999999.999 [DECIMAL(18,3) -> INT]
                LONG\t-999999999999999|999999999999999|null
                DATE\tno cast [10]
                TIMESTAMP\tno cast [10]
                FLOAT\t-1.0E15|1.0E15|null
                DOUBLE\t-1.0E15|1.0E15|null
                STRING\t-999999999999999.999|999999999999999.999|
                SYMBOL\tno cast [10]
                LONG256\tno cast [10]
                GEOBYTE\tno cast [10]
                GEOSHORT\tno cast [10]
                GEOINT\tno cast [10]
                GEOLONG\tno cast [10]
                BINARY\terror: [20] unsupported cast
                UUID\tno cast [10]
                LONG128\terror: [20] unsupported cast
                IPv4\tno cast [10]
                VARCHAR\t-999999999999999.999|999999999999999.999|
                DOUBLE[]\tno cast [10]
                DECIMAL8\terror: inconvertible value: -999999999999999.999 [DECIMAL(18,3) -> DECIMAL(2,1)]
                DECIMAL16\terror: inconvertible value: -999999999999999.999 [DECIMAL(18,3) -> DECIMAL(4,2)]
                DECIMAL32\terror: inconvertible value: -999999999999999.999 [DECIMAL(18,3) -> DECIMAL(9,0)]
                DECIMAL64\terror: inconvertible value: -999999999999999.999 [DECIMAL(18,3) -> DECIMAL(16,4)]
                DECIMAL128\t-999999999999999.9990000000|999999999999999.9990000000|
                DECIMAL256\t-999999999999999.99900000000000000000|999999999999999.99900000000000000000|
                INTERVAL\terror: [20] unsupported cast
                VARCHAR_SLICE\terror: [20] unsupported cast
                TIMESTAMP_NS\tno cast [10]
                GEOHASH(1c)\tno cast [10]
                GEOHASH(8b)\tno cast [10]
                GEOHASH(31b)\tno cast [10]
                GEOHASH(12c)\tno cast [10]
                DECIMAL(5,2)\terror: inconvertible value: -999999999999999.999 [DECIMAL(18,3) -> DECIMAL(5,2)]
                DECIMAL(18,3)\t-999999999999999.999|999999999999999.999|
                DOUBLE[][]\tno cast [10]
                INTERVAL(us)\terror: [20] unsupported cast
                INTERVAL(ns)\terror: [20] unsupported cast
                ## fill_null
                props: random_access=false size=unknown timestamp=ts:asc
                ts\tv
                1970-01-01T00:00:00.000000Z\t-999999999999999.999
                1970-01-01T00:00:01.000000Z\t
                1970-01-01T00:00:02.000000Z\t999999999999999.999
                1970-01-01T00:00:03.000000Z\t
                1970-01-01T00:00:04.000000Z\t
                ## fill_value
                error: [46] fill value of type DOUBLE cannot fill column of type DECIMAL(18,3)
                ## fill_linear
                error: [46] support for LINEAR fill is not yet implemented [function=last(v), class=io.questdb.griffin.engine.functions.groupby.LastDecimalGroupByFunctionFactory$Decimal64Func]
                ## subsample_stride
                min\terror: [60] integer expected for stride
                max\terror: [59] integer expected for stride
                null\terror: [38] integer expected for stride
                ## subsample_target
                min\terror: [60] integer expected for target point count
                max\terror: [59] integer expected for target point count
                null\terror: [38] integer expected for target point count
                ## memoized
                props: random_access=true size=known timestamp=none
                k\ta\ta2\ta3
                min\t-999999999999999.999\t-999999999999999.999\t-999999999999999.999
                max\t999999999999999.999\t999999999999999.999\t999999999999999.999
                null\t\t\t
                plan memoizes: true
                ## where_bound_const
                min\tmin,max,null
                max\t
                null\t
                ## where_bound_bind
                min\tmin,max,null
                max\t
                null\t
                ## where_key
                min\terror: [76] v (DECIMAL(18,3)): invalid type, only [BOOLEAN, BYTE, SHORT, INT, LONG, DATE, TIMESTAMP, FLOAT, DOUBLE, LONG128, LONG256, CHAR, STRING, VARCHAR, SYMBOL, UUID, GEOHASH, IPv4] are supported in LATEST ON
                max\terror: [75] v (DECIMAL(18,3)): invalid type, only [BOOLEAN, BYTE, SHORT, INT, LONG, DATE, TIMESTAMP, FLOAT, DOUBLE, LONG128, LONG256, CHAR, STRING, VARCHAR, SYMBOL, UUID, GEOHASH, IPv4] are supported in LATEST ON
                null\terror: [58] v (DECIMAL(18,3)): invalid type, only [BOOLEAN, BYTE, SHORT, INT, LONG, DATE, TIMESTAMP, FLOAT, DOUBLE, LONG128, LONG256, CHAR, STRING, VARCHAR, SYMBOL, UUID, GEOHASH, IPv4] are supported in LATEST ON
                ## copy_bind
                status\tmessage
                finished\t
                k
                max
                ## between_timestamp
                error: [27] there is no matching operator `between` with the argument type: DECIMAL(18,3)
                ## eq_null_double
                props: random_access=true size=unknown timestamp=none
                k\tv
                null\t
                ## bind_value
                setBoolean\terror: [0] bind variable at 0 is defined as DECIMAL(18,3) and cannot accept BOOLEAN
                setByte\terror: [0] bind variable at 0 is defined as DECIMAL(18,3) and cannot accept BYTE
                setShort\terror: [0] bind variable at 0 is defined as DECIMAL(18,3) and cannot accept SHORT
                setChar\terror: [0] bind variable at 0 is defined as DECIMAL(18,3) and cannot accept CHAR
                setInt\terror: [0] bind variable at 0 is defined as DECIMAL(18,3) and cannot accept INT
                setLong\terror: [0] bind variable at 0 is defined as DECIMAL(18,3) and cannot accept LONG
                setFloat\terror: [0] bind variable at 0 is defined as DECIMAL(18,3) and cannot accept FLOAT
                setDouble\terror: [0] bind variable at 0 is defined as DECIMAL(18,3) and cannot accept DOUBLE
                setDate\terror: [0] bind variable at 0 is defined as DECIMAL(18,3) and cannot accept DATE
                setTimestamp\terror: [0] bind variable at 0 is defined as DECIMAL(18,3) and cannot accept TIMESTAMP
                setStr\t1.000
                setVarchar\t1.000
                setLong256\terror: [0] bind variable at 0 is defined as DECIMAL(18,3) and cannot accept LONG256
                setUuid\terror: [0] bind variable at 0 is defined as DECIMAL(18,3) and cannot accept UUID
                setArray\terror: [0] bind variable at 0 is defined as DECIMAL(18,3) and cannot accept ARRAY
                ## window_anchor
                error: create view: [147] ANCHOR EXPRESSION must return TIMESTAMP, LONG, or INT; got DECIMAL(18,3)
                """);
        rec("DOUBLE[][]", """
                ## filter_eq
                props: random_access=true size=unknown timestamp=none
                k\tv
                max\t[[1.7976931348623157E308]]
                ## filter_ne
                props: random_access=true size=unknown timestamp=none
                k\tv
                min\t[[-1.7976931348623157E308]]
                empty\t[]
                specials\t[[null,null,null,-0.0]]
                null\tnull
                ## filter_null
                error: [27] there is no matching operator `=` with the argument types: DOUBLE[][] = NULL
                ## filter_not_null
                error: [27] there is no matching operator `!=` with the argument types: DOUBLE[][] != NULL
                ## filter_lt
                error: [27] there is no matching operator `<` with the argument types: DOUBLE[][] < DOUBLE[][]
                ## filter_ge
                error: [27] there is no matching operator `>=` with the argument types: DOUBLE[][] >= DOUBLE[][]
                ## order_asc
                error: [28] DOUBLE[][] is not a supported type in ORDER BY clause
                ## order_desc
                error: [28] DOUBLE[][] is not a supported type in ORDER BY clause
                ## group_by
                props: random_access=true size=known timestamp=f:asc
                v\tc\tf
                [[-1.7976931348623157E308]]\t2\t1970-01-01T00:00:00.000000Z
                [[1.7976931348623157E308]]\t2\t1970-01-01T00:00:01.000000Z
                []\t2\t1970-01-01T00:00:02.000000Z
                [[null,null,null,-0.0]]\t2\t1970-01-01T00:00:03.000000Z
                null\t2\t1970-01-01T00:00:04.000000Z
                ## join_inner
                props: random_access=true size=unknown timestamp=none
                tk\tuk\tv
                empty\tempty\t[]
                min\tmin\t[[-1.7976931348623157E308]]
                null\tnull\tnull
                ## join_left
                props: random_access=true size=unknown timestamp=none
                tk\tuk\tuv
                empty\tempty\t[]
                max\t\tnull
                min\tmin\t[[-1.7976931348623157E308]]
                null\tnull\tnull
                specials\t\tnull
                ## join_left_null
                props: random_access=true size=unknown timestamp=none
                tk\tuv
                empty\t[]
                max\tnull
                min\t[[-1.7976931348623157E308]]
                null\tnull
                specials\tnull
                ## union_all
                props: random_access=false size=known timestamp=none
                k\tv
                min\t[[-1.7976931348623157E308]]
                max\t[[1.7976931348623157E308]]
                empty\t[]
                specials\t[[null,null,null,-0.0]]
                null\tnull
                min\t[[-1.7976931348623157E308]]
                empty\t[]
                null\tnull
                ## union_null
                props: random_access=false size=known timestamp=none
                k\tv
                min\t[[-1.7976931348623157E308]]
                empty\t[]
                null\t
                null_branch\t
                ## case_no_else
                props: random_access=true size=known timestamp=none
                k\tc
                min\tnull
                max\t[[1.7976931348623157E308]]
                empty\tnull
                specials\tnull
                null\tnull
                ## case_else
                error: [20] type DOUBLE[][] is not supported in 'switch' type of 'case' statement
                ## lag
                error: [13] there is no matching function `lag` with the argument types: (DOUBLE[][])
                ## sample_by
                props: random_access=true size=known timestamp=ts:asc
                ts\tf\tl\tc
                1970-01-01T00:00:00.000000Z\t[[-1.7976931348623157E308]]\t[[1.7976931348623157E308]]\t2
                1970-01-01T00:00:02.000000Z\t[]\t[[null,null,null,-0.0]]\t2
                1970-01-01T00:00:04.000000Z\tnull\tnull\t1
                ## first_last
                props: random_access=true size=known timestamp=none
                k\tf\tl
                A\tnull\t[[1.7976931348623157E308]]
                B\t[[1.7976931348623157E308]]\tnull
                C\tnull\tnull
                ## first_not_null
                props: random_access=true size=known timestamp=none
                k\tf\tl
                A\t[[1.7976931348623157E308]]\t[[1.7976931348623157E308]]
                B\t[[1.7976931348623157E308]]\t[[1.7976931348623157E308]]
                C\tnull\tnull
                ## fill_prev
                props: random_access=false size=unknown timestamp=ts:asc
                ts\tv
                1970-01-01T00:00:00.000000Z\tnull
                1970-01-01T00:00:01.000000Z\tnull
                1970-01-01T00:00:02.000000Z\t[[1.7976931348623157E308]]
                1970-01-01T00:00:03.000000Z\t[[1.7976931348623157E308]]
                1970-01-01T00:00:04.000000Z\tnull
                ## latest_on
                error: [47] v (DOUBLE[][]): invalid type, only [BOOLEAN, BYTE, SHORT, INT, LONG, DATE, TIMESTAMP, FLOAT, DOUBLE, LONG128, LONG256, CHAR, STRING, VARCHAR, SYMBOL, UUID, GEOHASH, IPv4] are supported in LATEST ON
                ## cast
                target\tmin|max|empty|specials|null
                BOOLEAN\tno cast [10]
                BYTE\tno cast [10]
                SHORT\tno cast [10]
                CHAR\tno cast [10]
                INT\tno cast [10]
                LONG\tno cast [10]
                DATE\tno cast [10]
                TIMESTAMP\tno cast [10]
                FLOAT\tno cast [10]
                DOUBLE\tno cast [10]
                STRING\t[[-1.7976931348623157E308]]|[[1.7976931348623157E308]]|[]|[[null,null,null,-0.0]]|
                SYMBOL\tno cast [10]
                LONG256\tno cast [10]
                GEOBYTE\tno cast [10]
                GEOSHORT\tno cast [10]
                GEOINT\tno cast [10]
                GEOLONG\tno cast [10]
                BINARY\terror: [20] unsupported cast
                UUID\tno cast [10]
                LONG128\terror: [20] unsupported cast
                IPv4\tno cast [10]
                VARCHAR\t[[-1.7976931348623157E308]]|[[1.7976931348623157E308]]|[]|[[null,null,null,-0.0]]|
                DOUBLE[]\terror: [10] cannot cast array to lower dimension [from=DOUBLE[][] (2D), to=DOUBLE[] (1D)]. Use array flattening operation (e.g. 'flatten(arr)') instead
                DECIMAL8\tno cast [10]
                DECIMAL16\tno cast [10]
                DECIMAL32\tno cast [10]
                DECIMAL64\tno cast [10]
                DECIMAL128\tno cast [10]
                DECIMAL256\tno cast [10]
                INTERVAL\terror: [20] unsupported cast
                VARCHAR_SLICE\terror: [20] unsupported cast
                TIMESTAMP_NS\tno cast [10]
                GEOHASH(1c)\tno cast [10]
                GEOHASH(8b)\tno cast [10]
                GEOHASH(31b)\tno cast [10]
                GEOHASH(12c)\tno cast [10]
                DECIMAL(5,2)\tno cast [10]
                DECIMAL(18,3)\tno cast [10]
                DOUBLE[][]\t[[-1.7976931348623157E308]]|[[1.7976931348623157E308]]|[]|[[null,null,null,-0.0]]|null
                INTERVAL(us)\terror: [20] unsupported cast
                INTERVAL(ns)\terror: [20] unsupported cast
                ## fill_null
                props: random_access=false size=unknown timestamp=ts:asc
                ts\tv
                1970-01-01T00:00:00.000000Z\t[[-1.7976931348623157E308]]
                1970-01-01T00:00:01.000000Z\tnull
                1970-01-01T00:00:02.000000Z\t[[1.7976931348623157E308]]
                1970-01-01T00:00:03.000000Z\tnull
                1970-01-01T00:00:04.000000Z\tnull
                ## fill_value
                error: [46] support for VALUE fill is not yet implemented [function=last(v), class=io.questdb.griffin.engine.functions.groupby.LastArrayGroupByFunction]
                ## fill_linear
                error: [46] support for LINEAR fill is not yet implemented [function=last(v), class=io.questdb.griffin.engine.functions.groupby.LastArrayGroupByFunction]
                ## subsample_stride
                min\terror: [43] integer expected for stride
                max\terror: [43] integer expected for stride
                empty\terror: [45] integer expected for stride
                specials\terror: [43] integer expected for stride
                null\terror: [38] integer expected for stride
                ## subsample_target
                min\terror: [43] integer expected for target point count
                max\terror: [43] integer expected for target point count
                empty\terror: [45] integer expected for target point count
                specials\terror: [43] integer expected for target point count
                null\terror: [38] integer expected for target point count
                ## memoized
                props: random_access=true size=known timestamp=none
                k\ta\ta2\ta3
                min\t[[-1.7976931348623157E308]]\t[[-1.7976931348623157E308]]\t[[-1.7976931348623157E308]]
                max\t[[1.7976931348623157E308]]\t[[1.7976931348623157E308]]\t[[1.7976931348623157E308]]
                empty\t[]\t[]\t[]
                specials\t[[null,null,null,-0.0]]\t[[null,null,null,-0.0]]\t[[null,null,null,-0.0]]
                null\tnull\tnull\tnull
                plan memoizes: true
                ## where_bound_const
                min\terror: [31] there is no matching operator `>=` with the argument types: LONG >= DOUBLE[][]
                max\terror: [31] there is no matching operator `>=` with the argument types: LONG >= DOUBLE[][]
                empty\terror: [31] there is no matching operator `>=` with the argument types: LONG >= DOUBLE[][]
                specials\terror: [31] there is no matching operator `>=` with the argument types: LONG >= DOUBLE[][]
                null\terror: [31] there is no matching operator `>=` with the argument types: LONG >= DOUBLE[][]
                ## where_bound_bind
                min\terror: [31] there is no matching operator `>=` with the argument types: LONG >= DOUBLE[][]
                max\terror: [31] there is no matching operator `>=` with the argument types: LONG >= DOUBLE[][]
                empty\terror: [31] there is no matching operator `>=` with the argument types: LONG >= DOUBLE[][]
                specials\tbind error: inconvertible value: `[[null,null,null,-0.0]]` [STRING -> DOUBLE[][]]
                null\tbind error: [-1] array type mismatch [expected=DOUBLE[][], actual=NULL]
                ## where_key
                min\terror: [83] v (DOUBLE[][]): invalid type, only [BOOLEAN, BYTE, SHORT, INT, LONG, DATE, TIMESTAMP, FLOAT, DOUBLE, LONG128, LONG256, CHAR, STRING, VARCHAR, SYMBOL, UUID, GEOHASH, IPv4] are supported in LATEST ON
                max\terror: [82] v (DOUBLE[][]): invalid type, only [BOOLEAN, BYTE, SHORT, INT, LONG, DATE, TIMESTAMP, FLOAT, DOUBLE, LONG128, LONG256, CHAR, STRING, VARCHAR, SYMBOL, UUID, GEOHASH, IPv4] are supported in LATEST ON
                empty\terror: [58] v (DOUBLE[][]): invalid type, only [BOOLEAN, BYTE, SHORT, INT, LONG, DATE, TIMESTAMP, FLOAT, DOUBLE, LONG128, LONG256, CHAR, STRING, VARCHAR, SYMBOL, UUID, GEOHASH, IPv4] are supported in LATEST ON
                specials\terror: [79] v (DOUBLE[][]): invalid type, only [BOOLEAN, BYTE, SHORT, INT, LONG, DATE, TIMESTAMP, FLOAT, DOUBLE, LONG128, LONG256, CHAR, STRING, VARCHAR, SYMBOL, UUID, GEOHASH, IPv4] are supported in LATEST ON
                null\terror: [58] v (DOUBLE[][]): invalid type, only [BOOLEAN, BYTE, SHORT, INT, LONG, DATE, TIMESTAMP, FLOAT, DOUBLE, LONG128, LONG256, CHAR, STRING, VARCHAR, SYMBOL, UUID, GEOHASH, IPv4] are supported in LATEST ON
                ## copy_bind
                status\tmessage
                finished\t
                k
                ## between_timestamp
                error: [27] there is no matching operator `between` with the argument type: DOUBLE[][]
                ## eq_null_double
                error: [27] there is no matching operator `=` with the argument types: DOUBLE[][] = NULL
                ## bind_value
                setBoolean\terror: [0] bind variable at 0 is defined as DOUBLE[][] and cannot accept BOOLEAN
                setByte\terror: [0] bind variable at 0 is defined as DOUBLE[][] and cannot accept BYTE
                setShort\terror: [0] bind variable at 0 is defined as DOUBLE[][] and cannot accept SHORT
                setChar\terror: [0] bind variable at 0 is defined as DOUBLE[][] and cannot accept CHAR
                setInt\terror: [0] bind variable at 0 is defined as DOUBLE[][] and cannot accept INT
                setLong\terror: [0] bind variable at 0 is defined as DOUBLE[][] and cannot accept LONG
                setFloat\terror: [0] bind variable at 0 is defined as DOUBLE[][] and cannot accept FLOAT
                setDouble\terror: [0] bind variable at 0 is defined as DOUBLE[][] and cannot accept DOUBLE
                setDate\terror: [0] bind variable at 0 is defined as DOUBLE[][] and cannot accept DATE
                setTimestamp\terror: [0] bind variable at 0 is defined as DOUBLE[][] and cannot accept TIMESTAMP
                setStr\terror: inconvertible value: `1` [STRING -> DOUBLE[][]]
                setVarchar\terror: [0] bind variable at 0 is defined as DOUBLE[][] and cannot accept VARCHAR
                setLong256\terror: [0] bind variable at 0 is defined as DOUBLE[][] and cannot accept LONG256
                setUuid\terror: [0] bind variable at 0 is defined as DOUBLE[][] and cannot accept UUID
                setArray\terror: [-1] array type mismatch [expected=DOUBLE[][], actual=DOUBLE[]]
                ## window_anchor
                error: create view: [147] ANCHOR EXPRESSION must return TIMESTAMP, LONG, or INT; got DOUBLE[][]
                """);
        rec("INTERVAL(us)", """
                ## filter_eq
                error: create: [29] non-persisted type: INTERVAL
                ## filter_ne
                error: create: [29] non-persisted type: INTERVAL
                ## filter_null
                error: create: [29] non-persisted type: INTERVAL
                ## filter_not_null
                error: create: [29] non-persisted type: INTERVAL
                ## filter_lt
                error: create: [29] non-persisted type: INTERVAL
                ## filter_ge
                error: create: [29] non-persisted type: INTERVAL
                ## order_asc
                error: create: [29] non-persisted type: INTERVAL
                ## order_desc
                error: create: [29] non-persisted type: INTERVAL
                ## group_by
                error: create: [29] non-persisted type: INTERVAL
                ## join_inner
                error: create: [29] non-persisted type: INTERVAL
                ## join_left
                error: create: [29] non-persisted type: INTERVAL
                ## join_left_null
                error: create: [29] non-persisted type: INTERVAL
                ## union_all
                error: create: [29] non-persisted type: INTERVAL
                ## union_null
                error: create: [29] non-persisted type: INTERVAL
                ## case_no_else
                error: create: [29] non-persisted type: INTERVAL
                ## case_else
                error: create: [29] non-persisted type: INTERVAL
                ## lag
                error: create: [29] non-persisted type: INTERVAL
                ## sample_by
                error: create: [29] non-persisted type: INTERVAL
                ## first_last
                error: create: [29] non-persisted type: INTERVAL
                ## first_not_null
                error: create: [29] non-persisted type: INTERVAL
                ## fill_prev
                error: create: [29] non-persisted type: INTERVAL
                ## latest_on
                error: create: [29] non-persisted type: INTERVAL
                ## cast
                error: create: [29] non-persisted type: INTERVAL
                ## fill_null
                error: create: [29] non-persisted type: INTERVAL
                ## fill_value
                error: create: [29] non-persisted type: INTERVAL
                ## fill_linear
                error: create: [29] non-persisted type: INTERVAL
                ## subsample_stride
                error: create: [29] non-persisted type: INTERVAL
                ## subsample_target
                error: create: [29] non-persisted type: INTERVAL
                ## memoized
                error: create: [29] non-persisted type: INTERVAL
                ## where_bound_const
                error: create: [29] non-persisted type: INTERVAL
                ## where_bound_bind
                error: create: [29] non-persisted type: INTERVAL
                ## where_key
                error: create: [29] non-persisted type: INTERVAL
                ## copy_bind
                error: create: [29] non-persisted type: INTERVAL
                ## between_timestamp
                error: create: [29] non-persisted type: INTERVAL
                ## eq_null_double
                error: create: [29] non-persisted type: INTERVAL
                ## bind_value
                setBoolean\tdefine error: [0] bind variable cannot be used [contextType=131111, index=0]
                setByte\tdefine error: [0] bind variable cannot be used [contextType=131111, index=0]
                setShort\tdefine error: [0] bind variable cannot be used [contextType=131111, index=0]
                setChar\tdefine error: [0] bind variable cannot be used [contextType=131111, index=0]
                setInt\tdefine error: [0] bind variable cannot be used [contextType=131111, index=0]
                setLong\tdefine error: [0] bind variable cannot be used [contextType=131111, index=0]
                setFloat\tdefine error: [0] bind variable cannot be used [contextType=131111, index=0]
                setDouble\tdefine error: [0] bind variable cannot be used [contextType=131111, index=0]
                setDate\tdefine error: [0] bind variable cannot be used [contextType=131111, index=0]
                setTimestamp\tdefine error: [0] bind variable cannot be used [contextType=131111, index=0]
                setStr\tdefine error: [0] bind variable cannot be used [contextType=131111, index=0]
                setVarchar\tdefine error: [0] bind variable cannot be used [contextType=131111, index=0]
                setLong256\tdefine error: [0] bind variable cannot be used [contextType=131111, index=0]
                setUuid\tdefine error: [0] bind variable cannot be used [contextType=131111, index=0]
                setArray\tdefine error: [0] bind variable cannot be used [contextType=131111, index=0]
                ## window_anchor
                error: create: [41] non-persisted type: INTERVAL
                """);
        rec("INTERVAL(ns)", """
                ## cast
                error: create: [29] non-persisted type: INTERVAL
                ## filter_eq
                error: create: [29] non-persisted type: INTERVAL
                ## filter_ne
                error: create: [29] non-persisted type: INTERVAL
                ## filter_null
                error: create: [29] non-persisted type: INTERVAL
                ## filter_not_null
                error: create: [29] non-persisted type: INTERVAL
                ## filter_lt
                error: create: [29] non-persisted type: INTERVAL
                ## filter_ge
                error: create: [29] non-persisted type: INTERVAL
                ## order_asc
                error: create: [29] non-persisted type: INTERVAL
                ## order_desc
                error: create: [29] non-persisted type: INTERVAL
                ## group_by
                error: create: [29] non-persisted type: INTERVAL
                ## join_inner
                error: create: [29] non-persisted type: INTERVAL
                ## join_left
                error: create: [29] non-persisted type: INTERVAL
                ## join_left_null
                error: create: [29] non-persisted type: INTERVAL
                ## union_all
                error: create: [29] non-persisted type: INTERVAL
                ## union_null
                error: create: [29] non-persisted type: INTERVAL
                ## case_no_else
                error: create: [29] non-persisted type: INTERVAL
                ## case_else
                error: create: [29] non-persisted type: INTERVAL
                ## lag
                error: create: [29] non-persisted type: INTERVAL
                ## sample_by
                error: create: [29] non-persisted type: INTERVAL
                ## first_last
                error: create: [29] non-persisted type: INTERVAL
                ## first_not_null
                error: create: [29] non-persisted type: INTERVAL
                ## fill_prev
                error: create: [29] non-persisted type: INTERVAL
                ## latest_on
                error: create: [29] non-persisted type: INTERVAL
                ## fill_null
                error: create: [29] non-persisted type: INTERVAL
                ## fill_value
                error: create: [29] non-persisted type: INTERVAL
                ## fill_linear
                error: create: [29] non-persisted type: INTERVAL
                ## subsample_stride
                error: create: [29] non-persisted type: INTERVAL
                ## subsample_target
                error: create: [29] non-persisted type: INTERVAL
                ## memoized
                error: create: [29] non-persisted type: INTERVAL
                ## where_bound_const
                error: create: [29] non-persisted type: INTERVAL
                ## where_bound_bind
                error: create: [29] non-persisted type: INTERVAL
                ## where_key
                error: create: [29] non-persisted type: INTERVAL
                ## copy_bind
                error: create: [29] non-persisted type: INTERVAL
                ## between_timestamp
                error: create: [29] non-persisted type: INTERVAL
                ## eq_null_double
                error: create: [29] non-persisted type: INTERVAL
                ## bind_value
                setBoolean\tdefine error: [0] bind variable cannot be used [contextType=262183, index=0]
                setByte\tdefine error: [0] bind variable cannot be used [contextType=262183, index=0]
                setShort\tdefine error: [0] bind variable cannot be used [contextType=262183, index=0]
                setChar\tdefine error: [0] bind variable cannot be used [contextType=262183, index=0]
                setInt\tdefine error: [0] bind variable cannot be used [contextType=262183, index=0]
                setLong\tdefine error: [0] bind variable cannot be used [contextType=262183, index=0]
                setFloat\tdefine error: [0] bind variable cannot be used [contextType=262183, index=0]
                setDouble\tdefine error: [0] bind variable cannot be used [contextType=262183, index=0]
                setDate\tdefine error: [0] bind variable cannot be used [contextType=262183, index=0]
                setTimestamp\tdefine error: [0] bind variable cannot be used [contextType=262183, index=0]
                setStr\tdefine error: [0] bind variable cannot be used [contextType=262183, index=0]
                setVarchar\tdefine error: [0] bind variable cannot be used [contextType=262183, index=0]
                setLong256\tdefine error: [0] bind variable cannot be used [contextType=262183, index=0]
                setUuid\tdefine error: [0] bind variable cannot be used [contextType=262183, index=0]
                setArray\tdefine error: [0] bind variable cannot be used [contextType=262183, index=0]
                ## window_anchor
                error: create: [41] non-persisted type: INTERVAL
                """);
    }
    // recordings: end

    private static void rec(String label, String recording) {
        RECORDINGS.put(label, recording);
    }
}
