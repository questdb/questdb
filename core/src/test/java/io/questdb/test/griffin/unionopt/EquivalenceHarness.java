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

package io.questdb.test.griffin.unionopt;

import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.CursorPrinter;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.std.str.StringSink;
import org.junit.Assert;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.TreeSet;
import java.util.function.Consumer;

/**
 * Runs queries under a {@link GrantPolicySecurityContext} and compares two queries by their
 * permission decision (always), their rows (order-independent, when both are allowed), and,
 * when {@code requireSameChecks} is set, the exact set of SELECT checks — but only when both
 * outcomes are allowed. {@link GrantPolicySecurityContext} throws on the first failing check, so
 * a denied outcome records only the checks made up to that point; a rewrite that reorders UNION
 * branches (or otherwise changes evaluation order) can trip a different check first and so record
 * a different partial check set even though the permission decision is identical. Comparing checks
 * on a denied outcome would therefore flag an order difference, not a real permission mismatch.
 */
public final class EquivalenceHarness {
    private static final int MAX_ATOMS = 6;

    private EquivalenceHarness() {
    }

    public static void assertEquivalentUnderAllGrants(
            CairoEngine engine,
            String sqlA,
            String sqlB,
            boolean requireSameChecks,
            List<Grant> atoms
    ) throws SqlException {
        Assert.assertTrue("grant lattice too large: " + atoms.size(), atoms.size() <= MAX_ATOMS);
        for (int mask = 0, n = 1 << atoms.size(); mask < n; mask++) {
            final List<Grant> granted = new ArrayList<>();
            for (int i = 0; i < atoms.size(); i++) {
                if ((mask & (1 << i)) != 0) {
                    granted.add(atoms.get(i));
                }
            }
            final Outcome a = run(engine, sqlA, policyOf(granted));
            final Outcome b = run(engine, sqlB, policyOf(granted));
            final String label = "grants=" + granted;
            assertMismatchFree(label + " decision", a.denied(), b.denied());
            if (!a.denied()) {
                assertMismatchFree(label + " rows", a.rows(), b.rows());
                if (requireSameChecks) {
                    assertMismatchFree(label + " checks", a.checks(), b.checks());
                }
            }
        }
    }

    public static void assertRevokeBetweenCompileAndExecuteDenies(
            CairoEngine engine,
            String sql,
            List<Grant> fullGrants,
            Consumer<GrantPolicySecurityContext> revoke,
            String expectedDeniedObject
    ) throws SqlException {
        final GrantPolicySecurityContext policy = policyOf(fullGrants);
        try (
                SqlExecutionContext ctx = contextOf(engine, policy);
                SqlCompiler compiler = engine.getSqlCompiler();
                RecordCursorFactory factory = compiler.compile(sql, ctx).getRecordCursorFactory()
        ) {
            // first execution is allowed
            final Outcome firstRun = drain(factory, ctx, policy);
            Assert.assertFalse("unexpected denial: " + firstRun.rows(), firstRun.denied());
            revoke.accept(policy);
            // the same compiled plan must now be denied
            final Outcome afterRevoke = drain(factory, ctx, policy);
            Assert.assertTrue("expected denial after revoke: " + sql, afterRevoke.denied());
            final String expectedObjectTag = "[object=" + expectedDeniedObject + "]";
            Assert.assertTrue(
                    "expected denial for " + expectedObjectTag + " but was: " + afterRevoke.rows(),
                    afterRevoke.rows().contains(expectedObjectTag)
            );
        }
    }

    private static void assertMismatchFree(String label, Object expected, Object actual) {
        if (!Objects.equals(expected, actual)) {
            throw new EquivalenceMismatch(label + " expected:<" + expected + "> but was:<" + actual + ">");
        }
    }

    public static Outcome run(CairoEngine engine, String sql, GrantPolicySecurityContext policy) throws SqlException {
        policy.clearChecks();
        try (
                SqlExecutionContext ctx = contextOf(engine, policy);
                SqlCompiler compiler = engine.getSqlCompiler()
        ) {
            final RecordCursorFactory factory;
            try {
                factory = compiler.compile(sql, ctx).getRecordCursorFactory();
            } catch (Throwable th) {
                return deniedOrRethrow(th, policy);
            }
            try (factory) {
                return drain(factory, ctx, policy);
            }
        }
    }

    private static SqlExecutionContext contextOf(CairoEngine engine, GrantPolicySecurityContext policy) {
        return new SqlExecutionContextImpl(engine, 1)
                .with(policy, null, null, -1, SqlExecutionCircuitBreaker.NOOP_CIRCUIT_BREAKER);
    }

    private static Outcome deniedOrRethrow(Throwable th, GrantPolicySecurityContext policy) throws SqlException {
        final String message = th.getMessage();
        if (message != null && message.contains(GrantPolicySecurityContext.DENIED)) {
            return new Outcome(true, message, policy.getChecks());
        }
        if (th instanceof SqlException se) {
            throw se;
        }
        if (th instanceof RuntimeException re) {
            throw re;
        }
        if (th instanceof Error e) {
            throw e;
        }
        throw new AssertionError(th);
    }

    private static Outcome drain(RecordCursorFactory factory, SqlExecutionContext ctx, GrantPolicySecurityContext policy) throws SqlException {
        policy.clearChecks();
        try (RecordCursor cursor = factory.getCursor(ctx)) {
            final RecordMetadata metadata = factory.getMetadata();
            final Record record = cursor.getRecord();
            final StringSink sink = new StringSink();
            final List<String> rows = new ArrayList<>();
            while (cursor.hasNext()) {
                sink.clear();
                CursorPrinter.println(record, metadata, sink);
                rows.add(sink.toString());
            }
            Collections.sort(rows);
            return new Outcome(false, String.join("", rows), policy.getChecks());
        } catch (Throwable th) {
            return deniedOrRethrow(th, policy);
        }
    }

    private static GrantPolicySecurityContext policyOf(List<Grant> grants) {
        final GrantPolicySecurityContext policy = new GrantPolicySecurityContext();
        for (int i = 0, n = grants.size(); i < n; i++) {
            grants.get(i).applyTo(policy);
        }
        return policy;
    }

    public record Outcome(boolean denied, String rows, TreeSet<String> checks) {
    }

    /**
     * Thrown by {@link #assertEquivalentUnderAllGrants} when a decision, row set, or check set
     * genuinely diverges between the two queries under some grant subset. Kept distinct from
     * {@link AssertionError} in general so negative-control tests can assert on this exact type,
     * rather than incidentally passing when an unrelated checked throwable (e.g. a broken query)
     * is wrapped by {@link #deniedOrRethrow}.
     */
    public static final class EquivalenceMismatch extends AssertionError {
        public EquivalenceMismatch(String message) {
            super(message);
        }
    }
}
