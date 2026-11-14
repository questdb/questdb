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

package io.questdb.test.cairo.mv;

import io.questdb.std.IntList;
import io.questdb.std.ObjList;
import org.junit.Assert;

import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

/**
 * The oracle of {@link MatViewFuzzTest#testViewAndBaseOperations()}. It compares the view with a twin view that each
 * check creates and fully refreshes from the current base, bucket by bucket, and accepts a difference only where an
 * operation the fuzzer ran explains it, as the "Known limitations" of the design list them:
 * <ul>
 *     <li>A base day is <i>suspect</i> once an operation may have removed its partition: DROP and DETACH PARTITION,
 *     TRUNCATE, TTL and a base replacement. The view keeps its rows there, so a view bucket that the base no longer
 *     explains is accepted in a suspect day, and nowhere else.</li>
 *     <li>A suspect day is <i>frozen</i> while the base holds no partition there. No refresh reaches a frozen day, so
 *     its view rows stay as the previous check saw them, unless an operation touched the day since: a base row or an
 *     ATTACH that recreates the partition, or a view DROP PARTITION. A FULL refresh may rebuild them.</li>
 *     <li>A view day dropped with view DROP PARTITION holds no row until a refresh recomputes a bucket, so a bucket of
 *     the twin may be missing there, but a bucket the view has must match the twin's.</li>
 *     <li>An ATTACH or UPDATE range reaches the view through an in-memory task, which a restart before the task runs
 *     loses, and which a view with an invalidation pending drops. The buckets of such a range may hold old values
 *     until a RANGE refresh recomputes them.</li>
 *     <li>Every other bucket of the twin, the base's buckets, must be in the view with the same values in the view's
 *     columns that its SQL produces.</li>
 *     <li>A period view and its twin hold the days whose period ended, which the clock completes one at a time from
 *     the first. The view can also hold buckets of the other days, which a RANGE refresh recomputes, so the oracle
 *     compares the complete days only.</li>
 * </ul>
 * A RANGE refresh over every day, which the test runs after a RESUME SKIP BACKLOG and after a view column becomes
 * mapped, recomputes the days the base holds; a completed FULL refresh rebuilds the view and resets the oracle.
 */
final class MatViewFuzzOracle {
    private final IntList columnIds = new IntList();
    private final ObjList<String> columnNames = new ObjList<>();
    private final int dayCount;
    // rows of the frozen days as the previous check saw them, keyed by day
    private final HashMap<Integer, TreeMap<String, HashMap<Integer, String>>> frozenRows = new HashMap<>();
    private final boolean[] isPendingRange;
    private final boolean[] isRangeLost;
    private final boolean[] isSuspect;
    private final boolean[] isTouched;
    private final boolean[] isViewDropped;
    private int baseEpoch;
    private int baseEpochAtLastValid;
    // the days, from the first, that the oracle compares: the complete periods of a period view, every day otherwise
    private int completeDays;
    private boolean isFullPending;
    private boolean isGapAccepted;
    private boolean isInvalidationPending;
    private boolean isNeedsBackfill;
    private int nextColumnId;
    private int ttlDays;

    MatViewFuzzOracle(int dayCount, ObjList<String> viewColumns, boolean isPeriodView) {
        this.dayCount = dayCount;
        this.completeDays = isPeriodView ? 0 : dayCount;
        this.isPendingRange = new boolean[dayCount];
        this.isRangeLost = new boolean[dayCount];
        this.isSuspect = new boolean[dayCount];
        this.isTouched = new boolean[dayCount];
        this.isViewDropped = new boolean[dayCount];
        for (int i = 0, n = viewColumns.size(); i < n; i++) {
            columnNames.add(viewColumns.getQuick(i));
            columnIds.add(nextColumnId++);
        }
    }

    // Asserts the view's rows against the twin's: the base's buckets in the view, no bucket the base does not explain
    // outside the suspect days, NULL in the view columns no SQL produces, and the frozen days as the previous check
    // saw them.
    void assertView(
            TreeMap<String, Row> viewRows,
            TreeMap<String, Row> twinRows,
            boolean[] baseDays,
            ObjList<String> sqlColumns,
            CharSequence context
    ) {
        assertColumns(viewRows, context);
        for (Map.Entry<String, Row> e : twinRows.entrySet()) {
            final Row twinRow = e.getValue();
            final int day = twinRow.day;
            if (day >= completeDays || isRangeLost[day]) {
                continue;
            }
            final Row viewRow = viewRows.get(e.getKey());
            if (viewRow == null) {
                if (isViewDropped[day]) {
                    continue;
                }
                Assert.fail("view lacks a base bucket [key=" + e.getKey() + ", day=" + day + ", twin=" + twinRow.values + "] " + context);
            }
            for (int i = 0, n = columnNames.size(); i < n; i++) {
                final String name = columnNames.getQuick(i);
                if (MatViewFuzzOpGenerator.isCatalogueName(name) && sqlColumns.indexOf(name) > -1) {
                    final String expected = twinRow.values.get(name);
                    final String actual = viewRow.values.get(name);
                    if (!isSameValue(expected, actual)) {
                        Assert.fail("view bucket differs from the base [key=" + e.getKey() + ", column=" + name
                                + ", expected=" + expected + ", actual=" + actual + ", day=" + day
                                + ", view=" + viewRow.values + ", twin=" + twinRow.values + "] " + context);
                    }
                }
            }
        }
        for (Map.Entry<String, Row> e : viewRows.entrySet()) {
            final Row viewRow = e.getValue();
            if (viewRow.day >= completeDays) {
                continue;
            }
            for (int i = 0, n = columnNames.size(); i < n; i++) {
                final String name = columnNames.getQuick(i);
                if (!MatViewFuzzOpGenerator.isCatalogueName(name) && !name.equals(MatViewFuzzOpGenerator.TS) && !name.equals(MatViewFuzzOpGenerator.SYM)) {
                    Assert.assertEquals("view column that no SQL produces holds a value [key=" + e.getKey() + ", column=" + name + "] " + context, "null", viewRow.values.get(name));
                }
            }
            if (!twinRows.containsKey(e.getKey()) && !isSuspect[viewRow.day]) {
                Assert.fail("view holds a bucket the base does not explain [key=" + e.getKey() + ", day=" + viewRow.day
                        + ", view=" + viewRow.values + "] " + context);
            }
        }
        for (Map.Entry<Integer, TreeMap<String, HashMap<Integer, String>>> e : frozenRows.entrySet()) {
            final int day = e.getKey();
            if (!isSuspect[day] || baseDays[day] || isTouched[day]) {
                continue;
            }
            final TreeMap<String, HashMap<Integer, String>> expected = e.getValue();
            final TreeMap<String, HashMap<Integer, String>> actual = new TreeMap<>();
            collectDay(viewRows, day, actual);
            if (!expected.keySet().equals(actual.keySet())) {
                Assert.fail("frozen view day lost or gained buckets [day=" + day + ", expected=" + expected.keySet()
                        + ", actual=" + actual.keySet() + "] " + context);
            }
            for (Map.Entry<String, HashMap<Integer, String>> r : actual.entrySet()) {
                final HashMap<Integer, String> expectedValues = expected.get(r.getKey());
                for (Map.Entry<Integer, String> v : r.getValue().entrySet()) {
                    final String expectedValue = expectedValues.get(v.getKey());
                    // a column added since the previous check reads NULL in rows no refresh recomputed
                    Assert.assertEquals("frozen view bucket changed [day=" + day + ", key=" + r.getKey() + ", columnId=" + v.getKey() + "] " + context,
                            expectedValue != null ? expectedValue : "null", v.getValue());
                }
            }
        }
    }

    int getBaseEpoch() {
        return baseEpoch;
    }

    int getCompleteDays() {
        return completeDays;
    }

    boolean isBacklogExpectedTracked(String invalidationReason) {
        // A forced invalidation of a view positioned on the current base keeps its backlog; a base replacement loses
        // it.
        return MatViewFuzzTest.FUZZ_INVALIDATION_REASON.equals(invalidationReason) && baseEpoch == baseEpochAtLastValid;
    }

    boolean isBaseReplacedSinceValid() {
        return baseEpoch != baseEpochAtLastValid;
    }

    boolean isFullPending() {
        return isFullPending;
    }

    boolean isGapAccepted() {
        return isGapAccepted;
    }

    boolean isNeedsBackfill() {
        return isNeedsBackfill;
    }

    void onAttach(int day) {
        onRecreated(day);
        isPendingRange[day] = true;
    }

    void onBaseRemoved(int day) {
        isSuspect[day] = true;
    }

    void onBaseRemovedBefore(int day) {
        for (int d = 0; d < day; d++) {
            isSuspect[d] = true;
        }
    }

    void onBaseReplaced() {
        baseEpoch++;
        ttlDays = 0;
        onBaseRemovedBefore(dayCount);
    }

    // The view went through a check: it is valid, and the queues are drained.
    void onChecked(TreeMap<String, Row> viewRows, boolean[] baseDays) {
        baseEpochAtLastValid = baseEpoch;
        frozenRows.clear();
        for (int d = 0; d < dayCount; d++) {
            isTouched[d] = false;
            // the twin of a period view tells the base's days among the complete ones only
            if (d < completeDays && isSuspect[d] && !baseDays[d]) {
                final TreeMap<String, HashMap<Integer, String>> rows = new TreeMap<>();
                collectDay(viewRows, d, rows);
                frozenRows.put(d, rows);
            }
        }
    }

    // The queues drained: the WAL apply published every range task and the refresh job ran them. A FULL on an invalid
    // view waits for the view's own WAL, which a drain applies after the refresh job ran, so a FULL can outlive the
    // drain; the caller passes whether the view still holds one.
    void onDrained(boolean isFullPending) {
        onRefreshJobRun();
        this.isFullPending = isFullPending;
        for (int d = 0; d < dayCount; d++) {
            isPendingRange[d] = false;
        }
    }

    void onFullCompleted() {
        frozenRows.clear();
        for (int d = 0; d < dayCount; d++) {
            isSuspect[d] = false;
            isRangeLost[d] = false;
            isViewDropped[d] = false;
            isTouched[d] = false;
        }
        isNeedsBackfill = false;
        isGapAccepted = false;
        onTtl(ttlDays);
    }

    // A FULL refresh that may or may not run: it may rebuild any row.
    void onFullIssued() {
        frozenRows.clear();
        isFullPending = true;
    }

    // A RANGE refresh over every day: it recomputes the days the base holds.
    void onFullRange(boolean[] baseDays) {
        for (int d = 0; d < dayCount; d++) {
            if (baseDays[d]) {
                isViewDropped[d] = false;
                isRangeLost[d] = false;
            }
        }
        isNeedsBackfill = false;
        isGapAccepted = false;
    }

    void onGapAccepted() {
        isGapAccepted = true;
    }

    void onInsert(int day) {
        onRecreated(day);
    }

    void onInvalidationPublished() {
        isInvalidationPending = true;
    }

    // The period of the next day ended.
    void onPeriodCompleted() {
        completeDays++;
    }

    // The refresh job ran: a range task it ran while an invalidation was pending dropped its range.
    void onRefreshJobRun() {
        if (isInvalidationPending) {
            for (int d = 0; d < dayCount; d++) {
                if (isPendingRange[d]) {
                    isRangeLost[d] = true;
                }
            }
            isInvalidationPending = false;
        }
    }

    // A restart loses the range tasks that have not run and the pending invalidation.
    void onRestart() {
        for (int d = 0; d < dayCount; d++) {
            if (isPendingRange[d]) {
                isRangeLost[d] = true;
                isPendingRange[d] = false;
            }
        }
        isInvalidationPending = false;
        isFullPending = false;
    }

    void onResumed() {
        baseEpochAtLastValid = baseEpoch;
    }

    void onSqlChanged(ObjList<String> oldSqlColumns, ObjList<String> newSqlColumns) {
        for (int i = 0, n = newSqlColumns.size(); i < n; i++) {
            final String name = newSqlColumns.getQuick(i);
            if (MatViewFuzzOpGenerator.isCatalogueName(name) && oldSqlColumns.indexOf(name) < 0 && columnNames.indexOf(name) > -1) {
                // the column is mapped from now on, and the rows computed before hold NULL or older values
                isNeedsBackfill = true;
            }
        }
    }

    void onTtl(int days) {
        ttlDays = days;
        if (days > 0) {
            // TTL drops the partitions older than the newest base row less the TTL, at most these
            onBaseRemovedBefore(dayCount - days);
        }
    }

    void onUpdate(int day) {
        isPendingRange[day] = true;
    }

    void onViewColumnAdded(String name, ObjList<String> sqlColumns) {
        columnNames.add(name);
        columnIds.add(nextColumnId++);
        if (sqlColumns.indexOf(name) > -1) {
            isNeedsBackfill = true;
        }
    }

    void onViewColumnDropped(String name) {
        final int index = columnNames.indexOf(name);
        columnNames.remove(index);
        columnIds.removeIndex(index);
    }

    void onViewDropped(int day) {
        isViewDropped[day] = true;
        isTouched[day] = true;
    }

    void onViewDroppedBefore(int day) {
        for (int d = 0; d < day; d++) {
            onViewDropped(d);
        }
    }

    private static boolean isSameValue(String expected, String actual) {
        if (expected.equals(actual)) {
            return true;
        }
        if ("null".equals(expected) || "null".equals(actual)) {
            return false;
        }
        // the view column can take another numeric type than the twin's, with an implicit cast
        try {
            return Double.parseDouble(expected) == Double.parseDouble(actual);
        } catch (NumberFormatException e) {
            return false;
        }
    }

    private void assertColumns(TreeMap<String, Row> viewRows, CharSequence context) {
        for (Row row : viewRows.values()) {
            Assert.assertEquals("view columns differ from the fuzzer's " + context, columnNames.size(), row.values.size());
            for (int i = 0, n = columnNames.size(); i < n; i++) {
                Assert.assertTrue("view lacks column " + columnNames.getQuick(i) + ' ' + context, row.values.containsKey(columnNames.getQuick(i)));
            }
            break;
        }
    }

    private void collectDay(TreeMap<String, Row> viewRows, int day, TreeMap<String, HashMap<Integer, String>> sink) {
        for (Map.Entry<String, Row> e : viewRows.entrySet()) {
            if (e.getValue().day == day) {
                final HashMap<Integer, String> values = new HashMap<>();
                for (int i = 0, n = columnNames.size(); i < n; i++) {
                    values.put(columnIds.getQuick(i), e.getValue().values.get(columnNames.getQuick(i)));
                }
                sink.put(e.getKey(), values);
            }
        }
    }

    private void onRecreated(int day) {
        isTouched[day] = true;
    }

    static final class Row {
        final int day;
        final HashMap<String, String> values = new HashMap<>();

        Row(int day) {
            this.day = day;
        }
    }
}
