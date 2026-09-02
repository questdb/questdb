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

package io.questdb.cairo.lv;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.std.Chars;
import io.questdb.std.IntList;
import io.questdb.std.ObjList;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Which of a live view's PARTITION BY terms key as LV-private symbol ids, decided once at
 * CREATE and persisted in {@code _lv} so that every later compile of the same SELECT reaches
 * the same answer.
 *
 * <h2>Why the decision cannot be re-derived</h2>
 * The classification {@link LiveViewPartitionKeyClassifier} performs is a property of the
 * build, not of the SQL: a term keys as an id when the compiler can prove it is a direct
 * window-input SYMBOL column reference <i>and</i> the compiled plan can trace that column to
 * a base-scan SYMBOL column. Both halves widen as the optimizer learns new projection shapes,
 * and either can narrow. That answer is what the key type in the checkpoint's persisted key
 * schema records, so a build that classifies one term differently makes the schema on disk
 * disagree with the compiled runtime. Restore validation catches it and the view rebuilds
 * from the base table - correct, but a rebuild of a large view is expensive and reads as an
 * unexplained upgrade regression. Persisting the decision and honoring it removes the flip:
 * the view keeps keying the way it was created, and only a change to the definition itself
 * can change that.
 *
 * <h2>What is persisted, and why by name</h2>
 * The window-input column names whose terms key as ids, in classification order. The
 * classifier's own slot namespace is the window-input <i>column index</i>, which is exactly
 * what is not stable across builds - a projection the optimizer adds or prunes between the
 * base scan and the window shifts every index after it. The name survives that, because it
 * comes from the SELECT text, which the definition also pins.
 *
 * <h2>How it is honored</h2>
 * {@link #admittedSourceColumns} resolves the persisted names against the window-input
 * metadata of the compile at hand, and the classifier admits a term only when its source
 * column is in that list. So the allow-list can only narrow what this build would classify,
 * never widen it:
 * <ul>
 *     <li>a term this build would newly admit stays on its resolved string, because the
 *     persisted decision does not name it;</li>
 *     <li>a term this build no longer admits, or whose name no longer resolves, is not
 *     admitted either - the persisted decision cannot conjure a binding that stage 1 refuses.
 *     That view's key schema does disagree with its checkpoint, and it rebuilds; the refresh
 *     path logs the shrunk term so the rebuild has a stated cause.</li>
 * </ul>
 * A null decision - a view created before the definition carried one - means "re-derive",
 * which is the behavior every such view already shipped with. An empty decision is a
 * decision: it says this view translates nothing.
 */
public final class LiveViewPartitionKeyDecision {
    /**
     * The decision a view whose PARTITION BY holds no translatable term persists. Immutable
     * and shared; distinct from a null decision, which says nothing was persisted at all.
     */
    public static final LiveViewPartitionKeyDecision NOTHING_TRANSLATES =
            new LiveViewPartitionKeyDecision(new ObjList<>());

    private final ObjList<String> translatedColumnNames;

    private LiveViewPartitionKeyDecision(@NotNull ObjList<String> translatedColumnNames) {
        this.translatedColumnNames = translatedColumnNames;
    }

    /**
     * Resolves a persisted decision into the window-input column indexes the classifier of
     * the compile at hand may admit, or null when there is no decision to honor and the
     * classifier re-derives.
     * <p>
     * A persisted name the metadata cannot answer, or answers with a non-SYMBOL column, is
     * dropped rather than resolved to something else: the index it would take names a
     * different column's dictionary, which is a wrong key rather than a slow one.
     *
     * @param decision            the view's persisted decision, or null for a view created
     *                            before one was persisted
     * @param windowInputMetadata what the compile's PARTITION BY terms resolve against
     */
    public static @Nullable IntList admittedSourceColumns(
            @Nullable LiveViewPartitionKeyDecision decision,
            @NotNull RecordMetadata windowInputMetadata
    ) {
        if (decision == null) {
            return null;
        }
        final IntList admitted = new IntList(decision.translatedColumnNames.size());
        for (int i = 0, n = decision.translatedColumnNames.size(); i < n; i++) {
            final int index = windowInputMetadata.getColumnIndexQuiet(decision.translatedColumnNames.getQuick(i));
            if (index >= 0 && ColumnType.isSymbol(windowInputMetadata.getColumnType(index))) {
                admitted.add(index);
            }
        }
        return admitted;
    }

    /**
     * Reads the decision a compile just made off its plan: the terms stage 1 admitted,
     * narrowed to those a base-scan SYMBOL column backs, which is what stage 2 would bind a
     * dictionary to. CREATE calls this on the factory it compiles to validate the view, so
     * the decision the view ships with is the one its first refresh would have derived.
     */
    public static @NotNull LiveViewPartitionKeyDecision derive(@NotNull LiveViewCompiledPlan plan) {
        final LiveViewPartitionKeyClassifier classifier = plan.getWindowFactory().getLivePartitionKeyClassifier();
        if (classifier == null || classifier.getSourceColumnCount() == 0) {
            return NOTHING_TRANSLATES;
        }
        final RecordMetadata windowInputMetadata = plan.getWindowInputMetadata();
        final RecordMetadata baseScanMetadata = plan.getBaseScanMetadata();
        final ObjList<String> names = new ObjList<>(classifier.getSourceColumnCount());
        for (int i = 0, n = classifier.getSourceColumnCount(); i < n; i++) {
            final int windowInputColumnIndex = classifier.getSourceColumn(i);
            final int scanColumnIndex = plan.traceWindowInputColumnToBaseScan(windowInputColumnIndex);
            if (scanColumnIndex < 0 || !ColumnType.isSymbol(baseScanMetadata.getColumnType(scanColumnIndex))) {
                continue;
            }
            names.add(Chars.toString(windowInputMetadata.getColumnName(windowInputColumnIndex)));
        }
        return names.size() == 0 ? NOTHING_TRANSLATES : new LiveViewPartitionKeyDecision(names);
    }

    /**
     * Rebuilds a decision read back from {@code _lv}. Takes ownership of {@code names}.
     */
    public static @NotNull LiveViewPartitionKeyDecision of(@NotNull ObjList<String> names) {
        return names.size() == 0 ? NOTHING_TRANSLATES : new LiveViewPartitionKeyDecision(names);
    }

    public int getColumnCount() {
        return translatedColumnNames.size();
    }

    public String getColumnName(int index) {
        return translatedColumnNames.getQuick(index);
    }

    /**
     * Whether the term keyed by {@code windowInputColumnName} keys as an LV-private id.
     */
    public boolean isTranslated(@NotNull CharSequence windowInputColumnName) {
        for (int i = 0, n = translatedColumnNames.size(); i < n; i++) {
            if (Chars.equalsIgnoreCase(translatedColumnNames.getQuick(i), windowInputColumnName)) {
                return true;
            }
        }
        return false;
    }
}
