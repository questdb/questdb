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

package io.questdb.griffin.engine.window;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.ColumnTypes;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * What one SELECT-list window function offers a group: the accumulator component it would
 * contribute, and the projection kind its own output reads off that component.
 * <p>
 * Resolving the two is one answer and not two. The family a function declares decides
 * whether an argument is part of the identity at all; the argument, where there is one, has
 * to be a direct compiled column of the base metadata; and one shape rewrites both at once -
 * a {@code count(k)} over the very column its window partitions by is a
 * {@link WindowAccumulatorProjection#PROJECTION_COUNT_PARTITION_KEY guarded} reading of a
 * {@link WindowAccumulatorDescriptor#FAMILY_ROW_COUNT} component rather than a counter of
 * its own. Splitting that walk across the two owners is how the two would come to disagree
 * about which calls fuse, so both ask here.
 * <p>
 * What stays with each owner is what it means to be eligible in the first place - an
 * ordinary query asks for a private partition map to replace, a live view asks for anchored,
 * inline-budget checkpoint state - and what the group is keyed by, which is the
 * {@link PartitionKeyGuard} the caller supplies. Neither is a fact about the accumulator.
 */
public final class WindowAccumulatorCandidate {
    private final WindowAccumulatorDescriptor component;
    private final int projectionKind;

    private WindowAccumulatorCandidate(@NotNull WindowAccumulatorDescriptor component, int projectionKind) {
        this.component = component;
        this.projectionKind = projectionKind;
    }

    /**
     * Whether {@code function} would join a group as an unguarded
     * {@link WindowAccumulatorDescriptor#FAMILY_ROW_COUNT row count} - a {@code count(*)} or a
     * partitioned {@code row_number()}.
     * <p>
     * Such a function is the host a {@code count} over the window's own partition key may
     * join, and an owner looks for one before it offers a single projection: that count may
     * precede its host in the SELECT list, and whether it fuses must not depend on that. The
     * arm is deliberately narrow - the family must be the argumentless one and the function
     * must actually hold no argument, which is what keeps a {@code count(x)} from being read
     * as a row count here.
     * <p>
     * It is the family half of the search only. The owner adds its own eligibility gates, so
     * that it cannot name a host {@link #of} would decline.
     */
    public static boolean isRowCountHost(@NotNull WindowFunction function) {
        return function.windowAccumulatorFamily() == WindowAccumulatorDescriptor.FAMILY_ROW_COUNT
                && function.windowAccumulatorArgument() == null
                && function.windowAccumulatorProjection() != WindowAccumulatorProjection.PROJECTION_NONE;
    }

    /**
     * Reads {@code function}'s accumulator identity, or returns null when this build cannot
     * name every part of it. Every null is an ordinary answer: the function is not fusible
     * and keeps whatever state it owns outside a group.
     *
     * @param function          the offered function, already past its owner's own eligibility
     *                          gates
     * @param recordTypes       the types of the record the window functions and their
     *                          arguments read, by index - see
     *                          {@link WindowAccumulatorDescriptor#directColumnIndex}
     * @param rowCountHost      the group's unguarded row-count function, or null when it holds
     *                          none - see {@link #isRowCountHost}
     * @param partitionKeyGuard the owner's answer to "is this column the whole of the window's
     *                          partition key?"
     */
    public static @Nullable WindowAccumulatorCandidate of(
            @NotNull WindowFunction function,
            @NotNull ColumnTypes recordTypes,
            @Nullable WindowFunction rowCountHost,
            @NotNull PartitionKeyGuard partitionKeyGuard
    ) {
        int projectionKind = function.windowAccumulatorProjection();
        if (projectionKind == WindowAccumulatorProjection.PROJECTION_NONE) {
            return null;
        }
        int family = function.windowAccumulatorFamily();
        int argumentColumnIndex;
        int argumentColumnType;
        if (WindowAccumulatorDescriptor.familyTakesArgument(family)) {
            argumentColumnIndex = WindowAccumulatorDescriptor.directColumnIndex(
                    function.windowAccumulatorArgument(),
                    recordTypes
            );
            if (argumentColumnIndex < 0) {
                return null;
            }
            argumentColumnType = recordTypes.getColumnType(argumentColumnIndex);
            if (isCountOverTheWindowsOwnPartitionKey(
                    function,
                    family,
                    projectionKind,
                    argumentColumnIndex,
                    argumentColumnType,
                    rowCountHost,
                    partitionKeyGuard
            )) {
                family = WindowAccumulatorDescriptor.FAMILY_ROW_COUNT;
                projectionKind = WindowAccumulatorProjection.PROJECTION_COUNT_PARTITION_KEY;
                argumentColumnIndex = WindowAccumulatorDescriptor.NO_ARGUMENT_COLUMN_INDEX;
                argumentColumnType = ColumnType.UNDEFINED;
            }
        } else {
            // A row-count component counts rows and nothing about a column, so its identity
            // has no argument to carry. A function that declares the family and still holds an
            // argument is describing state the identity does not, and is turned away rather
            // than fused under a key that omits it.
            if (function.windowAccumulatorArgument() != null) {
                return null;
            }
            argumentColumnIndex = WindowAccumulatorDescriptor.NO_ARGUMENT_COLUMN_INDEX;
            argumentColumnType = ColumnType.UNDEFINED;
        }
        final WindowAccumulatorDescriptor component = WindowAccumulatorDescriptor.of(
                family,
                argumentColumnIndex,
                argumentColumnType
        );
        return component == null ? null : new WindowAccumulatorCandidate(component, projectionKind);
    }

    /**
     * The component this function would contribute, which is the whole of its accumulator
     * identity.
     */
    public @NotNull WindowAccumulatorDescriptor getComponent() {
        return component;
    }

    /**
     * The arithmetic this function's output reads off {@link #getComponent()}, one of the
     * {@link WindowAccumulatorProjection} {@code PROJECTION_*} constants.
     */
    public int getProjectionKind() {
        return projectionKind;
    }

    /**
     * Whether {@code function} is a {@code count(k)} whose argument is the single column its
     * own window partitions by, and whose group already holds an unguarded reading of the row
     * count it would join.
     * <p>
     * Every row of a partition carries the same {@code k}, so such a call's counter is the
     * partition's row count wherever {@code k} is present and zero where it is not. That makes
     * it a {@link WindowAccumulatorProjection#PROJECTION_COUNT_PARTITION_KEY guarded reading}
     * of the {@link WindowAccumulatorDescriptor#FAMILY_ROW_COUNT} component {@code count(*)}
     * and {@code row_number()} maintain, rather than a counter of its own.
     * <p>
     * Three things narrow it here, and the owner's {@link PartitionKeyGuard} adds the fourth:
     * <ul>
     *     <li><b>the family and projection are a plain {@code count(x)}</b>, which is the only
     *     call whose counter can be a row count at all;</li>
     *     <li><b>the argument's contribution predicate is the type's own null test.</b> A
     *     widened-to-double argument contributes on {@code Numbers.isFinite}, so a partition
     *     keyed by a DOUBLE {@code +Infinity} would be a present key the count does not count.
     *     SYMBOL and VARCHAR are the two types whose predicate is exactly "the key is
     *     absent";</li>
     *     <li><b>the group holds a row-count projection that is not itself guarded.</b> That
     *     projection is what the component's contributor will be, and it is the only kind that
     *     maintains a true row count. Without it the component would be maintained by the
     *     guarded count alone - correct for every output the group has today, but a state
     *     carrying a partition's {@code count(k)} where the layout says row count, which a
     *     later recompile adding {@code count(*)} would read back at face value.</li>
     * </ul>
     */
    private static boolean isCountOverTheWindowsOwnPartitionKey(
            WindowFunction function,
            int family,
            int projectionKind,
            int argumentColumnIndex,
            int argumentColumnType,
            @Nullable WindowFunction rowCountHost,
            PartitionKeyGuard partitionKeyGuard
    ) {
        if (family != WindowAccumulatorDescriptor.FAMILY_NON_NULL_COUNT
                || projectionKind != WindowAccumulatorProjection.PROJECTION_COUNT
                || rowCountHost == null) {
            return false;
        }
        final int tag = ColumnType.tagOf(argumentColumnType);
        if (tag != ColumnType.SYMBOL && tag != ColumnType.VARCHAR) {
            return false;
        }
        return partitionKeyGuard.isOwnSingleColumnPartitionKey(function, argumentColumnIndex, rowCountHost);
    }

    /**
     * The owner's answer to whether a group's key is the one column an argument names. Asked
     * only of a {@code count} whose argument type could carry the guard, and only where the
     * group already holds a row-count host.
     * <p>
     * The two owners prove it from different places - an ordinary query from the
     * {@link WindowMapSpec} it bucketed the group by, a live view from the key layout and the
     * PARTITION BY functions each function carries - which is why it is asked rather than
     * derived.
     */
    @FunctionalInterface
    public interface PartitionKeyGuard {
        /**
         * @param function            the {@code count(k)} being offered
         * @param argumentColumnIndex its argument's index in the base metadata
         * @param rowCountHost        the host it would join, which the caller has already
         *                            found in the same group
         * @return true when {@code function}'s window partitions by exactly that one column,
         * encoded as one map key column, and {@code rowCountHost} belongs to that same window
         */
        boolean isOwnSingleColumnPartitionKey(
                @NotNull WindowFunction function,
                int argumentColumnIndex,
                @NotNull WindowFunction rowCountHost
        );
    }
}
