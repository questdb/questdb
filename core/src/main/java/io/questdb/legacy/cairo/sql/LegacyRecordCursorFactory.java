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

package io.questdb.legacy.cairo.sql;

import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.vm.api.MemoryCARW;
import io.questdb.jit.CompiledFilter;
import io.questdb.legacy.griffin.model.ExpressionNode;
import io.questdb.std.ObjList;

/**
 * The methods master's {@link RecordCursorFactory} had for filter stealing and top-K peeling, which the legacy planner
 * still uses. The legacy factories that steal a filter or peel a top-K implement them; the static methods answer for
 * any factory, as master's defaults did for a factory that does not.
 */
public interface LegacyRecordCursorFactory extends RecordCursorFactory {

    static boolean canPeelForTopK(RecordCursorFactory factory) {
        return factory instanceof LegacyRecordCursorFactory legacy && legacy.canPeelForTopK();
    }

    static ObjList<Function> getBindVarFunctions(RecordCursorFactory factory) {
        return factory instanceof LegacyRecordCursorFactory legacy ? legacy.getBindVarFunctions() : null;
    }

    static MemoryCARW getBindVarMemory(RecordCursorFactory factory) {
        return factory instanceof LegacyRecordCursorFactory legacy ? legacy.getBindVarMemory() : null;
    }

    static CompiledFilter getCompiledFilter(RecordCursorFactory factory) {
        return factory instanceof LegacyRecordCursorFactory legacy ? legacy.getCompiledFilter() : null;
    }

    static ExpressionNode getStealFilterExpr(RecordCursorFactory factory) {
        return factory instanceof LegacyRecordCursorFactory legacy ? legacy.getStealFilterExpr() : null;
    }

    static void halfClose(RecordCursorFactory factory) {
        if (factory instanceof LegacyRecordCursorFactory legacy) {
            legacy.halfClose();
        }
    }

    /**
     * Hands the filter {@code factory} gave up to a steal, with its compiled form and bind variables, to the factory
     * that adopts them, so closing {@code factory} no longer frees them. A factory whose halfClose() already gives
     * them up does nothing.
     */
    static void releaseStolenFilter(RecordCursorFactory factory) {
        if (factory instanceof LegacyRecordCursorFactory legacy) {
            legacy.releaseStolenFilter();
        }
    }

    static RecordCursorFactory rewrapOverTopK(RecordCursorFactory factory, RecordCursorFactory topK, RecordMetadata orderedMetadata) {
        return factory instanceof LegacyRecordCursorFactory legacy ? legacy.rewrapOverTopK(topK, orderedMetadata) : topK;
    }

    static boolean supportsFilterStealing(RecordCursorFactory factory) {
        return factory instanceof LegacyRecordCursorFactory legacy && legacy.supportsFilterStealing();
    }

    static int translateOrderByColumnToBase(RecordCursorFactory factory, int projectedIndex) {
        return factory instanceof LegacyRecordCursorFactory legacy ? legacy.translateOrderByColumnToBase(projectedIndex) : projectedIndex;
    }

    default boolean canPeelForTopK() {
        return false;
    }

    default ObjList<Function> getBindVarFunctions() {
        return null;
    }

    default MemoryCARW getBindVarMemory() {
        return null;
    }

    default CompiledFilter getCompiledFilter() {
        return null;
    }

    default ExpressionNode getStealFilterExpr() {
        return null;
    }

    default void halfClose() {
    }

    default void releaseStolenFilter() {
    }

    default RecordCursorFactory rewrapOverTopK(RecordCursorFactory topK, RecordMetadata orderedMetadata) {
        return topK;
    }

    default boolean supportsFilterStealing() {
        return false;
    }

    default int translateOrderByColumnToBase(int projectedIndex) {
        return projectedIndex;
    }
}
