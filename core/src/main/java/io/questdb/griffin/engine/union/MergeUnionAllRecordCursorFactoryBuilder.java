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

package io.questdb.griffin.engine.union;

import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.griffin.SqlException;
import io.questdb.std.IntList;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import org.jetbrains.annotations.Nullable;

/**
 * Flattens compatible binary merge factories into one N-way owner. When set type widening changes
 * the result metadata, it rebuilds every leaf's cast directly against the final metadata instead of
 * retaining a cast-bearing merge layer.
 */
public final class MergeUnionAllRecordCursorFactoryBuilder {

    private MergeUnionAllRecordCursorFactoryBuilder() {
    }

    public static MergeUnionAllRecordCursorFactory build(
            RecordMetadata metadata,
            RecordCursorFactory factoryA,
            int positionA,
            RecordCursorFactory factoryB,
            int positionB,
            ObjList<Function> castFunctionsA,
            ObjList<Function> castFunctionsB,
            boolean isAscending,
            @Nullable IntList symbolUnionColumns,
            CastFunctionFactory castFunctionFactory
    ) throws SqlException {
        OperandState operandsA = null;
        OperandState operandsB = null;
        try {
            operandsA = take(factoryA, positionA, castFunctionsA, metadata, castFunctionFactory);
            factoryA = null;
            castFunctionsA = null;
            operandsB = take(factoryB, positionB, castFunctionsB, metadata, castFunctionFactory);
            factoryB = null;
            castFunctionsB = null;

            operandsA.factories.addAll(operandsB.factories);
            operandsA.positions.addAll(operandsB.positions);
            operandsA.castFunctions.addAll(operandsB.castFunctions);
            operandsB.factories.clear();
            operandsB.positions.clear();
            operandsB.castFunctions.clear();

            final MergeUnionAllRecordCursorFactory result = new MergeUnionAllRecordCursorFactory(
                    metadata,
                    operandsA.factories,
                    operandsA.positions,
                    operandsA.castFunctions,
                    metadata.getTimestampIndex(),
                    isAscending,
                    symbolUnionColumns
            );
            operandsA = null;
            return result;
        } catch (Throwable th) {
            close(operandsA);
            close(operandsB);
            Misc.free(factoryA);
            Misc.free(factoryB);
            Misc.freeObjList(castFunctionsA);
            Misc.freeObjList(castFunctionsB);
            throw th;
        }
    }

    private static void close(OperandState state) {
        if (state != null) {
            Misc.freeObjList(state.factories);
            for (int i = 0, n = state.castFunctions.size(); i < n; i++) {
                Misc.freeObjList(state.castFunctions.getQuick(i));
            }
        }
    }

    private static boolean hasSameColumnTypes(RecordMetadata a, RecordMetadata b) {
        final int columnCount = a.getColumnCount();
        if (columnCount != b.getColumnCount()) {
            return false;
        }
        for (int i = 0; i < columnCount; i++) {
            if (a.getColumnType(i) != b.getColumnType(i)) {
                return false;
            }
        }
        return true;
    }

    private static OperandState take(
            RecordCursorFactory factory,
            int position,
            ObjList<Function> edgeCastFunctions,
            RecordMetadata targetMetadata,
            CastFunctionFactory castFunctionFactory
    ) throws SqlException {
        if (factory instanceof UnionSymbolCastRecordCursorFactory symbolCastFactory
                && symbolCastFactory.getBaseFactory() instanceof MergeUnionAllRecordCursorFactory) {
            final RecordCursorFactory detachedBase = symbolCastFactory.detachBase();
            try {
                Misc.free(symbolCastFactory);
            } catch (Throwable th) {
                Misc.free(detachedBase);
                throw th;
            }
            factory = detachedBase;
        }

        if (factory instanceof MergeUnionAllRecordCursorFactory mergeFactory) {
            final RecordMetadata sourceMetadata = mergeFactory.getMetadata();
            final OperandState state = mergeFactory.detachOperands();
            Misc.free(mergeFactory);
            try {
                if (hasSameColumnTypes(sourceMetadata, targetMetadata)) {
                    Misc.freeObjList(edgeCastFunctions);
                    return state;
                }

                for (int i = 0, n = state.factories.size(); i < n; i++) {
                    Misc.freeObjList(state.castFunctions.getQuick(i));
                    state.castFunctions.setQuick(i, castFunctionFactory.generate(
                            targetMetadata,
                            state.factories.getQuick(i).getMetadata(),
                            state.positions.getQuick(i)
                    ));
                }
                Misc.freeObjList(edgeCastFunctions);
                return state;
            } catch (Throwable th) {
                close(state);
                Misc.freeObjList(edgeCastFunctions);
                throw th;
            }
        }

        final OperandState state = new OperandState();
        state.factories.add(factory);
        state.positions.add(position);
        state.castFunctions.add(edgeCastFunctions);
        return state;
    }

    @FunctionalInterface
    public interface CastFunctionFactory {
        ObjList<Function> generate(RecordMetadata toMetadata, RecordMetadata fromMetadata, int modelPosition) throws SqlException;
    }

    static final class OperandState {
        final ObjList<ObjList<Function>> castFunctions;
        final ObjList<RecordCursorFactory> factories;
        final IntList positions;

        OperandState() {
            this(new ObjList<>(), new IntList(), new ObjList<>());
        }

        OperandState(
                ObjList<RecordCursorFactory> factories,
                IntList positions,
                ObjList<ObjList<Function>> castFunctions
        ) {
            this.factories = factories;
            this.positions = positions;
            this.castFunctions = castFunctions;
        }
    }
}
