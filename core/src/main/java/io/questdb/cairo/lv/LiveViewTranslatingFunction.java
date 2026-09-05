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


package io.questdb.cairo.lv;

import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.engine.functions.IntFunction;
import io.questdb.griffin.engine.functions.UnaryFunction;
import org.jetbrains.annotations.NotNull;

/**
 * Wraps a compiled SYMBOL-typed key term so a function-backed checkpoint sink can translate
 * it, the way a record-column sink already does via {@code RecordSinkFactory.getTranslatingInstance}.
 * <p>
 * {@code LiveViewCheckpointFunctionCompiler.expressionKeyProjector} reads its key through
 * compiled {@link Function}s rather than a {@code Record}'s columns, so neither
 * {@code getTranslatingInstance} (needs a {@code ColumnFilter} over a record) nor
 * {@link LiveViewTranslatingRecord} (wraps a record, and there is none to wrap here) can reach
 * it. This class moves the same {@link LiveViewSymbolIdTranslator#translate} call one layer
 * down, into the term itself: reporting {@link io.questdb.cairo.ColumnType#INT} rather than
 * SYMBOL is what routes {@code RecordSinkFactory}'s function-backed generator to the plain
 * {@code getInt}/{@code putInt} pair it already emits for any INT-typed key function - the
 * generator itself needs no translating mode of its own.
 * <p>
 * <b>Ownership.</b> {@code close()} is deliberately a no-op rather than the
 * {@link UnaryFunction} default that closes the wrapped argument: this class only ever wraps a
 * term that a projector's own {@code keyFunctions} list already owns and frees, in a second,
 * throwaway list built solely to compile the translated sink. Closing the delegate here would
 * double-free it.
 */
public final class LiveViewTranslatingFunction extends IntFunction implements UnaryFunction {
    private final Function arg;
    private final int slot;
    private final LiveViewSymbolIdTranslator translator;

    public LiveViewTranslatingFunction(@NotNull Function arg, int slot, @NotNull LiveViewSymbolIdTranslator translator) {
        this.arg = arg;
        this.slot = slot;
        this.translator = translator;
    }

    @Override
    public void close() {
        // The delegate belongs to the canonical key-function list; see the class javadoc.
    }

    @Override
    public Function getArg() {
        return arg;
    }

    @Override
    public int getInt(Record rec) {
        return translator.translate(slot, arg.getInt(rec));
    }

    @Override
    public String getName() {
        return "translate";
    }
}
