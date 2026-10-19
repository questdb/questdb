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

package io.questdb.griffin.plan.logical;

import io.questdb.std.Chars;
import io.questdb.std.IntList;
import io.questdb.std.ObjList;
import io.questdb.std.ObjectFactory;

/**
 * Input columns followed by window results; every expression reads the input.
 */
public final class WindowPlan extends UnaryPlan {
    public static final ObjectFactory<WindowPlan> FACTORY = WindowPlan::new;
    private final IntList functionColumnIds = new IntList();
    private final ObjList<FunctionExpression> functions = new ObjList<>();
    private final SortKeys queryOrder = new SortKeys();
    private final ObjList<WindowSpec> specs = new ObjList<>();
    private Algorithm algorithm;
    private boolean isSelectOrdered;

    @Override
    public void clear() {
        super.clear();
        functionColumnIds.clear();
        functions.clear();
        queryOrder.clear();
        specs.clear();
        algorithm = null;
        isSelectOrdered = false;
    }

    /**
     * The one rule that lays the output out over the input's current output: the input's columns, attribute for
     * attribute, then the window results in the order of {@link #getFunctionColumnIds()}, each keeping its own
     * attributes, with the column it designated before designated again. An input column the previous output did not
     * expose stays out when a window result of its name replaced it. A rewrite that changes the input's output in place
     * calls it; {@code scratch} is a temporary schema it leaves empty.
     */
    public void deriveOutput(OutputSchema scratch) {
        final OutputSchema output = getOutput();
        final int timestampId = output.getTimestampColumnId();
        scratch.copyFrom(output);
        output.copyFrom(getChild().getOutput());
        int inputCount = output.getColumnCount();
        int replacedCount = 0;
        for (int i = 0; i < inputCount; i++) {
            if (scratch.getColumnIndexById(output.getColumnId(i)) < 0) {
                replacedCount++;
            }
        }
        for (int i = 0, n = functionColumnIds.size(); i < n; i++) {
            final int index = scratch.getColumnIndexById(functionColumnIds.getQuick(i));
            if (replacedCount > 0 && removeReplacedColumn(output, scratch, scratch.getColumnName(index), inputCount)) {
                inputCount--;
                replacedCount--;
            }
            output.addColumnFrom(scratch, index);
        }
        output.setTimestampColumnId(timestampId);
        scratch.clear();
    }

    /**
     * The window factory operator planning records for the window; null before planning.
     */
    public Algorithm getAlgorithm() {
        return algorithm;
    }

    public IntList getFunctionColumnIds() {
        return functionColumnIds;
    }

    public ObjList<FunctionExpression> getFunctions() {
        return functions;
    }

    /**
     * The ORDER BY of the query level the window output feeds, when it is the order this window's SELECT sorts
     * by; empty otherwise.
     */
    public SortKeys getQueryOrder() {
        return queryOrder;
    }

    public ObjList<WindowSpec> getSpecs() {
        return specs;
    }

    /**
     * True when the ORDER BY of the SELECT that computes these windows sorts their output.
     */
    public boolean isSelectOrdered() {
        return isSelectOrdered;
    }

    public void markSelectOrdered() {
        isSelectOrdered = true;
    }

    public WindowPlan of(LogicalPlan input, int position) {
        configure(input, position);
        return this;
    }

    public void setAlgorithm(Algorithm algorithm) {
        this.algorithm = algorithm;
    }

    @Override
    public void visitReads(PlanExpressionVisitor visitor) {
        PlanReads.functions(functions, visitor);
        for (int i = 0, n = specs.size(); i < n; i++) {
            specs.getQuick(i).visitReads(visitor);
        }
    }

    /**
     * Keeps out of the input columns of the output the nested window column, one the previous output did not expose,
     * that a top-level window result named {@code name} has replaced.
     */
    private static boolean removeReplacedColumn(OutputSchema output, OutputSchema previous, CharSequence name, int inputCount) {
        for (int i = 0; i < inputCount; i++) {
            if (Chars.equalsIgnoreCase(output.getColumnName(i), name) && previous.getColumnIndexById(output.getColumnId(i)) < 0) {
                output.remove(i);
                return true;
            }
        }
        return false;
    }

    /**
     * How the generator computes the windows: streaming over its input when every window is evaluated in one pass
     * over rows its input delivers in the window order; otherwise caching the rows, as row ids into an input with
     * random access whose sort keys all encode and whose window results are all fixed-width, or as copies of them.
     */
    public enum Algorithm {
        CACHED, CACHED_LIGHT, STREAMING
    }
}
