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

package io.questdb.griffin.engine.functions.groupby;

import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import org.jetbrains.annotations.NotNull;

/**
 * The geometric mean of a constant argument: the constant itself over a group that holds a row, which the logarithm
 * mean of {@link GeomeanDoubleGroupByFunction} only approximates, and NULL over an empty group or a constant that is
 * not positive.
 */
public class GeomeanConstantGroupByFunction extends GeomeanDoubleGroupByFunction {
    private final double value;

    public GeomeanConstantGroupByFunction(@NotNull Function arg) {
        super(arg);
        final double value = arg.getDouble(null);
        this.value = value > 0 ? value : Double.NaN;
    }

    @Override
    public double getDouble(Record rec) {
        return rec.getLong(getValueIndex() + 1) == 0 ? Double.NaN : value;
    }
}
