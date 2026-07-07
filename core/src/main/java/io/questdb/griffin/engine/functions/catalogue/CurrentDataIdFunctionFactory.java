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

package io.questdb.griffin.engine.functions.catalogue;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.DataID;
import io.questdb.cairo.sql.Function;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.constants.UuidConstant;
import io.questdb.std.IntList;
import io.questdb.std.ObjList;
import io.questdb.std.Uuid;

public class CurrentDataIdFunctionFactory implements FunctionFactory {
    @Override
    public String getSignature() {
        return "current_data_id()";
    }

    @Override
    public boolean isRuntimeConstant() {
        return true;
    }

    @Override
    public Function newInstance(int position, ObjList<Function> args, IntList argPositions, CairoConfiguration configuration, SqlExecutionContext sqlExecutionContext) {
        DataID id = sqlExecutionContext.getCairoEngine().getDataID();
        // Read both halves atomically: getLo()/getHi() are individually synchronized, but reading them
        // as two separate calls can tear across the one-shot replica DataID publish, producing a
        // UuidConstant that is neither SQL NULL nor the correct id. getSnapshot() reads the pair under
        // a single monitor acquisition.
        final Uuid snapshot = id.getSnapshot();
        return new UuidConstant(snapshot.getLo(), snapshot.getHi());
    }
}
