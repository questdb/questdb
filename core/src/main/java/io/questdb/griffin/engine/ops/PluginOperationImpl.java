/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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

package io.questdb.griffin.engine.ops;

import io.questdb.cairo.OperationCodes;
import io.questdb.cairo.sql.OperationFuture;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.mp.SCSequence;
import org.jetbrains.annotations.Nullable;

/**
 * Immutable implementation of PluginOperation.
 * Holds the plugin name and operation type.
 */
public class PluginOperationImpl implements PluginOperation {
    private final String pluginName;
    private final boolean isLoad;
    private final DoneOperationFuture future = new DoneOperationFuture();

    public PluginOperationImpl(String pluginName, boolean isLoad) {
        this.pluginName = pluginName;
        this.isLoad = isLoad;
    }

    @Override
    public void close() {
    }

    @Override
    public OperationFuture execute(SqlExecutionContext sqlExecutionContext, @Nullable SCSequence eventSubSeq) throws SqlException {
        try (SqlCompiler compiler = sqlExecutionContext.getCairoEngine().getSqlCompiler()) {
            compiler.execute(this, sqlExecutionContext);
        }
        return future;
    }

    @Override
    public int getOperationCode() {
        return isLoad ? OperationCodes.LOAD_PLUGIN : OperationCodes.UNLOAD_PLUGIN;
    }

    @Override
    public OperationFuture getOperationFuture() {
        return future;
    }

    @Override
    public String getPluginName() {
        return pluginName;
    }

    @Override
    public boolean isLoad() {
        return isLoad;
    }
}
