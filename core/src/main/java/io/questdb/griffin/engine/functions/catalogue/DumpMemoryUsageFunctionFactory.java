/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2020 QuestDB
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
import io.questdb.cairo.sql.Function;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.constants.BooleanConstant;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.*;

public class DumpMemoryUsageFunctionFactory implements FunctionFactory {
    private static final Log LOG = LogFactory.getLog(DumpMemoryUsageFunctionFactory.class);

    @Override
    public String getSignature() {
        return "dump_memory_usage()";
    }

    @Override
    public Function newInstance(int position,
                                ObjList<Function> args,
                                IntList argPositions,
                                CairoConfiguration configuration,
                                SqlExecutionContext sqlExecutionContext
    ) {
        LOG.info().$("total memory: ").$(Unsafe.getMemUsed()).$();
        for (int i = MemoryTag.MMAP_DEFAULT; i < MemoryTag.SIZE; i++) {
            final long memUsedByTag = Unsafe.getMemUsedByTag(i);
            if (memUsedByTag > 0) {
                LOG.info().$("memory tag(").$(i).$("): ").$(memUsedByTag).$();
            }
        }
        return BooleanConstant.TRUE;
    }
}
