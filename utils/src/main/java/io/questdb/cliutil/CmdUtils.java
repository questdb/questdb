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

package io.questdb.cliutil;

import io.questdb.cairo.CairoException;
import io.questdb.cairo.RebuildColumnBase;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.str.Utf8String;

public class CmdUtils {
    static void runColumnRebuild(RebuildColumnCommandArgs params, RebuildColumnBase ri) {
        final Log log = LogFactory.getLog("recover-var-index");
        ri.of(new Utf8String(params.tablePath));
        try {
            ri.reindex(params.partition, params.column);
        } catch (CairoException ex) {
            log.error().$safe(ex.getFlyweightMessage()).$();
        }
    }
}
