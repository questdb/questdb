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

import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryMARW;
import io.questdb.std.LongList;
import io.questdb.std.Misc;
import io.questdb.std.QuietCloseable;
import io.questdb.std.str.Path;

/**
 * Reusable owner for the retirement queue's paths, mapping and merge buffer.
 * <p>
 * Every publication reads the queue and writes it back, so building the mapping
 * wrapper and its three paths per call would charge the whole graph to each
 * cadence seal. The mapping itself is opened and closed per call - the file it
 * covers changes size and is renamed under it - but the Java shells persist for
 * the lifetime of the owner that holds this scratch.
 * <p>
 * One owner, one caller: the queue's reads and writes never nest, so a single
 * scratch serves the read, the merge and the write of one publication.
 */
final class LiveViewCheckpointRetirementQueueScratch implements QuietCloseable {

    final LongList entries = new LongList();
    final Path finalPath = new Path();
    final MemoryMARW mem = Vm.getCMARWInstance();
    final Path path = new Path();
    final Path tmpPath = new Path();

    @Override
    public void close() {
        Misc.free(mem);
        Misc.free(finalPath);
        Misc.free(path);
        Misc.free(tmpPath);
        entries.clear();
    }
}
