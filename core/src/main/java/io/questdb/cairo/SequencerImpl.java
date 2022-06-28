/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
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

package io.questdb.cairo;

import io.questdb.std.Misc;
import io.questdb.std.str.Path;

import static io.questdb.cairo.TableUtils.*;

public class SequencerImpl implements Sequencer {
    private final IDGenerator tnxGenerator;

    SequencerImpl(CairoConfiguration configuration) {
        tnxGenerator = new IDGenerator(configuration, SEQ_INDEX_FILE_NAME);
    }

    @Override
    public void open(Path path) {
        tnxGenerator.open(path);
    }

    @Override
    public long nextTxn() {
        return tnxGenerator.getNextId();
    }

    @Override
    public void close() {
        Misc.free(tnxGenerator);
    }
}
