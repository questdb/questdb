/*
 * Copyright (c) 2014. Vlad Ilyushchenko
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.nfsdb.journal.lang.cst.impl.rsrc;

import com.nfsdb.journal.lang.cst.PartitionSlice;
import com.nfsdb.journal.lang.cst.RowCursor;
import com.nfsdb.journal.lang.cst.RowSource;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

public class UnionRowSource implements RowSource, RowCursor {
    private final RowSource[] sources;
    private final RowCursor[] cursors;
    private int cursorIndex;

    @SuppressFBWarnings(justification = "By ref parameter to avoid paranoid array copying")
    public UnionRowSource(RowSource[] sources) {
        this.sources = sources;
        this.cursors = new RowCursor[sources.length];
    }

    @Override
    public RowCursor cursor(PartitionSlice slice) {
        for (int i = 0; i < sources.length; i++) {
            RowSource source = sources[i];
            cursors[i] = source.cursor(slice);
        }
        cursorIndex = 0;
        return this;
    }

    @Override
    public boolean hasNext() {

        while (cursorIndex < cursors.length) {
            if (cursors[cursorIndex].hasNext()) {
                return true;
            }
            cursorIndex++;
        }

        return false;
    }

    @Override
    public long next() {
        return cursors[cursorIndex].next();
    }

    @Override
    public void reset() {
        for (int i = 0; i < sources.length; i++) {
            sources[i].reset();
        }
    }
}
