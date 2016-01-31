/*******************************************************************************
 *  _  _ ___ ___     _ _
 * | \| | __/ __| __| | |__
 * | .` | _|\__ \/ _` | '_ \
 * |_|\_|_| |___/\__,_|_.__/
 *
 * Copyright (c) 2014-2016. The NFSdb project and its contributors.
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
 ******************************************************************************/

package com.nfsdb.ql.impl.unused;

import com.nfsdb.ql.KeyCursor;
import com.nfsdb.ql.KeySource;
import com.nfsdb.ql.ops.NLGlue;
import com.nfsdb.store.SymbolTable;

public class SymLookupKeySource implements KeySource, KeyCursor {

    private final NLGlue glue;
    private final SymbolTable slave;
    private boolean hasNext = true;

    public SymLookupKeySource(SymbolTable slave, NLGlue glue) {
        this.glue = glue;
        this.slave = slave;
    }

    @Override
    public boolean hasNext() {
        return hasNext;
    }

    @Override
    public int next() {
        hasNext = false;
        return slave.getQuick(glue.getFlyweightStr());
    }

    @Override
    public KeyCursor prepareCursor() {
        return this;
    }

    @Override
    public void reset() {
        hasNext = true;
    }
}
