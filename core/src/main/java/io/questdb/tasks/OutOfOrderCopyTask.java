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

package io.questdb.tasks;

import io.questdb.cairo.ContiguousVirtualMemory;
import io.questdb.std.AbstractLockable;

public class OutOfOrderCopyTask extends AbstractLockable {
    private long dstFixOffset;
    private long dstVarOffset;
    private long dstVarAddr;
    private long dstFixAddr;
    private long srcFixAddr;
    private long srcFixSize;
    private long srcVarAddr;
    private long srcVarSize;
    private long srcLo;
    private long srcHi;
    private int blockType;
    private int columnType;
    private ContiguousVirtualMemory oooFixColumn;
    private ContiguousVirtualMemory oooVarColumn;

    public long getSrcHi() {
        return srcHi;
    }

    public long getSrcLo() {
        return srcLo;
    }

    public int getBlockType() {
        return blockType;
    }

    public int getColumnType() {
        return columnType;
    }

    public long getDstVarAddr() {
        return dstVarAddr;
    }

    public long getDstFixOffset() {
        return dstFixOffset;
    }

    public long getDstVarOffset() {
        return dstVarOffset;
    }

    public long getDstFixAddr() {
        return dstFixAddr;
    }

    public ContiguousVirtualMemory getOooFixColumn() {
        return oooFixColumn;
    }

    public ContiguousVirtualMemory getOooVarColumn() {
        return oooVarColumn;
    }

    public long getSrcFixAddr() {
        return srcFixAddr;
    }

    public long getSrcFixSize() {
        return srcFixSize;
    }

    public long getSrcVarAddr() {
        return srcVarAddr;
    }

    public long getSrcVarSize() {
        return srcVarSize;
    }
}
