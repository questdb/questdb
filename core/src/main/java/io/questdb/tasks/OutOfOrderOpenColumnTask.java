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

import io.questdb.std.AbstractLockable;

import static io.questdb.cairo.TableWriter.OO_BLOCK_NONE;

public class OutOfOrderOpenColumnTask extends AbstractLockable {
    private int openColumnMode;
    private int prefixType ;
    private long prefixLo;
    private long prefixHi;
    private int mergeType;
    private long mergeDataLo;
    private long mergeDataHi;
    private long mergeOOOLo;
    private long mergeOOOHi;
    private int suffixType;
    private long suffixLo;
    private long suffixHi;

    public void of(
            int openColumnMode,
            int prefixType,
            long prefixLo,
            long prefixHi,
            int mergeType,
            long mergeDataLo,
            long mergeDataHi,
            long mergeOOOLo,
            long mergeOOOHi,
            int suffixType,
            long suffixLo,
            long suffixHi
    ) {
        this.openColumnMode = openColumnMode;
        this.prefixType = prefixType;
        this.prefixLo = prefixLo;
        this.prefixHi = prefixHi;
        this.mergeType = mergeType;
        this.mergeDataLo = mergeDataLo;
        this.mergeDataHi = mergeDataHi;
        this.mergeOOOLo = mergeOOOLo;
        this.mergeOOOHi = mergeOOOHi;
        this.suffixType = suffixType;
        this.suffixLo = suffixLo;
        this.suffixHi = suffixHi;
    }

    public int getOpenColumnMode() {
        return openColumnMode;
    }

    public int getPrefixType() {
        return prefixType;
    }

    public long getPrefixLo() {
        return prefixLo;
    }

    public long getPrefixHi() {
        return prefixHi;
    }

    public int getMergeType() {
        return mergeType;
    }

    public long getMergeDataLo() {
        return mergeDataLo;
    }

    public long getMergeDataHi() {
        return mergeDataHi;
    }

    public long getMergeOOOLo() {
        return mergeOOOLo;
    }

    public long getMergeOOOHi() {
        return mergeOOOHi;
    }

    public int getSuffixType() {
        return suffixType;
    }

    public long getSuffixLo() {
        return suffixLo;
    }

    public long getSuffixHi() {
        return suffixHi;
    }
}
