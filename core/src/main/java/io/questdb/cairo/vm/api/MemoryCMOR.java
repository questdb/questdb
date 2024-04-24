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

package io.questdb.cairo.vm.api;

import io.questdb.cairo.CairoException;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.vm.Vm;
import io.questdb.std.Unsafe;
import io.questdb.std.str.DirectString;

// contiguous mapped readable
public interface MemoryCMOR extends MemoryCMR, MemoryOM {

    @Override
    default DirectString getStr(long offset, DirectString view) {
        long addr = addressOf(offset);
        assert addr > 0;
        if (Vm.PARANOIA_MODE && offset + 4 > getSizeWithOffset()) {
            throw CairoException.critical(0)
                    .put("String is outside of file boundary [offset=")
                    .put(offset)
                    .put(", size=")
                    .put(getSizeWithOffset())
                    .put(']');
        }

        final int len = Unsafe.getUnsafe().getInt(addr);
        if (len != TableUtils.NULL_LEN) {
            if (Vm.getStorageLength(len) + offset <= getSizeWithOffset()) {
                return view.of(addr + Vm.STRING_LENGTH_BYTES, len);
            }
            throw CairoException.critical(0)
                    .put("String is outside of file boundary [offset=")
                    .put(offset)
                    .put(", len=")
                    .put(len)
                    .put(", size=")
                    .put(getSizeWithOffset())
                    .put(']');
        }
        return null;
    }
}
