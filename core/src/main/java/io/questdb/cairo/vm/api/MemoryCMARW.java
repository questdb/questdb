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

package io.questdb.cairo.vm.api;

//contiguous mapped appendable readable writable
public interface MemoryCMARW extends MemoryCM, MemoryMARW, MemoryCMR {

    /**
     * By default, a remap that fails to grow this memory closes it, and the memory reads and writes nothing until
     * its owner opens it again. With {@code isMappingKeptOnFailedRemap}, the memory stays as it was before the
     * write that tried to grow it: mapped, with the same size and append position. For owners that read their
     * file after a failed write, to roll it back, such as TxWriter. On every platform a failed remap leaves the
     * previous mapping in place, see {@code Files.mremap0()}.
     */
    void setMappingKeptOnFailedRemap(boolean isMappingKeptOnFailedRemap);
}
