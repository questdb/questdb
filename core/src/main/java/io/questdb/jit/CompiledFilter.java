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

package io.questdb.jit;

import io.questdb.cairo.vm.api.MemoryCARW;
import io.questdb.griffin.SqlException;
import io.questdb.std.ThreadLocal;

import java.io.Closeable;

public class CompiledFilter implements Closeable {

    private static final ThreadLocal<FiltersCompiler.JitError> tlJitError = new ThreadLocal<>(FiltersCompiler.JitError::new);

    private long fnAddress;

    public long call(
            long dataAddress,
            long dataSize,
            long varSizeAuxAddress,
            long varsAddress,
            long varsSize,
            long rowsAddress,
            long rowsSize,
            long rowsStartOffset
    ) {
        return FiltersCompiler.callFunction(
                fnAddress,
                dataAddress,
                dataSize,
                varSizeAuxAddress,
                varsAddress,
                varsSize,
                rowsAddress,
                rowsSize,
                rowsStartOffset
        );
    }

    @Override
    public void close() {
        if (fnAddress > 0) {
            FiltersCompiler.freeFunction(fnAddress);
            fnAddress = 0;
        }
    }

    public void compile(MemoryCARW filter, int options) throws SqlException {
        final long filterSize = filter.getAppendOffset();
        final long filterAddress = filter.getPageAddress(0);

        FiltersCompiler.JitError error = tlJitError.get();
        error.reset();
        fnAddress = FiltersCompiler.compileFunction(filterAddress, filterSize, options, error);
        if (error.errorCode() != 0) {
            throw SqlException.position(0)
                    .put("JIT compilation failed [errorCode").put(error.errorCode())
                    .put(", msg=").put(error.message()).put("]");
        }
    }
}
