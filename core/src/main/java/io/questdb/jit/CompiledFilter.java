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

package io.questdb.jit;

import io.questdb.cairo.vm.api.MemoryCARW;
import io.questdb.griffin.SqlException;
import io.questdb.std.MemoryTag;
import io.questdb.std.ThreadLocal;
import io.questdb.std.Unsafe;

import java.io.Closeable;

/**
 * A wrapper for a JIT-compiled filter function that evaluates SQL WHERE clause predicates
 * and collects matching row IDs.
 * <p>
 * This class compiles filter expressions into native machine code using the asmjit library,
 * supporting both SIMD (AVX2) and scalar execution paths. The compiled function iterates
 * through rows, evaluates the filter predicate, and stores the indices of matching rows
 * into the output array.
 * <p>
 * Use this class for general filter queries where you need access to the actual row data.
 * For count-only queries (e.g., {@code SELECT count(*) FROM table WHERE ...}), prefer
 * {@link CompiledCountOnlyFilter} which avoids the overhead of storing row IDs.
 *
 * @see CompiledCountOnlyFilter
 * @see CompiledFilterIRSerializer
 */
public class CompiledFilter implements Closeable {
    private static final ThreadLocal<FiltersCompiler.JitError> tlJitError = new ThreadLocal<>(FiltersCompiler.JitError::new);
    private long fnAddress;

    /**
     * Executes the compiled filter function on the given data.
     *
     * @param dataAddress         address of the column data pointers array
     * @param dataSize            number of columns
     * @param varSizeAuxAddress   address of variable-size column auxiliary data (binary/string/varchar)
     * @param varsAddress         address of bind variables array
     * @param varsSize            number of bind variables
     * @param filteredRowsAddress address of the output array where matching row indices will be stored
     * @param rowsCount           total number of rows to filter
     * @return the number of rows that matched the filter predicate
     */
    public long call(
            long dataAddress,
            long dataSize,
            long varSizeAuxAddress,
            long varsAddress,
            long varsSize,
            long filteredRowsAddress,
            long rowsCount
    ) {
        return FiltersCompiler.callFunction(
                fnAddress,
                dataAddress,
                dataSize,
                varSizeAuxAddress,
                varsAddress,
                varsSize,
                filteredRowsAddress,
                rowsCount
        );
    }

    /**
     * Releases the native memory associated with the compiled function.
     * Safe to call multiple times.
     */
    @Override
    public void close() {
        if (fnAddress > 0) {
            FiltersCompiler.freeFunction(fnAddress);
            Unsafe.recordMemAlloc(-1, MemoryTag.NATIVE_JIT);
            fnAddress = 0;
        }
    }

    /**
     * Compiles the filter expression into native machine code.
     *
     * @param filter  memory containing the serialized filter IR (intermediate representation)
     * @param options compilation options encoded as a bitmask:
     *                <ul>
     *                <li>bit 0: debug mode (1 = enabled)</li>
     *                <li>bits 1-3: log2 of the max column type size (0=1B, 1=2B, 2=4B, 3=8B, 4=16B)</li>
     *                <li>bits 4-5: execution hint (0=scalar, 1=single-size SIMD, 2=mixed-size)</li>
     *                <li>bit 6: null checks (1 = enabled)</li>
     *                </ul>
     * @throws SqlException if JIT compilation fails
     */
    public void compile(MemoryCARW filter, int options) throws SqlException {
        final long filterSize = filter.getAppendOffset();
        final long filterAddress = filter.getPageAddress(0);

        final FiltersCompiler.JitError error = tlJitError.get();
        error.reset();
        fnAddress = FiltersCompiler.compileFunction(filterAddress, filterSize, options, error);
        if (error.errorCode() != 0) {
            throw SqlException.position(0)
                    .put("JIT compilation failed [errorCode").put(error.errorCode())
                    .put(", msg=").put(error.message()).put("]");
        }
        Unsafe.recordMemAlloc(1, MemoryTag.NATIVE_JIT);
    }
}
