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
import io.questdb.std.MemoryTag;
import io.questdb.std.ThreadLocal;
import io.questdb.std.Unsafe;

import java.io.Closeable;
import java.util.Objects;

/**
 * Represents a compiled filter function via JIT compilation.
 * <p>
 * This class handles the lifecycle of the compiled native function,
 * including compilation, invocation, and resource cleanup.
 * <p>
 * Instances are not thread-safe and should be used with care if shared.
 */
public final class CompiledFilter implements Closeable {

    private static final ThreadLocal<FiltersCompiler.JitError> tlJitError = new ThreadLocal<>(FiltersCompiler.JitError::new);

    private long fnAddress;

    /**
     * Calls the compiled filter function with the provided data and parameters.
     *
     * @param dataAddress      address of the data to filter
     * @param dataSize         size of the data in bytes
     * @param varSizeAuxAddress auxiliary variables size address
     * @param varsAddress      address of variables
     * @param varsSize         size of the variables in bytes
     * @param rowsAddress      address of the rows
     * @param rowsSize         size of the rows in bytes
     * @param rowsStartOffset  offset from the start of rows
     * @return number of filtered rows or a result indicator
     * @throws IllegalStateException if the filter has not been compiled yet
     */
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
        if (!isCompiled()) {
            throw new IllegalStateException("Filter function is not compiled.");
        }
        // Defensive validation could be added here if needed
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

    /**
     * Frees the compiled function and releases native resources.
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
     * Compiles the given filter memory into a native function with the specified options.
     *
     * @param filter  the memory buffer containing the filter code
     * @param options compilation options flags
     * @throws SqlException if compilation fails with detailed error info
     * @throws NullPointerException if filter is null
     */
    public void compile(MemoryCARW filter, int options) throws SqlException {
        Objects.requireNonNull(filter, "filter cannot be null");
        final long filterSize = filter.getAppendOffset();
        final long filterAddress = filter.getPageAddress(0);

        FiltersCompiler.JitError error = tlJitError.get();
        error.reset();

        fnAddress = FiltersCompiler.compileFunction(filterAddress, filterSize, options, error);
        if (error.errorCode() != 0) {
            throw SqlException.position(0)
                    .put("JIT compilation failed [errorCode=").put(error.errorCode())
                    .put(", msg=").put(error.toString())
                    .put("]");
        }
        Unsafe.recordMemAlloc(1, MemoryTag.NATIVE_JIT);
    }

    /**
     * Returns true if the filter function has been successfully compiled.
     *
     * @return true if compiled; false otherwise
     */
    public boolean isCompiled() {
        return fnAddress > 0;
    }
}

