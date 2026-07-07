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

package io.questdb.test.tools;

import io.questdb.griffin.SqlExecutionContext;

/**
 * One bind-variable case for the {@code assertQuery(...).assertBinds(...)} terminal: a bind-variable
 * assignment paired with its own expected outcome. A case either succeeds with an expected result set
 * or fails with an error whose message contains a given fragment (optionally at a known position).
 * <p>
 * The query compiles once; each case clears the bind-variable service, applies its {@code binds}, and
 * re-opens the cursor. Bind variables are read at cursor-open time, so a single compiled factory can
 * legitimately produce different result sets - and even a different scan direction - across cases.
 * <p>
 * The optional {@link #ascending()}/{@link #descending()}, {@link #randomAccess(boolean)} and
 * {@link #expectSize(boolean)} knobs override the chain-level defaults for a single case only; left
 * unset, a case inherits whatever the {@code assertQuery(...)} chain declared. Use them for the cases
 * where a particular bind value flips the factory's behavior.
 */
public class BindVarTuple {
    public enum Order {ASC, DESC, INHERIT}

    public static final int ANY_POSITION = -1;

    private final BindVariableTestSetter binds;
    private final String description;
    private final int errorPosition;
    private final String expected;
    private final boolean expectedToFail;
    private SqlExecutionContext executionContext;
    private Boolean expectSizeOverride;
    private Order order = Order.INHERIT;
    private Boolean randomAccessOverride;

    private BindVarTuple(String description, String expected, BindVariableTestSetter binds, boolean expectedToFail, int errorPosition) {
        this.description = description;
        this.expected = expected;
        this.binds = binds;
        this.expectedToFail = expectedToFail;
        this.errorPosition = errorPosition;
    }

    /**
     * The case must fail at {@code position} with an error message containing {@code errorContains}.
     */
    public static BindVarTuple fails(String description, int position, String errorContains, BindVariableTestSetter binds) {
        return new BindVarTuple(description, errorContains, binds, true, position);
    }

    /**
     * The case must fail with an error message containing {@code errorContains}, without checking the
     * error position.
     */
    public static BindVarTuple fails(String description, String errorContains, BindVariableTestSetter binds) {
        return new BindVarTuple(description, errorContains, binds, true, ANY_POSITION);
    }

    /**
     * The case must succeed and produce exactly {@code expected}.
     */
    public static BindVarTuple ok(String description, String expected, BindVariableTestSetter binds) {
        return new BindVarTuple(description, expected, binds, false, ANY_POSITION);
    }

    /**
     * Override the chain's designated-timestamp order to ascending for this case only.
     */
    public BindVarTuple ascending() {
        this.order = Order.ASC;
        return this;
    }

    /**
     * Override the chain's designated-timestamp order to descending for this case only.
     */
    public BindVarTuple descending() {
        this.order = Order.DESC;
        return this;
    }

    /**
     * Override the chain's {@code expectSize} flag for this case only.
     */
    public BindVarTuple expectSize(boolean expectSize) {
        this.expectSizeOverride = expectSize;
        return this;
    }

    public BindVariableTestSetter getBinds() {
        return binds;
    }

    public String getDescription() {
        return description;
    }

    public int getErrorPosition() {
        return errorPosition;
    }

    public SqlExecutionContext getExecutionContext() {
        return executionContext;
    }

    public String getExpected() {
        return expected;
    }

    public Boolean getExpectSizeOverride() {
        return expectSizeOverride;
    }

    public Order getOrder() {
        return order;
    }

    public Boolean getRandomAccessOverride() {
        return randomAccessOverride;
    }

    public boolean isExpectedToFail() {
        return expectedToFail;
    }

    /**
     * Override the chain's random-access support for this case only.
     */
    public BindVarTuple randomAccess(boolean supportsRandomAccess) {
        this.randomAccessOverride = supportsRandomAccess;
        return this;
    }

    /**
     * Execute THIS case under a specific {@link SqlExecutionContext} (with its own bind-variable service)
     * instead of the chain's context. The factory still compiles once under the chain context; each case's
     * binds are applied to, and its cursor opened with, this context. Use to verify that a compiled factory
     * reads bind values from the execution context on every re-execution, not a cached service.
     */
    public BindVarTuple withContext(SqlExecutionContext executionContext) {
        this.executionContext = executionContext;
        return this;
    }
}
