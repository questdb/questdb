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

/**
 * One step for the {@code assertQuery(...).mutateStepwise(...)} terminal: a DDL/DML statement to run
 * against the live engine (typically an INSERT that grows the input table) paired with the result the
 * SAME held factory must produce once that statement has been applied.
 * <p>
 * Unlike {@code mutateWith(...)} (a single before/after transition), a list of {@link MutationStep}s
 * exercises one compiled factory across many incremental mutations, re-asserting after each - the shape
 * needed to verify that a shared factory survives repeated re-executions as its input grows.
 */
public class MutationStep {
    private final Runnable action;
    private final String expected;
    private final String mutation;

    private MutationStep(Runnable action, String mutation, String expected) {
        this.action = action;
        this.mutation = mutation;
        this.expected = expected;
    }

    /**
     * @param mutation a statement to execute against the live engine before re-asserting (e.g. an INSERT)
     * @param expected the result the held factory must produce once {@code mutation} has been applied
     */
    public static MutationStep of(String mutation, String expected) {
        return new MutationStep(null, mutation, expected);
    }

    /**
     * A step that runs {@code action} before {@code mutation}, for a change the statement cannot make on
     * its own, such as moving the test clock forward to check that the held factory follows the time of
     * each execution rather than the time it compiled at.
     *
     * @param action   runs before {@code mutation}
     * @param mutation a statement to execute against the live engine before re-asserting (e.g. an INSERT)
     * @param expected the result the held factory must produce once {@code action} and {@code mutation}
     *                 have been applied
     */
    public static MutationStep of(Runnable action, String mutation, String expected) {
        return new MutationStep(action, mutation, expected);
    }

    public String getExpected() {
        return expected;
    }

    public String getMutation() {
        return mutation;
    }

    public void runAction() {
        if (action != null) {
            action.run();
        }
    }
}
