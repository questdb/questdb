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

package io.questdb.test.tools;

public class BindVariableTestTuple {
    public static final int MUST_SUCCEED = -1;
    private final String description;
    private final int errorPosition;
    private final String expected;
    private final BindVariableTestSetter setter;

    public BindVariableTestTuple(String description, String expected, BindVariableTestSetter setter) {
        // failure is not expected when error position is -1
        this(description, expected, setter, MUST_SUCCEED);
    }

    public BindVariableTestTuple(String description, String expected, BindVariableTestSetter setter, int errorPosition) {
        this.description = description;
        this.expected = expected;
        this.setter = setter;
        this.errorPosition = errorPosition;
    }

    public String getDescription() {
        return description;
    }

    public int getErrorPosition() {
        return errorPosition;
    }

    public CharSequence getExpected() {
        return expected;
    }

    public BindVariableTestSetter getSetter() {
        return setter;
    }
}
