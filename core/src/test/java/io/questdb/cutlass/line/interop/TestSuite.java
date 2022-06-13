/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
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

package io.questdb.cutlass.line.interop;

import com.eclipsesource.json.*;

import java.io.IOException;
import java.io.Reader;
import java.util.ArrayList;
import java.util.List;

public final class TestSuite {

    private final List<TestCase> testCases;

    private TestSuite(List<TestCase> testCases) {
        this.testCases = testCases;
    }

    public static TestSuite fromJson(Reader reader) throws IOException {
        JsonArray testCasesJson = (JsonArray) Json.parse(reader);
        List<TestCase> testCases = new ArrayList<>(testCasesJson.size());
        for (JsonValue jsonValue : testCasesJson) {
            JsonObject testCaseJson = jsonValue.asObject();
            TestCase testCase = TestCase.fromJson(testCaseJson);
            testCases.add(testCase);
        }

        return new TestSuite(testCases);
    }

    public List<TestCase> getTestCases() {
        return testCases;
    }
}
