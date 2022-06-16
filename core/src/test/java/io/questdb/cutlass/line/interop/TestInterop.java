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

import io.questdb.cutlass.line.LineSenderException;
import io.questdb.cutlass.line.LineTcpSender;
import io.questdb.cutlass.line.Sender;
import org.junit.Test;

import java.io.InputStreamReader;
import java.io.Reader;

import static org.junit.Assert.*;

public class TestInterop {

    @Test
    public void testInterop() throws Exception {
        TestSuite testSuite;
        try (Reader reader = new InputStreamReader(TestInterop.class.getResourceAsStream("/io/questdb/cutlass/line/interop/ilp-client-interop-test.json"))) {
            testSuite = TestSuite.fromJson(reader);
        }

        StringChannel channel = new StringChannel();
        for (TestCase testCase : testSuite.getTestCases()) {
            try (Sender sender = new LineTcpSender(channel, 1024)) {
                System.out.println("test case: " + testCase.getName());
                try {
                    sender.table(testCase.getTable());
                    for (Symbol symbol : testCase.getSymbols()) {
                        sender.symbol(symbol.getName(), symbol.getValue());
                    }
                    for (Column column : testCase.getColumns()) {
                        String name = column.getName();
                        switch (column.getType()) {
                            case STRING:
                                sender.stringColumn(name, column.valueAsString());
                                break;
                            case LONG:
                                sender.longColumn(name, column.valueAsLong());
                                break;
                            case BOOLEAN:
                                sender.boolColumn(name, column.valueAsBoolean());
                                break;
                            case DOUBLE:
                                sender.doubleColumn(name, column.valueAsDouble());
                                break;
                            default:
                                throw new UnsupportedOperationException("unknown type " + column.getType());
                        }
                    }
                    sender.atNow();
                    sender.flush();

                    ResultStatus status = testCase.getExpectedResult().getStatus();
                    assertEquals("Case '" + testCase.getName() + "' should have failed!", status, ResultStatus.SUCCESS);
                    assertLine(testCase.getExpectedResult(), channel.toString());
                } catch (LineSenderException e) {
                    assertEquals(ResultStatus.ERROR, testCase.getExpectedResult().getStatus());
                } finally {
                    channel.reset();
                }
            }
        }
    }

    private static void assertLine(Result expectedResult, String actualLine) {
        if (!actualLine.endsWith("\n")) {
            fail("Produced lines must ends with EOL");
        }

        String expectedLine = expectedResult.getLine();
        if (!expectedLine.equals(actualLine.substring(0, actualLine.length() - 1))) {
            fail("Expected: '" + expectedLine + "', actual: '" + actualLine + "'");
        }
    }
}
