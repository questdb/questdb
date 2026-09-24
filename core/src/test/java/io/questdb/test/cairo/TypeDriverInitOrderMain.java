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

package io.questdb.test.cairo;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.ColumnTypeTag;
import io.questdb.cairo.IntTypeDriver;

/**
 * Entry point for {@code TypeDriverTest.testClassInitOrder}: a fresh JVM initialises the
 * type classes in the order given by the arguments, then checks that they agree. Exit code 0
 * means they do; any other output or exit code is the failure message.
 */
public final class TypeDriverInitOrderMain {

    private TypeDriverInitOrderMain() {
    }

    public static void main(String[] args) throws Exception {
        for (String step : args) {
            switch (step) {
                case "tag" -> ColumnTypeTag.of(5);
                case "leaf" -> IntTypeDriver.INSTANCE.getWidth();
                case "drivers" -> Class.forName("io.questdb.cairo.TypeDrivers");
                case "type" -> ColumnType.nameOf(5);
                default -> throw new IllegalArgumentException(step);
            }
        }
        check(ColumnTypeTag.of(ColumnType.INT) == ColumnTypeTag.INT, "tag lookup");
        check(ColumnTypeTag.INT.code() == ColumnType.INT, "tag code");
        check(ColumnType.getTypeDriver(ColumnType.INT) == IntTypeDriver.INSTANCE, "int driver");
        check(ColumnType.getTypeDriver(ColumnType.VARCHAR) == ColumnType.getDriver(ColumnType.VARCHAR), "varchar driver");
        check(ColumnType.getTypeDriver(ColumnType.TIMESTAMP_NANO).getTag() == ColumnTypeTag.TIMESTAMP, "encoded type");
        System.out.println("OK " + String.join(",", args));
    }

    private static void check(boolean condition, String what) {
        if (!condition) {
            System.out.println("FAILED: " + what);
            System.exit(1);
        }
    }
}
