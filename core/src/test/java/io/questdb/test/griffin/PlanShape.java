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

package io.questdb.test.griffin;

import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.TextPlanSink;

/**
 * Renders the factory tree of a physical plan as "Node > Node > ...", keeping every node line
 * (factory, algorithm, scan) and dropping attribute lines such as "filter:" or "intervals:".
 */
final class PlanShape {
    private PlanShape() {
    }

    static String of(RecordCursorFactory factory, SqlExecutionContext context) {
        final TextPlanSink sink = new TextPlanSink();
        sink.of(factory, context);
        final StringBuilder shape = new StringBuilder();
        for (int i = 1, n = sink.getLineCount(); i <= n; i++) {
            final String line = sink.getLine(i).toString().trim();
            if (line.isEmpty() || isAttribute(line)) {
                continue;
            }
            if (!shape.isEmpty()) {
                shape.append(" > ");
            }
            shape.append(line);
        }
        return shape.toString();
    }

    private static boolean isAttribute(String line) {
        if (!Character.isLowerCase(line.charAt(0))) {
            return false;
        }
        for (int i = 1, n = line.length(); i < n; i++) {
            final char c = line.charAt(i);
            if (c == ':') {
                return true;
            }
            if (!Character.isLetter(c) && c != '_' && c != ' ') {
                return false;
            }
        }
        return false;
    }
}
