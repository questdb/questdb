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

package io.questdb.test.fuzz.sql.generators;

import io.questdb.std.Rnd;
import io.questdb.test.fuzz.sql.GeneratorContext;

/**
 * Generates QuestDB LATEST ON clauses for deduplication queries.
 * <p>
 * Syntax: LATEST ON timestamp_column PARTITION BY column1, column2, ...
 * <p>
 * This returns the latest row for each unique combination of partition columns,
 * based on the timestamp column ordering.
 */
public final class LatestOnGenerator {

    // Common timestamp column names
    private static final String[] TIMESTAMP_COLUMNS = {
            "ts", "timestamp", "created_at", "updated_at", "event_time"
    };

    private LatestOnGenerator() {
        // Static utility class
    }

    /**
     * Generates a complete LATEST ON clause.
     */
    public static void generate(GeneratorContext ctx) {
        Rnd rnd = ctx.rnd();

        ctx.keyword("LATEST");
        ctx.keyword("ON");

        // Timestamp column
        ctx.identifier(TIMESTAMP_COLUMNS[rnd.nextInt(TIMESTAMP_COLUMNS.length)]);

        ctx.keyword("PARTITION");
        ctx.keyword("BY");

        // Partition columns (1-3 columns)
        int columnCount = 1 + rnd.nextInt(3);
        for (int i = 0; i < columnCount; i++) {
            if (i > 0) {
                ctx.comma();
            }
            ctx.identifier(ctx.randomColumnName());
        }
    }

    /**
     * Generates a LATEST ON clause with a specific timestamp column.
     */
    public static void generateWithTimestamp(GeneratorContext ctx, String timestampColumn) {
        Rnd rnd = ctx.rnd();

        ctx.keyword("LATEST");
        ctx.keyword("ON");
        ctx.identifier(timestampColumn);
        ctx.keyword("PARTITION");
        ctx.keyword("BY");

        // Partition columns (1-3 columns)
        int columnCount = 1 + rnd.nextInt(3);
        for (int i = 0; i < columnCount; i++) {
            if (i > 0) {
                ctx.comma();
            }
            ctx.identifier(ctx.randomColumnName());
        }
    }

    /**
     * Generates a LATEST ON clause with explicit timestamp and partition columns.
     */
    public static void generateExplicit(GeneratorContext ctx, String timestampColumn, String... partitionColumns) {
        ctx.keyword("LATEST");
        ctx.keyword("ON");
        ctx.identifier(timestampColumn);
        ctx.keyword("PARTITION");
        ctx.keyword("BY");

        for (int i = 0; i < partitionColumns.length; i++) {
            if (i > 0) {
                ctx.comma();
            }
            ctx.identifier(partitionColumns[i]);
        }
    }

    /**
     * Generates the deprecated LATEST BY syntax (for backwards compatibility testing).
     * Syntax: LATEST BY column1, column2, ...
     */
    public static void generateDeprecated(GeneratorContext ctx) {
        Rnd rnd = ctx.rnd();

        ctx.keyword("LATEST");
        ctx.keyword("BY");

        // Partition columns (1-3 columns)
        int columnCount = 1 + rnd.nextInt(3);
        for (int i = 0; i < columnCount; i++) {
            if (i > 0) {
                ctx.comma();
            }
            ctx.identifier(ctx.randomColumnName());
        }
    }
}
