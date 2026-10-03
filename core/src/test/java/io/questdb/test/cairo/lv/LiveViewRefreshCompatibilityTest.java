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

package io.questdb.test.cairo.lv;

import io.questdb.cairo.TableToken;
import io.questdb.cairo.file.BlockFileWriter;
import io.questdb.cairo.lv.LiveViewDefinition;
import io.questdb.cairo.lv.LiveViewInstance;
import io.questdb.cairo.lv.LiveViewRefreshJob;
import io.questdb.griffin.SqlException;
import io.questdb.std.Chars;
import io.questdb.std.str.Path;
import org.junit.Assert;
import org.junit.Test;

public class LiveViewRefreshCompatibilityTest extends AbstractLiveViewTest {
    @Test
    public void testPersistedCatalogueSubqueryRefreshesAfterRestart() throws Exception {
        assertMemoryLeak(() -> {
            setCurrentMicros(0);
            execute("CREATE TABLE base (ts TIMESTAMP, x INT, g SYMBOL) TIMESTAMP(ts) PARTITION BY DAY WAL");
            final String legacySql = "SELECT ts, x, count(*) OVER (PARTITION BY g ORDER BY ts "
                    + "ROWS BETWEEN 1_000_000 PRECEDING AND CURRENT ROW) AS rn "
                    + "FROM base WHERE g IN (SELECT table_name FROM tables())";
            try {
                execute("CREATE LIVE VIEW rejected FLUSH EVERY 100ms START FROM BEGINNING AS " + legacySql);
                Assert.fail("CREATE must reject newly restricted catalogue functions");
            } catch (SqlException e) {
                Assert.assertTrue(Chars.contains(e.getFlyweightMessage(),
                        "administrative function cannot be used in live view: tables"));
            }
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms START FROM BEGINNING AS "
                    + "SELECT ts, x, count(*) OVER (PARTITION BY g ORDER BY ts "
                    + "ROWS BETWEEN 1_000_000 PRECEDING AND CURRENT ROW) AS rn FROM base WHERE g = 'base'");
            execute("INSERT INTO base VALUES ('2026-01-01T00:00:01.000000Z', 10, 'base')");
            drainWalQueue();

            // Install the SQL an older release would have persisted, keeping the CREATE-time
            // metadata/dependencies. Rebuild the registry from _lv just as startup does.
            final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
            final LiveViewDefinition current = instance.getDefinition();
            final LiveViewDefinition legacy = new LiveViewDefinition(
                    current.getViewName(), legacySql, current.getBaseTableName(), current.getBaseTableToken(),
                    current.getBaseTimestampType(), current.getFlushEveryInterval(), current.getFlushEveryIntervalUnit(),
                    current.getInMemoryInterval(), current.getInMemoryIntervalUnit(), current.getPartitionBy(),
                    current.getViewLowerBoundTimestamp(), current.getStartFromKind(), current.getAnchorSpec(),
                    current.getDependencyColumnNames(), current.getDependencyColumnTypes(), current.getMetadata()
            );
            final TableToken token = instance.getLiveViewToken();
            try (Path path = new Path();
                 BlockFileWriter writer = new BlockFileWriter(engine.getConfiguration().getFilesFacade(),
                         engine.getConfiguration().getCommitMode())) {
                path.of(engine.getConfiguration().getDbRoot()).concat(token)
                        .concat(LiveViewDefinition.LIVE_VIEW_DEFINITION_FILE_NAME);
                writer.of(path.$());
                LiveViewDefinition.append(legacy, writer);
            }
            engine.getLiveViewRegistry().clear();
            engine.buildViewGraphs();

            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveSeedToCompletion(job, "lv");
                driveRefreshToQuiescence(job);
                assertQuery("SELECT x FROM lv ORDER BY ts").noLeakCheck().expectSize().returns("x\n10\n");
                execute("INSERT INTO base VALUES ('2026-01-01T00:00:02.000000Z', 20, 'base')");
                driveRefreshToQuiescence(job);
                assertQuery("SELECT x FROM lv ORDER BY ts").noLeakCheck().expectSize().returns("x\n10\n20\n");
            }
            engine.getLiveViewRegistry().clear();
            engine.buildViewGraphs();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                execute("INSERT INTO base VALUES ('2026-01-01T00:00:03.000000Z', 30, 'base')");
                driveRefreshToQuiescence(job);
                assertQuery("SELECT x FROM lv ORDER BY ts").noLeakCheck().expectSize().returns("x\n10\n20\n30\n");
            }
            assertQuery("SELECT view_name, view_status FROM live_views() WHERE view_name = 'lv'")
                    .noLeakCheck().noRandomAccess().returns("view_name\tview_status\nlv\tactive\n");
        });
    }
}
