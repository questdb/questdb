/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2020 QuestDB
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

package io.questdb.cairo;

import io.questdb.std.FilesFacadeImpl;
import io.questdb.std.str.Path;

import java.io.IOException;

public class TxnScoreboardMultiprocessTest {

    private final CharSequence root;

    public TxnScoreboardMultiprocessTest(String[] args) throws IOException, InterruptedException {
        root = System.getProperty("java.io.tmpdir");
        start();
    }

    private void start() throws InterruptedException {
        try (final Path shmPath = new Path()) {
            try (
                    final TxnScoreboard scoreboard = new TxnScoreboard(FilesFacadeImpl.INSTANCE, shmPath.of(root), 1024)
            ) {
                while (true) {
                    int i = (int) (scoreboard.getMin() + 1);
                    scoreboard.acquireTxn(i);
                    scoreboard.releaseTxn(i);
                    System.out.println("Min txn " + scoreboard.getMin());
                    i++;
                    Thread.sleep(1000);
                }
            }
        }
    }

    public static void main(String[] args) throws Exception {
        new TxnScoreboardMultiprocessTest(args);
    }
}
