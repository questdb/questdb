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

package io.questdb.cairo;


import io.questdb.std.str.Path;

public class TxnScoreboardPoolV1 implements TxnScoreboardPool {
    private final CairoConfiguration configuration;
    private final ThreadLocal<ScoreboardPoolTenant> tlScoreboardPoolV1 = new ThreadLocal<>();

    public TxnScoreboardPoolV1(CairoConfiguration configuration) {
        this.configuration = configuration;
    }

    @Override
    public void clear() {
        tlScoreboardPoolV1.remove();
    }

    @Override
    public TxnScoreboard getTxnScoreboard(TableToken token) {
        var scoreboard = tlScoreboardPoolV1.get();
        if (scoreboard == null) {
            scoreboard = new ScoreboardPoolTenant(configuration, this, token);
        } else {
            // Don't use .remove() here to keep the TL around.
            // Static analysis warning is irrelevant as our threads
            // have the same lifetime as the server instance.
            //noinspection ThreadLocalSetWithNull
            tlScoreboardPoolV1.set(null);
        }
        Path path = Path.getThreadLocal(configuration.getDbRoot());
        scoreboard.ofRW(token, path.concat(token));
        assert tlScoreboardPoolV1.get() == null;
        return scoreboard;
    }

    @Override
    public boolean releaseInactive() {
        return false;
    }

    @Override
    public void remove(TableToken token) {
        // no-op
    }

    static class ScoreboardPoolTenant extends TxnScoreboardV1 {
        private final TxnScoreboardPoolV1 parent;

        public ScoreboardPoolTenant(CairoConfiguration configuration, TxnScoreboardPoolV1 parent, TableToken tableToken) {
            super(configuration.getFilesFacade(), configuration.getTxnScoreboardEntryCount(), tableToken);
            this.parent = parent;
        }

        @Override
        public void close() {
            super.close();
            if (parent.tlScoreboardPoolV1.get() == null) {
                parent.tlScoreboardPoolV1.set(this);
            }
        }
    }
}
