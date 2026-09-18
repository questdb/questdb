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
 *******************************************************************************/

package io.questdb.compat.qwp;

import io.questdb.client.Sender;

/**
 * Public-API probe compiled against the last pre-schema client and then run
 * with either that client or the current client on an isolated classpath.
 */
public final class QwpClientCompatibilityProbe {

    public static void main(String[] args) {
        if (args.length != 2) {
            throw new IllegalArgumentException("expected: <ws-config> <table>");
        }

        try (Sender sender = Sender.fromConfig(args[0])) {
            sender.table(args[1])
                    .longColumn("value", 42)
                    .longColumn("case_id", 1)
                    .atNow();
            sender.table(args[1])
                    .longColumn("value", Long.MIN_VALUE)
                    .longColumn("case_id", 2)
                    .atNow();
            sender.table(args[1])
                    .longColumn("case_id", 3)
                    .atNow();
            sender.flush();
        }
    }

    private QwpClientCompatibilityProbe() {
    }
}
