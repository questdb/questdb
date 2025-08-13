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

package io.questdb;

import io.questdb.std.FilesFacade;
import io.questdb.std.datetime.MicrosecondClock;
import io.questdb.std.datetime.microtime.MicrosecondClockImpl;

import java.util.Map;

public class DefaultBootstrapConfiguration implements BootstrapConfiguration {
    private static final String BANNER =
            "     ___                  _   ____  ____\n" +
                    "    / _ \\ _   _  ___  ___| |_|  _ \\| __ )\n" +
                    "   | | | | | | |/ _ \\/ __| __| | | |  _ \\\n" +
                    "   | |_| | |_| |  __/\\__ \\ |_| |_| | |_) |\n" +
                    "    \\__\\_\\\\__,_|\\___||___/\\__|____/|____/\n" +
                    "                        www.questdb.io\n\n";

    @Override
    public String getBanner() {
        return BANNER;
    }

    @Override
    public Map<String, String> getEnv() {
        return System.getenv();
    }

    @Override
    public FilesFacade getFilesFacade() {
        return null;
    }

    @Override
    public MicrosecondClock getMicrosecondClock() {
        return MicrosecondClockImpl.INSTANCE;
    }

    @Override
    public ServerConfiguration getServerConfiguration(Bootstrap bootstrap) throws Exception {
        return null;
    }

    @Override
    public boolean useSite() {
        return true;
    }
}
