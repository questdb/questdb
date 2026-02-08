/*******************************************************************************
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

package io.questdb.recovery;

public final class AnsiColor {
    public static final AnsiColor NONE = new AnsiColor(false);
    private static final String BOLD = "\033[1m";
    private static final String CYAN = "\033[36m";
    private static final String GREEN = "\033[32m";
    private static final String RED = "\033[31m";
    private static final String RESET = "\033[0m";
    private static final String YELLOW = "\033[33m";

    private final boolean enabled;

    public AnsiColor(boolean enabled) {
        this.enabled = enabled;
    }

    public String bold(String text) { return enabled ? BOLD + text + RESET : text; }

    public String cyan(String text) { return enabled ? CYAN + text + RESET : text; }

    public String green(String text) { return enabled ? GREEN + text + RESET : text; }

    public String red(String text) { return enabled ? RED + text + RESET : text; }

    public String yellow(String text) { return enabled ? YELLOW + text + RESET : text; }
}
