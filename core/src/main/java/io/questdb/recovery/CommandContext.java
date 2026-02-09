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

import io.questdb.std.FilesFacade;

public class CommandContext {
    private final AnsiColor color;
    private final ColumnCheckService columnCheckService;
    private final ColumnValueReader columnValueReader;
    private final FilesFacade ff;
    private final NavigationContext nav;
    private final PartitionScanService partitionScanService;
    private final ConsoleRenderer renderer;

    public CommandContext(
            NavigationContext nav,
            ColumnCheckService columnCheckService,
            ColumnValueReader columnValueReader,
            FilesFacade ff,
            PartitionScanService partitionScanService,
            ConsoleRenderer renderer
    ) {
        this.color = renderer.getColor();
        this.columnCheckService = columnCheckService;
        this.columnValueReader = columnValueReader;
        this.ff = ff;
        this.nav = nav;
        this.partitionScanService = partitionScanService;
        this.renderer = renderer;
    }

    public AnsiColor getColor() {
        return color;
    }

    public ColumnCheckService getColumnCheckService() {
        return columnCheckService;
    }

    public ColumnValueReader getColumnValueReader() {
        return columnValueReader;
    }

    public FilesFacade getFf() {
        return ff;
    }

    public NavigationContext getNav() {
        return nav;
    }

    public PartitionScanService getPartitionScanService() {
        return partitionScanService;
    }

    public ConsoleRenderer getRenderer() {
        return renderer;
    }
}
