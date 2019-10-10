/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2019 Appsicle
 *
 * This program is free software: you can redistribute it and/or  modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 ******************************************************************************/

package io.questdb.cutlass.text;

import io.questdb.cutlass.json.JsonException;
import io.questdb.cutlass.json.JsonLexer;
import io.questdb.cutlass.text.types.InputFormatConfiguration;
import io.questdb.std.microtime.TimestampFormatFactory;
import io.questdb.std.microtime.TimestampLocaleFactory;
import io.questdb.std.time.DateFormatFactory;
import io.questdb.std.time.DateLocaleFactory;

public class DefaultTextConfiguration implements TextConfiguration {
    private final InputFormatConfiguration inputFormatConfiguration;

    public DefaultTextConfiguration() throws JsonException {
        this("/text_loader.json");
    }

    public DefaultTextConfiguration(String resourceName) throws JsonException {
        this.inputFormatConfiguration = new InputFormatConfiguration(
                new DateFormatFactory(),
                DateLocaleFactory.INSTANCE,
                new TimestampFormatFactory(),
                TimestampLocaleFactory.INSTANCE
        );

        try (JsonLexer lexer = new JsonLexer(1024, 1024)) {
            inputFormatConfiguration.parseConfiguration(lexer, resourceName);
        }
    }

    @Override
    public int getDateAdapterPoolCapacity() {
        return 16;
    }

    @Override
    public int getJsonCacheLimit() {
        return 16384;
    }

    @Override
    public int getJsonCacheSize() {
        return 8192;
    }

    @Override
    public double getMaxRequiredDelimiterStdDev() {
        return 0.1222d;
    }

    @Override
    public int getMetadataStringPoolCapacity() {
        return 128;
    }

    @Override
    public int getRollBufferLimit() {
        return 4096;
    }

    @Override
    public int getRollBufferSize() {
        return 1024;
    }

    @Override
    public int getTextAnalysisMaxLines() {
        return 1000;
    }

    @Override
    public int getTextLexerStringPoolCapacity() {
        return 32;
    }

    @Override
    public int getTimestampAdapterPoolCapacity() {
        return 16;
    }

    @Override
    public int getUtf8SinkSize() {
        return 4096;
    }

    @Override
    public InputFormatConfiguration getInputFormatConfiguration() {
        return inputFormatConfiguration;
    }
}
