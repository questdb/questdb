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

package io.questdb.cutlass.text;

import io.questdb.cairo.CairoError;
import io.questdb.cutlass.json.JsonException;
import io.questdb.cutlass.json.JsonLexer;
import io.questdb.cutlass.text.types.InputFormatConfiguration;
import io.questdb.std.datetime.DateLocale;
import io.questdb.std.datetime.DateLocaleFactory;
import io.questdb.std.datetime.millitime.DateFormatFactory;

import static io.questdb.std.datetime.DateLocaleFactory.EN_LOCALE;

public class DefaultTextConfiguration implements TextConfiguration {
    private final InputFormatConfiguration inputFormatConfiguration;

    public DefaultTextConfiguration() {
        this(null);
    }

    public DefaultTextConfiguration(String confRoot) {
        this(DefaultTextConfiguration.class, confRoot, "/text_loader.json");
    }

    public DefaultTextConfiguration(Class<?> resourceLoader, String confRoot, String resourceName) {
        this.inputFormatConfiguration = new InputFormatConfiguration(
                DateFormatFactory.INSTANCE,
                DateLocaleFactory.INSTANCE,
                EN_LOCALE
        );

        try (JsonLexer lexer = new JsonLexer(1024, 1024)) {
            inputFormatConfiguration.parseConfiguration(resourceLoader, lexer, confRoot, resourceName);
        } catch (JsonException e) {
            throw new CairoError(e);
        }
    }

    @Override
    public int getDateAdapterPoolCapacity() {
        return 16;
    }

    @Override
    public int getDecimalAdapterPoolCapacity() {
        return 16;
    }

    @Override
    public DateLocale getDefaultDateLocale() {
        return EN_LOCALE;
    }

    @Override
    public InputFormatConfiguration getInputFormatConfiguration() {
        return inputFormatConfiguration;
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
        return 0.35d;
    }

    @Override
    public double getMaxRequiredLineLengthStdDev() {
        return 0.8;
    }

    @Override
    public int getMetadataStringPoolCapacity() {
        return 128;
    }

    @Override
    public int getRollBufferLimit() {
        return 16 * 1024;
    }

    @Override
    public int getRollBufferSize() {
        return 4 * 1024;
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
    public boolean isUseLegacyStringDefault() {
        return false;
    }
}
