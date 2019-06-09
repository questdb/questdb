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

package com.questdb.cutlass.text;

public class DefaultTextConfiguration implements TextConfiguration {
    @Override
    public String getAdapterSetConfigurationFileName() {
        return "/text_loader.json";
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
}
