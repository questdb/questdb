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

package io.questdb.cutlass.http.processors;

import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.CairoException;
import io.questdb.cutlass.text.TextLoader;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.Mutable;
import io.questdb.std.str.Path;

import java.io.Closeable;

public class ImportProcessorState implements Mutable, Closeable {
    public static final int STATE_OK = 0;
    private static final Log LOG = LogFactory.getLog(ImportProcessorState.class);
    // Store engine reference for parquet operations
    private final CairoEngine cairoEngine;
    public int columnIndex = 0;
    public TextLoaderCompletedState completeState;
    boolean analysed = false;
    CharSequence errorMessage;
    boolean forceHeader = false;
    long hi;
    boolean json = false;
    long lo;
    int messagePart = ImportProcessor.MESSAGE_UNKNOWN;
    long parquetBytesWritten = 0;
    long parquetFileDescriptor = -1;
    long parquetFileSize = 0;
    // Parquet import fields
    String parquetFilename;
    long parquetMappedAddress = 0;
    Path parquetPath = new Path();
    int responseState = ImportProcessor.RESPONSE_PREFIX;
    int state;
    String stateMessage;
    TextLoader textLoader;


    ImportProcessorState(CairoEngine engine) {
        this.cairoEngine = engine;
        this.textLoader = new TextLoader(engine);
    }

    @Override
    public void clear() {
        responseState = ImportProcessor.RESPONSE_PREFIX;
        messagePart = ImportProcessor.MESSAGE_UNKNOWN;
        columnIndex = 0;
        analysed = false;
        json = false;
        state = STATE_OK;
        textLoader.clear();
        errorMessage = null;
        clearParquetState();
    }

    @Override
    public void close() {
        clear();
        textLoader = Misc.free(textLoader);
        parquetPath = Misc.free(parquetPath);
    }

    void clearParquetState() {
        if (parquetMappedAddress != 0) {
            try {
                cairoEngine.getConfiguration().getFilesFacade().munmap(parquetMappedAddress, parquetFileSize, MemoryTag.MMAP_IMPORT);
                parquetMappedAddress = 0;
            } catch (CairoException e) {
                LOG.error().$("failed to unmap parquet memory [message=").$safe(e.getFlyweightMessage())
                        .$(", errno=").$(e.getErrno())
                        .$(", address=0x").$(Long.toHexString(parquetMappedAddress))
                        .$(", size=").$(parquetFileSize).$(']').$();
            } catch (Throwable e) {
                LOG.error().$("unexpected error unmapping parquet memory [error=").$(e)
                        .$(", address=0x").$(Long.toHexString(parquetMappedAddress))
                        .$(", size=").$(parquetFileSize).$(']').$();
            }
        }

        if (parquetFileDescriptor != -1) {
            try {
                cairoEngine.getConfiguration().getFilesFacade().close(parquetFileDescriptor);
                parquetFileDescriptor = -1;
            } catch (CairoException e) {
                LOG.error().$("failed to close parquet file [message=").$safe(e.getFlyweightMessage())
                        .$(", errno=").$(e.getErrno())
                        .$(", fd=").$(parquetFileDescriptor)
                        .$(", path=").$(parquetPath).$(']').$();
            } catch (Throwable e) {
                LOG.error().$("unexpected error closing parquet file [error=").$(e)
                        .$(", fd=").$(parquetFileDescriptor)
                        .$(", path=").$(parquetPath).$(']').$();
            }
        }

        parquetPath.trimTo(0);
        parquetFilename = null;
        parquetFileSize = 0;
        parquetBytesWritten = 0;
    }

    void initParquetImport(CharSequence filename, CharSequence sqlCopyInputRoot, long expectedSize) {
        this.parquetFilename = filename.toString();
        parquetPath.of(sqlCopyInputRoot).concat(filename);
        this.parquetFileSize = expectedSize;
    }

    void snapshotStateAndCloseWriter() {
        if (completeState == null) {
            completeState = new TextLoaderCompletedState();
        }
        completeState.copyState(textLoader);
        textLoader.closeWriter();
    }
}
