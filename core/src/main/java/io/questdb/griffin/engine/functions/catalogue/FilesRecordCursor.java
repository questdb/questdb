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

package io.questdb.griffin.engine.functions.catalogue;

import io.questdb.cairo.TableUtils;
import io.questdb.cairo.sql.NoRandomAccessRecordCursor;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.engine.functions.str.SizePrettyFunctionFactory;
import io.questdb.std.Files;
import io.questdb.std.FilesFacade;
import io.questdb.std.IntStack;
import io.questdb.std.LongStack;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;
import io.questdb.std.str.Utf8Sequence;
import io.questdb.std.str.Utf8StringSink;
import io.questdb.std.str.Utf8s;

/**
 * Implementation for directory traversal and file listing.
 * Used by both ImportFilesFunctionFactory and ExportFilesFunctionFactory.
 */
public class FilesRecordCursor implements NoRandomAccessRecordCursor {
    private static final int MODIFIED_TIME_COLUMN = 3;
    private static final int PATH_COLUMN = 0;
    private static final int SIZE_COLUMN = 1;
    private static final int SIZE_HUMAN_COLUMN = 2;
    protected final FilesFacade ff;
    protected final Utf8StringSink fileNameSink = new Utf8StringSink();
    protected final FileRecord record = new FileRecord();
    protected final Utf8StringSink relativePathSink = new Utf8StringSink();
    protected final Path rootPath;
    protected final int rootPathLen;
    protected final Path workingPath = new Path(MemoryTag.NATIVE_PATH);
    private final LongStack dirFindPtrs = new LongStack();
    private final IntStack dirPathLens = new IntStack();
    protected long findPtr = 0;
    protected boolean initialized = false;
    private boolean cachedDirPathAscii;
    private int cachedDirPathLen = 0;
    private boolean dirPathChanged = true;

    public FilesRecordCursor(FilesFacade ff, Path rootPath, int rootPathLen) {
        this.ff = ff;
        this.rootPath = rootPath;
        this.rootPathLen = rootPathLen;
        this.workingPath.of(rootPath);
    }

    @Override
    public void close() {
        closeCurrentFind();
        Misc.free(workingPath);
    }

    @Override
    public Record getRecord() {
        return record;
    }

    /**
     * Performs depth-first traversal of the directory tree.
     */
    @Override
    public boolean hasNext() {
        while (true) {
            if (!initialized) {
                if (!ff.exists(rootPath.$())) {
                    return false;
                }
                workingPath.of(rootPath);
                findPtr = Files.findFirst(workingPath.$());
                initialized = true;
                if (findPtr <= 0) {
                    return false;
                }
            }

            while (findPtr <= 0 && dirFindPtrs.notEmpty()) {
                findPtr = dirFindPtrs.pop();
                int pathLen = dirPathLens.pop();
                workingPath.trimTo(pathLen);
                dirPathChanged = true;
            }

            if (findPtr <= 0) {
                return false;
            }
            final long namePtr = Files.findName(findPtr);
            if (namePtr == 0) {
                Files.findClose(findPtr);
                findPtr = 0;
                continue;
            }
            final int type = Files.findType(findPtr);
            fileNameSink.clear();
            Utf8s.utf8ZCopy(namePtr, fileNameSink);

            if (!Files.notDots(fileNameSink)) {
                if (Files.findNext(findPtr) <= 0) {
                    Files.findClose(findPtr);
                    findPtr = 0;
                }
                continue;
            }

            if (type == Files.DT_DIR) {
                if (Files.findNext(findPtr) > 0) {
                    dirFindPtrs.push(findPtr);
                    dirPathLens.push(workingPath.size());
                } else {
                    Files.findClose(findPtr);
                }

                workingPath.concat(fileNameSink);
                findPtr = Files.findFirst(workingPath.$());
                dirPathChanged = true;
                continue;
            }

            if (type == Files.DT_FILE) {
                buildRelativePath();
                int oldLen = workingPath.size();
                workingPath.concat(fileNameSink);
                long size = ff.length(workingPath.$());
                long modifiedTime = ff.getLastModified(workingPath.$());
                workingPath.trimTo(oldLen);
                record.of(relativePathSink, size, modifiedTime);
                if (Files.findNext(findPtr) <= 0) {
                    Files.findClose(findPtr);
                    findPtr = 0;
                }
                return true;
            }

            if (Files.findNext(findPtr) <= 0) {
                Files.findClose(findPtr);
                findPtr = 0;
            }
        }
    }

    @Override
    public long preComputedStateSize() {
        return 0;
    }

    @Override
    public long size() {
        return -1;
    }

    @Override
    public void toTop() {
        closeCurrentFind();
        initialized = false;
    }

    private void buildRelativePath() {
        if (dirPathChanged) {
            relativePathSink.clear();
            if (workingPath.size() > rootPathLen) {
                int start = rootPathLen + 1;
                for (int i = start; i < workingPath.size(); i++) {
                    relativePathSink.putAny(workingPath.byteAt(i));
                }
                relativePathSink.putAscii(Files.SEPARATOR);
            }
            cachedDirPathLen = relativePathSink.size();
            cachedDirPathAscii = relativePathSink.isAscii();
            dirPathChanged = false;
        } else {
            relativePathSink.clear(cachedDirPathLen, cachedDirPathAscii);
        }
        relativePathSink.put(fileNameSink);
    }

    private void closeCurrentFind() {
        if (findPtr > 0) {
            Files.findClose(findPtr);
            findPtr = 0;
        }
        while (dirFindPtrs.notEmpty()) {
            long ptr = dirFindPtrs.pop();
            if (ptr > 0) {
                Files.findClose(ptr);
            }
        }
        dirFindPtrs.clear();
        dirPathLens.clear();
        dirPathChanged = true;
        cachedDirPathLen = 0;
    }

    public static class FileRecord implements Record {
        private final Utf8StringSink sinkA = new Utf8StringSink();
        private final Utf8StringSink sinkB = new Utf8StringSink();
        private final StringSink sizeSink = new StringSink();
        protected Utf8StringSink fileName;
        protected long modifiedTime;
        protected long size;

        @Override
        public long getDate(int col) {
            if (col == MODIFIED_TIME_COLUMN) {
                return modifiedTime;
            }
            return 0;
        }

        @Override
        public long getLong(int col) {
            if (col == SIZE_COLUMN) {
                return size;
            }
            return 0;
        }

        @Override
        public CharSequence getStrA(int col) {
            if (col == SIZE_HUMAN_COLUMN) {
                return sizeSink;
            }
            return null;
        }

        @Override
        public CharSequence getStrB(int col) {
            return getStrA(col);
        }

        @Override
        public int getStrLen(int col) {
            return TableUtils.NULL_LEN;
        }

        @Override
        public Utf8Sequence getVarcharA(int col) {
            if (col == PATH_COLUMN) {
                sinkA.clear();
                sinkA.put(fileName);
                return sinkA;
            }
            return null;
        }

        @Override
        public Utf8Sequence getVarcharB(int col) {
            if (col == PATH_COLUMN) {
                sinkB.clear();
                sinkB.put(fileName);
                return sinkB;
            }
            return null;
        }

        public void of(Utf8StringSink fileName, long size, long modifiedTime) {
            this.fileName = fileName;
            this.size = size;
            this.modifiedTime = modifiedTime;
            this.sizeSink.clear();
            SizePrettyFunctionFactory.toSizePretty(sizeSink, size);
        }
    }
}