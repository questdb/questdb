/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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

import io.questdb.cairo.vm.api.*;
import io.questdb.std.FilesFacade;
import io.questdb.std.str.LPSZ;

public interface ColumnTypeDriver {

    /**
     * Returns bytes count for the given row count. This method is similar to {@link #getAuxVectorSize(long)}
     * except it is used in the intermediate calculations and must return exact bytes for the row
     * disregarding the N+1 storage model.
     *
     * @param rowCount the row count
     * @return returns size of storage in bytes
     */
    long auxRowsToBytes(long rowCount);

    void configureAuxMemMA(FilesFacade ff, MemoryMA auxMem, LPSZ fileName, long dataAppendPageSize, int memoryTag, long opts, int madviseOpts);

    void configureAuxMemMA(MemoryMA auxMem);

    void configureAuxMemO3RSS(MemoryARW auxMem);

    /**
     * Configures AUX memory used by TableWriter to read WAL data. The mapping size will
     * depend on the size of entry for each row.
     *
     * @param ff        files facade
     * @param auxMem    the memory to configure
     * @param fd        the fd of the file of the memory, could be -1
     * @param fileName  the file name for the memory
     * @param rowLo     the first row of the mapping
     * @param rowHi     the last row of the mapping
     * @param memoryTag the memory tag to help identify sources of memory leaks
     * @param opts      mapping options
     */
    void configureAuxMemOM(FilesFacade ff, MemoryOM auxMem, int fd, LPSZ fileName, long rowLo, long rowHi, int memoryTag, long opts);

    void configureDataMemOM(FilesFacade ff, MemoryR auxMem, MemoryOM dataMem, int dataFd, LPSZ fileName, long rowLo, long rowHi, int memoryTag, long opts);

    /**
     * Returns offset in bytes of the aux entry that describes the provided row number.
     *
     * @param row the row number to locate offset of
     * @return the offset
     */
    long getAuxVectorOffset(long row);

    /**
     * Calculates size in bytes that is required to store the given number of rows in the
     * entire vector. If storage model is N+1, this method must reflect that. Calculation
     * is similar to {@link #auxRowsToBytes(long)}, which ignored N+1 storage model.
     *
     * @param storageRowCount the number of rows to store in the aux vector
     * @return the size of the required vector.
     */
    long getAuxVectorSize(long storageRowCount);

    /**
     * Minimum entry size in the data vector, typically allocated for storing nulls.
     *
     * @return number of bytes required to store null value.
     */
    long getDataVectorMinEntrySize();

    long getDataVectorOffset(long auxMemAddr, long row);

    /**
     * Data vector size between two rows. Rows are inclusive.
     *
     * @param auxMemAddr pointer to the aux vector
     * @param rowLo      start row, inclusive.
     * @param rowHi      end row, inclusive.
     * @return size of data vector in bytes between these two rows.
     */
    long getDataVectorSize(long auxMemAddr, long rowLo, long rowHi);

    long getDataVectorSizeAt(long auxMemAddr, long row);

    long getDataVectorSizeAtFromFd(FilesFacade ff, int auxFd, long row);

    long getMinAuxVectorSize();

    void o3ColumnMerge(
            long timestampMergeIndexAddr,
            long timestampMergeIndexCount,
            long srcAuxAddr1,
            long srcDataAddr1,
            long srcAuxAddr2,
            long srcDataAddr2,
            long dstAuxAddr,
            long dstDataAddr,
            long dstDataOffset
    );

    /**
     * Copies aux vector from source memory pointer to either the destination memory or directly to file.
     *
     * @param ff            the file facade for test simulation
     * @param srcAddr       the source address where vector is
     * @param srcLo         the row number, inclusive, where to begin copy from
     * @param srcHi         the last row number, inclusive, that has to make it into the copy
     * @param dstAddr       the destination address, when mixedIOFlag is set, the destination address is ignored
     * @param dstFileOffset the file offset, to be used with mixedIOFlag. It is ignored when mixedIO is false.
     * @param dstFd         the destination file description, used when mixedIO is true
     * @param mixedIOFlag   the flag to pick the method of writing data, true means that file io will be used, otherwise mmap copy.
     */
    void o3copyAuxVector(
            FilesFacade ff,
            long srcAddr,
            long srcLo,
            long srcHi,
            long dstAddr,
            long dstFileOffset,
            int dstFd,
            boolean mixedIOFlag
    );

    /**
     * Sorts var size vectors. This method is also responsible for sizing the destination vectors and ensuring the
     * append position after sorting is correct.
     *
     * @param sortedTimestampsAddr     array of 128-bit entries, second 64 bits are row numbers that drive ordering.
     * @param sortedTimestampsRowCount size of the timestamp index
     * @param srcDataMem               source data vector
     * @param srcAuxMem                source aux vector
     * @param dstDataMem               destination data vector
     * @param dstAuxMem                destination aux vector
     */
    void o3sort(
            long sortedTimestampsAddr,
            long sortedTimestampsRowCount,
            MemoryCR srcDataMem,
            MemoryCR srcAuxMem,
            MemoryCARW dstDataMem,
            MemoryCARW dstAuxMem
    );

    /**
     * For now this method is called by WAL writer when data is rolled back (or row is cancelled). The
     * expectation of the WAL writer is to have the append position set correctly on aux mem and size of data vector
     * provided correctly.
     */
    long setAppendAuxMemAppendPosition(MemoryMA auxMem, long rowCount);

    long setAppendPosition(long pos, MemoryMA auxMem, MemoryMA dataMem);

    void setColumnRefs(long address, long initialOffset, long count);

    void setDataVectorEntriesToNull(long dataMemAddr, long rowCount);

    void shiftCopyAuxVector(long shift, long src, long srcLo, long srcHi, long dstAddr, long dstAddrSize);

    /**
     * Appends null encoding to the memory.
     *
     * @param dataMem the data memory
     * @param auxMem  the aux memory (fixed part)
     */
    void appendNull(MemoryA dataMem, MemoryA auxMem);
}
