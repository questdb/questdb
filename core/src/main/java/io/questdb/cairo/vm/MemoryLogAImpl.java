/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2020 QuestDB
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

package io.questdb.cairo.vm;

import io.questdb.cairo.vm.api.MemoryA;
import io.questdb.std.BinarySequence;
import io.questdb.std.Long256;
import io.questdb.std.Misc;
import org.jetbrains.annotations.NotNull;

public class MemoryLogAImpl implements MemoryA {
    private MemoryA log;
    private MemoryA main;

    @Override
    public void close() {
        log = Misc.free(log);
        assert main != this;
        main = Misc.free(main);
    }

    @Override
    public long getAppendOffset() {
        return main.getAppendOffset();
    }

    @Override
    public long getExtendSegmentSize() {
        return main.getExtendSegmentSize();
    }

    @Override
    public void jumpTo(long offset) {
        main.jumpTo(offset);
    }

    @Override
    public long putBin(BinarySequence value) {
        log.putBin(value);
        return main.putBin(value);
    }

    @Override
    public long putBin(long from, long len) {
        log.putBin(from, len);
        return main.putBin(from, len);
    }

    @Override
    public void putBlockOfBytes(long from, long len) {
        log.putBlockOfBytes(from, len);
        main.putBlockOfBytes(from, len);
    }

    @Override
    public void putBool(boolean value) {
        log.putBool(value);
        main.putBool(value);
    }

    @Override
    public void putByte(byte b) {
        log.putByte(b);
        main.putByte(b);
    }

    @Override
    public void putChar(char value) {
        log.putChar(value);
        main.putChar(value);
    }

    @Override
    public void putDouble(double value) {
        log.putDouble(value);
        main.putDouble(value);
    }

    @Override
    public void putFloat(float value) {
        log.putFloat(value);
        main.putFloat(value);
    }

    @Override
    public void putInt(int value) {
        log.putInt(value);
        main.putInt(value);
    }

    @Override
    public void putLong(long value) {
        log.putLong(value);
        main.putLong(value);
    }

    @Override
    public void putLong128(long l1, long l2) {
        log.putLong128(l1, l2);
        main.putLong128(l1, l2);
    }

    @Override
    public void putLong256(long l0, long l1, long l2, long l3) {
        log.putLong256(l0, l1, l2, l3);
        main.putLong256(l0, l1, l2, l3);
    }

    @Override
    public void putLong256(Long256 value) {
        log.putLong256(value);
        main.putLong256(value);
    }

    @Override
    public void putLong256(CharSequence hexString) {
        log.putLong256(hexString);
        main.putLong256(hexString);
    }

    @Override
    public void putLong256(@NotNull CharSequence hexString, int start, int end) {
        log.putLong256(hexString, start, end);
        main.putLong256(hexString, start, end);
    }

    @Override
    public long putNullBin() {
        log.putNullBin();
        return main.putNullBin();
    }

    @Override
    public long putNullStr() {
        log.putNullStr();
        return main.putNullStr();
    }

    @Override
    public void putShort(short value) {
        log.putShort(value);
        main.putShort(value);
    }

    @Override
    public long putStr(CharSequence value) {
        log.putStr(value);
        return main.putStr(value);
    }

    @Override
    public long putStr(char value) {
        log.putStr(value);
        return main.putStr(value);
    }

    @Override
    public long putStr(CharSequence value, int pos, int len) {
        log.putStr(value, pos, len);
        return main.putStr(value, pos, len);
    }

    @Override
    public void skip(long bytes) {
        log.skip(bytes);
        main.skip(bytes);
    }

    @Override
    public void truncate() {
        main.truncate();
    }

    public void of(MemoryA log, MemoryA main) {
        this.log = log;
        this.main = main;
    }
}
