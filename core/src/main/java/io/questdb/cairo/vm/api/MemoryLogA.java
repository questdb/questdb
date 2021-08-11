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

package io.questdb.cairo.vm.api;

import io.questdb.std.BinarySequence;
import io.questdb.std.Long256;
import org.jetbrains.annotations.NotNull;

public interface MemoryLogA<T extends MemoryA> extends MemoryA {
    default long getAppendOffset() {
        return getMainMemory().getAppendOffset();
    }

    default long getExtendSegmentSize() {
        return getMainMemory().getExtendSegmentSize();
    }

    default void jumpTo(long offset) {
        getMainMemory().jumpTo(offset);
    }

    default long putBin(BinarySequence value) {
        getLogMemory().putBin(value);
        return getMainMemory().putBin(value);
    }

    default long putBin(long from, long len) {
        getLogMemory().putBin(from, len);
        return getMainMemory().putBin(from, len);
    }

    default void putBlockOfBytes(long from, long len) {
        getLogMemory().putBlockOfBytes(from, len);
        getMainMemory().putBlockOfBytes(from, len);
    }

    default void putBool(boolean value) {
        getLogMemory().putBool(value);
        getMainMemory().putBool(value);
    }

    default void putByte(byte b) {
        getLogMemory().putByte(b);
        getMainMemory().putByte(b);
    }

    default void putChar(char value) {
        getLogMemory().putChar(value);
        getMainMemory().putChar(value);
    }

    default void putDouble(double value) {
        getLogMemory().putDouble(value);
        getMainMemory().putDouble(value);
    }

    default void putFloat(float value) {
        getLogMemory().putFloat(value);
        getMainMemory().putFloat(value);
    }

    default void putInt(int value) {
        getLogMemory().putInt(value);
        getMainMemory().putInt(value);
    }

    default void putLong(long value) {
        getLogMemory().putLong(value);
        getMainMemory().putLong(value);
    }

    default void putLong128(long l1, long l2) {
        getLogMemory().putLong128(l1, l2);
        getMainMemory().putLong128(l1, l2);
    }

    default void putLong256(long l0, long l1, long l2, long l3) {
        getLogMemory().putLong256(l0, l1, l2, l3);
        getMainMemory().putLong256(l0, l1, l2, l3);
    }

    default void putLong256(Long256 value) {
        getLogMemory().putLong256(value);
        getMainMemory().putLong256(value);
    }

    default void putLong256(CharSequence hexString) {
        getLogMemory().putLong256(hexString);
        getMainMemory().putLong256(hexString);
    }

    default void putLong256(@NotNull CharSequence hexString, int start, int end) {
        getLogMemory().putLong256(hexString, start, end);
        getMainMemory().putLong256(hexString, start, end);
    }

    default long putNullBin() {
        getLogMemory().putNullBin();
        return getMainMemory().putNullBin();
    }

    default long putNullStr() {
        getLogMemory().putNullStr();
        return getMainMemory().putNullStr();
    }

    default void putShort(short value) {
        getLogMemory().putShort(value);
        getMainMemory().putShort(value);
    }

    default long putStr(CharSequence value) {
        getLogMemory().putStr(value);
        return getMainMemory().putStr(value);
    }

    default long putStr(char value) {
        getLogMemory().putStr(value);
        return getMainMemory().putStr(value);
    }

    default long putStr(CharSequence value, int pos, int len) {
        getLogMemory().putStr(value, pos, len);
        return getMainMemory().putStr(value, pos, len);
    }

    default void skip(long bytes) {
        getLogMemory().skip(bytes);
        getMainMemory().skip(bytes);
    }

    default void truncate() {
        getMainMemory().truncate();
    }

    MemoryMA getLogMemory();

    T getMainMemory();

    void of(MemoryMA log, T main);
}
