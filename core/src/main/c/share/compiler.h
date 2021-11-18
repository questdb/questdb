/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
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

#ifndef QUESTDB_COMPILER_H
#define QUESTDB_COMPILER_H

#include <jni.h>
#include <asmjit/asmjit.h>
#include <limits>

enum data_type_t : uint8_t {
    i8,
    i16,
    i32,
    i64,
    f32,
    f64,
};

enum data_kind_t : uint8_t {
    kMemory,
    kConst,
};

enum instruction_t : uint8_t {
    MEM_I1 = 0,
    MEM_I2 = 1,
    MEM_I4 = 2,
    MEM_I8 = 3,
    MEM_F4 = 4,
    MEM_F8 = 5,

    IMM_I1 = 6,
    IMM_I2 = 7,
    IMM_I4 = 8,
    IMM_I8 = 9,
    IMM_F4 = 10,
    IMM_F8 = 11,

    NEG = 12,               // -a
    NOT = 13,               // !a
    AND = 14,               // a && b
    OR = 15,                // a || b
    EQ = 16,                // a == b
    NE = 17,                // a != b
    LT = 18,                // a <  b
    LE = 19,                // a <= b
    GT = 20,                // a >  b
    GE = 21,                // a >= b
    ADD = 22,               // a + b
    SUB = 23,               // a - b
    MUL = 24,               // a * b
    DIV = 25,               // a / b
    MOD = 26,               // a % b
    JZ = 27,                // if a == 0 jp b
    JNZ = 28,               // if a != 0 jp b
    JP = 29,                // jp a
    RET = 30,               // ret a
    IMM_NULL = 31,          // generic null const
};

static const int64_t long_null = std::numeric_limits<int64_t>::min();
static const int32_t int_null = std::numeric_limits<int32_t>::min();

template<typename T>
T read_at(const uint8_t *buf, size_t size, uint32_t pos) {
    if (pos + sizeof(T) <= size)
        return *((T *) &buf[pos]);
    return 0;
}

template<typename T>
T read(const uint8_t *buf, size_t size, uint32_t &pos) {
    T data = read_at<T>(buf, size, pos);
    pos += sizeof(T);
    return data;
}

struct jit_value_t {

    inline jit_value_t() noexcept
            : op_(), type_(), kind_() {}

    inline jit_value_t(asmjit::Operand op) noexcept
            : op_(op), type_(), kind_() {}

    inline jit_value_t(asmjit::Operand op, data_type_t type) noexcept
            : op_(op), type_(type), kind_() {}

    inline jit_value_t(asmjit::Operand op, data_type_t type, data_kind_t kind) noexcept
            : op_(op), type_(type), kind_(kind) {}

    inline jit_value_t(const jit_value_t &other) noexcept = default;

    inline jit_value_t &operator=(const jit_value_t &other) noexcept = default;

    inline void reset() noexcept {
        op_.reset();
    }

    inline void swap(jit_value_t &other) noexcept {
        jit_value_t t(*this);
        *this = other;
        other = t;
    }

    inline bool isNone() const noexcept { return op_.isNone(); }

    inline bool isMem() const noexcept { return op_.isMem(); }

    inline bool isZmm() const noexcept { return op_.isReg(asmjit::x86::Reg::kTypeZmm); }

    inline bool isYmm() const noexcept { return op_.isReg(asmjit::x86::Reg::kTypeYmm); }

    inline bool isXmm() const noexcept { return op_.isReg(asmjit::x86::Reg::kTypeXmm); }

    inline bool isGpq() const noexcept { return op_.isReg(asmjit::x86::Reg::kTypeGpq); }

    inline const asmjit::Operand &op() const noexcept { return op_; }

    inline const asmjit::x86::Mem &mem() const noexcept { return op_.as<asmjit::x86::Mem>(); }

    inline const asmjit::x86::Zmm &zmm() const noexcept { return op_.as<asmjit::x86::Zmm>(); }

    inline const asmjit::x86::Ymm &ymm() const noexcept { return op_.as<asmjit::x86::Ymm>(); }

    inline const asmjit::x86::Xmm &xmm() const noexcept { return op_.as<asmjit::x86::Xmm>(); }

    inline const asmjit::x86::Gpq &gp() const noexcept { return op_.as<asmjit::x86::Gpq>(); }

    inline data_type_t dtype() const noexcept { return type_; }

    inline data_kind_t dkind() const noexcept { return kind_; }

private:
    asmjit::Operand op_;
    data_type_t type_;
    data_kind_t kind_;
};

extern "C" {
    JNIEXPORT long JNICALL Java_io_questdb_jit_FiltersCompiler_compile(JNIEnv *e,
                                                                       jclass cl,
                                                                       jlong colsAddress,
                                                                       jlong colsSize,
                                                                       jlong filterAddress,
                                                                       jlong filterSize,
                                                                       jlong rowsAddress,
                                                                       jlong rowsSize,
                                                                       jlong rowsStartOffset);

    JNIEXPORT long JNICALL Java_io_questdb_jit_FiltersCompiler_compileFunction(JNIEnv *e,
                                                                               jclass cl,
                                                                               jlong filterAddres,
                                                                               jlong filterSize,
                                                                               jint options);

    JNIEXPORT void JNICALL Java_io_questdb_jit_FiltersCompiler_freeFunction(JNIEnv *e, jclass cl, jlong fnAddress);

    JNIEXPORT long JNICALL Java_io_questdb_jit_FiltersCompiler_callFunction(JNIEnv *e,
                                                                            jclass cl,
                                                                            jlong fnAddress,
                                                                            jlong colsAddress,
                                                                            jlong colsSize,
                                                                            jlong rowsAddress,
                                                                            jlong rowsSize,
                                                                            jlong rowsStartOffset);
}

#endif //QUESTDB_COMPILER_H
