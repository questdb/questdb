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

#ifndef QUESTDB_JIT_COMMON_H
#define QUESTDB_JIT_COMMON_H

#include <asmjit/asmjit.h>
#include <cstdint>
#include <stack>
#include <iostream>
#include <cassert>
#include <utility>
#include <variant>

enum class data_type_t : uint8_t {
    i8,
    i16,
    i32,
    i64,
    f32,
    f64,
};

enum class data_kind_t : uint8_t {
    kMemory,
    kConst,
};

enum class instruction_t : uint8_t {
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

    //todo: change serialisation format IMM/MEM/VAR + type info
    VAR_I1 = 32,
    VAR_I2 = 33,
    VAR_I4 = 34,
    VAR_I8 = 35,
    VAR_F4 = 36,
    VAR_F8 = 37,
};

struct jit_value_t {

    inline jit_value_t() noexcept
            : op_(), type_(), kind_() {}

    inline jit_value_t(asmjit::Operand op, data_type_t type, data_kind_t kind) noexcept
            : op_(op), type_(type), kind_(kind) {}

    inline jit_value_t(const jit_value_t &other) noexcept = default;

    inline jit_value_t &operator=(const jit_value_t &other) noexcept = default;

    inline const asmjit::x86::Ymm &ymm() const noexcept { return op_.as<asmjit::x86::Ymm>(); }

    inline const asmjit::x86::Xmm &xmm() const noexcept { return op_.as<asmjit::x86::Xmm>(); }

    inline const asmjit::x86::Gpq &gp() const noexcept { return op_.as<asmjit::x86::Gpq>(); }

    inline data_type_t dtype() const noexcept { return type_; }

    inline data_kind_t dkind() const noexcept { return kind_; }

    inline const asmjit::Operand& op() const noexcept { return op_; }

private:
    asmjit::Operand op_;
    data_type_t type_;
    data_kind_t kind_;
};

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

inline uint32_t type_shift(data_type_t type) {
    switch (type) {
        case data_type_t::i8:
            return 0;
        case data_type_t::i16:
            return 1;
        case data_type_t::i32:
        case data_type_t::f32:
            return 2;
        case data_type_t::i64:
        case data_type_t::f64:
            return 3;
        default:
            __builtin_unreachable();
    }
}

inline data_kind_t dst_kind(const jit_value_t &lhs, const jit_value_t &rhs) {
    auto dk = (lhs.dkind() == data_kind_t::kConst && rhs.dkind() == data_kind_t::kConst) ? data_kind_t::kConst
            : data_kind_t::kMemory;
    return dk;
}

#endif //QUESTDB_JIT_COMMON_H
