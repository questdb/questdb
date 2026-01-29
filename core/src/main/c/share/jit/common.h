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

#ifndef QUESTDB_JIT_COMMON_H
#define QUESTDB_JIT_COMMON_H

#include <asmjit/core.h>
#include <asmjit/x86.h>
#include <asmjit/support/arenavector.h>

enum class data_type_t : uint8_t {
    i8,
    i16,
    i32,
    f32,
    i64,
    f64,
    i128,
    string_header,
    binary_header,
    varchar_header
};

enum class data_kind_t : uint8_t {
    kMemory,
    kConst,
    kFlagsEq,  // CMP emitted for equality; use JNE to skip if not equal
    kFlagsNe,  // CMP emitted for inequality; use JE to skip if equal
};

enum class opcodes : int32_t {
    Inv = -1,
    Ret = 0,
    Imm = 1,
    Mem = 2,
    Var = 3,
    Neg = 4,
    Not = 5,
    And = 6,
    Or = 7,
    Eq = 8,
    Ne = 9,
    Lt = 10,
    Le = 11,
    Gt = 12,
    Ge = 13,
    Add = 14,
    Sub = 15,
    Mul = 16,
    Div = 17,
    And_Sc = 18, // Short-circuit AND: if false, jump to label[payload.lo] (0 = next_row)
    Or_Sc = 19,  // Short-circuit OR: if true, jump to label[payload.lo] (0 = next_row)
    Begin_Sc = 20, // Create label at index payload.lo
    End_Sc = 21,   // Bind label at index payload.lo
};

struct instruction_t {
    opcodes opcode;
    int32_t options;
    union {
        struct {
            int64_t lo;
            int64_t hi;
        } ipayload;
        double dpayload;
    };
};

struct jit_value_t {

    inline jit_value_t() noexcept
            : op_(), type_(), kind_() {}

    inline jit_value_t(asmjit::Operand op, data_type_t type, data_kind_t kind) noexcept
            : op_(op), type_(type), kind_(kind) {}

    inline jit_value_t(const jit_value_t &other) noexcept = default;

    inline jit_value_t &operator=(const jit_value_t &other) noexcept = default;

    inline const asmjit::x86::Vec &vec() const noexcept { return op_.as<asmjit::x86::Vec>(); }

    inline const asmjit::x86::Gp &gp() const noexcept { return op_.as<asmjit::x86::Gp>(); }

    inline data_type_t dtype() const noexcept { return type_; }

    inline data_kind_t dkind() const noexcept { return kind_; }

    inline const asmjit::Operand &op() const noexcept { return op_; }

private:
    asmjit::Operand op_;
    data_type_t type_;
    data_kind_t kind_;
};

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
        case data_type_t::i128:
            return 4;
        default:
            __builtin_unreachable();
    }
}

inline data_kind_t dst_kind(const jit_value_t &lhs, const jit_value_t &rhs) {
    auto dk = (lhs.dkind() == data_kind_t::kConst && rhs.dkind() == data_kind_t::kConst) ? data_kind_t::kConst
                                                                                         : data_kind_t::kMemory;
    return dk;
}

// Cache for pre-loaded column addresses to avoid redundant loads inside the loop
struct ColumnAddressCache {
    static constexpr size_t MAX_COLUMNS = 8;

    ColumnAddressCache() {
        for (size_t i = 0; i < MAX_COLUMNS; ++i) {
            valid[i] = false;
        }
    }

    bool has(int32_t column_idx) const {
        return column_idx >= 0 && static_cast<size_t>(column_idx) < MAX_COLUMNS && valid[column_idx];
    }

    asmjit::x86::Gp get(int32_t column_idx) const {
        return addresses[column_idx];
    }

    void set(int32_t column_idx, asmjit::x86::Gp reg) {
        if (column_idx >= 0 && static_cast<size_t>(column_idx) < MAX_COLUMNS) {
            addresses[column_idx] = reg;
            valid[column_idx] = true;
        }
    }

private:
    asmjit::x86::Gp addresses[MAX_COLUMNS];
    bool valid[MAX_COLUMNS];
};

// Cache for pre-loaded constants to avoid redundant loads inside the loop
struct ConstantCache {
    static constexpr size_t MAX_CONSTANTS = 8;

    ConstantCache() : count(0) {}

    // Find an integer constant and return its register
    bool findInt(int64_t value, asmjit::x86::Gp &out_reg) const {
        for (size_t i = 0; i < count; ++i) {
            if (!is_float[i] && int_values[i] == value) {
                out_reg = gp_regs[i];
                return true;
            }
        }
        return false;
    }

    // Find a float constant and return its register
    bool findFloat(double value, asmjit::x86::Vec &out_reg) const {
        for (size_t i = 0; i < count; ++i) {
            if (is_float[i] && float_values[i] == value) {
                out_reg = xmm_regs[i];
                return true;
            }
        }
        return false;
    }

    // Add an integer constant
    void addInt(int64_t value, asmjit::x86::Gp reg) {
        if (count < MAX_CONSTANTS) {
            is_float[count] = false;
            int_values[count] = value;
            gp_regs[count] = reg;
            count++;
        }
    }

    // Add a float constant
    void addFloat(double value, asmjit::x86::Vec reg) {
        if (count < MAX_CONSTANTS) {
            is_float[count] = true;
            float_values[count] = value;
            xmm_regs[count] = reg;
            count++;
        }
    }

private:
    size_t count;
    bool is_float[MAX_CONSTANTS];
    int64_t int_values[MAX_CONSTANTS];
    double float_values[MAX_CONSTANTS];
    asmjit::x86::Gp gp_regs[MAX_CONSTANTS];
    asmjit::x86::Vec xmm_regs[MAX_CONSTANTS];
};

// Cache for loaded column values to avoid redundant loads within a single row iteration
struct ColumnValueCache {
    static constexpr size_t MAX_VALUES = 8;

    ColumnValueCache() : count(0) {}

    // Find a cached value for a column
    bool find(int32_t column_idx, data_type_t type, asmjit::x86::Gp &out_reg) const {
        for (size_t i = 0; i < count; ++i) {
            if (column_idxs[i] == column_idx && types[i] == type && !is_xmm[i]) {
                out_reg = gp_regs[i];
                return true;
            }
        }
        return false;
    }

    // Find a cached float/double value for a column
    bool findXmm(int32_t column_idx, data_type_t type, asmjit::x86::Vec &out_reg) const {
        for (size_t i = 0; i < count; ++i) {
            if (column_idxs[i] == column_idx && types[i] == type && is_xmm[i]) {
                out_reg = xmm_regs[i];
                return true;
            }
        }
        return false;
    }

    // Add an integer column value
    void add(int32_t column_idx, data_type_t type, asmjit::x86::Gp reg) {
        if (count < MAX_VALUES) {
            column_idxs[count] = column_idx;
            types[count] = type;
            is_xmm[count] = false;
            gp_regs[count] = reg;
            count++;
        }
    }

    // Add a float/double column value
    void addXmm(int32_t column_idx, data_type_t type, asmjit::x86::Vec reg) {
        if (count < MAX_VALUES) {
            column_idxs[count] = column_idx;
            types[count] = type;
            is_xmm[count] = true;
            xmm_regs[count] = reg;
            count++;
        }
    }

    // Clear cache (call at start of each row iteration if needed)
    void clear() {
        count = 0;
    }

private:
    size_t count;
    int32_t column_idxs[MAX_VALUES];
    data_type_t types[MAX_VALUES];
    bool is_xmm[MAX_VALUES];
    asmjit::x86::Gp gp_regs[MAX_VALUES];
    asmjit::x86::Vec xmm_regs[MAX_VALUES];
};

// Cache for pre-broadcasted constants in YMM registers for AVX2 SIMD loops
struct ConstantCacheYmm {
    static constexpr size_t MAX_CONSTANTS = 8;

    ConstantCacheYmm() : count(0), has_varchar_null_perm_ctrl(false) {}

    // Get the varchar header permutation control vector (for extracting 4-byte headers from 16-byte aux entries)
    bool hasVarcharNullPermCtrl() const { return has_varchar_null_perm_ctrl; }
    asmjit::x86::Vec getVarcharNullPermCtrl() const { return varchar_null_perm_ctrl; }
    void setVarcharNullPermCtrl(asmjit::x86::Vec reg) {
        varchar_null_perm_ctrl = reg;
        has_varchar_null_perm_ctrl = true;
    }

    // Find an integer constant and return its YMM register
    bool findInt(int64_t value, asmjit::x86::Vec &out_reg) const {
        for (size_t i = 0; i < count; ++i) {
            if (!is_float[i] && int_values[i] == value) {
                out_reg = ymm_regs[i];
                return true;
            }
        }
        return false;
    }

    // Find a float constant and return its YMM register
    bool findFloat(double value, asmjit::x86::Vec &out_reg) const {
        for (size_t i = 0; i < count; ++i) {
            if (is_float[i] && float_values[i] == value) {
                out_reg = ymm_regs[i];
                return true;
            }
        }
        return false;
    }

    // Add an integer constant
    void addInt(int64_t value, asmjit::x86::Vec reg) {
        if (count < MAX_CONSTANTS) {
            is_float[count] = false;
            int_values[count] = value;
            ymm_regs[count] = reg;
            count++;
        }
    }

    // Add a float constant
    void addFloat(double value, asmjit::x86::Vec reg) {
        if (count < MAX_CONSTANTS) {
            is_float[count] = true;
            float_values[count] = value;
            ymm_regs[count] = reg;
            count++;
        }
    }

private:
    size_t count;
    bool is_float[MAX_CONSTANTS];
    int64_t int_values[MAX_CONSTANTS];
    double float_values[MAX_CONSTANTS];
    asmjit::x86::Vec ymm_regs[MAX_CONSTANTS];
    // Varchar NULL check permutation control: [0, 4, 0, 0, 0, 0, 0, 0] for extracting
    // the first dword from each 16-byte aux entry
    bool has_varchar_null_perm_ctrl;
    asmjit::x86::Vec varchar_null_perm_ctrl;
};

#endif //QUESTDB_JIT_COMMON_H
