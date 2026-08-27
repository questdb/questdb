/*+*****************************************************************************
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
#ifdef __aarch64__
#include <asmjit/a64.h>
#else
#include <asmjit/x86.h>
#endif
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
    Sx_I64 = 22,   // Sign-extend top of stack to i64
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

// Carries one value through a backend's value stack: the register or memory operand holding it,
// the width its instructions run at, where it came from, and how it spells a truth value.
//
// The last of those exists because the SIMD backend spells one two ways. questdb::avx2::cmp_eq
// emits vpcmpeqb / vpcmpeqd / ... , which write an all-ones lane for true and an all-zeros lane for
// false; a BOOLEAN column, constant or bind variable arrives instead as the raw byte QuestDB
// stores, 0 or 1. Neither dtype nor dkind can tell the two apart - mask_type() maps f32 and f64
// onto their integer widths and returns every other type unchanged, so an i8 mask and a raw i8
// boolean are both data_type_t::i8, and dkind answers where a value was READ from, not what it
// means. Comparing the two spellings for equality misses on every true row, which is silently
// wrong rows rather than a decline, so avx2.h asks this flag which spelling it holds and
// harmonises the pair before the comparison runs.
//
// The scalar backends leave it false throughout and never read it: x86::cmp_eq and
// aarch64::cmp_eq materialise 0 / 1 through SETcc and CSET, which is the same spelling a raw
// BOOLEAN byte already carries, and their int32_not is an XOR / EOR with 1 rather than a bitwise
// complement. Only the vectorized backend has two spellings to reconcile.
struct jit_value_t {

    inline jit_value_t() noexcept
            : op_(), type_(), kind_(), is_mask_(false) {}

    inline jit_value_t(asmjit::Operand op, data_type_t type, data_kind_t kind, bool is_mask = false) noexcept
            : op_(op), type_(type), kind_(kind), is_mask_(is_mask) {}

    inline jit_value_t(const jit_value_t &other) noexcept = default;

    inline jit_value_t &operator=(const jit_value_t &other) noexcept = default;

#ifdef __aarch64__
    inline const asmjit::a64::Vec &vec() const noexcept { return op_.as<asmjit::a64::Vec>(); }
    inline const asmjit::a64::Gp &gp() const noexcept { return op_.as<asmjit::a64::Gp>(); }
#else
    inline const asmjit::x86::Vec &vec() const noexcept { return op_.as<asmjit::x86::Vec>(); }
    inline const asmjit::x86::Gp &gp() const noexcept { return op_.as<asmjit::x86::Gp>(); }
#endif

    inline data_type_t dtype() const noexcept { return type_; }

    inline data_kind_t dkind() const noexcept { return kind_; }

    // Reports whether this value spells true as an all-ones lane rather than as the byte 1.
    inline bool is_mask() const noexcept { return is_mask_; }

    inline const asmjit::Operand &op() const noexcept { return op_; }

private:
    asmjit::Operand op_;
    data_type_t type_;
    data_kind_t kind_;
    bool is_mask_;
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

#ifdef __aarch64__
    asmjit::a64::Gp get(int32_t column_idx) const {
        return addresses[column_idx];
    }

    void set(int32_t column_idx, asmjit::a64::Gp reg) {
        if (column_idx >= 0 && static_cast<size_t>(column_idx) < MAX_COLUMNS) {
            addresses[column_idx] = reg;
            valid[column_idx] = true;
        }
    }

private:
    asmjit::a64::Gp addresses[MAX_COLUMNS];
    bool valid[MAX_COLUMNS];
#else
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
#endif
};

// Cache for pre-loaded constants to avoid redundant loads inside the loop
struct ConstantCache {
    static constexpr size_t MAX_CONSTANTS = 8;

    ConstantCache() : count(0) {}

#ifdef __aarch64__
    bool findInt(int64_t value, asmjit::a64::Gp &out_reg) const {
        for (size_t i = 0; i < count; ++i) {
            if (!is_float[i] && int_values[i] == value) {
                out_reg = gp_regs[i];
                return true;
            }
        }
        return false;
    }

    bool findFloat(double value, asmjit::a64::Vec &out_reg) const {
        for (size_t i = 0; i < count; ++i) {
            if (is_float[i] && float_values[i] == value) {
                out_reg = vec_regs[i];
                return true;
            }
        }
        return false;
    }

    void addInt(int64_t value, asmjit::a64::Gp reg) {
        if (count < MAX_CONSTANTS) {
            is_float[count] = false;
            int_values[count] = value;
            gp_regs[count] = reg;
            count++;
        }
    }

    void addFloat(double value, asmjit::a64::Vec reg) {
        if (count < MAX_CONSTANTS) {
            is_float[count] = true;
            float_values[count] = value;
            vec_regs[count] = reg;
            count++;
        }
    }

private:
    size_t count;
    bool is_float[MAX_CONSTANTS];
    int64_t int_values[MAX_CONSTANTS];
    double float_values[MAX_CONSTANTS];
    asmjit::a64::Gp gp_regs[MAX_CONSTANTS];
    asmjit::a64::Vec vec_regs[MAX_CONSTANTS];
#else
    bool findInt(int64_t value, asmjit::x86::Gp &out_reg) const {
        for (size_t i = 0; i < count; ++i) {
            if (!is_float[i] && int_values[i] == value) {
                out_reg = gp_regs[i];
                return true;
            }
        }
        return false;
    }

    bool findFloat(double value, data_type_t type, asmjit::x86::Vec &out_reg) const {
        for (size_t i = 0; i < count; ++i) {
            if (is_float[i] && float_values[i] == value && float_types[i] == type) {
                out_reg = xmm_regs[i];
                return true;
            }
        }
        return false;
    }

    void addInt(int64_t value, asmjit::x86::Gp reg) {
        if (count < MAX_CONSTANTS) {
            is_float[count] = false;
            int_values[count] = value;
            gp_regs[count] = reg;
            count++;
        }
    }

    void addFloat(double value, data_type_t type, asmjit::x86::Vec reg) {
        if (count < MAX_CONSTANTS) {
            is_float[count] = true;
            float_values[count] = value;
            float_types[count] = type;
            xmm_regs[count] = reg;
            count++;
        }
    }

private:
    size_t count;
    bool is_float[MAX_CONSTANTS];
    int64_t int_values[MAX_CONSTANTS];
    double float_values[MAX_CONSTANTS];
    data_type_t float_types[MAX_CONSTANTS];
    asmjit::x86::Gp gp_regs[MAX_CONSTANTS];
    asmjit::x86::Vec xmm_regs[MAX_CONSTANTS];
#endif
};

// Cache for loaded column values to avoid redundant loads within a single row iteration
struct ColumnValueCache {
    static constexpr size_t MAX_VALUES = 8;

    ColumnValueCache() : count(0) {}

#ifdef __aarch64__
    bool find(int32_t column_idx, data_type_t type, asmjit::a64::Gp &out_reg) const {
        for (size_t i = 0; i < count; ++i) {
            if (column_idxs[i] == column_idx && types[i] == type && !is_fp[i]) {
                out_reg = gp_regs[i];
                return true;
            }
        }
        return false;
    }

    bool findXmm(int32_t column_idx, data_type_t type, asmjit::a64::Vec &out_reg) const {
        for (size_t i = 0; i < count; ++i) {
            if (column_idxs[i] == column_idx && types[i] == type && is_fp[i]) {
                out_reg = vec_regs[i];
                return true;
            }
        }
        return false;
    }

    void add(int32_t column_idx, data_type_t type, asmjit::a64::Gp reg) {
        if (count < MAX_VALUES) {
            column_idxs[count] = column_idx;
            types[count] = type;
            is_fp[count] = false;
            gp_regs[count] = reg;
            count++;
        }
    }

    void addXmm(int32_t column_idx, data_type_t type, asmjit::a64::Vec reg) {
        if (count < MAX_VALUES) {
            column_idxs[count] = column_idx;
            types[count] = type;
            is_fp[count] = true;
            vec_regs[count] = reg;
            count++;
        }
    }

    void clear() {
        count = 0;
    }

    // Snapshots the current entry count so the cache can be rolled back via
    // truncate(). Used to drop entries added inside a BEGIN_SC/END_SC block,
    // where an OR_SC forward jump may skip the loads that populated them.
    size_t size() const {
        return count;
    }

    // Drops all entries past the given count. Pair with size() to restore the
    // cache to a snapshot taken earlier in the IR stream.
    void truncate(size_t new_count) {
        if (new_count < count) {
            count = new_count;
        }
    }

private:
    size_t count;
    int32_t column_idxs[MAX_VALUES];
    data_type_t types[MAX_VALUES];
    bool is_fp[MAX_VALUES];
    asmjit::a64::Gp gp_regs[MAX_VALUES];
    asmjit::a64::Vec vec_regs[MAX_VALUES];
#else
    bool find(int32_t column_idx, data_type_t type, asmjit::x86::Gp &out_reg) const {
        for (size_t i = 0; i < count; ++i) {
            if (column_idxs[i] == column_idx && types[i] == type && !is_xmm[i]) {
                out_reg = gp_regs[i];
                return true;
            }
        }
        return false;
    }

    bool findXmm(int32_t column_idx, data_type_t type, asmjit::x86::Vec &out_reg) const {
        for (size_t i = 0; i < count; ++i) {
            if (column_idxs[i] == column_idx && types[i] == type && is_xmm[i]) {
                out_reg = xmm_regs[i];
                return true;
            }
        }
        return false;
    }

    void add(int32_t column_idx, data_type_t type, asmjit::x86::Gp reg) {
        if (count < MAX_VALUES) {
            column_idxs[count] = column_idx;
            types[count] = type;
            is_xmm[count] = false;
            gp_regs[count] = reg;
            count++;
        }
    }

    void addXmm(int32_t column_idx, data_type_t type, asmjit::x86::Vec reg) {
        if (count < MAX_VALUES) {
            column_idxs[count] = column_idx;
            types[count] = type;
            is_xmm[count] = true;
            xmm_regs[count] = reg;
            count++;
        }
    }

    void clear() {
        count = 0;
    }

    // Snapshots the current entry count so the cache can be rolled back via
    // truncate(). Used to drop entries added inside a BEGIN_SC/END_SC block,
    // where an OR_SC forward jump may skip the loads that populated them.
    size_t size() const {
        return count;
    }

    // Drops all entries past the given count. Pair with size() to restore the
    // cache to a snapshot taken earlier in the IR stream.
    void truncate(size_t new_count) {
        if (new_count < count) {
            count = new_count;
        }
    }

private:
    size_t count;
    int32_t column_idxs[MAX_VALUES];
    data_type_t types[MAX_VALUES];
    bool is_xmm[MAX_VALUES];
    asmjit::x86::Gp gp_regs[MAX_VALUES];
    asmjit::x86::Vec xmm_regs[MAX_VALUES];
#endif
};

#ifndef __aarch64__
// Cache for pre-broadcasted constants in YMM registers for AVX2 SIMD loops
struct ConstantCacheYmm {
    static constexpr size_t MAX_CONSTANTS = 8;

    ConstantCacheYmm() : count(0) {}

    // Find an integer constant and return its YMM register
    bool findInt(int64_t value, data_type_t type, asmjit::x86::Vec &out_reg) const {
        for (size_t i = 0; i < count; ++i) {
            if (!is_float[i] && int_values[i] == value && int_types[i] == type) {
                out_reg = ymm_regs[i];
                return true;
            }
        }
        return false;
    }

    // Find a float constant and return its YMM register
    bool findFloat(double value, data_type_t type, asmjit::x86::Vec &out_reg) const {
        for (size_t i = 0; i < count; ++i) {
            if (is_float[i] && float_values[i] == value && float_types[i] == type) {
                out_reg = ymm_regs[i];
                return true;
            }
        }
        return false;
    }

    // Add an integer constant
    void addInt(int64_t value, data_type_t type, asmjit::x86::Vec reg) {
        if (count < MAX_CONSTANTS) {
            is_float[count] = false;
            int_values[count] = value;
            int_types[count] = type;
            ymm_regs[count] = reg;
            count++;
        }
    }

    // Add a float constant
    void addFloat(double value, data_type_t type, asmjit::x86::Vec reg) {
        if (count < MAX_CONSTANTS) {
            is_float[count] = true;
            float_values[count] = value;
            float_types[count] = type;
            ymm_regs[count] = reg;
            count++;
        }
    }

private:
    size_t count;
    bool is_float[MAX_CONSTANTS];
    int64_t int_values[MAX_CONSTANTS];
    data_type_t int_types[MAX_CONSTANTS];
    double float_values[MAX_CONSTANTS];
    data_type_t float_types[MAX_CONSTANTS];
    asmjit::x86::Vec ymm_regs[MAX_CONSTANTS];
};

// Cache for the values one vectorized loop body loads: a column vector read at the current
// input_index, and a bind variable broadcast out of the vars block. questdb::avx2::read_mem and
// read_vars_mem consult it, so a predicate that reads the same operand more than once emits one
// load and reuses the register. CompiledFilterIRSerializer's CHAR and IPv4 ordering expansions are
// what make that common - they re-traverse each operand four and five times respectively, to build
// the unsigned ordering out of signed comparisons - and the scalar backends have carried the same
// cache for their row loop all along. Only the AVX2 backend went without one.
//
// Scoped to ONE body, not to the whole loop: avx2_loop advances input_index between unrolled
// bodies, so each body reads different rows. compiler.cpp clears the cache before every emit_code
// call, exactly as the scalar loops clear ColumnValueCache before every row.
//
// Handing one virtual register to two consumers is safe only while no consumer writes into an
// operand register. The VEX encoding is three-operand and every helper in impl/avx2.h allocates its
// own destination; the two that used to fold a result into lhs - cmp_eq's i128 arm and mul's i8 arm
// - were rewritten to do the same. Anything added there has to keep that property. The scalar
// backend's int32_and is the cautionary tale: an in-place AND overwrote a column value that the
// rest of the predicate still had to read, and "(aboolean and aboolean2) = aboolean" matched every
// row.
struct ValueCacheYmm {
    static constexpr size_t MAX_VALUES = 8;

    ValueCacheYmm() : count(0) {}

    void add(int32_t idx, data_type_t type, bool is_var, asmjit::x86::Vec reg) {
        if (count < MAX_VALUES) {
            idxs[count] = idx;
            types[count] = type;
            is_vars[count] = is_var;
            ymm_regs[count] = reg;
            count++;
        }
    }

    void clear() {
        count = 0;
    }

    // A column index and a bind variable index share a numbering, so is_var keeps the two apart.
    bool find(int32_t idx, data_type_t type, bool is_var, asmjit::x86::Vec &out_reg) const {
        for (size_t i = 0; i < count; ++i) {
            if (idxs[i] == idx && types[i] == type && is_vars[i] == is_var) {
                out_reg = ymm_regs[i];
                return true;
            }
        }
        return false;
    }

private:
    size_t count;
    int32_t idxs[MAX_VALUES];
    data_type_t types[MAX_VALUES];
    bool is_vars[MAX_VALUES];
    asmjit::x86::Vec ymm_regs[MAX_VALUES];
};
#endif // !__aarch64__

#endif //QUESTDB_JIT_COMMON_H
