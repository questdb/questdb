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

#ifndef QUESTDB_JIT_X86_H
#define QUESTDB_JIT_X86_H

#include "common.h"
#include "impl/x86.h"

namespace questdb::x86 {
    using namespace asmjit::x86;

    // Pre-scan instruction stream and load column addresses before the loop.
    // This hoists column address loads out of the hot loop.
    void preload_column_addresses(Compiler &c,
                                   const instruction_t *istream,
                                   size_t size,
                                   const Gp &data_ptr,
                                   ColumnAddressCache &cache) {
        for (size_t i = 0; i < size; ++i) {
            auto &instr = istream[i];
            if (instr.opcode == opcodes::Mem) {
                auto type = static_cast<data_type_t>(instr.options);
                // Only cache fixed-size column addresses (not variable-size like string/binary/varchar)
                if (type != data_type_t::string_header &&
                    type != data_type_t::binary_header &&
                    type != data_type_t::varchar_header) {
                    auto column_idx = static_cast<int32_t>(instr.ipayload.lo);
                    if (!cache.has(column_idx)) {
                        Gp column_address = c.new_gp64("col_addr_%d", column_idx);
                        c.mov(column_address, ptr(data_ptr, 8 * column_idx, 8));
                        cache.set(column_idx, column_address);
                    }
                }
            }
        }
    }

    // Pre-scan instruction stream and load constants into registers before the loop.
    // This hoists constant loads out of the hot loop.
    void preload_constants(Compiler &c,
                           const instruction_t *istream,
                           size_t size,
                           ConstantCache &cache) {
        for (size_t i = 0; i < size; ++i) {
            auto &instr = istream[i];
            if (instr.opcode == opcodes::Imm) {
                auto type = static_cast<data_type_t>(instr.options);
                switch (type) {
                    case data_type_t::i8:
                    case data_type_t::i16:
                    case data_type_t::i32:
                    case data_type_t::i64: {
                        int64_t value = instr.ipayload.lo;
                        Gp dummy;
                        if (!cache.findInt(value, dummy)) {
                            Gp reg = c.new_gp64("const_%lld", value);
                            c.mov(reg, value);
                            cache.addInt(value, reg);
                        }
                        break;
                    }
                    case data_type_t::f32:
                    case data_type_t::f64: {
                        double value = instr.dpayload;
                        Vec dummy;
                        if (!cache.findFloat(value, type, dummy)) {
                            Vec reg;
                            if (type == data_type_t::f32) {
                                reg = c.new_xmm_ss("const_f_%f", value);
                                Mem mem = c.new_float_const(ConstPoolScope::kLocal, static_cast<float>(value));
                                c.movss(reg, mem);
                            } else {
                                reg = c.new_xmm_sd("const_d_%f", value);
                                Mem mem = c.new_double_const(ConstPoolScope::kLocal, value);
                                c.movsd(reg, mem);
                            }
                            cache.addFloat(value, type, reg);
                        }
                        break;
                    }
                    default:
                        // i128 constants are rare, skip caching
                        break;
                }
            }
        }
    }

    jit_value_t
    read_vars_mem(Compiler &c, data_type_t type, int32_t idx, const Gp &vars_ptr) {
        auto shift = type_shift(type);
        auto type_size = 1 << shift;
        return {Mem(vars_ptr, 8 * idx, type_size), type, data_kind_t::kMemory};
    }

    // Reads length of variable size column with header stored in data vector (string, binary).
    jit_value_t read_mem_varsize(Compiler &c,
                                 uint32_t header_size,
                                 int32_t column_idx,
                                 const Gp &data_ptr,
                                 const Gp &varsize_aux_ptr,
                                 const Gp &input_index) {
        // Column has variable-size data with header stored in data vector.
        // First, we load this and the next data vector offsets from the aux vector.
        // When the offset difference is zero, it can indicate an empty  value (length 0)
        // or a NULL (length -1). In the zero difference case, we have to load the header
        // from the data vector. In the positive difference case, the difference is equal
        // to the length, so there is no need to do an extra load.
        Label l_nonzero = c.new_label();
        Gp offset = c.new_gp64("offset");
        Gp length = c.new_gp64("length");
        Gp varsize_aux_address = c.new_gp64("varsize_aux_address");
        Gp next_input_index = c.new_gp64("next_input_index");
        c.mov(next_input_index, input_index);
        c.inc(next_input_index);
        c.mov(varsize_aux_address, ptr(varsize_aux_ptr, 8 * column_idx, 8));
        auto offset_shift = type_shift(data_type_t::i64);
        auto offset_size = 1 << offset_shift;
        c.mov(offset, ptr(varsize_aux_address, input_index, offset_shift, 0, offset_size));
        c.mov(length, ptr(varsize_aux_address, next_input_index, offset_shift, 0, offset_size));
        c.sub(length, offset);
        c.sub(length, header_size);
        // length now contains the length of the value. It can be zero for two reasons:
        // empty value or NULL value.
        c.jnz(l_nonzero);
        // If it's zero, we have to load the actual header value, which can be 0 or -1.
        Gp column_address = c.new_gp64("column_address");
        c.mov(column_address, ptr(data_ptr, 8 * column_idx, 8));
        c.mov(length, ptr(column_address, offset, 0, 0, header_size));
        c.bind(l_nonzero);
        if (header_size == 4) {
            return {length.r32(), data_type_t::i32, data_kind_t::kMemory};
        }
        return {length, data_type_t::i64, data_kind_t::kMemory};
    }

    // Reads length part of the varchar header for aux vector.
    // This part is stored in the lowest bytes of the header
    // (see VarcharTypeDriver to understand the format).
    //
    // Note: unlike read_mem_varsize this method doesn't return the length,
    //       so it can only be used in NULL checks.
    jit_value_t read_mem_varchar_header(Compiler &c,
                                        int32_t column_idx,
                                        const Gp &varsize_aux_ptr,
                                        const Gp &input_index) {
        Gp varsize_aux_address = c.new_gp64("varsize_aux_address");
        c.mov(varsize_aux_address, ptr(varsize_aux_ptr, 8 * column_idx, 8));

        Gp header_offset = c.new_gp64("header_offset");
        c.mov(header_offset, input_index);
        auto header_shift = type_shift(data_type_t::i128);
        c.sal(header_offset, header_shift);

        Gp header = c.new_gp64("header");
        c.mov(header, ptr(varsize_aux_address, header_offset, 0));

        return {header, data_type_t::i64, data_kind_t::kMemory};
    }

    jit_value_t read_mem(
            Compiler &c, data_type_t type, int32_t column_idx, const Gp &data_ptr,
            const Gp &varsize_aux_ptr, const Gp &input_index,
            const ColumnAddressCache &addr_cache,
            ColumnValueCache &value_cache
    ) {
        if (type == data_type_t::varchar_header) {
            return read_mem_varchar_header(c, column_idx, varsize_aux_ptr, input_index);
        }

        uint32_t header_size;
        switch (type) {
            case data_type_t::string_header:
                header_size = 4;
                break;
            case data_type_t::binary_header:
                header_size = 8;
                break;
            default:
                header_size = 0;
        }
        if (header_size != 0) {
            return read_mem_varsize(c, header_size, column_idx, data_ptr, varsize_aux_ptr, input_index);
        }

        // Check if value is already cached
        Gp cached_gp;
        Vec cached_xmm;
        if (type == data_type_t::f32 || type == data_type_t::f64) {
            if (value_cache.findXmm(column_idx, type, cached_xmm)) {
                return {cached_xmm, type, data_kind_t::kMemory};
            }
        } else {
            if (value_cache.find(column_idx, type, cached_gp)) {
                return {cached_gp, type, data_kind_t::kMemory};
            }
        }

        // Simple case: column has fixed-length data.
        // Use cached column address if available.
        Gp column_address;
        if (addr_cache.has(column_idx)) {
            column_address = addr_cache.get(column_idx);
        } else {
            column_address = c.new_gp64("column_address");
            c.mov(column_address, ptr(data_ptr, 8 * column_idx, 8));
        }

        auto shift = type_shift(type);
        auto type_size = 1 << shift;

        // Load value into register and cache it
        Mem mem_op = (type_size <= 8)
            ? Mem(column_address, input_index, shift, 0, type_size)
            : [&]() {
                Gp offset = c.new_gp64("row_offset");
                c.mov(offset, input_index);
                c.sal(offset, shift);
                return Mem(column_address, offset, 0, 0, type_size);
            }();

        switch (type) {
            case data_type_t::i8: {
                Gp reg = c.new_gp32("col_%d_i8", column_idx);
                c.movsx(reg, mem_op);
                value_cache.add(column_idx, type, reg);
                return {reg, type, data_kind_t::kMemory};
            }
            case data_type_t::i16: {
                Gp reg = c.new_gp32("col_%d_i16", column_idx);
                c.movsx(reg, mem_op);
                value_cache.add(column_idx, type, reg);
                return {reg, type, data_kind_t::kMemory};
            }
            case data_type_t::i32: {
                Gp reg = c.new_gp32("col_%d_i32", column_idx);
                c.mov(reg, mem_op);
                value_cache.add(column_idx, type, reg);
                return {reg, type, data_kind_t::kMemory};
            }
            case data_type_t::i64: {
                Gp reg = c.new_gp64("col_%d_i64", column_idx);
                c.mov(reg, mem_op);
                value_cache.add(column_idx, type, reg);
                return {reg, type, data_kind_t::kMemory};
            }
            case data_type_t::i128: {
                Vec reg =c.new_xmm("col_%d_i128", column_idx);
                c.movdqu(reg, mem_op);
                value_cache.addXmm(column_idx, type, reg);
                return {reg, type, data_kind_t::kMemory};
            }
            case data_type_t::f32: {
                Vec reg =c.new_xmm_ss("col_%d_f32", column_idx);
                c.movss(reg, mem_op);
                value_cache.addXmm(column_idx, type, reg);
                return {reg, type, data_kind_t::kMemory};
            }
            case data_type_t::f64: {
                Vec reg =c.new_xmm_sd("col_%d_f64", column_idx);
                c.movsd(reg, mem_op);
                value_cache.addXmm(column_idx, type, reg);
                return {reg, type, data_kind_t::kMemory};
            }
            default:
                __builtin_unreachable();
        }
    }

    jit_value_t mem2reg(Compiler &c, const jit_value_t &v) {
        auto type = v.dtype();
        auto mem = v.op().as<Mem>();
        switch (type) {
            case data_type_t::i8: {
                Gp row_data = c.new_gp32("i8_mem");
                c.movsx(row_data, mem);
                return {row_data, type, data_kind_t::kMemory};
            }
            case data_type_t::i16: {
                Gp row_data = c.new_gp32("i16_mem");
                c.movsx(row_data, mem);
                return {row_data, type, data_kind_t::kMemory};
            }
            case data_type_t::i32: {
                Gp row_data = c.new_gp32("i32_mem");
                c.mov(row_data, mem);
                return {row_data, type, data_kind_t::kMemory};
            }
            case data_type_t::i64: {
                Gp row_data = c.new_gp64("i64_mem");
                c.mov(row_data, mem);
                return {row_data, type, data_kind_t::kMemory};
            }
            case data_type_t::i128: {
                Vec row_data =c.new_xmm("i128_mem");
                c.movdqu(row_data, mem);
                return {row_data, type, data_kind_t::kMemory};
            }
            case data_type_t::f32: {
                Vec row_data =c.new_xmm_ss("f32_mem");
                c.movss(row_data, mem);
                return {row_data, type, data_kind_t::kMemory};
            }
            case data_type_t::f64: {
                Vec row_data =c.new_xmm_sd("f64_mem");
                c.movsd(row_data, mem);
                return {row_data, type, data_kind_t::kMemory};
            }
            default:
                __builtin_unreachable();
        }
    }

    jit_value_t read_imm(Compiler &c, const instruction_t &instr, const ConstantCache &cache) {
        auto type = static_cast<data_type_t>(instr.options);
        switch (type) {
            case data_type_t::i8:
            case data_type_t::i16:
            case data_type_t::i32:
            case data_type_t::i64: {
                // Check if constant is already in a register
                Gp reg;
                if (cache.findInt(instr.ipayload.lo, reg)) {
                    return {reg, type, data_kind_t::kConst};
                }
                return {imm(instr.ipayload.lo), type, data_kind_t::kConst};
            }
            case data_type_t::i128: {
                return {
                    c.new_const(ConstPoolScope::kLocal, &instr.ipayload, 16),
                    type,
                    data_kind_t::kMemory
                };
            }
            case data_type_t::f32:
            case data_type_t::f64: {
                // Check if constant is already in a register
                Vec reg;
                if (cache.findFloat(instr.dpayload, type, reg)) {
                    return {reg, type, data_kind_t::kConst};
                }
                return {imm(instr.dpayload), type, data_kind_t::kConst};
            }
            default:
                __builtin_unreachable();
        }
    }

    bool is_int32(int64_t x) {
        return x >= std::numeric_limits<int32_t>::min() && x <= std::numeric_limits<int32_t>::max();
    }

    bool is_float(double x) {
        return x >= -std::numeric_limits<float>::max() && x <= std::numeric_limits<float>::max();
    }

    jit_value_t imm2reg(Compiler &c, data_type_t dst_type, const jit_value_t &v) {
        Imm k = v.op().as<Imm>();
        if (k.is_int()) {
            auto value = k.value_as<int64_t>();
            switch (dst_type) {
                case data_type_t::f32: {
                    Vec reg =c.new_xmm_ss("f32_imm %f", value);
                    Mem mem = c.new_float_const(ConstPoolScope::kLocal, static_cast<float>(value));
                    c.movss(reg, mem);
                    return {reg, data_type_t::f32, data_kind_t::kConst};
                }
                case data_type_t::f64: {
                    Vec reg =c.new_xmm_sd("f64_imm %f", (double) value);
                    Mem mem = c.new_double_const(ConstPoolScope::kLocal, static_cast<double>(value));
                    c.movsd(reg, mem);
                    return {reg, data_type_t::f64, data_kind_t::kConst};
                }
                default: {
                    if (dst_type == data_type_t::i64 || !is_int32(value)) {
                        Gp reg = c.new_gp64("i64_imm %d", value);
                        c.movabs(reg, value);
                        return {reg, data_type_t::i64, data_kind_t::kConst};
                    } else {
                        Gp reg = c.new_gp32("i32_imm %d", value);
                        c.mov(reg, value);
                        return {reg, dst_type, data_kind_t::kConst};
                    }
                }
            }
        } else {
            auto value = k.value_as<double>();
            if (dst_type == data_type_t::i64 || dst_type == data_type_t::f64 || !is_float(value)) {
                Vec reg =c.new_xmm_sd("f64_imm %f", value);
                Mem mem = c.new_double_const(ConstPoolScope::kLocal, static_cast<double>(value));
                c.movsd(reg, mem);
                return {reg, data_type_t::f64, data_kind_t::kConst};
            } else {
                Vec reg =c.new_xmm_ss("f32_imm %f", value);
                Mem mem = c.new_float_const(ConstPoolScope::kLocal, static_cast<float>(value));
                c.movss(reg, mem);
                return {reg, data_type_t::f32, data_kind_t::kConst};
            }
        }
    }

    jit_value_t load_register(Compiler &c, data_type_t dst_type, const jit_value_t &v) {
        if (v.op().is_imm()) {
            return imm2reg(c, dst_type, v);
        } else if (v.op().is_mem()) {
            return mem2reg(c, v);
        } else {
            return v;
        }
    }

    jit_value_t load_register(Compiler &c, const jit_value_t &v) {
        return load_register(c, v.dtype(), v);
    }

    std::pair<jit_value_t, jit_value_t> load_registers(Compiler &c, const jit_value_t &lhs, const jit_value_t &rhs) {
        data_type_t lt;
        data_type_t rt;
        if (lhs.op().is_imm() && !rhs.op().is_imm()) {
            lt = rhs.dtype();
            rt = rhs.dtype();
        } else if (rhs.op().is_imm() && !lhs.op().is_imm()) {
            lt = lhs.dtype();
            rt = lhs.dtype();
        } else {
            lt = lhs.dtype();
            rt = rhs.dtype();
        }
        jit_value_t l = load_register(c, lt, lhs);
        jit_value_t r = load_register(c, rt, rhs);
        return {l, r};
    }

    jit_value_t neg(Compiler &c, const jit_value_t &lhs, bool null_check) {
        auto dt = lhs.dtype();
        auto dk = lhs.dkind();
        switch (dt) {
            case data_type_t::i8:
            case data_type_t::i16:
            case data_type_t::i32:
                return {int32_neg(c, lhs.gp().r32(), null_check), dt, dk};
            case data_type_t::i64:
                return {int64_neg(c, lhs.gp(), null_check), dt, dk};
            case data_type_t::f32:
                return {float_neg(c, lhs.vec()), dt, dk};
            case data_type_t::f64:
                return {double_neg(c, lhs.vec()), dt, dk};
            default:
                __builtin_unreachable();
        }
    }

    jit_value_t bin_not(Compiler &c, const jit_value_t &lhs) {
        auto dt = lhs.dtype();
        auto dk = lhs.dkind();
        return {int32_not(c, lhs.gp().r32()), dt, dk};
    }

    jit_value_t bin_and(Compiler &c, const jit_value_t &lhs, const jit_value_t &rhs) {
        auto dt = lhs.dtype();
        auto dk = dst_kind(lhs, rhs);
        return {int32_and(c, lhs.gp().r32(), rhs.gp().r32()), dt, dk};
    }

    jit_value_t bin_or(Compiler &c, const jit_value_t &lhs, const jit_value_t &rhs) {
        auto dt = lhs.dtype();
        auto dk = dst_kind(lhs, rhs);
        return {int32_or(c, lhs.gp().r32(), rhs.gp().r32()), dt, dk};
    }

    jit_value_t cmp_eq(Compiler &c, const jit_value_t &lhs, const jit_value_t &rhs) {
        auto dt = lhs.dtype();
        auto dk = dst_kind(lhs, rhs);
        switch (dt) {
            case data_type_t::i8:
            case data_type_t::i16:
            case data_type_t::i32:
            case data_type_t::string_header:
                return {int32_eq(c, lhs.gp().r32(), rhs.gp().r32()), data_type_t::i32, dk};
            case data_type_t::i64:
            case data_type_t::binary_header:
            case data_type_t::varchar_header:
                return {int64_eq(c, lhs.gp(), rhs.gp()), data_type_t::i32, dk};
            case data_type_t::i128:
                return {int128_eq(c, lhs.vec(), rhs.vec()), data_type_t::i32, dk};
            case data_type_t::f32:
                return {float_eq_epsilon(c, lhs.vec(), rhs.vec(), FLOAT_EPSILON), data_type_t::i32, dk};
            case data_type_t::f64:
                return {double_eq_epsilon(c, lhs.vec(), rhs.vec(), DOUBLE_EPSILON), data_type_t::i32, dk};
            default:
                __builtin_unreachable();
        }
    }

    jit_value_t cmp_ne(Compiler &c, const jit_value_t &lhs, const jit_value_t &rhs) {
        auto dt = lhs.dtype();
        auto dk = dst_kind(lhs, rhs);
        switch (dt) {
            case data_type_t::i8:
            case data_type_t::i16:
            case data_type_t::i32:
            case data_type_t::string_header:
                return {int32_ne(c, lhs.gp().r32(), rhs.gp().r32()), data_type_t::i32, dk};
            case data_type_t::i64:
            case data_type_t::binary_header:
            case data_type_t::varchar_header:
                return {int64_ne(c, lhs.gp(), rhs.gp()), data_type_t::i32, dk};
            case data_type_t::i128:
                return {int128_ne(c, lhs.vec(), rhs.vec()), data_type_t::i32, dk};
            case data_type_t::f32:
                return {float_ne_epsilon(c, lhs.vec(), rhs.vec(), FLOAT_EPSILON), data_type_t::i32, dk};
            case data_type_t::f64:
                return {double_ne_epsilon(c, lhs.vec(), rhs.vec(), DOUBLE_EPSILON), data_type_t::i32, dk};
            default:
                __builtin_unreachable();
        }
    }

    jit_value_t cmp_eq_zero(Compiler &c, const jit_value_t &lhs) {
        auto dt = lhs.dtype();
        auto dk = lhs.dkind();
        switch (dt) {
            case data_type_t::i8:
            case data_type_t::i16:
            case data_type_t::i32:
                return {int32_eq_zero(c, lhs.gp().r32()), data_type_t::i32, dk};
            case data_type_t::i64:
                return {int64_eq_zero(c, lhs.gp()), data_type_t::i32, dk};
            default:
                __builtin_unreachable();
        }
    }

    jit_value_t cmp_ne_zero(Compiler &c, const jit_value_t &lhs) {
        auto dt = lhs.dtype();
        auto dk = lhs.dkind();
        switch (dt) {
            case data_type_t::i8:
            case data_type_t::i16:
            case data_type_t::i32:
                return {int32_ne_zero(c, lhs.gp().r32()), data_type_t::i32, dk};
            case data_type_t::i64:
                return {int64_ne_zero(c, lhs.gp()), data_type_t::i32, dk};
            default:
                __builtin_unreachable();
        }
    }

    jit_value_t cmp_gt(Compiler &c, const jit_value_t &lhs, const jit_value_t &rhs, bool null_check) {
        auto dt = lhs.dtype();
        auto dk = dst_kind(lhs, rhs);
        switch (dt) {
            case data_type_t::i8:
            case data_type_t::i16:
            case data_type_t::i32:
                return {int32_gt(c, lhs.gp().r32(), rhs.gp().r32(), null_check), data_type_t::i32, dk};
            case data_type_t::i64:
                return {int64_gt(c, lhs.gp(), rhs.gp(), null_check), data_type_t::i32, dk};
            case data_type_t::f32: {
                Vec l =c.new_xmm_ss("lhs_copy");
                c.movss(l, lhs.vec());
                Vec r =c.new_xmm_ss("rhs_copy");
                c.movss(r, rhs.vec());
                return { bin_and(c,
                    {float_ne_epsilon(c, lhs.vec(), rhs.vec(), FLOAT_EPSILON), data_type_t::i32, dk},
                    {float_gt(c, l, r), data_type_t::i32, dk})
                };
            }
            case data_type_t::f64: {
                Vec l =c.new_xmm_sd("lhs_copy");
                c.movsd(l, lhs.vec());
                Vec r =c.new_xmm_sd("rhs_copy");
                c.movsd(r, rhs.vec());
                return { bin_and(c,
                    {double_ne_epsilon(c, lhs.vec(), rhs.vec(), DOUBLE_EPSILON), data_type_t::i32, dk},
                    {double_gt(c, l, r), data_type_t::i32, dk})
                };
            }
            default:
                __builtin_unreachable();
        }
    }

    jit_value_t cmp_ge(Compiler &c, const jit_value_t &lhs, const jit_value_t &rhs, bool null_check) {
        auto dt = lhs.dtype();
        auto dk = dst_kind(lhs, rhs);
        switch (dt) {
            case data_type_t::i8:
            case data_type_t::i16:
            case data_type_t::i32:
                return {int32_ge(c, lhs.gp().r32(), rhs.gp().r32(), null_check), data_type_t::i32, dk};
            case data_type_t::i64:
                return {int64_ge(c, lhs.gp(), rhs.gp(), null_check), data_type_t::i32, dk};
            case data_type_t::f32: {
                Vec l =c.new_xmm_ss("lhs_copy");
                c.movss(l, lhs.vec());
                Vec r =c.new_xmm_ss("rhs_copy");
                c.movss(r, rhs.vec());
                return { bin_or(c,
                    {float_eq_epsilon(c, lhs.vec(), rhs.vec(), FLOAT_EPSILON), data_type_t::i32, dk},
                    {float_ge(c, l, r), data_type_t::i32, dk})
                };
            }
            case data_type_t::f64: {
                Vec l =c.new_xmm_sd("lhs_copy");
                c.movsd(l, lhs.vec());
                Vec r =c.new_xmm_sd("rhs_copy");
                c.movsd(r, rhs.vec());
                return { bin_or(c,
                    {double_eq_epsilon(c, lhs.vec(), rhs.vec(), DOUBLE_EPSILON), data_type_t::i32, dk},
                    {double_ge(c, l, r), data_type_t::i32, dk})
                };
            }
            default:
                __builtin_unreachable();
        }
    }

    jit_value_t cmp_lt(Compiler &c, const jit_value_t &lhs, const jit_value_t &rhs, bool null_check) {
        auto dt = lhs.dtype();
        auto dk = dst_kind(lhs, rhs);
        switch (dt) {
            case data_type_t::i8:
            case data_type_t::i16:
            case data_type_t::i32:
                return {int32_lt(c, lhs.gp().r32(), rhs.gp().r32(), null_check), data_type_t::i32, dk};
            case data_type_t::i64:
                return {int64_lt(c, lhs.gp(), rhs.gp(), null_check), data_type_t::i32, dk};
            case data_type_t::f32: {
                Vec l =c.new_xmm_ss("lhs_copy");
                c.movss(l, lhs.vec());
                Vec r =c.new_xmm_ss("rhs_copy");
                c.movss(r, rhs.vec());
                return { bin_and(c,
                    {float_ne_epsilon(c, lhs.vec(), rhs.vec(), FLOAT_EPSILON), data_type_t::i32, dk},
                    {float_lt(c, l, r), data_type_t::i32, dk})
                };
            }
            case data_type_t::f64: {
                Vec l =c.new_xmm_sd("lhs_copy");
                c.movsd(l, lhs.vec());
                Vec r =c.new_xmm_sd("rhs_copy");
                c.movsd(r, rhs.vec());
                return { bin_and(c,
                    {double_ne_epsilon(c, lhs.vec(), rhs.vec(), DOUBLE_EPSILON), data_type_t::i32, dk},
                    {double_lt(c, l, r), data_type_t::i32, dk})
                };
            }
            default:
                __builtin_unreachable();
        }
    }

    jit_value_t cmp_le(Compiler &c, const jit_value_t &lhs, const jit_value_t &rhs, bool null_check) {
        auto dt = lhs.dtype();
        auto dk = dst_kind(lhs, rhs);
        switch (dt) {
            case data_type_t::i8:
            case data_type_t::i16:
            case data_type_t::i32:
                return {int32_le(c, lhs.gp().r32(), rhs.gp().r32(), null_check), data_type_t::i32, dk};
            case data_type_t::i64:
                return {int64_le(c, lhs.gp(), rhs.gp(), null_check), data_type_t::i32, dk};
            case data_type_t::f32: {
                Vec l =c.new_xmm_ss("lhs_copy");
                c.movss(l, lhs.vec());
                Vec r =c.new_xmm_ss("rhs_copy");
                c.movss(r, rhs.vec());
                return { bin_or(c,
                    {float_eq_epsilon(c, lhs.vec(), rhs.vec(), FLOAT_EPSILON), data_type_t::i32, dk},
                    {float_le(c, l, r), data_type_t::i32, dk})
                };
            }
            case data_type_t::f64: {
                Vec l =c.new_xmm_sd("lhs_copy");
                c.movsd(l, lhs.vec());
                Vec r =c.new_xmm_sd("rhs_copy");
                c.movsd(r, rhs.vec());
                return { bin_or(c,
                    {double_eq_epsilon(c, lhs.vec(), rhs.vec(), DOUBLE_EPSILON), data_type_t::i32, dk},
                    {double_le(c, l, r), data_type_t::i32, dk})
                };
            }
            default:
                __builtin_unreachable();
        }
    }

    jit_value_t add(Compiler &c, const jit_value_t &lhs, const jit_value_t &rhs, bool null_check) {
        auto dt = lhs.dtype();
        auto dk = dst_kind(lhs, rhs);
        switch (dt) {
            case data_type_t::i8:
            case data_type_t::i16:
            case data_type_t::i32:
                return {int32_add(c, lhs.gp().r32(), rhs.gp().r32(), null_check), dt, dk};
            case data_type_t::i64:
                return {int64_add(c, lhs.gp(), rhs.gp(), null_check), dt, dk};
            case data_type_t::f32:
                return {float_add(c, lhs.vec(), rhs.vec()), dt, dk};
            case data_type_t::f64:
                return {double_add(c, lhs.vec(), rhs.vec()), dt, dk};
            default:
                __builtin_unreachable();
        }
    }

    jit_value_t sub(Compiler &c, const jit_value_t &lhs, const jit_value_t &rhs, bool null_check) {
        auto dt = lhs.dtype();
        auto dk = dst_kind(lhs, rhs);
        switch (dt) {
            case data_type_t::i8:
            case data_type_t::i16:
            case data_type_t::i32:
                return {int32_sub(c, lhs.gp().r32(), rhs.gp().r32(), null_check), dt, dk};
            case data_type_t::i64:
                return {int64_sub(c, lhs.gp(), rhs.gp(), null_check), dt, dk};
            case data_type_t::f32:
                return {float_sub(c, lhs.vec(), rhs.vec()), dt, dk};
            case data_type_t::f64:
                return {double_sub(c, lhs.vec(), rhs.vec()), dt, dk};
            default:
                __builtin_unreachable();
        }
    }

    jit_value_t mul(Compiler &c, const jit_value_t &lhs, const jit_value_t &rhs, bool null_check) {
        auto dt = lhs.dtype();
        auto dk = dst_kind(lhs, rhs);
        switch (dt) {
            case data_type_t::i8:
            case data_type_t::i16:
            case data_type_t::i32:
                return {int32_mul(c, lhs.gp().r32(), rhs.gp().r32(), null_check), dt, dk};
            case data_type_t::i64:
                return {int64_mul(c, lhs.gp(), rhs.gp(), null_check), dt, dk};
            case data_type_t::f32:
                return {float_mul(c, lhs.vec(), rhs.vec()), dt, dk};
            case data_type_t::f64:
                return {double_mul(c, lhs.vec(), rhs.vec()), dt, dk};
            default:
                __builtin_unreachable();
        }
    }

    jit_value_t div(Compiler &c, const jit_value_t &lhs, const jit_value_t &rhs, bool null_check) {
        auto dt = lhs.dtype();
        auto dk = dst_kind(lhs, rhs);
        switch (dt) {
            case data_type_t::i8:
            case data_type_t::i16:
            case data_type_t::i32:
                return {int32_div(c, lhs.gp().r32(), rhs.gp().r32(), null_check), dt, dk};
            case data_type_t::i64:
                return {int64_div(c, lhs.gp(), rhs.gp(), null_check), dt, dk};
            case data_type_t::f32:
                return {float_div(c, lhs.vec(), rhs.vec()), dt, dk};
            case data_type_t::f64:
                return {double_div(c, lhs.vec(), rhs.vec()), dt, dk};
            default:
                __builtin_unreachable();
        }
    }

    inline bool cvt_null_check(data_type_t type) {
        return !(type == data_type_t::i8 || type == data_type_t::i16);
    }

    inline std::pair<jit_value_t, jit_value_t>
    convert(Compiler &c, const jit_value_t &lhs, const jit_value_t &rhs, bool null_check) {
        switch (lhs.dtype()) {
            case data_type_t::i8:
            case data_type_t::i16:
            case data_type_t::i32:
                switch (rhs.dtype()) {
                    case data_type_t::i8:
                    case data_type_t::i16:
                    case data_type_t::i32:
                        return std::make_pair(lhs, rhs);
                    case data_type_t::i64:
                        return std::make_pair(
                                jit_value_t(
                                        int32_to_int64(c, lhs.gp().r32(), null_check && cvt_null_check(lhs.dtype())),
                                        data_type_t::i64,
                                        lhs.dkind()), rhs);
                    case data_type_t::f32:
                        return std::make_pair(
                                jit_value_t(
                                        int32_to_float(c, lhs.gp().r32(), null_check && cvt_null_check(lhs.dtype())),
                                        data_type_t::f32,
                                        lhs.dkind()), rhs);
                    case data_type_t::f64:
                        return std::make_pair(
                                jit_value_t(
                                        int32_to_double(c, lhs.gp().r32(), null_check && cvt_null_check(lhs.dtype())),
                                        data_type_t::f64,
                                        lhs.dkind()), rhs);
                    default:
                        __builtin_unreachable();
                }
                break;
            case data_type_t::i64:
                switch (rhs.dtype()) {
                    case data_type_t::i8:
                    case data_type_t::i16:
                    case data_type_t::i32:
                        return std::make_pair(lhs,
                                              jit_value_t(
                                                      int32_to_int64(c, rhs.gp().r32(),
                                                                     null_check && cvt_null_check(rhs.dtype())),
                                                      data_type_t::i64, rhs.dkind()));
                    case data_type_t::i64:
                        return std::make_pair(lhs, rhs);
                    case data_type_t::f32:
                        return std::make_pair(
                                jit_value_t(int64_to_double(c, lhs.gp().r64(), null_check), data_type_t::f64,
                                            lhs.dkind()),
                                jit_value_t(float_to_double(c, rhs.vec()), data_type_t::f64, rhs.dkind()));
                    case data_type_t::f64:
                        return std::make_pair(
                                jit_value_t(int64_to_double(c, lhs.gp(), null_check), data_type_t::f64, lhs.dkind()),
                                rhs);
                    default:
                        __builtin_unreachable();
                }
                break;
            case data_type_t::f32:
                switch (rhs.dtype()) {
                    case data_type_t::i8:
                    case data_type_t::i16:
                    case data_type_t::i32:
                        return std::make_pair(lhs,
                                              jit_value_t(
                                                      int32_to_float(c, rhs.gp().r32(),
                                                                     null_check && cvt_null_check(rhs.dtype())),
                                                      data_type_t::f32, rhs.dkind()));
                    case data_type_t::i64:
                        return std::make_pair(jit_value_t(float_to_double(c, lhs.vec()), data_type_t::f64, lhs.dkind()),
                                              jit_value_t(int64_to_double(c, rhs.gp(), null_check), data_type_t::f64,
                                                          rhs.dkind()));
                    case data_type_t::f32:
                        return std::make_pair(lhs, rhs);
                    case data_type_t::f64:
                        return std::make_pair(jit_value_t(float_to_double(c, lhs.vec()), data_type_t::f64, lhs.dkind()),
                                              rhs);
                    default:
                        __builtin_unreachable();
                }
                break;
            case data_type_t::f64:
                switch (rhs.dtype()) {
                    case data_type_t::i8:
                    case data_type_t::i16:
                    case data_type_t::i32:
                        return std::make_pair(lhs,
                                              jit_value_t(
                                                      int32_to_double(c, rhs.gp().r32(),
                                                                      null_check && cvt_null_check(rhs.dtype())),
                                                      data_type_t::f64, rhs.dkind()));
                    case data_type_t::i64:
                        return std::make_pair(lhs,
                                              jit_value_t(int64_to_double(c, rhs.gp(), null_check), data_type_t::f64,
                                                          rhs.dkind()));
                    case data_type_t::f32:
                        return std::make_pair(lhs,
                                              jit_value_t(float_to_double(c, rhs.vec()),
                                                          data_type_t::f64,
                                                          rhs.dkind()));
                    case data_type_t::f64:
                        return std::make_pair(lhs, rhs);
                    default:
                        __builtin_unreachable();
                }
                break;
            case data_type_t::i128:
            case data_type_t::string_header:
            case data_type_t::binary_header:
            case data_type_t::varchar_header:
                return std::make_pair(lhs, rhs);
            default:
                __builtin_unreachable();
        }
    }

    inline jit_value_t get_argument(Compiler &c, ArenaVector<jit_value_t> &values) {
        auto arg = values.pop();
        return load_register(c, arg);
    }

    inline std::pair<jit_value_t, jit_value_t>
    get_arguments(Compiler &c, ArenaVector<jit_value_t> &values, bool null_check) {
        auto lhs = values.pop();
        auto rhs = values.pop();
        auto args = load_registers(c, lhs, rhs);
        return convert(c, args.first, args.second, null_check);
    }

    inline bool is_number(data_type_t dt) {
        return dt == data_type_t::i8 || dt == data_type_t::i16 ||
               dt == data_type_t::i32 || dt == data_type_t::i64 ||
               dt == data_type_t::f32 || dt == data_type_t::f64;
    }

    inline bool is_imm_int_zero(const jit_value_t &v) {
        if (!v.op().is_imm()) {
            return false;
        }
        if (!is_number(v.dtype())) {
            return false;
        }
        return v.op().as<Imm>().value_as<int64_t>() == 0;
    }

    // Check if a type supports flag-based short-circuit optimization
    inline bool supports_flag_optimization(data_type_t dt) {
        return dt == data_type_t::i8 || dt == data_type_t::i16 ||
               dt == data_type_t::i32 || dt == data_type_t::i64 ||
               dt == data_type_t::i128;
    }

    // Label array for short-circuit evaluation
    // Index 0 is reserved for l_next_row (skip current row)
    // Indices 1+ are for user-defined labels (e.g., OR success in IN())
    struct LabelArray {
        static constexpr size_t MAX_LABELS = 8;
        Label labels[MAX_LABELS];
        bool valid[MAX_LABELS] = {false};

        void set(size_t index, Label label) {
            if (index < MAX_LABELS) {
                labels[index] = label;
                valid[index] = true;
            }
        }

        Label get(size_t index) const {
            return labels[index];
        }

        bool has(size_t index) const {
            return index < MAX_LABELS && valid[index];
        }
    };

    void emit_bin_op(Compiler &c, Arena &arena, const instruction_t &instr, ArenaVector<jit_value_t> &values, bool null_check,
                     bool has_short_circuit_label, opcodes next_opcode) {
        // Special case: comparison with immediate zero can use TEST instead of CMP
        if (instr.opcode == opcodes::Eq || instr.opcode == opcodes::Ne) {
            auto lhs_raw = values.pop();
            auto rhs_raw = values.pop();

            bool lhs_is_zero = is_imm_int_zero(lhs_raw);
            bool rhs_is_zero = is_imm_int_zero(rhs_raw);

            if (lhs_is_zero || rhs_is_zero) {
                // Use TEST instruction for comparison with zero
                const jit_value_t &non_zero = lhs_is_zero ? rhs_raw : lhs_raw;
                auto loaded = load_register(c, non_zero);

                if (instr.opcode == opcodes::Eq) {
                    values.append(arena, cmp_eq_zero(c, loaded));
                } else {
                    values.append(arena, cmp_ne_zero(c, loaded));
                }
                return;
            }

            // Not a zero comparison, proceed with normal path
            auto args = load_registers(c, lhs_raw, rhs_raw);
            auto converted = convert(c, args.first, args.second, null_check);
            auto lhs = converted.first;
            auto rhs = converted.second;

            // Optimization: if next instruction is a short-circuit op, emit only CMP and push flag marker
            // This avoids boolean materialization (SETE/SETNE + TEST + JZ/JNZ)
            if (has_short_circuit_label &&
                (next_opcode == opcodes::And_Sc || next_opcode == opcodes::Or_Sc) &&
                supports_flag_optimization(lhs.dtype())) {
                auto dt = lhs.dtype();
                // Emit only CMP, no SETE/SETNE
                switch (dt) {
                    case data_type_t::i8:
                    case data_type_t::i16:
                    case data_type_t::i32:
                        c.cmp(lhs.gp().r32(), rhs.gp().r32());
                        break;
                    case data_type_t::i64:
                        c.cmp(lhs.gp(), rhs.gp());
                        break;
                    case data_type_t::i128: {
                        // For i128, use pcmpeqb + pmovmskb + cmp (same as int128_cmp)
                        Gp mask = c.new_gp16();
                        c.pcmpeqb(lhs.vec(), rhs.vec());
                        c.pmovmskb(mask, lhs.vec());
                        c.cmp(mask, 0xffff);
                        break;
                    }
                    default:
                        break;
                }
                // Push a dummy register with flag marker kind
                Gp dummy = c.new_gp32("flags_marker");
                auto flag_kind = (instr.opcode == opcodes::Eq) ? data_kind_t::kFlagsEq : data_kind_t::kFlagsNe;
                values.append(arena, {dummy, data_type_t::i32, flag_kind});
                return;
            }

            if (instr.opcode == opcodes::Eq) {
                values.append(arena, cmp_eq(c, lhs, rhs));
            } else {
                values.append(arena, cmp_ne(c, lhs, rhs));
            }
            return;
        }

        auto args = get_arguments(c, values, null_check);
        auto lhs = args.first;
        auto rhs = args.second;
        switch (instr.opcode) {
            case opcodes::And:
                values.append(arena, bin_and(c, lhs, rhs));
                break;
            case opcodes::Or:
                values.append(arena, bin_or(c, lhs, rhs));
                break;
            case opcodes::Gt:
                values.append(arena, cmp_gt(c, lhs, rhs, null_check));
                break;
            case opcodes::Ge:
                values.append(arena, cmp_ge(c, lhs, rhs, null_check));
                break;
            case opcodes::Lt:
                values.append(arena, cmp_lt(c, lhs, rhs, null_check));
                break;
            case opcodes::Le:
                values.append(arena, cmp_le(c, lhs, rhs, null_check));
                break;
            case opcodes::Add:
                values.append(arena, add(c, lhs, rhs, null_check));
                break;
            case opcodes::Sub:
                values.append(arena, sub(c, lhs, rhs, null_check));
                break;
            case opcodes::Mul:
                values.append(arena, mul(c, lhs, rhs, null_check));
                break;
            case opcodes::Div:
                values.append(arena, div(c, lhs, rhs, null_check));
                break;
            default:
                __builtin_unreachable();
        }
    }

    void
    emit_code(Compiler &c, Arena &arena, const instruction_t *istream, size_t size, ArenaVector<jit_value_t> &values,
              bool null_check,
              const Gp &data_ptr,
              const Gp &varsize_aux_ptr,
              const Gp &vars_ptr,
              const Gp &input_index,
              LabelArray &labels,
              const ColumnAddressCache &addr_cache,
              const ConstantCache &const_cache,
              ColumnValueCache &value_cache) {

        for (size_t i = 0; i < size; ++i) {
            auto &instr = istream[i];
            switch (instr.opcode) {
                case opcodes::Inv:
                    return; // todo: throw exception
                case opcodes::Ret:
                    return;
                case opcodes::Var: {
                    auto type = static_cast<data_type_t>(instr.options);
                    auto idx  = static_cast<int32_t>(instr.ipayload.lo);
                    values.append(arena, read_vars_mem(c, type, idx, vars_ptr));
                }
                    break;
                case opcodes::Mem: {
                    auto type = static_cast<data_type_t>(instr.options);
                    auto idx  = static_cast<int32_t>(instr.ipayload.lo);
                    values.append(arena, read_mem(c, type, idx, data_ptr, varsize_aux_ptr, input_index, addr_cache, value_cache));
                }
                    break;
                case opcodes::Imm:
                    values.append(arena, read_imm(c, instr, const_cache));
                    break;
                case opcodes::Neg:
                    values.append(arena, neg(c, get_argument(c, values), null_check));
                    break;
                case opcodes::Not:
                    values.append(arena, bin_not(c, get_argument(c, values)));
                    break;
                case opcodes::And_Sc: {
                    // Short-circuit AND: if false, jump to label[index]
                    auto label_idx = static_cast<size_t>(instr.ipayload.lo);
                    auto arg = values.pop();
                    if (labels.has(label_idx)) {
                        Label target = labels.get(label_idx);
                        if (arg.dkind() == data_kind_t::kFlagsEq) {
                            // Flags set by CMP for equality: jump if NOT equal
                            c.jne(target);
                        } else if (arg.dkind() == data_kind_t::kFlagsNe) {
                            // Flags set by CMP for inequality: jump if equal
                            c.je(target);
                        } else {
                            // Normal path: test and jump if zero (false)
                            auto loaded = load_register(c, arg);
                            c.test(loaded.gp().r32(), loaded.gp().r32());
                            c.jz(target);
                        }
                    }
                    break;
                }
                case opcodes::Or_Sc: {
                    // Short-circuit OR: if true, jump to label[index]
                    auto label_idx = static_cast<size_t>(instr.ipayload.lo);
                    auto arg = values.pop();
                    if (labels.has(label_idx)) {
                        Label target = labels.get(label_idx);
                        if (arg.dkind() == data_kind_t::kFlagsEq) {
                            // Flags set by CMP for equality: jump if equal
                            c.je(target);
                        } else if (arg.dkind() == data_kind_t::kFlagsNe) {
                            // Flags set by CMP for inequality: jump if not equal
                            c.jne(target);
                        } else {
                            // Normal path: test and jump if non-zero (true)
                            auto loaded = load_register(c, arg);
                            c.test(loaded.gp().r32(), loaded.gp().r32());
                            c.jnz(target);
                        }
                    }
                    break;
                }
                case opcodes::Begin_Sc: {
                    // Create label at index
                    auto label_idx = static_cast<size_t>(instr.ipayload.lo);
                    Label label = c.new_label();
                    labels.set(label_idx, label);
                    break;
                }
                case opcodes::End_Sc: {
                    // Bind label at index
                    auto label_idx = static_cast<size_t>(instr.ipayload.lo);
                    if (labels.has(label_idx)) {
                        c.bind(labels.get(label_idx));
                    }
                    break;
                }
                default: {
                    // Get next opcode for lookahead optimization
                    opcodes next_op = (i + 1 < size) ? istream[i + 1].opcode : opcodes::Ret;
                    emit_bin_op(c, arena, instr, values, null_check, labels.has(0), next_op);
                    break;
                }
            }
        }
    }

}

#endif //QUESTDB_JIT_X86_H
