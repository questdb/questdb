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

#ifndef QUESTDB_JIT_X86_H
#define QUESTDB_JIT_X86_H

#include "common.h"
#include "impl/x86.h"

namespace questdb::x86 {
    using namespace asmjit::x86;

    jit_value_t
    read_vars_mem(Compiler &c, data_type_t type, int32_t idx, const Gp &vars_ptr) {
        auto shift = type_shift(type);
        auto type_size = 1 << shift;
        return {Mem(vars_ptr, 8 * idx, type_size), type, data_kind_t::kMemory};
    }

    jit_value_t
    read_mem(Compiler &c, data_type_t type, int32_t column_idx, const Gp &cols_ptr, const Gp &input_index) {
        Gp column_address = c.newInt64("column_address");
        c.mov(column_address, ptr(cols_ptr, 8 * column_idx, 8));
        auto shift = type_shift(type);
        auto type_size = 1 << shift;
        if (type_size <= 8) {
            return {Mem(column_address, input_index, shift, 0, type_size), type, data_kind_t::kMemory};
        } else {
            Gp offset = c.newInt64("row_offset");
            c.mov(offset, input_index);
            c.sal(offset, shift);
            return {Mem(column_address, offset, 0, 0, type_size), type, data_kind_t::kMemory};
        }
    }

    jit_value_t mem2reg(Compiler &c, const jit_value_t &v) {
        auto type = v.dtype();
        auto mem = v.op().as<Mem>();
        switch (type) {
            case data_type_t::i8: {
                Gp row_data = c.newGpd("i8_mem");
                c.movsx(row_data, mem);
                return {row_data, type, data_kind_t::kMemory};
            }
            case data_type_t::i16: {
                Gp row_data = c.newGpd("i16_mem");
                c.movsx(row_data, mem);
                return {row_data, type, data_kind_t::kMemory};
            }
            case data_type_t::i32: {
                Gp row_data = c.newGpd("i32_mem");
                c.mov(row_data, mem);
                return {row_data, type, data_kind_t::kMemory};
            }
            case data_type_t::i64: {
                Gp row_data = c.newGpq("i64_mem");
                c.mov(row_data, mem);
                return {row_data, type, data_kind_t::kMemory};
            }
            case data_type_t::i128: {
                Xmm row_data = c.newXmm("i128_mem");
                c.movdqu(row_data, mem);
                return {row_data, type, data_kind_t::kMemory};
            }
            case data_type_t::f32: {
                Xmm row_data = c.newXmmSs("f32_mem");
                c.movss(row_data, mem);
                return {row_data, type, data_kind_t::kMemory};
            }
            case data_type_t::f64: {
                Xmm row_data = c.newXmmSd("f64_mem");
                c.movsd(row_data, mem);
                return {row_data, type, data_kind_t::kMemory};
            }
            default:
                __builtin_unreachable();
        }
    }

    jit_value_t read_imm(Compiler &c, const instruction_t &instr) {
        auto type = static_cast<data_type_t>(instr.options);
        switch (type) {
            case data_type_t::i8:
            case data_type_t::i16:
            case data_type_t::i32:
            case data_type_t::i64: {
                return {imm(instr.ipayload.lo), type, data_kind_t::kConst};
            }
            case data_type_t::i128: {
                return { c.newConst(ConstPool::kScopeLocal, &instr.ipayload, 16),
                         type,
                         data_kind_t::kMemory
                };
            }
            case data_type_t::f32:
            case data_type_t::f64: {
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
        return x >= std::numeric_limits<float>::min() && x <= std::numeric_limits<float>::max();
    }

    jit_value_t imm2reg(Compiler &c, data_type_t dst_type, const jit_value_t &v) {
        Imm k = v.op().as<Imm>();
        if (k.isInteger()) {
            auto value = k.valueAs<int64_t>();
            switch (dst_type) {
                case data_type_t::f32: {
                    Xmm reg = c.newXmmSs("f32_imm %f", value);
                    Mem mem = c.newFloatConst(ConstPool::kScopeLocal, static_cast<float>(value));
                    c.movss(reg, mem);
                    return {reg, data_type_t::f32, data_kind_t::kConst};
                }
                case data_type_t::f64: {
                    Xmm reg = c.newXmmSd("f64_imm %f", (double) value);
                    Mem mem = c.newDoubleConst(ConstPool::kScopeLocal, static_cast<double>(value));
                    c.movsd(reg, mem);
                    return {reg, data_type_t::f64, data_kind_t::kConst};
                }
                default: {
                    if (dst_type == data_type_t::i64 || !is_int32(value)) {
                        Gp reg = c.newGpq("i64_imm %d", value);
                        c.movabs(reg, value);
                        return {reg, data_type_t::i64, data_kind_t::kConst};
                    } else {
                        Gp reg = c.newGpd("i32_imm %d", value);
                        c.mov(reg, value);
                        return {reg, dst_type, data_kind_t::kConst};
                    }
                }
            }
        } else {
            auto value = k.valueAs<double>();
            if (dst_type == data_type_t::i64 || dst_type == data_type_t::f64 || !is_float(value)) {
                Xmm reg = c.newXmmSd("f64_imm %f", value);
                Mem mem = c.newDoubleConst(ConstPool::kScopeLocal, static_cast<double>(value));
                c.movsd(reg, mem);
                return {reg, data_type_t::f64, data_kind_t::kConst};
            } else {
                Xmm reg = c.newXmmSs("f32_imm %f", value);
                Mem mem = c.newFloatConst(ConstPool::kScopeLocal, static_cast<float>(value));
                c.movss(reg, mem);
                return {reg, data_type_t::f32, data_kind_t::kConst};
            }
        }
    }

    jit_value_t load_register(Compiler &c, data_type_t dst_type, const jit_value_t &v) {
        if (v.op().isImm()) {
            return imm2reg(c, dst_type, v);
        } else if (v.op().isMem()) {
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
        if (lhs.op().isImm() && !rhs.op().isImm()) {
            lt = rhs.dtype();
            rt = rhs.dtype();
        } else if (rhs.op().isImm() && !lhs.op().isImm()) {
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
                return {float_neg(c, lhs.xmm()), dt, dk};
            case data_type_t::f64:
                return {double_neg(c, lhs.xmm()), dt, dk};
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
                return {int32_eq(c, lhs.gp().r32(), rhs.gp().r32()), data_type_t::i32, dk};
            case data_type_t::i64:
                return {int64_eq(c, lhs.gp(), rhs.gp()), data_type_t::i32, dk};
            case data_type_t::i128:
                return {int128_eq(c, lhs.xmm(), rhs.xmm()), data_type_t::i32, dk};
            case data_type_t::f32:
                return {float_eq_epsilon(c, lhs.xmm(), rhs.xmm(), FLOAT_EPSILON), data_type_t::i32, dk};
            case data_type_t::f64:
                return {double_eq_epsilon(c, lhs.xmm(), rhs.xmm(), DOUBLE_EPSILON), data_type_t::i32, dk};
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
                return {int32_ne(c, lhs.gp().r32(), rhs.gp().r32()), data_type_t::i32, dk};
            case data_type_t::i64:
                return {int64_ne(c, lhs.gp(), rhs.gp()), data_type_t::i32, dk};
            case data_type_t::i128:
                return {int128_ne(c, lhs.xmm(), rhs.xmm()), data_type_t::i32, dk};
            case data_type_t::f32:
                return {float_ne_epsilon(c, lhs.xmm(), rhs.xmm(), FLOAT_EPSILON), data_type_t::i32, dk};
            case data_type_t::f64:
                return {double_ne_epsilon(c, lhs.xmm(), rhs.xmm(), DOUBLE_EPSILON), data_type_t::i32, dk};
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
            case data_type_t::f32:
                return {float_gt(c, lhs.xmm(), rhs.xmm()), data_type_t::i32, dk};
            case data_type_t::f64:
                return {double_gt(c, lhs.xmm(), rhs.xmm()), data_type_t::i32, dk};
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
            case data_type_t::f32:
                return {float_ge(c, lhs.xmm(), rhs.xmm()), data_type_t::i32, dk};
            case data_type_t::f64:
                return {double_ge(c, lhs.xmm(), rhs.xmm()), data_type_t::i32, dk};
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
            case data_type_t::f32:
                return {float_lt(c, lhs.xmm(), rhs.xmm()), data_type_t::i32, dk};
            case data_type_t::f64:
                return {double_lt(c, lhs.xmm(), rhs.xmm()), data_type_t::i32, dk};
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
            case data_type_t::f32:
                return {float_le(c, lhs.xmm(), rhs.xmm()), data_type_t::i32, dk};
            case data_type_t::f64:
                return {double_le(c, lhs.xmm(), rhs.xmm()), data_type_t::i32, dk};
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
                return {float_add(c, lhs.xmm(), rhs.xmm()), dt, dk};
            case data_type_t::f64:
                return {double_add(c, lhs.xmm(), rhs.xmm()), dt, dk};
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
                return {float_sub(c, lhs.xmm(), rhs.xmm()), dt, dk};
            case data_type_t::f64:
                return {double_sub(c, lhs.xmm(), rhs.xmm()), dt, dk};
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
                return {float_mul(c, lhs.xmm(), rhs.xmm()), dt, dk};
            case data_type_t::f64:
                return {double_mul(c, lhs.xmm(), rhs.xmm()), dt, dk};
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
                return {float_div(c, lhs.xmm(), rhs.xmm()), dt, dk};
            case data_type_t::f64:
                return {double_div(c, lhs.xmm(), rhs.xmm()), dt, dk};
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
                                jit_value_t(float_to_double(c, rhs.xmm()), data_type_t::f64, rhs.dkind()));
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
                        return std::make_pair(jit_value_t(float_to_double(c, lhs.xmm()), data_type_t::f64, lhs.dkind()),
                                              jit_value_t(int64_to_double(c, rhs.gp(), null_check), data_type_t::f64,
                                                          rhs.dkind()));
                    case data_type_t::f32:
                        return std::make_pair(lhs, rhs);
                    case data_type_t::f64:
                        return std::make_pair(jit_value_t(float_to_double(c, lhs.xmm()), data_type_t::f64, lhs.dkind()),
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
                                              jit_value_t(float_to_double(c, rhs.xmm()),
                                                          data_type_t::f64,
                                                          rhs.dkind()));
                    case data_type_t::f64:
                        return std::make_pair(lhs, rhs);
                    default:
                        __builtin_unreachable();
                }
                break;
            case data_type_t::i128:
                return std::make_pair(lhs, rhs);
            default:
                __builtin_unreachable();
        }
    }

    inline jit_value_t get_argument(Compiler &c, ZoneStack<jit_value_t> &values) {
        auto arg = values.pop();
        return load_register(c, arg);
    }

    inline std::pair<jit_value_t, jit_value_t>
    get_arguments(Compiler &c, ZoneStack<jit_value_t> &values, bool null_check) {
        auto lhs = values.pop();
        auto rhs = values.pop();
        auto args = load_registers(c, lhs, rhs);
        return convert(c, args.first, args.second, null_check);
    }

    void emit_bin_op(Compiler &c, const instruction_t &instr, ZoneStack<jit_value_t> &values, bool null_check) {
        auto args = get_arguments(c, values, null_check);
        auto lhs = args.first;
        auto rhs = args.second;
        switch (instr.opcode) {
            case opcodes::And:
                values.append(bin_and(c, lhs, rhs));
                break;
            case opcodes::Or:
                values.append(bin_or(c, lhs, rhs));
                break;
            case opcodes::Eq:
                values.append(cmp_eq(c, lhs, rhs));
                break;
            case opcodes::Ne:
                values.append(cmp_ne(c, lhs, rhs));
                break;
            case opcodes::Gt:
                values.append(cmp_gt(c, lhs, rhs, null_check));
                break;
            case opcodes::Ge:
                values.append(cmp_ge(c, lhs, rhs, null_check));
                break;
            case opcodes::Lt:
                values.append(cmp_lt(c, lhs, rhs, null_check));
                break;
            case opcodes::Le:
                values.append(cmp_le(c, lhs, rhs, null_check));
                break;
            case opcodes::Add:
                values.append(add(c, lhs, rhs, null_check));
                break;
            case opcodes::Sub:
                values.append(sub(c, lhs, rhs, null_check));
                break;
            case opcodes::Mul:
                values.append(mul(c, lhs, rhs, null_check));
                break;
            case opcodes::Div:
                values.append(div(c, lhs, rhs, null_check));
                break;
            default:
                __builtin_unreachable();
        }
    }

    void
    emit_code(Compiler &c, const instruction_t *istream, size_t size, ZoneStack<jit_value_t> &values,
              bool null_check,
              const Gp &cols_ptr,
              const Gp &vars_ptr,
              const Gp &input_index) {

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
                    values.append(read_vars_mem(c, type, idx, vars_ptr));
                }
                    break;
                case opcodes::Mem: {
                    auto type = static_cast<data_type_t>(instr.options);
                    auto idx  = static_cast<int32_t>(instr.ipayload.lo);
                    values.append(read_mem(c, type, idx, cols_ptr, input_index));
                }
                    break;
                case opcodes::Imm:
                    values.append(read_imm(c, instr));
                    break;
                case opcodes::Neg:
                    values.append(neg(c, get_argument(c, values), null_check));
                    break;
                case opcodes::Not:
                    values.append(bin_not(c, get_argument(c, values)));
                    break;
                default:
                    emit_bin_op(c, instr, values, null_check);
                    break;
            }
        }
    }
}

#endif //QUESTDB_JIT_X86_H
