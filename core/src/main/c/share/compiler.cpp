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

#include "compiler.h"
#include <stack>
#include <iostream>

using namespace asmjit;

class JitException : public std::exception {
public:
    Error err;
    std::string message;

    JitException(Error err, const char *message) noexcept
            : err(err), message(message) {}

    const char *what() const noexcept override { return message.c_str(); }
};

struct JitErrorHandler : public asmjit::ErrorHandler {
    void handleError(asmjit::Error err, const char *msg, asmjit::BaseEmitter * /*origin*/) override {
        throw JitException(err, msg);
    }
};

struct JitGlobalContext {
    //rt allocator is thread-safe
    JitRuntime rt;
    JitErrorHandler errorHandler;
};

#ifndef __aarch64__
static JitGlobalContext gGlobalContext;
#endif

uint32_t type_shift(data_type_t type) {
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

namespace questdb::x86 {
    using namespace asmjit::x86;

    jit_value_t
    read_vars_mem(Compiler &c, data_type_t type, const uint8_t *istream, size_t size, uint32_t &pos, const Gp &vars_ptr) {
        auto idx = static_cast<int32_t>(read<int64_t>(istream, size, pos));
        auto shift = type_shift(type);
        auto type_size  = 1 << shift;
        return {Mem(vars_ptr, 8*idx, type_size), type, data_kind_t::kMemory};
    }

    jit_value_t
    read_mem(Compiler &c, data_type_t type, const uint8_t *istream, size_t size, uint32_t &pos, const Gp &cols_ptr,
             const Gp &input_index) {

        auto column_idx = static_cast<int32_t>(read<int64_t>(istream, size, pos));
        Gp column_address = c.newInt64("column_address");
        c.mov(column_address, ptr(cols_ptr, 8 * column_idx, 8));
        auto shift = type_shift(type);
        auto type_size  = 1 << shift;
        return {Mem(column_address, input_index, shift, 0, type_size), type, data_kind_t::kMemory};
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
            case data_type_t::f32: {
                Xmm row_data = c.newXmmSs("f32_mem");
                c.vmovss(row_data, mem);
                return {row_data, type, data_kind_t::kMemory};
            }
            case data_type_t::f64: {
                Xmm row_data = c.newXmmSd("f64_mem");
                c.vmovsd(row_data, mem);
                return {row_data, type, data_kind_t::kMemory};
            }
            default:
                __builtin_unreachable();
        }
    }

    jit_value_t read_imm(Compiler &c, data_type_t type, const uint8_t *istream, size_t size, uint32_t &pos) {
        switch (type) {
            case data_type_t::i8:
            case data_type_t::i16:
            case data_type_t::i32:
            case data_type_t::i64: {
                auto value = read<int64_t>(istream, size, pos);
                return {imm(value), type, data_kind_t::kConst};
            }
            case data_type_t::f32:
            case data_type_t::f64: {
                auto value = read<double>(istream, size, pos);
                return {imm(value), type, data_kind_t::kConst};
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
        auto type = v.dtype();
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
        auto dk = (lhs.dkind() == data_kind_t::kConst && rhs.dkind() == data_kind_t::kConst) ? data_kind_t::kConst
                                                                                             : data_kind_t::kMemory;
        return {int32_and(c, lhs.gp().r32(), rhs.gp().r32()), dt, dk};
    }

    jit_value_t bin_or(Compiler &c, const jit_value_t &lhs, const jit_value_t &rhs) {
        auto dt = lhs.dtype();
        auto dk = (lhs.dkind() == data_kind_t::kConst && rhs.dkind() == data_kind_t::kConst) ? data_kind_t::kConst
                                                                                             : data_kind_t::kMemory;
        return {int32_or(c, lhs.gp().r32(), rhs.gp().r32()), dt, dk};
    }

    jit_value_t cmp_eq(Compiler &c, const jit_value_t &lhs, const jit_value_t &rhs) {
        auto dt = lhs.dtype();
        auto dk = (lhs.dkind() == data_kind_t::kConst && rhs.dkind() == data_kind_t::kConst) ? data_kind_t::kConst
                                                                                             : data_kind_t::kMemory;
        switch (dt) {
            case data_type_t::i8:
            case data_type_t::i16:
            case data_type_t::i32:
                return {int32_eq(c, lhs.gp().r32(), rhs.gp().r32()), data_type_t::i32, dk};
            case data_type_t::i64:
                return {int64_eq(c, lhs.gp(), rhs.gp()), data_type_t::i32, dk};
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
        auto dk = (lhs.dkind() == data_kind_t::kConst && rhs.dkind() == data_kind_t::kConst) ? data_kind_t::kConst
                                                                                             : data_kind_t::kMemory;
        switch (dt) {
            case data_type_t::i8:
            case data_type_t::i16:
            case data_type_t::i32:
                return {int32_ne(c, lhs.gp().r32(), rhs.gp().r32()), data_type_t::i32, dk};
            case data_type_t::i64:
                return {int64_ne(c, lhs.gp(), rhs.gp()), data_type_t::i32, dk};
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
        auto dk = (lhs.dkind() == data_kind_t::kConst && rhs.dkind() == data_kind_t::kConst) ? data_kind_t::kConst
                                                                                             : data_kind_t::kMemory;
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
        auto dk = (lhs.dkind() == data_kind_t::kConst && rhs.dkind() == data_kind_t::kConst) ? data_kind_t::kConst
                                                                                             : data_kind_t::kMemory;
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
        auto dk = (lhs.dkind() == data_kind_t::kConst && rhs.dkind() == data_kind_t::kConst) ? data_kind_t::kConst
                                                                                             : data_kind_t::kMemory;
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
        auto dk = (lhs.dkind() == data_kind_t::kConst && rhs.dkind() == data_kind_t::kConst) ? data_kind_t::kConst
                                                                                             : data_kind_t::kMemory;
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
        auto dk = (lhs.dkind() == data_kind_t::kConst && rhs.dkind() == data_kind_t::kConst) ? data_kind_t::kConst
                                                                                             : data_kind_t::kMemory;
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
        auto dk = (lhs.dkind() == data_kind_t::kConst && rhs.dkind() == data_kind_t::kConst) ? data_kind_t::kConst
                                                                                             : data_kind_t::kMemory;
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
        auto dk = (lhs.dkind() == data_kind_t::kConst && rhs.dkind() == data_kind_t::kConst) ? data_kind_t::kConst
                                                                                             : data_kind_t::kMemory;
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
        auto dk = (lhs.dkind() == data_kind_t::kConst && rhs.dkind() == data_kind_t::kConst) ? data_kind_t::kConst
                                                                                             : data_kind_t::kMemory;
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
            default:
                __builtin_unreachable();
        }
    }

    inline jit_value_t get_argument(Compiler &c, std::stack<jit_value_t> &values) {
        auto arg = values.top();
        values.pop();
        return load_register(c, arg);
    }

    inline std::pair<jit_value_t, jit_value_t>
    get_arguments(Compiler &c, std::stack<jit_value_t> &values, bool null_check) {
        auto lhs = values.top();
        values.pop();
        auto rhs = values.top();
        values.pop();
        auto args = load_registers(c, lhs, rhs);
        return convert(c, args.first, args.second, null_check);
    }

    void emit_bin_op(Compiler &c, instruction_t ic, std::stack<jit_value_t> &values, bool null_check) {
        auto args = get_arguments(c, values, null_check);
        switch (ic) {
            case instruction_t::AND:
                values.push(bin_and(c, args.first, args.second));
                break;
            case instruction_t::OR:
                values.push(bin_or(c, args.first, args.second));
                break;
            case instruction_t::EQ:
                values.push(cmp_eq(c, args.first, args.second));
                break;
            case instruction_t::NE:
                values.push(cmp_ne(c, args.first, args.second));
                break;
            case instruction_t::GT:
                values.push(cmp_gt(c, args.first, args.second, null_check));
                break;
            case instruction_t::GE:
                values.push(cmp_ge(c, args.first, args.second, null_check));
                break;
            case instruction_t::LT:
                values.push(cmp_lt(c, args.first, args.second, null_check));
                break;
            case instruction_t::LE:
                values.push(cmp_le(c, args.first, args.second, null_check));
                break;
            case instruction_t::ADD:
                values.push(add(c, args.first, args.second, null_check));
                break;
            case instruction_t::SUB:
                values.push(sub(c, args.first, args.second, null_check));
                break;
            case instruction_t::MUL:
                values.push(mul(c, args.first, args.second, null_check));
                break;
            case instruction_t::DIV:
                values.push(div(c, args.first, args.second, null_check));
                break;
            default:
                __builtin_unreachable();
        }
    }

    void
    emit_code(Compiler &c, const uint8_t *filter_expr, size_t filter_size, std::stack<jit_value_t> &values,
              bool null_check,
              const Gp &cols_ptr,
              const Gp &vars_ptr,
              const Gp &input_index) {
        uint32_t read_pos = 0;
        while (read_pos < filter_size) {
            auto ic = static_cast<instruction_t>(read<uint8_t>(filter_expr, filter_size, read_pos));
            switch (ic) {
                case instruction_t::RET:
                    break;
                case instruction_t::VAR_I1:
                    values.push(
                            read_vars_mem(c, data_type_t::i8, filter_expr, filter_size, read_pos, vars_ptr));
                    break;
                case instruction_t::VAR_I2:
                    values.push(
                            read_vars_mem(c, data_type_t::i16, filter_expr, filter_size, read_pos, vars_ptr));
                    break;
                case instruction_t::VAR_I4:
                    values.push(
                            read_vars_mem(c, data_type_t::i32, filter_expr, filter_size, read_pos, vars_ptr));
                    break;
                case instruction_t::VAR_I8:
                    values.push(
                            read_vars_mem(c, data_type_t::i64, filter_expr, filter_size, read_pos, vars_ptr));
                    break;
                case instruction_t::VAR_F4:
                    values.push(
                            read_vars_mem(c, data_type_t::f32, filter_expr, filter_size, read_pos, vars_ptr));
                    break;
                case instruction_t::VAR_F8:
                    values.push(
                            read_vars_mem(c, data_type_t::f64, filter_expr, filter_size, read_pos, vars_ptr));
                    break;
                case instruction_t::MEM_I1:
                    values.push(
                            read_mem(c, data_type_t::i8, filter_expr, filter_size, read_pos, cols_ptr, input_index));
                    break;
                case instruction_t::MEM_I2:
                    values.push(
                            read_mem(c, data_type_t::i16, filter_expr, filter_size, read_pos, cols_ptr, input_index));
                    break;
                case instruction_t::MEM_I4:
                    values.push(
                            read_mem(c, data_type_t::i32, filter_expr, filter_size, read_pos, cols_ptr, input_index));
                    break;
                case instruction_t::MEM_I8:
                    values.push(
                            read_mem(c, data_type_t::i64, filter_expr, filter_size, read_pos, cols_ptr, input_index));
                    break;
                case instruction_t::MEM_F4:
                    values.push(
                            read_mem(c, data_type_t::f32, filter_expr, filter_size, read_pos, cols_ptr, input_index));
                    break;
                case instruction_t::MEM_F8:
                    values.push(
                            read_mem(c, data_type_t::f64, filter_expr, filter_size, read_pos, cols_ptr, input_index));
                    break;
                case instruction_t::IMM_I1:
                    values.push(read_imm(c, data_type_t::i8, filter_expr, filter_size, read_pos));
                    break;
                case instruction_t::IMM_I2:
                    values.push(read_imm(c, data_type_t::i16, filter_expr, filter_size, read_pos));
                    break;
                case instruction_t::IMM_I4:
                    values.push(read_imm(c, data_type_t::i32, filter_expr, filter_size, read_pos));
                    break;
                case instruction_t::IMM_I8:
                    values.push(read_imm(c, data_type_t::i64, filter_expr, filter_size, read_pos));
                    break;
                case instruction_t::IMM_F4:
                    values.push(read_imm(c, data_type_t::f32, filter_expr, filter_size, read_pos));
                    break;
                case instruction_t::IMM_F8:
                    values.push(read_imm(c, data_type_t::f64, filter_expr, filter_size, read_pos));
                    break;
                case instruction_t::NEG:
                    values.push(neg(c, get_argument(c, values), null_check));
                    break;
                case instruction_t::NOT:
                    values.push(bin_not(c, get_argument(c, values)));
                    break;
                default:
                    emit_bin_op(c, ic, values, null_check);
                    break;
            }
        }
    }
}

namespace questdb::avx2 {
    using namespace asmjit::x86;

    data_type_t mask_type(data_type_t type) {
        switch (type) {
            case data_type_t::f32:
                return data_type_t::i32;
            case data_type_t::f64:
                return data_type_t::i64;
            default:
                return type;
        }
    }

    inline static void unrolled_loop2(Compiler &c,
                                      const Gp &bits,
                                      const Gp &rows_ptr,
                                      const Gp &input,
                                      const Gp &output,
                                      const Gp &rows_id_offset,
                                      int32_t step) {
        Gp offset = c.newInt64();
        for (int32_t i = 0; i < step; ++i) {
            c.lea(offset, asmjit::x86::ptr(input, rows_id_offset, 0, i, 0));
            c.mov(qword_ptr(rows_ptr, output, 3), offset);
            c.mov(offset, bits);
            c.shr(offset, i);
            c.and_(offset, 1);
            c.add(output, offset);
        }
    }

    jit_value_t
    read_vars_mem(Compiler &c, data_type_t type, const uint8_t *istream, size_t size, uint32_t &pos, const Gp &vars_ptr) {
        auto value = x86::read_vars_mem(c, type, istream, size, pos, vars_ptr);
        Mem mem = value.op().as<Mem>();
        Ymm val = c.newYmm();
        switch (type) {
            case data_type_t::i8: {
                c.vpbroadcastb(val, mem);
            }
            break;
            case data_type_t::i16: {
                c.vpbroadcastw(val, mem);
            }
            break;
            case data_type_t::i32: {
                c.vpbroadcastd(val, mem);
            }
            break;
            case data_type_t::i64: {
                c.vpbroadcastq(val, mem);
            }
            break;
            case data_type_t::f32: {
                c.vbroadcastss(val, mem);
            }
            break;
            case data_type_t::f64: {
                c.vbroadcastsd(val, mem);
            }
            break;
            default:
                __builtin_unreachable();
        }
        return {val, type, data_kind_t::kConst};
    }

    jit_value_t
    read_mem(Compiler &c, data_type_t type, const uint8_t *istream, size_t size, uint32_t &pos, const Gp &cols_ptr,
             const Gp &input_index) {

        auto column_idx = static_cast<int32_t>(read<int64_t>(istream, size, pos));
        Gp column_address = c.newInt64();
        c.mov(column_address, ptr(cols_ptr, 8 * column_idx, 8));

        uint32_t shift = type_shift(type);

        Mem m = ymmword_ptr(column_address, input_index, shift);
        Ymm row_data = c.newYmm();
        switch (type) {
            case data_type_t::i8:
            case data_type_t::i16:
            case data_type_t::i32:
            case data_type_t::i64:
                c.vmovdqu(row_data, m);
                break;
            case data_type_t::f32:
                c.vmovups(row_data, m);
                break;
            case data_type_t::f64:
                c.vmovupd(row_data, m);
                break;
            default:
                __builtin_unreachable();
        }
        return {row_data, type, data_kind_t::kMemory};
    }

    jit_value_t read_imm(Compiler &c, data_type_t type, const uint8_t *istream, size_t size, uint32_t &pos) {
        const auto scope = ConstPool::kScopeLocal;
        Ymm val = c.newYmm("imm_value");
        switch (type) {
            case data_type_t::i8: {
                auto value = static_cast<int8_t>(read<int64_t>(istream, size, pos));
                Mem mem = c.newConst(scope, &value, 1);
                c.vpbroadcastb(val, mem);
            }
                break;
            case data_type_t::i16: {
                auto value = static_cast<int16_t>(read<int64_t>(istream, size, pos));
                Mem mem = c.newConst(scope, &value, 2);
                c.vpbroadcastw(val, mem);
            }
                break;
            case data_type_t::i32: {
                auto value = static_cast<int32_t>(read<int64_t>(istream, size, pos));
                Mem mem = c.newConst(scope, &value, 4);
                c.vpbroadcastd(val, mem);
            }
                break;
            case data_type_t::i64: {
                auto value = read<int64_t>(istream, size, pos);
                Mem mem = c.newConst(scope, &value, 8);
                c.vpbroadcastq(val, mem);
            }
                break;
            case data_type_t::f32: {
                auto value = read<double>(istream, size, pos);
                Mem mem = c.newFloatConst(scope, static_cast<float>(value));
                c.vbroadcastss(val, mem);
            }
                break;
            case data_type_t::f64: {
                auto value = read<double>(istream, size, pos);
                Mem mem = c.newDoubleConst(scope, value);
                c.vbroadcastsd(val, mem);
            }
                break;
            default:
                __builtin_unreachable();
        }
        return {val, type, data_kind_t::kConst};
    }

    jit_value_t neg(Compiler &c, const jit_value_t &lhs, bool null_check) {
        auto dt = lhs.dtype();
        auto dk = lhs.dkind();
        return {neg(c, dt, lhs.ymm(), null_check), dt, dk};
    }

    jit_value_t bin_not(Compiler &c, const jit_value_t &lhs) {
        auto dt = lhs.dtype();
        auto dk = lhs.dkind();
        return {mask_not(c, lhs.ymm()), dt, dk};
    }

    jit_value_t bin_and(Compiler &c, const jit_value_t &lhs, const jit_value_t &rhs) {
        auto dt = lhs.dtype();
        auto dk = (lhs.dkind() == data_kind_t::kConst && rhs.dkind() == data_kind_t::kConst) ? data_kind_t::kConst
                                                                                             : data_kind_t::kMemory;
        return {mask_and(c, lhs.ymm(), rhs.ymm()), dt, dk};
    }

    jit_value_t bin_or(Compiler &c, const jit_value_t &lhs, const jit_value_t &rhs) {
        auto dt = lhs.dtype();
        auto dk = (lhs.dkind() == data_kind_t::kConst && rhs.dkind() == data_kind_t::kConst) ? data_kind_t::kConst
                                                                                             : data_kind_t::kMemory;
        return {mask_or(c, lhs.ymm(), rhs.ymm()), dt, dk};
    }

    jit_value_t cmp_eq(Compiler &c, const jit_value_t &lhs, const jit_value_t &rhs) {
        auto dt = lhs.dtype();
        auto dk = (lhs.dkind() == data_kind_t::kConst && rhs.dkind() == data_kind_t::kConst) ? data_kind_t::kConst
                                                                                             : data_kind_t::kMemory;
        return {cmp_eq(c, dt, lhs.ymm(), rhs.ymm()), data_type_t::i32, dk};
    }

    jit_value_t cmp_ne(Compiler &c, const jit_value_t &lhs, const jit_value_t &rhs) {
        auto dt = lhs.dtype();
        auto dk = (lhs.dkind() == data_kind_t::kConst && rhs.dkind() == data_kind_t::kConst) ? data_kind_t::kConst
                                                                                             : data_kind_t::kMemory;
        auto mt = mask_type(dt);
        return {cmp_ne(c, dt, lhs.ymm(), rhs.ymm()), mt, dk};
    }

    jit_value_t cmp_gt(Compiler &c, const jit_value_t &lhs, const jit_value_t &rhs, bool null_check) {
        auto dt = lhs.dtype();
        auto dk = (lhs.dkind() == data_kind_t::kConst && rhs.dkind() == data_kind_t::kConst) ? data_kind_t::kConst
                                                                                             : data_kind_t::kMemory;
        auto mt = mask_type(dt);
        return {cmp_gt(c, dt, lhs.ymm(), rhs.ymm(), null_check), mt, dk};
    }

    jit_value_t cmp_ge(Compiler &c, const jit_value_t &lhs, const jit_value_t &rhs, bool null_check) {
        auto dt = lhs.dtype();
        auto dk = (lhs.dkind() == data_kind_t::kConst && rhs.dkind() == data_kind_t::kConst) ? data_kind_t::kConst
                                                                                             : data_kind_t::kMemory;
        auto mt = mask_type(dt);
        return {cmp_ge(c, dt, lhs.ymm(), rhs.ymm(), null_check), mt, dk};
    }

    jit_value_t cmp_lt(Compiler &c, const jit_value_t &lhs, const jit_value_t &rhs, bool null_check) {
        auto dt = lhs.dtype();
        auto dk = (lhs.dkind() == data_kind_t::kConst && rhs.dkind() == data_kind_t::kConst) ? data_kind_t::kConst
                                                                                             : data_kind_t::kMemory;
        auto mt = mask_type(dt);
        return {cmp_lt(c, dt, lhs.ymm(), rhs.ymm(), null_check), mt, dk};
    }

    jit_value_t cmp_le(Compiler &c, const jit_value_t &lhs, const jit_value_t &rhs, bool null_check) {
        auto dt = lhs.dtype();
        auto dk = (lhs.dkind() == data_kind_t::kConst && rhs.dkind() == data_kind_t::kConst) ? data_kind_t::kConst
                                                                                             : data_kind_t::kMemory;
        auto mt = mask_type(dt);
        return {cmp_le(c, dt, lhs.ymm(), rhs.ymm(), null_check), mt, dk};
    }

    jit_value_t add(Compiler &c, const jit_value_t &lhs, const jit_value_t &rhs, bool null_check) {
        auto dt = lhs.dtype();
        auto dk = (lhs.dkind() == data_kind_t::kConst && rhs.dkind() == data_kind_t::kConst) ? data_kind_t::kConst
                                                                                             : data_kind_t::kMemory;
        return {add(c, dt, lhs.ymm(), rhs.ymm(), null_check), dt, dk};
    }

    jit_value_t sub(Compiler &c, const jit_value_t &lhs, const jit_value_t &rhs, bool null_check) {
        auto dt = lhs.dtype();
        auto dk = (lhs.dkind() == data_kind_t::kConst && rhs.dkind() == data_kind_t::kConst) ? data_kind_t::kConst
                                                                                             : data_kind_t::kMemory;
        return {sub(c, dt, lhs.ymm(), rhs.ymm(), null_check), dt, dk};
    }

    jit_value_t mul(Compiler &c, const jit_value_t &lhs, const jit_value_t &rhs, bool null_check) {
        auto dt = lhs.dtype();
        auto dk = (lhs.dkind() == data_kind_t::kConst && rhs.dkind() == data_kind_t::kConst) ? data_kind_t::kConst
                                                                                             : data_kind_t::kMemory;
        return {mul(c, dt, lhs.ymm(), rhs.ymm(), null_check), dt, dk};
    }

    jit_value_t div(Compiler &c, const jit_value_t &lhs, const jit_value_t &rhs, bool null_check) {
        auto dt = lhs.dtype();
        auto dk = (lhs.dkind() == data_kind_t::kConst && rhs.dkind() == data_kind_t::kConst) ? data_kind_t::kConst
                                                                                             : data_kind_t::kMemory;
        return {div(c, dt, lhs.ymm(), rhs.ymm(), null_check), dt, dk};
    }

    inline std::pair<jit_value_t, jit_value_t>
    convert(Compiler &c, const jit_value_t &lhs, const jit_value_t &rhs, bool null_check) {
        // data_type_t::i32 -> data_type_t::f32
        // data_type_t::i64 -> data_type_t::f64
        switch (lhs.dtype()) {
            case data_type_t::i32:
                switch (rhs.dtype()) {
                    case data_type_t::f32:
                        return std::make_pair(
                                jit_value_t(cvt_itof(c, lhs.ymm(), null_check), data_type_t::f32, lhs.dkind()), rhs);
                    default:
                        break;
                }
                break;
            case data_type_t::i64:
                switch (rhs.dtype()) {
                    case data_type_t::f64:
                        return std::make_pair(
                                jit_value_t(cvt_ltod(c, lhs.ymm(), null_check), data_type_t::f64, lhs.dkind()), rhs);
                    default:
                        break;
                }
                break;
            case data_type_t::f32:
                switch (rhs.dtype()) {
                    case data_type_t::i32:
                        return std::make_pair(lhs, jit_value_t(cvt_itof(c, rhs.ymm(), null_check), data_type_t::f32,
                                                               rhs.dkind()));
                    default:
                        break;
                }
                break;
            case data_type_t::f64:
                switch (rhs.dtype()) {
                    case data_type_t::i64:
                        return std::make_pair(lhs, jit_value_t(cvt_ltod(c, rhs.ymm(), null_check), data_type_t::f64,
                                                               rhs.dkind()));
                    default:
                        break;
                }
                break;
            default:
                break;
        }
        return std::make_pair(lhs, rhs);
    }

    inline jit_value_t get_argument(std::stack<jit_value_t> &values) {
        auto arg = values.top();
        values.pop();
        return arg;
    }

    inline std::pair<jit_value_t, jit_value_t>
    get_arguments(Compiler &c, std::stack<jit_value_t> &values, bool ncheck) {
        auto lhs = values.top();
        values.pop();
        auto rhs = values.top();
        values.pop();
        return convert(c, lhs, rhs, ncheck);
    }

    void emit_bin_op(Compiler &c, instruction_t ic, std::stack<jit_value_t> &values, bool ncheck) {
        auto args = get_arguments(c, values, ncheck);
        switch (ic) {
            case instruction_t::AND:
                values.push(bin_and(c, args.first, args.second));
                break;
            case instruction_t::OR:
                values.push(bin_or(c, args.first, args.second));
                break;
            case instruction_t::EQ:
                values.push(cmp_eq(c, args.first, args.second));
                break;
            case instruction_t::NE:
                values.push(cmp_ne(c, args.first, args.second));
                break;
            case instruction_t::GT:
                values.push(cmp_gt(c, args.first, args.second, ncheck));
                break;
            case instruction_t::GE:
                values.push(cmp_ge(c, args.first, args.second, ncheck));
                break;
            case instruction_t::LT:
                values.push(cmp_lt(c, args.first, args.second, ncheck));
                break;
            case instruction_t::LE:
                values.push(cmp_le(c, args.first, args.second, ncheck));
                break;
            case instruction_t::ADD:
                values.push(add(c, args.first, args.second, ncheck));
                break;
            case instruction_t::SUB:
                values.push(sub(c, args.first, args.second, ncheck));
                break;
            case instruction_t::MUL:
                values.push(mul(c, args.first, args.second, ncheck));
                break;
            case instruction_t::DIV:
                values.push(div(c, args.first, args.second, ncheck));
                break;
            default:
                __builtin_unreachable();
        }
    }

    void
    emit_code(Compiler &c, const uint8_t *filter_expr, size_t filter_size, std::stack<jit_value_t> &values, bool ncheck,
              const Gp &cols_ptr, const Gp &vars_ptr, const Gp &input_index) {
        uint32_t read_pos = 0;
        while (read_pos < filter_size) {
            auto ic = static_cast<instruction_t>(read<uint8_t>(filter_expr, filter_size, read_pos));
            switch (ic) {
                case instruction_t::RET:
                    break;
                case instruction_t::VAR_I1:
                    values.push(
                            read_vars_mem(c, data_type_t::i8, filter_expr, filter_size, read_pos, vars_ptr));
                    break;
                case instruction_t::VAR_I2:
                    values.push(
                            read_vars_mem(c, data_type_t::i16, filter_expr, filter_size, read_pos, vars_ptr));
                    break;
                case instruction_t::VAR_I4:
                    values.push(
                            read_vars_mem(c, data_type_t::i32, filter_expr, filter_size, read_pos, vars_ptr));
                    break;
                case instruction_t::VAR_I8:
                    values.push(
                            read_vars_mem(c, data_type_t::i64, filter_expr, filter_size, read_pos, vars_ptr));
                    break;
                case instruction_t::VAR_F4:
                    values.push(
                            read_vars_mem(c, data_type_t::f32, filter_expr, filter_size, read_pos, vars_ptr));
                    break;
                case instruction_t::VAR_F8:
                    values.push(
                            read_vars_mem(c, data_type_t::f64, filter_expr, filter_size, read_pos, vars_ptr));
                    break;
                case instruction_t::MEM_I1:
                    values.push(
                            read_mem(c, data_type_t::i8, filter_expr, filter_size, read_pos, cols_ptr, input_index));
                    break;
                case instruction_t::MEM_I2:
                    values.push(
                            read_mem(c, data_type_t::i16, filter_expr, filter_size, read_pos, cols_ptr, input_index));
                    break;
                case instruction_t::MEM_I4:
                    values.push(
                            read_mem(c, data_type_t::i32, filter_expr, filter_size, read_pos, cols_ptr, input_index));
                    break;
                case instruction_t::MEM_I8:
                    values.push(
                            read_mem(c, data_type_t::i64, filter_expr, filter_size, read_pos, cols_ptr, input_index));
                    break;
                case instruction_t::MEM_F4:
                    values.push(
                            read_mem(c, data_type_t::f32, filter_expr, filter_size, read_pos, cols_ptr, input_index));
                    break;
                case instruction_t::MEM_F8:
                    values.push(
                            read_mem(c, data_type_t::f64, filter_expr, filter_size, read_pos, cols_ptr, input_index));
                    break;
                case instruction_t::IMM_I1:
                    values.push(read_imm(c, data_type_t::i8, filter_expr, filter_size, read_pos));
                    break;
                case instruction_t::IMM_I2:
                    values.push(read_imm(c, data_type_t::i16, filter_expr, filter_size, read_pos));
                    break;
                case instruction_t::IMM_I4:
                    values.push(read_imm(c, data_type_t::i32, filter_expr, filter_size, read_pos));
                    break;
                case instruction_t::IMM_I8:
                    values.push(read_imm(c, data_type_t::i64, filter_expr, filter_size, read_pos));
                    break;
                case instruction_t::IMM_F4:
                    values.push(read_imm(c, data_type_t::f32, filter_expr, filter_size, read_pos));
                    break;
                case instruction_t::IMM_F8:
                    values.push(read_imm(c, data_type_t::f64, filter_expr, filter_size, read_pos));
                    break;
                case instruction_t::NEG:
                    values.push(neg(c, get_argument(values), ncheck));
                    break;
                case instruction_t::NOT:
                    values.push(bin_not(c, get_argument(values)));
                    break;
                default:
                    emit_bin_op(c, ic, values, ncheck);
                    break;
            }
        }
    }
}

using CompiledFn = int64_t (*)(int64_t *cols, int64_t cols_count, int64_t *vars, int64_t vars_count, int64_t *rows, int64_t rows_count,
                               int64_t rows_start_offset);

struct JitCompiler {
    JitCompiler(x86::Compiler &cc)
            : c(cc) {};

    void compile(const uint8_t *filter_expr, int64_t filter_size, uint32_t options) {
        auto features = CpuInfo::host().features().as<x86::Features>();
        enum type_size : uint32_t {
            scalar = 0,
            single_size = 1,
            mixed_size = 3,
        };

        uint32_t type_size = (options >> 1) & 3; // 0 - 1B, 1 - 2B, 2 - 4B, 3 - 8B
        uint32_t exec_hint = (options >> 3) & 3; // 0 - scalar, 1 - single size type, 2 - mixed size types, ...
        bool null_check    = (options >> 5) & 1; // 1 - with null check

        if (exec_hint == single_size && features.hasAVX2()) {
            auto step = 256 / ((1 << type_size) * 8);
            c.func()->frame().setAvxEnabled();
            avx2_loop2(filter_expr, filter_size, step, null_check);
        } else {
            scalar_loop(filter_expr, filter_size, null_check);
        }
    };

    void scalar_tail2(const uint8_t *filter_expr, size_t filter_size, bool null_check, const x86::Gp &stop) {

        Label l_loop = c.newLabel();
        Label l_exit = c.newLabel();

        c.cmp(input_index, stop);
        c.jge(l_exit);

        c.bind(l_loop);

        questdb::x86::emit_code(c, filter_expr, filter_size, registers, null_check, cols_ptr, vars_ptr, input_index);

        auto mask = registers.top();
        registers.pop();

        x86::Gp adjusted_id = c.newInt64("input_index_+_rows_id_start_offset");
        c.lea(adjusted_id, ptr(input_index, rows_id_start_offset)); // input_index + rows_id_start_offset
        c.mov(qword_ptr(rows_ptr, output_index, 3), adjusted_id);

        c.and_(mask.gp(), 1);
        c.add(output_index, mask.gp().r64());

        c.add(input_index, 1);
        c.cmp(input_index, stop);
        c.jl(l_loop); // input_index < stop
        c.bind(l_exit);
    }

    void scalar_loop(const uint8_t *filter_expr, size_t filter_size, bool null_check) {
        scalar_tail2(filter_expr, filter_size, null_check, rows_size);
        c.ret(output_index);
    }

    void avx2_loop2(const uint8_t *filter_expr, size_t filter_size, uint32_t step, bool null_check) {
        using namespace asmjit::x86;

        Label l_loop = c.newLabel();
        Label l_exit = c.newLabel();

        c.xor_(input_index, input_index); //input_index = 0

        Gp stop = c.newGpq();
        c.mov(stop, rows_size);
        c.sub(stop, step - 1); // stop = rows_size - step + 1

        c.cmp(input_index, stop);
        c.jge(l_exit);

        c.bind(l_loop);

        questdb::avx2::emit_code(c, filter_expr, filter_size, registers, null_check, cols_ptr, vars_ptr, input_index);

        auto mask = registers.top();
        registers.pop();

        Gp bits = questdb::avx2::to_bits(c, mask.ymm(), step);
        questdb::avx2::unrolled_loop2(c, bits.r64(), rows_ptr, input_index, output_index, rows_id_start_offset, step);

        c.add(input_index, step); // index += step
        c.cmp(input_index, stop);
        c.jl(l_loop); // index < stop
        c.bind(l_exit);

        scalar_tail2(filter_expr, filter_size, null_check, rows_size);
        c.ret(output_index);
    }

    void begin_fn() {
        c.addFunc(FuncSignatureT<int64_t, int64_t *, int64_t, int64_t *, int64_t, int64_t *, int64_t, int64_t>(CallConv::kIdHost));
        cols_ptr = c.newIntPtr("cols_ptr");
        cols_size = c.newInt64("cols_size");

        c.setArg(0, cols_ptr);
        c.setArg(1, cols_size);

        vars_ptr = c.newIntPtr("vars_ptr");
        vars_size = c.newInt64("vars_size");

        c.setArg(2, vars_ptr);
        c.setArg(3, vars_size);

        rows_ptr = c.newIntPtr("rows_ptr");
        rows_size = c.newInt64("rows_size");

        c.setArg(4, rows_ptr);
        c.setArg(5, rows_size);

        rows_id_start_offset = c.newInt64("rows_id_start_offset");
        c.setArg(6, rows_id_start_offset);

        input_index = c.newInt64("input_index");
        c.mov(input_index, 0);

        output_index = c.newInt64("output_index");
        c.mov(output_index, 0);
    }

    void end_fn() {
        c.endFunc();
    }

    x86::Compiler &c;

    std::stack<jit_value_t> registers;

    x86::Gp cols_ptr;
    x86::Gp cols_size;
    x86::Gp vars_ptr;
    x86::Gp vars_size;
    x86::Gp rows_ptr;
    x86::Gp rows_size;
    x86::Gp input_index;
    x86::Gp output_index;
    x86::Gp rows_id_start_offset;
};

void fillJitErrorObject(JNIEnv *e, jobject error, uint32_t code, const char *msg) {

    if (!msg) {
        return;
    }

    jclass errorClass = e->GetObjectClass(error);
    if (errorClass) {
        jfieldID fieldError = e->GetFieldID(errorClass, "errorCode", "I");
        if (fieldError) {
            e->SetIntField(error, fieldError, static_cast<jint>(code));
        }
        jmethodID methodPut = e->GetMethodID(errorClass, "put", "(B)V");
        if (methodPut) {
            for (const char *c = msg; *c; ++c) {
                e->CallVoidMethod(error, methodPut, *c);
            }
        }
    }
}

JNIEXPORT jlong JNICALL
Java_io_questdb_jit_FiltersCompiler_compileFunction(JNIEnv *e,
                                                    jclass cl,
                                                    jlong filterAddress,
                                                    jlong filterSize,
                                                    jint options,
                                                    jobject error) {
#ifndef __aarch64__

    if (filterAddress <= 0 || filterSize <= 0) {
        fillJitErrorObject(e, error, ErrorCode::kErrorInvalidArgument, "Invalid argument passed");
        return 0;
    }

    CodeHolder code;
    code.init(gGlobalContext.rt.environment());
    FileLogger logger(stdout);
    bool debug = options & 1;
    if (debug) {
        logger.addFlags(FormatOptions::kFlagRegCasts |
                        FormatOptions::kFlagExplainImms |
                        FormatOptions::kFlagAnnotations);
        code.setLogger(&logger);
    }
    code.setErrorHandler(&gGlobalContext.errorHandler);
    x86::Compiler c(&code);
    JitCompiler compiler(c);

    CompiledFn fn;
    try {
        compiler.begin_fn();
        compiler.compile(reinterpret_cast<uint8_t *>(filterAddress), filterSize, options);
        compiler.end_fn();
        c.finalize();
        gGlobalContext.rt.add(&fn, &code);
    } catch (JitException &ex) {
        fillJitErrorObject(e, error, ex.err, ex.what());
        return 0;
    }

    return reinterpret_cast<jlong>(fn);
#else
    return 0;
#endif

}

JNIEXPORT void JNICALL
Java_io_questdb_jit_FiltersCompiler_freeFunction(JNIEnv *e, jclass cl, jlong fnAddress) {
#ifndef __aarch64__
    auto fn = reinterpret_cast<void *>(fnAddress);
    gGlobalContext.rt.release(fn);
#endif
}

JNIEXPORT jlong JNICALL Java_io_questdb_jit_FiltersCompiler_callFunction(JNIEnv *e,
                                                                         jclass cl,
                                                                         jlong fnAddress,
                                                                         jlong colsAddress,
                                                                         jlong colsSize,
                                                                         jlong varsAddress,
                                                                         jlong varsSize,
                                                                         jlong rowsAddress,
                                                                         jlong rowsSize,
                                                                         jlong rowsStartOffset) {
#ifndef __aarch64__
    auto fn = reinterpret_cast<CompiledFn>(fnAddress);
    return fn(reinterpret_cast<int64_t *>(colsAddress),
              colsSize,
              reinterpret_cast<int64_t *>(varsAddress),
              varsSize,
              reinterpret_cast<int64_t *>(rowsAddress),
              rowsSize,
              rowsStartOffset);
#else
    return 0;
#endif
}
