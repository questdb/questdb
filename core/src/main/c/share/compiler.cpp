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
#include <cassert>

using namespace asmjit;

struct JitErrorHandler : public asmjit::ErrorHandler {
    Error _error = kErrorOk;

    void handleError(asmjit::Error err, const char *msg, asmjit::BaseEmitter * /*origin*/) override {
        _error = err;
        fprintf(stderr, "ERROR: %s\n", msg);
    }
};

struct JitGlobalContext {
    //rt allocator is thread-safe
    JitRuntime rt;
    JitErrorHandler errorHandler;
};

static JitGlobalContext gGlobalContext;

namespace questdb::x86 {
    using namespace asmjit::x86;

    jit_value_t
    read_mem(Compiler &c, data_type_t type, const uint8_t *istream, size_t size, uint32_t &pos, const Gp &cols_ptr,
             const Gp &input_index) {

        auto column_idx = static_cast<int32_t>(read<int64_t>(istream, size, pos));
        Gp column_address = c.newInt64("column_address");
        c.mov(column_address, ptr(cols_ptr, 8 * column_idx, 8));

        switch (type) {
            case i8: {
                Gp row_data = c.newGpd("i8_mem");
                c.movsx(row_data, Mem(column_address, input_index, 0, 0, 1));
                return {row_data, type, kMemory};
            }
            case i16: {
                Gp row_data = c.newGpd("i16_mem");
                c.movsx(row_data, Mem(column_address, input_index, 1, 0, 2));
                return {row_data, type, kMemory};
            }
            case i32: {
                Gp row_data = c.newGpd("i32_mem");
                c.mov(row_data, Mem(column_address, input_index, 2, 0, 4));
                return {row_data, type, kMemory};
            }
            case i64: {
                Gp row_data = c.newGpq("i64_mem");
                c.mov(row_data, Mem(column_address, input_index, 3, 0, 8));
                return {row_data, type, kMemory};
            }
            case f32: {
                Xmm row_data = c.newXmmSs("f32_mem");
                c.vmovss(row_data, Mem(column_address, input_index, 2, 0, 4));
                return {row_data, type, kMemory};
            }
            case f64: {
                Xmm row_data = c.newXmmSd("f64_mem");
                c.vmovsd(row_data, Mem(column_address, input_index, 3, 0, 8));
                return {row_data, type, kMemory};
            }
        }
    }

    jit_value_t read_imm(Compiler &c, data_type_t type, const uint8_t *istream, size_t size, uint32_t &pos) {
        switch (type) {
            case i8:
            case i16:
            case i32: {
                auto value = read<int64_t>(istream, size, pos);
                Gp reg = c.newGpd("i32_imm %d", value);
                c.mov(reg, value); //todo: check & cast value ?
                return {reg, type, kConst};
            }
            case i64: {
                auto value = read<int64_t>(istream, size, pos);
                Gp reg = c.newGpq("i64_imm %d", value);
                c.movabs(reg, value); //todo: check & cast value ?
                return {reg, type, kConst};
            }
            case f32: {
                auto value = read<double>(istream, size, pos);
                Xmm reg = c.newXmmSs("f32_imm %f", value);
                Mem mem = c.newFloatConst(ConstPool::kScopeLocal, (float) value);
                c.movss(reg, mem);
                return {reg, type, kConst};
            }
            case f64: {
                auto value = read<double>(istream, size, pos);
                Xmm reg = c.newXmmSd("f64_imm %f", value);
                Mem mem = c.newDoubleConst(ConstPool::kScopeLocal, value);
                c.movsd(reg, mem);
                return {reg, type, kConst};
            }
        }
    }

    jit_value_t neg(Compiler &c, const jit_value_t &lhs, bool null_check) {
        auto dt = lhs.dtype();
        auto dk = lhs.dkind();
        switch (dt) {
            case i8:
            case i16:
            case i32:
                return {int32_neg(c, lhs.gp().r32(), null_check), dt, dk};
            case i64:
                return {int64_neg(c, lhs.gp(), null_check), dt, dk};
            case f32:
                return {float_neg(c, lhs.xmm()), dt, dk};
            case f64:
                return {double_neg(c, lhs.xmm()), dt, dk};
        }
    }

    jit_value_t bin_not(Compiler &c, const jit_value_t &lhs) {
        auto dt = lhs.dtype();
        auto dk = lhs.dkind();
        return {int32_not(c, lhs.gp().r32()), dt, dk};
    }

    jit_value_t bin_and(Compiler &c, const jit_value_t &lhs, const jit_value_t &rhs) {
        auto dt = lhs.dtype();
        auto dk = (lhs.dkind() == kConst && rhs.dkind() == kConst) ? kConst : kMemory;
        return {int32_and(c, lhs.gp().r32(), rhs.gp().r32()), dt, dk};
    }

    jit_value_t bin_or(Compiler &c, const jit_value_t &lhs, const jit_value_t &rhs) {
        auto dt = lhs.dtype();
        auto dk = (lhs.dkind() == kConst && rhs.dkind() == kConst) ? kConst : kMemory;
        return {int32_or(c, lhs.gp().r32(), rhs.gp().r32()), dt, dk};
    }

    jit_value_t cmp_eq(Compiler &c, const jit_value_t &lhs, const jit_value_t &rhs) {
        auto dt = lhs.dtype();
        auto dk = (lhs.dkind() == kConst && rhs.dkind() == kConst) ? kConst : kMemory;
        switch (dt) {
            case i8:
            case i16:
            case i32:
                return {int32_eq(c, lhs.gp().r32(), rhs.gp().r32()), i32, dk};
            case i64:
                return {int64_eq(c, lhs.gp(), rhs.gp()), i32, dk};
            case f32:
                return {float_eq_delta(c, lhs.xmm(), rhs.xmm(), FLOAT_DELTA), i32, dk};
            case f64:
                return {double_eq_delta(c, lhs.xmm(), rhs.xmm(), DOUBLE_DELTA), i32, dk};
        }
    }

    jit_value_t cmp_ne(Compiler &c, const jit_value_t &lhs, const jit_value_t &rhs) {
        auto dt = lhs.dtype();
        auto dk = (lhs.dkind() == kConst && rhs.dkind() == kConst) ? kConst : kMemory;
        switch (dt) {
            case i8:
            case i16:
            case i32:
                return {int32_ne(c, lhs.gp().r32(), rhs.gp().r32()), i32, dk};
            case i64:
                return {int64_ne(c, lhs.gp(), rhs.gp()), i32, dk};
            case f32:
                return {float_ne_delta(c, lhs.xmm(), rhs.xmm(), FLOAT_DELTA), i32, dk};
            case f64:
                return {double_ne_delta(c, lhs.xmm(), rhs.xmm(), DOUBLE_DELTA), i32, dk};
        }
    }

    jit_value_t cmp_gt(Compiler &c, const jit_value_t &lhs, const jit_value_t &rhs, bool null_check) {
        auto dt = lhs.dtype();
        auto dk = (lhs.dkind() == kConst && rhs.dkind() == kConst) ? kConst : kMemory;
        switch (dt) {
            case i8:
            case i16:
            case i32:
                return {int32_gt(c, lhs.gp().r32(), rhs.gp().r32(), null_check), i32, dk};
            case i64:
                return {int64_gt(c, lhs.gp(), rhs.gp(), null_check), i32, dk};
            case f32:
                return {float_gt(c, lhs.xmm(), rhs.xmm()), i32, dk};
            case f64:
                return {double_gt(c, lhs.xmm(), rhs.xmm()), i32, dk};
        }
    }

    jit_value_t cmp_ge(Compiler &c, const jit_value_t &lhs, const jit_value_t &rhs, bool null_check) {
        auto dt = lhs.dtype();
        auto dk = (lhs.dkind() == kConst && rhs.dkind() == kConst) ? kConst : kMemory;
        switch (dt) {
            case i8:
            case i16:
            case i32:
                return {int32_ge(c, lhs.gp().r32(), rhs.gp().r32(), null_check), i32, dk};
            case i64:
                return {int64_ge(c, lhs.gp(), rhs.gp(), null_check), i32, dk};
            case f32:
                return {float_ge(c, lhs.xmm(), rhs.xmm()), i32, dk};
            case f64:
                return {double_ge(c, lhs.xmm(), rhs.xmm()), i32, dk};
        }
    }

    jit_value_t cmp_lt(Compiler &c, const jit_value_t &lhs, const jit_value_t &rhs, bool null_check) {
        auto dt = lhs.dtype();
        auto dk = (lhs.dkind() == kConst && rhs.dkind() == kConst) ? kConst : kMemory;
        switch (dt) {
            case i8:
            case i16:
            case i32:
                return {int32_lt(c, lhs.gp().r32(), rhs.gp().r32(), null_check), i32, dk};
            case i64:
                return {int64_lt(c, lhs.gp(), rhs.gp(), null_check), i32, dk};
            case f32:
                return {float_lt(c, lhs.xmm(), rhs.xmm()), i32, dk};
            case f64:
                return {double_lt(c, lhs.xmm(), rhs.xmm()), i32, dk};
        }
    }

    jit_value_t cmp_le(Compiler &c, const jit_value_t &lhs, const jit_value_t &rhs, bool null_check) {
        auto dt = lhs.dtype();
        auto dk = (lhs.dkind() == kConst && rhs.dkind() == kConst) ? kConst : kMemory;
        switch (dt) {
            case i8:
            case i16:
            case i32:
                return {int32_le(c, lhs.gp().r32(), rhs.gp().r32(), null_check), i32, dk};
            case i64:
                return {int64_le(c, lhs.gp(), rhs.gp(), null_check), i32, dk};
            case f32:
                return {float_le(c, lhs.xmm(), rhs.xmm()), i32, dk};
            case f64:
                return {double_le(c, lhs.xmm(), rhs.xmm()), i32, dk};
        }
    }

    jit_value_t add(Compiler &c, const jit_value_t &lhs, const jit_value_t &rhs, bool null_check) {
        auto dt = lhs.dtype();
        auto dk = (lhs.dkind() == kConst && rhs.dkind() == kConst) ? kConst : kMemory;
        switch (dt) {
            case i8:
            case i16:
            case i32:
                return {int32_add(c, lhs.gp().r32(), rhs.gp().r32(), null_check), dt, dk};
            case i64:
                return {int64_add(c, lhs.gp(), rhs.gp(), null_check), dt, dk};
            case f32:
                return {float_add(c, lhs.xmm(), rhs.xmm()), dt, dk};
            case f64:
                return {double_add(c, lhs.xmm(), rhs.xmm()), dt, dk};
        }
    }

    jit_value_t sub(Compiler &c, const jit_value_t &lhs, const jit_value_t &rhs, bool null_check) {
        auto dt = lhs.dtype();
        auto dk = (lhs.dkind() == kConst && rhs.dkind() == kConst) ? kConst : kMemory;
        switch (dt) {
            case i8:
            case i16:
            case i32:
                return {int32_sub(c, lhs.gp().r32(), rhs.gp().r32(), null_check), dt, dk};
            case i64:
                return {int64_sub(c, lhs.gp(), rhs.gp(), null_check), dt, dk};
            case f32:
                return {float_sub(c, lhs.xmm(), rhs.xmm()), dt, dk};
            case f64:
                return {double_sub(c, lhs.xmm(), rhs.xmm()), dt, dk};
        }
    }

    jit_value_t mul(Compiler &c, const jit_value_t &lhs, const jit_value_t &rhs, bool null_check) {
        auto dt = lhs.dtype();
        auto dk = (lhs.dkind() == kConst && rhs.dkind() == kConst) ? kConst : kMemory;
        switch (dt) {
            case i8:
            case i16:
            case i32:
                return {int32_mul(c, lhs.gp().r32(), rhs.gp().r32(), null_check), dt, dk};
            case i64:
                return {int64_mul(c, lhs.gp(), rhs.gp(), null_check), dt, dk};
            case f32:
                return {float_mul(c, lhs.xmm(), rhs.xmm()), dt, dk};
            case f64:
                return {double_mul(c, lhs.xmm(), rhs.xmm()), dt, dk};
        }
    }

    jit_value_t div(Compiler &c, const jit_value_t &lhs, const jit_value_t &rhs, bool null_check) {
        auto dt = lhs.dtype();
        auto dk = (lhs.dkind() == kConst && rhs.dkind() == kConst) ? kConst : kMemory;
        switch (dt) {
            case i8:
            case i16:
            case i32:
                return {int32_div(c, lhs.gp().r32(), rhs.gp().r32(), null_check), dt, dk};
            case i64:
                return {int64_div(c, lhs.gp(), rhs.gp(), null_check), dt, dk};
            case f32:
                return {float_div(c, lhs.xmm(), rhs.xmm()), dt, dk};
            case f64:
                return {double_div(c, lhs.xmm(), rhs.xmm()), dt, dk};
        }
    }

    inline bool cvt_null_check(data_type_t type) {
        return !(type == i8 || type == i16);
    }

    inline std::pair<jit_value_t, jit_value_t>
    convert(Compiler &c, const jit_value_t &lhs, const jit_value_t &rhs, bool null_check) {
        c.comment("convert");
        switch (lhs.dtype()) {
            case i8:
            case i16:
            case i32:
                switch (rhs.dtype()) {
                    case i8:
                    case i16:
                    case i32:
                        return std::make_pair(lhs, rhs);
                    case i64:
                        return std::make_pair(
                                jit_value_t(int32_to_int64(c, lhs.gp().r32(), null_check && cvt_null_check(lhs.dtype())), i64,
                                            lhs.dkind()), rhs);
                    case f32:
                        return std::make_pair(
                                jit_value_t(int32_to_float(c, lhs.gp().r32(), null_check && cvt_null_check(lhs.dtype())), f32,
                                            lhs.dkind()), rhs);
                    case f64:
                        return std::make_pair(
                                jit_value_t(int32_to_double(c, lhs.gp().r32(), null_check && cvt_null_check(lhs.dtype())), f64,
                                            lhs.dkind()), rhs);
                }
                break;
            case i64:
                switch (rhs.dtype()) {
                    case i8:
                    case i16:
                    case i32:
                        return std::make_pair(lhs,
                                              jit_value_t(
                                                      int32_to_int64(c, rhs.gp().r32(), null_check && cvt_null_check(rhs.dtype())),
                                                      i64, rhs.dkind()));
                    case i64:
                        return std::make_pair(lhs, rhs);
                    case f32:
                        return std::make_pair(
                                jit_value_t(int64_to_double(c, lhs.gp().r64(), null_check), f64, lhs.dkind()),
                                jit_value_t(float_to_double(c, rhs.xmm()), f64, rhs.dkind()));
                    case f64:
                        return std::make_pair(
                                jit_value_t(int64_to_double(c, lhs.gp(), null_check), f64, lhs.dkind()),
                                rhs);
                }
                break;
            case f32:
                switch (rhs.dtype()) {
                    case i8:
                    case i16:
                    case i32:
                        return std::make_pair(lhs,
                                              jit_value_t(
                                                      int32_to_float(c, rhs.gp().r32(), null_check && cvt_null_check(rhs.dtype())),
                                                      f32, rhs.dkind()));
                    case i64:
                        return std::make_pair(jit_value_t(float_to_double(c, lhs.xmm()), f64, lhs.dkind()),
                                              jit_value_t(int64_to_double(c, rhs.gp(), null_check), f64,
                                                          rhs.dkind()));
                    case f32:
                        return std::make_pair(lhs, rhs);
                    case f64:
                        return std::make_pair(jit_value_t(float_to_double(c, lhs.xmm()), f64, lhs.dkind()), rhs);
                }
                break;
            case f64:
                switch (rhs.dtype()) {
                    case i8:
                    case i16:
                    case i32:
                        return std::make_pair(lhs,
                                              jit_value_t(
                                                      int32_to_double(c, rhs.gp().r32(), null_check && cvt_null_check(rhs.dtype())),
                                                      f64, rhs.dkind()));
                    case i64:
                        return std::make_pair(lhs, jit_value_t(int64_to_double(c, rhs.gp(), null_check), f64,
                                                               rhs.dkind()));
                    case f32:
                        return std::make_pair(lhs,
                                              jit_value_t(float_to_double(c, rhs.xmm()),
                                                          f64,
                                                          rhs.dkind()));
                    case f64:
                        return std::make_pair(lhs, rhs);
                }
                break;
        }
    }

    inline jit_value_t get_argument(std::stack<jit_value_t> &values) {
        auto arg = values.top();
        values.pop();
        return arg;
    }

    inline std::pair<jit_value_t, jit_value_t>
    get_arguments(Compiler &c, std::stack<jit_value_t> &values, bool null_check) {
        auto lhs = values.top();
        values.pop();
        auto rhs = values.top();
        values.pop();
        return convert(c, lhs, rhs, null_check);
    }

    void emit_bin_op(Compiler &c, instruction_t ic, std::stack<jit_value_t> &values, bool null_check) {
        auto args = get_arguments(c, values, null_check);
        switch (ic) {
            case AND:
                values.push(bin_and(c, args.first, args.second));
                break;
            case OR:
                values.push(bin_or(c, args.first, args.second));
                break;
            case EQ:
                values.push(cmp_eq(c, args.first, args.second));
                break;
            case NE:
                values.push(cmp_ne(c, args.first, args.second));
                break;
            case GT:
                values.push(cmp_gt(c, args.first, args.second, null_check));
                break;
            case GE:
                values.push(cmp_ge(c, args.first, args.second, null_check));
                break;
            case LT:
                values.push(cmp_lt(c, args.first, args.second, null_check));
                break;
            case LE:
                values.push(cmp_le(c, args.first, args.second, null_check));
                break;
            case ADD:
                values.push(add(c, args.first, args.second, null_check));
                break;
            case SUB:
                values.push(sub(c, args.first, args.second, null_check));
                break;
            case MUL:
                values.push(mul(c, args.first, args.second, null_check));
                break;
            case DIV:
                values.push(div(c, args.first, args.second, null_check));
                break;
            default:
                assert(false);
                break;
        }
    }

    void
    emit_code(Compiler &c, const uint8_t *filter_expr, size_t filter_size, std::stack<jit_value_t> &values,
              bool null_check,
              const Gp &cols_ptr, const Gp &input_index) {
        uint32_t read_pos = 0;
        while (read_pos < filter_size) {
            auto ic = static_cast<instruction_t>(read<uint8_t>(filter_expr, filter_size, read_pos));
            switch (ic) {
                case RET:
                    break;
                case MEM_I1:
                    values.push(read_mem(c, i8, filter_expr, filter_size, read_pos, cols_ptr, input_index));
                    break;
                case MEM_I2:
                    values.push(read_mem(c, i16, filter_expr, filter_size, read_pos, cols_ptr, input_index));
                    break;
                case MEM_I4:
                    values.push(read_mem(c, i32, filter_expr, filter_size, read_pos, cols_ptr, input_index));
                    break;
                case MEM_I8:
                    values.push(read_mem(c, i64, filter_expr, filter_size, read_pos, cols_ptr, input_index));
                    break;
                case MEM_F4:
                    values.push(read_mem(c, f32, filter_expr, filter_size, read_pos, cols_ptr, input_index));
                    break;
                case MEM_F8:
                    values.push(read_mem(c, f64, filter_expr, filter_size, read_pos, cols_ptr, input_index));
                    break;
                case IMM_I1:
                    values.push(read_imm(c, i8, filter_expr, filter_size, read_pos));
                    break;
                case IMM_I2:
                    values.push(read_imm(c, i16, filter_expr, filter_size, read_pos));
                    break;
                case IMM_I4:
                    values.push(read_imm(c, i32, filter_expr, filter_size, read_pos));
                    break;
                case IMM_I8:
                    values.push(read_imm(c, i64, filter_expr, filter_size, read_pos));
                    break;
                case IMM_F4:
                    values.push(read_imm(c, f32, filter_expr, filter_size, read_pos));
                    break;
                case IMM_F8:
                    values.push(read_imm(c, f64, filter_expr, filter_size, read_pos));
                    break;
                case NEG:
                    values.push(neg(c, get_argument(values), null_check));
                    break;
                case NOT:
                    values.push(bin_not(c, get_argument(values)));
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

    uint32_t type_shift(data_type_t type) {
        switch (type) {
            case i8:
                return 0;
            case i16:
                return 1;
            case i32:
            case f32:
                return 2;
            case i64:
            case f64:
                return 3;
        }
    }

    data_type_t mask_type(data_type_t type) {
        switch (type) {
            case f32:
                return i32;
            case f64:
                return i64;
            default:
                return type;
        }
    }

    inline static void unrolled_loop2(Compiler &c,
                                      const Gp &bits,
                                      const Gp &rows_ptr,
                                      const Gp &input,
                                      const Gp &output, int32_t step) {
        Gp offset = c.newInt64();
        for (int32_t i = 0; i < step; ++i) {
            c.lea(offset, asmjit::x86::ptr(input, i));
            c.mov(qword_ptr(rows_ptr, output, 3), offset);
            c.mov(offset, bits);
            c.shr(offset, i);
            c.and_(offset, 1);
            c.add(output, offset);
        }
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
            case i8:
            case i16:
            case i32:
            case i64:
                c.vmovdqu(row_data, m);
                break;
            case f32:
                c.vmovups(row_data, m);
                break;
            case f64:
                c.vmovupd(row_data, m);
                break;
        }
        return {row_data, type, kMemory};
    }

    jit_value_t read_imm(Compiler &c, data_type_t type, const uint8_t *istream, size_t size, uint32_t &pos) {
        const auto scope = ConstPool::kScopeLocal;
        Ymm val = c.newYmm("imm_value");
        switch (type) {
            case i8: {
                auto value = static_cast<int8_t>(read<int64_t>(istream, size, pos));
                Mem mem = c.newConst(scope, &value, 1);
                c.vpbroadcastb(val, mem);
            }
                break;
            case i16: {
                auto value = static_cast<int16_t>(read<int64_t>(istream, size, pos));
                Mem mem = c.newConst(scope, &value, 2);
                c.vpbroadcastw(val, mem);
            }
                break;
            case i32: {
                auto value = static_cast<int32_t>(read<int64_t>(istream, size, pos));
                Mem mem = c.newConst(scope, &value, 4);
                c.vpbroadcastd(val, mem);
            }
                break;
            case i64: {
                auto value = read<int64_t>(istream, size, pos);
                Mem mem = c.newConst(scope, &value, 8);
                c.vpbroadcastq(val, mem);
            }
                break;
            case f32: {
                auto value = read<double>(istream, size, pos);
                Mem mem = c.newFloatConst(scope, static_cast<float>(value));
                c.vbroadcastss(val, mem);
            }
                break;
            case f64: {
                auto value = read<double>(istream, size, pos);
                Mem mem = c.newDoubleConst(scope, value);
                c.vbroadcastsd(val, mem);
            }
                break;
        }
        return {val, type, kConst};
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
        auto dk = (lhs.dkind() == kConst && rhs.dkind() == kConst) ? kConst : kMemory;
        return {mask_and(c, lhs.ymm(), rhs.ymm()), dt, dk};
    }

    jit_value_t bin_or(Compiler &c, const jit_value_t &lhs, const jit_value_t &rhs) {
        auto dt = lhs.dtype();
        auto dk = (lhs.dkind() == kConst && rhs.dkind() == kConst) ? kConst : kMemory;
        return {mask_or(c, lhs.ymm(), rhs.ymm()), dt, dk};
    }

    jit_value_t cmp_eq(Compiler &c, const jit_value_t &lhs, const jit_value_t &rhs) {
        auto dt = lhs.dtype();
        auto dk = (lhs.dkind() == kConst && rhs.dkind() == kConst) ? kConst : kMemory;
        return {cmp_eq(c, dt, lhs.ymm(), rhs.ymm()), i32, dk};
    }

    jit_value_t cmp_ne(Compiler &c, const jit_value_t &lhs, const jit_value_t &rhs) {
        auto dt = lhs.dtype();
        auto dk = (lhs.dkind() == kConst && rhs.dkind() == kConst) ? kConst : kMemory;
        auto mt = mask_type(dt);
        return {cmp_ne(c, dt, lhs.ymm(), rhs.ymm()), mt, dk};
    }

    jit_value_t cmp_gt(Compiler &c, const jit_value_t &lhs, const jit_value_t &rhs, bool null_check) {
        auto dt = lhs.dtype();
        auto dk = (lhs.dkind() == kConst && rhs.dkind() == kConst) ? kConst : kMemory;
        auto mt = mask_type(dt);
        return {cmp_gt(c, dt, lhs.ymm(), rhs.ymm(), null_check), mt, dk};
    }

    jit_value_t cmp_ge(Compiler &c, const jit_value_t &lhs, const jit_value_t &rhs, bool null_check) {
        auto dt = lhs.dtype();
        auto dk = (lhs.dkind() == kConst && rhs.dkind() == kConst) ? kConst : kMemory;
        auto mt = mask_type(dt);
        return {cmp_ge(c, dt, lhs.ymm(), rhs.ymm(), null_check), mt, dk};
    }

    jit_value_t cmp_lt(Compiler &c, const jit_value_t &lhs, const jit_value_t &rhs, bool null_check) {
        auto dt = lhs.dtype();
        auto dk = (lhs.dkind() == kConst && rhs.dkind() == kConst) ? kConst : kMemory;
        auto mt = mask_type(dt);
        return {cmp_lt(c, dt, lhs.ymm(), rhs.ymm(), null_check), mt, dk};
    }

    jit_value_t cmp_le(Compiler &c, const jit_value_t &lhs, const jit_value_t &rhs, bool null_check) {
        auto dt = lhs.dtype();
        auto dk = (lhs.dkind() == kConst && rhs.dkind() == kConst) ? kConst : kMemory;
        auto mt = mask_type(dt);
        return {cmp_le(c, dt, lhs.ymm(), rhs.ymm(), null_check), mt, dk};
    }

    jit_value_t add(Compiler &c, const jit_value_t &lhs, const jit_value_t &rhs, bool null_check) {
        auto dt = lhs.dtype();
        auto dk = (lhs.dkind() == kConst && rhs.dkind() == kConst) ? kConst : kMemory;
        return {add(c, dt, lhs.ymm(), rhs.ymm(), null_check), dt, dk};
    }

    jit_value_t sub(Compiler &c, const jit_value_t &lhs, const jit_value_t &rhs, bool null_check) {
        auto dt = lhs.dtype();
        auto dk = (lhs.dkind() == kConst && rhs.dkind() == kConst) ? kConst : kMemory;
        return {sub(c, dt, lhs.ymm(), rhs.ymm(), null_check), dt, dk};
    }

    jit_value_t mul(Compiler &c, const jit_value_t &lhs, const jit_value_t &rhs, bool null_check) {
        auto dt = lhs.dtype();
        auto dk = (lhs.dkind() == kConst && rhs.dkind() == kConst) ? kConst : kMemory;
        return {mul(c, dt, lhs.ymm(), rhs.ymm(), null_check), dt, dk};
    }

    jit_value_t div(Compiler &c, const jit_value_t &lhs, const jit_value_t &rhs, bool null_check) {
        auto dt = lhs.dtype();
        auto dk = (lhs.dkind() == kConst && rhs.dkind() == kConst) ? kConst : kMemory;
        return {div(c, dt, lhs.ymm(), rhs.ymm(), null_check), dt, dk};
    }

    inline std::pair<jit_value_t, jit_value_t>
    convert(Compiler &c, const jit_value_t &lhs, const jit_value_t &rhs, bool null_check) {
        // i32 -> f32
        // i64 -> f64
        switch (lhs.dtype()) {
            case i32:
                switch (rhs.dtype()) {
                    case f32:
                        return std::make_pair(jit_value_t(cvt_itof(c, lhs.ymm(), null_check), f32, lhs.dkind()), rhs);
                    default:
                        assert(false);
                }
                break;
            case i64:
                switch (rhs.dtype()) {
                    case f64:
                        return std::make_pair(jit_value_t(cvt_ltod(c, lhs.ymm(), null_check), f64, lhs.dkind()), rhs);
                    default:
                        assert(false);
                }
                break;
            case f32:
                switch (rhs.dtype()) {
                    case i32:
                        return std::make_pair(lhs, jit_value_t(cvt_itof(c, rhs.ymm(), null_check), f32, rhs.dkind()));
                    default:
                        assert(false);
                }
                break;
            case f64:
                switch (rhs.dtype()) {
                    case i64:
                        return std::make_pair(lhs, jit_value_t(cvt_ltod(c, rhs.ymm(), null_check), f64, rhs.dkind()));
                    default:
                        assert(false);
                }
                break;
            default:
                assert(false);
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
            case AND:
                values.push(bin_and(c, args.first, args.second));
                break;
            case OR:
                values.push(bin_or(c, args.first, args.second));
                break;
            case EQ:
                values.push(cmp_eq(c, args.first, args.second));
                break;
            case NE:
                values.push(cmp_ne(c, args.first, args.second));
                break;
            case GT:
                values.push(cmp_gt(c, args.first, args.second, ncheck));
                break;
            case GE:
                values.push(cmp_ge(c, args.first, args.second, ncheck));
                break;
            case LT:
                values.push(cmp_lt(c, args.first, args.second, ncheck));
                break;
            case LE:
                values.push(cmp_le(c, args.first, args.second, ncheck));
                break;
            case ADD:
                values.push(add(c, args.first, args.second, ncheck));
                break;
            case SUB:
                values.push(sub(c, args.first, args.second, ncheck));
                break;
            case MUL:
                values.push(mul(c, args.first, args.second, ncheck));
                break;
            case DIV:
                values.push(div(c, args.first, args.second, ncheck));
                break;
            default:
                assert(false);
                break;
        }
    }

    void
    emit_code(Compiler &c, const uint8_t *filter_expr, size_t filter_size, std::stack<jit_value_t> &values, bool ncheck,
              const Gp &cols_ptr, const Gp &input_index) {
        uint32_t read_pos = 0;
        while (read_pos < filter_size) {
            auto ic = static_cast<instruction_t>(read<uint8_t>(filter_expr, filter_size, read_pos));
            switch (ic) {
                case RET:
                    break;
                case MEM_I1:
                    values.push(read_mem(c, i8, filter_expr, filter_size, read_pos, cols_ptr, input_index));
                    break;
                case MEM_I2:
                    values.push(read_mem(c, i16, filter_expr, filter_size, read_pos, cols_ptr, input_index));
                    break;
                case MEM_I4:
                    values.push(read_mem(c, i32, filter_expr, filter_size, read_pos, cols_ptr, input_index));
                    break;
                case MEM_I8:
                    values.push(read_mem(c, i64, filter_expr, filter_size, read_pos, cols_ptr, input_index));
                    break;
                case MEM_F4:
                    values.push(read_mem(c, f32, filter_expr, filter_size, read_pos, cols_ptr, input_index));
                    break;
                case MEM_F8:
                    values.push(read_mem(c, f64, filter_expr, filter_size, read_pos, cols_ptr, input_index));
                    break;
                case IMM_I1:
                    values.push(read_imm(c, i8, filter_expr, filter_size, read_pos));
                    break;
                case IMM_I2:
                    values.push(read_imm(c, i16, filter_expr, filter_size, read_pos));
                    break;
                case IMM_I4:
                    values.push(read_imm(c, i32, filter_expr, filter_size, read_pos));
                    break;
                case IMM_I8:
                    values.push(read_imm(c, i64, filter_expr, filter_size, read_pos));
                    break;
                case IMM_F4:
                    values.push(read_imm(c, f32, filter_expr, filter_size, read_pos));
                    break;
                case IMM_F8:
                    values.push(read_imm(c, f64, filter_expr, filter_size, read_pos));
                    break;
                case NEG:
                    values.push(neg(c, get_argument(values), ncheck));
                    break;
                case NOT:
                    values.push(bin_not(c, get_argument(values)));
                    break;
                default:
                    emit_bin_op(c, ic, values, ncheck);
                    break;
            }
        }
    }
}


using CompiledFn = int64_t (*)(int64_t *cols, int64_t cols_count, int64_t *rows, int64_t rows_count,
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

        bool null_check = true;
        if (exec_hint == single_size && features.hasAVX2()) {
            auto step = 256 / ((1 << type_size) * 8);
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

        questdb::x86::emit_code(c, filter_expr, filter_size, registers, null_check, cols_ptr, input_index);

        auto mask = registers.top();
        registers.pop();

        c.mov(qword_ptr(rows_ptr, output_index, 3), input_index);

        c.and_(mask.gp(), 1);
        c.add(output_index, mask.gp().r64());

        c.add(input_index, 1);
        c.cmp(input_index, stop);
        c.jl(l_loop); // input_index < stop
        c.bind(l_exit);
    }

    void scalar_loop(const uint8_t *filter_expr, size_t filter_size, bool null_check) {
        if (filter_expr == nullptr || filter_size <= 0) {
            return; //todo: report error
        }

        scalar_tail2(filter_expr, filter_size, null_check, rows_size);
        c.ret(output_index);
    }

    void avx2_loop2(const uint8_t *filter_expr, size_t filter_size, uint32_t step, bool null_check) {
        using namespace asmjit::x86;

        //todo: move to compile fn
        if (filter_expr == nullptr || filter_size <= 0) {
            return; //todo: report error
        }

        Label l_loop = c.newLabel();
        Label l_exit = c.newLabel();

        c.xor_(input_index, input_index); //input_index = 0

        Gp stop = c.newGpq();
        c.mov(stop, rows_size);
        c.sub(stop, step - 1); // stop = rows_size - step + 1

        c.cmp(input_index, stop);
        c.jge(l_exit);

        c.bind(l_loop);

        questdb::avx2::emit_code(c, filter_expr, filter_size, registers, null_check, cols_ptr, input_index);

        auto mask = registers.top();
        registers.pop();

        Gp bits = questdb::avx2::to_bits(c, mask.ymm(), step);
        questdb::avx2::unrolled_loop2(c, bits.r64(), rows_ptr, input_index, output_index, step);

        c.add(input_index, step); // index += step
        c.cmp(input_index, stop);
        c.jl(l_loop); // index < stop
        c.bind(l_exit);

        scalar_tail2(filter_expr, filter_size, null_check, rows_size);
        c.ret(output_index);
    }

    void begin_fn() {
        c.addFunc(FuncSignatureT<int64_t, int64_t *, int64_t, int64_t *, int64_t, int64_t>(CallConv::kIdHost));
        cols_ptr = c.newIntPtr("cols_ptr");
        cols_size = c.newInt64("cols_size");

        c.setArg(0, cols_ptr);
        c.setArg(1, cols_size);

        rows_ptr = c.newIntPtr("rows_ptr");
        rows_size = c.newInt64("rows_size");

        c.setArg(2, rows_ptr);
        c.setArg(3, rows_size);

        input_index = c.newInt64("input_index");
        c.mov(input_index, 0);

        output_index = c.newInt64("output_index");
        c.setArg(4, output_index);
    }

    void end_fn() {
        c.endFunc();
    }

    x86::Compiler &c;

    std::stack<jit_value_t> registers;

    x86::Gp cols_ptr;
    x86::Gp cols_size;
    x86::Gp rows_ptr;
    x86::Gp rows_size;
    x86::Gp input_index;
    x86::Gp output_index;

};

JNIEXPORT jlong JNICALL
Java_io_questdb_jit_FiltersCompiler_compileFunction(JNIEnv *e,
                                                    jclass cl,
                                                    jlong filterAddress,
                                                    jlong filterSize,
                                                    jint options) {
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

    compiler.begin_fn();
    compiler.compile(reinterpret_cast<uint8_t *>(filterAddress), filterSize, options);
    compiler.end_fn();

    c.finalize();

    CompiledFn fn;
    Error err = gGlobalContext.rt.add(&fn, &code);
    if (err) {
        //todo: pass error to java side
        std::cerr << "some error happened" << std::endl;
    }
    auto r = reinterpret_cast<uint64_t>(fn);
    return r;
}

JNIEXPORT void JNICALL
Java_io_questdb_jit_FiltersCompiler_freeFunction(JNIEnv *e, jclass cl, jlong fnAddress) {
    auto fn = reinterpret_cast<void *>(fnAddress);
    gGlobalContext.rt.release(fn);
}

JNIEXPORT jlong JNICALL Java_io_questdb_jit_FiltersCompiler_callFunction(JNIEnv *e,
                                                                         jclass cl,
                                                                         jlong fnAddress,
                                                                         jlong colsAddress,
                                                                         jlong colsSize,
                                                                         jlong rowsAddress,
                                                                         jlong rowsSize,
                                                                         jlong rowsStartOffset) {
    auto fn = reinterpret_cast<CompiledFn>(fnAddress);
    return fn(reinterpret_cast<int64_t *>(colsAddress),
              colsSize,
              reinterpret_cast<int64_t *>(rowsAddress),
              rowsSize,
              rowsStartOffset);
}
