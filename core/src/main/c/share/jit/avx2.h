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

#ifndef QUESTDB_JIT_AVX2_H
#define QUESTDB_JIT_AVX2_H

#include "common.h"
#include "impl/avx2.h"

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
    read_vars_mem(Compiler &c, data_type_t type, const uint8_t *istream, size_t size, uint32_t &pos,
                  const Gp &vars_ptr) {
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
        auto dk = dst_kind(lhs, rhs);
        return {mask_and(c, lhs.ymm(), rhs.ymm()), dt, dk};
    }

    jit_value_t bin_or(Compiler &c, const jit_value_t &lhs, const jit_value_t &rhs) {
        auto dt = lhs.dtype();
        auto dk = dst_kind(lhs, rhs);
        return {mask_or(c, lhs.ymm(), rhs.ymm()), dt, dk};
    }

    jit_value_t cmp_eq(Compiler &c, const jit_value_t &lhs, const jit_value_t &rhs) {
        auto dt = lhs.dtype();
        auto dk = dst_kind(lhs, rhs);
        return {cmp_eq(c, dt, lhs.ymm(), rhs.ymm()), data_type_t::i32, dk};
    }

    jit_value_t cmp_ne(Compiler &c, const jit_value_t &lhs, const jit_value_t &rhs) {
        auto dt = lhs.dtype();
        auto dk = dst_kind(lhs, rhs);
        auto mt = mask_type(dt);
        return {cmp_ne(c, dt, lhs.ymm(), rhs.ymm()), mt, dk};
    }

    jit_value_t cmp_gt(Compiler &c, const jit_value_t &lhs, const jit_value_t &rhs, bool null_check) {
        auto dt = lhs.dtype();
        auto dk = dst_kind(lhs, rhs);
        auto mt = mask_type(dt);
        return {cmp_gt(c, dt, lhs.ymm(), rhs.ymm(), null_check), mt, dk};
    }

    jit_value_t cmp_ge(Compiler &c, const jit_value_t &lhs, const jit_value_t &rhs, bool null_check) {
        auto dt = lhs.dtype();
        auto dk = dst_kind(lhs, rhs);
        auto mt = mask_type(dt);
        return {cmp_ge(c, dt, lhs.ymm(), rhs.ymm(), null_check), mt, dk};
    }

    jit_value_t cmp_lt(Compiler &c, const jit_value_t &lhs, const jit_value_t &rhs, bool null_check) {
        auto dt = lhs.dtype();
        auto dk = dst_kind(lhs, rhs);
        auto mt = mask_type(dt);
        return {cmp_lt(c, dt, lhs.ymm(), rhs.ymm(), null_check), mt, dk};
    }

    jit_value_t cmp_le(Compiler &c, const jit_value_t &lhs, const jit_value_t &rhs, bool null_check) {
        auto dt = lhs.dtype();
        auto dk = dst_kind(lhs, rhs);
        auto mt = mask_type(dt);
        return {cmp_le(c, dt, lhs.ymm(), rhs.ymm(), null_check), mt, dk};
    }

    jit_value_t add(Compiler &c, const jit_value_t &lhs, const jit_value_t &rhs, bool null_check) {
        auto dt = lhs.dtype();
        auto dk = dst_kind(lhs, rhs);
        return {add(c, dt, lhs.ymm(), rhs.ymm(), null_check), dt, dk};
    }

    jit_value_t sub(Compiler &c, const jit_value_t &lhs, const jit_value_t &rhs, bool null_check) {
        auto dt = lhs.dtype();
        auto dk = dst_kind(lhs, rhs);
        return {sub(c, dt, lhs.ymm(), rhs.ymm(), null_check), dt, dk};
    }

    jit_value_t mul(Compiler &c, const jit_value_t &lhs, const jit_value_t &rhs, bool null_check) {
        auto dt = lhs.dtype();
        auto dk = dst_kind(lhs, rhs);
        return {mul(c, dt, lhs.ymm(), rhs.ymm(), null_check), dt, dk};
    }

    jit_value_t div(Compiler &c, const jit_value_t &lhs, const jit_value_t &rhs, bool null_check) {
        auto dt = lhs.dtype();
        auto dk = dst_kind(lhs, rhs);
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

#endif //QUESTDB_JIT_AVX2_H

