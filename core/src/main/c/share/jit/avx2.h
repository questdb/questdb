/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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
    read_vars_mem(Compiler &c, data_type_t type, int32_t idx, const Gp &vars_ptr) {
        auto value = x86::read_vars_mem(c, type, idx, vars_ptr);
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
            case data_type_t::i128: {
                c.vbroadcasti128(val, mem);
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

    inline Mem vec_broadcast_long(Compiler &c, uint32_t value) {
        uint64_t broadcast_value[4] = {value, value, value, value};
        return c.newConst(ConstPool::kScopeLocal, &broadcast_value, 32);
    }

    // Reads length of variable size column with header stored in data vector (string, binary).
    jit_value_t read_mem_varsize(Compiler &c,
                                 uint32_t header_size,
                                 int32_t column_idx,
                                 const Gp &data_ptr,
                                 const Gp &varsize_aux_ptr,
                                 const Gp &input_index) {
        Label l_nonzero = c.newLabel();
        auto offset_shift = type_shift(data_type_t::i64);

        Gp varsize_aux_address = c.newInt64("varsize_aux_address");
        c.mov(varsize_aux_address, ptr(varsize_aux_ptr, 8 * column_idx, 8));
        Ymm index_data = c.newYmm("index_data");
        Ymm next_index_data = c.newYmm("next_index_data");
        Ymm length_data = c.newYmm("length_data");

        // Load data from the aux vector at input_index to index_data
        c.vmovdqu(index_data, ymmword_ptr(varsize_aux_address, input_index, offset_shift, 0));
        // Load data from the aux vector at input_index + 1 to next_index_data
        c.vmovdqu(next_index_data, ymmword_ptr(varsize_aux_address, input_index, offset_shift, 1 << offset_shift));

        // Subtract the data at input_index from data at input_index + 1
        c.vpsubq(length_data, next_index_data, index_data);
        // Subtract the header size from the result
        Ymm broadcast_header_size = c.newYmm();
        c.vmovdqa(broadcast_header_size, vec_broadcast_long(c, header_size));
        c.vpsubq(length_data, length_data, broadcast_header_size);

        // Compare the entire length_data with zero
        Ymm zero = c.newYmm("zero");
        c.vpxor(zero, zero, zero);
        Ymm eq_result = c.newYmm("eq_result");
        c.vpcmpeqq(eq_result, length_data, zero);
        Gp eq_result_compressed = c.newInt32("eq_result_compressed");
        c.vpmovmskb(eq_result_compressed, eq_result);

        // Each byte in eq_result_compressed tells if the corresponding qword of auxiliary_data
        // is zero. A zero byte means "the qword is non-zero". Check whether all the qwords
        // are non-zero.
        c.test(eq_result_compressed, eq_result_compressed);
        c.jz(l_nonzero);

        Gp column_address = c.newInt64("column_address");
        c.mov(column_address, ptr(data_ptr, 8 * column_idx, 8));

        // Slow path: some value lengths are zero, load all the headers. The value in the header
        // may be either 0 (empty value) or -1 (NULL value) and we must distinguish the two.
        // index_data contains four items of the varsize_index column. The items are offsets into
        // the data column (based at column_address). For each offset:
        // 1: move the offset into a Gp register
        // 2: load the header at column_address + offset
        // 3: put the loaded header into the matching position in length_data

        Gp offset_0 = c.newInt64("offset_0");
        Gp offset_1 = c.newInt64("offset_1");
        Gp offset_2 = c.newInt64("offset_2");
        Gp offset_3 = c.newInt64("offset_3");

        c.vmovq(offset_0, index_data.xmm());
        // Rotate right the qwords in index_data
        c.vpermq(index_data, index_data, 0b00111001);
        c.vmovq(offset_1, index_data.xmm());
        c.vpermq(index_data, index_data, 0b00111001);
        c.vmovq(offset_2, index_data.xmm());
        c.vpermq(index_data, index_data, 0b00111001);
        c.vmovq(offset_3, index_data.xmm());

        Gp header_0 = c.newInt64("header_0");
        Gp header_1 = c.newInt64("header_1");
        Gp header_2 = c.newInt64("header_2");
        Gp header_3 = c.newInt64("header_3");

        // Now perform all the data-dependent loads. Hopefully there'll be some
        // parallelism because the four loads are independent from each other.
        if (header_size == 4) {
            c.movsxd(header_0, ptr(column_address, offset_0, 0, 0, header_size));
            c.movsxd(header_1, ptr(column_address, offset_1, 0, 0, header_size));
            c.movsxd(header_2, ptr(column_address, offset_2, 0, 0, header_size));
            c.movsxd(header_3, ptr(column_address, offset_3, 0, 0, header_size));
        } else {
            c.mov(header_0, ptr(column_address, offset_0, 0, 0, header_size));
            c.mov(header_1, ptr(column_address, offset_1, 0, 0, header_size));
            c.mov(header_2, ptr(column_address, offset_2, 0, 0, header_size));
            c.mov(header_3, ptr(column_address, offset_3, 0, 0, header_size));
        }

        // Combine the four header values into length_data
        c.vpinsrq(length_data.xmm(), length_data.xmm(), header_0, 0);
        c.vpinsrq(length_data.xmm(), length_data.xmm(), header_1, 1);
        Ymm acc = c.newYmm("acc");
        c.vpinsrq(acc.xmm(), acc.xmm(), header_2, 0);
        c.vpinsrq(acc.xmm(), acc.xmm(), header_3, 1);
        c.vinserti128(length_data, length_data, acc.xmm(), 1);

        c.bind(l_nonzero);
        return {length_data, data_type_t::i64, data_kind_t::kMemory};
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
        Gp varsize_aux_address = c.newInt64("varsize_aux_address");
        c.mov(varsize_aux_address, ptr(varsize_aux_ptr, 8 * column_idx, 8));

        Gp header_offset = c.newInt64("header_offset");

        c.mov(header_offset, input_index);
        auto header_shift = type_shift(data_type_t::i128);
        c.sal(header_offset, header_shift);

        Ymm headers_0_1 = c.newYmm("headers_0_1");
        Ymm headers_2_3 = c.newYmm("headers_2_3");

        // Load 4 headers into two YMMs.
        c.vmovdqu(headers_0_1, ymmword_ptr(varsize_aux_address, header_offset, 0));
        c.vmovdqu(headers_2_3, ymmword_ptr(varsize_aux_address, header_offset, 0, 32));

        // Permute the first i64 of each header and combine them into single YMM.
        // 0th and 1st i64 go to the first YMM lane in headers_0_1.
        c.vpermq(headers_0_1, headers_0_1, 0b00001000);
        // 2nd and 3rd i64 go to the second YMM lane in headers_2_3.
        c.vpermq(headers_2_3, headers_2_3, 0b10000000);
        c.vinserti128(headers_2_3, headers_2_3, headers_0_1.xmm(), 0);

        return {headers_2_3, data_type_t::i64, data_kind_t::kMemory};
    }

    jit_value_t
    read_mem(Compiler &c, data_type_t type, int32_t column_idx, const Gp &data_ptr, const Gp &varsize_aux_ptr, const Gp &input_index) {
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

        // Simple case: a fixed-width column

        Gp column_address = c.newInt64("column_address");
        c.mov(column_address, ptr(data_ptr, 8 * column_idx, 8));

        Mem m;
        uint32_t shift = type_shift(type);
        if (shift < 4) {
            m = ymmword_ptr(column_address, input_index, shift);
        } else {
            Gp offset = c.newInt64("row_offset");
            c.mov(offset, input_index);
            c.sal(offset, shift);
            m = ymmword_ptr(column_address, offset, 0);
        }
        Ymm row_data = c.newYmm();
        switch (type) {
            case data_type_t::i8:
            case data_type_t::i16:
            case data_type_t::i32:
            case data_type_t::i64:
            case data_type_t::i128:
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

    jit_value_t read_imm(Compiler &c, const instruction_t &instr) {
        const auto scope = ConstPool::kScopeLocal;
        Ymm val = c.newYmm("imm_value");
        auto type = static_cast<data_type_t>(instr.options);
        switch (type) {
            case data_type_t::i8: {
                auto value = static_cast<int8_t>(instr.ipayload.lo);
                Mem mem = c.newConst(scope, &value, 1);
                c.vpbroadcastb(val, mem);
            }
                break;
            case data_type_t::i16: {
                auto value = static_cast<int16_t>(instr.ipayload.lo);
                Mem mem = c.newConst(scope, &value, 2);
                c.vpbroadcastw(val, mem);
            }
                break;
            case data_type_t::i32: {
                auto value = static_cast<int32_t>(instr.ipayload.lo);
                Mem mem = c.newConst(scope, &value, 4);
                c.vpbroadcastd(val, mem);
            }
                break;
            case data_type_t::i64: {
                auto value = instr.ipayload.lo;
                Mem mem = c.newConst(scope, &value, 8);
                c.vpbroadcastq(val, mem);
            }
                break;
            case data_type_t::i128: {
                auto value = instr.ipayload;
                Mem mem = c.newConst(scope, &value, 16);
                c.vbroadcasti128(val, mem);
            }
                break;
            case data_type_t::f32: {
                auto value = instr.dpayload;
                Mem mem = c.newFloatConst(scope, static_cast<float>(value));
                c.vbroadcastss(val, mem);
            }
                break;
            case data_type_t::f64: {
                auto value = instr.dpayload;
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
            case data_type_t::i128:
                return std::make_pair(lhs, rhs);
            default:
                break;
        }
        return std::make_pair(lhs, rhs);
    }

    inline jit_value_t get_argument(ZoneStack<jit_value_t> &values) {
        auto arg = values.pop();
        return arg;
    }

    inline std::pair<jit_value_t, jit_value_t>
    get_arguments(Compiler &c, ZoneStack<jit_value_t> &values, bool ncheck) {
        auto lhs = values.pop();
        auto rhs = values.pop();
        return convert(c, lhs, rhs, ncheck);
    }

    void emit_bin_op(Compiler &c, const instruction_t &instr, ZoneStack<jit_value_t> &values, bool ncheck) {
        auto args = get_arguments(c, values, ncheck);
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
                values.append(cmp_gt(c, lhs, rhs, ncheck));
                break;
            case opcodes::Ge:
                values.append(cmp_ge(c, lhs, rhs, ncheck));
                break;
            case opcodes::Lt:
                values.append(cmp_lt(c, lhs, rhs, ncheck));
                break;
            case opcodes::Le:
                values.append(cmp_le(c, lhs, rhs, ncheck));
                break;
            case opcodes::Add:
                values.append(add(c, lhs, rhs, ncheck));
                break;
            case opcodes::Sub:
                values.append(sub(c, lhs, rhs, ncheck));
                break;
            case opcodes::Mul:
                values.append(mul(c, lhs, rhs, ncheck));
                break;
            case opcodes::Div:
                values.append(div(c, lhs, rhs, ncheck));
                break;
            default:
                __builtin_unreachable();
        }
    }

    void
    emit_code(Compiler &c, const instruction_t *istream, size_t size, ZoneStack<jit_value_t> &values, bool ncheck,
              const Gp &data_ptr, const Gp &varsize_aux_ptr, const Gp &vars_ptr, const Gp &input_index) {
        for (size_t i = 0; i < size; ++i) {
            auto instr = istream[i];
            switch (instr.opcode) {
                case opcodes::Inv:
                    return; // todo: throw exception
                case opcodes::Ret:
                    return;
                case opcodes::Var: {
                    auto type = static_cast<data_type_t>(instr.options);
                    auto idx = static_cast<int32_t>(instr.ipayload.lo);
                    values.append(read_vars_mem(c, type, idx, vars_ptr));
                }
                    break;
                case opcodes::Mem: {
                    auto type = static_cast<data_type_t>(instr.options);
                    auto idx = static_cast<int32_t>(instr.ipayload.lo);
                    values.append(read_mem(c, type, idx, data_ptr, varsize_aux_ptr, input_index));
                }
                    break;
                case opcodes::Imm:
                    values.append(read_imm(c, instr));
                    break;
                case opcodes::Neg:
                    values.append(neg(c, get_argument(values), ncheck));
                    break;
                case opcodes::Not:
                    values.append(bin_not(c, get_argument(values)));
                    break;
                default:
                    emit_bin_op(c, instr, values, ncheck);
                    break;
            }
        }
    }
}

#endif //QUESTDB_JIT_AVX2_H
