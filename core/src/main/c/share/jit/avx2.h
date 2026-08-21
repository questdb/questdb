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

    // Pre-scan instruction stream and broadcast constants into YMM registers before the loop.
    // This hoists constant broadcasts out of the hot loop.
    void preload_constants_ymm(Compiler &c,
                               const instruction_t *istream,
                               size_t size,
                               ConstantCacheYmm &cache) {
        const auto scope = ConstPoolScope::kLocal;
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
                        Vec dummy;
                        if (!cache.findInt(value, type, dummy)) {
                            Vec reg = c.new_ymm("const_ymm_%lld", value);
                            switch (type) {
                                case data_type_t::i8: {
                                    auto v = static_cast<int8_t>(value);
                                    Mem mem = c.new_const(scope, &v, 1);
                                    c.vpbroadcastb(reg, mem);
                                    break;
                                }
                                case data_type_t::i16: {
                                    auto v = static_cast<int16_t>(value);
                                    Mem mem = c.new_const(scope, &v, 2);
                                    c.vpbroadcastw(reg, mem);
                                    break;
                                }
                                case data_type_t::i32: {
                                    auto v = static_cast<int32_t>(value);
                                    Mem mem = c.new_const(scope, &v, 4);
                                    c.vpbroadcastd(reg, mem);
                                    break;
                                }
                                case data_type_t::i64: {
                                    Mem mem = c.new_const(scope, &value, 8);
                                    c.vpbroadcastq(reg, mem);
                                    break;
                                }
                                default:
                                    break;
                            }
                            cache.addInt(value, type, reg);
                        }
                        break;
                    }
                    case data_type_t::f32:
                    case data_type_t::f64: {
                        double value = instr.dpayload;
                        Vec dummy;
                        if (!cache.findFloat(value, type, dummy)) {
                            Vec reg = c.new_ymm("const_ymm_f_%f", value);
                            if (type == data_type_t::f32) {
                                Mem mem = c.new_float_const(scope, static_cast<float>(value));
                                c.vbroadcastss(reg, mem);
                            } else {
                                Mem mem = c.new_double_const(scope, value);
                                c.vbroadcastsd(reg, mem);
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

    inline static void unrolled_loop2(Compiler &c,
                                      const Gp &bits,
                                      const Gp &rows_ptr,
                                      const Gp &input,
                                      const Gp &output,
                                      int32_t step) {
        Gp offset = c.new_gp64();
        for (int32_t i = 0; i < step; ++i) {
            c.lea(offset, asmjit::x86::ptr(input, i, 0));
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
        Vec val = c.new_ymm();
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
        return c.new_const(ConstPoolScope::kLocal, &broadcast_value, 32);
    }

    // Reads length of variable size column with header stored in data vector (string, binary).
    jit_value_t read_mem_varsize(Compiler &c,
                                 uint32_t header_size,
                                 int32_t column_idx,
                                 const Gp &data_ptr,
                                 const Gp &varsize_aux_ptr,
                                 const Gp &input_index) {
        Label l_nonzero = c.new_label();
        auto offset_shift = type_shift(data_type_t::i64);

        Gp varsize_aux_address = c.new_gp64("varsize_aux_address");
        c.mov(varsize_aux_address, ptr(varsize_aux_ptr, 8 * column_idx, 8));
        Vec index_data = c.new_ymm("index_data");
        Vec next_index_data = c.new_ymm("next_index_data");
        Vec length_data = c.new_ymm("length_data");

        // Load data from the aux vector at input_index to index_data
        c.vmovdqu(index_data, ymmword_ptr(varsize_aux_address, input_index, offset_shift, 0));
        // Load data from the aux vector at input_index + 1 to next_index_data
        c.vmovdqu(next_index_data, ymmword_ptr(varsize_aux_address, input_index, offset_shift, 1 << offset_shift));

        // Subtract the data at input_index from data at input_index + 1
        c.vpsubq(length_data, next_index_data, index_data);
        // Subtract the header size from the result
        Vec broadcast_header_size = c.new_ymm();
        c.vmovdqa(broadcast_header_size, vec_broadcast_long(c, header_size));
        c.vpsubq(length_data, length_data, broadcast_header_size);

        // Compare the entire length_data with zero
        Vec zero = c.new_ymm("zero");
        c.vpxor(zero, zero, zero);
        Vec eq_result = c.new_ymm("eq_result");
        c.vpcmpeqq(eq_result, length_data, zero);
        Gp eq_result_compressed = c.new_gp32("eq_result_compressed");
        c.vpmovmskb(eq_result_compressed, eq_result);

        // Each byte in eq_result_compressed tells if the corresponding qword of auxiliary_data
        // is zero. A zero byte means "the qword is non-zero". Check whether all the qwords
        // are non-zero.
        c.test(eq_result_compressed, eq_result_compressed);
        c.jz(l_nonzero);

        Gp column_address = c.new_gp64("column_address");
        c.mov(column_address, ptr(data_ptr, 8 * column_idx, 8));

        // Slow path: some value lengths are zero, load all the headers. The value in the header
        // may be either 0 (empty value) or -1 (NULL value) and we must distinguish the two.
        // index_data contains four items of the varsize_index column. The items are offsets into
        // the data column (based at column_address). For each offset:
        // 1: move the offset into a Gp register
        // 2: load the header at column_address + offset
        // 3: put the loaded header into the matching position in length_data

        Gp offset_0 = c.new_gp64("offset_0");
        Gp offset_1 = c.new_gp64("offset_1");
        Gp offset_2 = c.new_gp64("offset_2");
        Gp offset_3 = c.new_gp64("offset_3");

        c.vmovq(offset_0, index_data.xmm());
        // Rotate right the qwords in index_data
        c.vpermq(index_data, index_data, 0b00111001);
        c.vmovq(offset_1, index_data.xmm());
        c.vpermq(index_data, index_data, 0b00111001);
        c.vmovq(offset_2, index_data.xmm());
        c.vpermq(index_data, index_data, 0b00111001);
        c.vmovq(offset_3, index_data.xmm());

        Gp header_0 = c.new_gp64("header_0");
        Gp header_1 = c.new_gp64("header_1");
        Gp header_2 = c.new_gp64("header_2");
        Gp header_3 = c.new_gp64("header_3");

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
        Vec acc = c.new_ymm("acc");
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
        Gp varsize_aux_address = c.new_gp64("varsize_aux_address");
        c.mov(varsize_aux_address, ptr(varsize_aux_ptr, 8 * column_idx, 8));

        Gp header_offset = c.new_gp64("header_offset");

        c.mov(header_offset, input_index);
        auto header_shift = type_shift(data_type_t::i128);
        c.sal(header_offset, header_shift);

        Vec headers_0_1 = c.new_ymm("headers_0_1");
        Vec headers_2_3 = c.new_ymm("headers_2_3");

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
    read_mem(Compiler &c, data_type_t type, int32_t column_idx, const Gp &data_ptr, const Gp &varsize_aux_ptr, const Gp &input_index, bool wide_lane,
             const ColumnAddressCache &cache) {
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
        // Use cached column address if available
        Gp column_address;
        if (cache.has(column_idx)) {
            column_address = cache.get(column_idx);
        } else {
            column_address = c.new_gp64("column_address");
            c.mov(column_address, ptr(data_ptr, 8 * column_idx, 8));
        }

        Mem m;
        uint32_t shift = type_shift(type);
        if (shift < 4) {
            if (wide_lane) {
                switch (type) {
                    case data_type_t::i8:
                        m = dword_ptr(column_address, input_index, shift);
                        break;
                    case data_type_t::i16:
                        m = qword_ptr(column_address, input_index, shift);
                        break;
                    case data_type_t::i32:
                    case data_type_t::f32:
                        m = xmmword_ptr(column_address, input_index, shift);
                        break;
                    case data_type_t::i64:
                    case data_type_t::f64:
                        // A 64-bit column already spans the four wide lanes: 4 x 8B = 32B.
                        // It loads exactly as it does outside wide-lane mode.
                        m = ymmword_ptr(column_address, input_index, shift);
                        break;
                    default:
                        __builtin_unreachable();
                }
            } else {
                m = ymmword_ptr(column_address, input_index, shift);
            }
        } else {
            Gp offset = c.new_gp64("row_offset");
            c.mov(offset, input_index);
            c.sal(offset, shift);
            m = ymmword_ptr(column_address, offset, 0);
        }
        Vec row_data = c.new_ymm();
        switch (type) {
            case data_type_t::i8:
                if (wide_lane) {
                    c.vmovd(row_data.xmm(), m);
                    break;
                }
                [[fallthrough]];
            case data_type_t::i16:
                if (wide_lane) {
                    c.vmovq(row_data.xmm(), m);
                    break;
                }
                [[fallthrough]];
            case data_type_t::i32:
                if (wide_lane) {
                    c.vmovdqu(row_data.xmm(), m);
                    break;
                }
                [[fallthrough]];
            case data_type_t::i64:
            case data_type_t::i128:
                c.vmovdqu(row_data, m);
                break;
            case data_type_t::f32:
                if (wide_lane) {
                    c.vmovups(row_data.xmm(), m);
                } else {
                    c.vmovups(row_data, m);
                }
                break;
            case data_type_t::f64:
                c.vmovupd(row_data, m);
                break;
            default:
                __builtin_unreachable();
        }
        return {row_data, type, data_kind_t::kMemory};
    }

    jit_value_t read_imm(Compiler &c, const instruction_t &instr, const ConstantCacheYmm&cache) {
        auto type = static_cast<data_type_t>(instr.options);

        // Check cache for integer constants
        if (type == data_type_t::i8 || type == data_type_t::i16 ||
            type == data_type_t::i32 || type == data_type_t::i64) {
            Vec cached;
            if (cache.findInt(instr.ipayload.lo, type, cached)) {
                return {cached, type, data_kind_t::kConst};
            }
        }
        // Check cache for float constants
        if (type == data_type_t::f32 || type == data_type_t::f64) {
            Vec cached;
            if (cache.findFloat(instr.dpayload, type, cached)) {
                return {cached, type, data_kind_t::kConst};
            }
        }

        // Not in cache, broadcast from memory
        const auto scope = ConstPoolScope::kLocal;
        Vec val = c.new_ymm("imm_value");
        switch (type) {
            case data_type_t::i8: {
                auto value = static_cast<int8_t>(instr.ipayload.lo);
                Mem mem = c.new_const(scope, &value, 1);
                c.vpbroadcastb(val, mem);
            }
                break;
            case data_type_t::i16: {
                auto value = static_cast<int16_t>(instr.ipayload.lo);
                Mem mem = c.new_const(scope, &value, 2);
                c.vpbroadcastw(val, mem);
            }
                break;
            case data_type_t::i32: {
                auto value = static_cast<int32_t>(instr.ipayload.lo);
                Mem mem = c.new_const(scope, &value, 4);
                c.vpbroadcastd(val, mem);
            }
                break;
            case data_type_t::i64: {
                auto value = instr.ipayload.lo;
                Mem mem = c.new_const(scope, &value, 8);
                c.vpbroadcastq(val, mem);
            }
                break;
            case data_type_t::i128: {
                auto value = instr.ipayload;
                Mem mem = c.new_const(scope, &value, 16);
                c.vbroadcasti128(val, mem);
            }
                break;
            case data_type_t::f32: {
                auto value = instr.dpayload;
                Mem mem = c.new_float_const(scope, static_cast<float>(value));
                c.vbroadcastss(val, mem);
            }
                break;
            case data_type_t::f64: {
                auto value = instr.dpayload;
                Mem mem = c.new_double_const(scope, value);
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
        return {neg(c, dt, lhs.vec(), null_check), dt, dk};
    }

    jit_value_t bin_not(Compiler &c, const jit_value_t &lhs) {
        auto dt = lhs.dtype();
        auto dk = lhs.dkind();
        return {mask_not(c, lhs.vec()), dt, dk};
    }

    jit_value_t normalize_wide_mask(Compiler &c, const jit_value_t &value) {
        if (value.dtype() != data_type_t::i32) {
            return value;
        }
        Vec dst = c.new_ymm("wide_mask");
        c.vpmovsxdq(dst, value.vec().xmm());
        return {dst, data_type_t::i64, value.dkind()};
    }

    // Declines the filter instead of emitting a widening that would be wrong for the loop it lands
    // in. Recording the error makes compileFunction() discard the function and report it, and
    // SqlCodeGenerator then falls back to the Java filter - the same graceful decline any other
    // unsupported shape takes. The operands come back unchanged so the rest of code generation
    // stays well-formed; nothing it emits is ever run.
    inline void decline_filter(Compiler &c, const char *reason) {
        c.report_error(asmjit::Error::kInvalidState, reason);
    }

    // Declines the filter and hands the pairing back untouched. convert() uses it for every
    // pairing it cannot harmonise for the lane count in force: the caller keeps emitting
    // well-formed code so the value stack stays balanced, and compileFunction() discards the
    // function before anything it emitted can run.
    inline std::pair<jit_value_t, jit_value_t>
    decline_pairing(Compiler &c, const jit_value_t &lhs, const jit_value_t &rhs, const char *reason) {
        decline_filter(c, reason);
        return std::make_pair(lhs, rhs);
    }

    jit_value_t sx_i64(Compiler &c, const jit_value_t &value, bool null_check) {
        if (value.dtype() != data_type_t::i32) {
            // Fail closed. The frontend emits SX_I64 only over a narrow-int leaf that
            // isWideLaneEligible has admitted, so this is unreachable today; a future gap there
            // costs a JIT decline rather than the undefined behaviour __builtin_unreachable()
            // would hand it, which has no recovery inside a JVM.
            decline_filter(c, "sx_i64 expects an i32 operand");
            Vec zero = c.new_ymm("sx_i64_declined");
            c.vpxor(zero, zero, zero);
            return {zero, data_type_t::i64, value.dkind()};
        }

        Vec extended = c.new_ymm("sx_i64");
        c.vpmovsxdq(extended, value.vec().xmm());
        if (null_check) {
            Vec null_mask_i32 = c.new_ymm("sx_i64_null_i32");
            c.vpcmpeqd(null_mask_i32, value.vec(), vec_int_null(c));
            Vec null_mask_i64 = c.new_ymm("sx_i64_null_i64");
            c.vpmovsxdq(null_mask_i64, null_mask_i32.xmm());
            extended = select_bytes(c, null_mask_i64, extended, vec_long_null(c));
        }
        return {extended, data_type_t::i64, value.dkind()};
    }

    jit_value_t bin_and(Compiler &c, const jit_value_t &lhs, const jit_value_t &rhs, bool wide_lane) {
        auto left = wide_lane ? normalize_wide_mask(c, lhs) : lhs;
        auto right = wide_lane ? normalize_wide_mask(c, rhs) : rhs;
        auto dk = dst_kind(left, right);
        return {mask_and(c, left.vec(), right.vec()), left.dtype(), dk};
    }

    jit_value_t bin_or(Compiler &c, const jit_value_t &lhs, const jit_value_t &rhs, bool wide_lane) {
        auto left = wide_lane ? normalize_wide_mask(c, lhs) : lhs;
        auto right = wide_lane ? normalize_wide_mask(c, rhs) : rhs;
        auto dk = dst_kind(left, right);
        return {mask_or(c, left.vec(), right.vec()), left.dtype(), dk};
    }

    jit_value_t cmp_eq(Compiler &c, const jit_value_t &lhs, const jit_value_t &rhs) {
        auto dt = lhs.dtype();
        auto dk = dst_kind(lhs, rhs);
        auto mt = mask_type(dt);
        return {cmp_eq(c, dt, lhs.vec(), rhs.vec()), mt, dk};
    }

    jit_value_t cmp_ne(Compiler &c, const jit_value_t &lhs, const jit_value_t &rhs) {
        auto dt = lhs.dtype();
        auto dk = dst_kind(lhs, rhs);
        auto mt = mask_type(dt);
        return {cmp_ne(c, dt, lhs.vec(), rhs.vec()), mt, dk};
    }

    jit_value_t cmp_gt(Compiler &c, const jit_value_t &lhs, const jit_value_t &rhs, bool null_check) {
        auto dt = lhs.dtype();
        auto dk = dst_kind(lhs, rhs);
        auto mt = mask_type(dt);
        return {cmp_gt(c, dt, lhs.vec(), rhs.vec(), null_check), mt, dk};
    }

    jit_value_t cmp_ge(Compiler &c, const jit_value_t &lhs, const jit_value_t &rhs, bool null_check) {
        auto dt = lhs.dtype();
        auto dk = dst_kind(lhs, rhs);
        auto mt = mask_type(dt);
        return {cmp_ge(c, dt, lhs.vec(), rhs.vec(), null_check), mt, dk};
    }

    jit_value_t cmp_lt(Compiler &c, const jit_value_t &lhs, const jit_value_t &rhs, bool null_check) {
        auto dt = lhs.dtype();
        auto dk = dst_kind(lhs, rhs);
        auto mt = mask_type(dt);
        return {cmp_lt(c, dt, lhs.vec(), rhs.vec(), null_check), mt, dk};
    }

    jit_value_t cmp_le(Compiler &c, const jit_value_t &lhs, const jit_value_t &rhs, bool null_check) {
        auto dt = lhs.dtype();
        auto dk = dst_kind(lhs, rhs);
        auto mt = mask_type(dt);
        return {cmp_le(c, dt, lhs.vec(), rhs.vec(), null_check), mt, dk};
    }

    jit_value_t add(Compiler &c, const jit_value_t &lhs, const jit_value_t &rhs, bool null_check) {
        auto dt = lhs.dtype();
        auto dk = dst_kind(lhs, rhs);
        return {add(c, dt, lhs.vec(), rhs.vec(), null_check), dt, dk};
    }

    jit_value_t sub(Compiler &c, const jit_value_t &lhs, const jit_value_t &rhs, bool null_check) {
        auto dt = lhs.dtype();
        auto dk = dst_kind(lhs, rhs);
        return {sub(c, dt, lhs.vec(), rhs.vec(), null_check), dt, dk};
    }

    jit_value_t mul(Compiler &c, const jit_value_t &lhs, const jit_value_t &rhs, bool null_check) {
        auto dt = lhs.dtype();
        auto dk = dst_kind(lhs, rhs);
        return {mul(c, dt, lhs.vec(), rhs.vec(), null_check), dt, dk};
    }

    jit_value_t div(Compiler &c, const jit_value_t &lhs, const jit_value_t &rhs, bool null_check) {
        auto dt = lhs.dtype();
        auto dk = dst_kind(lhs, rhs);
        return {div(c, dt, lhs.vec(), rhs.vec(), null_check), dt, dk};
    }

    // Harmonises the two operands of a binary op before emit_bin_op issues the instruction, which
    // types itself from the LEFT operand alone. A pairing with no case here falls through unchanged
    // and the op would then be emitted at the left width against a register holding the right one -
    // wrong rows, silently - so every pairing this function cannot harmonise fails CLOSED: it
    // declines the filter and SqlCodeGenerator falls back to the Java one.
    //
    // The gate is the loop's LANE COUNT, not the wide-lane flag. sx_i64, cvt_itod, cvt_ftod and
    // cvt_ltod each produce exactly four results - the first three read only the low 128 bits of
    // their operand, cvt_ltod converts four i64 - so each is correct at four lanes and only at
    // four lanes. compiler.cpp runs four lanes for WIDE_LANE and for SINGLE_SIZE over an
    // eight-byte column alike (step = 256 / (type_size_bytes * 8) is 4 for an eight-byte lane), so
    // gating on wide_lane declined to convert in a loop that was already four lanes wide. That is
    // what let an (i32, f64) pairing reach vcmppd against a register holding eight packed i32,
    // and left the mask at 32-bit granularity for compress_register's vpermps to splice two row
    // ids into one out-of-range id.
    //
    // cvt_itof is the exception: it converts a full register of eight i32 and changes no lane
    // width, so it needs no lane count and stays ungated.
    //
    // A var-size column reads as four packed i64 lanes (read_mem_varsize /
    // read_mem_varchar_header) and serializeNull spells the header NULL sentinel as an i64
    // immediate for STRING, BINARY and VARCHAR alike, so `<varsize> IS [NOT] NULL` arrives here
    // already same-width and falls straight through to the return below. STRING used to arrive as
    // an (i64, i32) pairing, which this function harmonised correctly but at the price of an
    // sx_i64 the four-lane loop re-ran on every iteration for a loop-invariant broadcast; the
    // frontend types the sentinel to match the lane instead. The mixed-width arms stay for the
    // pairings that still reach them.
    inline std::pair<jit_value_t, jit_value_t>
    convert(Compiler &c, const jit_value_t &lhs, const jit_value_t &rhs, bool null_check, uint32_t lane_count) {
        // data_type_t::i32 -> data_type_t::f32
        // data_type_t::i64 -> data_type_t::f64
        const bool four_lane = lane_count == 4;
        switch (lhs.dtype()) {
            case data_type_t::i32:
                switch (rhs.dtype()) {
                    case data_type_t::f32:
                        return std::make_pair(
                                jit_value_t(cvt_itof(c, lhs.vec(), null_check), data_type_t::f32, lhs.dkind()), rhs);
                    case data_type_t::i64:
                        if (!four_lane) {
                            return decline_pairing(c, lhs, rhs, "i32-with-i64 pairing outside the four-lane loop");
                        }
                        return std::make_pair(sx_i64(c, lhs, null_check), rhs);
                    case data_type_t::f64:
                        if (!four_lane) {
                            return decline_pairing(c, lhs, rhs, "i32-with-f64 pairing outside the four-lane loop");
                        }
                        return std::make_pair(
                                jit_value_t(cvt_itod(c, lhs.vec(), null_check), data_type_t::f64, lhs.dkind()),
                                rhs);
                    default:
                        break;
                }
                break;
            case data_type_t::i64:
                switch (rhs.dtype()) {
                    case data_type_t::i32:
                        if (!four_lane) {
                            return decline_pairing(c, lhs, rhs, "i64-with-i32 pairing outside the four-lane loop");
                        }
                        return std::make_pair(lhs, sx_i64(c, rhs, null_check));
                    case data_type_t::f32:
                        if (!four_lane) {
                            return decline_pairing(c, lhs, rhs, "i64-with-f32 pairing outside the four-lane loop");
                        }
                        return std::make_pair(
                                jit_value_t(cvt_ltod(c, lhs.vec(), null_check), data_type_t::f64, lhs.dkind()),
                                jit_value_t(cvt_ftod(c, rhs.vec()), data_type_t::f64, rhs.dkind()));
                    case data_type_t::f64:
                        if (!four_lane) {
                            return decline_pairing(c, lhs, rhs, "i64-with-f64 pairing outside the four-lane loop");
                        }
                        return std::make_pair(
                                jit_value_t(cvt_ltod(c, lhs.vec(), null_check), data_type_t::f64, lhs.dkind()), rhs);
                    default:
                        break;
                }
                break;
            case data_type_t::f32:
                switch (rhs.dtype()) {
                    case data_type_t::i32:
                        return std::make_pair(lhs, jit_value_t(cvt_itof(c, rhs.vec(), null_check), data_type_t::f32,
                                                               rhs.dkind()));
                    case data_type_t::i64:
                        if (!four_lane) {
                            return decline_pairing(c, lhs, rhs, "f32-with-i64 pairing outside the four-lane loop");
                        }
                        return std::make_pair(
                                jit_value_t(cvt_ftod(c, lhs.vec()), data_type_t::f64, lhs.dkind()),
                                jit_value_t(cvt_ltod(c, rhs.vec(), null_check), data_type_t::f64, rhs.dkind()));
                    case data_type_t::f64:
                        if (!four_lane) {
                            return decline_pairing(c, lhs, rhs, "f32-with-f64 pairing outside the four-lane loop");
                        }
                        return std::make_pair(
                                jit_value_t(cvt_ftod(c, lhs.vec()), data_type_t::f64, lhs.dkind()), rhs);
                    default:
                        break;
                }
                break;
            case data_type_t::f64:
                switch (rhs.dtype()) {
                    case data_type_t::i32:
                        if (!four_lane) {
                            return decline_pairing(c, lhs, rhs, "f64-with-i32 pairing outside the four-lane loop");
                        }
                        return std::make_pair(
                                lhs,
                                jit_value_t(cvt_itod(c, rhs.vec(), null_check), data_type_t::f64, rhs.dkind()));
                    case data_type_t::i64:
                        if (!four_lane) {
                            return decline_pairing(c, lhs, rhs, "f64-with-i64 pairing outside the four-lane loop");
                        }
                        return std::make_pair(lhs, jit_value_t(cvt_ltod(c, rhs.vec(), null_check), data_type_t::f64,
                                                               rhs.dkind()));
                    case data_type_t::f32:
                        if (!four_lane) {
                            return decline_pairing(c, lhs, rhs, "f64-with-f32 pairing outside the four-lane loop");
                        }
                        return std::make_pair(lhs, jit_value_t(cvt_ftod(c, rhs.vec()), data_type_t::f64, rhs.dkind()));
                    default:
                        break;
                }
                break;
            case data_type_t::i128:
                // Fail closed, like every other arm here. An (i128, i128) pairing needs no
                // conversion and the terminal same-type return below hands it back untouched, so
                // breaking out costs that pairing nothing. Returning HERE instead skipped the
                // terminal check, and an (i128, i64) pairing - or any other mismatched i128 one -
                // then fell through unharmonised while its mirror image, (i64, i128), declined
                // through the inner switch's default. cmp_eq() and the arithmetic helpers type the
                // instruction from the LEFT operand alone, so the unharmonised direction compared
                // 128-bit lanes against a register holding 64-bit ones - wrong rows, silently.
                //
                // No IR stream the frontend emits today reaches it. An i128 operand comes from a
                // UUID or LONG128 column, a UUID literal or a UUID / LONG128 bind variable;
                // PredicateContext::updateType rejects any other column beside a UUID one outright,
                // and SQL resolves no comparison operator for LONG128 against another type at all
                // ("there is no matching operator `=` with the argument types: LONG128 = LONG").
                // Even where the serializer is driven directly past those, an i128 operand makes
                // getExecHint() answer MIXED_SIZE or SCALAR the moment anything narrower joins the
                // filter, and neither hint runs an AVX2 loop. That is an argument for closing the
                // hole, not for leaving it guarded by the absence of an input.
                break;
            default:
                break;
        }
        // Same-width pairings need no conversion and are the common case. Anything else has no arm
        // above, so nothing harmonised it and nothing may assume it is harmless.
        if (lhs.dtype() != rhs.dtype()) {
            return decline_pairing(c, lhs, rhs, "no conversion for this operand pairing");
        }
        return std::make_pair(lhs, rhs);
    }

    inline jit_value_t get_argument(ArenaVector<jit_value_t> &values) {
        return values.pop();
    }

    inline std::pair<jit_value_t, jit_value_t>
    get_arguments(Compiler &c, ArenaVector<jit_value_t> &values, bool ncheck, uint32_t lane_count) {
        auto lhs = values.pop();
        auto rhs = values.pop();
        return convert(c, lhs, rhs, ncheck, lane_count);
    }

    void emit_bin_op(Compiler &c, Arena &arena, const instruction_t &instr, ArenaVector<jit_value_t> &values, bool ncheck, bool wide_lane,
                     uint32_t lane_count) {
        // AND and OR combine comparison MASKS, not values, and bin_and / bin_or already widen a
        // four-lane i32 mask themselves. Routing them through convert() would take the i32-with-i64
        // arm below and pay for a null blend a mask can never need - a lane is 0 or -1, never
        // INT_NULL - so they take the operands untouched, exactly as they did before that arm
        // existed. serializeOperator declines the bitwise operators, so these two opcodes never
        // carry values.
        switch (instr.opcode) {
            case opcodes::And: {
                auto lhs = get_argument(values);
                auto rhs = get_argument(values);
                values.append(arena, bin_and(c, lhs, rhs, wide_lane));
                return;
            }
            case opcodes::Or: {
                auto lhs = get_argument(values);
                auto rhs = get_argument(values);
                values.append(arena, bin_or(c, lhs, rhs, wide_lane));
                return;
            }
            default:
                break;
        }
        auto args = get_arguments(c, values, ncheck, lane_count);
        auto lhs = args.first;
        auto rhs = args.second;
        switch (instr.opcode) {
            case opcodes::Eq:
                values.append(arena, cmp_eq(c, lhs, rhs));
                break;
            case opcodes::Ne:
                values.append(arena, cmp_ne(c, lhs, rhs));
                break;
            case opcodes::Gt:
                values.append(arena, cmp_gt(c, lhs, rhs, ncheck));
                break;
            case opcodes::Ge:
                values.append(arena, cmp_ge(c, lhs, rhs, ncheck));
                break;
            case opcodes::Lt:
                values.append(arena, cmp_lt(c, lhs, rhs, ncheck));
                break;
            case opcodes::Le:
                values.append(arena, cmp_le(c, lhs, rhs, ncheck));
                break;
            case opcodes::Add:
                values.append(arena, add(c, lhs, rhs, ncheck));
                break;
            case opcodes::Sub:
                values.append(arena, sub(c, lhs, rhs, ncheck));
                break;
            case opcodes::Mul:
                values.append(arena, mul(c, lhs, rhs, ncheck));
                break;
            case opcodes::Div:
                values.append(arena, div(c, lhs, rhs, ncheck));
                break;
            default:
                // Fail closed. emit_code() routes EVERY opcode it does not handle itself into this
                // function, so this arm sees whatever a corrupt or future-extended IR stream
                // carries. __builtin_unreachable() made that undefined behaviour: the compiler
                // drops the range check on the jump table and an out-of-enum opcode indexes past
                // its end, inside the JVM and with no recovery. Declining costs a JIT decline
                // instead, and SqlCodeGenerator falls back to the Java filter.
                //
                // get_arguments() already popped both operands, so push one back. avx2_loop() pops
                // the mask this instruction owes it, and ArenaVector::pop() asserts only in a debug
                // build - a release build underflows _size to UINT32_MAX and reads out of bounds.
                // Every avx2 jit_value_t is a Vec, so lhs is a well-formed operand for that pop.
                // The declined function is never register-allocated, assembled or registered:
                // compileFunction() reads the error before c.finalize().
                decline_filter(c, "unsupported opcode in the SIMD path");
                values.append(arena, lhs);
                break;
        }
    }

    void
    emit_code(Compiler &c, Arena &arena, const instruction_t *istream, size_t size, ArenaVector<jit_value_t> &values, bool ncheck, bool wide_lane,
              uint32_t lane_count,
              const Gp &data_ptr, const Gp &varsize_aux_ptr, const Gp &vars_ptr, const Gp &input_index,
              const ColumnAddressCache &addr_cache, const ConstantCacheYmm&const_cache) {
        for (size_t i = 0; i < size; ++i) {
            auto instr = istream[i];
            switch (instr.opcode) {
                case opcodes::Inv:
                    // Fail closed. Inv is the placeholder the serializer writes for a symbol bind
                    // variable and a constant stub; backfillNode() either overwrites it or throws,
                    // so a finished IR stream never carries one. Should it ever survive, abandoning
                    // here leaves the value stack unbalanced - decline first so the filter falls
                    // back to the Java one and the log names the reason.
                    decline_filter(c, "invalid opcode in the SIMD path");
                    return;
                case opcodes::Ret:
                    return;
                case opcodes::Var: {
                    auto type = static_cast<data_type_t>(instr.options);
                    auto idx = static_cast<int32_t>(instr.ipayload.lo);
                    values.append(arena, read_vars_mem(c, type, idx, vars_ptr));
                }
                    break;
                case opcodes::Mem: {
                    auto type = static_cast<data_type_t>(instr.options);
                    auto idx = static_cast<int32_t>(instr.ipayload.lo);
                    values.append(arena, read_mem(c, type, idx, data_ptr, varsize_aux_ptr, input_index, wide_lane, addr_cache));
                }
                    break;
                case opcodes::Imm:
                    values.append(arena, read_imm(c, instr, const_cache));
                    break;
                case opcodes::Neg:
                    values.append(arena, neg(c, get_argument(values), ncheck));
                    break;
                case opcodes::Not:
                    values.append(arena, bin_not(c, get_argument(values)));
                    break;
                case opcodes::And_Sc: // Short-circuit opcodes should never reach SIMD path
                case opcodes::Or_Sc:
                case opcodes::Begin_Sc:
                case opcodes::End_Sc:
                    // Fail closed. serializePredicatesAndSc / serializePredicatesOrSc are the only
                    // producers of these opcodes and both throw when getExecHint() answers
                    // SINGLE_SIZE or WIDE_LANE, so no stream carrying one reaches an AVX2 loop
                    // today. Decline anyway: a short-circuit opcode sits PART-WAY through a stream,
                    // so abandoning here leaves values behind rather than none at all, and
                    // avx2_loop's empty-stack guard would not see it - the loop would pop an
                    // operand instead of a mask and run a filter that selects the wrong rows.
                    decline_filter(c, "short-circuit opcode in the SIMD path");
                    return;
                case opcodes::Sx_I64:
                    if (!wide_lane) {
                        // Fail closed. The frontend emits SX_I64 only for the wide-lane loop, so
                        // this is unreachable; abandoning code generation here would instead leave
                        // the value stack short and avx2_loop would pop an empty ArenaVector -
                        // undefined behaviour inside the JVM rather than a decline. Keep emitting
                        // so the stack stays balanced; the declined function never runs.
                        decline_filter(c, "SX_I64 outside the wide-lane loop");
                    }
                    values.append(arena, sx_i64(c, get_argument(values), ncheck));
                    break;
                default:
                    emit_bin_op(c, arena, instr, values, ncheck, wide_lane, lane_count);
                    break;
            }
        }
    }

}

#endif //QUESTDB_JIT_AVX2_H
