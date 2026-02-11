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

#ifndef QUESTDB_JIT_IMPL_AVX2_H
#define QUESTDB_JIT_IMPL_AVX2_H

#include "consts.h"

namespace questdb::avx2 {
    using namespace asmjit;
    using namespace asmjit::x86;

    inline Vec get_low(Compiler &c, const Vec &x) {
        return x.half();
    }

    inline Vec get_high(Compiler &c, const Vec &x) {
        Vec y = c.new_xmm();
        c.vextracti128(y, x, 1);
        return y;
    }

    inline Gp to_bits32(Compiler &c, const Vec &x) {
        //   return (uint32_t)_mm256_movemask_epi8(x);
        Gp r = c.new_gp32();
        c.vpmovmskb(r, x);
        return r.as<Gp>();
    }

    inline Gp to_bits16(Compiler &c, const Vec &x) {
        //    __m128i a = _mm_packs_epi16(x.get_low(), x.get_high());  // 16-bit words to bytes
        //    return (uint16_t)_mm_movemask_epi8(a);
        Gp r = c.new_gp32();
        Vec l = get_low(c, x);
        Vec h = get_high(c, x);
        c.packsswb(l, h); // 16-bit words to bytes
        c.pmovmskb(r, l);
        c.and_(r, 0xffff);
        return r.as<Gp>();
    }

    inline Gp to_bits8(Compiler &c, const Vec &x) {
        Gp r = c.new_gp32();
        c.vmovmskps(r, x);
        return r.as<Gp>();
    }

    inline Gp to_bits4(Compiler &c, const Vec &mask) {
        Gp r = c.new_gp32();
        c.vmovmskpd(r, mask);
        return r.as<Gp>();
    }

    inline Gp to_bits2(Compiler &c, const Vec &mask) {
        Gp r = to_bits4(c, mask);
        Gp lo = c.new_gp32();
        c.mov(lo, r);
        c.and_(lo, 1);

        c.shr(r, 1);
        c.and_(r, 2);
        c.or_(r, lo);

        return r.as<Gp>();
    }

    inline Gp to_bits(Compiler &c, const Vec &mask, uint32_t step) {
        switch (step) {
            case 32:
                return to_bits32(c, mask);
            case 16:
                return to_bits16(c, mask);
            case 8:
                return to_bits8(c, mask);
            case 4:
                return to_bits4(c, mask);
            case 2:
                return to_bits2(c, mask);
            default:
                __builtin_unreachable();
        }
    }

    // https://stackoverflow.com/questions/36932240/avx2-what-is-the-most-efficient-way-to-pack-left-based-on-a-mask
    inline Vec compress_register(Compiler &c, const Vec &ymm0, const Vec &mask) {
        c.comment("compress_register");
        x86::Gp bits = to_bits32(c, mask);
        Gp identity_indices = c.new_gp64("identity_indices");
        c.mov(identity_indices.r32(), 0x76543210);
        c.pext(identity_indices.r32(), identity_indices.r32(), bits.r32());
        Gp expanded_indices = c.new_gp64("expanded_indices");
        c.movabs(expanded_indices, 0x0F0F0F0F0F0F0F0F);
        c.pdep(identity_indices, identity_indices, expanded_indices);
        Vec ymm1 = c.new_ymm();
        c.vmovq(ymm1.xmm(), identity_indices);
        c.vpmovzxbd(ymm1, ymm1.xmm());
        c.vpermps(ymm1, ymm1, ymm0);
        return ymm1;
    }

    inline Mem vec_long_null(Compiler &c) {
        int64_t nulls[4] = {LONG_NULL, LONG_NULL, LONG_NULL, LONG_NULL};
        return c.new_const(ConstPoolScope::kLocal, &nulls, 32);
    }

    inline Mem vec_int_null(Compiler &c) {
        int32_t nulls[8] = {INT_NULL, INT_NULL, INT_NULL, INT_NULL, INT_NULL, INT_NULL, INT_NULL, INT_NULL};
        return c.new_const(ConstPoolScope::kLocal, &nulls, 32);
    }

    inline Mem vec_float_null(Compiler &c) {
        int32_t nulls[8] = {0x7fc00000, 0x7fc00000, 0x7fc00000, 0x7fc00000, 0x7fc00000, 0x7fc00000, 0x7fc00000, 0x7fc00000};
        return c.new_const(ConstPoolScope::kLocal, &nulls, 32);
    }

    inline Mem vec_double_null(Compiler &c) {
        int64_t nulls[4] = {0x7ff8000000000000LL, 0x7ff8000000000000LL, 0x7ff8000000000000LL, 0x7ff8000000000000LL};
        return c.new_const(ConstPoolScope::kLocal, &nulls, 32);
    }

    inline Mem vec_sign_mask(Compiler &c, data_type_t type) {
        switch (type) {
            case data_type_t::i8: {
                uint8_t mask[32] = {};
                memset(mask, 0x7fu, 32);
                return c.new_const(ConstPoolScope::kLocal, &mask, 32);
            }
                break;
            case data_type_t::i16: {
                uint16_t mask[16] = {0x7fffu, 0x7fffu, 0x7fffu, 0x7fffu, 0x7fffu, 0x7fffu, 0x7fffu, 0x7fffu, 0x7fffu, 0x7fffu, 0x7fffu, 0x7fffu, 0x7fffu, 0x7fffu, 0x7fffu, 0x7fffu };
                return c.new_const(ConstPoolScope::kLocal, &mask, 32);
            }
                break;
            case data_type_t::i32:
            case data_type_t::f32: {
                uint32_t mask[] = {0x7fffffffu, 0x7fffffffu, 0x7fffffffu, 0x7fffffffu, 0x7fffffffu, 0x7fffffffu, 0x7fffffffu, 0x7fffffffu};
                return c.new_const(ConstPoolScope::kLocal, &mask, 32);
            }
                break;
            case data_type_t::i64:
            case data_type_t::f64: {
                uint64_t mask[] = {0x7fffffffffffffffu, 0x7fffffffffffffffu, 0x7fffffffffffffffu, 0x7fffffffffffffffu};
                return c.new_const(ConstPoolScope::kLocal, &mask, 32);
            }
                break;
            default:
                __builtin_unreachable();
        }
    }

    inline bool is_check_for_null(data_type_t t, bool null_check) {
        return null_check && (t == data_type_t::i32 || t == data_type_t::i64);
    }

    inline Vec is_nan(Compiler &c, data_type_t type, const Vec &x) {
        Vec sub = c.new_ymm();
        Vec dst = c.new_ymm();
        switch (type) {
            case data_type_t::f32:
                c.vsubps(sub, x, x); // x - x = 0.0, NaN - NaN = NaN, Inf - Inf = NaN, -Inf - -Inf = NaN
                c.vcmpps(dst, sub, sub, CmpImm::kUNORD);
                break;
            default:
                c.vsubpd(sub, x, x);
                c.vcmppd(dst, sub, sub, CmpImm::kUNORD);
                break;
        }
        return dst;
    }

    inline Vec cmp_eq_null(Compiler &c, data_type_t type, const Vec &x) {
        Vec dst = c.new_ymm();
        switch (type) {
            case data_type_t::i8:
            case data_type_t::i16:
                c.vpxor(dst, dst, dst);
                break;
            case data_type_t::i32:
                c.vpcmpeqd(dst, x, vec_int_null(c));
                break;
            case data_type_t::i64:
                c.vpcmpeqq(dst, x, vec_long_null(c));
                break;
            case data_type_t::f32:
            case data_type_t::f64:
                return is_nan(c, type, x);
            default:
                __builtin_unreachable();
        }
        return dst;
    }

    inline Vec select_bytes(Compiler &c, const Vec &mask, const Vec &a, const Vec &b) {
        Vec dst = c.new_ymm();
        c.vpblendvb(dst, a, b, mask);
        return dst;
    }

    inline Vec select_bytes(Compiler &c, const Vec &mask, const Vec &a, const Mem &b) {
        Vec dst = c.new_ymm();
        c.vpblendvb(dst, a, b, mask);
        return dst;
    }

    inline Vec mask_not(Compiler &c, const Vec &rhs) {
        Vec dst = c.new_ymm();
        Vec mask = c.new_ymm();
        c.vpcmpeqd(mask, mask, mask);
        c.vpxor(dst, rhs, mask);
        return dst;
    }

    inline Vec mask_and(Compiler &c, const Vec &lhs, const Vec &rhs) {
        Vec dst = c.new_ymm();
        c.vpand(dst, lhs, rhs);
        return dst;
    }

    inline Vec mask_and(Compiler &c, const Vec &lhs, const Mem &rhs) {
        Vec dst = c.new_ymm();
        c.vpand(dst, lhs, rhs);
        return dst;
    }

    inline Vec mask_or(Compiler &c, const Vec &lhs, const Vec &rhs) {
        Vec dst = c.new_ymm();
        c.vpor(dst, lhs, rhs);
        return dst;
    }

    inline Vec mask_xor(Compiler &c, const Vec &lhs, const Vec &rhs) {
        Vec dst = c.new_ymm();
        c.vpxor(dst, lhs, rhs);
        return dst;
    }

    inline Vec nulls_mask(Compiler &c, data_type_t &type, const Vec &lhs, const Vec &rhs) {
        Vec lhs_nulls = cmp_eq_null(c, type, lhs);
        Vec rhs_nulls = cmp_eq_null(c, type, rhs);
        return mask_or(c, lhs_nulls, rhs_nulls);
    }

    inline Vec not_nulls_mask(Compiler &c, data_type_t &type, const Vec &lhs, const Vec &rhs) {
        return mask_not(c, nulls_mask(c, type, lhs, rhs));
    }

    inline Vec xor_nulls_mask(Compiler &c, data_type_t &type, const Vec &lhs, const Vec &rhs) {
        Vec lhs_nulls = cmp_eq_null(c, type, lhs);
        Vec rhs_nulls = cmp_eq_null(c, type, rhs);
        return mask_xor(c, lhs_nulls, rhs_nulls);
    }

    inline Vec not_xor_nulls_mask(Compiler &c, data_type_t &type, const Vec &lhs, const Vec &rhs) {
        return mask_not(c, xor_nulls_mask(c, type, lhs, rhs));
    }

    inline Vec cmp_eq_float(Compiler &c, data_type_t type, const Vec &lhs, const Vec &rhs) {
        Vec lhs_copy = c.new_ymm();
        Vec rhs_copy = c.new_ymm();
        c.vmovaps(lhs_copy, lhs);
        c.vmovaps(rhs_copy, rhs);
        Vec dst = c.new_ymm();
        Vec nans = mask_and(c, is_nan(c, type, lhs_copy), is_nan(c, type, rhs_copy));
        Mem sign_mask = vec_sign_mask(c, type);
        c.vsubps(lhs_copy, lhs_copy, rhs_copy); // (lhs - rhs)
        c.vpand(lhs_copy, lhs_copy, sign_mask); // abs(lhs - rhs)
        float eps[8] = {FLOAT_EPSILON,FLOAT_EPSILON,FLOAT_EPSILON,FLOAT_EPSILON,FLOAT_EPSILON,FLOAT_EPSILON,FLOAT_EPSILON,FLOAT_EPSILON};
        Mem epsilon = c.new_const(ConstPoolScope::kLocal, &eps, 32);
        c.vcmpps(dst, lhs_copy, epsilon, CmpImm::kLT);
        c.vpor(dst, dst, nans);
        return dst;
    }

    inline Vec cmp_eq_double(Compiler &c, data_type_t type, const Vec &lhs, const Vec &rhs) {
        Vec lhs_copy = c.new_ymm();
        Vec rhs_copy = c.new_ymm();
        c.vmovapd(lhs_copy, lhs);
        c.vmovapd(rhs_copy, rhs);
        Vec dst = c.new_ymm();
        Vec nans = mask_and(c, is_nan(c, type, lhs_copy), is_nan(c, type, rhs_copy));
        Mem sign_mask = vec_sign_mask(c, type);
        c.vsubpd(lhs_copy, lhs_copy, rhs_copy); // (lhs - rhs)
        c.vpand(lhs_copy, lhs_copy, sign_mask); // abs(lhs - rhs)
        double eps[4] = {DOUBLE_EPSILON, DOUBLE_EPSILON, DOUBLE_EPSILON, DOUBLE_EPSILON};
        Mem epsilon = c.new_const(ConstPoolScope::kLocal, &eps, 32);
        c.vcmppd(dst, lhs_copy, epsilon, CmpImm::kLT);
        c.vpor(dst, dst, nans);
        return dst;
    }

    inline Vec cmp_eq(Compiler &c, data_type_t type, const Vec &lhs, const Vec &rhs) {
        Vec dst = c.new_ymm();
        switch (type) {
            case data_type_t::i8: {
                c.vpcmpeqb(dst, lhs, rhs);
            }
                break;
            case data_type_t::i16: {
                c.vpcmpeqw(dst, lhs, rhs);
            }
                break;
            case data_type_t::i32: {
                c.vpcmpeqd(dst, lhs, rhs);
            }
                break;
            case data_type_t::i64: {
                c.vpcmpeqq(dst, lhs, rhs);
            }
                break;
            case data_type_t::i128: {
                c.vpcmpeqq(lhs, lhs, rhs);
                c.vpermq(dst, lhs, (1 << 0) | (0 << 2) | (3 << 4) | (2 << 6));
                c.vpand(dst, dst, lhs);
            }
                break;
            case data_type_t::f32:
                return cmp_eq_float(c, type, lhs, rhs);
            case data_type_t::f64: 
                return cmp_eq_double(c, type, lhs, rhs);
            default:
                __builtin_unreachable();
        }
        return dst;
    }

    inline Vec cmp_ne(Compiler &c, data_type_t type, const Vec &lhs, const Vec &rhs) {
        return mask_not(c, cmp_eq(c, type, lhs, rhs));
    }

    inline Vec cmp_lt(Compiler &c, data_type_t type, const Vec &lhs, const Vec &rhs) {
        Vec dst = c.new_ymm();
        switch (type) {
            case data_type_t::i8:
                c.vpcmpgtb(dst, rhs, lhs);
                break;
            case data_type_t::i16:
                c.vpcmpgtw(dst, rhs, lhs);
                break;
            case data_type_t::i32:
                c.vpcmpgtd(dst, rhs, lhs);
                break;
            case data_type_t::i64:
                c.vpcmpgtq(dst, rhs, lhs);
                break;
            case data_type_t::f32: {
                Vec neq = mask_not(c, cmp_eq_float(c, type, lhs, rhs));
                c.vcmpps(dst, lhs, rhs, CmpImm::kLT);
                return mask_and(c, dst, neq);
            }
            case data_type_t::f64: {
                Vec neq = mask_not(c, cmp_eq_double(c, type, lhs, rhs));
                c.vcmppd(dst, lhs, rhs, CmpImm::kLT);
                return mask_and(c, dst, neq);
            }
            default:
                __builtin_unreachable();
        }
        return dst;
    }

    inline Vec cmp_gt(Compiler &c, data_type_t type, const Vec &lhs, const Vec &rhs) {
        return cmp_lt(c, type, rhs, lhs);
    }

    inline Vec cmp_gt(Compiler &c, data_type_t type, const Vec &lhs, const Vec &rhs, bool null_check) {
        if(!is_check_for_null(type, null_check)) {
            return cmp_gt(c, type, lhs, rhs);
        } else {
            Vec r = cmp_gt(c, type, lhs, rhs);
            Vec not_nulls = not_nulls_mask(c, type, lhs, rhs);
            return mask_and(c, r, not_nulls);
        }
    }

    inline Vec cmp_lt(Compiler &c, data_type_t type, const Vec &lhs, const Vec &rhs, bool null_check) {
        if(!is_check_for_null(type, null_check)) {
            return cmp_lt(c, type, lhs, rhs);
        } else {
            Vec r = cmp_lt(c, type, lhs, rhs);
            Vec not_nulls = not_nulls_mask(c, type, lhs, rhs);
            return mask_and(c, r, not_nulls);
        }
    }

    inline Vec cmp_le(Compiler &c, data_type_t type, const Vec &lhs, const Vec &rhs, bool null_check);

    inline Vec cmp_ge(Compiler &c, data_type_t type, const Vec &lhs, const Vec &rhs, bool null_check) {
        switch (type) {
            case data_type_t::f32:
            case data_type_t::f64:
                return cmp_le(c, type, rhs, lhs, null_check);
            default: {
                Vec mask = mask_not(c, cmp_lt(c, type, lhs, rhs));
                Vec not_xor_nulls = not_xor_nulls_mask(c, type, lhs, rhs);
                return mask_and(c, mask, not_xor_nulls);
            }
        }
    }

    inline Vec cmp_le(Compiler &c, data_type_t type, const Vec &lhs, const Vec &rhs, bool null_check) {
        switch (type) {
            case data_type_t::f32: {
                Vec eq = cmp_eq_float(c, type, lhs, rhs);
                Vec dst = c.new_ymm();
                c.vcmpps(dst.ymm(), lhs.ymm(), rhs.ymm(), CmpImm::kLE);
                return mask_or(c, dst, eq);
            }
            case data_type_t::f64: {
                Vec eq = cmp_eq_double(c, type, lhs, rhs);
                Vec dst = c.new_ymm();
                c.vcmppd(dst.ymm(), lhs.ymm(), rhs.ymm(), CmpImm::kLE);
                return mask_or(c, dst, eq);
            }
            default:
                return cmp_ge(c, type, rhs, lhs, null_check);
        }
    }

    inline Vec add(Compiler &c, data_type_t type, const Vec &lhs, const Vec &rhs) {
        Vec dst = c.new_ymm();
        switch (type) {
            case data_type_t::i8:
                c.vpaddb(dst, lhs, rhs);
                break;
            case data_type_t::i16:
                c.vpaddw(dst, lhs, rhs);
                break;
            case data_type_t::i32:
                c.vpaddd(dst, lhs, rhs);
                break;
            case data_type_t::i64:
                c.vpaddq(dst, lhs, rhs);
                break;
            case data_type_t::f32:
                c.vaddps(dst, lhs, rhs);
                break;
            case data_type_t::f64:
                c.vaddpd(dst, lhs, rhs);
                break;
            default:
                __builtin_unreachable();
        }
        return dst;
    }

    inline Vec blend_with_nulls(Compiler &c, data_type_t &type, const Vec &t, const Vec &lhs, const Vec &rhs) {
        Vec nulls_msk = nulls_mask(c, type, lhs, rhs);
        Mem nulls_const =  (type == data_type_t::i32) ? vec_int_null(c) : vec_long_null(c);
        return select_bytes(c, nulls_msk, t, nulls_const);
    }

    inline Vec add(Compiler &c, data_type_t type, const Vec &lhs, const Vec &rhs, bool null_check) {
        if(!is_check_for_null(type, null_check)) {
            return add(c, type, lhs, rhs);
        } else {
            Vec t = add(c, type, lhs, rhs);
            return blend_with_nulls(c, type, t, lhs, rhs);
        }
    }

    inline Vec sub(Compiler &c, data_type_t type, const Vec &lhs, const Vec &rhs) {
        Vec dst = c.new_ymm();
        switch (type) {
            case data_type_t::i8:
                c.vpsubb(dst, lhs, rhs);
                break;
            case data_type_t::i16:
                c.vpsubw(dst, lhs, rhs);
                break;
            case data_type_t::i32:
                c.vpsubd(dst, lhs, rhs);
                break;
            case data_type_t::i64:
                c.vpsubq(dst, lhs, rhs);
                break;
            case data_type_t::f32:
                c.vsubps(dst, lhs, rhs);
                break;
            case data_type_t::f64:
                c.vsubpd(dst, lhs, rhs);
                break;
            default:
                __builtin_unreachable();
        }
        return dst;
    }

    inline Vec sub(Compiler &c, data_type_t type, const Vec &lhs, const Vec &rhs, bool null_check) {
        if(!is_check_for_null(type, null_check)) {
            return sub(c, type, lhs, rhs);
        } else {
            Vec t = sub(c, type, lhs, rhs);
            return blend_with_nulls(c, type, t, lhs, rhs);
        }
    }

    inline Vec mul(Compiler &c, data_type_t type, const Vec &lhs, const Vec &rhs) {
        Vec dst = c.new_ymm();
        switch (type) {
            case data_type_t::i8:
                // There is no 8-bit multiply in AVX2. Split into two 16-bit multiplications
                //            __m256i aodd    = _mm256_srli_epdata_type_t::i16(a,8);              // odd numbered elements of a
                //            __m256i bodd    = _mm256_srli_epdata_type_t::i16(b,8);              // odd numbered elements of b
                //            __m256i muleven = _mm256_mullo_epdata_type_t::i16(a,b);             // product of even numbered elements
                //            __m256i mulodd  = _mm256_mullo_epdata_type_t::i16(aodd,bodd);       // product of odd  numbered elements
                //            mulodd  = _mm256_slli_epdata_type_t::i16(mulodd,8);         // put odd numbered elements back in place
                //            __m256i mask    = _mm256_set1_epdata_type_t::i32(0x00FF00FF);       // mask for even positions
                //            __m256i product = _mm256_blendv_epdata_type_t::i8(mask,muleven,mulodd);        // interleave even and odd
                //            return product
            {
                Vec aodd = c.new_ymm();
                c.vpsrlw(aodd, lhs, 8);
                Vec bodd = c.new_ymm();
                c.vpsrlw(bodd, rhs, 8);
                c.vpmullw(lhs, lhs, rhs); // muleven
                c.vpmullw(aodd, aodd, bodd); // mulodd
                c.vpsllw(aodd, aodd, 8); // mulodd
                uint8_t array[] = {255, 0, 255, 0, 255, 0, 255, 0, 255, 0, 255, 0, 255, 0, 255, 0, 255, 0, 255, 0, 255,
                                   0,
                                   255, 0, 255, 0, 255, 0, 255, 0, 255, 0};
                Mem c0 = c.new_const(asmjit::ConstPoolScope::kLocal, &array, 32);
                Vec mask = c.new_ymm();
                c.vmovdqa(mask, c0);
                c.vpblendvb(dst, aodd, lhs, mask);
            }
                break;
            case data_type_t::i16:
                c.vpmullw(dst, lhs, rhs);
                break;
            case data_type_t::i32:
                c.vpmulld(dst, lhs, rhs);
                break;
            case data_type_t::i64:
                //            __m256i bswap   = _mm256_shuffle_epdata_type_t::i32(b,0xB1);        // swap H<->L
                //            __m256i prodlh  = _mm256_mullo_epdata_type_t::i32(a,bswap);         // 32 bit L*H products
                //            __m256i zero    = _mm256_setzero_si256();              // 0
                //            __m256i prodlh2 = _mm256_hadd_epdata_type_t::i32(prodlh,zero);      // a0Lb0H+a0Hb0L,a1Lb1H+a1Hb1L,0,0
                //            __m256i prodlh3 = _mm256_shuffle_epdata_type_t::i32(prodlh2,0x73);  // 0, a0Lb0H+a0Hb0L, 0, a1Lb1H+a1Hb1L
                //            __m256i prodll  = _mm256_mul_epu32(a,b);               // a0Lb0L,a1Lb1L, 64 bit unsigned products
                //            __m256i prod    = _mm256_add_epdata_type_t::i64(prodll,prodlh3);    // a0Lb0L+(a0Lb0H+a0Hb0L)<<32, a1Lb1L+(a1Lb1H+a1Hb1L)<<32
                //            return  prod;
            {
                Vec t = c.new_ymm();
                c.vpshufd(t, rhs, 0xB1);
                c.vpmulld(t, t, lhs);
                Vec z = c.new_ymm();
                c.vpxor(z, z, z);
                c.vphaddd(t, t, z);
                c.vpshufd(t, t, 0x73);
                c.vpmuludq(lhs, lhs, rhs);
                c.vpaddq(dst, t, lhs);
            }
                break;
            case data_type_t::f32:
                c.vmulps(dst, lhs, rhs);
                break;
            case data_type_t::f64:
                c.vmulpd(dst, lhs, rhs);
                break;
            default:
                __builtin_unreachable();
        }
        return dst;
    }

    inline Vec mul(Compiler &c, data_type_t type, const Vec &lhs, const Vec &rhs, bool null_check) {
        if(!is_check_for_null(type, null_check)) {
            return mul(c, type, lhs, rhs);
        } else {
            Vec t = mul(c, type, lhs, rhs);
            return blend_with_nulls(c, type, t, lhs, rhs);
        }
    }

    inline Vec div_unrolled(Compiler &c, data_type_t type, const Vec &lhs, const Vec &rhs) {
        Vec dst = c.new_ymm();
        switch (type) {
            case data_type_t::i8:
            case data_type_t::i16:
            case data_type_t::i32:
            case data_type_t::i64: {
                Mem lhs_m = c.new_stack(32, 32);
                Mem rhs_m = c.new_stack(32, 32);

                lhs_m.set_size(32);
                rhs_m.set_size(32);

                c.vmovdqu(lhs_m, lhs);
                c.vmovdqu(rhs_m, rhs);

                switch (type) {
                    case data_type_t::i8: {
                        auto size = 1;
                        auto step = 32;

                        Gp a = c.new_gp32();
                        Gp b = c.new_gp32();

                        lhs_m.set_size(size);
                        rhs_m.set_size(size);
                        for (int32_t i = 0; i < step; ++i) {
                            lhs_m.set_offset(i * size);
                            c.movsx(a.r32(), lhs_m);
                            rhs_m.set_offset(i * size);
                            c.movsx(b.r32(), rhs_m);
                            Gp r = x86::int32_div(c, a.r32(), b.r32(), true);
                            c.mov(lhs_m, r.r8());
                        }

                    }
                        break;
                    case data_type_t::i16: {
                        auto size = 2;
                        auto step = 16;

                        Gp a = c.new_gp32();
                        Gp b = c.new_gp32();

                        lhs_m.set_size(size);
                        rhs_m.set_size(size);

                        for (int32_t i = 0; i < step; ++i) {
                            lhs_m.set_offset(i * size);
                            c.movsx(a.r32(), lhs_m);
                            rhs_m.set_offset(i * size);
                            c.movsx(b.r32(), rhs_m);

                            Gp r = x86::int32_div(c, a.r32(), b.r32(), true);
                            c.mov(lhs_m, r.r16());
                        }
                    }
                        break;
                    case data_type_t::i32: {
                        auto size = 4;
                        auto step = 8;

                        Gp a = c.new_gp32();
                        Gp b = c.new_gp32();

                        lhs_m.set_size(size);
                        rhs_m.set_size(size);

                        for (int32_t i = 0; i < step; ++i) {
                            lhs_m.set_offset(i * size);
                            c.mov(a.r32(), lhs_m);
                            rhs_m.set_offset(i * size);
                            c.mov(b.r32(), rhs_m);
                            Gp r = x86::int32_div(c, a.r32(), b.r32(), true);
                            c.mov(lhs_m, r.r32());
                        }
                    }
                        break;
                    default: {
                        auto size = 8;
                        auto step = 4;

                        Gp a = c.new_gp64();
                        Gp b = c.new_gp64();

                        lhs_m.set_size(size);
                        rhs_m.set_size(size);

                        for (int32_t i = 0; i < step; ++i) {
                            lhs_m.set_offset(i * size);
                            rhs_m.set_offset(i * size);
                            c.mov(a, lhs_m);
                            c.mov(b, rhs_m);
                            Gp r = x86::int64_div(c, a.r64(), b.r64(), true);
                            c.mov(lhs_m, r);
                        }
                    }
                        break;
                }

                lhs_m.reset_offset();
                lhs_m.set_size(32);
                c.vmovdqu(dst, lhs_m);
            }
                break;
            case data_type_t::f32:
                c.vdivps(dst, lhs, rhs);
                break;
            case data_type_t::f64:
                c.vdivpd(dst, lhs, rhs);
                break;
            default:
                __builtin_unreachable();
        }
        return dst;
    }

    inline Vec div(Compiler &c, data_type_t type, const Vec &lhs, const Vec &rhs, bool null_check) {
        if(!is_check_for_null(type, null_check)) {
            return div_unrolled(c, type, lhs, rhs);
        } else {
            Vec t = div_unrolled(c, type, lhs, rhs);
            return blend_with_nulls(c, type, t, lhs, rhs);
        }
    }

    inline Vec neg(Compiler &c, data_type_t type, const Vec &rhs) {
        Vec zero = c.new_ymm();
        c.vxorps(zero, zero, zero);
        return sub(c, type, zero, rhs);
    }

    inline Vec neg(Compiler &c, data_type_t type, const Vec &rhs, bool null_check) {
        if(!is_check_for_null(type, null_check)) {
            return neg(c, type, rhs);
        } else {
            Vec r = neg(c, type, rhs);
            Vec nulls = cmp_eq_null(c, type, rhs);
            return select_bytes(c, nulls, r, rhs);
        }
    }

    inline Vec abs(Compiler &c, data_type_t type, const Vec &rhs) {
        return mask_and(c, rhs, vec_sign_mask(c, type));
    }

    inline Vec cvt_itof(Compiler &c, const Vec &rhs, bool null_check) {
        Vec dst = c.new_ymm();
        c.vcvtdq2ps(dst, rhs);
        if(null_check) {
            Vec int_nulls_mask = cmp_eq_null(c, data_type_t::i32, rhs);
            Vec nans = c.new_ymm();
            c.vmovups(nans, vec_float_null(c));
            return select_bytes(c, int_nulls_mask, dst, nans);
        }
        return dst;
    }

    inline Vec cvt_ltod(Compiler &c, const Vec &rhs, bool null_check) {
        Vec dst = c.new_ymm();

        Vec xmm1 = c.new_xmm();
        Vec xmm2 = c.new_xmm();
        Vec xmm3 = c.new_xmm();
        Vec xmm4 = c.new_xmm();
        Vec xmm5 = c.new_xmm();
        Vec xmm6 = c.new_xmm();

        c.vxorpd(xmm1, xmm1, xmm1);
        c.vxorpd(xmm2, xmm2, xmm2);
        c.vxorpd(xmm3, xmm3, xmm3);
        c.vxorpd(xmm4, xmm4, xmm4);

        Mem mem = c.new_stack(32, 32);
        c.vmovdqu(mem, rhs);
        mem.set_size(8);
        c.vcvtsi2sd(xmm1, xmm1, mem);
        mem.add_offset(8);
        c.vcvtsi2sd(xmm2, xmm2, mem);
        mem.add_offset(8);
        c.vcvtsi2sd(xmm3, xmm3, mem);
        mem.add_offset(8);
        c.vcvtsi2sd(xmm4, xmm4, mem);

        c.vunpcklpd(xmm5, xmm1, xmm2);
        c.vunpcklpd(xmm6, xmm3, xmm4);
        c.vinsertf128(dst, xmm5.ymm(), xmm6, 1);

        if (null_check) {
            Vec int_nulls_mask = cmp_eq_null(c, data_type_t::i64, rhs);
            Vec nans = c.new_ymm();
            c.vmovups(nans, vec_double_null(c));
            return select_bytes(c, int_nulls_mask, dst, nans);
        }
        return dst;
    }
}

#endif //QUESTDB_JIT_IMPL_AVX2_H
