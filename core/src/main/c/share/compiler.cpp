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

class JitErrorHandler : public asmjit::ErrorHandler{
public:
    void handleError(asmjit::Error /*err*/, const char *msg, asmjit::BaseEmitter * /*origin*/) override {
        fprintf(stderr, "ERROR: %s\n", msg);
    }
};

struct JitGlobalContext {
    //rt allocator is thead-safe
    JitRuntime rt;
    JitErrorHandler errorHandler;
};

static JitGlobalContext gGlobalContext;

inline static void avx2_not(asmjit::x86::Compiler &c, jit_value_t &dst, jit_value_t &rhs) {
    asmjit::x86::Mem c0 = c.newInt32Const(asmjit::ConstPool::kScopeLocal, -1);
    asmjit::x86::Ymm mask = c.newYmm();
    c.vpbroadcastd(mask, c0);
    c.vpxor(dst.ymm(), rhs.ymm(), mask);
}

inline static void avx2_and(asmjit::x86::Compiler &c, jit_value_t &dst, jit_value_t &lhs, jit_value_t &rhs) {
    c.vpand(dst.ymm(), lhs.ymm(), rhs.ymm());
}

inline static void avx2_or(asmjit::x86::Compiler &c, jit_value_t &dst, jit_value_t &lhs, jit_value_t &rhs) {
    c.vpor(dst.ymm(), lhs.ymm(), rhs.ymm());
}

inline static void avx2_cmp_eq(asmjit::x86::Compiler &c, jit_value_t &dst, jit_value_t &lhs, jit_value_t &rhs) {
    switch (lhs.dtype()) {
        case i8:
            c.vpcmpeqb(dst.ymm(), lhs.ymm(), rhs.ymm());
            break;
        case i16:
            c.vpcmpeqw(dst.ymm(), lhs.ymm(), rhs.ymm());
            break;
        case i32:
            c.vpcmpeqd(dst.ymm(), lhs.ymm(), rhs.ymm());
            break;
        case i64:
            c.vpcmpeqq(dst.ymm(), lhs.ymm(), rhs.ymm());
            break;
        case f32:
            c.vcmpps(dst.ymm(), lhs.ymm(), rhs.ymm(), asmjit::x86::Predicate::kCmpEQ);
            break;
        case f64:
            c.vcmppd(dst.ymm(), lhs.ymm(), rhs.ymm(), asmjit::x86::Predicate::kCmpEQ);
            break;
        default:
            break;
    }
}

inline static void avx2_cmp_neq(asmjit::x86::Compiler &c, jit_value_t &dst, jit_value_t &lhs, jit_value_t &rhs) {
    switch (lhs.dtype()) {
        case f32:
            c.vcmpps(dst.ymm(), lhs.ymm(), rhs.ymm(), asmjit::x86::Predicate::kCmpNEQ);
            break;
        case f64:
            c.vcmppd(dst.ymm(), lhs.ymm(), rhs.ymm(), asmjit::x86::Predicate::kCmpNEQ);
            break;
        default:
            avx2_cmp_eq(c, dst, lhs, rhs);
            avx2_not(c, dst, dst);
            break;
    }
}

inline static void avx2_not_null(asmjit::x86::Compiler &c, jit_value_t &dst, jit_value_t &lhs, jit_value_t &rhs) {
    asmjit::x86::Ymm lmask = c.newYmm();
    asmjit::x86::Ymm rmask = c.newYmm();
    if(dst.dtype() == i32 || rhs.dtype() == i32) {
        int32_t inulls_a[8] = {int_null, int_null, int_null, int_null, int_null, int_null, int_null, int_null};
        int32_t ineg_a[8] = {-1, -1, -1, -1, -1, -1, -1, -1};
        asmjit::x86::Mem inulls = c.newConst(asmjit::ConstPool::kScopeLocal, &inulls_a, 32);
        asmjit::x86::Mem ineg = c.newConst(asmjit::ConstPool::kScopeLocal, &ineg_a, 32);
        c.vpcmpeqd(lmask, lhs.ymm(), inulls);
        c.vpcmpeqd(rmask, rhs.ymm(), inulls);
        c.vpor(dst.ymm(), lmask, rmask);
        c.vpxor(dst.ymm(), dst.ymm(), ineg);
    } else {
        int64_t lnulls_a[4] = {long_null, long_null, long_null, long_null};
        int64_t lneg_a[4] = {-1ll, -1ll, -1ll, -1ll};
        asmjit::x86::Mem lneg = c.newConst(asmjit::ConstPool::kScopeLocal, &lneg_a, 32);
        asmjit::x86::Mem lnulls = c.newConst(asmjit::ConstPool::kScopeLocal, &lnulls_a, 32);
        c.vpcmpeqq(lmask, lhs.ymm(), lnulls);
        c.vpcmpeqq(rmask, rhs.ymm(), lnulls);
        c.vpor(dst.ymm(), lmask, rmask);
        c.vpxor(dst.ymm(), dst.ymm(), lneg);
    }
}

inline static void avx2_ignore_null(asmjit::x86::Compiler &c, jit_value_t &dst, jit_value_t &lhs, jit_value_t &rhs) {
    asmjit::x86::Ymm rmask = c.newYmm();
    jit_value_t v(rmask, lhs.dtype(), lhs.dkind());
    avx2_not_null(c, v, lhs, rhs);
    c.vpand(dst.ymm(), dst.ymm(), rmask.ymm());
}

inline static void avx2_select_byte(asmjit::x86::Compiler &c, jit_value_t &dst, jit_value_t &lhs, jit_value_t &mask) {
    if(lhs.dtype() == i32) {
        int32_t inulls_a[8] = {int_null, int_null, int_null, int_null, int_null, int_null, int_null, int_null};
        asmjit::x86::Mem inulls = c.newConst(asmjit::ConstPool::kScopeLocal, &inulls_a, 32);
        c.vpblendvb(dst.ymm(), lhs.ymm(), inulls, mask.ymm());
    } else {
        int64_t lnulls_a[4] = {long_null, long_null, long_null, long_null};
        asmjit::x86::Mem lnulls = c.newConst(asmjit::ConstPool::kScopeLocal, &lnulls_a, 32);
        c.vpblendvb(dst.ymm(), lhs.ymm(), lnulls, mask.ymm());
    }
}

inline static void avx2_cmp_gt(asmjit::x86::Compiler &c, jit_value_t &dst, jit_value_t &lhs, jit_value_t &rhs) {
    switch (lhs.dtype()) {
        case i8:
            c.vpcmpgtb(dst.ymm(), lhs.ymm(), rhs.ymm());
            break;
        case i16:
            c.vpcmpgtw(dst.ymm(), lhs.ymm(), rhs.ymm());
            break;
        case i32:
            c.vpcmpgtd(dst.ymm(), lhs.ymm(), rhs.ymm());
            break;
        case i64:
            c.vpcmpgtq(dst.ymm(), lhs.ymm(), rhs.ymm());
            break;
        case f32:
            c.vcmpps(dst.ymm(), lhs.ymm(), rhs.ymm(), asmjit::x86::Predicate::kCmpNLE);
            break;
        case f64:
            c.vcmppd(dst.ymm(), lhs.ymm(), rhs.ymm(), asmjit::x86::Predicate::kCmpNLE);
            break;
        default:
            break;
    }
}

inline static void avx2_cmp_lt(asmjit::x86::Compiler &c, jit_value_t &dst, jit_value_t &lhs, jit_value_t &rhs) {
    switch (lhs.dtype()) {
        case f32:
            c.vcmpps(dst.ymm(), lhs.ymm(), rhs.ymm(), asmjit::x86::Predicate::kCmpLT);
            break;
        case f64:
            c.vcmppd(dst.ymm(), lhs.ymm(), rhs.ymm(), asmjit::x86::Predicate::kCmpLT);
            break;
        default:
            avx2_cmp_gt(c, dst, rhs, lhs);
            break;
    }
}

inline static void avx2_cmp_ge(asmjit::x86::Compiler &c, jit_value_t &dst, jit_value_t &lhs, jit_value_t &rhs) {
    switch (lhs.dtype()) {
        case f32:
            c.vcmpps(dst.ymm(), lhs.ymm(), rhs.ymm(), asmjit::x86::Predicate::kCmpNLT);
            break;
        case f64:
            c.vcmppd(dst.ymm(), lhs.ymm(), rhs.ymm(), asmjit::x86::Predicate::kCmpNLT);
            break;
        default:
            avx2_cmp_gt(c, dst, rhs, lhs);
            avx2_not(c, dst, dst);
            break;
    }
}

inline static void avx2_cmp_le(asmjit::x86::Compiler &c, jit_value_t &dst, jit_value_t &lhs, jit_value_t &rhs) {
    switch (lhs.dtype()) {
        case f32:
            c.vcmpps(dst.ymm(), lhs.ymm(), rhs.ymm(), asmjit::x86::Predicate::kCmpLE);
            break;
        case f64:
            c.vcmppd(dst.ymm(), lhs.ymm(), rhs.ymm(), asmjit::x86::Predicate::kCmpLE);
            break;
        default:
            avx2_cmp_ge(c, dst, rhs, lhs);
            break;
    }
}

inline static void avx2_add(asmjit::x86::Compiler &c, jit_value_t &dst, jit_value_t &lhs, jit_value_t &rhs) {
    switch (lhs.dtype()) {
        case i8:
            c.vpaddb(dst.ymm(), lhs.ymm(), rhs.ymm());
            break;
        case i16:
            c.vpaddw(dst.ymm(), lhs.ymm(), rhs.ymm());
            break;
        case i32:
            c.vpaddd(dst.ymm(), lhs.ymm(), rhs.ymm());
            break;
        case i64:
            c.vpaddq(dst.ymm(), lhs.ymm(), rhs.ymm());
            break;
        case f32:
            c.vaddps(dst.ymm(), lhs.ymm(), rhs.ymm());
            break;
        case f64:
            c.vaddpd(dst.ymm(), lhs.ymm(), rhs.ymm());
            break;
        default:
            break;
    }
}

inline static void avx2_sub(asmjit::x86::Compiler &c, jit_value_t &dst, jit_value_t &lhs, jit_value_t &rhs) {
    switch (lhs.dtype()) {
        case i8:
            c.vpsubb(dst.ymm(), lhs.ymm(), rhs.ymm());
            break;
        case i16:
            c.vpsubw(dst.ymm(), lhs.ymm(), rhs.ymm());
            break;
        case i32:
            c.vpsubd(dst.ymm(), lhs.ymm(), rhs.ymm());
            break;
        case i64:
            c.vpsubq(dst.ymm(), lhs.ymm(), rhs.ymm());
            break;
        case f32:
            c.vsubps(dst.ymm(), lhs.ymm(), rhs.ymm());
            break;
        case f64:
            c.vsubpd(dst.ymm(), lhs.ymm(), rhs.ymm());
            break;
        default:
            break;
    }
}

inline static void avx2_mul(asmjit::x86::Compiler &c, jit_value_t &dst, jit_value_t &lhs, jit_value_t &rhs) {
    switch (lhs.dtype()) {
        case i8:
            // There is no 8-bit multiply in AVX2. Split into two 16-bit multiplications
            //            __m256i aodd    = _mm256_srli_epi16(a,8);              // odd numbered elements of a
            //            __m256i bodd    = _mm256_srli_epi16(b,8);              // odd numbered elements of b
            //            __m256i muleven = _mm256_mullo_epi16(a,b);             // product of even numbered elements
            //            __m256i mulodd  = _mm256_mullo_epi16(aodd,bodd);       // product of odd  numbered elements
            //            mulodd  = _mm256_slli_epi16(mulodd,8);         // put odd numbered elements back in place
            //            __m256i mask    = _mm256_set1_epi32(0x00FF00FF);       // mask for even positions
            //            __m256i product = _mm256_blendv_epi8(mask,muleven,mulodd);        // interleave even and odd
            //            return product
        {
            asmjit::x86::Ymm y2 = c.newYmm();
            c.vpsrlw(y2, lhs.ymm(), 8);
            asmjit::x86::Ymm y3 = c.newYmm();
            c.vpsrlw(y3, rhs.ymm(), 8);
            c.vpmullw(y2, y3, y2);
            c.vpmullw(lhs.ymm(), rhs.ymm(), lhs.ymm());
            c.vpsllw(rhs.ymm(), y2, 8);
            uint8_t array[] = {255, 0, 255, 0, 255, 0, 255, 0, 255, 0, 255, 0, 255, 0, 255, 0, 255, 0, 255, 0, 255, 0,
                               255, 0, 255, 0, 255, 0, 255, 0, 255, 0};
            asmjit::x86::Mem c0 = c.newConst(asmjit::ConstPool::kScopeLocal, &array, 32);
            c.vmovdqa(y2, c0);
            c.vpblendvb(dst.ymm(), y2, lhs.ymm(), rhs.ymm());
        }
            break;
        case i16:
            c.vpmullw(dst.ymm(), lhs.ymm(), rhs.ymm());
            break;
        case i32:
            c.vpmulld(dst.ymm(), lhs.ymm(), rhs.ymm());
            break;
        case i64:
            //            __m256i bswap   = _mm256_shuffle_epi32(b,0xB1);        // swap H<->L
            //            __m256i prodlh  = _mm256_mullo_epi32(a,bswap);         // 32 bit L*H products
            //            __m256i zero    = _mm256_setzero_si256();              // 0
            //            __m256i prodlh2 = _mm256_hadd_epi32(prodlh,zero);      // a0Lb0H+a0Hb0L,a1Lb1H+a1Hb1L,0,0
            //            __m256i prodlh3 = _mm256_shuffle_epi32(prodlh2,0x73);  // 0, a0Lb0H+a0Hb0L, 0, a1Lb1H+a1Hb1L
            //            __m256i prodll  = _mm256_mul_epu32(a,b);               // a0Lb0L,a1Lb1L, 64 bit unsigned products
            //            __m256i prod    = _mm256_add_epi64(prodll,prodlh3);    // a0Lb0L+(a0Lb0H+a0Hb0L)<<32, a1Lb1L+(a1Lb1H+a1Hb1L)<<32
            //            return  prod;
        {
            asmjit::x86::Ymm t = c.newYmm();
            c.vpshufd(t, rhs.ymm(), 0xB1);
            c.vpmulld(t, t, lhs.ymm());
            asmjit::x86::Ymm z = c.newYmm();
            c.vpxor(z, z, z);
            c.vphaddd(t, t, z);
            c.vpshufd(t, t, 0x73);
            c.vpmuludq(lhs.ymm(), lhs.ymm(), rhs.ymm());
            c.vpaddq(dst.ymm(), t, lhs.ymm());
        }
            break;
        case f32:
            c.vmulps(dst.ymm(), lhs.ymm(), rhs.ymm());
            break;
        case f64:
            c.vsubpd(dst.ymm(), lhs.ymm(), rhs.ymm());
            break;
        default:
            break;
    }
}

inline static void avx2_div(asmjit::x86::Compiler &c, jit_value_t &dst, jit_value_t &lhs, jit_value_t &rhs) {
    switch (lhs.dtype()) {
        case i8:
        case i16:
        case i32:
        case i64: {
            uint32_t size = 0;
            uint32_t shift = 0;
            uint32_t step = 0;
            switch (lhs.dtype()) {
                case i8:
                    size = 1;
                    shift = 0;
                    step = 32;
                    break;
                case i16:
                    size = 2;
                    shift = 1;
                    step = 16;
                    break;
                case i32:
                    size = 4;
                    shift = 2;
                    step = 8;
                    break;
                default:
                    size = 8;
                    shift = 3;
                    step = 4;
                    break;
            }
            asmjit::x86::Mem lhs_m = c.newStack(32, 32);
            asmjit::x86::Mem rhs_m = c.newStack(32, 32);

            lhs_m.setSize(size);
            rhs_m.setSize(size);

            c.vmovdqu(lhs_m, lhs.ymm());
            c.vmovdqu(rhs_m, rhs.ymm());

            asmjit::x86::Gp i = c.newGpq();
            c.xor_(i, i);

            asmjit::x86::Mem lhs_c = lhs_m.clone();
            lhs_c.setIndex(i, shift);
            asmjit::x86::Mem rhs_c = rhs_m.clone();
            rhs_c.setIndex(i, shift);

            asmjit::Label l_loop = c.newLabel();
            asmjit::Label l_zero = c.newLabel();
            asmjit::x86::Gp b = c.newGpq();
            asmjit::x86::Gp a = c.newGpq();
            asmjit::x86::Gp r = c.newGpq();

            c.bind(l_loop);
            c.mov(b, rhs_c);
            c.test(b, b);

            c.je(l_zero);
            c.mov(a, lhs_c);

            c.cqo(r, a);
            c.idiv(r, a, b);

            c.bind(l_zero);
            c.mov(lhs_c, a);
            c.inc(i);
            c.cmp(i, step);
            c.jne(l_loop);
            c.vmovdqu(dst.ymm(), lhs_m);
        }
            //todo:
            //there is no vectorized integer division
            break;
        case f32:
            c.vdivps(dst.ymm(), lhs.ymm(), rhs.ymm());
            break;
        case f64:
            c.vdivpd(dst.ymm(), lhs.ymm(), rhs.ymm());
            break;
        default:
            break;
    }
}

inline static void avx2_neg(asmjit::x86::Compiler &c, jit_value_t &dst, jit_value_t &rhs) {
    asmjit::x86::Ymm zero = c.newYmm();
    c.vxorps(zero, zero, zero);
    jit_value_t v(zero, rhs.dtype());
    avx2_sub(c, dst, v, rhs);
}

static inline asmjit::x86::Xmm get_low(asmjit::x86::Compiler &c, const asmjit::x86::Ymm &x) {
    return x.half();
}

static inline asmjit::x86::Xmm get_high(asmjit::x86::Compiler &c, const asmjit::x86::Ymm &x) {
    asmjit::x86::Xmm y = c.newXmm();
    c.vextracti128(y, x, 1);
    return y;
}

inline static void to_bits32(asmjit::x86::Compiler &c, asmjit::x86::Gp &dst, const asmjit::x86::Ymm &x) {
    //    return (uint32_t)_mm256_movemask_epi8(x);
    c.vpmovmskb(dst, x);
}

inline static void to_bits16(asmjit::x86::Compiler &c, asmjit::x86::Gp &dst, const asmjit::x86::Ymm &x) {
    //    __m128i a = _mm_packs_epi16(x.get_low(), x.get_high());  // 16-bit words to bytes
    //    return (uint16_t)_mm_movemask_epi8(a);
    asmjit::x86::Xmm l = get_low(c, x);
    asmjit::x86::Xmm h = get_high(c, x);
    c.packsswb(l, h); // 16-bit words to bytes
    c.pmovmskb(dst, l);
    c.and_(dst, 0xffff);
}
#define OP(type, name, op, nc) type type##_##name(type a, type b) { return (a == nc || b == nc)? nc : a op b; }
inline static void to_bits8(asmjit::x86::Compiler &c, asmjit::x86::Gp &dst, const asmjit::x86::Ymm &x) {
    //    __m128i a = _mm_packs_epi32(x.get_low(), x.get_high());  // 32-bit dwords to 16-bit words
    //    __m128i b = _mm_packs_epi16(a, a);  // 16-bit words to bytes
    //    return (uint8_t)_mm_movemask_epi8(b);
    asmjit::x86::Xmm l = get_low(c, x);
    asmjit::x86::Xmm h = get_high(c, x);
    c.packssdw(l, h); // 32-bit dwords to 16-bit words
    c.packsswb(l, l); // 16-bit words to bytes
    c.pmovmskb(dst, l);
    c.and_(dst, 0xff);
}

inline static void to_bits4(asmjit::x86::Compiler &c, asmjit::x86::Gp &dst, const asmjit::x86::Ymm &mask) {
    //    uint32_t a = (uint32_t)_mm256_movemask_epi8(mask);
    //    return ((a & 1) | ((a >> 7) & 2)) | (((a >> 14) & 4) | ((a >> 21) & 8));
    c.vpmovmskb(dst, mask);
    asmjit::x86::Gp y = c.newGpq();
    c.mov(y, dst);
    c.and_(y, 1);
    asmjit::x86::Gp z = c.newGpq();
    c.mov(z, dst);
    c.shr(z, 7);
    c.and_(z, 2);
    c.or_(z, y);
    c.mov(y, dst);
    c.shr(y, 14);
    c.and_(y, 4);
    c.shr(dst, 21);
    c.and_(dst, 8);
    c.or_(dst, z);
    c.or_(dst, y);
    c.and_(dst, 0xf); // 4 bits
}

inline static void to_bits(asmjit::x86::Compiler &c, asmjit::x86::Gp &dst, const asmjit::x86::Ymm &mask, int64_t step) {
    switch (step) {
        case 32:
            to_bits32(c, dst, mask);
            break;
        case 16:
            to_bits16(c, dst, mask);
            break;
        case 8:
            to_bits8(c, dst, mask);
            break;
        case 4:
            to_bits4(c, dst, mask);
            break;
        default:
            break;
    }
}

inline static void unrolled_loop2(asmjit::x86::Compiler &c,
                                  const asmjit::x86::Gp &bits,
                                  const asmjit::x86::Gp &rows_ptr,
                                  const asmjit::x86::Gp &input,
                                  asmjit::x86::Gp &output, int32_t step) {
    asmjit::x86::Gp offset = c.newInt64();
    for (int32_t i = 0; i < step; ++i) {
        c.lea(offset, asmjit::x86::ptr(input, i));
        c.mov(qword_ptr(rows_ptr, output, 3), offset);
        c.mov(offset, bits);
        c.shr(offset, i);
        c.and_(offset, 1);
        c.add(output, offset);
    }
}

bool is_float(jit_value_t &v) {
    return v.dtype() == f32 || v.dtype() == f64;
}


using CompiledFn = int64_t (*)(int64_t *cols, int64_t cols_count, int64_t *rows, int64_t rows_count,
                               int64_t rows_start_offset);

struct JitCompiler {
    JitCompiler(x86::Compiler &cc)
            : c(cc) {};

    void compile(const uint8_t *filter_expr, int64_t filter_size, uint32_t options) {
        //todo: process options
        bool null_check = true;
        if (options == 0) {
            scalar_loop(filter_expr, filter_size, null_check);
        } else {
            uint32_t step = options;
            avx2_loop(filter_expr, filter_size, step, null_check);
        }
    };

    void scalar_tail(const uint8_t *filter_expr, size_t filter_size, bool null_check, const x86::Gp &stop) {

        Label l_loop = c.newLabel();
        Label l_exit = c.newLabel();

        c.cmp(input_index, stop);
        c.jge(l_exit);

        c.bind(l_loop);

        uint32_t rpos = 0;
        while (rpos < filter_size) {
            auto ic = static_cast<instruction_t>(read<uint8_t>(filter_expr, filter_size, rpos));
            switch (ic) {
                case RET:
                    break;
                case MEM_I1: {
                    auto column_input_index = static_cast<int32_t>(read<int64_t>(filter_expr, filter_size, rpos));
                    asmjit::x86::Gp col = c.newInt64("byte column[%d]", column_input_index);
                    c.mov(col, ptr(cols_ptr, 8 * column_input_index, 8));
                    c.movsx(col, x86::Mem(col, input_index, 0, 0, 1));
                    registers.push(jit_value_t(col, i8, kMemory));
                }
                    break;
                case MEM_I2: {
                    asmjit::x86::Gp col = c.newGpq();
                    auto column_input_index = static_cast<int32_t>(read<int64_t>(filter_expr, filter_size, rpos));
                    c.mov(col, ptr(cols_ptr, 8 * column_input_index, 8));
                    c.movsx(col, asmjit::x86::Mem(col, input_index, 1, 0, 2));
                    registers.push(jit_value_t(col, i16, kMemory));
                }
                    break;
                case MEM_I4: {
                    asmjit::x86::Gp col = c.newGpq();
                    auto column_input_index = static_cast<int32_t>(read<int64_t>(filter_expr, filter_size, rpos));
                    c.mov(col, ptr(cols_ptr, 8 * column_input_index, 8));
                    c.movsxd(col, asmjit::x86::Mem(col, input_index, 2, 0, 4));
                    registers.push(jit_value_t(col, i32, kMemory));
                }
                    break;
                case MEM_I8: {
                    asmjit::x86::Gp col = c.newGpq();
                    auto column_input_index = static_cast<int32_t>(read<int64_t>(filter_expr, filter_size, rpos));
                    c.mov(col, ptr(cols_ptr, 8 * column_input_index, 8));
                    c.mov(col, asmjit::x86::Mem(col, input_index, 3, 0, 8));
                    registers.push(jit_value_t(col, i64, kMemory));
                }
                    break;
                case MEM_F4: {
                    asmjit::x86::Gp col = c.newGpq();
                    auto column_input_index = static_cast<int32_t>(read<int64_t>(filter_expr, filter_size, rpos));
                    c.mov(col, ptr(cols_ptr, 8 * column_input_index, 8));
                    asmjit::x86::Xmm data = c.newXmm();
                    //                c.vmovss(data, Mem(col, input_index, 2, 0, 4));
                    c.cvtss2sd(data, asmjit::x86::Mem(col, input_index, 2, 0, 4)); // float to double
                    registers.push(jit_value_t(data, f32, kMemory));
                }
                    break;
                case MEM_F8: {
                    asmjit::x86::Gp col = c.newGpq();
                    auto column_input_index = static_cast<int32_t>(read<int64_t>(filter_expr, filter_size, rpos));
                    c.mov(col, ptr(cols_ptr, 8 * column_input_index, 8));
                    asmjit::x86::Xmm data = c.newXmm();
                    c.vmovsd(data, asmjit::x86::Mem(col, input_index, 3, 0, 8));
                    registers.push(jit_value_t(data, f64, kMemory));
                }
                    break;
                case IMM_I1: {
                    asmjit::x86::Gp val = c.newGpq();
                    auto value = read<int64_t>(filter_expr, filter_size, rpos);
                    c.mov(val, value);
                    registers.push(jit_value_t(val, i8, kConst));
                }
                    break;
                case IMM_I2: {
                    asmjit::x86::Gp val = c.newGpq();
                    auto value = read<int64_t>(filter_expr, filter_size, rpos);
                    c.mov(val, value);
                    registers.push(jit_value_t(val, i16, kConst));
                }
                    break;
                case IMM_I4: {
                    asmjit::x86::Gp val = c.newGpq();
                    auto value = read<int64_t>(filter_expr, filter_size, rpos);
                    c.mov(val, value);
                    registers.push(jit_value_t(val, i32, kConst));
                }
                    break;
                case IMM_I8: {
                    asmjit::x86::Gp val = c.newGpq();
                    auto value = read<int64_t>(filter_expr, filter_size, rpos);
                    c.mov(val, value);
                    registers.push(jit_value_t(val, i64, kConst));
                }
                    break;
                case IMM_F4: {
                    auto value = read<double>(filter_expr, filter_size, rpos);
                    asmjit::x86::Mem c0 = c.newDoubleConst(asmjit::ConstPool::kScopeLocal, value);
                    asmjit::x86::Xmm val = c.newXmm();
                    c.movsd(val, c0);
                    registers.push(jit_value_t(val, f32, kConst));
                }
                    break;
                case IMM_F8: {
                    auto value = read<double>(filter_expr, filter_size, rpos);
                    asmjit::x86::Mem c0 = c.newDoubleConst(asmjit::ConstPool::kScopeLocal, value);
                    asmjit::x86::Xmm val = c.newXmm();
                    c.movsd(val, c0);
                    registers.push(jit_value_t(val, f64, kConst));
                }
                    break;
                case NEG: {
                    auto arg = registers.top();
                    registers.pop();
                    if (arg.isXmm()) {
                        asmjit::x86::Xmm r = c.newXmmSd();
                        c.xorpd(r, r);
                        c.subpd(r, arg.xmm());
                        arg = r;
                    } else {
                        if(!null_check) {
                            c.neg(arg.gp());
                        } else {
                            switch (arg.dtype()) {
                                case i8:
                                case i16:
                                    c.neg(arg.gp());
                                    break;
                                case i32:
                                {
                                    asmjit::x86::Gp t = c.newGpq();
                                    c.mov(t, arg.gp());
                                    c.cmp(t, int_null);
                                    c.cmove(arg.gp(), t);
                                }
                                    break;
                                case i64:
                                {
                                    asmjit::x86::Gp t = c.newGpq();
                                    c.mov(t, arg.gp());
                                    asmjit::x86::Gp n = c.newGpq();
                                    c.mov(n, long_null);
                                    c.cmp(t, n);
                                    c.cmove(arg.gp(), t);
                                }
                                    break;
                                default:
                                    break;
                            }
                        }
                    }
                    registers.push(arg);
                }
                    break;
                case NOT: {
                    auto arg = registers.top();
                    registers.pop();
                    if (arg.isXmm()) {
                        // error?
                    } else {
                        c.not_(arg.gp());
                        c.and_(arg.gp(), 1);
                    }
                    registers.push(arg);
                }
                    break;
                default:
                    auto lhs = registers.top();
                    registers.pop();
                    auto rhs = registers.top();
                    registers.pop();
                    if (rhs.isXmm() && !lhs.isXmm()) {
                        // lhs long to double
                        asmjit::x86::Gp i = lhs.gp();
                        lhs = c.newXmm();
                        c.vcvtsi2sd(lhs.xmm(), rhs.xmm(), i);
                    }
                    if (lhs.isXmm() && !rhs.isXmm()) {
                        // rhs long to double
                        asmjit::x86::Gp i = rhs.gp();
                        rhs = c.newXmm();
                        c.vcvtsi2sd(rhs.xmm(), lhs.xmm(), i);
                    }
                    switch (ic) {
                        case AND:
                            c.and_(lhs.gp(), rhs.gp());
                            registers.push(lhs);
                            break;
                        case OR:
                            c.or_(lhs.gp(), rhs.gp());
                            registers.push(lhs);
                            break;
                        case EQ:
                            if (lhs.isXmm()) {
                                c.emit(asmjit::x86::Inst::kIdCmpsd, lhs.xmm(), rhs.xmm(),
                                       (uint32_t) asmjit::x86::Predicate::kCmpEQ);
                                asmjit::x86::Gp r = c.newGpq();
                                c.vmovq(r, lhs.xmm());
                                c.and_(r, 1);
                                registers.push(r);
                            } else {
                                asmjit::x86::Gp t = c.newGpq();
                                c.xor_(t, t);
                                c.cmp(lhs.gp(), rhs.gp());
                                c.sete(t.r8Lo());
                                registers.push(t);
                            }
                            break;
                        case NE:
                            if (lhs.isXmm()) {
                                c.emit(asmjit::x86::Inst::kIdCmpsd, lhs.xmm(), rhs.xmm(),
                                       (uint32_t) asmjit::x86::Predicate::kCmpNEQ);
                                asmjit::x86::Gp r = c.newGpq();
                                c.vmovq(r, lhs.xmm());
                                c.and_(r, 1);
                                registers.push(r);
                            } else {
                                asmjit::x86::Gp t = c.newGpq();
                                c.xor_(t, t);
                                c.cmp(lhs.gp(), rhs.gp());
                                c.setne(t.r8Lo());
                                registers.push(t);
                            }
                            break;
                        case GT:
                            if (lhs.isXmm()) {
                                c.emit(asmjit::x86::Inst::kIdCmpsd, lhs.xmm(), rhs.xmm(),
                                       (uint32_t) asmjit::x86::Predicate::kCmpNLE);
                                asmjit::x86::Gp r = c.newGpq();
                                c.vmovq(r, lhs.xmm());
                                c.and_(r, 1);
                                registers.push(r);
                            } else {
                                if (!null_check) {
                                    asmjit::x86::Gp t = c.newGpq();
                                    c.xor_(t, t);
                                    c.cmp(lhs.gp(), rhs.gp());
                                    c.setg(t.r8Lo());
                                    registers.push(t);
                                } else {
                                    auto nc = (lhs.dtype() == i32 || rhs.dtype() == i32) ? int_null : long_null;
                                    asmjit::x86::Gp n = c.newGpq();
                                    c.movabs(n, nc);
                                    c.cmp(rhs.gp(), n);
                                    c.setne(n.r8Lo());
                                    c.cmp(lhs.gp(), rhs.gp());
                                    asmjit::x86::Gp l = c.newGpq();
                                    c.setg(l.r8Lo());
                                    c.and_(l.r8Lo(), n.r8Lo());
                                    c.movzx(lhs.gp(), l.r8Lo());
                                    registers.push(lhs.gp());
                                }
                            }
                            break;
                        case GE:
                            if (lhs.isXmm()) {
                                c.emit(asmjit::x86::Inst::kIdCmpsd, lhs.xmm(), rhs.xmm(),
                                       (uint32_t) asmjit::x86::Predicate::kCmpNLT);
                                asmjit::x86::Gp r = c.newGpq();
                                c.vmovq(r, lhs.xmm());
                                c.and_(r, 1);
                                registers.push(r);
                            } else {
                                if (!null_check) {
                                    asmjit::x86::Gp t = c.newGpq();
                                    c.xor_(t, t);
                                    c.cmp(lhs.gp(), rhs.gp());
                                    c.setge(t.r8Lo());
                                    registers.push(t);
                                } else {
                                    auto nc = (lhs.dtype() == i32 || rhs.dtype() == i32) ? int_null : long_null;
                                    asmjit::x86::Gp n = c.newGpq();
                                    c.movabs(n, nc);
                                    c.cmp(lhs.gp(), n);
                                    asmjit::x86::Gp l = c.newGpq();
                                    c.setne(l.r8Lo());
                                    c.cmp(rhs.gp(), n);
                                    c.setne(n.r8Lo());
                                    c.and_(l.r8Lo(), n.r8Lo());
                                    c.cmp(lhs.gp(), rhs.gp());
                                    c.setge(n.r8Lo());
                                    c.and_(l.r8Lo(), n.r8Lo());
                                    c.movzx(lhs.gp(), l.r8Lo());
                                    registers.push(lhs.gp());
                                }
                            }
                            break;
                        case LT:
                            if (lhs.isXmm()) {
                                c.emit(asmjit::x86::Inst::kIdCmpsd, lhs.xmm(), rhs.xmm(),
                                       (uint32_t) asmjit::x86::Predicate::kCmpLT);
                                asmjit::x86::Gp r = c.newGpq();
                                c.vmovq(r, lhs.xmm());
                                c.and_(r, 1);
                                registers.push(r);
                            } else {
                                if (!null_check) {
                                    asmjit::x86::Gp t = c.newGpq();
                                    c.xor_(t, t);
                                    c.cmp(lhs.gp(), rhs.gp());
                                    c.setl(t.r8Lo());
                                    registers.push(t);
                                } else {
                                    auto nc = (lhs.dtype() == i32 || rhs.dtype() == i32) ? int_null : long_null;
                                    asmjit::x86::Gp n = c.newGpq();
                                    c.movabs(n, nc);
                                    c.cmp(lhs.gp(), n);
                                    c.setne(n.r8Lo());
                                    c.cmp(lhs.gp(), rhs.gp());
                                    asmjit::x86::Gp l = c.newGpq();
                                    c.setl(l.r8Lo());
                                    c.and_(l.r8Lo(), n.r8Lo());
                                    c.movzx(lhs.gp(), l.r8Lo());
                                    registers.push(lhs.gp());
                                }
                            }
                            break;
                        case LE:
                            if (lhs.isXmm()) {
                                c.emit(asmjit::x86::Inst::kIdCmpsd, lhs.xmm(), rhs.xmm(),
                                       (uint32_t) asmjit::x86::Predicate::kCmpLE);
                                asmjit::x86::Gp r = c.newGpq();
                                c.vmovq(r, lhs.xmm());
                                c.and_(r, 1);
                                registers.push(r);
                            } else {
                                if (!null_check) {
                                    asmjit::x86::Gp t = c.newGpq();
                                    c.xor_(t, t);
                                    c.cmp(lhs.gp(), rhs.gp());
                                    c.setle(t.r8Lo());
                                    registers.push(t);
                                } else {
                                    auto nc = (lhs.dtype() == i32 || rhs.dtype() == i32) ? int_null : long_null;
                                    asmjit::x86::Gp n = c.newGpq();
                                    c.movabs(n, nc);
                                    c.cmp(lhs.gp(), n);
                                    asmjit::x86::Gp l = c.newGpq();
                                    c.setne(l.r8Lo());
                                    c.cmp(rhs.gp(), n);
                                    c.setne(n.r8Lo());
                                    c.and_(l.r8Lo(), n.r8Lo());
                                    c.cmp(lhs.gp(), rhs.gp());
                                    c.setle(n.r8Lo());
                                    c.and_(l.r8Lo(), n.r8Lo());
                                    c.movzx(lhs.gp(), l.r8Lo());
                                    registers.push(lhs.gp());
                                }
                            }
                            break;
                        case ADD:
                            if (lhs.isXmm()) {
                                c.vaddsd(lhs.xmm(), rhs.xmm(), lhs.xmm());
                            } else {
                                if (!null_check) {
                                    c.add(lhs.gp(), rhs.gp());
                                } else {
                                    auto nc = (lhs.dtype() == i32 || rhs.dtype() == i32) ? int_null : long_null;
                                    asmjit::x86::Gp t = c.newGpq();
                                    c.mov(t, lhs.gp());
                                    asmjit::x86::Mem m = asmjit::x86::Mem(lhs.gp(), rhs.gp(), 0, 0);
                                    c.lea(lhs.gp(), m);
                                    if(nc == int_null) {
                                        c.cmp(t, nc);
                                        c.cmove(lhs.gp(), t);
                                        c.cmp(rhs.gp(), nc);
                                        c.cmove(lhs.gp(), rhs.gp());
                                    } else {
                                        asmjit::x86::Gp n = c.newGpq();
                                        c.mov(n, nc);
                                        c.cmp(t, n);
                                        c.cmove(lhs.gp(), t);
                                        c.cmp(rhs.gp(), n);
                                        c.cmove(lhs.gp(), rhs.gp());
                                    }
                                }
                            }
                            registers.push(lhs);
                            break;
                        case SUB:
                            if (lhs.isXmm()) {
                                c.vsubsd(lhs.xmm(), lhs.xmm(), rhs.xmm());
                            } else {
                                if (!null_check) {
                                    c.sub(lhs.gp(), rhs.gp());
                                } else {
                                    auto nc = (lhs.dtype() == i32 || rhs.dtype() == i32) ? int_null : long_null;
                                    asmjit::x86::Gp t = c.newGpq();
                                    c.mov(t, lhs.gp());
                                    c.sub(lhs.gp(), rhs.gp());
                                    asmjit::x86::Gp n = c.newGpq();
                                    c.movabs(n, nc);
                                    c.cmp(t, n);
                                    c.cmove(lhs.gp(), t);
                                    c.cmp(rhs.gp(), n);
                                    c.cmove(lhs.gp(), rhs.gp());
                                }
                            }
                            registers.push(lhs);
                            break;
                        case MUL:
                            if (lhs.isXmm()) {
                                c.vmulsd(lhs.xmm(), rhs.xmm(), lhs.xmm());
                            } else {
                                if (!null_check) {
                                    c.imul(lhs.gp(), rhs.gp());
                                } else {
                                    auto nc = (lhs.dtype() == i32 || rhs.dtype() == i32) ? int_null : long_null;
                                    asmjit::x86::Gp t = c.newGpq();
                                    c.mov(t, lhs.gp());
                                    c.imul(lhs.gp(), rhs.gp());
                                    asmjit::x86::Gp n = c.newGpq();
                                    c.movabs(n, nc);
                                    c.cmp(t, n);
                                    c.cmove(lhs.gp(), t);
                                    c.cmp(rhs.gp(), n);
                                    c.cmove(lhs.gp(), rhs.gp());
                                }
                            }
                            registers.push(lhs);
                            break;
                        case DIV:
                            if (lhs.isXmm()) {
                                c.vdivsd(lhs.xmm(), lhs.xmm(), rhs.xmm());
                            } else {
                                if (!null_check) {
                                    asmjit::Label l_zero = c.newLabel();
                                    asmjit::x86::Gp r = c.newGpq();
                                    c.mov(r, rhs.gp());
                                    c.test(r, r);
                                    c.je(l_zero);
                                    c.cqo(r, lhs.gp());
                                    c.idiv(r, lhs.gp(), rhs.gp());
                                    c.mov(r, lhs.gp());
                                    c.bind(l_zero);
                                    c.mov(lhs.gp(), r);
                                } else {
                                    auto nc = (lhs.dtype() == i32 || rhs.dtype() == i32) ? int_null : long_null;
                                    asmjit::Label l_zero = c.newLabel();
                                    asmjit::x86::Gp r = c.newGpq();
                                    c.movabs(r, nc);
                                    c.cmp(r, lhs.gp());
                                    c.je(l_zero);
                                    c.cmp(r, rhs.gp());
                                    c.je(l_zero);
                                    c.mov(r, rhs.gp());
                                    c.test(r, r);
                                    c.je(l_zero);

                                    c.xor_(r, r);
                                    c.idiv(r, lhs.gp(), rhs.gp());
                                    c.mov(r, lhs.gp());

                                    c.bind(l_zero);
                                    c.mov(lhs.gp(), r);
                                }
                            }
                            registers.push(lhs);
                            break;
                        default:
                            break; // dead case
                    }
            }
        }
        auto mask = registers.top();
        registers.pop();

        c.mov(qword_ptr(rows_ptr, output_index, 3), input_index);
        c.add(output_index, mask.gp());

        c.add(input_index, 1);
        c.cmp(input_index, stop);
        c.jl(l_loop); // input_index < stop
        c.bind(l_exit);
    }

    void scalar_loop(const uint8_t *filter_expr, size_t filter_size, bool null_check) {
        if (filter_expr == nullptr || filter_size <= 0) {
            return; //todo: report error
        }

        scalar_tail(filter_expr, filter_size, null_check, rows_size);
        c.ret(output_index);
    }

    void avx2_loop(const uint8_t *filter_expr, size_t filter_size, uint32_t step, bool null_check) {
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
        c.sub(stop, step + 1); // stop = rows_size - step + 1

        c.cmp(input_index, stop);
        c.jge(l_exit);

        c.bind(l_loop);

        uint32_t rpos = 0;
        while (rpos < filter_size) {
            auto ic = static_cast<instruction_t>(read<uint8_t>(filter_expr, filter_size, rpos));
            switch (ic) {
                case RET:
                    break;
                case MEM_I1: {
                    Gp col = c.newGpq();
                    auto column_index = static_cast<int32_t>(read<int64_t>(filter_expr, filter_size, rpos));
                    c.mov(col, ptr(cols_ptr, 8 * column_index, 8));
                    Ymm data = c.newYmm();
                    c.vmovdqu(data, ymmword_ptr(col, input_index, 0));
                    registers.push(jit_value_t(data, i8));
                }
                    break;
                case MEM_I2: {
                    Gp col = c.newGpq();
                    auto column_input_index = static_cast<int32_t>(read<int64_t>(filter_expr, filter_size, rpos));
                    c.mov(col, ptr(cols_ptr, 8 * column_input_index, 8));
                    Ymm data = c.newYmm();
                    c.vmovdqu(data, ymmword_ptr(col, input_index, 1));
                    registers.push(jit_value_t(data, i16));
                }
                    break;
                case MEM_I4: {
                    Gp col = c.newGpq();
                    auto column_input_index = static_cast<int32_t>(read<int64_t>(filter_expr, filter_size, rpos));
                    c.mov(col, ptr(cols_ptr, 8 * column_input_index, 8));
                    Ymm data = c.newYmm();
                    c.vmovdqu(data, ymmword_ptr(col, input_index, 2));
                    registers.push(jit_value_t(data, i32));
                }
                    break;
                case MEM_I8: {
                    Gp col = c.newGpq();
                    auto column_input_index = static_cast<int32_t>(read<int64_t>(filter_expr, filter_size, rpos));
                    c.mov(col, ptr(cols_ptr, 8 * column_input_index, 8));
                    Ymm data = c.newYmm();
                    c.vmovdqu(data, ymmword_ptr(col, input_index, 3));
                    registers.push(jit_value_t(data, i64));
                }
                    break;
                case MEM_F4: {
                    Gp col = c.newGpq();
                    auto column_input_index = static_cast<int32_t>(read<int64_t>(filter_expr, filter_size, rpos));
                    c.mov(col, ptr(cols_ptr, 8 * column_input_index, 8));
                    Ymm data = c.newYmm();
                    c.vmovups(data, ymmword_ptr(col, input_index, 2));
                    registers.push(jit_value_t(data, f32));
                }
                    break;
                case MEM_F8: {
                    Gp col = c.newGpq();
                    auto column_input_index = static_cast<int32_t>(read<int64_t>(filter_expr, filter_size, rpos));
                    c.mov(col, ptr(cols_ptr, 8 * column_input_index, 8));
                    Ymm data = c.newYmm();
                    c.vmovupd(data, ymmword_ptr(col, input_index, 3));
                    registers.push(jit_value_t(data, f64));
                }
                    break;
                case IMM_I1: {
                    auto value = read<int64_t>(filter_expr, filter_size, rpos);
                    Mem c0 = c.newConst(ConstPool::kScopeLocal, &value, 1);
                    Ymm val = c.newYmm();
                    c.vpbroadcastb(val, c0);
                    registers.push(jit_value_t(val, i8));
                }
                    break;
                case IMM_I2: {
                    auto value = read<int64_t>(filter_expr, filter_size, rpos);
                    Mem c0 = c.newInt16Const(ConstPool::kScopeLocal, (int16_t) value);
                    Ymm val = c.newYmm();
                    c.vpbroadcastw(val, c0);
                    registers.push(jit_value_t(val, i16));
                }
                    break;
                case IMM_I4: {
                    auto value = read<int64_t>(filter_expr, filter_size, rpos);
                    Mem c0 = c.newInt32Const(ConstPool::kScopeLocal, (int32_t) value);
                    Ymm val = c.newYmm();
                    c.vpbroadcastd(val, c0);
                    registers.push(jit_value_t(val, i32));
                }
                    break;
                case IMM_I8: {
                    auto value = read<int64_t>(filter_expr, filter_size, rpos);
                    Mem c0 = c.newInt64Const(ConstPool::kScopeLocal, value);
                    Ymm val = c.newYmm();
                    c.vpbroadcastq(val, c0);
                    registers.push(jit_value_t(val, i64));
                }
                    break;
                case IMM_F4: {
                    auto value = read<double>(filter_expr, filter_size, rpos); //todo: change serialization format
                    Mem c0 = c.newFloatConst(ConstPool::kScopeLocal, (float) value);
                    Ymm val = c.newYmm();
                    c.vbroadcastss(val, c0);
                    registers.push(jit_value_t(val, f32));
                }
                    break;
                case IMM_F8: {
                    auto value = read<double>(filter_expr, filter_size, rpos); //todo: change serialization format
                    Mem c0 = c.newDoubleConst(ConstPool::kScopeLocal, value);
                    Ymm val = c.newYmm();
                    c.vbroadcastsd(val, c0);
                    registers.push(jit_value_t(val, f64));
                }
                    break;
                case NEG: {
                    auto arg = registers.top();
                    registers.pop();
                    if(!null_check) {
                        avx2_neg(c, arg, arg);
                    } else {
                        Ymm r = c.newYmm();
                        jit_value_t v(r, arg.dtype(), arg.dkind());
                        avx2_not_null(c, v, arg, arg);
                        avx2_neg(c, arg, arg);
                        avx2_not(c, v, v);
                        avx2_select_byte(c, arg, arg, v);
                    }
                    registers.push(arg);
                }
                    break;
                case NOT: {
                    auto arg = registers.top();
                    registers.pop();
                    avx2_not(c, arg, arg);
                    registers.push(arg);
                }
                    break;
                default:
                    auto lhs = registers.top();
                    registers.pop();
                    auto rhs = registers.top();
                    registers.pop();
                    switch (ic) {
                        case AND:
                            avx2_and(c, lhs, lhs, rhs);
                            registers.push(lhs);
                            break;
                        case OR:
                            avx2_or(c, lhs, lhs, rhs);
                            registers.push(lhs);
                            break;
                        case EQ:
                            avx2_cmp_eq(c, lhs, lhs, rhs);
                            registers.push(lhs);
                            break;
                        case NE:
                            avx2_cmp_neq(c, lhs, lhs, rhs);
                            registers.push(lhs);
                            break;
                        case GT:
                            if (!null_check || is_float(lhs) || is_float(rhs)) {
                                avx2_cmp_gt(c, lhs, lhs, rhs);
                                registers.push(lhs);
                            } else {
                                Ymm r = c.newYmm();
                                jit_value_t v(r, lhs.dtype(), lhs.dkind());
                                avx2_cmp_gt(c, v, lhs, rhs);
                                avx2_ignore_null(c, v, lhs, rhs);
                                registers.push(v);
                            }
                            break;
                        case GE:
                            if (!null_check || is_float(lhs) || is_float(rhs)) {
                                avx2_cmp_ge(c, lhs, lhs, rhs);
                                registers.push(lhs);
                            } else {
                                Ymm r = c.newYmm();
                                jit_value_t v(r, lhs.dtype(), lhs.dkind());
                                avx2_cmp_ge(c, v, lhs, rhs);
                                avx2_ignore_null(c, v, lhs, rhs);
                                registers.push(v);
                            }
                            break;
                        case LT:
                            if (!null_check || is_float(lhs) || is_float(rhs)) {
                                avx2_cmp_lt(c, lhs, lhs, rhs);
                                registers.push(lhs);
                            } else {
                                Ymm r = c.newYmm();
                                jit_value_t v(r, lhs.dtype(), lhs.dkind());
                                avx2_cmp_lt(c, v, lhs, rhs);
                                avx2_ignore_null(c, v, lhs, rhs);
                                registers.push(v);
                            }
                            break;
                        case LE:
                            if (!null_check || is_float(lhs) || is_float(rhs)) {
                                avx2_cmp_le(c, lhs, lhs, rhs);
                                registers.push(lhs);
                            } else {
                                Ymm r = c.newYmm();
                                jit_value_t v(r, lhs.dtype(), lhs.dkind());
                                avx2_cmp_le(c, v, lhs, rhs);
                                avx2_ignore_null(c, v, lhs, rhs);
                                registers.push(v);
                            }
                            break;
                        case ADD:
                            if (!null_check || is_float(lhs) || is_float(rhs)) {
                                avx2_add(c, lhs, lhs, rhs);
                            } else {
                                Ymm r = c.newYmm();
                                jit_value_t v(r, lhs.dtype(), lhs.dkind());
                                avx2_not_null(c, v, lhs, rhs);
                                avx2_add(c, lhs, lhs, rhs);
                                avx2_not(c, v, v);
                                avx2_select_byte(c, lhs, lhs, v);
                            }
                            registers.push(lhs);
                            break;
                        case SUB:
                            if (!null_check || is_float(lhs) || is_float(rhs)) {
                                avx2_sub(c, lhs, lhs, rhs);
                            } else {
                                Ymm r = c.newYmm();
                                jit_value_t v(r, lhs.dtype(), lhs.dkind());
                                avx2_not_null(c, v, lhs, rhs);
                                avx2_sub(c, lhs, lhs, rhs);
                                avx2_not(c, v, v);
                                avx2_select_byte(c, lhs, lhs, v);
                            }
                            registers.push(lhs);
                            break;
                        case MUL:
                            if (!null_check || is_float(lhs) || is_float(rhs)) {
                                avx2_mul(c, lhs, lhs, rhs);
                            } else {
                                Ymm r = c.newYmm();
                                jit_value_t v(r, lhs.dtype(), lhs.dkind());
                                avx2_not_null(c, v, lhs, rhs);
                                avx2_mul(c, lhs, lhs, rhs);
                                avx2_not(c, v, v);
                                avx2_select_byte(c, lhs, lhs, v);
                            }
                            registers.push(lhs);
                            break;
                        case DIV:
                            if (!null_check || is_float(lhs) || is_float(rhs)) {
                                avx2_div(c, lhs, lhs, rhs);
                            } else {
                                Ymm r = c.newYmm();
                                jit_value_t v(r, lhs.dtype(), lhs.dkind());
                                avx2_not_null(c, v, lhs, rhs);
                                avx2_div(c, lhs, lhs, rhs);
                                avx2_not(c, v, v);
                                avx2_select_byte(c, lhs, lhs, v);
                            }
                            registers.push(lhs);
                            break;
                        default:
                            break; // dead case
                    }
            }
        }
        auto mask = registers.top();
        registers.pop();

        Gp bits = x86::r10;
        to_bits(c, bits, mask.ymm(), step);
        unrolled_loop2(c, bits, rows_ptr, input_index, output_index, step);

        c.add(input_index, step); // index += step
        c.cmp(input_index, stop);
        c.jl(l_loop); // index < stop
        c.bind(l_exit);

        scalar_tail(filter_expr, filter_size, null_check, rows_size);
        c.ret(output_index);
    }

    void begin_fn() {
        c.addFunc(FuncSignatureT<int64_t, int64_t *, int64_t, int64_t *, int64_t, int64_t>(CallConv::kIdHost));
        cols_ptr = c.newIntPtr("cols");
        cols_size = c.newInt64("cols_size");

        c.setArg(0, cols_ptr);
        c.setArg(1, cols_size);

        rows_ptr = c.newIntPtr("rows");
        rows_size = c.newInt64("rows_size");

        c.setArg(2, rows_ptr);
        c.setArg(3, rows_size);

        input_index = c.newGpq();
        c.mov(input_index, 0);

        output_index = c.newInt64("row_id_hi");
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

    int64_t long_nulls_array[4] = {long_null, long_null, long_null, long_null};
    int64_t long_true_mask_array[4] = {-1ll, -1ll, -1ll, -1ll};

    int32_t int_nulls_array[8] = {int_null, int_null, int_null, int_null, int_null, int_null, int_null, int_null};
    int32_t int_true_mask_array[8] = {-1, -1, -1, -1, -1, -1, -1, -1};

//    x86::Mem long_nulls = c.newConst(ConstPool::kScopeLocal, &long_nulls_array, 32);
//    x86::Mem long_true_mask = c.newConst(ConstPool::kScopeLocal, &long_true_mask_array, 32);
//    x86::Mem int_nulls = c.newConst(ConstPool::kScopeLocal, &int_nulls_array, 32);
//    x86::Mem int_true_mask = c.newConst(ConstPool::kScopeLocal, &int_true_mask_array, 32);
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
    logger.addFlags(FormatOptions::kFlagRegCasts |
                    FormatOptions::kFlagExplainImms |
                    FormatOptions::kFlagAnnotations);

    code.setLogger(&logger);
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
