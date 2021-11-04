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

int64_t compile_x86_scalar_loop(const uint64_t *columns,
                                int64_t column_size,
                                uint8_t *instrs,
                                int64_t i_count,
                                int64_t *rows,
                                int64_t r_count,
                                int64_t rowid_start) {

    if (nullptr == columns ||
        nullptr == instrs ||
        nullptr == rows ||
        i_count <= 0 ||
        r_count <= 0 ||
        column_size <= 0) {
        return 0;
    }

    using namespace asmjit;
    using namespace asmjit::x86;

    JitRuntime rt;
    CodeHolder code;
    code.init(rt.environment());
    //FileLogger logger(stdout);
    //code.setLogger(&logger);
    Compiler c(&code);

    std::stack<jit_value_t> tmp;

    using func_t = int64_t (*)(int64_t *rows, int64_t rows_count);
    c.addFunc(FuncSignatureT<int64_t, int64_t *, int64_t>(CallConv::kIdHost));

    Gp rows_ptr = c.newIntPtr("rows");
    Gp rows_count = c.newInt64("r_count");

    c.setArg(0, rows_ptr);
    c.setArg(1, rows_count);

    Gp output = c.newGpq();
    c.mov(output, rowid_start);

    int64_t step = 1; // this is scalar loop version
    const int64_t stop = r_count;

    Label l_loop = c.newLabel();
    Label l_exit = c.newLabel();

    Gp index = c.newGpq();
    c.mov(index, 0);

    c.cmp(index, r_count);
    c.jge(l_exit);

    c.bind(l_loop);

    uint32_t rpos = 0;
    while (rpos < i_count) {
        auto ic = static_cast<instruction_t>(read<uint8_t>(instrs, i_count, rpos));
        switch (ic) {
            case RET:
                break;
            case MEM_I1: {
                Gp col = c.newGpq();
                auto column_index = read<uint64_t>(instrs, i_count, rpos);
                uint64_t column_addr = columns[column_index];
                c.mov(col, column_addr);
                c.movsx(col, Mem(col, index, 0, 0, 1));
                tmp.push(col);
            }
                break;
            case MEM_I2: {
                Gp col = c.newGpq();
                auto column_index = read<uint64_t>(instrs, i_count, rpos);
                uint64_t column_addr = columns[column_index];
                c.mov(col, column_addr);
                c.movsx(col, Mem(col, index, 1, 0, 2));
                tmp.push(col);
            }
                break;
            case MEM_I4: {
                Gp col = c.newGpq();
                auto column_index = read<uint64_t>(instrs, i_count, rpos);
                uint64_t column_addr = columns[column_index];
                c.mov(col, column_addr);
                c.movsxd(col, Mem(col, index, 2, 0, 4));
                tmp.push(col);
            }
                break;
            case MEM_I8: {
                Gp col = c.newGpq();
                auto column_index = read<uint64_t>(instrs, i_count, rpos);
                uint64_t column_addr = columns[column_index];
                c.mov(col, column_addr);
                c.mov(col, Mem(col, index, 3, 0, 8));
                tmp.push(col);
            }
                break;
            case MEM_F4: {
                Gp col = c.newGpq();
                auto column_index = read<uint64_t>(instrs, i_count, rpos);
                uint64_t column_addr = columns[column_index];
                c.mov(col, column_addr);
                Xmm data = c.newXmm();
//                c.vmovss(data, Mem(col, index, 2, 0, 4));
                c.cvtss2sd(data, Mem(col, index, 2, 0, 4)); // float to double
                tmp.push(data);
            }
                break;
            case MEM_F8: {
                Gp col = c.newGpq();
                auto column_index = read<uint64_t>(instrs, i_count, rpos);
                uint64_t column_addr = columns[column_index];
                c.mov(col, column_addr);
                Xmm data = c.newXmm();
                c.vmovsd(data, Mem(col, index, 3, 0, 8));
                tmp.push(data);
            }
                break;
            case IMM_I1:
            case IMM_I2:
            case IMM_I4:
            case IMM_I8: {
                Gp val = c.newGpq();
                auto value = read<uint64_t>(instrs, i_count, rpos);
                c.mov(val, value);
                tmp.push(val);
            }
                break;
            case IMM_F4:
            case IMM_F8: {
                auto value = read<double>(instrs, i_count, rpos);
                Mem c0 = c.newDoubleConst(ConstPool::kScopeLocal, value);
                Xmm val = c.newXmm();
                c.movsd(val, c0);
                tmp.push(val);
            }
                break;
            case NEG: {
                auto arg = tmp.top();
                tmp.pop();
                if (arg.isXmm()) {
                    Xmm r = c.newXmmSd();
                    c.xorpd(r, r);
                    c.subpd(r, arg.xmm());
                    arg = r;
                } else {
                    c.neg(arg.gp());
                }
                tmp.push(arg);
            }
                break;
            case NOT: {
                auto arg = tmp.top();
                tmp.pop();
                if (arg.isXmm()) {
                    // error?
                } else {
                    c.not_(arg.gp());
                    c.and_(arg.gp(), 1);
                }
                tmp.push(arg);
            }
                break;
            default:
                auto rhs = tmp.top();
                tmp.pop();
                auto lhs = tmp.top();
                tmp.pop();
                if (rhs.isXmm() && !lhs.isXmm()) {
                    // lhs long to double
                    Gp i = lhs.gp();
                    lhs = c.newXmm();
                    c.vcvtsi2sd(lhs.xmm(), rhs.xmm(), i);
                }
                if (lhs.isXmm() && !rhs.isXmm()) {
                    // rhs long to double
                    Gp i = rhs.gp();
                    rhs = c.newXmm();
                    c.vcvtsi2sd(rhs.xmm(), lhs.xmm(), i);
                }
                switch (ic) {
                    case AND:
                        c.and_(lhs.gp(), rhs.gp());
                        tmp.push(lhs);
                        break;
                    case OR:
                        c.or_(lhs.gp(), rhs.gp());
                        tmp.push(lhs);
                        break;
                    case EQ:
                        if (lhs.isXmm()) {
                            c.emit(x86::Inst::kIdCmpsd, lhs.xmm(), rhs.xmm(), (uint32_t) x86::Predicate::kCmpEQ);
                            Gp r = c.newGpq();
                            c.vmovq(r, lhs.xmm());
                            c.and_(r, 1);
                            tmp.push(r);
                        } else {
                            Gp t = c.newGpq();
                            c.xor_(t, t);
                            c.cmp(lhs.gp(), rhs.gp());
                            c.sete(t.r8Lo());
                            tmp.push(t);
                        }
                        break;
                    case NE:
                        if (lhs.isXmm()) {
                            c.emit(x86::Inst::kIdCmpsd, lhs.xmm(), rhs.xmm(), (uint32_t) x86::Predicate::kCmpNEQ);
                            Gp r = c.newGpq();
                            c.vmovq(r, lhs.xmm());
                            c.and_(r, 1);
                            tmp.push(r);
                        } else {
                            Gp t = c.newGpq();
                            c.xor_(t, t);
                            c.cmp(lhs.gp(), rhs.gp());
                            c.setne(t.r8Lo());
                            tmp.push(t);
                        }
                        break;
                    case GT:
                        if (lhs.isXmm()) {
                            c.emit(x86::Inst::kIdCmpsd, lhs.xmm(), rhs.xmm(), (uint32_t) x86::Predicate::kCmpNLE);
                            Gp r = c.newGpq();
                            c.vmovq(r, lhs.xmm());
                            c.and_(r, 1);
                            tmp.push(r);
                        } else {
                            Gp t = c.newGpq();
                            c.xor_(t, t);
                            c.cmp(lhs.gp(), rhs.gp());
                            c.setg(t.r8Lo());
                            tmp.push(t);
                        }
                        break;
                    case GE:
                        if (lhs.isXmm()) {
                            c.emit(x86::Inst::kIdCmpsd, lhs.xmm(), rhs.xmm(), (uint32_t) x86::Predicate::kCmpNLT);
                            Gp r = c.newGpq();
                            c.vmovq(r, lhs.xmm());
                            c.and_(r, 1);
                            tmp.push(r);
                        } else {
                            Gp t = c.newGpq();
                            c.xor_(t, t);
                            c.cmp(lhs.gp(), rhs.gp());
                            c.setge(t.r8Lo());
                            tmp.push(t);
                        }
                        break;
                    case LT:
                        if (lhs.isXmm()) {
                            c.emit(x86::Inst::kIdCmpsd, lhs.xmm(), rhs.xmm(), (uint32_t) x86::Predicate::kCmpLT);
                            Gp r = c.newGpq();
                            c.vmovq(r, lhs.xmm());
                            c.and_(r, 1);
                            tmp.push(r);
                        } else {
                            Gp t = c.newGpq();
                            c.xor_(t, t);
                            c.cmp(lhs.gp(), rhs.gp());
                            c.setl(t.r8Lo());
                            tmp.push(t);
                        }
                        break;
                    case LE:
                        if (lhs.isXmm()) {
                            c.emit(x86::Inst::kIdCmpsd, lhs.xmm(), rhs.xmm(), (uint32_t) x86::Predicate::kCmpLE);
                            Gp r = c.newGpq();
                            c.vmovq(r, lhs.xmm());
                            c.and_(r, 1);
                            tmp.push(r);
                        } else {
                            Gp t = c.newGpq();
                            c.xor_(t, t);
                            c.cmp(lhs.gp(), rhs.gp());
                            c.setle(t.r8Lo());
                            tmp.push(t);
                        }
                        break;
                    case ADD:
                        if (lhs.isXmm()) {
                            c.vaddsd(lhs.xmm(), rhs.xmm(), lhs.xmm());
                        } else {
                            c.add(lhs.gp(), rhs.gp());
                        }
                        tmp.push(lhs);
                        break;
                    case SUB:
                        if (lhs.isXmm()) {
                            c.vsubsd(lhs.xmm(), lhs.xmm(), rhs.xmm());
                        } else {
                            c.sub(lhs.gp(), rhs.gp());
                        }
                        tmp.push(lhs);
                        break;
                    case MUL:
                        if (lhs.isXmm()) {
                            c.vmulsd(lhs.xmm(), rhs.xmm(), lhs.xmm());
                        } else {
                            c.imul(lhs.gp(), rhs.gp());
                        }
                        tmp.push(lhs);
                        break;
                    case DIV:
                        if (lhs.isXmm()) {
                            c.vdivsd(lhs.xmm(), lhs.xmm(), rhs.xmm());
                        } else {
                            Gp r = c.newGpq();
                            c.xor_(r, r);
                            c.idiv(r, lhs.gp(), rhs.gp());
                        }
                        tmp.push(lhs);
                        break;
                    default:
                        break; // dead case
                }
        }
    }
    auto mask = tmp.top();
    tmp.pop();

    c.mov(qword_ptr(rows_ptr, output, 3), index);
    c.add(output, mask.gp());

    c.add(index, step); // index += step
    c.cmp(index, stop);
    c.jl(l_loop); // index < stop
    c.bind(l_exit);

    c.ret(output);
    c.endFunc();
    c.finalize();

    func_t fn;
    Error err = rt.add(&fn, &code);
    if (err) {
        std::cerr << "some error happened" << std::endl;
    }
    auto res = fn(rows, r_count);
    rt.release(fn);
    return res;
}

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
        case i64:
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

inline static void to_bits2(asmjit::x86::Compiler &c, asmjit::x86::Gp &dst, const asmjit::x86::Ymm &x) {
    asmjit::x86::Xmm l = get_low(c, x);
    asmjit::x86::Xmm h = get_high(c, x);
    c.packssdw(l, h); // 32-bit dwords to 16-bit words
    c.packsswb(l, l); // 16-bit words to bytes
    c.pmovmskb(dst, l);
    c.and_(dst, 0xff);
}

inline static void unrolled_loop2(asmjit::x86::Compiler &c,
                                  const asmjit::x86::Gp &bits,
                                  const asmjit::x86::Gp &rows_ptr,
                                  const asmjit::x86::Gp &input,
                                  asmjit::x86::Gp &output) {
    asmjit::x86::Gp offset = c.newInt64();
    for (int i = 0; i < 8; ++i) {
        c.mov(qword_ptr(rows_ptr, output, 3), input);
        c.mov(offset, bits);
        c.shr(offset, i);
        c.and_(offset, 1);
        c.add(output, offset);
    }
}

int64_t compile_x86_avx2_loop(const uint64_t *columns,
                              int64_t column_size,
                              uint8_t *instrs,
                              int64_t i_count,
                              int64_t *rows,
                              int64_t r_count,
                              int64_t rowid_start) {

    if (nullptr == columns ||
        nullptr == instrs ||
        nullptr == rows ||
        i_count <= 0 ||
        r_count <= 0 ||
        column_size <= 0) {
        return 0;
    }

    using namespace asmjit;
    using namespace asmjit::x86;

    JitRuntime rt;
    CodeHolder code;
    code.init(rt.environment());
    //FileLogger logger(stdout);
    //code.setLogger(&logger);
    Compiler c(&code);

    std::stack<jit_value_t> tmp;

    using func_t = int64_t (*)(int64_t *rows, int64_t rows_count);
    c.addFunc(FuncSignatureT<int64_t, int64_t *, int64_t>(CallConv::kIdHost));

    Gp rows_ptr = c.newIntPtr("rows");
    Gp rows_count = c.newInt64("r_count");

    c.setArg(0, rows_ptr);
    c.setArg(1, rows_count);

    Gp output = c.newGpq();
    c.mov(output, rowid_start);

    constexpr int64_t step = 8; // todo: size ??
    const int64_t stop = r_count - step + 1;

    Label l_loop = c.newLabel();
    Label l_exit = c.newLabel();

    Gp index = c.newGpq();
    c.cmp(index, stop);
    c.jge(l_exit);

    c.bind(l_loop);

    uint32_t rpos = 0;
    while (rpos < i_count) {
        auto ic = static_cast<instruction_t>(read<uint8_t>(instrs, i_count, rpos));
        switch (ic) {
            case RET:
                break;
            case MEM_I1:
            case MEM_I2:
            case MEM_I4:
            case MEM_I8: {
                Gp col = c.newGpq();
                auto column_index = read<uint64_t>(instrs, i_count, rpos);
                uint64_t column_addr = columns[column_index];
                c.mov(col, column_addr);
                Ymm data = c.newYmm();
                c.vmovdqu(data, ymmword_ptr(col, index));
                tmp.push(data);
            }
                break;
            case MEM_F4: {
                Gp col = c.newGpq();
                auto column_index = read<uint64_t>(instrs, i_count, rpos);
                uint64_t column_addr = columns[column_index];
                c.mov(col, column_addr);
                Ymm data = c.newYmm();
                c.vmovups(data, ymmword_ptr(col, index));
                tmp.push(data);
            }
                break;
            case MEM_F8: {
                Gp col = c.newGpq();
                auto column_index = read<uint64_t>(instrs, i_count, rpos);
                uint64_t column_addr = columns[column_index];
                c.mov(col, column_addr);
                Ymm data = c.newYmm();
                c.vmovupd(data, ymmword_ptr(col, index));
                tmp.push(data);
            }
                break;
            case IMM_I1: {
                auto value = read<int8_t>(instrs, i_count, rpos);
                Mem c0 = c.newConst(ConstPool::kScopeLocal, &value, 1);
                Ymm val = c.newYmm();
                c.vpbroadcastb(val, c0);
                tmp.push(val);
            }
                break;
            case IMM_I2: {
                auto value = read<int16_t>(instrs, i_count, rpos);
                Mem c0 = c.newInt16Const(ConstPool::kScopeLocal, value);
                Ymm val = c.newYmm();
                c.vpbroadcastw(val, c0);
                tmp.push(val);
            }
                break;
            case IMM_I4: {
                auto value = read<int32_t>(instrs, i_count, rpos);
                Mem c0 = c.newInt32Const(ConstPool::kScopeLocal, value);
                Ymm val = c.newYmm();
                c.vpbroadcastd(val, c0);
                tmp.push(val);
            }
                break;
            case IMM_I8: {
                auto value = read<int64_t>(instrs, i_count, rpos);
                Mem c0 = c.newInt64Const(ConstPool::kScopeLocal, value);
                Ymm val = c.newYmm();
                c.vpbroadcastq(val, c0);
                tmp.push(val);
            }
                break;
            case IMM_F4: {
                auto value = read<float>(instrs, i_count, rpos); //todo: change serialization format
                Mem c0 = c.newFloatConst(ConstPool::kScopeLocal, value);
                Ymm val = c.newYmm();
                c.vbroadcastss(val, c0);
                tmp.push(val);
            }
                break;
            case IMM_F8: {
                auto value = read<double>(instrs, i_count, rpos); //todo: change serialization format
                Mem c0 = c.newDoubleConst(ConstPool::kScopeLocal, value);
                Ymm val = c.newYmm();
                c.vbroadcastsd(val, c0);
                tmp.push(val);
            }
                break;
            case NEG: {
                auto arg = tmp.top();
                tmp.pop();
                avx2_neg(c, arg, arg);
                tmp.push(arg);
            }
                break;
            case NOT: {
                auto arg = tmp.top();
                tmp.pop();
                avx2_not(c, arg, arg);
                tmp.push(arg);
            }
                break;
            default:
                auto rhs = tmp.top();
                tmp.pop();
                auto lhs = tmp.top();
                tmp.pop();
                switch (ic) {
                    case AND:
                        avx2_and(c, lhs, lhs, rhs);
                        tmp.push(lhs);
                        break;
                    case OR:
                        avx2_or(c, lhs, lhs, rhs);
                        tmp.push(lhs);
                        break;
                    case EQ:
                        avx2_cmp_eq(c, lhs, lhs, rhs);
                        tmp.push(lhs);
                        break;
                    case NE:
                        avx2_cmp_neq(c, lhs, lhs, rhs);
                        tmp.push(lhs);
                        break;
                    case GT:
                        avx2_cmp_gt(c, lhs, lhs, rhs);
                        tmp.push(lhs);
                        break;
                    case GE:
                        avx2_cmp_ge(c, lhs, lhs, rhs);
                        tmp.push(lhs);
                        break;
                    case LT:
                        avx2_cmp_lt(c, lhs, lhs, rhs);
                        tmp.push(lhs);
                        break;
                    case LE:
                        avx2_cmp_le(c, lhs, lhs, rhs);
                        tmp.push(lhs);
                        break;
                    case ADD:
                        avx2_add(c, lhs, lhs, rhs);
                        tmp.push(lhs);
                        break;
                    case SUB:
                        avx2_sub(c, lhs, lhs, rhs);
                        tmp.push(lhs);
                        break;
                    case MUL:
                        avx2_mul(c, lhs, lhs, rhs);
                        tmp.push(lhs);
                        break;
                    case DIV:
                        avx2_div(c, lhs, lhs, rhs);
                        tmp.push(lhs);
                        break;
                    default:
                        break; // dead case
                }
        }
    }
    auto mask = tmp.top();
    tmp.pop();

    Gp bits = x86::r10;
    to_bits2(c, bits, mask.ymm());
    unrolled_loop2(c, bits, rows_ptr, index, output);

    c.mov(qword_ptr(rows_ptr, output, 3), index);
    c.add(output, mask.gp());

    c.add(index, step); // index += step
    c.cmp(index, stop);
    c.jl(l_loop); // index < stop
    c.bind(l_exit);

    c.ret(output);
    c.endFunc();
    c.finalize();

    func_t fn;
    Error err = rt.add(&fn, &code);
    if (err) {
        std::cerr << "some error happened" << std::endl;
    }
    auto res = fn(rows, r_count);
    rt.release(fn);
    return res;
}

JNIEXPORT long JNICALL
Java_io_questdb_jit_FiltersCompiler_compile(JNIEnv *e,
                                            jclass cl,
                                            jlong columnsAddr,
                                            jlong columnsSize,
                                            jlong filterAddr,
                                            jlong filterSize,
                                            jlong rowsAddr,
                                            jlong rowsSize,
                                            jlong rowidStart) {
    return compile_x86_scalar_loop(reinterpret_cast<uint64_t *>(columnsAddr),
                                   columnsSize,
                                   reinterpret_cast<uint8_t *>(filterAddr),
                                   filterSize,
                                   reinterpret_cast<int64_t *>(rowsAddr),
                                   rowsSize,
                                   rowidStart);
}
