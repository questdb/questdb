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

#ifndef QUESTDB_JIT_IMPL_X86_H
#define QUESTDB_JIT_IMPL_X86_H

#include "consts.h"

namespace questdb::x86 {
    using namespace asmjit;
    using namespace asmjit::x86;

    inline Gp int32_not(Compiler &c, const Gp &b) {
        Gp r = c.new_gp32();
        c.mov(r, b);
        c.xor_(r, 1);
        return r.as<Gp>();
    }

    // Writes into a fresh register instead of over b1, the way int32_sub, int32_mul and every other
    // binary helper in this file already do - and the way aarch64's int32_and does.
    //
    // AND and OR used to fold their result back into b1 and return it. b1 is a VIRTUAL register
    // that its jit_value_t is not the only holder of: read_mem hands the same register back for
    // every later read of the same column in the same row through ColumnValueCache, so overwriting
    // it rewrote a value the rest of the predicate still had to read. "(aboolean and aboolean2) =
    // aboolean" compiled to "cmp edx, edx" - the AND result against itself - and matched every row.
    // avx2_loop finishes each frame with scalar_tail, so it took the vectorized filter down with it
    // on the rows past the last full vector iteration.
    //
    // The extra MOV is a register-to-register copy the renamer handles without an execution slot,
    // and asmjit's allocator drops it outright where b1 dies at the AND.
    inline Gp int32_and(Compiler &c, const Gp &b1, const Gp &b2) {
        Gp r = c.new_gp32();
        c.mov(r, b1);
        c.and_(r, b2);
        return r.as<Gp>();
    }

    inline Gp int32_or(Compiler &c, const Gp &b1, const Gp &b2) {
        c.comment("int32_or_start");
        Gp r = c.new_gp32();
        c.mov(r, b1);
        c.or_(r, b2);
        c.comment("int32_or_stop");
        return r.as<Gp>();
    }

    inline Gp int32_to_int64(Compiler &c, const Gp &rhs, bool check_null) {
        c.comment("int32_to_int64");
        Gp r = c.new_gp64();
        if (!check_null) {
            c.movsxd(r, rhs);
            return r.as<Gp>();
        }
        Gp t = c.new_gp64();
        c.cmp(rhs, INT_NULL);
        c.movsxd(t, rhs);
        c.movabs(r, LONG_NULL);
        c.cmovne(r, t);
        return r.as<Gp>();
    }

    inline Vec int32_to_float(Compiler &c, const Gp &rhs, bool check_null) {
        c.comment("int32_to_float");
        Vec r =c.new_xmm_ss();
        if (!check_null) {
            c.cvtsi2ss(r, rhs);
            return r;
        }
        Label l_null = c.new_label();
        Label l_exit = c.new_label();
        Mem NaN = c.new_int32_const(asmjit::ConstPoolScope::kLocal, 0x7fc00000); // float NaN

        c.cmp(rhs, INT_NULL);
        c.je(l_null);
        c.cvtsi2ss(r, rhs);
        c.jmp(l_exit);
        c.bind(l_null);
        c.movss(r, NaN);
        c.bind(l_exit);
        return r;
    }

    inline Vec int32_to_double(Compiler &c, const Gp &rhs, bool check_null) {
        c.comment("int32_to_double");
        Vec r =c.new_xmm_sd();
        c.xorps(r, r);
        if (!check_null) {
            c.cvtsi2sd(r, rhs);
            return r;
        }
        Label l_null = c.new_label();
        Label l_exit = c.new_label();
        Mem NaN = c.new_int64_const(asmjit::ConstPoolScope::kLocal, 0x7ff8000000000000LL); // double NaN

        c.cmp(rhs, INT_NULL);
        c.je(l_null);
        c.cvtsi2sd(r, rhs);
        c.jmp(l_exit);
        c.bind(l_null);
        c.movsd(r, NaN);
        c.bind(l_exit);
        return r;
    }

    //coverage: we don't have int64 to float conversion for now
    inline Vec int64_to_float(Compiler &c, const Gp &rhs, bool check_null) {
        c.comment("int64_to_float");
        Vec r =c.new_xmm_ss();
        if (!check_null) {
            c.cvtsi2ss(r, rhs);
            return r;
        }
        Label l_null = c.new_label();
        Label l_exit = c.new_label();
        Mem NaN = c.new_int32_const(asmjit::ConstPoolScope::kLocal, 0x7fc00000); // float NaN

        Gp n = c.new_gp64();
        c.movabs(n, LONG_NULL);
        c.cmp(rhs, n);
        c.je(l_null);
        c.cvtsi2ss(r, rhs);
        c.jmp(l_exit);
        c.bind(l_null);
        c.movss(r, NaN);
        c.bind(l_exit);
        return r;
    }

    inline Vec int64_to_double(Compiler &c, const Gp &rhs, bool check_null) {
        c.comment("int64_to_double");
        Vec r =c.new_xmm_sd();
        c.xorps(r, r);
        if (!check_null) {
            c.cvtsi2sd(r, rhs);
            return r;
        }
        Label l_null = c.new_label();
        Label l_exit = c.new_label();
        Mem NaN = c.new_int64_const(asmjit::ConstPoolScope::kLocal, 0x7ff8000000000000LL); // double NaN

        Gp n = c.new_gp64();
        c.movabs(n, LONG_NULL);
        c.cmp(rhs, n);
        c.je(l_null);
        c.cvtsi2sd(r, rhs);
        c.jmp(l_exit);
        c.bind(l_null);
        c.movsd(r, NaN);
        c.bind(l_exit);
        return r;
    }

    inline Vec float_to_double(Compiler &c, const Vec &rhs) {
        c.comment("float_to_double");
        Vec r =c.new_xmm_sd();
        c.xorps(r, r);
        c.cvtss2sd(r, rhs);
        return r;
    }

    // Per-operand NULL propagation for int arithmetic: only a NULLABLE operand's sentinel
    // means NULL (a NOT NULL operand's identical bit pattern is data), so the emitter
    // resolves at compile time which side gets the check. Mirrors the interpreted
    // AddIntFunctionFactory-family: (!left.isNotNull() && left == INT_NULL) || ...
    inline void check_int32_null(Compiler &c, const Gp &dst, const Gp &lhs, const Gp &rhs, bool check_lhs, bool check_rhs) {
        if (check_lhs) {
            c.cmp(lhs, INT_NULL);
            c.cmove(dst, lhs);
        }
        if (check_rhs) {
            c.cmp(rhs, INT_NULL);
            c.cmove(dst, rhs);
        }
    }

    inline Gp int32_neg(Compiler &c, const Gp &rhs, bool check_null) {
        c.comment("int32_neg");

        Gp r = c.new_gp32();
        c.mov(r, rhs);
        c.neg(r);
        if (check_null) {
            Gp t = c.new_gp32();
            c.mov(t, INT_NULL);
            c.cmp(rhs, t);
            c.cmove(r, t);
        }
        return r.as<Gp>();
    }

    inline Gp int64_neg(Compiler &c, const Gp &rhs, bool check_null) {
        c.comment("int64_neg");

        Gp r = c.new_gp64();
        c.mov(r, rhs);
        c.neg(r);
        if (check_null) {
            Gp t = c.new_gp64();
            c.movabs(t, LONG_NULL);
            c.cmp(rhs, t);
            c.cmove(r, rhs);
        }
        return r.as<Gp>();
    }

    inline Gp int32_add(Compiler &c, const Gp &lhs, const Gp &rhs, bool check_lhs, bool check_rhs) {
        c.comment("int32_add");

        Gp r = c.new_gp64();
        c.lea(r, ptr(lhs, rhs));
        check_int32_null(c, r, lhs, rhs, check_lhs, check_rhs);
        return r.as<Gp>();
    }

    inline Gp int32_sub(Compiler &c, const Gp &lhs, const Gp &rhs, bool check_lhs, bool check_rhs) {
        c.comment("int32_sub");

        Gp r = c.new_gp32();
        c.mov(r, lhs);
        c.sub(r, rhs);
        check_int32_null(c, r, lhs, rhs, check_lhs, check_rhs);
        return r.as<Gp>();
    }

    inline Gp int32_mul(Compiler &c, const Gp &lhs, const Gp &rhs, bool check_lhs, bool check_rhs) {
        c.comment("int32_mul");

        Gp r = c.new_gp32();
        c.mov(r, lhs);
        c.imul(r, rhs);
        check_int32_null(c, r, lhs, rhs, check_lhs, check_rhs);
        return r.as<Gp>();
    }

    // A zero divisor yields NULL for every nullability combination (matches the interpreted
    // DivIntFunctionFactory); a sentinel-valued operand yields NULL only when that operand is
    // nullable - resolved at compile time via check_lhs / check_rhs.
    //
    // idiv raises #DE (a fatal SIGFPE, not a Java exception - the code lives in anonymous
    // asmjit memory HotSpot knows nothing about) when the quotient does not fit the
    // destination register, which for a 32-bit idiv happens only at INT_MIN / -1. A checked
    // dividend can never reach idiv holding INT_MIN, but an unchecked one can: on a NOT NULL
    // column the INT_MIN bit pattern is data. Java's DivIntFunctionFactory returns l / r
    // there, and Java defines Integer.MIN_VALUE / -1 == Integer.MIN_VALUE == INT_NULL, so
    // routing that one case to l_null (which leaves r == INT_NULL) is value-identical to the
    // interpreted path while keeping idiv out of its overflow case.
    inline Gp int32_div(Compiler &c, const Gp &lhs, const Gp &rhs, bool check_lhs, bool check_rhs) {
        c.comment("int32_div");

        Label l_null = c.new_label();

        Gp r = c.new_gp32();
        Gp t = c.new_gp32();

        c.mov(r, INT_NULL);
        if (check_rhs) {
            // one test for both: rhs == 0 and rhs == INT_NULL clear all low 31 bits
            c.test(rhs, 2147483647); //INT_NULL - 1
            c.je(l_null);
        } else {
            c.test(rhs, rhs);
            c.je(l_null);
        }
        if (check_lhs) {
            c.cmp(lhs, INT_NULL);
            c.je(l_null);
        } else {
            // dividend unchecked: guard the single idiv overflow case, INT_MIN / -1
            Label l_div = c.new_label();
            c.cmp(rhs, -1);
            c.jne(l_div);
            c.cmp(lhs, INT_NULL);
            c.je(l_null);
            c.bind(l_div);
        }
        c.mov(r, lhs);
        c.cdq(t, r);
        c.idiv(t, r, rhs);
        c.bind(l_null);
        return r.as<Gp>();
    }

    // See check_int32_null: per-operand NULL propagation, resolved at compile time.
    inline void check_int64_null(Compiler &c, const Gp &dst, const Gp &lhs, const Gp &rhs, bool check_lhs, bool check_rhs) {
        if (!check_lhs && !check_rhs) {
            return;
        }
        c.comment("check_int64_null");
        Gp n = c.new_gp64();
        c.movabs(n, LONG_NULL);
        if (check_lhs) {
            c.cmp(lhs, n);
            c.cmove(dst, lhs);
        }
        if (check_rhs) {
            c.cmp(rhs, n);
            c.cmove(dst, rhs);
        }
    }

    inline Gp int64_add(Compiler &c, const Gp &lhs, const Gp &rhs, bool check_lhs, bool check_rhs) {
        c.comment("int64_add");

        Gp r = c.new_gp64();
        c.lea(r, ptr(lhs, rhs));
        check_int64_null(c, r, lhs, rhs, check_lhs, check_rhs);
        return r.as<Gp>();
    }

    inline Gp int64_sub(Compiler &c, const Gp &lhs, const Gp &rhs, bool check_lhs, bool check_rhs) {
        c.comment("int64_sub");
        Gp r = c.new_gp64();
        c.mov(r, lhs);
        c.sub(r, rhs);
        check_int64_null(c, r, lhs, rhs, check_lhs, check_rhs);
        return r.as<Gp>();
    }

    inline Gp int64_mul(Compiler &c, const Gp &lhs, const Gp &rhs, bool check_lhs, bool check_rhs) {
        c.comment("int64_mul");
        Gp r = c.new_gp64();
        c.mov(r, lhs);
        c.imul(r, rhs);
        check_int64_null(c, r, lhs, rhs, check_lhs, check_rhs);
        return r.as<Gp>();
    }

    // See int32_div: zero divisor yields NULL always, sentinel operands only when nullable, and
    // an unchecked dividend still has to dodge the idiv overflow case (here LONG_MIN / -1,
    // whose quotient 2^63 does not fit rax). Java's DivLongFunctionFactory yields
    // Long.MIN_VALUE == LONG_NULL for it, which is exactly what l_null stores.
    inline Gp int64_div(Compiler &c, const Gp &lhs, const Gp &rhs, bool check_lhs, bool check_rhs) {
        c.comment("int64_div");

        Label l_null = c.new_label();
        Label l_exit = c.new_label();

        Gp r = c.new_gp64();
        Gp t = c.new_gp64();
        c.mov(r, lhs);
        if (check_rhs) {
            // one test for both: rhs == 0 and rhs == LONG_NULL clear all low 63 bits
            c.mov(t, rhs);
            c.btr(t, 63);
            c.test(t, t);
            c.je(l_null);
        } else {
            c.test(rhs, rhs);
            c.je(l_null);
        }
        if (check_lhs) {
            c.movabs(t, LONG_NULL);
            c.cmp(lhs, t);
            c.je(l_null);
        } else {
            // dividend unchecked: guard the single idiv overflow case, LONG_MIN / -1
            Label l_div = c.new_label();
            c.cmp(rhs, -1);
            c.jne(l_div);
            c.movabs(t, LONG_NULL);
            c.cmp(lhs, t);
            c.je(l_null);
            c.bind(l_div);
        }
        c.cqo(t, r);
        c.idiv(t, r, rhs);
        c.jmp(l_exit);

        c.bind(l_null);
        c.movabs(r, LONG_NULL);
        c.bind(l_exit);
        return r.as<Gp>();
    }

    // new_const copies its third argument's worth of bytes out of the pointer it is given, so that
    // size must track sizeof() of the local it reads from. These helpers used to pass 32 for a
    // 16-byte local, which made asmjit read 16 bytes past the end of the stack object and deposit
    // whatever the stack happened to hold into the constant pool. The consumers below (xorps,
    // xorpd, and andpd in double_cmp_epsilon) are SSE, so they only ever read the low 16 bytes -
    // the trailing garbage never reached the result, but the over-read was real and ASAN flags it.
    inline Vec float_neg(Compiler &c, const Vec &rhs) {
        Vec r =c.new_xmm_ss();
        c.movss(r, rhs);
        int32_t array[4] = {INT_NULL, 0, 0, 0};
        Mem mem = c.new_const(ConstPoolScope::kLocal, &array, 16);
        c.xorps(r, mem);
        return r;
    }

    inline Vec double_neg(Compiler &c, const Vec &rhs) {
        Vec r =c.new_xmm_sd();
        c.movsd(r, rhs);
        int32_t array[4] = {0, INT_NULL, 0, 0};
        Mem mem = c.new_const(ConstPoolScope::kLocal, &array, 16);
        c.xorpd(r, mem);
        return r;
    }

    inline Vec float_add(Compiler &c, const Vec &lhs, const Vec &rhs) {
        Vec r =c.new_xmm_ss();
        c.movss(r, lhs);
        c.addss(r, rhs);
        return r;
    }

    inline Vec float_sub(Compiler &c, const Vec &lhs, const Vec &rhs) {
        Vec r =c.new_xmm_ss();
        c.movss(r, lhs);
        c.subss(r, rhs);
        return r;
    }

    inline Vec float_mul(Compiler &c, const Vec &lhs, const Vec &rhs) {
        Vec r =c.new_xmm_ss();
        c.movss(r, lhs);
        c.mulss(r, rhs);
        return r;
    }

    // QuestDB reads any non-finite floating point value as NULL - Numbers#isNull is an
    // exponent-bits test, so it covers +/-Infinity as well as NaN - and
    // DivFloatFunctionFactory / DivDoubleFunctionFactory fold a non-finite quotient to NaN
    // ("Numbers.isFinite(f) ? f : Float.NaN") so the comparison treats it as NULL rather than
    // as a very large number. The division has to fold the same way here, or a zero divisor
    // makes the two filters select different rows: the Java filter reads NaN > 0 as false and
    // drops the row while a propagated +Infinity > 0 keeps it.
    //
    // Only division folds. Mul/Add/Sub deliberately do NOT fold in the Java factories either,
    // so overflowing to an infinity there already agrees on both paths and must stay that way.
    inline Vec float_div(Compiler &c, const Vec &lhs, const Vec &rhs) {
        Vec r =c.new_xmm_ss();
        c.movss(r, lhs);
        c.divss(r, rhs);
        // Same exponent-bits test float_cmp_epsilon uses: all ones in the exponent means
        // +/-Infinity or NaN.
        Mem NaN = c.new_int32_const(asmjit::ConstPoolScope::kLocal, 0x7fc00000); // float NaN
        Label l_exit = c.new_label();
        Gp int_r = c.new_gp32();
        c.movd(int_r, r);
        c.and_(int_r, 0x7F800000);
        c.cmp(int_r, 0x7F800000);
        c.jne(l_exit);
        c.movss(r, NaN);
        c.bind(l_exit);
        return r;
    }

    inline Vec double_add(Compiler &c, const Vec &lhs, const Vec &rhs) {
        Vec r =c.new_xmm_sd();
        c.movsd(r, lhs);
        c.addsd(r, rhs);
        return r;
    }

    inline Vec double_sub(Compiler &c, const Vec &lhs, const Vec &rhs) {
        Vec r =c.new_xmm_sd();
        c.movsd(r, lhs);
        c.subsd(r, rhs);
        return r;
    }

    inline Vec double_mul(Compiler &c, const Vec &lhs, const Vec &rhs) {
        Vec r =c.new_xmm_sd();
        c.movsd(r, lhs);
        c.mulsd(r, rhs);
        return r;
    }

    // See float_div: a non-finite quotient folds to NaN so it orders as NULL, matching
    // DivDoubleFunctionFactory.
    inline Vec double_div(Compiler &c, const Vec &lhs, const Vec &rhs) {
        Vec r =c.new_xmm_sd();
        c.movsd(r, lhs);
        c.divsd(r, rhs);
        Mem NaN = c.new_int64_const(asmjit::ConstPoolScope::kLocal, 0x7ff8000000000000LL); // double NaN
        Mem inf_memory = c.new_int64_const(asmjit::ConstPoolScope::kLocal, 0x7FF0000000000000LL);
        Label l_exit = c.new_label();
        Gp int_r = c.new_gp64();
        c.movq(int_r, r);
        c.and_(int_r, inf_memory);
        c.cmp(int_r, inf_memory);
        c.jne(l_exit);
        c.movsd(r, NaN);
        c.bind(l_exit);
        return r;
    }

    inline Gp int32_eq(Compiler &c, const Gp &lhs, const Gp &rhs) {
        Gp r = c.new_gp32();
        c.xor_(r, r);
        c.cmp(lhs, rhs);
        c.sete(r.r8_lo());
        return r.as<Gp>();
    }

    inline Gp int32_ne(Compiler &c, const Gp &lhs, const Gp &rhs) {
        Gp r = c.new_gp32();
        c.xor_(r, r);
        c.cmp(lhs, rhs);
        c.setne(r.r8_lo());
        return r.as<Gp>();
    }

    inline Gp int32_eq_zero(Compiler &c, const Gp &lhs) {
        Gp r = c.new_gp32();
        c.xor_(r, r);
        c.test(lhs, lhs);
        c.sete(r.r8_lo());
        return r.as<Gp>();
    }

    inline Gp int32_ne_zero(Compiler &c, const Gp &lhs) {
        Gp r = c.new_gp32();
        c.xor_(r, r);
        c.test(lhs, lhs);
        c.setne(r.r8_lo());
        return r.as<Gp>();
    }

    // Strict comparison with per-operand NULL exclusion, resolved at compile time: a NULL on
    // a nullable side orders nothing; an unchecked side reads the sentinel bit pattern as
    // data. result = (lhs OP rhs) & !lhs_is_null & !rhs_is_null, per-side terms emitted only
    // for nullable operands.
    inline Gp int32_lt_gt(Compiler &c, const Gp &lhs, const Gp &rhs, bool gt, bool check_lhs, bool check_rhs) {
        Gp v = c.new_gp32();
        c.xor_(v, v);
        c.cmp(lhs, rhs);
        if (gt) {
            c.setg(v.r8_lo());
        } else {
            c.setl(v.r8_lo());
        }
        if (check_lhs) {
            Gp l = c.new_gp32();
            c.xor_(l, l);
            c.cmp(lhs, INT_NULL);
            c.setne(l.r8_lo());
            c.and_(v, l);
        }
        if (check_rhs) {
            Gp r = c.new_gp32();
            c.xor_(r, r);
            c.cmp(rhs, INT_NULL);
            c.setne(r.r8_lo());
            c.and_(v, r);
        }
        return v.as<Gp>();
    }

    // Inclusive comparison with per-operand NULL exclusion, resolved at compile time. The
    // allow mask is !(lhs_is_null XOR rhs_is_null): two genuine NULLs compare equal (so <=
    // and >= hold), one NULL orders nothing, and an unchecked side contributes constant
    // false to the XOR, which reduces to !other_is_null. No checked side, no mask - that is
    // the sentinel-as-data path.
    inline Gp int32_le_ge(Compiler &c, const Gp &lhs, const Gp &rhs, bool ge, bool check_lhs, bool check_rhs) {
        Gp v = c.new_gp32();
        c.xor_(v, v);
        c.cmp(lhs, rhs);
        if (ge) {
            c.setge(v.r8_lo());
        } else {
            c.setle(v.r8_lo());
        }
        if (check_lhs && check_rhs) {
            Gp l = c.new_gp32();
            Gp r = c.new_gp32();
            c.xor_(l, l);
            c.cmp(lhs, INT_NULL);
            c.sete(l.r8_lo());
            c.xor_(r, r);
            c.cmp(rhs, INT_NULL);
            c.setne(r.r8_lo());
            c.xor_(r, l);
            c.and_(v, r);
        } else if (check_lhs) {
            Gp l = c.new_gp32();
            c.xor_(l, l);
            c.cmp(lhs, INT_NULL);
            c.setne(l.r8_lo());
            c.and_(v, l);
        } else if (check_rhs) {
            Gp r = c.new_gp32();
            c.xor_(r, r);
            c.cmp(rhs, INT_NULL);
            c.setne(r.r8_lo());
            c.and_(v, r);
        }
        return v.as<Gp>();
    }

    inline Gp int32_lt(Compiler &c, const Gp &lhs, const Gp &rhs, bool check_lhs, bool check_rhs) {
        return int32_lt_gt(c, lhs, rhs, false, check_lhs, check_rhs);
    }

    inline Gp int32_le(Compiler &c, const Gp &lhs, const Gp &rhs, bool check_lhs, bool check_rhs) {
        return int32_le_ge(c, lhs, rhs, false, check_lhs, check_rhs);
    }

    inline Gp int32_gt(Compiler &c, const Gp &lhs, const Gp &rhs, bool check_lhs, bool check_rhs) {
        return int32_lt_gt(c, lhs, rhs, true, check_lhs, check_rhs);
    }

    inline Gp int32_ge(Compiler &c, const Gp &lhs, const Gp &rhs, bool check_lhs, bool check_rhs) {
        return int32_le_ge(c, lhs, rhs, true, check_lhs, check_rhs);
    }

    inline Gp int64_eq(Compiler &c, const Gp &lhs, const Gp &rhs) {
        Gp r = c.new_gp64();
        c.xor_(r, r);
        c.cmp(lhs, rhs);
        c.sete(r.r8_lo());
        return r.as<Gp>();
    }

    inline Gp int64_ne(Compiler &c, const Gp &lhs, const Gp &rhs) {
        Gp r = c.new_gp64();
        c.xor_(r, r);
        c.cmp(lhs, rhs);
        c.setne(r.r8_lo());
        return r.as<Gp>();
    }

    inline Gp int64_eq_zero(Compiler &c, const Gp &lhs) {
        Gp r = c.new_gp64();
        c.xor_(r, r);
        c.test(lhs, lhs);
        c.sete(r.r8_lo());
        return r.as<Gp>();
    }

    inline Gp int64_ne_zero(Compiler &c, const Gp &lhs) {
        Gp r = c.new_gp64();
        c.xor_(r, r);
        c.test(lhs, lhs);
        c.setne(r.r8_lo());
        return r.as<Gp>();
    }

    inline void int128_cmp(Compiler &c, const Vec &lhs, const Vec &rhs) {
        Gp mask = c.new_gp16();
        // pcmpeqb is the two-operand SSE form and writes its result into the first operand, so it
        // needs a copy: read_mem hands the same register back for every later read of the same
        // column in the same row through ColumnValueCache, and folding the comparison into lhs
        // rewrites a value the rest of the predicate still has to read. The float and double
        // comparisons in this file take their lhs_copy / rhs_copy for the same reason. Until the
        // fix beside this one, an i128 column read never HIT that cache - x86::read_mem added it
        // through addXmm and looked it up through find(), which only answers for a general-purpose
        // register - so the clobber was unreachable rather than harmless.
        Vec l = c.new_xmm("i128_cmp_lhs");
        c.movdqu(l, lhs);
        c.pcmpeqb(l, rhs);
        c.pmovmskb(mask, l);
        c.cmp(mask, 0xffff);
    }

    inline Gp int128_eq(Compiler &c, const Vec &lhs, const Vec &rhs) {
        Gp r = c.new_gp64();
        c.xor_(r, r);
        int128_cmp(c, lhs, rhs);
        c.sete(r.r8_lo());
        return r.as<Gp>();
    }

    inline Gp int128_ne(Compiler &c, const Vec &lhs, const Vec &rhs) {
        Gp r = c.new_gp64();
        c.xor_(r, r);
        int128_cmp(c, lhs, rhs);
        c.setne(r.r8_lo());
        return r.as<Gp>();
    }

    // See int32_lt_gt: strict comparison, per-operand NULL exclusion at compile time.
    inline Gp int64_lt_gt(Compiler &c, const Gp &lhs, const Gp &rhs, bool gt, bool check_lhs, bool check_rhs) {
        Gp v = c.new_gp64();
        c.xor_(v, v);
        c.cmp(lhs, rhs);
        if (gt) {
            c.setg(v.r8_lo());
        } else {
            c.setl(v.r8_lo());
        }
        if (check_lhs || check_rhs) {
            Gp n = c.new_gp64();
            c.movabs(n, LONG_NULL);
            if (check_lhs) {
                Gp l = c.new_gp64();
                c.xor_(l, l);
                c.cmp(lhs, n);
                c.setne(l.r8_lo());
                c.and_(v, l);
            }
            if (check_rhs) {
                Gp r = c.new_gp64();
                c.xor_(r, r);
                c.cmp(rhs, n);
                c.setne(r.r8_lo());
                c.and_(v, r);
            }
        }
        return v.as<Gp>();
    }

    // See int32_le_ge: inclusive comparison, per-operand NULL exclusion at compile time.
    inline Gp int64_le_ge(Compiler &c, const Gp &lhs, const Gp &rhs, bool ge, bool check_lhs, bool check_rhs) {
        Gp v = c.new_gp64();
        c.xor_(v, v);
        c.cmp(lhs, rhs);
        if (ge) {
            c.setge(v.r8_lo());
        } else {
            c.setle(v.r8_lo());
        }
        if (check_lhs && check_rhs) {
            Gp l = c.new_gp64();
            Gp r = c.new_gp64();
            Gp n = c.new_gp64();
            c.movabs(n, LONG_NULL);
            c.xor_(l, l);
            c.cmp(lhs, n);
            c.sete(l.r8_lo());
            c.xor_(r, r);
            c.cmp(rhs, n);
            c.setne(r.r8_lo());
            c.xor_(r, l);
            c.and_(v, r);
        } else if (check_lhs) {
            Gp l = c.new_gp64();
            Gp n = c.new_gp64();
            c.movabs(n, LONG_NULL);
            c.xor_(l, l);
            c.cmp(lhs, n);
            c.setne(l.r8_lo());
            c.and_(v, l);
        } else if (check_rhs) {
            Gp r = c.new_gp64();
            Gp n = c.new_gp64();
            c.movabs(n, LONG_NULL);
            c.xor_(r, r);
            c.cmp(rhs, n);
            c.setne(r.r8_lo());
            c.and_(v, r);
        }
        return v.as<Gp>();
    }

    inline Gp int64_lt(Compiler &c, const Gp &lhs, const Gp &rhs, bool check_lhs, bool check_rhs) {
        return int64_lt_gt(c, lhs, rhs, false, check_lhs, check_rhs);
    }

    inline Gp int64_le(Compiler &c, const Gp &lhs, const Gp &rhs, bool check_lhs, bool check_rhs) {
        return int64_le_ge(c, lhs, rhs, false, check_lhs, check_rhs);
    }

    inline Gp int64_gt(Compiler &c, const Gp &lhs, const Gp &rhs, bool check_lhs, bool check_rhs) {
        return int64_lt_gt(c, lhs, rhs, true, check_lhs, check_rhs);
    }

    inline Gp int64_ge(Compiler &c, const Gp &lhs, const Gp &rhs, bool check_lhs, bool check_rhs) {
        return int64_le_ge(c, lhs, rhs, true, check_lhs, check_rhs);
    }

    //coverage: double_cmp_epsilon used instead
    //    inline Gp  double_eq(Compiler &c, const Vec &lhs, const Vec &rhs) {
    //        Gp r = c.new_gp32();
    //        c.cmpsd(lhs, rhs, CmpImm::kCmpEQ);
    //        c.vmovd(r, lhs);
    //        c.neg(r);
    //        return r.as<Gp>();
    //    }
    //
    //    inline Gp  double_ne(Compiler &c, const Vec &lhs, const Vec &rhs) {
    //        Gp r = c.new_gp32();
    //        c.cmpsd(lhs, rhs, CmpImm::kCmpNEQ);
    //        c.vmovd(r, lhs);
    //        c.neg(r);
    //        return r.as<Gp>();
    //    }

    inline Gp double_lt(Compiler &c, const Vec &lhs, const Vec &rhs) {
        Gp r = c.new_gp32();
        c.cmpsd(lhs, rhs, CmpImm::kLT);
        c.movd(r, lhs);
        c.neg(r);
        return r.as<Gp>();
    }

    inline Gp double_le(Compiler &c, const Vec &lhs, const Vec &rhs) {
        Gp r = c.new_gp32();
        c.cmpsd(lhs, rhs, CmpImm::kLE);
        c.movd(r, lhs);
        c.neg(r);
        return r.as<Gp>();
    }

    inline Gp double_gt(Compiler &c, const Vec &lhs, const Vec &rhs) {
        Gp r = c.new_gp32();
        c.cmpsd(rhs, lhs, CmpImm::kLT);
        c.movd(r, rhs);
        c.neg(r);
        return r.as<Gp>();
    }

    inline Gp double_ge(Compiler &c, const Vec &lhs, const Vec &rhs) {
        Gp r = c.new_gp32();
        c.cmpsd(rhs, lhs, CmpImm::kLE);
        c.movd(r, rhs);
        c.neg(r);
        return r.as<Gp>();
    }

    //coverage: float_cmp_epsilon used instead
    //    inline Gp  float_eq(Compiler &c, const Vec &lhs, const Vec &rhs) {
    //        Gp r = c.new_gp32();
    //        c.cmpss(lhs, rhs, CmpImm::kCmpEQ);
    //        c.vmovd(r, lhs);
    //        c.neg(r);
    //        return r.as<Gp>();
    //    }
    //
    //    inline Gp  float_ne(Compiler &c, const Vec &lhs, const Vec &rhs) {
    //        Gp r = c.new_gp32();
    //        c.cmpss(lhs, rhs, CmpImm::kCmpNEQ);
    //        c.vmovd(r, lhs);
    //        c.neg(r);
    //        return r.as<Gp>();
    //    }

    inline Gp float_lt(Compiler &c, const Vec &lhs, const Vec &rhs) {
        Gp r = c.new_gp32();
        c.cmpss(lhs, rhs, CmpImm::kLT);
        c.movd(r, lhs);
        c.neg(r);
        return r.as<Gp>();
    }

    inline Gp float_le(Compiler &c, const Vec &lhs, const Vec &rhs) {
        Gp r = c.new_gp32();
        c.cmpss(lhs, rhs, CmpImm::kLE);
        c.movd(r, lhs);
        c.neg(r);
        return r.as<Gp>();
    }

    inline Gp float_gt(Compiler &c, const Vec &lhs, const Vec &rhs) {
        Gp r = c.new_gp32();
        c.cmpss(rhs, lhs, CmpImm::kLT);
        c.movd(r, rhs);
        c.neg(r);
        return r.as<Gp>();
    }

    inline Gp float_ge(Compiler &c, const Vec &lhs, const Vec &rhs) {
        c.comment("float_ge_start");
        Gp r = c.new_gp32();
        c.cmpss(rhs, lhs, CmpImm::kLE);
        c.movd(r, rhs);
        c.neg(r);
        c.comment("float_ge_stop");
        return r.as<Gp>();
    }

    inline Gp float_is_finite(Compiler &c, const Vec &value) {
        Gp bits = c.new_gp32();
        Gp result = c.new_gp32();
        c.movd(bits, value);
        c.and_(bits, 0x7F800000);
        c.xor_(result, result);
        c.cmp(bits, 0x7F800000);
        c.setne(result.r8_lo());
        return result;
    }

    inline Gp double_is_finite(Compiler &c, const Vec &value) {
        Gp bits = c.new_gp64();
        Gp result = c.new_gp32();
        Mem inf_memory = c.new_int64_const(ConstPoolScope::kLocal, 0x7FF0000000000000LL);
        c.movq(bits, value);
        c.and_(bits, inf_memory);
        c.xor_(result, result);
        c.cmp(bits, inf_memory);
        c.setne(result.r8_lo());
        return result;
    }

    // (isnan(lhs) && isnan(rhs) || fabs(l - r) <= 0.0000000001);
    // The tolerance test is INCLUSIVE, matching Numbers.equals() ("Math.abs(l - r) <= DOUBLE_TOLERANCE").
    inline Gp double_cmp_epsilon(Compiler &c, const Vec &xmm0, const Vec &xmm1, double epsilon, bool eq) {
        c.comment("double_cmp_epsilon_start");
        int64_t nans[] = {0x7fffffffffffffff, 0x7fffffffffffffff}; // double NaN
        // 16, not 32: the size must match sizeof(nans). See the note above float_neg.
        Mem nans_memory = c.new_const(ConstPoolScope::kLocal, &nans, 16);
        Mem d = c.new_double_const(ConstPoolScope::kLocal, epsilon);
        Mem inf_memory = c.new_int64_const(ConstPoolScope::kLocal, 0x7FF0000000000000LL);
        Label l_nan = c.new_label();
        Label l_exit = c.new_label();
        Gp r = c.new_gp32();
        Gp int_r = c.new_gp64();
        // Work on copies to avoid modifying cached registers
        Vec lhs =c.new_xmm_sd();
        Vec rhs =c.new_xmm_sd();
        c.movsd(lhs, xmm0);
        c.movsd(rhs, xmm1);
        c.movq(int_r, lhs);
        c.and_(int_r, inf_memory);
        c.cmp(int_r, inf_memory);
        c.jne(l_nan);
        if (eq) {
            c.mov(r, 1);
        } else {
            c.xor_(r, r);
        }
        c.movq(int_r, rhs);
        c.and_(int_r, inf_memory);
        c.cmp(int_r, inf_memory);
        c.jne(l_nan);
        c.jmp(l_exit);

        c.bind(l_nan);
        c.subsd(lhs, rhs);
        c.andpd(lhs, nans_memory);
        c.movsd(rhs, d);
        c.xor_(r, r);
        c.ucomisd(rhs, lhs);
        // ucomisd sets CF=1 when rhs < lhs and when the operands are unordered, CF=0 when
        // rhs > lhs and when rhs == lhs. setae (CF==0) is therefore "epsilon >= |diff|",
        // the inclusive test, and it still answers false on a NaN diff. setb (CF==1) is its
        // exact complement and still answers true on a NaN diff, as double_ne_epsilon needs.
        if (eq) {
            c.setae(r.r8_lo());
        } else {
            c.setb(r.r8_lo());
        }
        c.bind(l_exit);
        c.comment("double_cmp_epsilon_stop");
        return r.as<Gp>();
    }

    inline Gp double_eq_epsilon(Compiler &c, const Vec &xmm0, const Vec &xmm1, double epsilon) {
        return double_cmp_epsilon(c, xmm0, xmm1, epsilon, true);
    }

    inline Gp double_ne_epsilon(Compiler &c, const Vec &xmm0, const Vec &xmm1, double epsilon) {
        return double_cmp_epsilon(c, xmm0, xmm1, epsilon, false);
    }

    inline Gp float_cmp_epsilon(Compiler &c, const Vec &xmm0, const Vec &xmm1, float epsilon, bool eq) {
        c.comment("float_cmp_epsilon_start");
        int32_t nans[] = {0x7fffffff, 0x7fffffff, 0x7fffffff, 0x7fffffff}; // float NaN
        Mem nans_memory = c.new_const(ConstPoolScope::kLocal, &nans, 16);
        Mem inf_memory = c.new_float_const(ConstPoolScope::kLocal, 0x7F800000);
        Mem d = c.new_float_const(ConstPoolScope::kLocal, epsilon);
        Label l_nan = c.new_label();
        Label l_exit = c.new_label();
        Gp int_r = c.new_gp32("tmp_int_r");
        // Work on copies to avoid modifying cached registers
        Vec lhs =c.new_xmm_ss();
        Vec rhs =c.new_xmm_ss();
        c.movss(lhs, xmm0);
        c.movss(rhs, xmm1);
        c.movd(int_r, lhs);
        c.and_(int_r, 0x7F800000);
        c.cmp(int_r,  0x7F800000);
        c.jne(l_nan);
        Gp r = c.new_gp32();
        if (eq) {
            c.mov(r, 1);
        } else {
            c.xor_(r, r);
        }
        c.movd(int_r, rhs);
        c.and_(int_r, 0x7F800000);
        c.cmp(int_r,  0x7F800000);
        c.jne(l_nan);
        c.jmp(l_exit);

        c.bind(l_nan);
        c.subss(lhs, rhs);
        c.andps(lhs, nans_memory);
        c.movss(rhs, d);
        c.xor_(r, r);
        c.ucomiss(rhs, lhs);
        // As in double_cmp_epsilon: setae (CF==0) is the inclusive "epsilon >= |diff|" and
        // setb (CF==1) its exact complement; both keep the unordered (NaN) answers unchanged.
        if (eq) {
            c.setae(r.r8_lo());
        } else {
            c.setb(r.r8_lo());
        }
        c.bind(l_exit);
        c.comment("float_cmp_epsilon_stop");
        return r.as<Gp>();
    }

    inline Gp float_eq_epsilon(Compiler &c, const Vec &xmm0, const Vec &xmm1, float epsilon) {
        return float_cmp_epsilon(c, xmm0, xmm1, epsilon, true);
    }

    inline Gp float_ne_epsilon(Compiler &c, const Vec &xmm0, const Vec &xmm1, float epsilon) {
        return float_cmp_epsilon(c, xmm0, xmm1, epsilon, false);
    }

}
#endif //QUESTDB_JIT_IMPL_X86_H
