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

    inline Gp int32_and(Compiler &c, const Gp &b1, const Gp &b2) {
        c.and_(b1, b2);
        return b1;
    }

    inline Gp int32_or(Compiler &c, const Gp &b1, const Gp &b2) {
        c.comment("int32_or_start");
        c.or_(b1, b2);
        c.comment("int32_or_stop");
        return b1;
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

    inline void check_int32_null(Compiler &c, const Gp &dst, const Gp &lhs, const Gp &rhs) {
        c.cmp(lhs, INT_NULL);
        c.cmove(dst, lhs);
        c.cmp(rhs, INT_NULL);
        c.cmove(dst, rhs);
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

    inline Gp int32_add(Compiler &c, const Gp &lhs, const Gp &rhs, bool check_null) {
        c.comment("int32_add");

        Gp r = c.new_gp64();
        c.lea(r, ptr(lhs, rhs));
        if (check_null) check_int32_null(c, r, lhs, rhs);
        return r.as<Gp>();
    }

    inline Gp int32_sub(Compiler &c, const Gp &lhs, const Gp &rhs, bool check_null) {
        c.comment("int32_sub");

        Gp r = c.new_gp32();
        c.mov(r, lhs);
        c.sub(r, rhs);
        if (check_null) check_int32_null(c, r, lhs, rhs);
        return r.as<Gp>();
    }

    inline Gp int32_mul(Compiler &c, const Gp &lhs, const Gp &rhs, bool check_null) {
        c.comment("int32_mul");

        Gp r = c.new_gp32();
        c.mov(r, lhs);
        c.imul(r, rhs);
        if (check_null) check_int32_null(c, r, lhs, rhs);
        return r.as<Gp>();
    }

    inline Gp int32_div(Compiler &c, const Gp &lhs, const Gp &rhs, bool check_null) {
        c.comment("int32_div");

        Label l_null = c.new_label();
        Label l_exit = c.new_label();

        Gp r = c.new_gp32();
        Gp t = c.new_gp32();

        if (!check_null) {
            c.mov(r, lhs);
            c.test(rhs, rhs);
            c.je(l_null);
            c.cdq(t, r);
            c.idiv(t, r, rhs);
            c.jmp(l_exit);
            c.bind(l_null);
            c.mov(r, INT_NULL);
            c.bind(l_exit);
            return r.as<Gp>();
        }
        c.mov(r, INT_NULL);
        c.test(rhs, 2147483647); //INT_NULL - 1
        c.je(l_null);
        c.cmp(lhs, INT_NULL);
        c.je(l_null);
        c.mov(r, lhs);
        c.cdq(t, r);
        c.idiv(t, r, rhs);
        c.bind(l_null);
        return r.as<Gp>();
    }

    inline void check_int64_null(Compiler &c, const Gp &dst, const Gp &lhs, const Gp &rhs) {
        c.comment("check_int64_null");
        Gp n = c.new_gp64();
        c.movabs(n, LONG_NULL);
        c.cmp(lhs, n);
        c.cmove(dst, lhs);
        c.cmp(rhs, n);
        c.cmove(dst, rhs);
    }

    inline Gp int64_add(Compiler &c, const Gp &lhs, const Gp &rhs, bool check_null) {
        c.comment("int64_add");

        Gp r = c.new_gp64();
        c.lea(r, ptr(lhs, rhs));
        if (check_null) check_int64_null(c, r, lhs, rhs);
        return r.as<Gp>();
    }

    inline Gp int64_sub(Compiler &c, const Gp &lhs, const Gp &rhs, bool check_null) {
        c.comment("int64_sub");
        Gp r = c.new_gp64();
        c.mov(r, lhs);
        c.sub(r, rhs);
        if (check_null) check_int64_null(c, r, lhs, rhs);
        return r.as<Gp>();
    }

    inline Gp int64_mul(Compiler &c, const Gp &lhs, const Gp &rhs, bool check_null) {
        c.comment("int64_mul");
        Gp r = c.new_gp64();
        c.mov(r, lhs);
        c.imul(r, rhs);
        if (check_null) check_int64_null(c, r, lhs, rhs);
        return r.as<Gp>();
    }

    inline Gp int64_div(Compiler &c, const Gp &lhs, const Gp &rhs, bool check_null) {
        c.comment("int64_div");

        Label l_null = c.new_label();
        Label l_exit = c.new_label();

        Gp r = c.new_gp64();
        Gp t = c.new_gp64();
        if (!check_null) {
            c.mov(r, lhs);
            c.test(rhs, rhs);
            c.je(l_null);
            c.cqo(t, r);
            c.idiv(t, r, rhs);
            c.jmp(l_exit);
            c.bind(l_null);
            c.movabs(r, LONG_NULL);
            c.bind(l_exit);
            return r.as<Gp>();
        }
        c.mov(t, rhs);
        c.mov(r, lhs);
        c.btr(t, 63);
        c.test(t, t);
        c.je(l_null);
        c.movabs(t, LONG_NULL);
        c.cmp(lhs, t);
        c.je(l_null);
        c.cqo(t, r);
        c.idiv(t, r, rhs);
        c.jmp(l_exit);

        c.bind(l_null);
        c.movabs(r, LONG_NULL);
        c.bind(l_exit);
        return r.as<Gp>();
    }

    inline Vec float_neg(Compiler &c, const Vec &rhs) {
        Vec r =c.new_xmm_ss();
        c.movss(r, rhs);
        int32_t array[4] = {INT_NULL, 0, 0, 0};
        Mem mem = c.new_const(ConstPoolScope::kLocal, &array, 32);
        c.xorps(r, mem);
        return r;
    }

    inline Vec double_neg(Compiler &c, const Vec &rhs) {
        Vec r =c.new_xmm_sd();
        c.movsd(r, rhs);
        int32_t array[4] = {0, INT_NULL, 0, 0};
        Mem mem = c.new_const(ConstPoolScope::kLocal, &array, 32);
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

    inline Vec float_div(Compiler &c, const Vec &lhs, const Vec &rhs) {
        Vec r =c.new_xmm_ss();
        c.movss(r, lhs);
        c.divss(r, rhs);
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

    inline Vec double_div(Compiler &c, const Vec &lhs, const Vec &rhs) {
        Vec r =c.new_xmm_sd();
        c.movsd(r, lhs);
        c.divsd(r, rhs);
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

    inline Gp int32_lt_gt(Compiler &c, const Gp &lhs, const Gp &rhs, bool gt, bool check_null) {
        if (!check_null) {
            Gp r = c.new_gp32();
            c.xor_(r, r);
            c.cmp(lhs, rhs);
            if (gt) {
                c.setg(r.r8_lo());
            } else {
                c.setl(r.r8_lo());
            }
            return r.as<Gp>();
        } else {
            Gp v = c.new_gp32();
            Gp l = c.new_gp32();
            Gp r = c.new_gp32();
            c.xor_(l, l);
            c.cmp(lhs, INT_NULL);
            c.setne(l.r8_lo());
            c.xor_(r, r);
            c.cmp(rhs, INT_NULL);
            c.setne(r.r8_lo());
            c.and_(r, l);
            c.xor_(v, v);
            c.cmp(lhs, rhs);
            if (gt) {
                c.setg(v.r8_lo());
            } else {
                c.setl(v.r8_lo());
            }
            c.and_(v, r);
            return v.as<Gp>();
        }
    }

    inline Gp int32_le_ge(Compiler &c, const Gp &lhs, const Gp &rhs, bool ge, bool check_null) {
        if (!check_null) {
            Gp r = c.new_gp32();
            c.xor_(r, r);
            c.cmp(lhs, rhs);
            if (ge) {
                c.setge(r.r8_lo());
            } else {
                c.setle(r.r8_lo());
            }
            return r.as<Gp>();
        } else {
            Gp v = c.new_gp32();
            Gp l = c.new_gp32();
            Gp r = c.new_gp32();

            c.xor_(l, l);
            c.cmp(lhs, INT_NULL);
            c.sete(l.r8_lo());
            c.xor_(r, r);
            c.cmp(rhs, INT_NULL);
            c.setne(r.r8_lo());
            c.xor_(r, l);
            c.xor_(v, v);
            c.cmp(lhs, rhs);
            if (ge) {
                c.setge(v.r8_lo());
            } else {
                c.setle(v.r8_lo());
            }
            c.and_(v, r);
            return v.as<Gp>();
        }
    }

    inline Gp int32_lt(Compiler &c, const Gp &lhs, const Gp &rhs, bool check_null) {
        return int32_lt_gt(c, lhs, rhs, false, check_null);
    }

    inline Gp int32_le(Compiler &c, const Gp &lhs, const Gp &rhs, bool check_null) {
        return int32_le_ge(c, lhs, rhs, false, check_null);
    }

    inline Gp int32_gt(Compiler &c, const Gp &lhs, const Gp &rhs, bool check_null) {
        return int32_lt_gt(c, lhs, rhs, true, check_null);
    }

    inline Gp int32_ge(Compiler &c, const Gp &lhs, const Gp &rhs, bool check_null) {
        return int32_le_ge(c, lhs, rhs, true, check_null);
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
        c.pcmpeqb(lhs, rhs);
        c.pmovmskb(mask, lhs);
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

    inline Gp int64_lt_gt(Compiler &c, const Gp &lhs, const Gp &rhs, bool gt, bool check_null) {
        if (!check_null) {
            Gp r = c.new_gp64();
            c.xor_(r, r);
            c.cmp(lhs, rhs);
            if (gt) {
                c.setg(r.r8_lo());
            } else {
                c.setl(r.r8_lo());
            }
            return r.as<Gp>();
        } else {
            Gp v = c.new_gp64();
            Gp l = c.new_gp64();
            Gp r = c.new_gp64();
            Gp n = c.new_gp64();

            c.movabs(n, LONG_NULL);
            c.xor_(l, l);
            c.cmp(lhs, n);
            c.setne(l.r8_lo());
            c.xor_(r, r);
            c.cmp(rhs, n);
            c.setne(r.r8_lo());
            c.and_(r, l);
            c.xor_(v, v);
            c.cmp(lhs, rhs);
            if (gt) {
                c.setg(v.r8_lo());
            } else {
                c.setl(v.r8_lo());
            }
            c.and_(v, r);

            return v.as<Gp>();
        }
    }

    inline Gp int64_le_ge(Compiler &c, const Gp &lhs, const Gp &rhs, bool ge, bool check_null) {
        if (!check_null) {
            Gp r = c.new_gp64();
            c.xor_(r, r);
            c.cmp(lhs, rhs);
            if (ge) {
                c.setge(r.r8_lo());
            } else {
                c.setle(r.r8_lo());
            }
            return r.as<Gp>();
        } else {
            Gp v = c.new_gp64();
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
            c.xor_(v, v);
            c.cmp(lhs, rhs);
            if (ge) {
                c.setge(v.r8_lo());
            } else {
                c.setle(v.r8_lo());
            }
            c.and_(v, r);

            return v.as<Gp>();
        }
    }

    inline Gp int64_lt(Compiler &c, const Gp &lhs, const Gp &rhs, bool check_null) {
        return int64_lt_gt(c, lhs, rhs, false, check_null);
    }

    inline Gp int64_le(Compiler &c, const Gp &lhs, const Gp &rhs, bool check_null) {
        return int64_le_ge(c, lhs, rhs, false, check_null);
    }

    inline Gp int64_gt(Compiler &c, const Gp &lhs, const Gp &rhs, bool check_null) {
        return int64_lt_gt(c, lhs, rhs, true, check_null);
    }

    inline Gp int64_ge(Compiler &c, const Gp &lhs, const Gp &rhs, bool check_null) {
        return int64_le_ge(c, lhs, rhs, true, check_null);
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

    // (isnan(lhs) && isnan(rhs) || fabs(l - r) < 0.0000000001);
    inline Gp double_cmp_epsilon(Compiler &c, const Vec &xmm0, const Vec &xmm1, double epsilon, bool eq) {
        c.comment("double_cmp_epsilon_start");
        int64_t nans[] = {0x7fffffffffffffff, 0x7fffffffffffffff}; // double NaN
        Mem nans_memory = c.new_const(ConstPoolScope::kLocal, &nans, 32);
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
        if (eq) {
            c.seta(r.r8_lo());
        } else {
            c.setbe(r.r8_lo());
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
        if (eq) {
            c.seta(r.r8_lo());
        } else {
            c.setbe(r.r8_lo());
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
