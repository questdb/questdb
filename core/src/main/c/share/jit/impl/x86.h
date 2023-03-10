/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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

    inline Gpd int32_not(Compiler &c, const Gpd &b) {
        c.not_(b);
        return b;
    }

    inline Gpd int32_and(Compiler &c, const Gpd &b1, const Gpd &b2) {
        c.and_(b1, b2);
        return b1;
    }

    inline Gpd int32_or(Compiler &c, const Gpd &b1, const Gpd &b2) {
        c.or_(b1, b2);
        return b1;
    }

    inline Gpq int32_to_int64(Compiler &c, const Gpd &rhs, bool check_null) {
        c.comment("int32_to_int64");
        Gp r = c.newInt64();
        if (!check_null) {
            c.movsxd(r, rhs);
            return r.as<Gpq>();
        }
        Gp t = c.newInt64();
        c.cmp(rhs, INT_NULL);
        c.movsxd(t, rhs);
        c.movabs(r, LONG_NULL);
        c.cmovne(r, t);
        return r.as<Gpq>();
    }

    inline Xmm int32_to_float(Compiler &c, const Gpd &rhs, bool check_null) {
        c.comment("int32_to_float");
        Xmm r = c.newXmmSs();
        if (!check_null) {
            c.cvtsi2ss(r, rhs);
            return r;
        }
        Label l_null = c.newLabel();
        Label l_exit = c.newLabel();
        Mem NaN = c.newInt32Const(asmjit::ConstPool::kScopeLocal, 0x7fc00000); // float NaN

        c.cmp(rhs, INT_NULL);
        c.je(l_null);
        c.cvtsi2ss(r, rhs);
        c.jmp(l_exit);
        c.bind(l_null);
        c.movss(r, NaN);
        c.bind(l_exit);
        return r;
    }

    inline Xmm int32_to_double(Compiler &c, const Gpd &rhs, bool check_null) {
        c.comment("int32_to_double");
        Xmm r = c.newXmmSd();
        c.xorps(r, r);
        if (!check_null) {
            c.cvtsi2sd(r, rhs);
            return r;
        }
        Label l_null = c.newLabel();
        Label l_exit = c.newLabel();
        Mem NaN = c.newInt64Const(asmjit::ConstPool::kScopeLocal, 0x7ff8000000000000LL); // double NaN

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
    inline Xmm int64_to_float(Compiler &c, const Gpq &rhs, bool check_null) {
        c.comment("int64_to_float");
        Xmm r = c.newXmmSs();
        if (!check_null) {
            c.cvtsi2ss(r, rhs);
            return r;
        }
        Label l_null = c.newLabel();
        Label l_exit = c.newLabel();
        Mem NaN = c.newInt32Const(asmjit::ConstPool::kScopeLocal, 0x7fc00000); // float NaN

        Gp n = c.newGpq();
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

    inline Xmm int64_to_double(Compiler &c, const Gpq &rhs, bool check_null) {
        c.comment("int64_to_double");
        Xmm r = c.newXmmSd();
        c.xorps(r, r);
        if (!check_null) {
            c.cvtsi2sd(r, rhs);
            return r;
        }
        Label l_null = c.newLabel();
        Label l_exit = c.newLabel();
        Mem NaN = c.newInt64Const(asmjit::ConstPool::kScopeLocal, 0x7ff8000000000000LL); // double NaN

        Gp n = c.newGpq();
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

    inline Xmm float_to_double(Compiler &c, const Xmm &rhs) {
        c.comment("float_to_double");
        Xmm r = c.newXmmSd();
        c.xorps(r, r);
        c.cvtss2sd(r, rhs);
        return r;
    }

    inline void check_int32_null(Compiler &c, const Gp &dst, const Gpd &lhs, const Gpd &rhs) {
        c.cmp(lhs, INT_NULL);
        c.cmove(dst, lhs);
        c.cmp(rhs, INT_NULL);
        c.cmove(dst, rhs);
    }

    inline Gpd int32_neg(Compiler &c, const Gpd &rhs, bool check_null) {
        c.comment("int32_neg");

        Gp r = c.newInt32();
        c.mov(r, rhs);
        c.neg(r);
        if (check_null) {
            Gp t = c.newInt32();
            c.mov(t, INT_NULL);
            c.cmp(rhs, t);
            c.cmove(r, t);
        }
        return r.as<Gpd>();
    }

    inline Gpq int64_neg(Compiler &c, const Gpq &rhs, bool check_null) {
        c.comment("int64_neg");

        Gp r = c.newInt64();
        c.mov(r, rhs);
        c.neg(r);
        if (check_null) {
            Gp t = c.newInt64();
            c.movabs(t, LONG_NULL);
            c.cmp(rhs, t);
            c.cmove(r, rhs);
        }
        return r.as<Gpq>();
    }

    inline Gpd int32_add(Compiler &c, const Gpd &lhs, const Gpd &rhs, bool check_null) {
        c.comment("int32_add");

        Gp r = c.newInt64();
        c.lea(r, ptr(lhs, rhs));
        if (check_null) check_int32_null(c, r, lhs, rhs);
        return r.as<Gpd>();
    }

    inline Gpd int32_sub(Compiler &c, const Gpd &lhs, const Gpd &rhs, bool check_null) {
        c.comment("int32_sub");

        Gp r = c.newInt32();
        c.mov(r, lhs);
        c.sub(r, rhs);
        if (check_null) check_int32_null(c, r, lhs, rhs);
        return r.as<Gpd>();
    }

    inline Gpd int32_mul(Compiler &c, const Gpd &lhs, const Gpd &rhs, bool check_null) {
        c.comment("int32_mul");

        Gp r = c.newInt32();
        c.mov(r, lhs);
        c.imul(r, rhs);
        if (check_null) check_int32_null(c, r, lhs, rhs);
        return r.as<Gpd>();
    }

    inline Gpd int32_div(Compiler &c, const Gpd &lhs, const Gpd &rhs, bool check_null) {
        c.comment("int32_div");

        Label l_null = c.newLabel();
        Label l_exit = c.newLabel();

        Gp r = c.newInt32();
        Gp t = c.newInt32();

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
            return r.as<Gpd>();
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
        return r.as<Gpd>();
    }

    inline void check_int64_null(Compiler &c, const Gp &dst, const Gp &lhs, const Gp &rhs) {
        c.comment("check_int64_null");
        Gp n = c.newGpq();
        c.movabs(n, LONG_NULL);
        c.cmp(lhs, n);
        c.cmove(dst, lhs);
        c.cmp(rhs, n);
        c.cmove(dst, rhs);
    }

    inline Gpq int64_add(Compiler &c, const Gpq &lhs, const Gpq &rhs, bool check_null) {
        c.comment("int64_add");

        Gp r = c.newInt64();
        c.lea(r, ptr(lhs, rhs));
        if (check_null) check_int64_null(c, r, lhs, rhs);
        return r.as<Gpq>();
    }

    inline Gpq int64_sub(Compiler &c, const Gpq &lhs, const Gpq &rhs, bool check_null) {
        c.comment("int64_sub");
        Gp r = c.newInt64();
        c.mov(r, lhs);
        c.sub(r, rhs);
        if (check_null) check_int64_null(c, r, lhs, rhs);
        return r.as<Gpq>();
    }

    inline Gpq int64_mul(Compiler &c, const Gpq &lhs, const Gpq &rhs, bool check_null) {
        c.comment("int64_mul");
        Gp r = c.newInt64();
        c.mov(r, lhs);
        c.imul(r, rhs);
        if (check_null) check_int64_null(c, r, lhs, rhs);
        return r.as<Gpq>();
    }

    inline Gpq int64_div(Compiler &c, const Gpq &lhs, const Gpq &rhs, bool check_null) {
        c.comment("int64_div");

        Label l_null = c.newLabel();
        Label l_exit = c.newLabel();

        Gp r = c.newInt64();
        Gp t = c.newInt64();
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
            return r.as<Gpq>();
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
        return r.as<Gpq>();
    }

    inline Xmm float_neg(Compiler &c, const Xmm &rhs) {
        int32_t array[4] = {INT_NULL, 0, 0, 0};
        Mem mem = c.newConst(ConstPool::kScopeLocal, &array, 32);
        c.xorps(rhs, mem);
        return rhs;
    }

    inline Xmm double_neg(Compiler &c, const Xmm &rhs) {
        int32_t array[4] = {0, INT_NULL, 0, 0};
        Mem mem = c.newConst(ConstPool::kScopeLocal, &array, 32);
        c.xorpd(rhs, mem);
        return rhs;
    }

    inline Xmm float_add(Compiler &c, const Xmm &lhs, const Xmm &rhs) {
        c.addss(lhs, rhs);
        return lhs;
    }

    inline Xmm float_sub(Compiler &c, const Xmm &lhs, const Xmm &rhs) {
        c.subss(lhs, rhs);
        return lhs;
    }

    inline Xmm float_mul(Compiler &c, const Xmm &lhs, const Xmm &rhs) {
        c.mulss(lhs, rhs);
        return lhs;
    }

    inline Xmm float_div(Compiler &c, const Xmm &lhs, const Xmm &rhs) {
        c.divss(lhs, rhs);
        return lhs;
    }

    inline Xmm double_add(Compiler &c, const Xmm &lhs, const Xmm &rhs) {
        c.addsd(lhs, rhs);
        return lhs;
    }

    inline Xmm double_sub(Compiler &c, const Xmm &lhs, const Xmm &rhs) {
        c.subsd(lhs, rhs);
        return lhs;
    }

    inline Xmm double_mul(Compiler &c, const Xmm &lhs, const Xmm &rhs) {
        c.mulsd(lhs, rhs);
        return lhs;
    }

    inline Xmm double_div(Compiler &c, const Xmm &lhs, const Xmm &rhs) {
        c.divsd(lhs, rhs);
        return lhs;
    }

    inline Gpd int32_eq(Compiler &c, const Gpd &lhs, const Gpd &rhs) {
        Gp r = c.newInt32();
        c.xor_(r, r);
        c.cmp(lhs, rhs);
        c.sete(r.r8Lo());
        return r.as<Gpd>();
    }

    inline Gpd int32_ne(Compiler &c, const Gpd &lhs, const Gpd &rhs) {
        Gp r = c.newInt32();
        c.xor_(r, r);
        c.cmp(lhs, rhs);
        c.setne(r.r8Lo());
        return r.as<Gpd>();
    }

    inline Gpd int32_lt(Compiler &c, const Gpd &lhs, const Gpd &rhs, bool check_null) {
        Gp r = c.newInt32();
        c.xor_(r, r);
        if (!check_null) {
            c.cmp(lhs, rhs);
            c.setl(r.r8Lo());
            return r.as<Gpd>();
        } else {
            Gp t = c.newInt32();
            c.xor_(t, t);
            c.cmp(lhs, INT_NULL);
            c.setne(r.r8Lo());
            c.cmp(lhs, rhs);
            c.setl(t.r8Lo());
            c.and_(r, t);
            return r.as<Gpd>();
        }
    }

    inline Gpd int32_le(Compiler &c, const Gpd &lhs, const Gpd &rhs, bool check_null) {
        Gp r = c.newInt32();
        c.xor_(r, r);
        if (!check_null) {
            c.cmp(lhs, rhs);
            c.setle(r.r8Lo());
            return r.as<Gpd>();
        } else {
            Gp t = c.newInt32();
            c.xor_(t, t);
            c.cmp(lhs, INT_NULL);
            c.setne(r.r8Lo());
            c.cmp(lhs, rhs);
            c.setle(t.r8Lo());
            c.and_(r, t);
            c.cmp(rhs, INT_NULL);
            c.setne(t.r8Lo());
            c.and_(r, t);
            return r.as<Gpd>();
        }
    }

    inline Gpd int32_gt(Compiler &c, const Gpd &lhs, const Gpd &rhs, bool check_null) {
        Gp r = c.newInt32();
        c.xor_(r, r);
        if (!check_null) {
            c.cmp(lhs, rhs);
            c.setg(r.r8Lo());
            return r.as<Gpd>();
        } else {
            Gp t = c.newInt32();
            c.xor_(t, t);
            c.cmp(rhs, INT_NULL);
            c.setne(r.r8Lo());
            c.cmp(lhs, rhs);
            c.setg(t.r8Lo());
            c.and_(r, t);
            return r.as<Gpd>();
        }
    }

    inline Gpd int32_ge(Compiler &c, const Gpd &lhs, const Gpd &rhs, bool check_null) {
        Gp r = c.newInt32();
        if (!check_null) {
            c.xor_(r, r);
            c.cmp(lhs, rhs);
            c.setge(r.r8Lo());
            return r.as<Gpd>();
        } else {
            Gp t = c.newInt32();
            c.xor_(r, r);
            c.cmp(lhs, INT_NULL);
            c.setne(r.r8Lo());
            c.xor_(t, t);
            c.cmp(lhs, rhs);
            c.setge(t.r8Lo());
            c.and_(r, t);
            c.cmp(rhs, INT_NULL);
            c.setne(t.r8Lo());
            c.and_(r, t);
            return r.as<Gpd>();
        }
    }

    inline Gpq int64_eq(Compiler &c, const Gpq &lhs, const Gpq &rhs) {
        Gp r = c.newInt64();
        c.xor_(r, r);
        c.cmp(lhs, rhs);
        c.sete(r.r8Lo());
        return r.as<Gpq>();
    }

    inline Gpq int64_ne(Compiler &c, const Gpq &lhs, const Gpq &rhs) {
        Gp r = c.newInt64();
        c.xor_(r, r);
        c.cmp(lhs, rhs);
        c.setne(r.r8Lo());
        return r.as<Gpq>();
    }

    inline void int128_cmp(Compiler &c, const Xmm &lhs, const Xmm &rhs) {
        Gp mask = c.newInt16();
        c.pcmpeqb(lhs, rhs);
        c.pmovmskb(mask, lhs);
        c.cmp(mask, 0xffff);
    }

    inline Gpq int128_eq(Compiler &c, const Xmm &lhs, const Xmm &rhs) {
        int128_cmp(c, lhs, rhs);
        Gp r = c.newInt64();
        c.sete(r.r8Lo());
        return r.as<Gpq>();
    }

    inline Gpq int128_ne(Compiler &c, const Xmm &lhs, const Xmm &rhs) {
        int128_cmp(c, lhs, rhs);
        Gp r = c.newInt64();
        c.setne(r.r8Lo());
        return r.as<Gpq>();
    }

    inline Gpq int64_lt(Compiler &c, const Gpq &lhs, const Gpq &rhs, bool check_null) {
        Gp r = c.newInt64();
        c.xor_(r, r);
        if (!check_null) {
            c.cmp(lhs, rhs);
            c.setl(r.r8Lo());
            return r.as<Gpq>();
        } else {
            Gp t = c.newInt64();
            c.xor_(t, t);
            c.movabs(r, LONG_NULL);
            c.cmp(lhs, r);
            c.setne(r.r8Lo());
            c.cmp(lhs, rhs);
            c.setl(t.r8Lo());
            c.and_(r, t);
            return r.as<Gpq>();
        }
    }

    inline Gpq int64_le(Compiler &c, const Gpq &lhs, const Gpq &rhs, bool check_null) {
        Gp r = c.newInt64();
        if (!check_null) {
            c.xor_(r, r);
            c.cmp(lhs, rhs);
            c.setle(r.r8Lo());
            return r.as<Gpq>();
        } else {
            Gp t = c.newInt64();
            c.movabs(t, LONG_NULL);
            c.xor_(r, r);
            c.cmp(lhs, t);
            c.setne(r.r8Lo());
            Gp t2 = c.newInt64();
            c.xor_(t2, t2);
            c.cmp(lhs, rhs);
            c.setle(t2.r8Lo());
            c.and_(r, t2);
            c.cmp(rhs, t);
            c.setne(t.r8Lo());
            c.and_(r, t);
            return r.as<Gpq>();
        }
    }

    inline Gpq int64_gt(Compiler &c, const Gpq &lhs, const Gpq &rhs, bool check_null) {
        Gp r = c.newInt64("int64_gt_r");
        c.xor_(r, r);
        if (!check_null) {
            c.cmp(lhs, rhs);
            c.setg(r.r8Lo());
            return r.as<Gpq>();
        } else {
            Gp t = c.newInt64("int64_gt_t");
            c.xor_(t, t);
            c.movabs(r, LONG_NULL);
            c.cmp(rhs, r);
            c.setne(r.r8Lo());
            c.cmp(lhs, rhs);
            c.setg(t.r8Lo());
            c.and_(r, t);
            return r.as<Gpq>();
        }
    }

    inline Gpq int64_ge(Compiler &c, const Gpq &lhs, const Gpq &rhs, bool check_null) {
        Gp r = c.newInt64("int64_ge_r");
        c.xor_(r, r);
        if (!check_null) {
            c.cmp(lhs, rhs);
            c.setge(r.r8Lo());
            return r.as<Gpq>();
        } else {
            Gp t = c.newInt64("int64_t_ge_t");
            c.xor_(t, t);
            Gp t2 = c.newInt64("int64_t_ge_t2");
            c.xor_(t2, t2);
            c.movabs(t, LONG_NULL);
            c.cmp(lhs, t);
            c.setne(r.r8Lo());
            c.cmp(lhs, rhs);
            c.setge(t2.r8Lo());
            c.and_(r, t2);
            c.cmp(rhs, t);
            c.setne(t.r8Lo());
            c.and_(r, t);
            return r.as<Gpq>();
        }
    }

    //coverage: double_cmp_epsilon used instead
    //    inline Gpd double_eq(Compiler &c, const Xmm &lhs, const Xmm &rhs) {
    //        Gp r = c.newInt32();
    //        c.cmpsd(lhs, rhs, Predicate::kCmpEQ);
    //        c.vmovd(r, lhs);
    //        c.neg(r);
    //        return r.as<Gpd>();
    //    }
    //
    //    inline Gpd double_ne(Compiler &c, const Xmm &lhs, const Xmm &rhs) {
    //        Gp r = c.newInt32();
    //        c.cmpsd(lhs, rhs, Predicate::kCmpNEQ);
    //        c.vmovd(r, lhs);
    //        c.neg(r);
    //        return r.as<Gpd>();
    //    }

    inline Gpd double_lt(Compiler &c, const Xmm &lhs, const Xmm &rhs) {
        Gp r = c.newInt32();
        c.cmpsd(lhs, rhs, Predicate::kCmpLT);
        c.movd(r, lhs);
        c.neg(r);
        return r.as<Gpd>();
    }

    inline Gpd double_le(Compiler &c, const Xmm &lhs, const Xmm &rhs) {
        Gp r = c.newInt32();
        c.cmpsd(lhs, rhs, Predicate::kCmpLE);
        c.movd(r, lhs);
        c.neg(r);
        return r.as<Gpd>();
    }

    inline Gpd double_gt(Compiler &c, const Xmm &lhs, const Xmm &rhs) {
        Gp r = c.newInt32();
        c.cmpsd(rhs, lhs, Predicate::kCmpLT);
        c.movd(r, rhs);
        c.neg(r);
        return r.as<Gpd>();
    }

    inline Gpd double_ge(Compiler &c, const Xmm &lhs, const Xmm &rhs) {
        Gp r = c.newInt32();
        c.cmpsd(rhs, lhs, Predicate::kCmpLE);
        c.movd(r, rhs);
        c.neg(r);
        return r.as<Gpd>();
    }

    //coverage: float_cmp_epsilon used instead
    //    inline Gpd float_eq(Compiler &c, const Xmm &lhs, const Xmm &rhs) {
    //        Gp r = c.newInt32();
    //        c.cmpss(lhs, rhs, Predicate::kCmpEQ);
    //        c.vmovd(r, lhs);
    //        c.neg(r);
    //        return r.as<Gpd>();
    //    }
    //
    //    inline Gpd float_ne(Compiler &c, const Xmm &lhs, const Xmm &rhs) {
    //        Gp r = c.newInt32();
    //        c.cmpss(lhs, rhs, Predicate::kCmpNEQ);
    //        c.vmovd(r, lhs);
    //        c.neg(r);
    //        return r.as<Gpd>();
    //    }

    inline Gpd float_lt(Compiler &c, const Xmm &lhs, const Xmm &rhs) {
        Gp r = c.newInt32();
        c.cmpss(lhs, rhs, Predicate::kCmpLT);
        c.movd(r, lhs);
        c.neg(r);
        return r.as<Gpd>();
    }

    inline Gpd float_le(Compiler &c, const Xmm &lhs, const Xmm &rhs) {
        Gp r = c.newInt32();
        c.cmpss(lhs, rhs, Predicate::kCmpLE);
        c.movd(r, lhs);
        c.neg(r);
        return r.as<Gpd>();
    }

    inline Gpd float_gt(Compiler &c, const Xmm &lhs, const Xmm &rhs) {
        Gp r = c.newInt32();
        c.cmpss(rhs, lhs, Predicate::kCmpLT);
        c.movd(r, rhs);
        c.neg(r);
        return r.as<Gpd>();
    }

    inline Gpd float_ge(Compiler &c, const Xmm &lhs, const Xmm &rhs) {
        Gp r = c.newInt32();
        c.cmpss(rhs, lhs, Predicate::kCmpLE);
        c.movd(r, rhs);
        c.neg(r);
        return r.as<Gpd>();
    }

    // (isnan(lhs) && isnan(rhs) || fabs(l - r) < 0.0000000001);
    inline Gpd double_cmp_epsilon(Compiler &c, const Xmm &xmm0, const Xmm &xmm1, double epsilon, bool eq) {
        int64_t nans[] = {0x7fffffffffffffff, 0x7fffffffffffffff}; // double NaN
        Mem nans_memory = c.newConst(ConstPool::kScopeLocal, &nans, 32);
        Mem d = c.newDoubleConst(ConstPool::kScopeLocal, epsilon);
        Label l_nan = c.newLabel();
        Label l_exit = c.newLabel();
        Gp r = c.newInt32();
        c.ucomisd(xmm0, xmm0);
        c.jnp(l_nan);
        if (eq) {
            c.mov(r.r8Lo(), 1);
        } else {
            c.xor_(r, r);
        }
        c.ucomisd(xmm1, xmm1);
        c.jnp(l_nan);
        c.jmp(l_exit);

        c.bind(l_nan);
        c.subsd(xmm0, xmm1);
        c.andpd(xmm0, nans_memory);
        c.movsd(xmm1, d);
        c.ucomisd(xmm1, xmm0);
        if (eq) {
            c.seta(r.r8Lo());
        } else {
            c.setbe(r.r8Lo());
        }
        c.bind(l_exit);
        return r.as<Gpd>();
    }

    inline Gpd double_eq_epsilon(Compiler &c, const Xmm &xmm0, const Xmm &xmm1, double epsilon) {
        return double_cmp_epsilon(c, xmm0, xmm1, epsilon, true);
    }

    inline Gpd double_ne_epsilon(Compiler &c, const Xmm &xmm0, const Xmm &xmm1, double epsilon) {
        return double_cmp_epsilon(c, xmm0, xmm1, epsilon, false);
    }

    inline Gpd float_cmp_epsilon(Compiler &c, const Xmm &xmm0, const Xmm &xmm1, float epsilon, bool eq) {
        int32_t nans[] = {0x7fffffff, 0x7fffffff, 0x7fffffff, 0x7fffffff}; // float NaN
        Mem nans_memory = c.newConst(ConstPool::kScopeLocal, &nans, 16);
        Mem d = c.newFloatConst(ConstPool::kScopeLocal, epsilon);
        Label l_nan = c.newLabel();
        Label l_exit = c.newLabel();
        Gp r = c.newInt32();
        c.ucomiss(xmm0, xmm0);
        c.jnp(l_nan);
        if (eq) {
            c.mov(r.r8Lo(), 1);
        } else {
            c.xor_(r, r);
        }
        c.ucomiss(xmm1, xmm1);
        c.jnp(l_nan);
        c.jmp(l_exit);

        c.bind(l_nan);
        c.subss(xmm0, xmm1);
        c.andps(xmm0, nans_memory);
        c.movss(xmm1, d);
        c.ucomiss(xmm1, xmm0);
        if (eq) {
            c.seta(r.r8Lo());
        } else {
            c.setbe(r.r8Lo());
        }
        c.bind(l_exit);
        return r.as<Gpd>();
    }

    inline Gpd float_eq_epsilon(Compiler &c, const Xmm &xmm0, const Xmm &xmm1, float epsilon) {
        return float_cmp_epsilon(c, xmm0, xmm1, epsilon, true);
    }

    inline Gpd float_ne_epsilon(Compiler &c, const Xmm &xmm0, const Xmm &xmm1, float epsilon) {
        return float_cmp_epsilon(c, xmm0, xmm1, epsilon, false);
    }
}

#endif //QUESTDB_JIT_IMPL_X86_H
