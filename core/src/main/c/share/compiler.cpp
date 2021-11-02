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
            case NEG:
            {
                auto arg = tmp.top();
                tmp.pop();
                if(arg.isXmm()) {
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
            case NOT:
            {
                auto arg = tmp.top();
                tmp.pop();
                if(arg.isXmm()) {
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
                            c.xor_(r,r);
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
