// AsmJit - Machine code generation for C++
//
//  * Official AsmJit Home Page: https://asmjit.com
//  * Official Github Repository: https://github.com/asmjit/asmjit
//
// Copyright (c) 2008-2020 The AsmJit Authors
//
// This software is provided 'as-is', without any express or implied
// warranty. In no event will the authors be held liable for any damages
// arising from the use of this software.
//
// Permission is granted to anyone to use this software for any purpose,
// including commercial applications, and to alter it and redistribute it
// freely, subject to the following restrictions:
//
// 1. The origin of this software must not be misrepresented; you must not
//    claim that you wrote the original software. If you use this software
//    in a product, an acknowledgment in the product documentation would be
//    appreciated but is not required.
// 2. Altered source versions must be plainly marked as such, and must not be
//    misrepresented as being the original software.
// 3. This notice may not be removed or altered from any source distribution.

#ifndef ASMJIT_X86_X86INSTDB_P_H_INCLUDED
#define ASMJIT_X86_X86INSTDB_P_H_INCLUDED

#include "../x86/x86instdb.h"

ASMJIT_BEGIN_SUB_NAMESPACE(x86)

//! \cond INTERNAL
//! \addtogroup asmjit_x86
//! \{

namespace InstDB {

// ============================================================================
// [asmjit::x86::InstDB::Encoding]
// ============================================================================

//! Instruction encoding (X86).
//!
//! This is a specific identifier that is used by AsmJit to describe the way
//! each instruction is encoded. Some encodings are special only for a single
//! instruction as X86 instruction set contains a lot of legacy encodings, and
//! some encodings describe a group of instructions that share some commons,
//! like MMX, SSE, AVX, AVX512 instructions, etc...
enum EncodingId : uint32_t {
  kEncodingNone = 0,                     //!< Never used.
  kEncodingX86Op,                        //!< X86 [OP].
  kEncodingX86Op_Mod11RM,                //!< X86 [OP] (opcode with ModRM byte where MOD must be 11b).
  kEncodingX86Op_Mod11RM_I8,             //!< X86 [OP] (opcode with ModRM byte + 8-bit immediate).
  kEncodingX86Op_xAddr,                  //!< X86 [OP] (implicit address in the first register operand).
  kEncodingX86Op_xAX,                    //!< X86 [OP] (implicit or explicit '?AX' form).
  kEncodingX86Op_xDX_xAX,                //!< X86 [OP] (implicit or explicit '?DX, ?AX' form).
  kEncodingX86Op_MemZAX,                 //!< X86 [OP] (implicit or explicit '[EAX|RAX]' form).
  kEncodingX86I_xAX,                     //!< X86 [I] (implicit or explicit '?AX' form).
  kEncodingX86M,                         //!< X86 [M] (handles 2|4|8-bytes size).
  kEncodingX86M_NoMemSize,               //!< X86 [M] (handles 2|4|8-bytes size, but doesn't consider memory size).
  kEncodingX86M_NoSize,                  //!< X86 [M] (doesn't handle any size).
  kEncodingX86M_GPB,                     //!< X86 [M] (handles single-byte size).
  kEncodingX86M_GPB_MulDiv,              //!< X86 [M] (like GPB, handles implicit|explicit MUL|DIV|IDIV).
  kEncodingX86M_Only,                    //!< X86 [M] (restricted to memory operand of any size).
  kEncodingX86M_Only_EDX_EAX,            //!< X86 [M] (memory operand only, followed by implicit <edx> and <eax>).
  kEncodingX86M_Nop,                     //!< X86 [M] (special case of NOP instruction).
  kEncodingX86R_Native,                  //!< X86 [R] (register must be either 32-bit or 64-bit depending on arch).
  kEncodingX86R_FromM,                   //!< X86 [R] - which specifies memory address.
  kEncodingX86R32_EDX_EAX,               //!< X86 [R32] followed by implicit EDX and EAX.
  kEncodingX86Rm,                        //!< X86 [RM] (doesn't handle single-byte size).
  kEncodingX86Rm_Raw66H,                 //!< X86 [RM] (used by LZCNT, POPCNT, and TZCNT).
  kEncodingX86Rm_NoSize,                 //!< X86 [RM] (doesn't add REX.W prefix if 64-bit reg is used).
  kEncodingX86Mr,                        //!< X86 [MR] (doesn't handle single-byte size).
  kEncodingX86Mr_NoSize,                 //!< X86 [MR] (doesn't handle any size).
  kEncodingX86Arith,                     //!< X86 adc, add, and, cmp, or, sbb, sub, xor.
  kEncodingX86Bswap,                     //!< X86 bswap.
  kEncodingX86Bt,                        //!< X86 bt, btc, btr, bts.
  kEncodingX86Call,                      //!< X86 call.
  kEncodingX86Cmpxchg,                   //!< X86 [MR] cmpxchg.
  kEncodingX86Cmpxchg8b_16b,             //!< X86 [MR] cmpxchg8b, cmpxchg16b.
  kEncodingX86Crc,                       //!< X86 crc32.
  kEncodingX86Enter,                     //!< X86 enter.
  kEncodingX86Imul,                      //!< X86 imul.
  kEncodingX86In,                        //!< X86 in.
  kEncodingX86Ins,                       //!< X86 ins[b|q|d].
  kEncodingX86IncDec,                    //!< X86 inc, dec.
  kEncodingX86Int,                       //!< X86 int (interrupt).
  kEncodingX86Jcc,                       //!< X86 jcc.
  kEncodingX86JecxzLoop,                 //!< X86 jcxz, jecxz, jrcxz, loop, loope, loopne.
  kEncodingX86Jmp,                       //!< X86 jmp.
  kEncodingX86JmpRel,                    //!< X86 xbegin.
  kEncodingX86LcallLjmp,                 //!< X86 lcall/ljmp.
  kEncodingX86Lea,                       //!< X86 lea.
  kEncodingX86Mov,                       //!< X86 mov (all possible cases).
  kEncodingX86Movabs,                    //!< X86 movabs.
  kEncodingX86MovsxMovzx,                //!< X86 movsx, movzx.
  kEncodingX86MovntiMovdiri,             //!< X86 movnti/movdiri.
  kEncodingX86EnqcmdMovdir64b,           //!< X86 enqcmd/enqcmds/movdir64b.
  kEncodingX86Out,                       //!< X86 out.
  kEncodingX86Outs,                      //!< X86 out[b|w|d].
  kEncodingX86Push,                      //!< X86 push.
  kEncodingX86Pop,                       //!< X86 pop.
  kEncodingX86Ret,                       //!< X86 ret.
  kEncodingX86Rot,                       //!< X86 rcl, rcr, rol, ror, sal, sar, shl, shr.
  kEncodingX86Set,                       //!< X86 setcc.
  kEncodingX86ShldShrd,                  //!< X86 shld, shrd.
  kEncodingX86StrRm,                     //!< X86 lods.
  kEncodingX86StrMr,                     //!< X86 scas, stos.
  kEncodingX86StrMm,                     //!< X86 cmps, movs.
  kEncodingX86Test,                      //!< X86 test.
  kEncodingX86Xadd,                      //!< X86 xadd.
  kEncodingX86Xchg,                      //!< X86 xchg.
  kEncodingX86Fence,                     //!< X86 lfence, mfence, sfence.
  kEncodingX86Bndmov,                    //!< X86 [RM|MR] (used by BNDMOV).
  kEncodingFpuOp,                        //!< FPU [OP].
  kEncodingFpuArith,                     //!< FPU fadd, fdiv, fdivr, fmul, fsub, fsubr.
  kEncodingFpuCom,                       //!< FPU fcom, fcomp.
  kEncodingFpuFldFst,                    //!< FPU fld, fst, fstp.
  kEncodingFpuM,                         //!< FPU fiadd, ficom, ficomp, fidiv, fidivr, fild, fimul, fist, fistp, fisttp, fisub, fisubr.
  kEncodingFpuR,                         //!< FPU fcmov, fcomi, fcomip, ffree, fucom, fucomi, fucomip, fucomp, fxch.
  kEncodingFpuRDef,                      //!< FPU faddp, fdivp, fdivrp, fmulp, fsubp, fsubrp.
  kEncodingFpuStsw,                      //!< FPU fnstsw, Fstsw.
  kEncodingExtRm,                        //!< EXT [RM].
  kEncodingExtRm_XMM0,                   //!< EXT [RM<XMM0>].
  kEncodingExtRm_ZDI,                    //!< EXT [RM<ZDI>].
  kEncodingExtRm_P,                      //!< EXT [RM] (propagates 66H if the instruction uses XMM register).
  kEncodingExtRm_Wx,                     //!< EXT [RM] (propagates REX.W if GPQ is used or the second operand is GPQ/QWORD_PTR).
  kEncodingExtRm_Wx_GpqOnly,             //!< EXT [RM] (propagates REX.W if the first operand is GPQ register).
  kEncodingExtRmRi,                      //!< EXT [RM|RI].
  kEncodingExtRmRi_P,                    //!< EXT [RM|RI] (propagates 66H if the instruction uses XMM register).
  kEncodingExtRmi,                       //!< EXT [RMI].
  kEncodingExtRmi_P,                     //!< EXT [RMI] (propagates 66H if the instruction uses XMM register).
  kEncodingExtPextrw,                    //!< EXT pextrw.
  kEncodingExtExtract,                   //!< EXT pextrb, pextrd, pextrq, extractps.
  kEncodingExtMov,                       //!< EXT mov?? - #1:[MM|XMM, MM|XMM|Mem] #2:[MM|XMM|Mem, MM|XMM].
  kEncodingExtMovbe,                     //!< EXT movbe.
  kEncodingExtMovd,                      //!< EXT movd.
  kEncodingExtMovq,                      //!< EXT movq.
  kEncodingExtExtrq,                     //!< EXT extrq (SSE4A).
  kEncodingExtInsertq,                   //!< EXT insrq (SSE4A).
  kEncodingExt3dNow,                     //!< EXT [RMI] (3DNOW specific).
  kEncodingVexOp,                        //!< VEX [OP].
  kEncodingVexOpMod,                     //!< VEX [OP] with MODR/M.
  kEncodingVexKmov,                      //!< VEX [RM|MR] (used by kmov[b|w|d|q]).
  kEncodingVexR_Wx,                      //!< VEX|EVEX [R] (propagatex VEX.W if GPQ used).
  kEncodingVexM,                         //!< VEX|EVEX [M].
  kEncodingVexM_VM,                      //!< VEX|EVEX [M] (propagates VEX|EVEX.L, VSIB support).
  kEncodingVexMr_Lx,                     //!< VEX|EVEX [MR] (propagates VEX|EVEX.L if YMM used).
  kEncodingVexMr_VM,                     //!< VEX|EVEX [MR] (VSIB support).
  kEncodingVexMri,                       //!< VEX|EVEX [MRI].
  kEncodingVexMri_Lx,                    //!< VEX|EVEX [MRI] (propagates VEX|EVEX.L if YMM used).
  kEncodingVexMri_Vpextrw,               //!< VEX|EVEX [MRI] (special case required by VPEXTRW instruction).
  kEncodingVexRm,                        //!< VEX|EVEX [RM].
  kEncodingVexRm_ZDI,                    //!< VEX|EVEX [RM<ZDI>].
  kEncodingVexRm_Wx,                     //!< VEX|EVEX [RM] (propagates VEX|EVEX.W if GPQ used).
  kEncodingVexRm_Lx,                     //!< VEX|EVEX [RM] (propagates VEX|EVEX.L if YMM used).
  kEncodingVexRm_Lx_Narrow,              //!< VEX|EVEX [RM] (the destination vector size is narrowed).
  kEncodingVexRm_Lx_Bcst,                //!< VEX|EVEX [RM] (can handle broadcast r32/r64).
  kEncodingVexRm_VM,                     //!< VEX|EVEX [RM] (propagates VEX|EVEX.L, VSIB support).
  kEncodingVexRm_T1_4X,                  //!<     EVEX [RM] (used by NN instructions that use RM-T1_4X encoding).
  kEncodingVexRmi,                       //!< VEX|EVEX [RMI].
  kEncodingVexRmi_Wx,                    //!< VEX|EVEX [RMI] (propagates VEX|EVEX.W if GPQ used).
  kEncodingVexRmi_Lx,                    //!< VEX|EVEX [RMI] (propagates VEX|EVEX.L if YMM used).
  kEncodingVexRvm,                       //!< VEX|EVEX [RVM].
  kEncodingVexRvm_Wx,                    //!< VEX|EVEX [RVM] (propagates VEX|EVEX.W if GPQ used).
  kEncodingVexRvm_ZDX_Wx,                //!< VEX|EVEX [RVM<ZDX>] (propagates VEX|EVEX.W if GPQ used).
  kEncodingVexRvm_Lx,                    //!< VEX|EVEX [RVM] (propagates VEX|EVEX.L if YMM used).
  kEncodingVexRvm_Lx_KEvex,              //!< VEX|EVEX [RVM] (forces EVEX prefix if K register is used on destination).
  kEncodingVexRvm_Lx_2xK,                //!< VEX|EVEX [RVM] (vp2intersectd/vp2intersectq).
  kEncodingVexRvmr,                      //!< VEX|EVEX [RVMR].
  kEncodingVexRvmr_Lx,                   //!< VEX|EVEX [RVMR] (propagates VEX|EVEX.L if YMM used).
  kEncodingVexRvmi,                      //!< VEX|EVEX [RVMI].
  kEncodingVexRvmi_KEvex,                //!< VEX|EVEX [RVMI] (forces EVEX prefix if K register is used on destination).
  kEncodingVexRvmi_Lx,                   //!< VEX|EVEX [RVMI] (propagates VEX|EVEX.L if YMM used).
  kEncodingVexRvmi_Lx_KEvex,             //!< VEX|EVEX [RVMI] (forces EVEX prefix if K register is used on destination).
  kEncodingVexRmv,                       //!< VEX|EVEX [RMV].
  kEncodingVexRmv_Wx,                    //!< VEX|EVEX [RMV] (propagates VEX|EVEX.W if GPQ used).
  kEncodingVexRmv_VM,                    //!< VEX|EVEX [RMV] (propagates VEX|EVEX.L, VSIB support).
  kEncodingVexRmvRm_VM,                  //!< VEX|EVEX [RMV|RM] (propagates VEX|EVEX.L, VSIB support).
  kEncodingVexRmvi,                      //!< VEX|EVEX [RMVI].
  kEncodingVexRmMr,                      //!< VEX|EVEX [RM|MR].
  kEncodingVexRmMr_Lx,                   //!< VEX|EVEX [RM|MR] (propagates VEX|EVEX.L if YMM used).
  kEncodingVexRvmRmv,                    //!< VEX|EVEX [RVM|RMV].
  kEncodingVexRvmRmi,                    //!< VEX|EVEX [RVM|RMI].
  kEncodingVexRvmRmi_Lx,                 //!< VEX|EVEX [RVM|RMI] (propagates VEX|EVEX.L if YMM used).
  kEncodingVexRvmRmvRmi,                 //!< VEX|EVEX [RVM|RMV|RMI].
  kEncodingVexRvmMr,                     //!< VEX|EVEX [RVM|MR].
  kEncodingVexRvmMvr,                    //!< VEX|EVEX [RVM|MVR].
  kEncodingVexRvmMvr_Lx,                 //!< VEX|EVEX [RVM|MVR] (propagates VEX|EVEX.L if YMM used).
  kEncodingVexRvmVmi,                    //!< VEX|EVEX [RVM|VMI].
  kEncodingVexRvmVmi_Lx,                 //!< VEX|EVEX [RVM|VMI] (propagates VEX|EVEX.L if YMM used).
  kEncodingVexRvmVmi_Lx_MEvex,           //!< VEX|EVEX [RVM|VMI] (propagates EVEX if the second operand is memory).
  kEncodingVexVm,                        //!< VEX|EVEX [VM].
  kEncodingVexVm_Wx,                     //!< VEX|EVEX [VM] (propagates VEX|EVEX.W if GPQ used).
  kEncodingVexVmi,                       //!< VEX|EVEX [VMI].
  kEncodingVexVmi_Lx,                    //!< VEX|EVEX [VMI] (propagates VEX|EVEX.L if YMM used).
  kEncodingVexVmi4_Wx,                   //!< VEX|EVEX [VMI] (propagates VEX|EVEX.W if GPQ used, DWORD Immediate).
  kEncodingVexVmi_Lx_MEvex,              //!< VEX|EVEX [VMI] (force EVEX prefix when the second operand is memory)
  kEncodingVexRvrmRvmr,                  //!< VEX|EVEX [RVRM|RVMR].
  kEncodingVexRvrmRvmr_Lx,               //!< VEX|EVEX [RVRM|RVMR] (propagates VEX|EVEX.L if YMM used).
  kEncodingVexRvrmiRvmri_Lx,             //!< VEX|EVEX [RVRMI|RVMRI] (propagates VEX|EVEX.L if YMM used).
  kEncodingVexMovdMovq,                  //!< VEX|EVEX vmovd, vmovq.
  kEncodingVexMovssMovsd,                //!< VEX|EVEX vmovss, vmovsd.
  kEncodingFma4,                         //!< FMA4 [R, R, R/M, R/M].
  kEncodingFma4_Lx,                      //!< FMA4 [R, R, R/M, R/M] (propagates AVX.L if YMM used).
  kEncodingAmxCfg,                       //!< AMX ldtilecfg/sttilecfg.
  kEncodingAmxR,                         //!< AMX [R] - tilezero.
  kEncodingAmxRm,                        //!< AMX tileloadd/tileloaddt1.
  kEncodingAmxMr,                        //!< AMX tilestored.
  kEncodingAmxRmv,                       //!< AMX instructions that use TMM registers.
  kEncodingCount                         //!< Count of instruction encodings.
};

// ============================================================================
// [asmjit::x86::InstDB - CommonInfoTableB]
// ============================================================================

//! CPU extensions required to execute instruction.
struct CommonInfoTableB {
  //! Features vector.
  uint8_t _features[6];
  //! Index to `_rwFlagsTable`.
  uint8_t _rwFlagsIndex;
  //! Reserved for future use.
  uint8_t _reserved;

  inline const uint8_t* featuresBegin() const noexcept { return _features; }
  inline const uint8_t* featuresEnd() const noexcept { return _features + ASMJIT_ARRAY_SIZE(_features); }
};

// ============================================================================
// [asmjit::x86::InstDB - InstNameIndex]
// ============================================================================

// ${NameLimits:Begin}
// ------------------- Automatically generated, do not edit -------------------
enum : uint32_t { kMaxNameSize = 17 };
// ----------------------------------------------------------------------------
// ${NameLimits:End}

struct InstNameIndex {
  uint16_t start;
  uint16_t end;
};

// ============================================================================
// [asmjit::x86::InstDB - RWInfo]
// ============================================================================

struct RWInfo {
  enum Category : uint8_t {
    kCategoryGeneric,
    kCategoryMov,
    kCategoryMovabs,
    kCategoryImul,
    kCategoryMovh64,
    kCategoryPunpcklxx,
    kCategoryVmaskmov,
    kCategoryVmovddup,
    kCategoryVmovmskpd,
    kCategoryVmovmskps,
    kCategoryVmov1_2,
    kCategoryVmov1_4,
    kCategoryVmov1_8,
    kCategoryVmov2_1,
    kCategoryVmov4_1,
    kCategoryVmov8_1
  };

  uint8_t category;
  uint8_t rmInfo;
  uint8_t opInfoIndex[6];
};

struct RWInfoOp {
  uint64_t rByteMask;
  uint64_t wByteMask;
  uint8_t physId;
  uint8_t reserved[3];
  uint32_t flags;
};

//! R/M information.
//!
//! This data is used to replace register operand by a memory operand reliably.
struct RWInfoRm {
  enum Category : uint8_t {
    kCategoryNone = 0,
    kCategoryFixed,
    kCategoryConsistent,
    kCategoryHalf,
    kCategoryQuarter,
    kCategoryEighth
  };

  enum Flags : uint8_t {
    kFlagAmbiguous = 0x01
  };

  uint8_t category;
  uint8_t rmOpsMask;
  uint8_t fixedSize;
  uint8_t flags;
  uint8_t rmFeature;
};

struct RWFlagsInfoTable {
  //! CPU/FPU flags read.
  uint32_t readFlags;
  //! CPU/FPU flags written or undefined.
  uint32_t writeFlags;
};

extern const uint8_t rwInfoIndexA[Inst::_kIdCount];
extern const uint8_t rwInfoIndexB[Inst::_kIdCount];
extern const RWInfo rwInfoA[];
extern const RWInfo rwInfoB[];
extern const RWInfoOp rwInfoOp[];
extern const RWInfoRm rwInfoRm[];
extern const RWFlagsInfoTable _rwFlagsInfoTable[];

// ============================================================================
// [asmjit::x86::InstDB::Tables]
// ============================================================================

extern const uint32_t _mainOpcodeTable[];
extern const uint32_t _altOpcodeTable[];

#ifndef ASMJIT_NO_TEXT
extern const char _nameData[];
extern const InstNameIndex instNameIndex[26];
#endif // !ASMJIT_NO_TEXT

extern const CommonInfoTableB _commonInfoTableB[];

} // {InstDB}

//! \}
//! \endcond

ASMJIT_END_SUB_NAMESPACE

#endif // ASMJIT_X86_X86INSTDB_P_H_INCLUDED
