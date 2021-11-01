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

#ifndef ASMJIT_X86_X86GLOBALS_H_INCLUDED
#define ASMJIT_X86_X86GLOBALS_H_INCLUDED

#include "../core/archtraits.h"
#include "../core/inst.h"

//! \namespace asmjit::x86
//! \ingroup asmjit_x86
//!
//! X86/X64 API.

ASMJIT_BEGIN_SUB_NAMESPACE(x86)

//! \addtogroup asmjit_x86
//! \{

// ============================================================================
// [asmjit::x86::Inst]
// ============================================================================

//! Instruction.
//!
//! \note Only used to hold x86-specific enumerations and static functions.
struct Inst : public BaseInst {
  //! Instruction id.
  enum Id : uint32_t {
    // ${InstId:Begin}
    kIdNone = 0,                         //!< Invalid instruction id.
    kIdAaa,                              //!< Instruction 'aaa' (X86).
    kIdAad,                              //!< Instruction 'aad' (X86).
    kIdAam,                              //!< Instruction 'aam' (X86).
    kIdAas,                              //!< Instruction 'aas' (X86).
    kIdAdc,                              //!< Instruction 'adc'.
    kIdAdcx,                             //!< Instruction 'adcx' {ADX}.
    kIdAdd,                              //!< Instruction 'add'.
    kIdAddpd,                            //!< Instruction 'addpd' {SSE2}.
    kIdAddps,                            //!< Instruction 'addps' {SSE}.
    kIdAddsd,                            //!< Instruction 'addsd' {SSE2}.
    kIdAddss,                            //!< Instruction 'addss' {SSE}.
    kIdAddsubpd,                         //!< Instruction 'addsubpd' {SSE3}.
    kIdAddsubps,                         //!< Instruction 'addsubps' {SSE3}.
    kIdAdox,                             //!< Instruction 'adox' {ADX}.
    kIdAesdec,                           //!< Instruction 'aesdec' {AESNI}.
    kIdAesdeclast,                       //!< Instruction 'aesdeclast' {AESNI}.
    kIdAesenc,                           //!< Instruction 'aesenc' {AESNI}.
    kIdAesenclast,                       //!< Instruction 'aesenclast' {AESNI}.
    kIdAesimc,                           //!< Instruction 'aesimc' {AESNI}.
    kIdAeskeygenassist,                  //!< Instruction 'aeskeygenassist' {AESNI}.
    kIdAnd,                              //!< Instruction 'and'.
    kIdAndn,                             //!< Instruction 'andn' {BMI}.
    kIdAndnpd,                           //!< Instruction 'andnpd' {SSE2}.
    kIdAndnps,                           //!< Instruction 'andnps' {SSE}.
    kIdAndpd,                            //!< Instruction 'andpd' {SSE2}.
    kIdAndps,                            //!< Instruction 'andps' {SSE}.
    kIdArpl,                             //!< Instruction 'arpl' (X86).
    kIdBextr,                            //!< Instruction 'bextr' {BMI}.
    kIdBlcfill,                          //!< Instruction 'blcfill' {TBM}.
    kIdBlci,                             //!< Instruction 'blci' {TBM}.
    kIdBlcic,                            //!< Instruction 'blcic' {TBM}.
    kIdBlcmsk,                           //!< Instruction 'blcmsk' {TBM}.
    kIdBlcs,                             //!< Instruction 'blcs' {TBM}.
    kIdBlendpd,                          //!< Instruction 'blendpd' {SSE4_1}.
    kIdBlendps,                          //!< Instruction 'blendps' {SSE4_1}.
    kIdBlendvpd,                         //!< Instruction 'blendvpd' {SSE4_1}.
    kIdBlendvps,                         //!< Instruction 'blendvps' {SSE4_1}.
    kIdBlsfill,                          //!< Instruction 'blsfill' {TBM}.
    kIdBlsi,                             //!< Instruction 'blsi' {BMI}.
    kIdBlsic,                            //!< Instruction 'blsic' {TBM}.
    kIdBlsmsk,                           //!< Instruction 'blsmsk' {BMI}.
    kIdBlsr,                             //!< Instruction 'blsr' {BMI}.
    kIdBndcl,                            //!< Instruction 'bndcl' {MPX}.
    kIdBndcn,                            //!< Instruction 'bndcn' {MPX}.
    kIdBndcu,                            //!< Instruction 'bndcu' {MPX}.
    kIdBndldx,                           //!< Instruction 'bndldx' {MPX}.
    kIdBndmk,                            //!< Instruction 'bndmk' {MPX}.
    kIdBndmov,                           //!< Instruction 'bndmov' {MPX}.
    kIdBndstx,                           //!< Instruction 'bndstx' {MPX}.
    kIdBound,                            //!< Instruction 'bound' (X86).
    kIdBsf,                              //!< Instruction 'bsf'.
    kIdBsr,                              //!< Instruction 'bsr'.
    kIdBswap,                            //!< Instruction 'bswap'.
    kIdBt,                               //!< Instruction 'bt'.
    kIdBtc,                              //!< Instruction 'btc'.
    kIdBtr,                              //!< Instruction 'btr'.
    kIdBts,                              //!< Instruction 'bts'.
    kIdBzhi,                             //!< Instruction 'bzhi' {BMI2}.
    kIdCall,                             //!< Instruction 'call'.
    kIdCbw,                              //!< Instruction 'cbw'.
    kIdCdq,                              //!< Instruction 'cdq'.
    kIdCdqe,                             //!< Instruction 'cdqe' (X64).
    kIdClac,                             //!< Instruction 'clac' {SMAP}.
    kIdClc,                              //!< Instruction 'clc'.
    kIdCld,                              //!< Instruction 'cld'.
    kIdCldemote,                         //!< Instruction 'cldemote' {CLDEMOTE}.
    kIdClflush,                          //!< Instruction 'clflush' {CLFLUSH}.
    kIdClflushopt,                       //!< Instruction 'clflushopt' {CLFLUSHOPT}.
    kIdClgi,                             //!< Instruction 'clgi' {SVM}.
    kIdCli,                              //!< Instruction 'cli'.
    kIdClrssbsy,                         //!< Instruction 'clrssbsy' {CET_SS}.
    kIdClts,                             //!< Instruction 'clts'.
    kIdClui,                             //!< Instruction 'clui' {UINTR} (X64).
    kIdClwb,                             //!< Instruction 'clwb' {CLWB}.
    kIdClzero,                           //!< Instruction 'clzero' {CLZERO}.
    kIdCmc,                              //!< Instruction 'cmc'.
    kIdCmova,                            //!< Instruction 'cmova' {CMOV}.
    kIdCmovae,                           //!< Instruction 'cmovae' {CMOV}.
    kIdCmovb,                            //!< Instruction 'cmovb' {CMOV}.
    kIdCmovbe,                           //!< Instruction 'cmovbe' {CMOV}.
    kIdCmovc,                            //!< Instruction 'cmovc' {CMOV}.
    kIdCmove,                            //!< Instruction 'cmove' {CMOV}.
    kIdCmovg,                            //!< Instruction 'cmovg' {CMOV}.
    kIdCmovge,                           //!< Instruction 'cmovge' {CMOV}.
    kIdCmovl,                            //!< Instruction 'cmovl' {CMOV}.
    kIdCmovle,                           //!< Instruction 'cmovle' {CMOV}.
    kIdCmovna,                           //!< Instruction 'cmovna' {CMOV}.
    kIdCmovnae,                          //!< Instruction 'cmovnae' {CMOV}.
    kIdCmovnb,                           //!< Instruction 'cmovnb' {CMOV}.
    kIdCmovnbe,                          //!< Instruction 'cmovnbe' {CMOV}.
    kIdCmovnc,                           //!< Instruction 'cmovnc' {CMOV}.
    kIdCmovne,                           //!< Instruction 'cmovne' {CMOV}.
    kIdCmovng,                           //!< Instruction 'cmovng' {CMOV}.
    kIdCmovnge,                          //!< Instruction 'cmovnge' {CMOV}.
    kIdCmovnl,                           //!< Instruction 'cmovnl' {CMOV}.
    kIdCmovnle,                          //!< Instruction 'cmovnle' {CMOV}.
    kIdCmovno,                           //!< Instruction 'cmovno' {CMOV}.
    kIdCmovnp,                           //!< Instruction 'cmovnp' {CMOV}.
    kIdCmovns,                           //!< Instruction 'cmovns' {CMOV}.
    kIdCmovnz,                           //!< Instruction 'cmovnz' {CMOV}.
    kIdCmovo,                            //!< Instruction 'cmovo' {CMOV}.
    kIdCmovp,                            //!< Instruction 'cmovp' {CMOV}.
    kIdCmovpe,                           //!< Instruction 'cmovpe' {CMOV}.
    kIdCmovpo,                           //!< Instruction 'cmovpo' {CMOV}.
    kIdCmovs,                            //!< Instruction 'cmovs' {CMOV}.
    kIdCmovz,                            //!< Instruction 'cmovz' {CMOV}.
    kIdCmp,                              //!< Instruction 'cmp'.
    kIdCmppd,                            //!< Instruction 'cmppd' {SSE2}.
    kIdCmpps,                            //!< Instruction 'cmpps' {SSE}.
    kIdCmps,                             //!< Instruction 'cmps'.
    kIdCmpsd,                            //!< Instruction 'cmpsd' {SSE2}.
    kIdCmpss,                            //!< Instruction 'cmpss' {SSE}.
    kIdCmpxchg,                          //!< Instruction 'cmpxchg' {I486}.
    kIdCmpxchg16b,                       //!< Instruction 'cmpxchg16b' {CMPXCHG16B} (X64).
    kIdCmpxchg8b,                        //!< Instruction 'cmpxchg8b' {CMPXCHG8B}.
    kIdComisd,                           //!< Instruction 'comisd' {SSE2}.
    kIdComiss,                           //!< Instruction 'comiss' {SSE}.
    kIdCpuid,                            //!< Instruction 'cpuid' {I486}.
    kIdCqo,                              //!< Instruction 'cqo' (X64).
    kIdCrc32,                            //!< Instruction 'crc32' {SSE4_2}.
    kIdCvtdq2pd,                         //!< Instruction 'cvtdq2pd' {SSE2}.
    kIdCvtdq2ps,                         //!< Instruction 'cvtdq2ps' {SSE2}.
    kIdCvtpd2dq,                         //!< Instruction 'cvtpd2dq' {SSE2}.
    kIdCvtpd2pi,                         //!< Instruction 'cvtpd2pi' {SSE2}.
    kIdCvtpd2ps,                         //!< Instruction 'cvtpd2ps' {SSE2}.
    kIdCvtpi2pd,                         //!< Instruction 'cvtpi2pd' {SSE2}.
    kIdCvtpi2ps,                         //!< Instruction 'cvtpi2ps' {SSE}.
    kIdCvtps2dq,                         //!< Instruction 'cvtps2dq' {SSE2}.
    kIdCvtps2pd,                         //!< Instruction 'cvtps2pd' {SSE2}.
    kIdCvtps2pi,                         //!< Instruction 'cvtps2pi' {SSE}.
    kIdCvtsd2si,                         //!< Instruction 'cvtsd2si' {SSE2}.
    kIdCvtsd2ss,                         //!< Instruction 'cvtsd2ss' {SSE2}.
    kIdCvtsi2sd,                         //!< Instruction 'cvtsi2sd' {SSE2}.
    kIdCvtsi2ss,                         //!< Instruction 'cvtsi2ss' {SSE}.
    kIdCvtss2sd,                         //!< Instruction 'cvtss2sd' {SSE2}.
    kIdCvtss2si,                         //!< Instruction 'cvtss2si' {SSE}.
    kIdCvttpd2dq,                        //!< Instruction 'cvttpd2dq' {SSE2}.
    kIdCvttpd2pi,                        //!< Instruction 'cvttpd2pi' {SSE2}.
    kIdCvttps2dq,                        //!< Instruction 'cvttps2dq' {SSE2}.
    kIdCvttps2pi,                        //!< Instruction 'cvttps2pi' {SSE}.
    kIdCvttsd2si,                        //!< Instruction 'cvttsd2si' {SSE2}.
    kIdCvttss2si,                        //!< Instruction 'cvttss2si' {SSE}.
    kIdCwd,                              //!< Instruction 'cwd'.
    kIdCwde,                             //!< Instruction 'cwde'.
    kIdDaa,                              //!< Instruction 'daa' (X86).
    kIdDas,                              //!< Instruction 'das' (X86).
    kIdDec,                              //!< Instruction 'dec'.
    kIdDiv,                              //!< Instruction 'div'.
    kIdDivpd,                            //!< Instruction 'divpd' {SSE2}.
    kIdDivps,                            //!< Instruction 'divps' {SSE}.
    kIdDivsd,                            //!< Instruction 'divsd' {SSE2}.
    kIdDivss,                            //!< Instruction 'divss' {SSE}.
    kIdDppd,                             //!< Instruction 'dppd' {SSE4_1}.
    kIdDpps,                             //!< Instruction 'dpps' {SSE4_1}.
    kIdEmms,                             //!< Instruction 'emms' {MMX}.
    kIdEndbr32,                          //!< Instruction 'endbr32' {CET_IBT}.
    kIdEndbr64,                          //!< Instruction 'endbr64' {CET_IBT}.
    kIdEnqcmd,                           //!< Instruction 'enqcmd' {ENQCMD}.
    kIdEnqcmds,                          //!< Instruction 'enqcmds' {ENQCMD}.
    kIdEnter,                            //!< Instruction 'enter'.
    kIdExtractps,                        //!< Instruction 'extractps' {SSE4_1}.
    kIdExtrq,                            //!< Instruction 'extrq' {SSE4A}.
    kIdF2xm1,                            //!< Instruction 'f2xm1'.
    kIdFabs,                             //!< Instruction 'fabs'.
    kIdFadd,                             //!< Instruction 'fadd'.
    kIdFaddp,                            //!< Instruction 'faddp'.
    kIdFbld,                             //!< Instruction 'fbld'.
    kIdFbstp,                            //!< Instruction 'fbstp'.
    kIdFchs,                             //!< Instruction 'fchs'.
    kIdFclex,                            //!< Instruction 'fclex'.
    kIdFcmovb,                           //!< Instruction 'fcmovb' {CMOV}.
    kIdFcmovbe,                          //!< Instruction 'fcmovbe' {CMOV}.
    kIdFcmove,                           //!< Instruction 'fcmove' {CMOV}.
    kIdFcmovnb,                          //!< Instruction 'fcmovnb' {CMOV}.
    kIdFcmovnbe,                         //!< Instruction 'fcmovnbe' {CMOV}.
    kIdFcmovne,                          //!< Instruction 'fcmovne' {CMOV}.
    kIdFcmovnu,                          //!< Instruction 'fcmovnu' {CMOV}.
    kIdFcmovu,                           //!< Instruction 'fcmovu' {CMOV}.
    kIdFcom,                             //!< Instruction 'fcom'.
    kIdFcomi,                            //!< Instruction 'fcomi'.
    kIdFcomip,                           //!< Instruction 'fcomip'.
    kIdFcomp,                            //!< Instruction 'fcomp'.
    kIdFcompp,                           //!< Instruction 'fcompp'.
    kIdFcos,                             //!< Instruction 'fcos'.
    kIdFdecstp,                          //!< Instruction 'fdecstp'.
    kIdFdiv,                             //!< Instruction 'fdiv'.
    kIdFdivp,                            //!< Instruction 'fdivp'.
    kIdFdivr,                            //!< Instruction 'fdivr'.
    kIdFdivrp,                           //!< Instruction 'fdivrp'.
    kIdFemms,                            //!< Instruction 'femms' {3DNOW}.
    kIdFfree,                            //!< Instruction 'ffree'.
    kIdFiadd,                            //!< Instruction 'fiadd'.
    kIdFicom,                            //!< Instruction 'ficom'.
    kIdFicomp,                           //!< Instruction 'ficomp'.
    kIdFidiv,                            //!< Instruction 'fidiv'.
    kIdFidivr,                           //!< Instruction 'fidivr'.
    kIdFild,                             //!< Instruction 'fild'.
    kIdFimul,                            //!< Instruction 'fimul'.
    kIdFincstp,                          //!< Instruction 'fincstp'.
    kIdFinit,                            //!< Instruction 'finit'.
    kIdFist,                             //!< Instruction 'fist'.
    kIdFistp,                            //!< Instruction 'fistp'.
    kIdFisttp,                           //!< Instruction 'fisttp' {SSE3}.
    kIdFisub,                            //!< Instruction 'fisub'.
    kIdFisubr,                           //!< Instruction 'fisubr'.
    kIdFld,                              //!< Instruction 'fld'.
    kIdFld1,                             //!< Instruction 'fld1'.
    kIdFldcw,                            //!< Instruction 'fldcw'.
    kIdFldenv,                           //!< Instruction 'fldenv'.
    kIdFldl2e,                           //!< Instruction 'fldl2e'.
    kIdFldl2t,                           //!< Instruction 'fldl2t'.
    kIdFldlg2,                           //!< Instruction 'fldlg2'.
    kIdFldln2,                           //!< Instruction 'fldln2'.
    kIdFldpi,                            //!< Instruction 'fldpi'.
    kIdFldz,                             //!< Instruction 'fldz'.
    kIdFmul,                             //!< Instruction 'fmul'.
    kIdFmulp,                            //!< Instruction 'fmulp'.
    kIdFnclex,                           //!< Instruction 'fnclex'.
    kIdFninit,                           //!< Instruction 'fninit'.
    kIdFnop,                             //!< Instruction 'fnop'.
    kIdFnsave,                           //!< Instruction 'fnsave'.
    kIdFnstcw,                           //!< Instruction 'fnstcw'.
    kIdFnstenv,                          //!< Instruction 'fnstenv'.
    kIdFnstsw,                           //!< Instruction 'fnstsw'.
    kIdFpatan,                           //!< Instruction 'fpatan'.
    kIdFprem,                            //!< Instruction 'fprem'.
    kIdFprem1,                           //!< Instruction 'fprem1'.
    kIdFptan,                            //!< Instruction 'fptan'.
    kIdFrndint,                          //!< Instruction 'frndint'.
    kIdFrstor,                           //!< Instruction 'frstor'.
    kIdFsave,                            //!< Instruction 'fsave'.
    kIdFscale,                           //!< Instruction 'fscale'.
    kIdFsin,                             //!< Instruction 'fsin'.
    kIdFsincos,                          //!< Instruction 'fsincos'.
    kIdFsqrt,                            //!< Instruction 'fsqrt'.
    kIdFst,                              //!< Instruction 'fst'.
    kIdFstcw,                            //!< Instruction 'fstcw'.
    kIdFstenv,                           //!< Instruction 'fstenv'.
    kIdFstp,                             //!< Instruction 'fstp'.
    kIdFstsw,                            //!< Instruction 'fstsw'.
    kIdFsub,                             //!< Instruction 'fsub'.
    kIdFsubp,                            //!< Instruction 'fsubp'.
    kIdFsubr,                            //!< Instruction 'fsubr'.
    kIdFsubrp,                           //!< Instruction 'fsubrp'.
    kIdFtst,                             //!< Instruction 'ftst'.
    kIdFucom,                            //!< Instruction 'fucom'.
    kIdFucomi,                           //!< Instruction 'fucomi'.
    kIdFucomip,                          //!< Instruction 'fucomip'.
    kIdFucomp,                           //!< Instruction 'fucomp'.
    kIdFucompp,                          //!< Instruction 'fucompp'.
    kIdFwait,                            //!< Instruction 'fwait'.
    kIdFxam,                             //!< Instruction 'fxam'.
    kIdFxch,                             //!< Instruction 'fxch'.
    kIdFxrstor,                          //!< Instruction 'fxrstor' {FXSR}.
    kIdFxrstor64,                        //!< Instruction 'fxrstor64' {FXSR} (X64).
    kIdFxsave,                           //!< Instruction 'fxsave' {FXSR}.
    kIdFxsave64,                         //!< Instruction 'fxsave64' {FXSR} (X64).
    kIdFxtract,                          //!< Instruction 'fxtract'.
    kIdFyl2x,                            //!< Instruction 'fyl2x'.
    kIdFyl2xp1,                          //!< Instruction 'fyl2xp1'.
    kIdGetsec,                           //!< Instruction 'getsec' {SMX}.
    kIdGf2p8affineinvqb,                 //!< Instruction 'gf2p8affineinvqb' {GFNI}.
    kIdGf2p8affineqb,                    //!< Instruction 'gf2p8affineqb' {GFNI}.
    kIdGf2p8mulb,                        //!< Instruction 'gf2p8mulb' {GFNI}.
    kIdHaddpd,                           //!< Instruction 'haddpd' {SSE3}.
    kIdHaddps,                           //!< Instruction 'haddps' {SSE3}.
    kIdHlt,                              //!< Instruction 'hlt'.
    kIdHreset,                           //!< Instruction 'hreset' {HRESET}.
    kIdHsubpd,                           //!< Instruction 'hsubpd' {SSE3}.
    kIdHsubps,                           //!< Instruction 'hsubps' {SSE3}.
    kIdIdiv,                             //!< Instruction 'idiv'.
    kIdImul,                             //!< Instruction 'imul'.
    kIdIn,                               //!< Instruction 'in'.
    kIdInc,                              //!< Instruction 'inc'.
    kIdIncsspd,                          //!< Instruction 'incsspd' {CET_SS}.
    kIdIncsspq,                          //!< Instruction 'incsspq' {CET_SS} (X64).
    kIdIns,                              //!< Instruction 'ins'.
    kIdInsertps,                         //!< Instruction 'insertps' {SSE4_1}.
    kIdInsertq,                          //!< Instruction 'insertq' {SSE4A}.
    kIdInt,                              //!< Instruction 'int'.
    kIdInt3,                             //!< Instruction 'int3'.
    kIdInto,                             //!< Instruction 'into' (X86).
    kIdInvd,                             //!< Instruction 'invd' {I486}.
    kIdInvept,                           //!< Instruction 'invept' {VMX}.
    kIdInvlpg,                           //!< Instruction 'invlpg' {I486}.
    kIdInvlpga,                          //!< Instruction 'invlpga' {SVM}.
    kIdInvpcid,                          //!< Instruction 'invpcid' {I486}.
    kIdInvvpid,                          //!< Instruction 'invvpid' {VMX}.
    kIdIret,                             //!< Instruction 'iret'.
    kIdIretd,                            //!< Instruction 'iretd'.
    kIdIretq,                            //!< Instruction 'iretq' (X64).
    kIdJa,                               //!< Instruction 'ja'.
    kIdJae,                              //!< Instruction 'jae'.
    kIdJb,                               //!< Instruction 'jb'.
    kIdJbe,                              //!< Instruction 'jbe'.
    kIdJc,                               //!< Instruction 'jc'.
    kIdJe,                               //!< Instruction 'je'.
    kIdJecxz,                            //!< Instruction 'jecxz'.
    kIdJg,                               //!< Instruction 'jg'.
    kIdJge,                              //!< Instruction 'jge'.
    kIdJl,                               //!< Instruction 'jl'.
    kIdJle,                              //!< Instruction 'jle'.
    kIdJmp,                              //!< Instruction 'jmp'.
    kIdJna,                              //!< Instruction 'jna'.
    kIdJnae,                             //!< Instruction 'jnae'.
    kIdJnb,                              //!< Instruction 'jnb'.
    kIdJnbe,                             //!< Instruction 'jnbe'.
    kIdJnc,                              //!< Instruction 'jnc'.
    kIdJne,                              //!< Instruction 'jne'.
    kIdJng,                              //!< Instruction 'jng'.
    kIdJnge,                             //!< Instruction 'jnge'.
    kIdJnl,                              //!< Instruction 'jnl'.
    kIdJnle,                             //!< Instruction 'jnle'.
    kIdJno,                              //!< Instruction 'jno'.
    kIdJnp,                              //!< Instruction 'jnp'.
    kIdJns,                              //!< Instruction 'jns'.
    kIdJnz,                              //!< Instruction 'jnz'.
    kIdJo,                               //!< Instruction 'jo'.
    kIdJp,                               //!< Instruction 'jp'.
    kIdJpe,                              //!< Instruction 'jpe'.
    kIdJpo,                              //!< Instruction 'jpo'.
    kIdJs,                               //!< Instruction 'js'.
    kIdJz,                               //!< Instruction 'jz'.
    kIdKaddb,                            //!< Instruction 'kaddb' {AVX512_DQ}.
    kIdKaddd,                            //!< Instruction 'kaddd' {AVX512_BW}.
    kIdKaddq,                            //!< Instruction 'kaddq' {AVX512_BW}.
    kIdKaddw,                            //!< Instruction 'kaddw' {AVX512_DQ}.
    kIdKandb,                            //!< Instruction 'kandb' {AVX512_DQ}.
    kIdKandd,                            //!< Instruction 'kandd' {AVX512_BW}.
    kIdKandnb,                           //!< Instruction 'kandnb' {AVX512_DQ}.
    kIdKandnd,                           //!< Instruction 'kandnd' {AVX512_BW}.
    kIdKandnq,                           //!< Instruction 'kandnq' {AVX512_BW}.
    kIdKandnw,                           //!< Instruction 'kandnw' {AVX512_F}.
    kIdKandq,                            //!< Instruction 'kandq' {AVX512_BW}.
    kIdKandw,                            //!< Instruction 'kandw' {AVX512_F}.
    kIdKmovb,                            //!< Instruction 'kmovb' {AVX512_DQ}.
    kIdKmovd,                            //!< Instruction 'kmovd' {AVX512_BW}.
    kIdKmovq,                            //!< Instruction 'kmovq' {AVX512_BW}.
    kIdKmovw,                            //!< Instruction 'kmovw' {AVX512_F}.
    kIdKnotb,                            //!< Instruction 'knotb' {AVX512_DQ}.
    kIdKnotd,                            //!< Instruction 'knotd' {AVX512_BW}.
    kIdKnotq,                            //!< Instruction 'knotq' {AVX512_BW}.
    kIdKnotw,                            //!< Instruction 'knotw' {AVX512_F}.
    kIdKorb,                             //!< Instruction 'korb' {AVX512_DQ}.
    kIdKord,                             //!< Instruction 'kord' {AVX512_BW}.
    kIdKorq,                             //!< Instruction 'korq' {AVX512_BW}.
    kIdKortestb,                         //!< Instruction 'kortestb' {AVX512_DQ}.
    kIdKortestd,                         //!< Instruction 'kortestd' {AVX512_BW}.
    kIdKortestq,                         //!< Instruction 'kortestq' {AVX512_BW}.
    kIdKortestw,                         //!< Instruction 'kortestw' {AVX512_F}.
    kIdKorw,                             //!< Instruction 'korw' {AVX512_F}.
    kIdKshiftlb,                         //!< Instruction 'kshiftlb' {AVX512_DQ}.
    kIdKshiftld,                         //!< Instruction 'kshiftld' {AVX512_BW}.
    kIdKshiftlq,                         //!< Instruction 'kshiftlq' {AVX512_BW}.
    kIdKshiftlw,                         //!< Instruction 'kshiftlw' {AVX512_F}.
    kIdKshiftrb,                         //!< Instruction 'kshiftrb' {AVX512_DQ}.
    kIdKshiftrd,                         //!< Instruction 'kshiftrd' {AVX512_BW}.
    kIdKshiftrq,                         //!< Instruction 'kshiftrq' {AVX512_BW}.
    kIdKshiftrw,                         //!< Instruction 'kshiftrw' {AVX512_F}.
    kIdKtestb,                           //!< Instruction 'ktestb' {AVX512_DQ}.
    kIdKtestd,                           //!< Instruction 'ktestd' {AVX512_BW}.
    kIdKtestq,                           //!< Instruction 'ktestq' {AVX512_BW}.
    kIdKtestw,                           //!< Instruction 'ktestw' {AVX512_DQ}.
    kIdKunpckbw,                         //!< Instruction 'kunpckbw' {AVX512_F}.
    kIdKunpckdq,                         //!< Instruction 'kunpckdq' {AVX512_BW}.
    kIdKunpckwd,                         //!< Instruction 'kunpckwd' {AVX512_BW}.
    kIdKxnorb,                           //!< Instruction 'kxnorb' {AVX512_DQ}.
    kIdKxnord,                           //!< Instruction 'kxnord' {AVX512_BW}.
    kIdKxnorq,                           //!< Instruction 'kxnorq' {AVX512_BW}.
    kIdKxnorw,                           //!< Instruction 'kxnorw' {AVX512_F}.
    kIdKxorb,                            //!< Instruction 'kxorb' {AVX512_DQ}.
    kIdKxord,                            //!< Instruction 'kxord' {AVX512_BW}.
    kIdKxorq,                            //!< Instruction 'kxorq' {AVX512_BW}.
    kIdKxorw,                            //!< Instruction 'kxorw' {AVX512_F}.
    kIdLahf,                             //!< Instruction 'lahf' {LAHFSAHF}.
    kIdLar,                              //!< Instruction 'lar'.
    kIdLcall,                            //!< Instruction 'lcall'.
    kIdLddqu,                            //!< Instruction 'lddqu' {SSE3}.
    kIdLdmxcsr,                          //!< Instruction 'ldmxcsr' {SSE}.
    kIdLds,                              //!< Instruction 'lds' (X86).
    kIdLdtilecfg,                        //!< Instruction 'ldtilecfg' {AMX_TILE} (X64).
    kIdLea,                              //!< Instruction 'lea'.
    kIdLeave,                            //!< Instruction 'leave'.
    kIdLes,                              //!< Instruction 'les' (X86).
    kIdLfence,                           //!< Instruction 'lfence' {SSE2}.
    kIdLfs,                              //!< Instruction 'lfs'.
    kIdLgdt,                             //!< Instruction 'lgdt'.
    kIdLgs,                              //!< Instruction 'lgs'.
    kIdLidt,                             //!< Instruction 'lidt'.
    kIdLjmp,                             //!< Instruction 'ljmp'.
    kIdLldt,                             //!< Instruction 'lldt'.
    kIdLlwpcb,                           //!< Instruction 'llwpcb' {LWP}.
    kIdLmsw,                             //!< Instruction 'lmsw'.
    kIdLods,                             //!< Instruction 'lods'.
    kIdLoop,                             //!< Instruction 'loop'.
    kIdLoope,                            //!< Instruction 'loope'.
    kIdLoopne,                           //!< Instruction 'loopne'.
    kIdLsl,                              //!< Instruction 'lsl'.
    kIdLss,                              //!< Instruction 'lss'.
    kIdLtr,                              //!< Instruction 'ltr'.
    kIdLwpins,                           //!< Instruction 'lwpins' {LWP}.
    kIdLwpval,                           //!< Instruction 'lwpval' {LWP}.
    kIdLzcnt,                            //!< Instruction 'lzcnt' {LZCNT}.
    kIdMaskmovdqu,                       //!< Instruction 'maskmovdqu' {SSE2}.
    kIdMaskmovq,                         //!< Instruction 'maskmovq' {MMX2}.
    kIdMaxpd,                            //!< Instruction 'maxpd' {SSE2}.
    kIdMaxps,                            //!< Instruction 'maxps' {SSE}.
    kIdMaxsd,                            //!< Instruction 'maxsd' {SSE2}.
    kIdMaxss,                            //!< Instruction 'maxss' {SSE}.
    kIdMcommit,                          //!< Instruction 'mcommit' {MCOMMIT}.
    kIdMfence,                           //!< Instruction 'mfence' {SSE2}.
    kIdMinpd,                            //!< Instruction 'minpd' {SSE2}.
    kIdMinps,                            //!< Instruction 'minps' {SSE}.
    kIdMinsd,                            //!< Instruction 'minsd' {SSE2}.
    kIdMinss,                            //!< Instruction 'minss' {SSE}.
    kIdMonitor,                          //!< Instruction 'monitor' {MONITOR}.
    kIdMonitorx,                         //!< Instruction 'monitorx' {MONITORX}.
    kIdMov,                              //!< Instruction 'mov'.
    kIdMovabs,                           //!< Instruction 'movabs' (X64).
    kIdMovapd,                           //!< Instruction 'movapd' {SSE2}.
    kIdMovaps,                           //!< Instruction 'movaps' {SSE}.
    kIdMovbe,                            //!< Instruction 'movbe' {MOVBE}.
    kIdMovd,                             //!< Instruction 'movd' {MMX|SSE2}.
    kIdMovddup,                          //!< Instruction 'movddup' {SSE3}.
    kIdMovdir64b,                        //!< Instruction 'movdir64b' {MOVDIR64B}.
    kIdMovdiri,                          //!< Instruction 'movdiri' {MOVDIRI}.
    kIdMovdq2q,                          //!< Instruction 'movdq2q' {SSE2}.
    kIdMovdqa,                           //!< Instruction 'movdqa' {SSE2}.
    kIdMovdqu,                           //!< Instruction 'movdqu' {SSE2}.
    kIdMovhlps,                          //!< Instruction 'movhlps' {SSE}.
    kIdMovhpd,                           //!< Instruction 'movhpd' {SSE2}.
    kIdMovhps,                           //!< Instruction 'movhps' {SSE}.
    kIdMovlhps,                          //!< Instruction 'movlhps' {SSE}.
    kIdMovlpd,                           //!< Instruction 'movlpd' {SSE2}.
    kIdMovlps,                           //!< Instruction 'movlps' {SSE}.
    kIdMovmskpd,                         //!< Instruction 'movmskpd' {SSE2}.
    kIdMovmskps,                         //!< Instruction 'movmskps' {SSE}.
    kIdMovntdq,                          //!< Instruction 'movntdq' {SSE2}.
    kIdMovntdqa,                         //!< Instruction 'movntdqa' {SSE4_1}.
    kIdMovnti,                           //!< Instruction 'movnti' {SSE2}.
    kIdMovntpd,                          //!< Instruction 'movntpd' {SSE2}.
    kIdMovntps,                          //!< Instruction 'movntps' {SSE}.
    kIdMovntq,                           //!< Instruction 'movntq' {MMX2}.
    kIdMovntsd,                          //!< Instruction 'movntsd' {SSE4A}.
    kIdMovntss,                          //!< Instruction 'movntss' {SSE4A}.
    kIdMovq,                             //!< Instruction 'movq' {MMX|SSE2}.
    kIdMovq2dq,                          //!< Instruction 'movq2dq' {SSE2}.
    kIdMovs,                             //!< Instruction 'movs'.
    kIdMovsd,                            //!< Instruction 'movsd' {SSE2}.
    kIdMovshdup,                         //!< Instruction 'movshdup' {SSE3}.
    kIdMovsldup,                         //!< Instruction 'movsldup' {SSE3}.
    kIdMovss,                            //!< Instruction 'movss' {SSE}.
    kIdMovsx,                            //!< Instruction 'movsx'.
    kIdMovsxd,                           //!< Instruction 'movsxd' (X64).
    kIdMovupd,                           //!< Instruction 'movupd' {SSE2}.
    kIdMovups,                           //!< Instruction 'movups' {SSE}.
    kIdMovzx,                            //!< Instruction 'movzx'.
    kIdMpsadbw,                          //!< Instruction 'mpsadbw' {SSE4_1}.
    kIdMul,                              //!< Instruction 'mul'.
    kIdMulpd,                            //!< Instruction 'mulpd' {SSE2}.
    kIdMulps,                            //!< Instruction 'mulps' {SSE}.
    kIdMulsd,                            //!< Instruction 'mulsd' {SSE2}.
    kIdMulss,                            //!< Instruction 'mulss' {SSE}.
    kIdMulx,                             //!< Instruction 'mulx' {BMI2}.
    kIdMwait,                            //!< Instruction 'mwait' {MONITOR}.
    kIdMwaitx,                           //!< Instruction 'mwaitx' {MONITORX}.
    kIdNeg,                              //!< Instruction 'neg'.
    kIdNop,                              //!< Instruction 'nop'.
    kIdNot,                              //!< Instruction 'not'.
    kIdOr,                               //!< Instruction 'or'.
    kIdOrpd,                             //!< Instruction 'orpd' {SSE2}.
    kIdOrps,                             //!< Instruction 'orps' {SSE}.
    kIdOut,                              //!< Instruction 'out'.
    kIdOuts,                             //!< Instruction 'outs'.
    kIdPabsb,                            //!< Instruction 'pabsb' {SSSE3}.
    kIdPabsd,                            //!< Instruction 'pabsd' {SSSE3}.
    kIdPabsw,                            //!< Instruction 'pabsw' {SSSE3}.
    kIdPackssdw,                         //!< Instruction 'packssdw' {MMX|SSE2}.
    kIdPacksswb,                         //!< Instruction 'packsswb' {MMX|SSE2}.
    kIdPackusdw,                         //!< Instruction 'packusdw' {SSE4_1}.
    kIdPackuswb,                         //!< Instruction 'packuswb' {MMX|SSE2}.
    kIdPaddb,                            //!< Instruction 'paddb' {MMX|SSE2}.
    kIdPaddd,                            //!< Instruction 'paddd' {MMX|SSE2}.
    kIdPaddq,                            //!< Instruction 'paddq' {SSE2}.
    kIdPaddsb,                           //!< Instruction 'paddsb' {MMX|SSE2}.
    kIdPaddsw,                           //!< Instruction 'paddsw' {MMX|SSE2}.
    kIdPaddusb,                          //!< Instruction 'paddusb' {MMX|SSE2}.
    kIdPaddusw,                          //!< Instruction 'paddusw' {MMX|SSE2}.
    kIdPaddw,                            //!< Instruction 'paddw' {MMX|SSE2}.
    kIdPalignr,                          //!< Instruction 'palignr' {SSE3}.
    kIdPand,                             //!< Instruction 'pand' {MMX|SSE2}.
    kIdPandn,                            //!< Instruction 'pandn' {MMX|SSE2}.
    kIdPause,                            //!< Instruction 'pause'.
    kIdPavgb,                            //!< Instruction 'pavgb' {MMX2|SSE2}.
    kIdPavgusb,                          //!< Instruction 'pavgusb' {3DNOW}.
    kIdPavgw,                            //!< Instruction 'pavgw' {MMX2|SSE2}.
    kIdPblendvb,                         //!< Instruction 'pblendvb' {SSE4_1}.
    kIdPblendw,                          //!< Instruction 'pblendw' {SSE4_1}.
    kIdPclmulqdq,                        //!< Instruction 'pclmulqdq' {PCLMULQDQ}.
    kIdPcmpeqb,                          //!< Instruction 'pcmpeqb' {MMX|SSE2}.
    kIdPcmpeqd,                          //!< Instruction 'pcmpeqd' {MMX|SSE2}.
    kIdPcmpeqq,                          //!< Instruction 'pcmpeqq' {SSE4_1}.
    kIdPcmpeqw,                          //!< Instruction 'pcmpeqw' {MMX|SSE2}.
    kIdPcmpestri,                        //!< Instruction 'pcmpestri' {SSE4_2}.
    kIdPcmpestrm,                        //!< Instruction 'pcmpestrm' {SSE4_2}.
    kIdPcmpgtb,                          //!< Instruction 'pcmpgtb' {MMX|SSE2}.
    kIdPcmpgtd,                          //!< Instruction 'pcmpgtd' {MMX|SSE2}.
    kIdPcmpgtq,                          //!< Instruction 'pcmpgtq' {SSE4_2}.
    kIdPcmpgtw,                          //!< Instruction 'pcmpgtw' {MMX|SSE2}.
    kIdPcmpistri,                        //!< Instruction 'pcmpistri' {SSE4_2}.
    kIdPcmpistrm,                        //!< Instruction 'pcmpistrm' {SSE4_2}.
    kIdPconfig,                          //!< Instruction 'pconfig' {PCONFIG}.
    kIdPdep,                             //!< Instruction 'pdep' {BMI2}.
    kIdPext,                             //!< Instruction 'pext' {BMI2}.
    kIdPextrb,                           //!< Instruction 'pextrb' {SSE4_1}.
    kIdPextrd,                           //!< Instruction 'pextrd' {SSE4_1}.
    kIdPextrq,                           //!< Instruction 'pextrq' {SSE4_1} (X64).
    kIdPextrw,                           //!< Instruction 'pextrw' {MMX2|SSE2|SSE4_1}.
    kIdPf2id,                            //!< Instruction 'pf2id' {3DNOW}.
    kIdPf2iw,                            //!< Instruction 'pf2iw' {3DNOW2}.
    kIdPfacc,                            //!< Instruction 'pfacc' {3DNOW}.
    kIdPfadd,                            //!< Instruction 'pfadd' {3DNOW}.
    kIdPfcmpeq,                          //!< Instruction 'pfcmpeq' {3DNOW}.
    kIdPfcmpge,                          //!< Instruction 'pfcmpge' {3DNOW}.
    kIdPfcmpgt,                          //!< Instruction 'pfcmpgt' {3DNOW}.
    kIdPfmax,                            //!< Instruction 'pfmax' {3DNOW}.
    kIdPfmin,                            //!< Instruction 'pfmin' {3DNOW}.
    kIdPfmul,                            //!< Instruction 'pfmul' {3DNOW}.
    kIdPfnacc,                           //!< Instruction 'pfnacc' {3DNOW2}.
    kIdPfpnacc,                          //!< Instruction 'pfpnacc' {3DNOW2}.
    kIdPfrcp,                            //!< Instruction 'pfrcp' {3DNOW}.
    kIdPfrcpit1,                         //!< Instruction 'pfrcpit1' {3DNOW}.
    kIdPfrcpit2,                         //!< Instruction 'pfrcpit2' {3DNOW}.
    kIdPfrcpv,                           //!< Instruction 'pfrcpv' {GEODE}.
    kIdPfrsqit1,                         //!< Instruction 'pfrsqit1' {3DNOW}.
    kIdPfrsqrt,                          //!< Instruction 'pfrsqrt' {3DNOW}.
    kIdPfrsqrtv,                         //!< Instruction 'pfrsqrtv' {GEODE}.
    kIdPfsub,                            //!< Instruction 'pfsub' {3DNOW}.
    kIdPfsubr,                           //!< Instruction 'pfsubr' {3DNOW}.
    kIdPhaddd,                           //!< Instruction 'phaddd' {SSSE3}.
    kIdPhaddsw,                          //!< Instruction 'phaddsw' {SSSE3}.
    kIdPhaddw,                           //!< Instruction 'phaddw' {SSSE3}.
    kIdPhminposuw,                       //!< Instruction 'phminposuw' {SSE4_1}.
    kIdPhsubd,                           //!< Instruction 'phsubd' {SSSE3}.
    kIdPhsubsw,                          //!< Instruction 'phsubsw' {SSSE3}.
    kIdPhsubw,                           //!< Instruction 'phsubw' {SSSE3}.
    kIdPi2fd,                            //!< Instruction 'pi2fd' {3DNOW}.
    kIdPi2fw,                            //!< Instruction 'pi2fw' {3DNOW2}.
    kIdPinsrb,                           //!< Instruction 'pinsrb' {SSE4_1}.
    kIdPinsrd,                           //!< Instruction 'pinsrd' {SSE4_1}.
    kIdPinsrq,                           //!< Instruction 'pinsrq' {SSE4_1} (X64).
    kIdPinsrw,                           //!< Instruction 'pinsrw' {MMX2|SSE2}.
    kIdPmaddubsw,                        //!< Instruction 'pmaddubsw' {SSSE3}.
    kIdPmaddwd,                          //!< Instruction 'pmaddwd' {MMX|SSE2}.
    kIdPmaxsb,                           //!< Instruction 'pmaxsb' {SSE4_1}.
    kIdPmaxsd,                           //!< Instruction 'pmaxsd' {SSE4_1}.
    kIdPmaxsw,                           //!< Instruction 'pmaxsw' {MMX2|SSE2}.
    kIdPmaxub,                           //!< Instruction 'pmaxub' {MMX2|SSE2}.
    kIdPmaxud,                           //!< Instruction 'pmaxud' {SSE4_1}.
    kIdPmaxuw,                           //!< Instruction 'pmaxuw' {SSE4_1}.
    kIdPminsb,                           //!< Instruction 'pminsb' {SSE4_1}.
    kIdPminsd,                           //!< Instruction 'pminsd' {SSE4_1}.
    kIdPminsw,                           //!< Instruction 'pminsw' {MMX2|SSE2}.
    kIdPminub,                           //!< Instruction 'pminub' {MMX2|SSE2}.
    kIdPminud,                           //!< Instruction 'pminud' {SSE4_1}.
    kIdPminuw,                           //!< Instruction 'pminuw' {SSE4_1}.
    kIdPmovmskb,                         //!< Instruction 'pmovmskb' {MMX2|SSE2}.
    kIdPmovsxbd,                         //!< Instruction 'pmovsxbd' {SSE4_1}.
    kIdPmovsxbq,                         //!< Instruction 'pmovsxbq' {SSE4_1}.
    kIdPmovsxbw,                         //!< Instruction 'pmovsxbw' {SSE4_1}.
    kIdPmovsxdq,                         //!< Instruction 'pmovsxdq' {SSE4_1}.
    kIdPmovsxwd,                         //!< Instruction 'pmovsxwd' {SSE4_1}.
    kIdPmovsxwq,                         //!< Instruction 'pmovsxwq' {SSE4_1}.
    kIdPmovzxbd,                         //!< Instruction 'pmovzxbd' {SSE4_1}.
    kIdPmovzxbq,                         //!< Instruction 'pmovzxbq' {SSE4_1}.
    kIdPmovzxbw,                         //!< Instruction 'pmovzxbw' {SSE4_1}.
    kIdPmovzxdq,                         //!< Instruction 'pmovzxdq' {SSE4_1}.
    kIdPmovzxwd,                         //!< Instruction 'pmovzxwd' {SSE4_1}.
    kIdPmovzxwq,                         //!< Instruction 'pmovzxwq' {SSE4_1}.
    kIdPmuldq,                           //!< Instruction 'pmuldq' {SSE4_1}.
    kIdPmulhrsw,                         //!< Instruction 'pmulhrsw' {SSSE3}.
    kIdPmulhrw,                          //!< Instruction 'pmulhrw' {3DNOW}.
    kIdPmulhuw,                          //!< Instruction 'pmulhuw' {MMX2|SSE2}.
    kIdPmulhw,                           //!< Instruction 'pmulhw' {MMX|SSE2}.
    kIdPmulld,                           //!< Instruction 'pmulld' {SSE4_1}.
    kIdPmullw,                           //!< Instruction 'pmullw' {MMX|SSE2}.
    kIdPmuludq,                          //!< Instruction 'pmuludq' {SSE2}.
    kIdPop,                              //!< Instruction 'pop'.
    kIdPopa,                             //!< Instruction 'popa' (X86).
    kIdPopad,                            //!< Instruction 'popad' (X86).
    kIdPopcnt,                           //!< Instruction 'popcnt' {POPCNT}.
    kIdPopf,                             //!< Instruction 'popf'.
    kIdPopfd,                            //!< Instruction 'popfd' (X86).
    kIdPopfq,                            //!< Instruction 'popfq' (X64).
    kIdPor,                              //!< Instruction 'por' {MMX|SSE2}.
    kIdPrefetch,                         //!< Instruction 'prefetch' {3DNOW}.
    kIdPrefetchnta,                      //!< Instruction 'prefetchnta' {MMX2}.
    kIdPrefetcht0,                       //!< Instruction 'prefetcht0' {MMX2}.
    kIdPrefetcht1,                       //!< Instruction 'prefetcht1' {MMX2}.
    kIdPrefetcht2,                       //!< Instruction 'prefetcht2' {MMX2}.
    kIdPrefetchw,                        //!< Instruction 'prefetchw' {PREFETCHW}.
    kIdPrefetchwt1,                      //!< Instruction 'prefetchwt1' {PREFETCHWT1}.
    kIdPsadbw,                           //!< Instruction 'psadbw' {MMX2|SSE2}.
    kIdPshufb,                           //!< Instruction 'pshufb' {SSSE3}.
    kIdPshufd,                           //!< Instruction 'pshufd' {SSE2}.
    kIdPshufhw,                          //!< Instruction 'pshufhw' {SSE2}.
    kIdPshuflw,                          //!< Instruction 'pshuflw' {SSE2}.
    kIdPshufw,                           //!< Instruction 'pshufw' {MMX2}.
    kIdPsignb,                           //!< Instruction 'psignb' {SSSE3}.
    kIdPsignd,                           //!< Instruction 'psignd' {SSSE3}.
    kIdPsignw,                           //!< Instruction 'psignw' {SSSE3}.
    kIdPslld,                            //!< Instruction 'pslld' {MMX|SSE2}.
    kIdPslldq,                           //!< Instruction 'pslldq' {SSE2}.
    kIdPsllq,                            //!< Instruction 'psllq' {MMX|SSE2}.
    kIdPsllw,                            //!< Instruction 'psllw' {MMX|SSE2}.
    kIdPsmash,                           //!< Instruction 'psmash' {SNP} (X64).
    kIdPsrad,                            //!< Instruction 'psrad' {MMX|SSE2}.
    kIdPsraw,                            //!< Instruction 'psraw' {MMX|SSE2}.
    kIdPsrld,                            //!< Instruction 'psrld' {MMX|SSE2}.
    kIdPsrldq,                           //!< Instruction 'psrldq' {SSE2}.
    kIdPsrlq,                            //!< Instruction 'psrlq' {MMX|SSE2}.
    kIdPsrlw,                            //!< Instruction 'psrlw' {MMX|SSE2}.
    kIdPsubb,                            //!< Instruction 'psubb' {MMX|SSE2}.
    kIdPsubd,                            //!< Instruction 'psubd' {MMX|SSE2}.
    kIdPsubq,                            //!< Instruction 'psubq' {SSE2}.
    kIdPsubsb,                           //!< Instruction 'psubsb' {MMX|SSE2}.
    kIdPsubsw,                           //!< Instruction 'psubsw' {MMX|SSE2}.
    kIdPsubusb,                          //!< Instruction 'psubusb' {MMX|SSE2}.
    kIdPsubusw,                          //!< Instruction 'psubusw' {MMX|SSE2}.
    kIdPsubw,                            //!< Instruction 'psubw' {MMX|SSE2}.
    kIdPswapd,                           //!< Instruction 'pswapd' {3DNOW2}.
    kIdPtest,                            //!< Instruction 'ptest' {SSE4_1}.
    kIdPtwrite,                          //!< Instruction 'ptwrite' {PTWRITE}.
    kIdPunpckhbw,                        //!< Instruction 'punpckhbw' {MMX|SSE2}.
    kIdPunpckhdq,                        //!< Instruction 'punpckhdq' {MMX|SSE2}.
    kIdPunpckhqdq,                       //!< Instruction 'punpckhqdq' {SSE2}.
    kIdPunpckhwd,                        //!< Instruction 'punpckhwd' {MMX|SSE2}.
    kIdPunpcklbw,                        //!< Instruction 'punpcklbw' {MMX|SSE2}.
    kIdPunpckldq,                        //!< Instruction 'punpckldq' {MMX|SSE2}.
    kIdPunpcklqdq,                       //!< Instruction 'punpcklqdq' {SSE2}.
    kIdPunpcklwd,                        //!< Instruction 'punpcklwd' {MMX|SSE2}.
    kIdPush,                             //!< Instruction 'push'.
    kIdPusha,                            //!< Instruction 'pusha' (X86).
    kIdPushad,                           //!< Instruction 'pushad' (X86).
    kIdPushf,                            //!< Instruction 'pushf'.
    kIdPushfd,                           //!< Instruction 'pushfd' (X86).
    kIdPushfq,                           //!< Instruction 'pushfq' (X64).
    kIdPvalidate,                        //!< Instruction 'pvalidate' {SNP}.
    kIdPxor,                             //!< Instruction 'pxor' {MMX|SSE2}.
    kIdRcl,                              //!< Instruction 'rcl'.
    kIdRcpps,                            //!< Instruction 'rcpps' {SSE}.
    kIdRcpss,                            //!< Instruction 'rcpss' {SSE}.
    kIdRcr,                              //!< Instruction 'rcr'.
    kIdRdfsbase,                         //!< Instruction 'rdfsbase' {FSGSBASE} (X64).
    kIdRdgsbase,                         //!< Instruction 'rdgsbase' {FSGSBASE} (X64).
    kIdRdmsr,                            //!< Instruction 'rdmsr' {MSR}.
    kIdRdpid,                            //!< Instruction 'rdpid' {RDPID}.
    kIdRdpkru,                           //!< Instruction 'rdpkru' {OSPKE}.
    kIdRdpmc,                            //!< Instruction 'rdpmc'.
    kIdRdpru,                            //!< Instruction 'rdpru' {RDPRU}.
    kIdRdrand,                           //!< Instruction 'rdrand' {RDRAND}.
    kIdRdseed,                           //!< Instruction 'rdseed' {RDSEED}.
    kIdRdsspd,                           //!< Instruction 'rdsspd' {CET_SS}.
    kIdRdsspq,                           //!< Instruction 'rdsspq' {CET_SS} (X64).
    kIdRdtsc,                            //!< Instruction 'rdtsc' {RDTSC}.
    kIdRdtscp,                           //!< Instruction 'rdtscp' {RDTSCP}.
    kIdRet,                              //!< Instruction 'ret'.
    kIdRetf,                             //!< Instruction 'retf'.
    kIdRmpadjust,                        //!< Instruction 'rmpadjust' {SNP} (X64).
    kIdRmpupdate,                        //!< Instruction 'rmpupdate' {SNP} (X64).
    kIdRol,                              //!< Instruction 'rol'.
    kIdRor,                              //!< Instruction 'ror'.
    kIdRorx,                             //!< Instruction 'rorx' {BMI2}.
    kIdRoundpd,                          //!< Instruction 'roundpd' {SSE4_1}.
    kIdRoundps,                          //!< Instruction 'roundps' {SSE4_1}.
    kIdRoundsd,                          //!< Instruction 'roundsd' {SSE4_1}.
    kIdRoundss,                          //!< Instruction 'roundss' {SSE4_1}.
    kIdRsm,                              //!< Instruction 'rsm' (X86).
    kIdRsqrtps,                          //!< Instruction 'rsqrtps' {SSE}.
    kIdRsqrtss,                          //!< Instruction 'rsqrtss' {SSE}.
    kIdRstorssp,                         //!< Instruction 'rstorssp' {CET_SS}.
    kIdSahf,                             //!< Instruction 'sahf' {LAHFSAHF}.
    kIdSal,                              //!< Instruction 'sal'.
    kIdSar,                              //!< Instruction 'sar'.
    kIdSarx,                             //!< Instruction 'sarx' {BMI2}.
    kIdSaveprevssp,                      //!< Instruction 'saveprevssp' {CET_SS}.
    kIdSbb,                              //!< Instruction 'sbb'.
    kIdScas,                             //!< Instruction 'scas'.
    kIdSenduipi,                         //!< Instruction 'senduipi' {UINTR} (X64).
    kIdSerialize,                        //!< Instruction 'serialize' {SERIALIZE}.
    kIdSeta,                             //!< Instruction 'seta'.
    kIdSetae,                            //!< Instruction 'setae'.
    kIdSetb,                             //!< Instruction 'setb'.
    kIdSetbe,                            //!< Instruction 'setbe'.
    kIdSetc,                             //!< Instruction 'setc'.
    kIdSete,                             //!< Instruction 'sete'.
    kIdSetg,                             //!< Instruction 'setg'.
    kIdSetge,                            //!< Instruction 'setge'.
    kIdSetl,                             //!< Instruction 'setl'.
    kIdSetle,                            //!< Instruction 'setle'.
    kIdSetna,                            //!< Instruction 'setna'.
    kIdSetnae,                           //!< Instruction 'setnae'.
    kIdSetnb,                            //!< Instruction 'setnb'.
    kIdSetnbe,                           //!< Instruction 'setnbe'.
    kIdSetnc,                            //!< Instruction 'setnc'.
    kIdSetne,                            //!< Instruction 'setne'.
    kIdSetng,                            //!< Instruction 'setng'.
    kIdSetnge,                           //!< Instruction 'setnge'.
    kIdSetnl,                            //!< Instruction 'setnl'.
    kIdSetnle,                           //!< Instruction 'setnle'.
    kIdSetno,                            //!< Instruction 'setno'.
    kIdSetnp,                            //!< Instruction 'setnp'.
    kIdSetns,                            //!< Instruction 'setns'.
    kIdSetnz,                            //!< Instruction 'setnz'.
    kIdSeto,                             //!< Instruction 'seto'.
    kIdSetp,                             //!< Instruction 'setp'.
    kIdSetpe,                            //!< Instruction 'setpe'.
    kIdSetpo,                            //!< Instruction 'setpo'.
    kIdSets,                             //!< Instruction 'sets'.
    kIdSetssbsy,                         //!< Instruction 'setssbsy' {CET_SS}.
    kIdSetz,                             //!< Instruction 'setz'.
    kIdSfence,                           //!< Instruction 'sfence' {MMX2}.
    kIdSgdt,                             //!< Instruction 'sgdt'.
    kIdSha1msg1,                         //!< Instruction 'sha1msg1' {SHA}.
    kIdSha1msg2,                         //!< Instruction 'sha1msg2' {SHA}.
    kIdSha1nexte,                        //!< Instruction 'sha1nexte' {SHA}.
    kIdSha1rnds4,                        //!< Instruction 'sha1rnds4' {SHA}.
    kIdSha256msg1,                       //!< Instruction 'sha256msg1' {SHA}.
    kIdSha256msg2,                       //!< Instruction 'sha256msg2' {SHA}.
    kIdSha256rnds2,                      //!< Instruction 'sha256rnds2' {SHA}.
    kIdShl,                              //!< Instruction 'shl'.
    kIdShld,                             //!< Instruction 'shld'.
    kIdShlx,                             //!< Instruction 'shlx' {BMI2}.
    kIdShr,                              //!< Instruction 'shr'.
    kIdShrd,                             //!< Instruction 'shrd'.
    kIdShrx,                             //!< Instruction 'shrx' {BMI2}.
    kIdShufpd,                           //!< Instruction 'shufpd' {SSE2}.
    kIdShufps,                           //!< Instruction 'shufps' {SSE}.
    kIdSidt,                             //!< Instruction 'sidt'.
    kIdSkinit,                           //!< Instruction 'skinit' {SKINIT}.
    kIdSldt,                             //!< Instruction 'sldt'.
    kIdSlwpcb,                           //!< Instruction 'slwpcb' {LWP}.
    kIdSmsw,                             //!< Instruction 'smsw'.
    kIdSqrtpd,                           //!< Instruction 'sqrtpd' {SSE2}.
    kIdSqrtps,                           //!< Instruction 'sqrtps' {SSE}.
    kIdSqrtsd,                           //!< Instruction 'sqrtsd' {SSE2}.
    kIdSqrtss,                           //!< Instruction 'sqrtss' {SSE}.
    kIdStac,                             //!< Instruction 'stac' {SMAP}.
    kIdStc,                              //!< Instruction 'stc'.
    kIdStd,                              //!< Instruction 'std'.
    kIdStgi,                             //!< Instruction 'stgi' {SKINIT}.
    kIdSti,                              //!< Instruction 'sti'.
    kIdStmxcsr,                          //!< Instruction 'stmxcsr' {SSE}.
    kIdStos,                             //!< Instruction 'stos'.
    kIdStr,                              //!< Instruction 'str'.
    kIdSttilecfg,                        //!< Instruction 'sttilecfg' {AMX_TILE} (X64).
    kIdStui,                             //!< Instruction 'stui' {UINTR} (X64).
    kIdSub,                              //!< Instruction 'sub'.
    kIdSubpd,                            //!< Instruction 'subpd' {SSE2}.
    kIdSubps,                            //!< Instruction 'subps' {SSE}.
    kIdSubsd,                            //!< Instruction 'subsd' {SSE2}.
    kIdSubss,                            //!< Instruction 'subss' {SSE}.
    kIdSwapgs,                           //!< Instruction 'swapgs' (X64).
    kIdSyscall,                          //!< Instruction 'syscall' (X64).
    kIdSysenter,                         //!< Instruction 'sysenter'.
    kIdSysexit,                          //!< Instruction 'sysexit'.
    kIdSysexitq,                         //!< Instruction 'sysexitq'.
    kIdSysret,                           //!< Instruction 'sysret' (X64).
    kIdSysretq,                          //!< Instruction 'sysretq' (X64).
    kIdT1mskc,                           //!< Instruction 't1mskc' {TBM}.
    kIdTdpbf16ps,                        //!< Instruction 'tdpbf16ps' {AMX_BF16} (X64).
    kIdTdpbssd,                          //!< Instruction 'tdpbssd' {AMX_INT8} (X64).
    kIdTdpbsud,                          //!< Instruction 'tdpbsud' {AMX_INT8} (X64).
    kIdTdpbusd,                          //!< Instruction 'tdpbusd' {AMX_INT8} (X64).
    kIdTdpbuud,                          //!< Instruction 'tdpbuud' {AMX_INT8} (X64).
    kIdTest,                             //!< Instruction 'test'.
    kIdTestui,                           //!< Instruction 'testui' {UINTR} (X64).
    kIdTileloadd,                        //!< Instruction 'tileloadd' {AMX_TILE} (X64).
    kIdTileloaddt1,                      //!< Instruction 'tileloaddt1' {AMX_TILE} (X64).
    kIdTilerelease,                      //!< Instruction 'tilerelease' {AMX_TILE} (X64).
    kIdTilestored,                       //!< Instruction 'tilestored' {AMX_TILE} (X64).
    kIdTilezero,                         //!< Instruction 'tilezero' {AMX_TILE} (X64).
    kIdTpause,                           //!< Instruction 'tpause' {WAITPKG}.
    kIdTzcnt,                            //!< Instruction 'tzcnt' {BMI}.
    kIdTzmsk,                            //!< Instruction 'tzmsk' {TBM}.
    kIdUcomisd,                          //!< Instruction 'ucomisd' {SSE2}.
    kIdUcomiss,                          //!< Instruction 'ucomiss' {SSE}.
    kIdUd0,                              //!< Instruction 'ud0'.
    kIdUd1,                              //!< Instruction 'ud1'.
    kIdUd2,                              //!< Instruction 'ud2'.
    kIdUiret,                            //!< Instruction 'uiret' {UINTR} (X64).
    kIdUmonitor,                         //!< Instruction 'umonitor' {WAITPKG}.
    kIdUmwait,                           //!< Instruction 'umwait' {WAITPKG}.
    kIdUnpckhpd,                         //!< Instruction 'unpckhpd' {SSE2}.
    kIdUnpckhps,                         //!< Instruction 'unpckhps' {SSE}.
    kIdUnpcklpd,                         //!< Instruction 'unpcklpd' {SSE2}.
    kIdUnpcklps,                         //!< Instruction 'unpcklps' {SSE}.
    kIdV4fmaddps,                        //!< Instruction 'v4fmaddps' {AVX512_4FMAPS}.
    kIdV4fmaddss,                        //!< Instruction 'v4fmaddss' {AVX512_4FMAPS}.
    kIdV4fnmaddps,                       //!< Instruction 'v4fnmaddps' {AVX512_4FMAPS}.
    kIdV4fnmaddss,                       //!< Instruction 'v4fnmaddss' {AVX512_4FMAPS}.
    kIdVaddpd,                           //!< Instruction 'vaddpd' {AVX|AVX512_F+VL}.
    kIdVaddps,                           //!< Instruction 'vaddps' {AVX|AVX512_F+VL}.
    kIdVaddsd,                           //!< Instruction 'vaddsd' {AVX|AVX512_F}.
    kIdVaddss,                           //!< Instruction 'vaddss' {AVX|AVX512_F}.
    kIdVaddsubpd,                        //!< Instruction 'vaddsubpd' {AVX}.
    kIdVaddsubps,                        //!< Instruction 'vaddsubps' {AVX}.
    kIdVaesdec,                          //!< Instruction 'vaesdec' {AVX|AVX512_F+VL & AESNI|VAES}.
    kIdVaesdeclast,                      //!< Instruction 'vaesdeclast' {AVX|AVX512_F+VL & AESNI|VAES}.
    kIdVaesenc,                          //!< Instruction 'vaesenc' {AVX|AVX512_F+VL & AESNI|VAES}.
    kIdVaesenclast,                      //!< Instruction 'vaesenclast' {AVX|AVX512_F+VL & AESNI|VAES}.
    kIdVaesimc,                          //!< Instruction 'vaesimc' {AVX & AESNI}.
    kIdVaeskeygenassist,                 //!< Instruction 'vaeskeygenassist' {AVX & AESNI}.
    kIdValignd,                          //!< Instruction 'valignd' {AVX512_F+VL}.
    kIdValignq,                          //!< Instruction 'valignq' {AVX512_F+VL}.
    kIdVandnpd,                          //!< Instruction 'vandnpd' {AVX|AVX512_DQ+VL}.
    kIdVandnps,                          //!< Instruction 'vandnps' {AVX|AVX512_DQ+VL}.
    kIdVandpd,                           //!< Instruction 'vandpd' {AVX|AVX512_DQ+VL}.
    kIdVandps,                           //!< Instruction 'vandps' {AVX|AVX512_DQ+VL}.
    kIdVblendmpd,                        //!< Instruction 'vblendmpd' {AVX512_F+VL}.
    kIdVblendmps,                        //!< Instruction 'vblendmps' {AVX512_F+VL}.
    kIdVblendpd,                         //!< Instruction 'vblendpd' {AVX}.
    kIdVblendps,                         //!< Instruction 'vblendps' {AVX}.
    kIdVblendvpd,                        //!< Instruction 'vblendvpd' {AVX}.
    kIdVblendvps,                        //!< Instruction 'vblendvps' {AVX}.
    kIdVbroadcastf128,                   //!< Instruction 'vbroadcastf128' {AVX}.
    kIdVbroadcastf32x2,                  //!< Instruction 'vbroadcastf32x2' {AVX512_DQ+VL}.
    kIdVbroadcastf32x4,                  //!< Instruction 'vbroadcastf32x4' {AVX512_F}.
    kIdVbroadcastf32x8,                  //!< Instruction 'vbroadcastf32x8' {AVX512_DQ}.
    kIdVbroadcastf64x2,                  //!< Instruction 'vbroadcastf64x2' {AVX512_DQ+VL}.
    kIdVbroadcastf64x4,                  //!< Instruction 'vbroadcastf64x4' {AVX512_F}.
    kIdVbroadcasti128,                   //!< Instruction 'vbroadcasti128' {AVX2}.
    kIdVbroadcasti32x2,                  //!< Instruction 'vbroadcasti32x2' {AVX512_DQ+VL}.
    kIdVbroadcasti32x4,                  //!< Instruction 'vbroadcasti32x4' {AVX512_F+VL}.
    kIdVbroadcasti32x8,                  //!< Instruction 'vbroadcasti32x8' {AVX512_DQ}.
    kIdVbroadcasti64x2,                  //!< Instruction 'vbroadcasti64x2' {AVX512_DQ+VL}.
    kIdVbroadcasti64x4,                  //!< Instruction 'vbroadcasti64x4' {AVX512_F}.
    kIdVbroadcastsd,                     //!< Instruction 'vbroadcastsd' {AVX|AVX2|AVX512_F+VL}.
    kIdVbroadcastss,                     //!< Instruction 'vbroadcastss' {AVX|AVX2|AVX512_F+VL}.
    kIdVcmppd,                           //!< Instruction 'vcmppd' {AVX|AVX512_F+VL}.
    kIdVcmpps,                           //!< Instruction 'vcmpps' {AVX|AVX512_F+VL}.
    kIdVcmpsd,                           //!< Instruction 'vcmpsd' {AVX|AVX512_F}.
    kIdVcmpss,                           //!< Instruction 'vcmpss' {AVX|AVX512_F}.
    kIdVcomisd,                          //!< Instruction 'vcomisd' {AVX|AVX512_F}.
    kIdVcomiss,                          //!< Instruction 'vcomiss' {AVX|AVX512_F}.
    kIdVcompresspd,                      //!< Instruction 'vcompresspd' {AVX512_F+VL}.
    kIdVcompressps,                      //!< Instruction 'vcompressps' {AVX512_F+VL}.
    kIdVcvtdq2pd,                        //!< Instruction 'vcvtdq2pd' {AVX|AVX512_F+VL}.
    kIdVcvtdq2ps,                        //!< Instruction 'vcvtdq2ps' {AVX|AVX512_F+VL}.
    kIdVcvtne2ps2bf16,                   //!< Instruction 'vcvtne2ps2bf16' {AVX512_BF16+VL}.
    kIdVcvtneps2bf16,                    //!< Instruction 'vcvtneps2bf16' {AVX512_BF16+VL}.
    kIdVcvtpd2dq,                        //!< Instruction 'vcvtpd2dq' {AVX|AVX512_F+VL}.
    kIdVcvtpd2ps,                        //!< Instruction 'vcvtpd2ps' {AVX|AVX512_F+VL}.
    kIdVcvtpd2qq,                        //!< Instruction 'vcvtpd2qq' {AVX512_DQ+VL}.
    kIdVcvtpd2udq,                       //!< Instruction 'vcvtpd2udq' {AVX512_F+VL}.
    kIdVcvtpd2uqq,                       //!< Instruction 'vcvtpd2uqq' {AVX512_DQ+VL}.
    kIdVcvtph2ps,                        //!< Instruction 'vcvtph2ps' {AVX512_F+VL & F16C}.
    kIdVcvtps2dq,                        //!< Instruction 'vcvtps2dq' {AVX|AVX512_F+VL}.
    kIdVcvtps2pd,                        //!< Instruction 'vcvtps2pd' {AVX|AVX512_F+VL}.
    kIdVcvtps2ph,                        //!< Instruction 'vcvtps2ph' {AVX512_F+VL & F16C}.
    kIdVcvtps2qq,                        //!< Instruction 'vcvtps2qq' {AVX512_DQ+VL}.
    kIdVcvtps2udq,                       //!< Instruction 'vcvtps2udq' {AVX512_F+VL}.
    kIdVcvtps2uqq,                       //!< Instruction 'vcvtps2uqq' {AVX512_DQ+VL}.
    kIdVcvtqq2pd,                        //!< Instruction 'vcvtqq2pd' {AVX512_DQ+VL}.
    kIdVcvtqq2ps,                        //!< Instruction 'vcvtqq2ps' {AVX512_DQ+VL}.
    kIdVcvtsd2si,                        //!< Instruction 'vcvtsd2si' {AVX|AVX512_F}.
    kIdVcvtsd2ss,                        //!< Instruction 'vcvtsd2ss' {AVX|AVX512_F}.
    kIdVcvtsd2usi,                       //!< Instruction 'vcvtsd2usi' {AVX512_F}.
    kIdVcvtsi2sd,                        //!< Instruction 'vcvtsi2sd' {AVX|AVX512_F}.
    kIdVcvtsi2ss,                        //!< Instruction 'vcvtsi2ss' {AVX|AVX512_F}.
    kIdVcvtss2sd,                        //!< Instruction 'vcvtss2sd' {AVX|AVX512_F}.
    kIdVcvtss2si,                        //!< Instruction 'vcvtss2si' {AVX|AVX512_F}.
    kIdVcvtss2usi,                       //!< Instruction 'vcvtss2usi' {AVX512_F}.
    kIdVcvttpd2dq,                       //!< Instruction 'vcvttpd2dq' {AVX|AVX512_F+VL}.
    kIdVcvttpd2qq,                       //!< Instruction 'vcvttpd2qq' {AVX512_F+VL}.
    kIdVcvttpd2udq,                      //!< Instruction 'vcvttpd2udq' {AVX512_F+VL}.
    kIdVcvttpd2uqq,                      //!< Instruction 'vcvttpd2uqq' {AVX512_DQ+VL}.
    kIdVcvttps2dq,                       //!< Instruction 'vcvttps2dq' {AVX|AVX512_F+VL}.
    kIdVcvttps2qq,                       //!< Instruction 'vcvttps2qq' {AVX512_DQ+VL}.
    kIdVcvttps2udq,                      //!< Instruction 'vcvttps2udq' {AVX512_F+VL}.
    kIdVcvttps2uqq,                      //!< Instruction 'vcvttps2uqq' {AVX512_DQ+VL}.
    kIdVcvttsd2si,                       //!< Instruction 'vcvttsd2si' {AVX|AVX512_F}.
    kIdVcvttsd2usi,                      //!< Instruction 'vcvttsd2usi' {AVX512_F}.
    kIdVcvttss2si,                       //!< Instruction 'vcvttss2si' {AVX|AVX512_F}.
    kIdVcvttss2usi,                      //!< Instruction 'vcvttss2usi' {AVX512_F}.
    kIdVcvtudq2pd,                       //!< Instruction 'vcvtudq2pd' {AVX512_F+VL}.
    kIdVcvtudq2ps,                       //!< Instruction 'vcvtudq2ps' {AVX512_F+VL}.
    kIdVcvtuqq2pd,                       //!< Instruction 'vcvtuqq2pd' {AVX512_DQ+VL}.
    kIdVcvtuqq2ps,                       //!< Instruction 'vcvtuqq2ps' {AVX512_DQ+VL}.
    kIdVcvtusi2sd,                       //!< Instruction 'vcvtusi2sd' {AVX512_F}.
    kIdVcvtusi2ss,                       //!< Instruction 'vcvtusi2ss' {AVX512_F}.
    kIdVdbpsadbw,                        //!< Instruction 'vdbpsadbw' {AVX512_BW+VL}.
    kIdVdivpd,                           //!< Instruction 'vdivpd' {AVX|AVX512_F+VL}.
    kIdVdivps,                           //!< Instruction 'vdivps' {AVX|AVX512_F+VL}.
    kIdVdivsd,                           //!< Instruction 'vdivsd' {AVX|AVX512_F}.
    kIdVdivss,                           //!< Instruction 'vdivss' {AVX|AVX512_F}.
    kIdVdpbf16ps,                        //!< Instruction 'vdpbf16ps' {AVX512_BF16+VL}.
    kIdVdppd,                            //!< Instruction 'vdppd' {AVX}.
    kIdVdpps,                            //!< Instruction 'vdpps' {AVX}.
    kIdVerr,                             //!< Instruction 'verr'.
    kIdVerw,                             //!< Instruction 'verw'.
    kIdVexp2pd,                          //!< Instruction 'vexp2pd' {AVX512_ERI}.
    kIdVexp2ps,                          //!< Instruction 'vexp2ps' {AVX512_ERI}.
    kIdVexpandpd,                        //!< Instruction 'vexpandpd' {AVX512_F+VL}.
    kIdVexpandps,                        //!< Instruction 'vexpandps' {AVX512_F+VL}.
    kIdVextractf128,                     //!< Instruction 'vextractf128' {AVX}.
    kIdVextractf32x4,                    //!< Instruction 'vextractf32x4' {AVX512_F+VL}.
    kIdVextractf32x8,                    //!< Instruction 'vextractf32x8' {AVX512_DQ}.
    kIdVextractf64x2,                    //!< Instruction 'vextractf64x2' {AVX512_DQ+VL}.
    kIdVextractf64x4,                    //!< Instruction 'vextractf64x4' {AVX512_F}.
    kIdVextracti128,                     //!< Instruction 'vextracti128' {AVX2}.
    kIdVextracti32x4,                    //!< Instruction 'vextracti32x4' {AVX512_F+VL}.
    kIdVextracti32x8,                    //!< Instruction 'vextracti32x8' {AVX512_DQ}.
    kIdVextracti64x2,                    //!< Instruction 'vextracti64x2' {AVX512_DQ+VL}.
    kIdVextracti64x4,                    //!< Instruction 'vextracti64x4' {AVX512_F}.
    kIdVextractps,                       //!< Instruction 'vextractps' {AVX|AVX512_F}.
    kIdVfixupimmpd,                      //!< Instruction 'vfixupimmpd' {AVX512_F+VL}.
    kIdVfixupimmps,                      //!< Instruction 'vfixupimmps' {AVX512_F+VL}.
    kIdVfixupimmsd,                      //!< Instruction 'vfixupimmsd' {AVX512_F}.
    kIdVfixupimmss,                      //!< Instruction 'vfixupimmss' {AVX512_F}.
    kIdVfmadd132pd,                      //!< Instruction 'vfmadd132pd' {FMA|AVX512_F+VL}.
    kIdVfmadd132ps,                      //!< Instruction 'vfmadd132ps' {FMA|AVX512_F+VL}.
    kIdVfmadd132sd,                      //!< Instruction 'vfmadd132sd' {FMA|AVX512_F}.
    kIdVfmadd132ss,                      //!< Instruction 'vfmadd132ss' {FMA|AVX512_F}.
    kIdVfmadd213pd,                      //!< Instruction 'vfmadd213pd' {FMA|AVX512_F+VL}.
    kIdVfmadd213ps,                      //!< Instruction 'vfmadd213ps' {FMA|AVX512_F+VL}.
    kIdVfmadd213sd,                      //!< Instruction 'vfmadd213sd' {FMA|AVX512_F}.
    kIdVfmadd213ss,                      //!< Instruction 'vfmadd213ss' {FMA|AVX512_F}.
    kIdVfmadd231pd,                      //!< Instruction 'vfmadd231pd' {FMA|AVX512_F+VL}.
    kIdVfmadd231ps,                      //!< Instruction 'vfmadd231ps' {FMA|AVX512_F+VL}.
    kIdVfmadd231sd,                      //!< Instruction 'vfmadd231sd' {FMA|AVX512_F}.
    kIdVfmadd231ss,                      //!< Instruction 'vfmadd231ss' {FMA|AVX512_F}.
    kIdVfmaddpd,                         //!< Instruction 'vfmaddpd' {FMA4}.
    kIdVfmaddps,                         //!< Instruction 'vfmaddps' {FMA4}.
    kIdVfmaddsd,                         //!< Instruction 'vfmaddsd' {FMA4}.
    kIdVfmaddss,                         //!< Instruction 'vfmaddss' {FMA4}.
    kIdVfmaddsub132pd,                   //!< Instruction 'vfmaddsub132pd' {FMA|AVX512_F+VL}.
    kIdVfmaddsub132ps,                   //!< Instruction 'vfmaddsub132ps' {FMA|AVX512_F+VL}.
    kIdVfmaddsub213pd,                   //!< Instruction 'vfmaddsub213pd' {FMA|AVX512_F+VL}.
    kIdVfmaddsub213ps,                   //!< Instruction 'vfmaddsub213ps' {FMA|AVX512_F+VL}.
    kIdVfmaddsub231pd,                   //!< Instruction 'vfmaddsub231pd' {FMA|AVX512_F+VL}.
    kIdVfmaddsub231ps,                   //!< Instruction 'vfmaddsub231ps' {FMA|AVX512_F+VL}.
    kIdVfmaddsubpd,                      //!< Instruction 'vfmaddsubpd' {FMA4}.
    kIdVfmaddsubps,                      //!< Instruction 'vfmaddsubps' {FMA4}.
    kIdVfmsub132pd,                      //!< Instruction 'vfmsub132pd' {FMA|AVX512_F+VL}.
    kIdVfmsub132ps,                      //!< Instruction 'vfmsub132ps' {FMA|AVX512_F+VL}.
    kIdVfmsub132sd,                      //!< Instruction 'vfmsub132sd' {FMA|AVX512_F}.
    kIdVfmsub132ss,                      //!< Instruction 'vfmsub132ss' {FMA|AVX512_F}.
    kIdVfmsub213pd,                      //!< Instruction 'vfmsub213pd' {FMA|AVX512_F+VL}.
    kIdVfmsub213ps,                      //!< Instruction 'vfmsub213ps' {FMA|AVX512_F+VL}.
    kIdVfmsub213sd,                      //!< Instruction 'vfmsub213sd' {FMA|AVX512_F}.
    kIdVfmsub213ss,                      //!< Instruction 'vfmsub213ss' {FMA|AVX512_F}.
    kIdVfmsub231pd,                      //!< Instruction 'vfmsub231pd' {FMA|AVX512_F+VL}.
    kIdVfmsub231ps,                      //!< Instruction 'vfmsub231ps' {FMA|AVX512_F+VL}.
    kIdVfmsub231sd,                      //!< Instruction 'vfmsub231sd' {FMA|AVX512_F}.
    kIdVfmsub231ss,                      //!< Instruction 'vfmsub231ss' {FMA|AVX512_F}.
    kIdVfmsubadd132pd,                   //!< Instruction 'vfmsubadd132pd' {FMA|AVX512_F+VL}.
    kIdVfmsubadd132ps,                   //!< Instruction 'vfmsubadd132ps' {FMA|AVX512_F+VL}.
    kIdVfmsubadd213pd,                   //!< Instruction 'vfmsubadd213pd' {FMA|AVX512_F+VL}.
    kIdVfmsubadd213ps,                   //!< Instruction 'vfmsubadd213ps' {FMA|AVX512_F+VL}.
    kIdVfmsubadd231pd,                   //!< Instruction 'vfmsubadd231pd' {FMA|AVX512_F+VL}.
    kIdVfmsubadd231ps,                   //!< Instruction 'vfmsubadd231ps' {FMA|AVX512_F+VL}.
    kIdVfmsubaddpd,                      //!< Instruction 'vfmsubaddpd' {FMA4}.
    kIdVfmsubaddps,                      //!< Instruction 'vfmsubaddps' {FMA4}.
    kIdVfmsubpd,                         //!< Instruction 'vfmsubpd' {FMA4}.
    kIdVfmsubps,                         //!< Instruction 'vfmsubps' {FMA4}.
    kIdVfmsubsd,                         //!< Instruction 'vfmsubsd' {FMA4}.
    kIdVfmsubss,                         //!< Instruction 'vfmsubss' {FMA4}.
    kIdVfnmadd132pd,                     //!< Instruction 'vfnmadd132pd' {FMA|AVX512_F+VL}.
    kIdVfnmadd132ps,                     //!< Instruction 'vfnmadd132ps' {FMA|AVX512_F+VL}.
    kIdVfnmadd132sd,                     //!< Instruction 'vfnmadd132sd' {FMA|AVX512_F}.
    kIdVfnmadd132ss,                     //!< Instruction 'vfnmadd132ss' {FMA|AVX512_F}.
    kIdVfnmadd213pd,                     //!< Instruction 'vfnmadd213pd' {FMA|AVX512_F+VL}.
    kIdVfnmadd213ps,                     //!< Instruction 'vfnmadd213ps' {FMA|AVX512_F+VL}.
    kIdVfnmadd213sd,                     //!< Instruction 'vfnmadd213sd' {FMA|AVX512_F}.
    kIdVfnmadd213ss,                     //!< Instruction 'vfnmadd213ss' {FMA|AVX512_F}.
    kIdVfnmadd231pd,                     //!< Instruction 'vfnmadd231pd' {FMA|AVX512_F+VL}.
    kIdVfnmadd231ps,                     //!< Instruction 'vfnmadd231ps' {FMA|AVX512_F+VL}.
    kIdVfnmadd231sd,                     //!< Instruction 'vfnmadd231sd' {FMA|AVX512_F}.
    kIdVfnmadd231ss,                     //!< Instruction 'vfnmadd231ss' {FMA|AVX512_F}.
    kIdVfnmaddpd,                        //!< Instruction 'vfnmaddpd' {FMA4}.
    kIdVfnmaddps,                        //!< Instruction 'vfnmaddps' {FMA4}.
    kIdVfnmaddsd,                        //!< Instruction 'vfnmaddsd' {FMA4}.
    kIdVfnmaddss,                        //!< Instruction 'vfnmaddss' {FMA4}.
    kIdVfnmsub132pd,                     //!< Instruction 'vfnmsub132pd' {FMA|AVX512_F+VL}.
    kIdVfnmsub132ps,                     //!< Instruction 'vfnmsub132ps' {FMA|AVX512_F+VL}.
    kIdVfnmsub132sd,                     //!< Instruction 'vfnmsub132sd' {FMA|AVX512_F}.
    kIdVfnmsub132ss,                     //!< Instruction 'vfnmsub132ss' {FMA|AVX512_F}.
    kIdVfnmsub213pd,                     //!< Instruction 'vfnmsub213pd' {FMA|AVX512_F+VL}.
    kIdVfnmsub213ps,                     //!< Instruction 'vfnmsub213ps' {FMA|AVX512_F+VL}.
    kIdVfnmsub213sd,                     //!< Instruction 'vfnmsub213sd' {FMA|AVX512_F}.
    kIdVfnmsub213ss,                     //!< Instruction 'vfnmsub213ss' {FMA|AVX512_F}.
    kIdVfnmsub231pd,                     //!< Instruction 'vfnmsub231pd' {FMA|AVX512_F+VL}.
    kIdVfnmsub231ps,                     //!< Instruction 'vfnmsub231ps' {FMA|AVX512_F+VL}.
    kIdVfnmsub231sd,                     //!< Instruction 'vfnmsub231sd' {FMA|AVX512_F}.
    kIdVfnmsub231ss,                     //!< Instruction 'vfnmsub231ss' {FMA|AVX512_F}.
    kIdVfnmsubpd,                        //!< Instruction 'vfnmsubpd' {FMA4}.
    kIdVfnmsubps,                        //!< Instruction 'vfnmsubps' {FMA4}.
    kIdVfnmsubsd,                        //!< Instruction 'vfnmsubsd' {FMA4}.
    kIdVfnmsubss,                        //!< Instruction 'vfnmsubss' {FMA4}.
    kIdVfpclasspd,                       //!< Instruction 'vfpclasspd' {AVX512_DQ+VL}.
    kIdVfpclassps,                       //!< Instruction 'vfpclassps' {AVX512_DQ+VL}.
    kIdVfpclasssd,                       //!< Instruction 'vfpclasssd' {AVX512_DQ}.
    kIdVfpclassss,                       //!< Instruction 'vfpclassss' {AVX512_DQ}.
    kIdVfrczpd,                          //!< Instruction 'vfrczpd' {XOP}.
    kIdVfrczps,                          //!< Instruction 'vfrczps' {XOP}.
    kIdVfrczsd,                          //!< Instruction 'vfrczsd' {XOP}.
    kIdVfrczss,                          //!< Instruction 'vfrczss' {XOP}.
    kIdVgatherdpd,                       //!< Instruction 'vgatherdpd' {AVX2|AVX512_F+VL}.
    kIdVgatherdps,                       //!< Instruction 'vgatherdps' {AVX2|AVX512_F+VL}.
    kIdVgatherpf0dpd,                    //!< Instruction 'vgatherpf0dpd' {AVX512_PFI}.
    kIdVgatherpf0dps,                    //!< Instruction 'vgatherpf0dps' {AVX512_PFI}.
    kIdVgatherpf0qpd,                    //!< Instruction 'vgatherpf0qpd' {AVX512_PFI}.
    kIdVgatherpf0qps,                    //!< Instruction 'vgatherpf0qps' {AVX512_PFI}.
    kIdVgatherpf1dpd,                    //!< Instruction 'vgatherpf1dpd' {AVX512_PFI}.
    kIdVgatherpf1dps,                    //!< Instruction 'vgatherpf1dps' {AVX512_PFI}.
    kIdVgatherpf1qpd,                    //!< Instruction 'vgatherpf1qpd' {AVX512_PFI}.
    kIdVgatherpf1qps,                    //!< Instruction 'vgatherpf1qps' {AVX512_PFI}.
    kIdVgatherqpd,                       //!< Instruction 'vgatherqpd' {AVX2|AVX512_F+VL}.
    kIdVgatherqps,                       //!< Instruction 'vgatherqps' {AVX2|AVX512_F+VL}.
    kIdVgetexppd,                        //!< Instruction 'vgetexppd' {AVX512_F+VL}.
    kIdVgetexpps,                        //!< Instruction 'vgetexpps' {AVX512_F+VL}.
    kIdVgetexpsd,                        //!< Instruction 'vgetexpsd' {AVX512_F}.
    kIdVgetexpss,                        //!< Instruction 'vgetexpss' {AVX512_F}.
    kIdVgetmantpd,                       //!< Instruction 'vgetmantpd' {AVX512_F+VL}.
    kIdVgetmantps,                       //!< Instruction 'vgetmantps' {AVX512_F+VL}.
    kIdVgetmantsd,                       //!< Instruction 'vgetmantsd' {AVX512_F}.
    kIdVgetmantss,                       //!< Instruction 'vgetmantss' {AVX512_F}.
    kIdVgf2p8affineinvqb,                //!< Instruction 'vgf2p8affineinvqb' {AVX|AVX512_F+VL & GFNI}.
    kIdVgf2p8affineqb,                   //!< Instruction 'vgf2p8affineqb' {AVX|AVX512_F+VL & GFNI}.
    kIdVgf2p8mulb,                       //!< Instruction 'vgf2p8mulb' {AVX|AVX512_F+VL & GFNI}.
    kIdVhaddpd,                          //!< Instruction 'vhaddpd' {AVX}.
    kIdVhaddps,                          //!< Instruction 'vhaddps' {AVX}.
    kIdVhsubpd,                          //!< Instruction 'vhsubpd' {AVX}.
    kIdVhsubps,                          //!< Instruction 'vhsubps' {AVX}.
    kIdVinsertf128,                      //!< Instruction 'vinsertf128' {AVX}.
    kIdVinsertf32x4,                     //!< Instruction 'vinsertf32x4' {AVX512_F+VL}.
    kIdVinsertf32x8,                     //!< Instruction 'vinsertf32x8' {AVX512_DQ}.
    kIdVinsertf64x2,                     //!< Instruction 'vinsertf64x2' {AVX512_DQ+VL}.
    kIdVinsertf64x4,                     //!< Instruction 'vinsertf64x4' {AVX512_F}.
    kIdVinserti128,                      //!< Instruction 'vinserti128' {AVX2}.
    kIdVinserti32x4,                     //!< Instruction 'vinserti32x4' {AVX512_F+VL}.
    kIdVinserti32x8,                     //!< Instruction 'vinserti32x8' {AVX512_DQ}.
    kIdVinserti64x2,                     //!< Instruction 'vinserti64x2' {AVX512_DQ+VL}.
    kIdVinserti64x4,                     //!< Instruction 'vinserti64x4' {AVX512_F}.
    kIdVinsertps,                        //!< Instruction 'vinsertps' {AVX|AVX512_F}.
    kIdVlddqu,                           //!< Instruction 'vlddqu' {AVX}.
    kIdVldmxcsr,                         //!< Instruction 'vldmxcsr' {AVX}.
    kIdVmaskmovdqu,                      //!< Instruction 'vmaskmovdqu' {AVX}.
    kIdVmaskmovpd,                       //!< Instruction 'vmaskmovpd' {AVX}.
    kIdVmaskmovps,                       //!< Instruction 'vmaskmovps' {AVX}.
    kIdVmaxpd,                           //!< Instruction 'vmaxpd' {AVX|AVX512_F+VL}.
    kIdVmaxps,                           //!< Instruction 'vmaxps' {AVX|AVX512_F+VL}.
    kIdVmaxsd,                           //!< Instruction 'vmaxsd' {AVX|AVX512_F+VL}.
    kIdVmaxss,                           //!< Instruction 'vmaxss' {AVX|AVX512_F+VL}.
    kIdVmcall,                           //!< Instruction 'vmcall' {VMX}.
    kIdVmclear,                          //!< Instruction 'vmclear' {VMX}.
    kIdVmfunc,                           //!< Instruction 'vmfunc' {VMX}.
    kIdVminpd,                           //!< Instruction 'vminpd' {AVX|AVX512_F+VL}.
    kIdVminps,                           //!< Instruction 'vminps' {AVX|AVX512_F+VL}.
    kIdVminsd,                           //!< Instruction 'vminsd' {AVX|AVX512_F+VL}.
    kIdVminss,                           //!< Instruction 'vminss' {AVX|AVX512_F+VL}.
    kIdVmlaunch,                         //!< Instruction 'vmlaunch' {VMX}.
    kIdVmload,                           //!< Instruction 'vmload' {SVM}.
    kIdVmmcall,                          //!< Instruction 'vmmcall' {SVM}.
    kIdVmovapd,                          //!< Instruction 'vmovapd' {AVX|AVX512_F+VL}.
    kIdVmovaps,                          //!< Instruction 'vmovaps' {AVX|AVX512_F+VL}.
    kIdVmovd,                            //!< Instruction 'vmovd' {AVX|AVX512_F}.
    kIdVmovddup,                         //!< Instruction 'vmovddup' {AVX|AVX512_F+VL}.
    kIdVmovdqa,                          //!< Instruction 'vmovdqa' {AVX}.
    kIdVmovdqa32,                        //!< Instruction 'vmovdqa32' {AVX512_F+VL}.
    kIdVmovdqa64,                        //!< Instruction 'vmovdqa64' {AVX512_F+VL}.
    kIdVmovdqu,                          //!< Instruction 'vmovdqu' {AVX}.
    kIdVmovdqu16,                        //!< Instruction 'vmovdqu16' {AVX512_BW+VL}.
    kIdVmovdqu32,                        //!< Instruction 'vmovdqu32' {AVX512_F+VL}.
    kIdVmovdqu64,                        //!< Instruction 'vmovdqu64' {AVX512_F+VL}.
    kIdVmovdqu8,                         //!< Instruction 'vmovdqu8' {AVX512_BW+VL}.
    kIdVmovhlps,                         //!< Instruction 'vmovhlps' {AVX|AVX512_F}.
    kIdVmovhpd,                          //!< Instruction 'vmovhpd' {AVX|AVX512_F}.
    kIdVmovhps,                          //!< Instruction 'vmovhps' {AVX|AVX512_F}.
    kIdVmovlhps,                         //!< Instruction 'vmovlhps' {AVX|AVX512_F}.
    kIdVmovlpd,                          //!< Instruction 'vmovlpd' {AVX|AVX512_F}.
    kIdVmovlps,                          //!< Instruction 'vmovlps' {AVX|AVX512_F}.
    kIdVmovmskpd,                        //!< Instruction 'vmovmskpd' {AVX}.
    kIdVmovmskps,                        //!< Instruction 'vmovmskps' {AVX}.
    kIdVmovntdq,                         //!< Instruction 'vmovntdq' {AVX|AVX512_F+VL}.
    kIdVmovntdqa,                        //!< Instruction 'vmovntdqa' {AVX|AVX2|AVX512_F+VL}.
    kIdVmovntpd,                         //!< Instruction 'vmovntpd' {AVX|AVX512_F+VL}.
    kIdVmovntps,                         //!< Instruction 'vmovntps' {AVX|AVX512_F+VL}.
    kIdVmovq,                            //!< Instruction 'vmovq' {AVX|AVX512_F}.
    kIdVmovsd,                           //!< Instruction 'vmovsd' {AVX|AVX512_F}.
    kIdVmovshdup,                        //!< Instruction 'vmovshdup' {AVX|AVX512_F+VL}.
    kIdVmovsldup,                        //!< Instruction 'vmovsldup' {AVX|AVX512_F+VL}.
    kIdVmovss,                           //!< Instruction 'vmovss' {AVX|AVX512_F}.
    kIdVmovupd,                          //!< Instruction 'vmovupd' {AVX|AVX512_F+VL}.
    kIdVmovups,                          //!< Instruction 'vmovups' {AVX|AVX512_F+VL}.
    kIdVmpsadbw,                         //!< Instruction 'vmpsadbw' {AVX|AVX2}.
    kIdVmptrld,                          //!< Instruction 'vmptrld' {VMX}.
    kIdVmptrst,                          //!< Instruction 'vmptrst' {VMX}.
    kIdVmread,                           //!< Instruction 'vmread' {VMX}.
    kIdVmresume,                         //!< Instruction 'vmresume' {VMX}.
    kIdVmrun,                            //!< Instruction 'vmrun' {SVM}.
    kIdVmsave,                           //!< Instruction 'vmsave' {SVM}.
    kIdVmulpd,                           //!< Instruction 'vmulpd' {AVX|AVX512_F+VL}.
    kIdVmulps,                           //!< Instruction 'vmulps' {AVX|AVX512_F+VL}.
    kIdVmulsd,                           //!< Instruction 'vmulsd' {AVX|AVX512_F}.
    kIdVmulss,                           //!< Instruction 'vmulss' {AVX|AVX512_F}.
    kIdVmwrite,                          //!< Instruction 'vmwrite' {VMX}.
    kIdVmxon,                            //!< Instruction 'vmxon' {VMX}.
    kIdVorpd,                            //!< Instruction 'vorpd' {AVX|AVX512_DQ+VL}.
    kIdVorps,                            //!< Instruction 'vorps' {AVX|AVX512_DQ+VL}.
    kIdVp2intersectd,                    //!< Instruction 'vp2intersectd' {AVX512_VP2INTERSECT}.
    kIdVp2intersectq,                    //!< Instruction 'vp2intersectq' {AVX512_VP2INTERSECT}.
    kIdVp4dpwssd,                        //!< Instruction 'vp4dpwssd' {AVX512_4VNNIW}.
    kIdVp4dpwssds,                       //!< Instruction 'vp4dpwssds' {AVX512_4VNNIW}.
    kIdVpabsb,                           //!< Instruction 'vpabsb' {AVX|AVX2|AVX512_BW+VL}.
    kIdVpabsd,                           //!< Instruction 'vpabsd' {AVX|AVX2|AVX512_F+VL}.
    kIdVpabsq,                           //!< Instruction 'vpabsq' {AVX512_F+VL}.
    kIdVpabsw,                           //!< Instruction 'vpabsw' {AVX|AVX2|AVX512_BW+VL}.
    kIdVpackssdw,                        //!< Instruction 'vpackssdw' {AVX|AVX2|AVX512_BW+VL}.
    kIdVpacksswb,                        //!< Instruction 'vpacksswb' {AVX|AVX2|AVX512_BW+VL}.
    kIdVpackusdw,                        //!< Instruction 'vpackusdw' {AVX|AVX2|AVX512_BW+VL}.
    kIdVpackuswb,                        //!< Instruction 'vpackuswb' {AVX|AVX2|AVX512_BW+VL}.
    kIdVpaddb,                           //!< Instruction 'vpaddb' {AVX|AVX2|AVX512_BW+VL}.
    kIdVpaddd,                           //!< Instruction 'vpaddd' {AVX|AVX2|AVX512_F+VL}.
    kIdVpaddq,                           //!< Instruction 'vpaddq' {AVX|AVX2|AVX512_F+VL}.
    kIdVpaddsb,                          //!< Instruction 'vpaddsb' {AVX|AVX2|AVX512_BW+VL}.
    kIdVpaddsw,                          //!< Instruction 'vpaddsw' {AVX|AVX2|AVX512_BW+VL}.
    kIdVpaddusb,                         //!< Instruction 'vpaddusb' {AVX|AVX2|AVX512_BW+VL}.
    kIdVpaddusw,                         //!< Instruction 'vpaddusw' {AVX|AVX2|AVX512_BW+VL}.
    kIdVpaddw,                           //!< Instruction 'vpaddw' {AVX|AVX2|AVX512_BW+VL}.
    kIdVpalignr,                         //!< Instruction 'vpalignr' {AVX|AVX2|AVX512_BW+VL}.
    kIdVpand,                            //!< Instruction 'vpand' {AVX|AVX2}.
    kIdVpandd,                           //!< Instruction 'vpandd' {AVX512_F+VL}.
    kIdVpandn,                           //!< Instruction 'vpandn' {AVX|AVX2}.
    kIdVpandnd,                          //!< Instruction 'vpandnd' {AVX512_F+VL}.
    kIdVpandnq,                          //!< Instruction 'vpandnq' {AVX512_F+VL}.
    kIdVpandq,                           //!< Instruction 'vpandq' {AVX512_F+VL}.
    kIdVpavgb,                           //!< Instruction 'vpavgb' {AVX|AVX2|AVX512_BW+VL}.
    kIdVpavgw,                           //!< Instruction 'vpavgw' {AVX|AVX2|AVX512_BW+VL}.
    kIdVpblendd,                         //!< Instruction 'vpblendd' {AVX2}.
    kIdVpblendmb,                        //!< Instruction 'vpblendmb' {AVX512_BW+VL}.
    kIdVpblendmd,                        //!< Instruction 'vpblendmd' {AVX512_F+VL}.
    kIdVpblendmq,                        //!< Instruction 'vpblendmq' {AVX512_F+VL}.
    kIdVpblendmw,                        //!< Instruction 'vpblendmw' {AVX512_BW+VL}.
    kIdVpblendvb,                        //!< Instruction 'vpblendvb' {AVX|AVX2}.
    kIdVpblendw,                         //!< Instruction 'vpblendw' {AVX|AVX2}.
    kIdVpbroadcastb,                     //!< Instruction 'vpbroadcastb' {AVX2|AVX512_BW+VL}.
    kIdVpbroadcastd,                     //!< Instruction 'vpbroadcastd' {AVX2|AVX512_F+VL}.
    kIdVpbroadcastmb2q,                  //!< Instruction 'vpbroadcastmb2q' {AVX512_CDI+VL}.
    kIdVpbroadcastmw2d,                  //!< Instruction 'vpbroadcastmw2d' {AVX512_CDI+VL}.
    kIdVpbroadcastq,                     //!< Instruction 'vpbroadcastq' {AVX2|AVX512_F+VL}.
    kIdVpbroadcastw,                     //!< Instruction 'vpbroadcastw' {AVX2|AVX512_BW+VL}.
    kIdVpclmulqdq,                       //!< Instruction 'vpclmulqdq' {AVX|AVX512_F+VL & PCLMULQDQ|VPCLMULQDQ}.
    kIdVpcmov,                           //!< Instruction 'vpcmov' {XOP}.
    kIdVpcmpb,                           //!< Instruction 'vpcmpb' {AVX512_BW+VL}.
    kIdVpcmpd,                           //!< Instruction 'vpcmpd' {AVX512_F+VL}.
    kIdVpcmpeqb,                         //!< Instruction 'vpcmpeqb' {AVX|AVX2|AVX512_BW+VL}.
    kIdVpcmpeqd,                         //!< Instruction 'vpcmpeqd' {AVX|AVX2|AVX512_F+VL}.
    kIdVpcmpeqq,                         //!< Instruction 'vpcmpeqq' {AVX|AVX2|AVX512_F+VL}.
    kIdVpcmpeqw,                         //!< Instruction 'vpcmpeqw' {AVX|AVX2|AVX512_BW+VL}.
    kIdVpcmpestri,                       //!< Instruction 'vpcmpestri' {AVX}.
    kIdVpcmpestrm,                       //!< Instruction 'vpcmpestrm' {AVX}.
    kIdVpcmpgtb,                         //!< Instruction 'vpcmpgtb' {AVX|AVX2|AVX512_BW+VL}.
    kIdVpcmpgtd,                         //!< Instruction 'vpcmpgtd' {AVX|AVX2|AVX512_F+VL}.
    kIdVpcmpgtq,                         //!< Instruction 'vpcmpgtq' {AVX|AVX2|AVX512_F+VL}.
    kIdVpcmpgtw,                         //!< Instruction 'vpcmpgtw' {AVX|AVX2|AVX512_BW+VL}.
    kIdVpcmpistri,                       //!< Instruction 'vpcmpistri' {AVX}.
    kIdVpcmpistrm,                       //!< Instruction 'vpcmpistrm' {AVX}.
    kIdVpcmpq,                           //!< Instruction 'vpcmpq' {AVX512_F+VL}.
    kIdVpcmpub,                          //!< Instruction 'vpcmpub' {AVX512_BW+VL}.
    kIdVpcmpud,                          //!< Instruction 'vpcmpud' {AVX512_F+VL}.
    kIdVpcmpuq,                          //!< Instruction 'vpcmpuq' {AVX512_F+VL}.
    kIdVpcmpuw,                          //!< Instruction 'vpcmpuw' {AVX512_BW+VL}.
    kIdVpcmpw,                           //!< Instruction 'vpcmpw' {AVX512_BW+VL}.
    kIdVpcomb,                           //!< Instruction 'vpcomb' {XOP}.
    kIdVpcomd,                           //!< Instruction 'vpcomd' {XOP}.
    kIdVpcompressb,                      //!< Instruction 'vpcompressb' {AVX512_VBMI2+VL}.
    kIdVpcompressd,                      //!< Instruction 'vpcompressd' {AVX512_F+VL}.
    kIdVpcompressq,                      //!< Instruction 'vpcompressq' {AVX512_F+VL}.
    kIdVpcompressw,                      //!< Instruction 'vpcompressw' {AVX512_VBMI2+VL}.
    kIdVpcomq,                           //!< Instruction 'vpcomq' {XOP}.
    kIdVpcomub,                          //!< Instruction 'vpcomub' {XOP}.
    kIdVpcomud,                          //!< Instruction 'vpcomud' {XOP}.
    kIdVpcomuq,                          //!< Instruction 'vpcomuq' {XOP}.
    kIdVpcomuw,                          //!< Instruction 'vpcomuw' {XOP}.
    kIdVpcomw,                           //!< Instruction 'vpcomw' {XOP}.
    kIdVpconflictd,                      //!< Instruction 'vpconflictd' {AVX512_CDI+VL}.
    kIdVpconflictq,                      //!< Instruction 'vpconflictq' {AVX512_CDI+VL}.
    kIdVpdpbusd,                         //!< Instruction 'vpdpbusd' {AVX_VNNI|AVX512_VNNI+VL}.
    kIdVpdpbusds,                        //!< Instruction 'vpdpbusds' {AVX_VNNI|AVX512_VNNI+VL}.
    kIdVpdpwssd,                         //!< Instruction 'vpdpwssd' {AVX_VNNI|AVX512_VNNI+VL}.
    kIdVpdpwssds,                        //!< Instruction 'vpdpwssds' {AVX_VNNI|AVX512_VNNI+VL}.
    kIdVperm2f128,                       //!< Instruction 'vperm2f128' {AVX}.
    kIdVperm2i128,                       //!< Instruction 'vperm2i128' {AVX2}.
    kIdVpermb,                           //!< Instruction 'vpermb' {AVX512_VBMI+VL}.
    kIdVpermd,                           //!< Instruction 'vpermd' {AVX2|AVX512_F+VL}.
    kIdVpermi2b,                         //!< Instruction 'vpermi2b' {AVX512_VBMI+VL}.
    kIdVpermi2d,                         //!< Instruction 'vpermi2d' {AVX512_F+VL}.
    kIdVpermi2pd,                        //!< Instruction 'vpermi2pd' {AVX512_F+VL}.
    kIdVpermi2ps,                        //!< Instruction 'vpermi2ps' {AVX512_F+VL}.
    kIdVpermi2q,                         //!< Instruction 'vpermi2q' {AVX512_F+VL}.
    kIdVpermi2w,                         //!< Instruction 'vpermi2w' {AVX512_BW+VL}.
    kIdVpermil2pd,                       //!< Instruction 'vpermil2pd' {XOP}.
    kIdVpermil2ps,                       //!< Instruction 'vpermil2ps' {XOP}.
    kIdVpermilpd,                        //!< Instruction 'vpermilpd' {AVX|AVX512_F+VL}.
    kIdVpermilps,                        //!< Instruction 'vpermilps' {AVX|AVX512_F+VL}.
    kIdVpermpd,                          //!< Instruction 'vpermpd' {AVX2|AVX512_F+VL}.
    kIdVpermps,                          //!< Instruction 'vpermps' {AVX2|AVX512_F+VL}.
    kIdVpermq,                           //!< Instruction 'vpermq' {AVX2|AVX512_F+VL}.
    kIdVpermt2b,                         //!< Instruction 'vpermt2b' {AVX512_VBMI+VL}.
    kIdVpermt2d,                         //!< Instruction 'vpermt2d' {AVX512_F+VL}.
    kIdVpermt2pd,                        //!< Instruction 'vpermt2pd' {AVX512_F+VL}.
    kIdVpermt2ps,                        //!< Instruction 'vpermt2ps' {AVX512_F+VL}.
    kIdVpermt2q,                         //!< Instruction 'vpermt2q' {AVX512_F+VL}.
    kIdVpermt2w,                         //!< Instruction 'vpermt2w' {AVX512_BW+VL}.
    kIdVpermw,                           //!< Instruction 'vpermw' {AVX512_BW+VL}.
    kIdVpexpandb,                        //!< Instruction 'vpexpandb' {AVX512_VBMI2+VL}.
    kIdVpexpandd,                        //!< Instruction 'vpexpandd' {AVX512_F+VL}.
    kIdVpexpandq,                        //!< Instruction 'vpexpandq' {AVX512_F+VL}.
    kIdVpexpandw,                        //!< Instruction 'vpexpandw' {AVX512_VBMI2+VL}.
    kIdVpextrb,                          //!< Instruction 'vpextrb' {AVX|AVX512_BW}.
    kIdVpextrd,                          //!< Instruction 'vpextrd' {AVX|AVX512_DQ}.
    kIdVpextrq,                          //!< Instruction 'vpextrq' {AVX|AVX512_DQ} (X64).
    kIdVpextrw,                          //!< Instruction 'vpextrw' {AVX|AVX512_BW}.
    kIdVpgatherdd,                       //!< Instruction 'vpgatherdd' {AVX2|AVX512_F+VL}.
    kIdVpgatherdq,                       //!< Instruction 'vpgatherdq' {AVX2|AVX512_F+VL}.
    kIdVpgatherqd,                       //!< Instruction 'vpgatherqd' {AVX2|AVX512_F+VL}.
    kIdVpgatherqq,                       //!< Instruction 'vpgatherqq' {AVX2|AVX512_F+VL}.
    kIdVphaddbd,                         //!< Instruction 'vphaddbd' {XOP}.
    kIdVphaddbq,                         //!< Instruction 'vphaddbq' {XOP}.
    kIdVphaddbw,                         //!< Instruction 'vphaddbw' {XOP}.
    kIdVphaddd,                          //!< Instruction 'vphaddd' {AVX|AVX2}.
    kIdVphadddq,                         //!< Instruction 'vphadddq' {XOP}.
    kIdVphaddsw,                         //!< Instruction 'vphaddsw' {AVX|AVX2}.
    kIdVphaddubd,                        //!< Instruction 'vphaddubd' {XOP}.
    kIdVphaddubq,                        //!< Instruction 'vphaddubq' {XOP}.
    kIdVphaddubw,                        //!< Instruction 'vphaddubw' {XOP}.
    kIdVphaddudq,                        //!< Instruction 'vphaddudq' {XOP}.
    kIdVphadduwd,                        //!< Instruction 'vphadduwd' {XOP}.
    kIdVphadduwq,                        //!< Instruction 'vphadduwq' {XOP}.
    kIdVphaddw,                          //!< Instruction 'vphaddw' {AVX|AVX2}.
    kIdVphaddwd,                         //!< Instruction 'vphaddwd' {XOP}.
    kIdVphaddwq,                         //!< Instruction 'vphaddwq' {XOP}.
    kIdVphminposuw,                      //!< Instruction 'vphminposuw' {AVX}.
    kIdVphsubbw,                         //!< Instruction 'vphsubbw' {XOP}.
    kIdVphsubd,                          //!< Instruction 'vphsubd' {AVX|AVX2}.
    kIdVphsubdq,                         //!< Instruction 'vphsubdq' {XOP}.
    kIdVphsubsw,                         //!< Instruction 'vphsubsw' {AVX|AVX2}.
    kIdVphsubw,                          //!< Instruction 'vphsubw' {AVX|AVX2}.
    kIdVphsubwd,                         //!< Instruction 'vphsubwd' {XOP}.
    kIdVpinsrb,                          //!< Instruction 'vpinsrb' {AVX|AVX512_BW}.
    kIdVpinsrd,                          //!< Instruction 'vpinsrd' {AVX|AVX512_DQ}.
    kIdVpinsrq,                          //!< Instruction 'vpinsrq' {AVX|AVX512_DQ} (X64).
    kIdVpinsrw,                          //!< Instruction 'vpinsrw' {AVX|AVX512_BW}.
    kIdVplzcntd,                         //!< Instruction 'vplzcntd' {AVX512_CDI+VL}.
    kIdVplzcntq,                         //!< Instruction 'vplzcntq' {AVX512_CDI+VL}.
    kIdVpmacsdd,                         //!< Instruction 'vpmacsdd' {XOP}.
    kIdVpmacsdqh,                        //!< Instruction 'vpmacsdqh' {XOP}.
    kIdVpmacsdql,                        //!< Instruction 'vpmacsdql' {XOP}.
    kIdVpmacssdd,                        //!< Instruction 'vpmacssdd' {XOP}.
    kIdVpmacssdqh,                       //!< Instruction 'vpmacssdqh' {XOP}.
    kIdVpmacssdql,                       //!< Instruction 'vpmacssdql' {XOP}.
    kIdVpmacsswd,                        //!< Instruction 'vpmacsswd' {XOP}.
    kIdVpmacssww,                        //!< Instruction 'vpmacssww' {XOP}.
    kIdVpmacswd,                         //!< Instruction 'vpmacswd' {XOP}.
    kIdVpmacsww,                         //!< Instruction 'vpmacsww' {XOP}.
    kIdVpmadcsswd,                       //!< Instruction 'vpmadcsswd' {XOP}.
    kIdVpmadcswd,                        //!< Instruction 'vpmadcswd' {XOP}.
    kIdVpmadd52huq,                      //!< Instruction 'vpmadd52huq' {AVX512_IFMA+VL}.
    kIdVpmadd52luq,                      //!< Instruction 'vpmadd52luq' {AVX512_IFMA+VL}.
    kIdVpmaddubsw,                       //!< Instruction 'vpmaddubsw' {AVX|AVX2|AVX512_BW+VL}.
    kIdVpmaddwd,                         //!< Instruction 'vpmaddwd' {AVX|AVX2|AVX512_BW+VL}.
    kIdVpmaskmovd,                       //!< Instruction 'vpmaskmovd' {AVX2}.
    kIdVpmaskmovq,                       //!< Instruction 'vpmaskmovq' {AVX2}.
    kIdVpmaxsb,                          //!< Instruction 'vpmaxsb' {AVX|AVX2|AVX512_BW+VL}.
    kIdVpmaxsd,                          //!< Instruction 'vpmaxsd' {AVX|AVX2|AVX512_F+VL}.
    kIdVpmaxsq,                          //!< Instruction 'vpmaxsq' {AVX512_F+VL}.
    kIdVpmaxsw,                          //!< Instruction 'vpmaxsw' {AVX|AVX2|AVX512_BW+VL}.
    kIdVpmaxub,                          //!< Instruction 'vpmaxub' {AVX|AVX2|AVX512_BW+VL}.
    kIdVpmaxud,                          //!< Instruction 'vpmaxud' {AVX|AVX2|AVX512_F+VL}.
    kIdVpmaxuq,                          //!< Instruction 'vpmaxuq' {AVX512_F+VL}.
    kIdVpmaxuw,                          //!< Instruction 'vpmaxuw' {AVX|AVX2|AVX512_BW+VL}.
    kIdVpminsb,                          //!< Instruction 'vpminsb' {AVX|AVX2|AVX512_BW+VL}.
    kIdVpminsd,                          //!< Instruction 'vpminsd' {AVX|AVX2|AVX512_F+VL}.
    kIdVpminsq,                          //!< Instruction 'vpminsq' {AVX512_F+VL}.
    kIdVpminsw,                          //!< Instruction 'vpminsw' {AVX|AVX2|AVX512_BW+VL}.
    kIdVpminub,                          //!< Instruction 'vpminub' {AVX|AVX2|AVX512_BW+VL}.
    kIdVpminud,                          //!< Instruction 'vpminud' {AVX|AVX2|AVX512_F+VL}.
    kIdVpminuq,                          //!< Instruction 'vpminuq' {AVX512_F+VL}.
    kIdVpminuw,                          //!< Instruction 'vpminuw' {AVX|AVX2|AVX512_BW+VL}.
    kIdVpmovb2m,                         //!< Instruction 'vpmovb2m' {AVX512_BW+VL}.
    kIdVpmovd2m,                         //!< Instruction 'vpmovd2m' {AVX512_DQ+VL}.
    kIdVpmovdb,                          //!< Instruction 'vpmovdb' {AVX512_F+VL}.
    kIdVpmovdw,                          //!< Instruction 'vpmovdw' {AVX512_F+VL}.
    kIdVpmovm2b,                         //!< Instruction 'vpmovm2b' {AVX512_BW+VL}.
    kIdVpmovm2d,                         //!< Instruction 'vpmovm2d' {AVX512_DQ+VL}.
    kIdVpmovm2q,                         //!< Instruction 'vpmovm2q' {AVX512_DQ+VL}.
    kIdVpmovm2w,                         //!< Instruction 'vpmovm2w' {AVX512_BW+VL}.
    kIdVpmovmskb,                        //!< Instruction 'vpmovmskb' {AVX|AVX2}.
    kIdVpmovq2m,                         //!< Instruction 'vpmovq2m' {AVX512_DQ+VL}.
    kIdVpmovqb,                          //!< Instruction 'vpmovqb' {AVX512_F+VL}.
    kIdVpmovqd,                          //!< Instruction 'vpmovqd' {AVX512_F+VL}.
    kIdVpmovqw,                          //!< Instruction 'vpmovqw' {AVX512_F+VL}.
    kIdVpmovsdb,                         //!< Instruction 'vpmovsdb' {AVX512_F+VL}.
    kIdVpmovsdw,                         //!< Instruction 'vpmovsdw' {AVX512_F+VL}.
    kIdVpmovsqb,                         //!< Instruction 'vpmovsqb' {AVX512_F+VL}.
    kIdVpmovsqd,                         //!< Instruction 'vpmovsqd' {AVX512_F+VL}.
    kIdVpmovsqw,                         //!< Instruction 'vpmovsqw' {AVX512_F+VL}.
    kIdVpmovswb,                         //!< Instruction 'vpmovswb' {AVX512_BW+VL}.
    kIdVpmovsxbd,                        //!< Instruction 'vpmovsxbd' {AVX|AVX2|AVX512_F+VL}.
    kIdVpmovsxbq,                        //!< Instruction 'vpmovsxbq' {AVX|AVX2|AVX512_F+VL}.
    kIdVpmovsxbw,                        //!< Instruction 'vpmovsxbw' {AVX|AVX2|AVX512_BW+VL}.
    kIdVpmovsxdq,                        //!< Instruction 'vpmovsxdq' {AVX|AVX2|AVX512_F+VL}.
    kIdVpmovsxwd,                        //!< Instruction 'vpmovsxwd' {AVX|AVX2|AVX512_F+VL}.
    kIdVpmovsxwq,                        //!< Instruction 'vpmovsxwq' {AVX|AVX2|AVX512_F+VL}.
    kIdVpmovusdb,                        //!< Instruction 'vpmovusdb' {AVX512_F+VL}.
    kIdVpmovusdw,                        //!< Instruction 'vpmovusdw' {AVX512_F+VL}.
    kIdVpmovusqb,                        //!< Instruction 'vpmovusqb' {AVX512_F+VL}.
    kIdVpmovusqd,                        //!< Instruction 'vpmovusqd' {AVX512_F+VL}.
    kIdVpmovusqw,                        //!< Instruction 'vpmovusqw' {AVX512_F+VL}.
    kIdVpmovuswb,                        //!< Instruction 'vpmovuswb' {AVX512_BW+VL}.
    kIdVpmovw2m,                         //!< Instruction 'vpmovw2m' {AVX512_BW+VL}.
    kIdVpmovwb,                          //!< Instruction 'vpmovwb' {AVX512_BW+VL}.
    kIdVpmovzxbd,                        //!< Instruction 'vpmovzxbd' {AVX|AVX2|AVX512_F+VL}.
    kIdVpmovzxbq,                        //!< Instruction 'vpmovzxbq' {AVX|AVX2|AVX512_F+VL}.
    kIdVpmovzxbw,                        //!< Instruction 'vpmovzxbw' {AVX|AVX2|AVX512_BW+VL}.
    kIdVpmovzxdq,                        //!< Instruction 'vpmovzxdq' {AVX|AVX2|AVX512_F+VL}.
    kIdVpmovzxwd,                        //!< Instruction 'vpmovzxwd' {AVX|AVX2|AVX512_F+VL}.
    kIdVpmovzxwq,                        //!< Instruction 'vpmovzxwq' {AVX|AVX2|AVX512_F+VL}.
    kIdVpmuldq,                          //!< Instruction 'vpmuldq' {AVX|AVX2|AVX512_F+VL}.
    kIdVpmulhrsw,                        //!< Instruction 'vpmulhrsw' {AVX|AVX2|AVX512_BW+VL}.
    kIdVpmulhuw,                         //!< Instruction 'vpmulhuw' {AVX|AVX2|AVX512_BW+VL}.
    kIdVpmulhw,                          //!< Instruction 'vpmulhw' {AVX|AVX2|AVX512_BW+VL}.
    kIdVpmulld,                          //!< Instruction 'vpmulld' {AVX|AVX2|AVX512_F+VL}.
    kIdVpmullq,                          //!< Instruction 'vpmullq' {AVX512_DQ+VL}.
    kIdVpmullw,                          //!< Instruction 'vpmullw' {AVX|AVX2|AVX512_BW+VL}.
    kIdVpmultishiftqb,                   //!< Instruction 'vpmultishiftqb' {AVX512_VBMI+VL}.
    kIdVpmuludq,                         //!< Instruction 'vpmuludq' {AVX|AVX2|AVX512_F+VL}.
    kIdVpopcntb,                         //!< Instruction 'vpopcntb' {AVX512_BITALG+VL}.
    kIdVpopcntd,                         //!< Instruction 'vpopcntd' {AVX512_VPOPCNTDQ+VL}.
    kIdVpopcntq,                         //!< Instruction 'vpopcntq' {AVX512_VPOPCNTDQ+VL}.
    kIdVpopcntw,                         //!< Instruction 'vpopcntw' {AVX512_BITALG+VL}.
    kIdVpor,                             //!< Instruction 'vpor' {AVX|AVX2}.
    kIdVpord,                            //!< Instruction 'vpord' {AVX512_F+VL}.
    kIdVporq,                            //!< Instruction 'vporq' {AVX512_F+VL}.
    kIdVpperm,                           //!< Instruction 'vpperm' {XOP}.
    kIdVprold,                           //!< Instruction 'vprold' {AVX512_F+VL}.
    kIdVprolq,                           //!< Instruction 'vprolq' {AVX512_F+VL}.
    kIdVprolvd,                          //!< Instruction 'vprolvd' {AVX512_F+VL}.
    kIdVprolvq,                          //!< Instruction 'vprolvq' {AVX512_F+VL}.
    kIdVprord,                           //!< Instruction 'vprord' {AVX512_F+VL}.
    kIdVprorq,                           //!< Instruction 'vprorq' {AVX512_F+VL}.
    kIdVprorvd,                          //!< Instruction 'vprorvd' {AVX512_F+VL}.
    kIdVprorvq,                          //!< Instruction 'vprorvq' {AVX512_F+VL}.
    kIdVprotb,                           //!< Instruction 'vprotb' {XOP}.
    kIdVprotd,                           //!< Instruction 'vprotd' {XOP}.
    kIdVprotq,                           //!< Instruction 'vprotq' {XOP}.
    kIdVprotw,                           //!< Instruction 'vprotw' {XOP}.
    kIdVpsadbw,                          //!< Instruction 'vpsadbw' {AVX|AVX2|AVX512_BW+VL}.
    kIdVpscatterdd,                      //!< Instruction 'vpscatterdd' {AVX512_F+VL}.
    kIdVpscatterdq,                      //!< Instruction 'vpscatterdq' {AVX512_F+VL}.
    kIdVpscatterqd,                      //!< Instruction 'vpscatterqd' {AVX512_F+VL}.
    kIdVpscatterqq,                      //!< Instruction 'vpscatterqq' {AVX512_F+VL}.
    kIdVpshab,                           //!< Instruction 'vpshab' {XOP}.
    kIdVpshad,                           //!< Instruction 'vpshad' {XOP}.
    kIdVpshaq,                           //!< Instruction 'vpshaq' {XOP}.
    kIdVpshaw,                           //!< Instruction 'vpshaw' {XOP}.
    kIdVpshlb,                           //!< Instruction 'vpshlb' {XOP}.
    kIdVpshld,                           //!< Instruction 'vpshld' {XOP}.
    kIdVpshldd,                          //!< Instruction 'vpshldd' {AVX512_VBMI2+VL}.
    kIdVpshldq,                          //!< Instruction 'vpshldq' {AVX512_VBMI2+VL}.
    kIdVpshldvd,                         //!< Instruction 'vpshldvd' {AVX512_VBMI2+VL}.
    kIdVpshldvq,                         //!< Instruction 'vpshldvq' {AVX512_VBMI2+VL}.
    kIdVpshldvw,                         //!< Instruction 'vpshldvw' {AVX512_VBMI2+VL}.
    kIdVpshldw,                          //!< Instruction 'vpshldw' {AVX512_VBMI2+VL}.
    kIdVpshlq,                           //!< Instruction 'vpshlq' {XOP}.
    kIdVpshlw,                           //!< Instruction 'vpshlw' {XOP}.
    kIdVpshrdd,                          //!< Instruction 'vpshrdd' {AVX512_VBMI2+VL}.
    kIdVpshrdq,                          //!< Instruction 'vpshrdq' {AVX512_VBMI2+VL}.
    kIdVpshrdvd,                         //!< Instruction 'vpshrdvd' {AVX512_VBMI2+VL}.
    kIdVpshrdvq,                         //!< Instruction 'vpshrdvq' {AVX512_VBMI2+VL}.
    kIdVpshrdvw,                         //!< Instruction 'vpshrdvw' {AVX512_VBMI2+VL}.
    kIdVpshrdw,                          //!< Instruction 'vpshrdw' {AVX512_VBMI2+VL}.
    kIdVpshufb,                          //!< Instruction 'vpshufb' {AVX|AVX2|AVX512_BW+VL}.
    kIdVpshufbitqmb,                     //!< Instruction 'vpshufbitqmb' {AVX512_BITALG+VL}.
    kIdVpshufd,                          //!< Instruction 'vpshufd' {AVX|AVX2|AVX512_F+VL}.
    kIdVpshufhw,                         //!< Instruction 'vpshufhw' {AVX|AVX2|AVX512_BW+VL}.
    kIdVpshuflw,                         //!< Instruction 'vpshuflw' {AVX|AVX2|AVX512_BW+VL}.
    kIdVpsignb,                          //!< Instruction 'vpsignb' {AVX|AVX2}.
    kIdVpsignd,                          //!< Instruction 'vpsignd' {AVX|AVX2}.
    kIdVpsignw,                          //!< Instruction 'vpsignw' {AVX|AVX2}.
    kIdVpslld,                           //!< Instruction 'vpslld' {AVX|AVX2|AVX512_F+VL}.
    kIdVpslldq,                          //!< Instruction 'vpslldq' {AVX|AVX2|AVX512_BW+VL}.
    kIdVpsllq,                           //!< Instruction 'vpsllq' {AVX|AVX2|AVX512_F+VL}.
    kIdVpsllvd,                          //!< Instruction 'vpsllvd' {AVX2|AVX512_F+VL}.
    kIdVpsllvq,                          //!< Instruction 'vpsllvq' {AVX2|AVX512_F+VL}.
    kIdVpsllvw,                          //!< Instruction 'vpsllvw' {AVX512_BW+VL}.
    kIdVpsllw,                           //!< Instruction 'vpsllw' {AVX|AVX2|AVX512_BW+VL}.
    kIdVpsrad,                           //!< Instruction 'vpsrad' {AVX|AVX2|AVX512_F+VL}.
    kIdVpsraq,                           //!< Instruction 'vpsraq' {AVX512_F+VL}.
    kIdVpsravd,                          //!< Instruction 'vpsravd' {AVX2|AVX512_F+VL}.
    kIdVpsravq,                          //!< Instruction 'vpsravq' {AVX512_F+VL}.
    kIdVpsravw,                          //!< Instruction 'vpsravw' {AVX512_BW+VL}.
    kIdVpsraw,                           //!< Instruction 'vpsraw' {AVX|AVX2|AVX512_BW+VL}.
    kIdVpsrld,                           //!< Instruction 'vpsrld' {AVX|AVX2|AVX512_F+VL}.
    kIdVpsrldq,                          //!< Instruction 'vpsrldq' {AVX|AVX2|AVX512_BW+VL}.
    kIdVpsrlq,                           //!< Instruction 'vpsrlq' {AVX|AVX2|AVX512_F+VL}.
    kIdVpsrlvd,                          //!< Instruction 'vpsrlvd' {AVX2|AVX512_F+VL}.
    kIdVpsrlvq,                          //!< Instruction 'vpsrlvq' {AVX2|AVX512_F+VL}.
    kIdVpsrlvw,                          //!< Instruction 'vpsrlvw' {AVX512_BW+VL}.
    kIdVpsrlw,                           //!< Instruction 'vpsrlw' {AVX|AVX2|AVX512_BW+VL}.
    kIdVpsubb,                           //!< Instruction 'vpsubb' {AVX|AVX2|AVX512_BW+VL}.
    kIdVpsubd,                           //!< Instruction 'vpsubd' {AVX|AVX2|AVX512_F+VL}.
    kIdVpsubq,                           //!< Instruction 'vpsubq' {AVX|AVX2|AVX512_F+VL}.
    kIdVpsubsb,                          //!< Instruction 'vpsubsb' {AVX|AVX2|AVX512_BW+VL}.
    kIdVpsubsw,                          //!< Instruction 'vpsubsw' {AVX|AVX2|AVX512_BW+VL}.
    kIdVpsubusb,                         //!< Instruction 'vpsubusb' {AVX|AVX2|AVX512_BW+VL}.
    kIdVpsubusw,                         //!< Instruction 'vpsubusw' {AVX|AVX2|AVX512_BW+VL}.
    kIdVpsubw,                           //!< Instruction 'vpsubw' {AVX|AVX2|AVX512_BW+VL}.
    kIdVpternlogd,                       //!< Instruction 'vpternlogd' {AVX512_F+VL}.
    kIdVpternlogq,                       //!< Instruction 'vpternlogq' {AVX512_F+VL}.
    kIdVptest,                           //!< Instruction 'vptest' {AVX}.
    kIdVptestmb,                         //!< Instruction 'vptestmb' {AVX512_BW+VL}.
    kIdVptestmd,                         //!< Instruction 'vptestmd' {AVX512_F+VL}.
    kIdVptestmq,                         //!< Instruction 'vptestmq' {AVX512_F+VL}.
    kIdVptestmw,                         //!< Instruction 'vptestmw' {AVX512_BW+VL}.
    kIdVptestnmb,                        //!< Instruction 'vptestnmb' {AVX512_BW+VL}.
    kIdVptestnmd,                        //!< Instruction 'vptestnmd' {AVX512_F+VL}.
    kIdVptestnmq,                        //!< Instruction 'vptestnmq' {AVX512_F+VL}.
    kIdVptestnmw,                        //!< Instruction 'vptestnmw' {AVX512_BW+VL}.
    kIdVpunpckhbw,                       //!< Instruction 'vpunpckhbw' {AVX|AVX2|AVX512_BW+VL}.
    kIdVpunpckhdq,                       //!< Instruction 'vpunpckhdq' {AVX|AVX2|AVX512_F+VL}.
    kIdVpunpckhqdq,                      //!< Instruction 'vpunpckhqdq' {AVX|AVX2|AVX512_F+VL}.
    kIdVpunpckhwd,                       //!< Instruction 'vpunpckhwd' {AVX|AVX2|AVX512_BW+VL}.
    kIdVpunpcklbw,                       //!< Instruction 'vpunpcklbw' {AVX|AVX2|AVX512_BW+VL}.
    kIdVpunpckldq,                       //!< Instruction 'vpunpckldq' {AVX|AVX2|AVX512_F+VL}.
    kIdVpunpcklqdq,                      //!< Instruction 'vpunpcklqdq' {AVX|AVX2|AVX512_F+VL}.
    kIdVpunpcklwd,                       //!< Instruction 'vpunpcklwd' {AVX|AVX2|AVX512_BW+VL}.
    kIdVpxor,                            //!< Instruction 'vpxor' {AVX|AVX2}.
    kIdVpxord,                           //!< Instruction 'vpxord' {AVX512_F+VL}.
    kIdVpxorq,                           //!< Instruction 'vpxorq' {AVX512_F+VL}.
    kIdVrangepd,                         //!< Instruction 'vrangepd' {AVX512_DQ+VL}.
    kIdVrangeps,                         //!< Instruction 'vrangeps' {AVX512_DQ+VL}.
    kIdVrangesd,                         //!< Instruction 'vrangesd' {AVX512_DQ}.
    kIdVrangess,                         //!< Instruction 'vrangess' {AVX512_DQ}.
    kIdVrcp14pd,                         //!< Instruction 'vrcp14pd' {AVX512_F+VL}.
    kIdVrcp14ps,                         //!< Instruction 'vrcp14ps' {AVX512_F+VL}.
    kIdVrcp14sd,                         //!< Instruction 'vrcp14sd' {AVX512_F}.
    kIdVrcp14ss,                         //!< Instruction 'vrcp14ss' {AVX512_F}.
    kIdVrcp28pd,                         //!< Instruction 'vrcp28pd' {AVX512_ERI}.
    kIdVrcp28ps,                         //!< Instruction 'vrcp28ps' {AVX512_ERI}.
    kIdVrcp28sd,                         //!< Instruction 'vrcp28sd' {AVX512_ERI}.
    kIdVrcp28ss,                         //!< Instruction 'vrcp28ss' {AVX512_ERI}.
    kIdVrcpps,                           //!< Instruction 'vrcpps' {AVX}.
    kIdVrcpss,                           //!< Instruction 'vrcpss' {AVX}.
    kIdVreducepd,                        //!< Instruction 'vreducepd' {AVX512_DQ+VL}.
    kIdVreduceps,                        //!< Instruction 'vreduceps' {AVX512_DQ+VL}.
    kIdVreducesd,                        //!< Instruction 'vreducesd' {AVX512_DQ}.
    kIdVreducess,                        //!< Instruction 'vreducess' {AVX512_DQ}.
    kIdVrndscalepd,                      //!< Instruction 'vrndscalepd' {AVX512_F+VL}.
    kIdVrndscaleps,                      //!< Instruction 'vrndscaleps' {AVX512_F+VL}.
    kIdVrndscalesd,                      //!< Instruction 'vrndscalesd' {AVX512_F}.
    kIdVrndscaless,                      //!< Instruction 'vrndscaless' {AVX512_F}.
    kIdVroundpd,                         //!< Instruction 'vroundpd' {AVX}.
    kIdVroundps,                         //!< Instruction 'vroundps' {AVX}.
    kIdVroundsd,                         //!< Instruction 'vroundsd' {AVX}.
    kIdVroundss,                         //!< Instruction 'vroundss' {AVX}.
    kIdVrsqrt14pd,                       //!< Instruction 'vrsqrt14pd' {AVX512_F+VL}.
    kIdVrsqrt14ps,                       //!< Instruction 'vrsqrt14ps' {AVX512_F+VL}.
    kIdVrsqrt14sd,                       //!< Instruction 'vrsqrt14sd' {AVX512_F}.
    kIdVrsqrt14ss,                       //!< Instruction 'vrsqrt14ss' {AVX512_F}.
    kIdVrsqrt28pd,                       //!< Instruction 'vrsqrt28pd' {AVX512_ERI}.
    kIdVrsqrt28ps,                       //!< Instruction 'vrsqrt28ps' {AVX512_ERI}.
    kIdVrsqrt28sd,                       //!< Instruction 'vrsqrt28sd' {AVX512_ERI}.
    kIdVrsqrt28ss,                       //!< Instruction 'vrsqrt28ss' {AVX512_ERI}.
    kIdVrsqrtps,                         //!< Instruction 'vrsqrtps' {AVX}.
    kIdVrsqrtss,                         //!< Instruction 'vrsqrtss' {AVX}.
    kIdVscalefpd,                        //!< Instruction 'vscalefpd' {AVX512_F+VL}.
    kIdVscalefps,                        //!< Instruction 'vscalefps' {AVX512_F+VL}.
    kIdVscalefsd,                        //!< Instruction 'vscalefsd' {AVX512_F}.
    kIdVscalefss,                        //!< Instruction 'vscalefss' {AVX512_F}.
    kIdVscatterdpd,                      //!< Instruction 'vscatterdpd' {AVX512_F+VL}.
    kIdVscatterdps,                      //!< Instruction 'vscatterdps' {AVX512_F+VL}.
    kIdVscatterpf0dpd,                   //!< Instruction 'vscatterpf0dpd' {AVX512_PFI}.
    kIdVscatterpf0dps,                   //!< Instruction 'vscatterpf0dps' {AVX512_PFI}.
    kIdVscatterpf0qpd,                   //!< Instruction 'vscatterpf0qpd' {AVX512_PFI}.
    kIdVscatterpf0qps,                   //!< Instruction 'vscatterpf0qps' {AVX512_PFI}.
    kIdVscatterpf1dpd,                   //!< Instruction 'vscatterpf1dpd' {AVX512_PFI}.
    kIdVscatterpf1dps,                   //!< Instruction 'vscatterpf1dps' {AVX512_PFI}.
    kIdVscatterpf1qpd,                   //!< Instruction 'vscatterpf1qpd' {AVX512_PFI}.
    kIdVscatterpf1qps,                   //!< Instruction 'vscatterpf1qps' {AVX512_PFI}.
    kIdVscatterqpd,                      //!< Instruction 'vscatterqpd' {AVX512_F+VL}.
    kIdVscatterqps,                      //!< Instruction 'vscatterqps' {AVX512_F+VL}.
    kIdVshuff32x4,                       //!< Instruction 'vshuff32x4' {AVX512_F+VL}.
    kIdVshuff64x2,                       //!< Instruction 'vshuff64x2' {AVX512_F+VL}.
    kIdVshufi32x4,                       //!< Instruction 'vshufi32x4' {AVX512_F+VL}.
    kIdVshufi64x2,                       //!< Instruction 'vshufi64x2' {AVX512_F+VL}.
    kIdVshufpd,                          //!< Instruction 'vshufpd' {AVX|AVX512_F+VL}.
    kIdVshufps,                          //!< Instruction 'vshufps' {AVX|AVX512_F+VL}.
    kIdVsqrtpd,                          //!< Instruction 'vsqrtpd' {AVX|AVX512_F+VL}.
    kIdVsqrtps,                          //!< Instruction 'vsqrtps' {AVX|AVX512_F+VL}.
    kIdVsqrtsd,                          //!< Instruction 'vsqrtsd' {AVX|AVX512_F}.
    kIdVsqrtss,                          //!< Instruction 'vsqrtss' {AVX|AVX512_F}.
    kIdVstmxcsr,                         //!< Instruction 'vstmxcsr' {AVX}.
    kIdVsubpd,                           //!< Instruction 'vsubpd' {AVX|AVX512_F+VL}.
    kIdVsubps,                           //!< Instruction 'vsubps' {AVX|AVX512_F+VL}.
    kIdVsubsd,                           //!< Instruction 'vsubsd' {AVX|AVX512_F}.
    kIdVsubss,                           //!< Instruction 'vsubss' {AVX|AVX512_F}.
    kIdVtestpd,                          //!< Instruction 'vtestpd' {AVX}.
    kIdVtestps,                          //!< Instruction 'vtestps' {AVX}.
    kIdVucomisd,                         //!< Instruction 'vucomisd' {AVX|AVX512_F}.
    kIdVucomiss,                         //!< Instruction 'vucomiss' {AVX|AVX512_F}.
    kIdVunpckhpd,                        //!< Instruction 'vunpckhpd' {AVX|AVX512_F+VL}.
    kIdVunpckhps,                        //!< Instruction 'vunpckhps' {AVX|AVX512_F+VL}.
    kIdVunpcklpd,                        //!< Instruction 'vunpcklpd' {AVX|AVX512_F+VL}.
    kIdVunpcklps,                        //!< Instruction 'vunpcklps' {AVX|AVX512_F+VL}.
    kIdVxorpd,                           //!< Instruction 'vxorpd' {AVX|AVX512_DQ+VL}.
    kIdVxorps,                           //!< Instruction 'vxorps' {AVX|AVX512_DQ+VL}.
    kIdVzeroall,                         //!< Instruction 'vzeroall' {AVX}.
    kIdVzeroupper,                       //!< Instruction 'vzeroupper' {AVX}.
    kIdWbinvd,                           //!< Instruction 'wbinvd'.
    kIdWbnoinvd,                         //!< Instruction 'wbnoinvd' {WBNOINVD}.
    kIdWrfsbase,                         //!< Instruction 'wrfsbase' {FSGSBASE} (X64).
    kIdWrgsbase,                         //!< Instruction 'wrgsbase' {FSGSBASE} (X64).
    kIdWrmsr,                            //!< Instruction 'wrmsr' {MSR}.
    kIdWrssd,                            //!< Instruction 'wrssd' {CET_SS}.
    kIdWrssq,                            //!< Instruction 'wrssq' {CET_SS} (X64).
    kIdWrussd,                           //!< Instruction 'wrussd' {CET_SS}.
    kIdWrussq,                           //!< Instruction 'wrussq' {CET_SS} (X64).
    kIdXabort,                           //!< Instruction 'xabort' {RTM}.
    kIdXadd,                             //!< Instruction 'xadd' {I486}.
    kIdXbegin,                           //!< Instruction 'xbegin' {RTM}.
    kIdXchg,                             //!< Instruction 'xchg'.
    kIdXend,                             //!< Instruction 'xend' {RTM}.
    kIdXgetbv,                           //!< Instruction 'xgetbv' {XSAVE}.
    kIdXlatb,                            //!< Instruction 'xlatb'.
    kIdXor,                              //!< Instruction 'xor'.
    kIdXorpd,                            //!< Instruction 'xorpd' {SSE2}.
    kIdXorps,                            //!< Instruction 'xorps' {SSE}.
    kIdXresldtrk,                        //!< Instruction 'xresldtrk' {TSXLDTRK}.
    kIdXrstor,                           //!< Instruction 'xrstor' {XSAVE}.
    kIdXrstor64,                         //!< Instruction 'xrstor64' {XSAVE} (X64).
    kIdXrstors,                          //!< Instruction 'xrstors' {XSAVES}.
    kIdXrstors64,                        //!< Instruction 'xrstors64' {XSAVES} (X64).
    kIdXsave,                            //!< Instruction 'xsave' {XSAVE}.
    kIdXsave64,                          //!< Instruction 'xsave64' {XSAVE} (X64).
    kIdXsavec,                           //!< Instruction 'xsavec' {XSAVEC}.
    kIdXsavec64,                         //!< Instruction 'xsavec64' {XSAVEC} (X64).
    kIdXsaveopt,                         //!< Instruction 'xsaveopt' {XSAVEOPT}.
    kIdXsaveopt64,                       //!< Instruction 'xsaveopt64' {XSAVEOPT} (X64).
    kIdXsaves,                           //!< Instruction 'xsaves' {XSAVES}.
    kIdXsaves64,                         //!< Instruction 'xsaves64' {XSAVES} (X64).
    kIdXsetbv,                           //!< Instruction 'xsetbv' {XSAVE}.
    kIdXsusldtrk,                        //!< Instruction 'xsusldtrk' {TSXLDTRK}.
    kIdXtest,                            //!< Instruction 'xtest' {TSX}.
    _kIdCount
    // ${InstId:End}
  };

  //! Instruction options.
  enum Options : uint32_t {
    kOptionModMR          = 0x00000100u, //!< Use ModMR instead of ModRM if applicable.
    kOptionModRM          = 0x00000200u, //!< Use ModRM instead of ModMR if applicable.
    kOptionVex3           = 0x00000400u, //!< Use 3-byte VEX prefix if possible (AVX) (must be 0x00000400).
    kOptionVex            = 0x00000800u, //!< Use VEX prefix when both VEX|EVEX prefixes are available (HINT: AVX_VNNI).
    kOptionEvex           = 0x00001000u, //!< Use 4-byte EVEX prefix if possible (AVX-512) (must be 0x00001000).

    kOptionLock           = 0x00002000u, //!< LOCK prefix (lock-enabled instructions only).
    kOptionRep            = 0x00004000u, //!< REP prefix (string instructions only).
    kOptionRepne          = 0x00008000u, //!< REPNE prefix (string instructions only).

    kOptionXAcquire       = 0x00010000u, //!< XACQUIRE prefix (only allowed instructions).
    kOptionXRelease       = 0x00020000u, //!< XRELEASE prefix (only allowed instructions).

    kOptionER             = 0x00040000u, //!< AVX-512: embedded-rounding {er} and implicit {sae}.
    kOptionSAE            = 0x00080000u, //!< AVX-512: suppress-all-exceptions {sae}.
    kOptionRN_SAE         = 0x00000000u, //!< AVX-512: round-to-nearest (even)      {rn-sae} (bits 00).
    kOptionRD_SAE         = 0x00200000u, //!< AVX-512: round-down (toward -inf)     {rd-sae} (bits 01).
    kOptionRU_SAE         = 0x00400000u, //!< AVX-512: round-up (toward +inf)       {ru-sae} (bits 10).
    kOptionRZ_SAE         = 0x00600000u, //!< AVX-512: round-toward-zero (truncate) {rz-sae} (bits 11).
    kOptionZMask          = 0x00800000u, //!< AVX-512: Use zeroing {k}{z} instead of merging {k}.
    _kOptionAvx512Mask    = 0x00FC0000u, //!< AVX-512: Mask of all possible AVX-512 options except EVEX prefix flag.

    kOptionOpCodeB        = 0x01000000u, //!< REX.B and/or VEX.B field (X64).
    kOptionOpCodeX        = 0x02000000u, //!< REX.X and/or VEX.X field (X64).
    kOptionOpCodeR        = 0x04000000u, //!< REX.R and/or VEX.R field (X64).
    kOptionOpCodeW        = 0x08000000u, //!< REX.W and/or VEX.W field (X64).
    kOptionRex            = 0x40000000u, //!< Force REX prefix (X64).
    _kOptionInvalidRex    = 0x80000000u  //!< Invalid REX prefix (set by X86 or when AH|BH|CH|DH regs are used on X64).
  };

  // --------------------------------------------------------------------------
  // [Statics]
  // --------------------------------------------------------------------------

  //! Tests whether the `instId` is defined (counts also Inst::kIdNone, which must be zero).
  static inline bool isDefinedId(uint32_t instId) noexcept { return instId < _kIdCount; }
};

// ============================================================================
// [asmjit::x86::Condition]
// ============================================================================

namespace Condition {
  //! Condition code.
  enum Code : uint32_t {
    kO                    = 0x00u,       //!<                 OF==1
    kNO                   = 0x01u,       //!<                 OF==0
    kB                    = 0x02u,       //!< CF==1                  (unsigned < )
    kC                    = 0x02u,       //!< CF==1
    kNAE                  = 0x02u,       //!< CF==1                  (unsigned < )
    kAE                   = 0x03u,       //!< CF==0                  (unsigned >=)
    kNB                   = 0x03u,       //!< CF==0                  (unsigned >=)
    kNC                   = 0x03u,       //!< CF==0
    kE                    = 0x04u,       //!<         ZF==1          (any_sign ==)
    kZ                    = 0x04u,       //!<         ZF==1          (any_sign ==)
    kNE                   = 0x05u,       //!<         ZF==0          (any_sign !=)
    kNZ                   = 0x05u,       //!<         ZF==0          (any_sign !=)
    kBE                   = 0x06u,       //!< CF==1 | ZF==1          (unsigned <=)
    kNA                   = 0x06u,       //!< CF==1 | ZF==1          (unsigned <=)
    kA                    = 0x07u,       //!< CF==0 & ZF==0          (unsigned > )
    kNBE                  = 0x07u,       //!< CF==0 & ZF==0          (unsigned > )
    kS                    = 0x08u,       //!<                 SF==1  (is negative)
    kNS                   = 0x09u,       //!<                 SF==0  (is positive or zero)
    kP                    = 0x0Au,       //!< PF==1
    kPE                   = 0x0Au,       //!< PF==1
    kPO                   = 0x0Bu,       //!< PF==0
    kNP                   = 0x0Bu,       //!< PF==0
    kL                    = 0x0Cu,       //!<                 SF!=OF (signed   < )
    kNGE                  = 0x0Cu,       //!<                 SF!=OF (signed   < )
    kGE                   = 0x0Du,       //!<                 SF==OF (signed   >=)
    kNL                   = 0x0Du,       //!<                 SF==OF (signed   >=)
    kLE                   = 0x0Eu,       //!<         ZF==1 | SF!=OF (signed   <=)
    kNG                   = 0x0Eu,       //!<         ZF==1 | SF!=OF (signed   <=)
    kG                    = 0x0Fu,       //!<         ZF==0 & SF==OF (signed   > )
    kNLE                  = 0x0Fu,       //!<         ZF==0 & SF==OF (signed   > )
    kCount                = 0x10u,

    kSign                 = kS,          //!< Sign.
    kNotSign              = kNS,         //!< Not Sign.

    kOverflow             = kO,          //!< Signed overflow.
    kNotOverflow          = kNO,         //!< Not signed overflow.

    kEqual                = kE,          //!< Equal      `a == b`.
    kNotEqual             = kNE,         //!< Not Equal  `a != b`.

    kSignedLT             = kL,          //!< Signed     `a <  b`.
    kSignedLE             = kLE,         //!< Signed     `a <= b`.
    kSignedGT             = kG,          //!< Signed     `a >  b`.
    kSignedGE             = kGE,         //!< Signed     `a >= b`.

    kUnsignedLT           = kB,          //!< Unsigned   `a <  b`.
    kUnsignedLE           = kBE,         //!< Unsigned   `a <= b`.
    kUnsignedGT           = kA,          //!< Unsigned   `a >  b`.
    kUnsignedGE           = kAE,         //!< Unsigned   `a >= b`.

    kZero                 = kZ,          //!< Zero flag.
    kNotZero              = kNZ,         //!< Non-zero flag.

    kNegative             = kS,          //!< Sign flag.
    kPositive             = kNS,         //!< No sign flag.

    kParityEven           = kP,          //!< Even parity flag.
    kParityOdd            = kPO          //!< Odd parity flag.
  };

  static constexpr uint8_t reverseTable[kCount] = {
    kO, kNO, kA , kBE, // O|NO|B |AE
    kE, kNE, kAE, kB , // E|NE|BE|A
    kS, kNS, kPE, kPO, // S|NS|PE|PO
    kG, kLE, kGE, kL   // L|GE|LE|G
  };

  #define ASMJIT_INST_FROM_COND(ID) \
    ID##o, ID##no, ID##b , ID##ae,  \
    ID##e, ID##ne, ID##be, ID##a ,  \
    ID##s, ID##ns, ID##pe, ID##po,  \
    ID##l, ID##ge, ID##le, ID##g
  static constexpr uint16_t jccTable[] = { ASMJIT_INST_FROM_COND(Inst::kIdJ) };
  static constexpr uint16_t setccTable[] = { ASMJIT_INST_FROM_COND(Inst::kIdSet) };
  static constexpr uint16_t cmovccTable[] = { ASMJIT_INST_FROM_COND(Inst::kIdCmov) };
  #undef ASMJIT_INST_FROM_COND

  //! Reverses a condition code (reverses the corresponding operands of a comparison).
  static constexpr uint32_t reverse(uint32_t cond) noexcept { return reverseTable[cond]; }
  //! Negates a condition code.
  static constexpr uint32_t negate(uint32_t cond) noexcept { return cond ^ 1u; }

  //! Translates a condition code `cond` to a `jcc` instruction id.
  static constexpr uint32_t toJcc(uint32_t cond) noexcept { return jccTable[cond]; }
  //! Translates a condition code `cond` to a `setcc` instruction id.
  static constexpr uint32_t toSetcc(uint32_t cond) noexcept { return setccTable[cond]; }
  //! Translates a condition code `cond` to a `cmovcc` instruction id.
  static constexpr uint32_t toCmovcc(uint32_t cond) noexcept { return cmovccTable[cond]; }
}

// ============================================================================
// [asmjit::x86::FpuWord]
// ============================================================================

//! FPU control and status words.
namespace FpuWord {
  //! FPU status word.
  enum Status : uint32_t {
    //! Invalid operation.
    kStatusInvalid        = 0x0001u,
    //! Denormalized operand.
    kStatusDenormalized   = 0x0002u,
    //! Division by zero.
    kStatusDivByZero      = 0x0004u,
    //! Overflown.
    kStatusOverflow       = 0x0008u,
    //! Underflown.
    kStatusUnderflow      = 0x0010u,
    //! Precision lost.
    kStatusPrecision      = 0x0020u,
    //! Stack fault.
    kStatusStackFault     = 0x0040u,
    //! Interrupt.
    kStatusInterrupt      = 0x0080u,
    //! C0 flag.
    kStatusC0             = 0x0100u,
    //! C1 flag.
    kStatusC1             = 0x0200u,
    //! C2 flag.
    kStatusC2             = 0x0400u,
    //! Top of the stack.
    kStatusTop            = 0x3800u,
    //! C3 flag.
    kStatusC3             = 0x4000u,
    //! FPU is busy.
    kStatusBusy           = 0x8000u
  };

  //! FPU control word.
  enum Control : uint32_t {
    // [Bits 0-5]

    //! Exception mask (0x3F).
    kControlEM_Mask       = 0x003Fu,
    //! Invalid operation exception.
    kControlEM_Invalid    = 0x0001u,
    //! Denormalized operand exception.
    kControlEM_Denormal   = 0x0002u,
    //! Division by zero exception.
    kControlEM_DivByZero  = 0x0004u,
    //! Overflow exception.
    kControlEM_Overflow   = 0x0008u,
    //! Underflow exception.
    kControlEM_Underflow  = 0x0010u,
    //! Inexact operation exception.
    kControlEM_Inexact    = 0x0020u,

    // [Bits 8-9]

    //! Precision control mask.
    kControlPC_Mask       = 0x0300u,
    //! Single precision (24 bits).
    kControlPC_Float      = 0x0000u,
    //! Reserved.
    kControlPC_Reserved   = 0x0100u,
    //! Double precision (53 bits).
    kControlPC_Double     = 0x0200u,
    //! Extended precision (64 bits).
    kControlPC_Extended   = 0x0300u,

    // [Bits 10-11]

    //! Rounding control mask.
    kControlRC_Mask       = 0x0C00u,
    //! Round to nearest even.
    kControlRC_Nearest    = 0x0000u,
    //! Round down (floor).
    kControlRC_Down       = 0x0400u,
    //! Round up (ceil).
    kControlRC_Up         = 0x0800u,
    //! Round towards zero (truncate).
    kControlRC_Truncate   = 0x0C00u,

    // [Bit 12]

    //! Infinity control.
    kControlIC_Mask       = 0x1000u,
    //! Projective (not supported on X64).
    kControlIC_Projective = 0x0000u,
    //! Affine (default).
    kControlIC_Affine     = 0x1000u
  };
}

// ============================================================================
// [asmjit::x86::Status]
// ============================================================================

//! CPU and FPU status flags.
namespace Status {
  //! CPU and FPU status flags used by `InstRWInfo`
  enum Flags : uint32_t {
    // ------------------------------------------------------------------------
    // [Architecture Neutral Flags - 0x000000FF]
    // ------------------------------------------------------------------------

    //! Carry flag.
    kCF = 0x00000001u,
    //! Signed overflow flag.
    kOF = 0x00000002u,
    //! Sign flag (negative/sign, if set).
    kSF = 0x00000004u,
    //! Zero and/or equality flag (1 if zero/equal).
    kZF = 0x00000008u,

    // ------------------------------------------------------------------------
    // [Architecture Specific Flags - 0xFFFFFF00]
    // ------------------------------------------------------------------------

    //! Adjust flag.
    kAF = 0x00000100u,
    //! Parity flag.
    kPF = 0x00000200u,
    //! Direction flag.
    kDF = 0x00000400u,
    //! Interrupt enable flag.
    kIF = 0x00000800u,

    //! Alignment check.
    kAC = 0x00001000u,

    //! FPU C0 status flag.
    kC0 = 0x00010000u,
    //! FPU C1 status flag.
    kC1 = 0x00020000u,
    //! FPU C2 status flag.
    kC2 = 0x00040000u,
    //! FPU C3 status flag.
    kC3 = 0x00080000u
  };
}

// ============================================================================
// [asmjit::x86::Predicate]
// ============================================================================

//! Contains predicates used by SIMD instructions.
namespace Predicate {
  //! A predicate used by CMP[PD|PS|SD|SS] instructions.
  enum Cmp : uint32_t {
    kCmpEQ                = 0x00u,       //!< Equal (Quiet).
    kCmpLT                = 0x01u,       //!< Less (Signaling).
    kCmpLE                = 0x02u,       //!< Less/Equal (Signaling).
    kCmpUNORD             = 0x03u,       //!< Unordered (Quiet).
    kCmpNEQ               = 0x04u,       //!< Not Equal (Quiet).
    kCmpNLT               = 0x05u,       //!< Not Less (Signaling).
    kCmpNLE               = 0x06u,       //!< Not Less/Equal (Signaling).
    kCmpORD               = 0x07u        //!< Ordered (Quiet).
  };

  //! A predicate used by [V]PCMP[I|E]STR[I|M] instructions.
  enum PCmpStr : uint32_t {
    // Source data format:
    kPCmpStrUB            = 0x00u << 0,  //!< The source data format is unsigned bytes.
    kPCmpStrUW            = 0x01u << 0,  //!< The source data format is unsigned words.
    kPCmpStrSB            = 0x02u << 0,  //!< The source data format is signed bytes.
    kPCmpStrSW            = 0x03u << 0,  //!< The source data format is signed words.

    // Aggregation operation:
    kPCmpStrEqualAny      = 0x00u << 2,  //!< The arithmetic comparison is "equal".
    kPCmpStrRanges        = 0x01u << 2,  //!< The arithmetic comparison is "greater than or equal"
                                         //!< between even indexed elements and "less than or equal"
                                         //!< between odd indexed elements.
    kPCmpStrEqualEach     = 0x02u << 2,  //!< The arithmetic comparison is "equal".
    kPCmpStrEqualOrdered  = 0x03u << 2,  //!< The arithmetic comparison is "equal".

    // Polarity:
    kPCmpStrPosPolarity   = 0x00u << 4,  //!< IntRes2 = IntRes1.
    kPCmpStrNegPolarity   = 0x01u << 4,  //!< IntRes2 = -1 XOR IntRes1.
    kPCmpStrPosMasked     = 0x02u << 4,  //!< IntRes2 = IntRes1.
    kPCmpStrNegMasked     = 0x03u << 4,  //!< IntRes2[i] = second[i] == invalid ? IntRes1[i] : ~IntRes1[i].

    // Output selection (pcmpstri):
    kPCmpStrOutputLSI     = 0x00u << 6,  //!< The index returned to ECX is of the least significant set bit in IntRes2.
    kPCmpStrOutputMSI     = 0x01u << 6,  //!< The index returned to ECX is of the most significant set bit in IntRes2.

    // Output selection (pcmpstrm):
    kPCmpStrBitMask       = 0x00u << 6,  //!< IntRes2 is returned as the mask to the least significant bits of XMM0.
    kPCmpStrIndexMask     = 0x01u << 6   //!< IntRes2 is expanded into a byte/word mask and placed in XMM0.
  };

  //! A predicate used by ROUND[PD|PS|SD|SS] instructions.
  enum Round : uint32_t {
    //! Round to nearest (even).
    kRoundNearest = 0x00u,
    //! Round to down toward -INF (floor),
    kRoundDown = 0x01u,
    //! Round to up toward +INF (ceil).
    kRoundUp = 0x02u,
    //! Round toward zero (truncate).
    kRoundTrunc = 0x03u,
    //! Round to the current rounding mode set (ignores other RC bits).
    kRoundCurrent = 0x04u,
    //! Avoids inexact exception, if set.
    kRoundInexact = 0x08u
  };

  //! A predicate used by VCMP[PD|PS|SD|SS] instructions.
  //!
  //! The first 8 values are compatible with `Cmp`.
  enum VCmp : uint32_t {
    kVCmpEQ_OQ            = kCmpEQ,      //!< Equal             (Quiet    , Ordered).
    kVCmpLT_OS            = kCmpLT,      //!< Less              (Signaling, Ordered).
    kVCmpLE_OS            = kCmpLE,      //!< Less/Equal        (Signaling, Ordered).
    kVCmpUNORD_Q          = kCmpUNORD,   //!< Unordered         (Quiet).
    kVCmpNEQ_UQ           = kCmpNEQ,     //!< Not Equal         (Quiet    , Unordered).
    kVCmpNLT_US           = kCmpNLT,     //!< Not Less          (Signaling, Unordered).
    kVCmpNLE_US           = kCmpNLE,     //!< Not Less/Equal    (Signaling, Unordered).
    kVCmpORD_Q            = kCmpORD,     //!< Ordered           (Quiet).
    kVCmpEQ_UQ            = 0x08u,       //!< Equal             (Quiet    , Unordered).
    kVCmpNGE_US           = 0x09u,       //!< Not Greater/Equal (Signaling, Unordered).
    kVCmpNGT_US           = 0x0Au,       //!< Not Greater       (Signaling, Unordered).
    kVCmpFALSE_OQ         = 0x0Bu,       //!< False             (Quiet    , Ordered).
    kVCmpNEQ_OQ           = 0x0Cu,       //!< Not Equal         (Quiet    , Ordered).
    kVCmpGE_OS            = 0x0Du,       //!< Greater/Equal     (Signaling, Ordered).
    kVCmpGT_OS            = 0x0Eu,       //!< Greater           (Signaling, Ordered).
    kVCmpTRUE_UQ          = 0x0Fu,       //!< True              (Quiet    , Unordered).
    kVCmpEQ_OS            = 0x10u,       //!< Equal             (Signaling, Ordered).
    kVCmpLT_OQ            = 0x11u,       //!< Less              (Quiet    , Ordered).
    kVCmpLE_OQ            = 0x12u,       //!< Less/Equal        (Quiet    , Ordered).
    kVCmpUNORD_S          = 0x13u,       //!< Unordered         (Signaling).
    kVCmpNEQ_US           = 0x14u,       //!< Not Equal         (Signaling, Unordered).
    kVCmpNLT_UQ           = 0x15u,       //!< Not Less          (Quiet    , Unordered).
    kVCmpNLE_UQ           = 0x16u,       //!< Not Less/Equal    (Quiet    , Unordered).
    kVCmpORD_S            = 0x17u,       //!< Ordered           (Signaling).
    kVCmpEQ_US            = 0x18u,       //!< Equal             (Signaling, Unordered).
    kVCmpNGE_UQ           = 0x19u,       //!< Not Greater/Equal (Quiet    , Unordered).
    kVCmpNGT_UQ           = 0x1Au,       //!< Not Greater       (Quiet    , Unordered).
    kVCmpFALSE_OS         = 0x1Bu,       //!< False             (Signaling, Ordered).
    kVCmpNEQ_OS           = 0x1Cu,       //!< Not Equal         (Signaling, Ordered).
    kVCmpGE_OQ            = 0x1Du,       //!< Greater/Equal     (Quiet    , Ordered).
    kVCmpGT_OQ            = 0x1Eu,       //!< Greater           (Quiet    , Ordered).
    kVCmpTRUE_US          = 0x1Fu        //!< True              (Signaling, Unordered).
  };

  //! A predicate used by VFIXUPIMM[PD|PS|SD|SS] instructions (AVX-512).
  enum VFixupImm : uint32_t {
    kVFixupImmZEOnZero    = 0x01u,
    kVFixupImmIEOnZero    = 0x02u,
    kVFixupImmZEOnOne     = 0x04u,
    kVFixupImmIEOnOne     = 0x08u,
    kVFixupImmIEOnSNaN    = 0x10u,
    kVFixupImmIEOnNInf    = 0x20u,
    kVFixupImmIEOnNegative= 0x40u,
    kVFixupImmIEOnPInf    = 0x80u
  };

  //! A predicate used by VFPCLASS[PD|PS|SD|SS] instructions (AVX-512).
  //!
  //! \note Values can be combined together to form the final 8-bit mask.
  enum VFPClass : uint32_t {
    kVFPClassQNaN         = 0x01u,       //!< Checks for QNaN.
    kVFPClassPZero        = 0x02u,       //!< Checks for +0.
    kVFPClassNZero        = 0x04u,       //!< Checks for -0.
    kVFPClassPInf         = 0x08u,       //!< Checks for +Inf.
    kVFPClassNInf         = 0x10u,       //!< Checks for -Inf.
    kVFPClassDenormal     = 0x20u,       //!< Checks for denormal.
    kVFPClassNegative     = 0x40u,       //!< Checks for negative finite value.
    kVFPClassSNaN         = 0x80u        //!< Checks for SNaN.
  };

  //! A predicate used by VGETMANT[PD|PS|SD|SS] instructions (AVX-512).
  enum VGetMant : uint32_t {
    kVGetMant1To2         = 0x00u,
    kVGetMant1Div2To2     = 0x01u,
    kVGetMant1Div2To1     = 0x02u,
    kVGetMant3Div4To3Div2 = 0x03u,
    kVGetMantNoSign       = 0x04u,
    kVGetMantQNaNIfSign   = 0x08u
  };

  //! A predicate used by VPCMP[U][B|W|D|Q] instructions (AVX-512).
  enum VPCmp : uint32_t {
    kVPCmpEQ              = 0x00u,       //!< Equal.
    kVPCmpLT              = 0x01u,       //!< Less.
    kVPCmpLE              = 0x02u,       //!< Less/Equal.
    kVPCmpFALSE           = 0x03u,       //!< False.
    kVPCmpNE              = 0x04u,       //!< Not Equal.
    kVPCmpGE              = 0x05u,       //!< Greater/Equal.
    kVPCmpGT              = 0x06u,       //!< Greater.
    kVPCmpTRUE            = 0x07u        //!< True.
  };

  //! A predicate used by VPCOM[U][B|W|D|Q] instructions (XOP).
  enum VPCom : uint32_t {
    kVPComLT              = 0x00u,       //!< Less.
    kVPComLE              = 0x01u,       //!< Less/Equal
    kVPComGT              = 0x02u,       //!< Greater.
    kVPComGE              = 0x03u,       //!< Greater/Equal.
    kVPComEQ              = 0x04u,       //!< Equal.
    kVPComNE              = 0x05u,       //!< Not Equal.
    kVPComFALSE           = 0x06u,       //!< False.
    kVPComTRUE            = 0x07u        //!< True.
  };

  //! A predicate used by VRANGE[PD|PS|SD|SS] instructions (AVX-512).
  enum VRange : uint32_t {
    kVRangeSelectMin      = 0x00u,       //!< Select minimum value.
    kVRangeSelectMax      = 0x01u,       //!< Select maximum value.
    kVRangeSelectAbsMin   = 0x02u,       //!< Select minimum absolute value.
    kVRangeSelectAbsMax   = 0x03u,       //!< Select maximum absolute value.
    kVRangeSignSrc1       = 0x00u,       //!< Select sign of SRC1.
    kVRangeSignSrc2       = 0x04u,       //!< Select sign of SRC2.
    kVRangeSign0          = 0x08u,       //!< Set sign to 0.
    kVRangeSign1          = 0x0Cu        //!< Set sign to 1.
  };

  //! A predicate used by VREDUCE[PD|PS|SD|SS] instructions (AVX-512).
  enum VReduce : uint32_t {
    kVReduceRoundCurrent  = 0x00u,       //!< Round to the current mode set.
    kVReduceRoundEven     = 0x04u,       //!< Round to nearest even.
    kVReduceRoundDown     = 0x05u,       //!< Round down.
    kVReduceRoundUp       = 0x06u,       //!< Round up.
    kVReduceRoundTrunc    = 0x07u,       //!< Truncate.
    kVReduceSuppress      = 0x08u        //!< Suppress exceptions.
  };

  //! Pack a shuffle constant to be used by SSE/AVX/AVX-512 instructions (2 values).
  //!
  //! \param a Position of the first  component [0, 1].
  //! \param b Position of the second component [0, 1].
  //!
  //! Shuffle constants can be used to encode an immediate for these instructions:
  //!   - `shufpd|vshufpd`
  static constexpr uint32_t shuf(uint32_t a, uint32_t b) noexcept {
    return (a << 1) | b;
  }

  //! Pack a shuffle constant to be used by SSE/AVX/AVX-512 instructions (4 values).
  //!
  //! \param a Position of the first  component [0, 3].
  //! \param b Position of the second component [0, 3].
  //! \param c Position of the third  component [0, 3].
  //! \param d Position of the fourth component [0, 3].
  //!
  //! Shuffle constants can be used to encode an immediate for these instructions:
  //!   - `pshufw`
  //!   - `pshuflw|vpshuflw`
  //!   - `pshufhw|vpshufhw`
  //!   - `pshufd|vpshufd`
  //!   - `shufps|vshufps`
  static constexpr uint32_t shuf(uint32_t a, uint32_t b, uint32_t c, uint32_t d) noexcept {
    return (a << 6) | (b << 4) | (c << 2) | d;
  }
}

// ============================================================================
// [asmjit::x86::TLog]
// ============================================================================

//! Bitwise ternary logic between 3 operands introduced by AVX-512.
namespace TLog {
  //! A predicate that can be used to create a common predicate for VPTERNLOG[D|Q].
  //!
  //! There are 3 inputs to the instruction (\ref kA, \ref kB, \ref kC), and
  //! ternary logic can define any combination that would be performed on these
  //! 3 inputs to get the desired output - any combination of AND, OR, XOR, NOT.
  enum Operator : uint32_t {
    //! 0 value.
    k0 = 0x00u,
    //! 1 value.
    k1 = 0xFFu,
    //! A value.
    kA = 0xF0u,
    //! B value.
    kB = 0xCCu,
    //! C value.
    kC = 0xAAu,

    //! `!A` expression.
    kNotA = kA ^ k1,
    //! `!B` expression.
    kNotB = kB ^ k1,
    //! `!C` expression.
    kNotC = kC ^ k1,

    //! `A & B` expression.
    kAB = kA & kB,
    //! `A & C` expression.
    kAC = kA & kC,
    //! `B & C` expression.
    kBC = kB & kC,
    //! `!(A & B)` expression.
    kNotAB = kAB ^ k1,
    //! `!(A & C)` expression.
    kNotAC = kAC ^ k1,
    //! `!(B & C)` expression.
    kNotBC = kBC ^ k1,

    //! `A & B & C` expression.
    kABC = kAB & kC,
    //! `!(A & B & C)` expression.
    kNotABC = kABC ^ k1
  };

  //! Creates an immediate that can be used by VPTERNLOG[D|Q] instructions.
  static constexpr uint32_t make(uint32_t b000, uint32_t b001, uint32_t b010, uint32_t b011, uint32_t b100, uint32_t b101, uint32_t b110, uint32_t b111) noexcept {
    return (b000 << 0) | (b001 << 1) | (b010 << 2) | (b011 << 3) | (b100 << 4) | (b101 << 5) | (b110 << 6) | (b111 << 7);
  }

  //! Creates an immediate that can be used by VPTERNLOG[D|Q] instructions.
  static constexpr uint32_t value(uint32_t x) noexcept { return x & 0xFF; }
  //! Negate an immediate that can be used by VPTERNLOG[D|Q] instructions.
  static constexpr uint32_t negate(uint32_t x) noexcept { return x ^ 0xFF; }
  //! Creates an if/else logic that can be used by VPTERNLOG[D|Q] instructions.
  static constexpr uint32_t ifElse(uint32_t condition, uint32_t a, uint32_t b) noexcept { return (condition & a) | (negate(condition) & b); }
}

//! \}

ASMJIT_END_SUB_NAMESPACE

#endif // ASMJIT_X86_X86GLOBALS_H_INCLUDED
