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

#ifndef ASMJIT_X86_X86FEATURES_H_INCLUDED
#define ASMJIT_X86_X86FEATURES_H_INCLUDED

#include "../core/features.h"

ASMJIT_BEGIN_SUB_NAMESPACE(x86)

//! \addtogroup asmjit_x86
//! \{

// ============================================================================
// [asmjit::x86::Features]
// ============================================================================

//! CPU features (X86).
class Features : public BaseFeatures {
public:
  //! CPU feature ID.
  enum Id : uint32_t {
    // @EnumValuesBegin{"enum": "x86::Features::Id"}@

    kNone = 0,                 //!< No feature (never set, used internally).

    kMT,                       //!< CPU has multi-threading capabilities.
    kNX,                       //!< CPU has Not-Execute-Bit aka DEP (data-execution prevention).

    k3DNOW,                    //!< CPU has 3DNOW            (3DNOW base instructions) [AMD].
    k3DNOW2,                   //!< CPU has 3DNOW2           (enhanced 3DNOW) [AMD].
    kADX,                      //!< CPU has ADX              (multi-precision add-carry instruction extensions).
    kAESNI,                    //!< CPU has AESNI            (AES encode/decode instructions).
    kALTMOVCR8,                //!< CPU has LOCK MOV R<->CR0 (supports `MOV R<->CR8` via `LOCK MOV R<->CR0` in 32-bit mode) [AMD].
    kAMX_BF16,                 //!< CPU has AMX_BF16         (advanced matrix extensions - BF16 instructions).
    kAMX_INT8,                 //!< CPU has AMX_INT8         (advanced matrix extensions - INT8 instructions).
    kAMX_TILE,                 //!< CPU has AMX_TILE         (advanced matrix extensions).
    kAVX,                      //!< CPU has AVX              (advanced vector extensions).
    kAVX2,                     //!< CPU has AVX2             (advanced vector extensions 2).
    kAVX512_4FMAPS,            //!< CPU has AVX512_FMAPS     (FMA packed single).
    kAVX512_4VNNIW,            //!< CPU has AVX512_VNNIW     (vector NN instructions word variable precision).
    kAVX512_BF16,              //!< CPU has AVX512_BF16      (BFLOAT16 support instruction).
    kAVX512_BITALG,            //!< CPU has AVX512_BITALG    (VPOPCNT[B|W], VPSHUFBITQMB).
    kAVX512_BW,                //!< CPU has AVX512_BW        (packed BYTE|WORD).
    kAVX512_CDI,               //!< CPU has AVX512_CDI       (conflict detection).
    kAVX512_DQ,                //!< CPU has AVX512_DQ        (packed DWORD|QWORD).
    kAVX512_ERI,               //!< CPU has AVX512_ERI       (exponential and reciprocal).
    kAVX512_F,                 //!< CPU has AVX512_F         (AVX512 foundation).
    kAVX512_IFMA,              //!< CPU has AVX512_IFMA      (integer fused-multiply-add using 52-bit precision).
    kAVX512_PFI,               //!< CPU has AVX512_PFI       (prefetch instructions).
    kAVX512_VBMI,              //!< CPU has AVX512_VBMI      (vector byte manipulation).
    kAVX512_VBMI2,             //!< CPU has AVX512_VBMI2     (vector byte manipulation 2).
    kAVX512_VL,                //!< CPU has AVX512_VL        (vector length extensions).
    kAVX512_VNNI,              //!< CPU has AVX512_VNNI      (vector neural network instructions).
    kAVX512_VP2INTERSECT,      //!< CPU has AVX512_VP2INTERSECT
    kAVX512_VPOPCNTDQ,         //!< CPU has AVX512_VPOPCNTDQ (VPOPCNT[D|Q] instructions).
    kAVX_VNNI,                 //!< CPU has AVX_VNNI         (VEX encoding of vpdpbusd/vpdpbusds/vpdpwssd/vpdpwssds).
    kBMI,                      //!< CPU has BMI              (bit manipulation instructions #1).
    kBMI2,                     //!< CPU has BMI2             (bit manipulation instructions #2).
    kCET_IBT,                  //!< CPU has CET-IBT          (indirect branch tracking).
    kCET_SS,                   //!< CPU has CET-SS.
    kCLDEMOTE,                 //!< CPU has CLDEMOTE         (cache line demote).
    kCLFLUSH,                  //!< CPU has CLFUSH           (Cache Line flush).
    kCLFLUSHOPT,               //!< CPU has CLFUSHOPT        (Cache Line flush - optimized).
    kCLWB,                     //!< CPU has CLWB.
    kCLZERO,                   //!< CPU has CLZERO.
    kCMOV,                     //!< CPU has CMOV             (CMOV and FCMOV instructions).
    kCMPXCHG16B,               //!< CPU has CMPXCHG16B       (compare-exchange 16 bytes) [X86_64].
    kCMPXCHG8B,                //!< CPU has CMPXCHG8B        (compare-exchange 8 bytes).
    kENCLV,                    //!< CPU has ENCLV.
    kENQCMD,                   //!< CPU has ENQCMD           (enqueue stores).
    kERMS,                     //!< CPU has ERMS             (enhanced REP MOVSB/STOSB).
    kF16C,                     //!< CPU has F16C.
    kFMA,                      //!< CPU has FMA              (fused-multiply-add 3 operand form).
    kFMA4,                     //!< CPU has FMA4             (fused-multiply-add 4 operand form).
    kFPU,                      //!< CPU has FPU              (FPU support).
    kFSGSBASE,                 //!< CPU has FSGSBASE.
    kFXSR,                     //!< CPU has FXSR             (FXSAVE/FXRSTOR instructions).
    kFXSROPT,                  //!< CPU has FXSROTP          (FXSAVE/FXRSTOR is optimized).
    kGEODE,                    //!< CPU has GEODE extensions (3DNOW additions).
    kGFNI,                     //!< CPU has GFNI             (Galois field instructions).
    kHLE,                      //!< CPU has HLE.
    kHRESET,                   //!< CPU has HRESET.
    kI486,                     //!< CPU has I486 features    (I486+ support).
    kLAHFSAHF,                 //!< CPU has LAHF/SAHF        (LAHF/SAHF in 64-bit mode) [X86_64].
    kLWP,                      //!< CPU has LWP              (lightweight profiling) [AMD].
    kLZCNT,                    //!< CPU has LZCNT            (LZCNT instruction).
    kMCOMMIT,                  //!< CPU has MCOMMIT          (MCOMMIT instruction).
    kMMX,                      //!< CPU has MMX              (MMX base instructions).
    kMMX2,                     //!< CPU has MMX2             (MMX extensions or MMX2).
    kMONITOR,                  //!< CPU has MONITOR          (MONITOR/MWAIT instructions).
    kMONITORX,                 //!< CPU has MONITORX         (MONITORX/MWAITX instructions).
    kMOVBE,                    //!< CPU has MOVBE            (move with byte-order swap).
    kMOVDIR64B,                //!< CPU has MOVDIR64B        (move 64 bytes as direct store).
    kMOVDIRI,                  //!< CPU has MOVDIRI          (move dword/qword as direct store).
    kMPX,                      //!< CPU has MPX              (memory protection extensions).
    kMSR,                      //!< CPU has MSR              (RDMSR/WRMSR instructions).
    kMSSE,                     //!< CPU has MSSE             (misaligned SSE support).
    kOSXSAVE,                  //!< CPU has OSXSAVE          (XSAVE enabled by OS).
    kOSPKE,                    //!< CPU has OSPKE            (PKE enabled by OS).
    kPCLMULQDQ,                //!< CPU has PCLMULQDQ        (packed carry-less multiplication).
    kPCONFIG,                  //!< CPU has PCONFIG          (PCONFIG instruction).
    kPOPCNT,                   //!< CPU has POPCNT           (POPCNT instruction).
    kPREFETCHW,                //!< CPU has PREFETCHW.
    kPREFETCHWT1,              //!< CPU has PREFETCHWT1.
    kPTWRITE,                  //!< CPU has PTWRITE.
    kRDPID,                    //!< CPU has RDPID.
    kRDPRU,                    //!< CPU has RDPRU.
    kRDRAND,                   //!< CPU has RDRAND.
    kRDSEED,                   //!< CPU has RDSEED.
    kRDTSC,                    //!< CPU has RDTSC.
    kRDTSCP,                   //!< CPU has RDTSCP.
    kRTM,                      //!< CPU has RTM.
    kSERIALIZE,                //!< CPU has SERIALIZE.
    kSHA,                      //!< CPU has SHA              (SHA-1 and SHA-256 instructions).
    kSKINIT,                   //!< CPU has SKINIT           (SKINIT/STGI instructions) [AMD].
    kSMAP,                     //!< CPU has SMAP             (supervisor-mode access prevention).
    kSMEP,                     //!< CPU has SMEP             (supervisor-mode execution prevention).
    kSMX,                      //!< CPU has SMX              (safer mode extensions).
    kSNP,                      //!< CPU has SNP.
    kSSE,                      //!< CPU has SSE.
    kSSE2,                     //!< CPU has SSE2.
    kSSE3,                     //!< CPU has SSE3.
    kSSE4_1,                   //!< CPU has SSE4.1.
    kSSE4_2,                   //!< CPU has SSE4.2.
    kSSE4A,                    //!< CPU has SSE4A [AMD].
    kSSSE3,                    //!< CPU has SSSE3.
    kSVM,                      //!< CPU has SVM              (virtualization) [AMD].
    kTBM,                      //!< CPU has TBM              (trailing bit manipulation) [AMD].
    kTSX,                      //!< CPU has TSX.
    kTSXLDTRK,                 //!< CPU has TSXLDTRK.
    kUINTR,                    //!< CPU has UINTR            (user interrupts).
    kVAES,                     //!< CPU has VAES             (vector AES 256|512 bit support).
    kVMX,                      //!< CPU has VMX              (virtualization) [INTEL].
    kVPCLMULQDQ,               //!< CPU has VPCLMULQDQ       (vector PCLMULQDQ 256|512-bit support).
    kWAITPKG,                  //!< CPU has WAITPKG          (UMONITOR, UMWAIT, TPAUSE).
    kWBNOINVD,                 //!< CPU has WBNOINVD.
    kXOP,                      //!< CPU has XOP              (XOP instructions) [AMD].
    kXSAVE,                    //!< CPU has XSAVE.
    kXSAVEC,                   //!< CPU has XSAVEC.
    kXSAVEOPT,                 //!< CPU has XSAVEOPT.
    kXSAVES,                   //!< CPU has XSAVES.

    // @EnumValuesEnd@

    kCount                     //!< Count of X86 CPU features.
  };

  //! \name Construction / Destruction
  //! \{

  inline Features() noexcept
    : BaseFeatures() {}

  inline Features(const Features& other) noexcept
    : BaseFeatures(other) {}

  //! \}

  //! \name Overloaded Operators
  //! \{

  inline Features& operator=(const Features& other) noexcept = default;

  //! \}

  //! \name Accessors
  //! \{

  #define ASMJIT_X86_FEATURE(FEATURE) \
    inline bool has##FEATURE() const noexcept { return has(k##FEATURE); }

  ASMJIT_X86_FEATURE(MT)
  ASMJIT_X86_FEATURE(NX)

  ASMJIT_X86_FEATURE(3DNOW)
  ASMJIT_X86_FEATURE(3DNOW2)
  ASMJIT_X86_FEATURE(ADX)
  ASMJIT_X86_FEATURE(AESNI)
  ASMJIT_X86_FEATURE(ALTMOVCR8)
  ASMJIT_X86_FEATURE(AMX_BF16)
  ASMJIT_X86_FEATURE(AMX_INT8)
  ASMJIT_X86_FEATURE(AMX_TILE)
  ASMJIT_X86_FEATURE(AVX)
  ASMJIT_X86_FEATURE(AVX2)
  ASMJIT_X86_FEATURE(AVX512_4FMAPS)
  ASMJIT_X86_FEATURE(AVX512_4VNNIW)
  ASMJIT_X86_FEATURE(AVX512_BF16)
  ASMJIT_X86_FEATURE(AVX512_BITALG)
  ASMJIT_X86_FEATURE(AVX512_BW)
  ASMJIT_X86_FEATURE(AVX512_CDI)
  ASMJIT_X86_FEATURE(AVX512_DQ)
  ASMJIT_X86_FEATURE(AVX512_ERI)
  ASMJIT_X86_FEATURE(AVX512_F)
  ASMJIT_X86_FEATURE(AVX512_IFMA)
  ASMJIT_X86_FEATURE(AVX512_PFI)
  ASMJIT_X86_FEATURE(AVX512_VBMI)
  ASMJIT_X86_FEATURE(AVX512_VBMI2)
  ASMJIT_X86_FEATURE(AVX512_VL)
  ASMJIT_X86_FEATURE(AVX512_VNNI)
  ASMJIT_X86_FEATURE(AVX512_VP2INTERSECT)
  ASMJIT_X86_FEATURE(AVX512_VPOPCNTDQ)
  ASMJIT_X86_FEATURE(AVX_VNNI)
  ASMJIT_X86_FEATURE(BMI)
  ASMJIT_X86_FEATURE(BMI2)
  ASMJIT_X86_FEATURE(CET_IBT)
  ASMJIT_X86_FEATURE(CET_SS)
  ASMJIT_X86_FEATURE(CLDEMOTE)
  ASMJIT_X86_FEATURE(CLFLUSH)
  ASMJIT_X86_FEATURE(CLFLUSHOPT)
  ASMJIT_X86_FEATURE(CLWB)
  ASMJIT_X86_FEATURE(CLZERO)
  ASMJIT_X86_FEATURE(CMOV)
  ASMJIT_X86_FEATURE(CMPXCHG16B)
  ASMJIT_X86_FEATURE(CMPXCHG8B)
  ASMJIT_X86_FEATURE(ENCLV)
  ASMJIT_X86_FEATURE(ENQCMD)
  ASMJIT_X86_FEATURE(ERMS)
  ASMJIT_X86_FEATURE(F16C)
  ASMJIT_X86_FEATURE(FMA)
  ASMJIT_X86_FEATURE(FMA4)
  ASMJIT_X86_FEATURE(FPU)
  ASMJIT_X86_FEATURE(FSGSBASE)
  ASMJIT_X86_FEATURE(FXSR)
  ASMJIT_X86_FEATURE(FXSROPT)
  ASMJIT_X86_FEATURE(GEODE)
  ASMJIT_X86_FEATURE(GFNI)
  ASMJIT_X86_FEATURE(HLE)
  ASMJIT_X86_FEATURE(HRESET)
  ASMJIT_X86_FEATURE(I486)
  ASMJIT_X86_FEATURE(LAHFSAHF)
  ASMJIT_X86_FEATURE(LWP)
  ASMJIT_X86_FEATURE(LZCNT)
  ASMJIT_X86_FEATURE(MCOMMIT)
  ASMJIT_X86_FEATURE(MMX)
  ASMJIT_X86_FEATURE(MMX2)
  ASMJIT_X86_FEATURE(MONITOR)
  ASMJIT_X86_FEATURE(MONITORX)
  ASMJIT_X86_FEATURE(MOVBE)
  ASMJIT_X86_FEATURE(MOVDIR64B)
  ASMJIT_X86_FEATURE(MOVDIRI)
  ASMJIT_X86_FEATURE(MPX)
  ASMJIT_X86_FEATURE(MSR)
  ASMJIT_X86_FEATURE(MSSE)
  ASMJIT_X86_FEATURE(OSXSAVE)
  ASMJIT_X86_FEATURE(PCLMULQDQ)
  ASMJIT_X86_FEATURE(PCONFIG)
  ASMJIT_X86_FEATURE(POPCNT)
  ASMJIT_X86_FEATURE(PREFETCHW)
  ASMJIT_X86_FEATURE(PREFETCHWT1)
  ASMJIT_X86_FEATURE(PTWRITE)
  ASMJIT_X86_FEATURE(RDPID)
  ASMJIT_X86_FEATURE(RDPRU)
  ASMJIT_X86_FEATURE(RDRAND)
  ASMJIT_X86_FEATURE(RDSEED)
  ASMJIT_X86_FEATURE(RDTSC)
  ASMJIT_X86_FEATURE(RDTSCP)
  ASMJIT_X86_FEATURE(RTM)
  ASMJIT_X86_FEATURE(SERIALIZE)
  ASMJIT_X86_FEATURE(SHA)
  ASMJIT_X86_FEATURE(SKINIT)
  ASMJIT_X86_FEATURE(SMAP)
  ASMJIT_X86_FEATURE(SMEP)
  ASMJIT_X86_FEATURE(SMX)
  ASMJIT_X86_FEATURE(SNP)
  ASMJIT_X86_FEATURE(SSE)
  ASMJIT_X86_FEATURE(SSE2)
  ASMJIT_X86_FEATURE(SSE3)
  ASMJIT_X86_FEATURE(SSSE3)
  ASMJIT_X86_FEATURE(SSE4A)
  ASMJIT_X86_FEATURE(SSE4_1)
  ASMJIT_X86_FEATURE(SSE4_2)
  ASMJIT_X86_FEATURE(SVM)
  ASMJIT_X86_FEATURE(TBM)
  ASMJIT_X86_FEATURE(TSX)
  ASMJIT_X86_FEATURE(TSXLDTRK)
  ASMJIT_X86_FEATURE(UINTR)
  ASMJIT_X86_FEATURE(XSAVE)
  ASMJIT_X86_FEATURE(XSAVEC)
  ASMJIT_X86_FEATURE(XSAVEOPT)
  ASMJIT_X86_FEATURE(XSAVES)
  ASMJIT_X86_FEATURE(VAES)
  ASMJIT_X86_FEATURE(VMX)
  ASMJIT_X86_FEATURE(VPCLMULQDQ)
  ASMJIT_X86_FEATURE(WAITPKG)
  ASMJIT_X86_FEATURE(WBNOINVD)
  ASMJIT_X86_FEATURE(XOP)

  #undef ASMJIT_X86_FEATURE

  //! \}
};

//! \}

ASMJIT_END_SUB_NAMESPACE

#endif // ASMJIT_X86_X86FEATURES_H_INCLUDED
