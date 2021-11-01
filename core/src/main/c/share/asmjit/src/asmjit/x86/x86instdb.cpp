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

// ----------------------------------------------------------------------------
// IMPORTANT: AsmJit now uses an external instruction database to populate
// static tables within this file. Perform the following steps to regenerate
// all tables enclosed by ${...}:
//
//   1. Install node.js environment <https://nodejs.org>
//   2. Go to asmjit/tools directory
//   3. Get the latest asmdb from <https://github.com/asmjit/asmdb> and
//      copy/link the `asmdb` directory to `asmjit/tools/asmdb`.
//   4. Execute `node tablegen-x86.js`
//
// Instruction encoding and opcodes were added to the `x86inst.cpp` database
// manually in the past and they are not updated by the script as it became
// tricky. However, everything else is updated including instruction operands
// and tables required to validate them, instruction read/write information
// (including registers and flags), and all indexes to all tables.
// ----------------------------------------------------------------------------

#include "../core/api-build_p.h"
#if !defined(ASMJIT_NO_X86)

#include "../core/cpuinfo.h"
#include "../core/misc_p.h"
#include "../core/support.h"
#include "../x86/x86features.h"
#include "../x86/x86instdb_p.h"
#include "../x86/x86opcode_p.h"
#include "../x86/x86operand.h"

ASMJIT_BEGIN_SUB_NAMESPACE(x86)

// ============================================================================
// [asmjit::x86::InstDB - InstInfo]
// ============================================================================

// Instruction opcode definitions:
//   - `O` encodes X86|MMX|SSE instructions.
//   - `V` encodes VEX|XOP|EVEX instructions.
//   - `E` encodes EVEX instructions only.
#define O_ENCODE(VEX, PREFIX, OPCODE, O, L, W, EvexW, N, TT) \
  ((PREFIX) | (OPCODE) | (O) | (L) | (W) | (EvexW) | (N) | (TT) | \
   (VEX && ((PREFIX) & Opcode::kMM_Mask) != Opcode::kMM_0F ? int(Opcode::kMM_ForceVex3) : 0))

#define O(PREFIX, OPCODE, ModO, LL, W, EvexW, N, ModRM) (O_ENCODE(0, Opcode::k##PREFIX, 0x##OPCODE, Opcode::kModO_##ModO, Opcode::kLL_##LL, Opcode::kW_##W, Opcode::kEvex_W_##EvexW, Opcode::kCDSHL_##N, Opcode::kModRM_##ModRM))
#define V(PREFIX, OPCODE, ModO, LL, W, EvexW, N, TT) (O_ENCODE(1, Opcode::k##PREFIX, 0x##OPCODE, Opcode::kModO_##ModO, Opcode::kLL_##LL, Opcode::kW_##W, Opcode::kEvex_W_##EvexW, Opcode::kCDSHL_##N, Opcode::kCDTT_##TT))
#define E(PREFIX, OPCODE, ModO, LL, W, EvexW, N, TT) (O_ENCODE(1, Opcode::k##PREFIX, 0x##OPCODE, Opcode::kModO_##ModO, Opcode::kLL_##LL, Opcode::kW_##W, Opcode::kEvex_W_##EvexW, Opcode::kCDSHL_##N, Opcode::kCDTT_##TT) | Opcode::kMM_ForceEvex)
#define O_FPU(PREFIX, OPCODE, ModO) (Opcode::kFPU_##PREFIX | (0x##OPCODE & 0xFFu) | ((0x##OPCODE >> 8) << Opcode::kFPU_2B_Shift) | Opcode::kModO_##ModO)

// Don't store `_nameDataIndex` if instruction names are disabled. Since some
// APIs can use `_nameDataIndex` it's much safer if it's zero if it's not defined.
#ifndef ASMJIT_NO_TEXT
  #define NAME_DATA_INDEX(Index) Index
#else
  #define NAME_DATA_INDEX(Index) 0
#endif

// Defines an X86 instruction.
#define INST(id, encoding, opcode0, opcode1, mainOpcodeIndex, altOpcodeIndex, nameDataIndex, commomInfoIndexA, commomInfoIndexB) { \
  uint32_t(NAME_DATA_INDEX(nameDataIndex)), \
  uint32_t(commomInfoIndexA),               \
  uint32_t(commomInfoIndexB),               \
  uint8_t(InstDB::kEncoding##encoding),     \
  uint8_t((opcode0) & 0xFFu),               \
  uint8_t(mainOpcodeIndex),                 \
  uint8_t(altOpcodeIndex)                   \
}

const InstDB::InstInfo InstDB::_instInfoTable[] = {
  /*--------------------+--------------------+------------------+--------+------------------+--------+----+----+------+----+----+
  |    Instruction      |    Instruction     |    Main Opcode   |  EVEX  |Alternative Opcode|  EVEX  |Op0X|Op1X|Name-X|IdxA|IdxB|
  |     Id & Name       |      Encoding      |  (pp+mmm|op/o|L|w|W|N|TT.)|--(pp+mmm|op/o|L|w|W|N|TT.)|     (auto-generated)     |
  +---------------------+--------------------+---------+----+-+-+-+-+----+---------+----+-+-+-+-+----+----+----+------+----+---*/
  // ${InstInfo:Begin}
  INST(None             , None               , 0                         , 0                         , 0  , 0  , 0    , 0  , 0  ), // #0
  INST(Aaa              , X86Op_xAX          , O(000000,37,_,_,_,_,_,_  ), 0                         , 0  , 0  , 1    , 1  , 1  ), // #1
  INST(Aad              , X86I_xAX           , O(000000,D5,_,_,_,_,_,_  ), 0                         , 0  , 0  , 5    , 2  , 1  ), // #2
  INST(Aam              , X86I_xAX           , O(000000,D4,_,_,_,_,_,_  ), 0                         , 0  , 0  , 9    , 2  , 1  ), // #3
  INST(Aas              , X86Op_xAX          , O(000000,3F,_,_,_,_,_,_  ), 0                         , 0  , 0  , 13   , 1  , 1  ), // #4
  INST(Adc              , X86Arith           , O(000000,10,2,_,x,_,_,_  ), 0                         , 1  , 0  , 17   , 3  , 2  ), // #5
  INST(Adcx             , X86Rm              , O(660F38,F6,_,_,x,_,_,_  ), 0                         , 2  , 0  , 21   , 4  , 3  ), // #6
  INST(Add              , X86Arith           , O(000000,00,0,_,x,_,_,_  ), 0                         , 0  , 0  , 3146 , 3  , 1  ), // #7
  INST(Addpd            , ExtRm              , O(660F00,58,_,_,_,_,_,_  ), 0                         , 3  , 0  , 5106 , 5  , 4  ), // #8
  INST(Addps            , ExtRm              , O(000F00,58,_,_,_,_,_,_  ), 0                         , 4  , 0  , 5118 , 5  , 5  ), // #9
  INST(Addsd            , ExtRm              , O(F20F00,58,_,_,_,_,_,_  ), 0                         , 5  , 0  , 5340 , 6  , 4  ), // #10
  INST(Addss            , ExtRm              , O(F30F00,58,_,_,_,_,_,_  ), 0                         , 6  , 0  , 3283 , 7  , 5  ), // #11
  INST(Addsubpd         , ExtRm              , O(660F00,D0,_,_,_,_,_,_  ), 0                         , 3  , 0  , 4845 , 5  , 6  ), // #12
  INST(Addsubps         , ExtRm              , O(F20F00,D0,_,_,_,_,_,_  ), 0                         , 5  , 0  , 4857 , 5  , 6  ), // #13
  INST(Adox             , X86Rm              , O(F30F38,F6,_,_,x,_,_,_  ), 0                         , 7  , 0  , 26   , 4  , 7  ), // #14
  INST(Aesdec           , ExtRm              , O(660F38,DE,_,_,_,_,_,_  ), 0                         , 2  , 0  , 3338 , 5  , 8  ), // #15
  INST(Aesdeclast       , ExtRm              , O(660F38,DF,_,_,_,_,_,_  ), 0                         , 2  , 0  , 3346 , 5  , 8  ), // #16
  INST(Aesenc           , ExtRm              , O(660F38,DC,_,_,_,_,_,_  ), 0                         , 2  , 0  , 3358 , 5  , 8  ), // #17
  INST(Aesenclast       , ExtRm              , O(660F38,DD,_,_,_,_,_,_  ), 0                         , 2  , 0  , 3366 , 5  , 8  ), // #18
  INST(Aesimc           , ExtRm              , O(660F38,DB,_,_,_,_,_,_  ), 0                         , 2  , 0  , 3378 , 5  , 8  ), // #19
  INST(Aeskeygenassist  , ExtRmi             , O(660F3A,DF,_,_,_,_,_,_  ), 0                         , 8  , 0  , 3386 , 8  , 8  ), // #20
  INST(And              , X86Arith           , O(000000,20,4,_,x,_,_,_  ), 0                         , 9  , 0  , 2525 , 9  , 1  ), // #21
  INST(Andn             , VexRvm_Wx          , V(000F38,F2,_,0,x,_,_,_  ), 0                         , 10 , 0  , 6814 , 10 , 9  ), // #22
  INST(Andnpd           , ExtRm              , O(660F00,55,_,_,_,_,_,_  ), 0                         , 3  , 0  , 3419 , 5  , 4  ), // #23
  INST(Andnps           , ExtRm              , O(000F00,55,_,_,_,_,_,_  ), 0                         , 4  , 0  , 3427 , 5  , 5  ), // #24
  INST(Andpd            , ExtRm              , O(660F00,54,_,_,_,_,_,_  ), 0                         , 3  , 0  , 4359 , 11 , 4  ), // #25
  INST(Andps            , ExtRm              , O(000F00,54,_,_,_,_,_,_  ), 0                         , 4  , 0  , 4369 , 11 , 5  ), // #26
  INST(Arpl             , X86Mr_NoSize       , O(000000,63,_,_,_,_,_,_  ), 0                         , 0  , 0  , 31   , 12 , 10 ), // #27
  INST(Bextr            , VexRmv_Wx          , V(000F38,F7,_,0,x,_,_,_  ), 0                         , 10 , 0  , 36   , 13 , 9  ), // #28
  INST(Blcfill          , VexVm_Wx           , V(XOP_M9,01,1,0,x,_,_,_  ), 0                         , 11 , 0  , 42   , 14 , 11 ), // #29
  INST(Blci             , VexVm_Wx           , V(XOP_M9,02,6,0,x,_,_,_  ), 0                         , 12 , 0  , 50   , 14 , 11 ), // #30
  INST(Blcic            , VexVm_Wx           , V(XOP_M9,01,5,0,x,_,_,_  ), 0                         , 13 , 0  , 55   , 14 , 11 ), // #31
  INST(Blcmsk           , VexVm_Wx           , V(XOP_M9,02,1,0,x,_,_,_  ), 0                         , 11 , 0  , 61   , 14 , 11 ), // #32
  INST(Blcs             , VexVm_Wx           , V(XOP_M9,01,3,0,x,_,_,_  ), 0                         , 14 , 0  , 68   , 14 , 11 ), // #33
  INST(Blendpd          , ExtRmi             , O(660F3A,0D,_,_,_,_,_,_  ), 0                         , 8  , 0  , 3469 , 8  , 12 ), // #34
  INST(Blendps          , ExtRmi             , O(660F3A,0C,_,_,_,_,_,_  ), 0                         , 8  , 0  , 3478 , 8  , 12 ), // #35
  INST(Blendvpd         , ExtRm_XMM0         , O(660F38,15,_,_,_,_,_,_  ), 0                         , 2  , 0  , 3487 , 15 , 12 ), // #36
  INST(Blendvps         , ExtRm_XMM0         , O(660F38,14,_,_,_,_,_,_  ), 0                         , 2  , 0  , 3497 , 15 , 12 ), // #37
  INST(Blsfill          , VexVm_Wx           , V(XOP_M9,01,2,0,x,_,_,_  ), 0                         , 15 , 0  , 73   , 14 , 11 ), // #38
  INST(Blsi             , VexVm_Wx           , V(000F38,F3,3,0,x,_,_,_  ), 0                         , 16 , 0  , 81   , 14 , 9  ), // #39
  INST(Blsic            , VexVm_Wx           , V(XOP_M9,01,6,0,x,_,_,_  ), 0                         , 12 , 0  , 86   , 14 , 11 ), // #40
  INST(Blsmsk           , VexVm_Wx           , V(000F38,F3,2,0,x,_,_,_  ), 0                         , 17 , 0  , 92   , 14 , 9  ), // #41
  INST(Blsr             , VexVm_Wx           , V(000F38,F3,1,0,x,_,_,_  ), 0                         , 18 , 0  , 99   , 14 , 9  ), // #42
  INST(Bndcl            , X86Rm              , O(F30F00,1A,_,_,_,_,_,_  ), 0                         , 6  , 0  , 104  , 16 , 13 ), // #43
  INST(Bndcn            , X86Rm              , O(F20F00,1B,_,_,_,_,_,_  ), 0                         , 5  , 0  , 110  , 16 , 13 ), // #44
  INST(Bndcu            , X86Rm              , O(F20F00,1A,_,_,_,_,_,_  ), 0                         , 5  , 0  , 116  , 16 , 13 ), // #45
  INST(Bndldx           , X86Rm              , O(000F00,1A,_,_,_,_,_,_  ), 0                         , 4  , 0  , 122  , 17 , 13 ), // #46
  INST(Bndmk            , X86Rm              , O(F30F00,1B,_,_,_,_,_,_  ), 0                         , 6  , 0  , 129  , 18 , 13 ), // #47
  INST(Bndmov           , X86Bndmov          , O(660F00,1A,_,_,_,_,_,_  ), O(660F00,1B,_,_,_,_,_,_  ), 3  , 1  , 135  , 19 , 13 ), // #48
  INST(Bndstx           , X86Mr              , O(000F00,1B,_,_,_,_,_,_  ), 0                         , 4  , 0  , 142  , 20 , 13 ), // #49
  INST(Bound            , X86Rm              , O(000000,62,_,_,_,_,_,_  ), 0                         , 0  , 0  , 149  , 21 , 0  ), // #50
  INST(Bsf              , X86Rm              , O(000F00,BC,_,_,x,_,_,_  ), 0                         , 4  , 0  , 155  , 22 , 1  ), // #51
  INST(Bsr              , X86Rm              , O(000F00,BD,_,_,x,_,_,_  ), 0                         , 4  , 0  , 159  , 22 , 1  ), // #52
  INST(Bswap            , X86Bswap           , O(000F00,C8,_,_,x,_,_,_  ), 0                         , 4  , 0  , 163  , 23 , 0  ), // #53
  INST(Bt               , X86Bt              , O(000F00,A3,_,_,x,_,_,_  ), O(000F00,BA,4,_,x,_,_,_  ), 4  , 2  , 169  , 24 , 14 ), // #54
  INST(Btc              , X86Bt              , O(000F00,BB,_,_,x,_,_,_  ), O(000F00,BA,7,_,x,_,_,_  ), 4  , 3  , 172  , 25 , 14 ), // #55
  INST(Btr              , X86Bt              , O(000F00,B3,_,_,x,_,_,_  ), O(000F00,BA,6,_,x,_,_,_  ), 4  , 4  , 176  , 25 , 14 ), // #56
  INST(Bts              , X86Bt              , O(000F00,AB,_,_,x,_,_,_  ), O(000F00,BA,5,_,x,_,_,_  ), 4  , 5  , 180  , 25 , 14 ), // #57
  INST(Bzhi             , VexRmv_Wx          , V(000F38,F5,_,0,x,_,_,_  ), 0                         , 10 , 0  , 184  , 13 , 15 ), // #58
  INST(Call             , X86Call            , O(000000,FF,2,_,_,_,_,_  ), 0                         , 1  , 0  , 3038 , 26 , 1  ), // #59
  INST(Cbw              , X86Op_xAX          , O(660000,98,_,_,_,_,_,_  ), 0                         , 19 , 0  , 189  , 27 , 0  ), // #60
  INST(Cdq              , X86Op_xDX_xAX      , O(000000,99,_,_,_,_,_,_  ), 0                         , 0  , 0  , 193  , 28 , 0  ), // #61
  INST(Cdqe             , X86Op_xAX          , O(000000,98,_,_,1,_,_,_  ), 0                         , 20 , 0  , 197  , 29 , 0  ), // #62
  INST(Clac             , X86Op              , O(000F01,CA,_,_,_,_,_,_  ), 0                         , 21 , 0  , 202  , 30 , 16 ), // #63
  INST(Clc              , X86Op              , O(000000,F8,_,_,_,_,_,_  ), 0                         , 0  , 0  , 207  , 30 , 17 ), // #64
  INST(Cld              , X86Op              , O(000000,FC,_,_,_,_,_,_  ), 0                         , 0  , 0  , 211  , 30 , 18 ), // #65
  INST(Cldemote         , X86M_Only          , O(000F00,1C,0,_,_,_,_,_  ), 0                         , 4  , 0  , 215  , 31 , 19 ), // #66
  INST(Clflush          , X86M_Only          , O(000F00,AE,7,_,_,_,_,_  ), 0                         , 22 , 0  , 224  , 31 , 20 ), // #67
  INST(Clflushopt       , X86M_Only          , O(660F00,AE,7,_,_,_,_,_  ), 0                         , 23 , 0  , 232  , 31 , 21 ), // #68
  INST(Clgi             , X86Op              , O(000F01,DD,_,_,_,_,_,_  ), 0                         , 21 , 0  , 243  , 30 , 22 ), // #69
  INST(Cli              , X86Op              , O(000000,FA,_,_,_,_,_,_  ), 0                         , 0  , 0  , 248  , 30 , 23 ), // #70
  INST(Clrssbsy         , X86M_Only          , O(F30F00,AE,6,_,_,_,_,_  ), 0                         , 24 , 0  , 252  , 32 , 24 ), // #71
  INST(Clts             , X86Op              , O(000F00,06,_,_,_,_,_,_  ), 0                         , 4  , 0  , 261  , 30 , 0  ), // #72
  INST(Clui             , X86Op              , O(F30F01,EE,_,_,_,_,_,_  ), 0                         , 25 , 0  , 266  , 33 , 25 ), // #73
  INST(Clwb             , X86M_Only          , O(660F00,AE,6,_,_,_,_,_  ), 0                         , 26 , 0  , 271  , 31 , 26 ), // #74
  INST(Clzero           , X86Op_MemZAX       , O(000F01,FC,_,_,_,_,_,_  ), 0                         , 21 , 0  , 276  , 34 , 27 ), // #75
  INST(Cmc              , X86Op              , O(000000,F5,_,_,_,_,_,_  ), 0                         , 0  , 0  , 283  , 30 , 28 ), // #76
  INST(Cmova            , X86Rm              , O(000F00,47,_,_,x,_,_,_  ), 0                         , 4  , 0  , 287  , 22 , 29 ), // #77
  INST(Cmovae           , X86Rm              , O(000F00,43,_,_,x,_,_,_  ), 0                         , 4  , 0  , 293  , 22 , 30 ), // #78
  INST(Cmovb            , X86Rm              , O(000F00,42,_,_,x,_,_,_  ), 0                         , 4  , 0  , 648  , 22 , 30 ), // #79
  INST(Cmovbe           , X86Rm              , O(000F00,46,_,_,x,_,_,_  ), 0                         , 4  , 0  , 655  , 22 , 29 ), // #80
  INST(Cmovc            , X86Rm              , O(000F00,42,_,_,x,_,_,_  ), 0                         , 4  , 0  , 300  , 22 , 30 ), // #81
  INST(Cmove            , X86Rm              , O(000F00,44,_,_,x,_,_,_  ), 0                         , 4  , 0  , 663  , 22 , 31 ), // #82
  INST(Cmovg            , X86Rm              , O(000F00,4F,_,_,x,_,_,_  ), 0                         , 4  , 0  , 306  , 22 , 32 ), // #83
  INST(Cmovge           , X86Rm              , O(000F00,4D,_,_,x,_,_,_  ), 0                         , 4  , 0  , 312  , 22 , 33 ), // #84
  INST(Cmovl            , X86Rm              , O(000F00,4C,_,_,x,_,_,_  ), 0                         , 4  , 0  , 319  , 22 , 33 ), // #85
  INST(Cmovle           , X86Rm              , O(000F00,4E,_,_,x,_,_,_  ), 0                         , 4  , 0  , 325  , 22 , 32 ), // #86
  INST(Cmovna           , X86Rm              , O(000F00,46,_,_,x,_,_,_  ), 0                         , 4  , 0  , 332  , 22 , 29 ), // #87
  INST(Cmovnae          , X86Rm              , O(000F00,42,_,_,x,_,_,_  ), 0                         , 4  , 0  , 339  , 22 , 30 ), // #88
  INST(Cmovnb           , X86Rm              , O(000F00,43,_,_,x,_,_,_  ), 0                         , 4  , 0  , 670  , 22 , 30 ), // #89
  INST(Cmovnbe          , X86Rm              , O(000F00,47,_,_,x,_,_,_  ), 0                         , 4  , 0  , 678  , 22 , 29 ), // #90
  INST(Cmovnc           , X86Rm              , O(000F00,43,_,_,x,_,_,_  ), 0                         , 4  , 0  , 347  , 22 , 30 ), // #91
  INST(Cmovne           , X86Rm              , O(000F00,45,_,_,x,_,_,_  ), 0                         , 4  , 0  , 687  , 22 , 31 ), // #92
  INST(Cmovng           , X86Rm              , O(000F00,4E,_,_,x,_,_,_  ), 0                         , 4  , 0  , 354  , 22 , 32 ), // #93
  INST(Cmovnge          , X86Rm              , O(000F00,4C,_,_,x,_,_,_  ), 0                         , 4  , 0  , 361  , 22 , 33 ), // #94
  INST(Cmovnl           , X86Rm              , O(000F00,4D,_,_,x,_,_,_  ), 0                         , 4  , 0  , 369  , 22 , 33 ), // #95
  INST(Cmovnle          , X86Rm              , O(000F00,4F,_,_,x,_,_,_  ), 0                         , 4  , 0  , 376  , 22 , 32 ), // #96
  INST(Cmovno           , X86Rm              , O(000F00,41,_,_,x,_,_,_  ), 0                         , 4  , 0  , 384  , 22 , 34 ), // #97
  INST(Cmovnp           , X86Rm              , O(000F00,4B,_,_,x,_,_,_  ), 0                         , 4  , 0  , 391  , 22 , 35 ), // #98
  INST(Cmovns           , X86Rm              , O(000F00,49,_,_,x,_,_,_  ), 0                         , 4  , 0  , 398  , 22 , 36 ), // #99
  INST(Cmovnz           , X86Rm              , O(000F00,45,_,_,x,_,_,_  ), 0                         , 4  , 0  , 405  , 22 , 31 ), // #100
  INST(Cmovo            , X86Rm              , O(000F00,40,_,_,x,_,_,_  ), 0                         , 4  , 0  , 412  , 22 , 34 ), // #101
  INST(Cmovp            , X86Rm              , O(000F00,4A,_,_,x,_,_,_  ), 0                         , 4  , 0  , 418  , 22 , 35 ), // #102
  INST(Cmovpe           , X86Rm              , O(000F00,4A,_,_,x,_,_,_  ), 0                         , 4  , 0  , 424  , 22 , 35 ), // #103
  INST(Cmovpo           , X86Rm              , O(000F00,4B,_,_,x,_,_,_  ), 0                         , 4  , 0  , 431  , 22 , 35 ), // #104
  INST(Cmovs            , X86Rm              , O(000F00,48,_,_,x,_,_,_  ), 0                         , 4  , 0  , 438  , 22 , 36 ), // #105
  INST(Cmovz            , X86Rm              , O(000F00,44,_,_,x,_,_,_  ), 0                         , 4  , 0  , 444  , 22 , 31 ), // #106
  INST(Cmp              , X86Arith           , O(000000,38,7,_,x,_,_,_  ), 0                         , 27 , 0  , 450  , 35 , 1  ), // #107
  INST(Cmppd            , ExtRmi             , O(660F00,C2,_,_,_,_,_,_  ), 0                         , 3  , 0  , 3723 , 8  , 4  ), // #108
  INST(Cmpps            , ExtRmi             , O(000F00,C2,_,_,_,_,_,_  ), 0                         , 4  , 0  , 3730 , 8  , 5  ), // #109
  INST(Cmps             , X86StrMm           , O(000000,A6,_,_,_,_,_,_  ), 0                         , 0  , 0  , 454  , 36 , 37 ), // #110
  INST(Cmpsd            , ExtRmi             , O(F20F00,C2,_,_,_,_,_,_  ), 0                         , 5  , 0  , 3737 , 37 , 4  ), // #111
  INST(Cmpss            , ExtRmi             , O(F30F00,C2,_,_,_,_,_,_  ), 0                         , 6  , 0  , 3744 , 38 , 5  ), // #112
  INST(Cmpxchg          , X86Cmpxchg         , O(000F00,B0,_,_,x,_,_,_  ), 0                         , 4  , 0  , 459  , 39 , 38 ), // #113
  INST(Cmpxchg16b       , X86Cmpxchg8b_16b   , O(000F00,C7,1,_,1,_,_,_  ), 0                         , 28 , 0  , 467  , 40 , 39 ), // #114
  INST(Cmpxchg8b        , X86Cmpxchg8b_16b   , O(000F00,C7,1,_,_,_,_,_  ), 0                         , 29 , 0  , 478  , 41 , 40 ), // #115
  INST(Comisd           , ExtRm              , O(660F00,2F,_,_,_,_,_,_  ), 0                         , 3  , 0  , 10290, 6  , 41 ), // #116
  INST(Comiss           , ExtRm              , O(000F00,2F,_,_,_,_,_,_  ), 0                         , 4  , 0  , 10299, 7  , 42 ), // #117
  INST(Cpuid            , X86Op              , O(000F00,A2,_,_,_,_,_,_  ), 0                         , 4  , 0  , 488  , 42 , 43 ), // #118
  INST(Cqo              , X86Op_xDX_xAX      , O(000000,99,_,_,1,_,_,_  ), 0                         , 20 , 0  , 494  , 43 , 0  ), // #119
  INST(Crc32            , X86Crc             , O(F20F38,F0,_,_,x,_,_,_  ), 0                         , 30 , 0  , 498  , 44 , 44 ), // #120
  INST(Cvtdq2pd         , ExtRm              , O(F30F00,E6,_,_,_,_,_,_  ), 0                         , 6  , 0  , 3791 , 6  , 4  ), // #121
  INST(Cvtdq2ps         , ExtRm              , O(000F00,5B,_,_,_,_,_,_  ), 0                         , 4  , 0  , 3801 , 5  , 4  ), // #122
  INST(Cvtpd2dq         , ExtRm              , O(F20F00,E6,_,_,_,_,_,_  ), 0                         , 5  , 0  , 3840 , 5  , 4  ), // #123
  INST(Cvtpd2pi         , ExtRm              , O(660F00,2D,_,_,_,_,_,_  ), 0                         , 3  , 0  , 504  , 45 , 4  ), // #124
  INST(Cvtpd2ps         , ExtRm              , O(660F00,5A,_,_,_,_,_,_  ), 0                         , 3  , 0  , 3850 , 5  , 4  ), // #125
  INST(Cvtpi2pd         , ExtRm              , O(660F00,2A,_,_,_,_,_,_  ), 0                         , 3  , 0  , 513  , 46 , 4  ), // #126
  INST(Cvtpi2ps         , ExtRm              , O(000F00,2A,_,_,_,_,_,_  ), 0                         , 4  , 0  , 522  , 46 , 5  ), // #127
  INST(Cvtps2dq         , ExtRm              , O(660F00,5B,_,_,_,_,_,_  ), 0                         , 3  , 0  , 3902 , 5  , 4  ), // #128
  INST(Cvtps2pd         , ExtRm              , O(000F00,5A,_,_,_,_,_,_  ), 0                         , 4  , 0  , 3912 , 6  , 4  ), // #129
  INST(Cvtps2pi         , ExtRm              , O(000F00,2D,_,_,_,_,_,_  ), 0                         , 4  , 0  , 531  , 47 , 5  ), // #130
  INST(Cvtsd2si         , ExtRm_Wx_GpqOnly   , O(F20F00,2D,_,_,x,_,_,_  ), 0                         , 5  , 0  , 3984 , 48 , 4  ), // #131
  INST(Cvtsd2ss         , ExtRm              , O(F20F00,5A,_,_,_,_,_,_  ), 0                         , 5  , 0  , 3994 , 6  , 4  ), // #132
  INST(Cvtsi2sd         , ExtRm_Wx           , O(F20F00,2A,_,_,x,_,_,_  ), 0                         , 5  , 0  , 4015 , 49 , 4  ), // #133
  INST(Cvtsi2ss         , ExtRm_Wx           , O(F30F00,2A,_,_,x,_,_,_  ), 0                         , 6  , 0  , 4025 , 49 , 5  ), // #134
  INST(Cvtss2sd         , ExtRm              , O(F30F00,5A,_,_,_,_,_,_  ), 0                         , 6  , 0  , 4035 , 7  , 4  ), // #135
  INST(Cvtss2si         , ExtRm_Wx_GpqOnly   , O(F30F00,2D,_,_,x,_,_,_  ), 0                         , 6  , 0  , 4045 , 50 , 5  ), // #136
  INST(Cvttpd2dq        , ExtRm              , O(660F00,E6,_,_,_,_,_,_  ), 0                         , 3  , 0  , 4066 , 5  , 4  ), // #137
  INST(Cvttpd2pi        , ExtRm              , O(660F00,2C,_,_,_,_,_,_  ), 0                         , 3  , 0  , 540  , 45 , 4  ), // #138
  INST(Cvttps2dq        , ExtRm              , O(F30F00,5B,_,_,_,_,_,_  ), 0                         , 6  , 0  , 4112 , 5  , 4  ), // #139
  INST(Cvttps2pi        , ExtRm              , O(000F00,2C,_,_,_,_,_,_  ), 0                         , 4  , 0  , 550  , 47 , 5  ), // #140
  INST(Cvttsd2si        , ExtRm_Wx_GpqOnly   , O(F20F00,2C,_,_,x,_,_,_  ), 0                         , 5  , 0  , 4158 , 48 , 4  ), // #141
  INST(Cvttss2si        , ExtRm_Wx_GpqOnly   , O(F30F00,2C,_,_,x,_,_,_  ), 0                         , 6  , 0  , 4181 , 50 , 5  ), // #142
  INST(Cwd              , X86Op_xDX_xAX      , O(660000,99,_,_,_,_,_,_  ), 0                         , 19 , 0  , 560  , 51 , 0  ), // #143
  INST(Cwde             , X86Op_xAX          , O(000000,98,_,_,_,_,_,_  ), 0                         , 0  , 0  , 564  , 52 , 0  ), // #144
  INST(Daa              , X86Op              , O(000000,27,_,_,_,_,_,_  ), 0                         , 0  , 0  , 569  , 1  , 1  ), // #145
  INST(Das              , X86Op              , O(000000,2F,_,_,_,_,_,_  ), 0                         , 0  , 0  , 573  , 1  , 1  ), // #146
  INST(Dec              , X86IncDec          , O(000000,FE,1,_,x,_,_,_  ), O(000000,48,_,_,x,_,_,_  ), 31 , 6  , 3341 , 53 , 45 ), // #147
  INST(Div              , X86M_GPB_MulDiv    , O(000000,F6,6,_,x,_,_,_  ), 0                         , 32 , 0  , 810  , 54 , 1  ), // #148
  INST(Divpd            , ExtRm              , O(660F00,5E,_,_,_,_,_,_  ), 0                         , 3  , 0  , 4280 , 5  , 4  ), // #149
  INST(Divps            , ExtRm              , O(000F00,5E,_,_,_,_,_,_  ), 0                         , 4  , 0  , 4287 , 5  , 5  ), // #150
  INST(Divsd            , ExtRm              , O(F20F00,5E,_,_,_,_,_,_  ), 0                         , 5  , 0  , 4294 , 6  , 4  ), // #151
  INST(Divss            , ExtRm              , O(F30F00,5E,_,_,_,_,_,_  ), 0                         , 6  , 0  , 4301 , 7  , 5  ), // #152
  INST(Dppd             , ExtRmi             , O(660F3A,41,_,_,_,_,_,_  ), 0                         , 8  , 0  , 4318 , 8  , 12 ), // #153
  INST(Dpps             , ExtRmi             , O(660F3A,40,_,_,_,_,_,_  ), 0                         , 8  , 0  , 4324 , 8  , 12 ), // #154
  INST(Emms             , X86Op              , O(000F00,77,_,_,_,_,_,_  ), 0                         , 4  , 0  , 778  , 55 , 46 ), // #155
  INST(Endbr32          , X86Op_Mod11RM      , O(F30F00,1E,7,_,_,_,_,3  ), 0                         , 33 , 0  , 577  , 30 , 47 ), // #156
  INST(Endbr64          , X86Op_Mod11RM      , O(F30F00,1E,7,_,_,_,_,2  ), 0                         , 34 , 0  , 585  , 30 , 47 ), // #157
  INST(Enqcmd           , X86EnqcmdMovdir64b , O(F20F38,F8,_,_,_,_,_,_  ), 0                         , 30 , 0  , 593  , 56 , 48 ), // #158
  INST(Enqcmds          , X86EnqcmdMovdir64b , O(F30F38,F8,_,_,_,_,_,_  ), 0                         , 7  , 0  , 600  , 56 , 48 ), // #159
  INST(Enter            , X86Enter           , O(000000,C8,_,_,_,_,_,_  ), 0                         , 0  , 0  , 3046 , 57 , 0  ), // #160
  INST(Extractps        , ExtExtract         , O(660F3A,17,_,_,_,_,_,_  ), 0                         , 8  , 0  , 4514 , 58 , 12 ), // #161
  INST(Extrq            , ExtExtrq           , O(660F00,79,_,_,_,_,_,_  ), O(660F00,78,0,_,_,_,_,_  ), 3  , 7  , 7650 , 59 , 49 ), // #162
  INST(F2xm1            , FpuOp              , O_FPU(00,D9F0,_)          , 0                         , 35 , 0  , 608  , 30 , 0  ), // #163
  INST(Fabs             , FpuOp              , O_FPU(00,D9E1,_)          , 0                         , 35 , 0  , 614  , 30 , 0  ), // #164
  INST(Fadd             , FpuArith           , O_FPU(00,C0C0,0)          , 0                         , 36 , 0  , 2121 , 60 , 0  ), // #165
  INST(Faddp            , FpuRDef            , O_FPU(00,DEC0,_)          , 0                         , 37 , 0  , 619  , 61 , 0  ), // #166
  INST(Fbld             , X86M_Only          , O_FPU(00,00DF,4)          , 0                         , 38 , 0  , 625  , 62 , 0  ), // #167
  INST(Fbstp            , X86M_Only          , O_FPU(00,00DF,6)          , 0                         , 39 , 0  , 630  , 62 , 0  ), // #168
  INST(Fchs             , FpuOp              , O_FPU(00,D9E0,_)          , 0                         , 35 , 0  , 636  , 30 , 0  ), // #169
  INST(Fclex            , FpuOp              , O_FPU(9B,DBE2,_)          , 0                         , 40 , 0  , 641  , 30 , 0  ), // #170
  INST(Fcmovb           , FpuR               , O_FPU(00,DAC0,_)          , 0                         , 41 , 0  , 647  , 63 , 30 ), // #171
  INST(Fcmovbe          , FpuR               , O_FPU(00,DAD0,_)          , 0                         , 41 , 0  , 654  , 63 , 29 ), // #172
  INST(Fcmove           , FpuR               , O_FPU(00,DAC8,_)          , 0                         , 41 , 0  , 662  , 63 , 31 ), // #173
  INST(Fcmovnb          , FpuR               , O_FPU(00,DBC0,_)          , 0                         , 42 , 0  , 669  , 63 , 30 ), // #174
  INST(Fcmovnbe         , FpuR               , O_FPU(00,DBD0,_)          , 0                         , 42 , 0  , 677  , 63 , 29 ), // #175
  INST(Fcmovne          , FpuR               , O_FPU(00,DBC8,_)          , 0                         , 42 , 0  , 686  , 63 , 31 ), // #176
  INST(Fcmovnu          , FpuR               , O_FPU(00,DBD8,_)          , 0                         , 42 , 0  , 694  , 63 , 35 ), // #177
  INST(Fcmovu           , FpuR               , O_FPU(00,DAD8,_)          , 0                         , 41 , 0  , 702  , 63 , 35 ), // #178
  INST(Fcom             , FpuCom             , O_FPU(00,D0D0,2)          , 0                         , 43 , 0  , 709  , 64 , 0  ), // #179
  INST(Fcomi            , FpuR               , O_FPU(00,DBF0,_)          , 0                         , 42 , 0  , 714  , 63 , 50 ), // #180
  INST(Fcomip           , FpuR               , O_FPU(00,DFF0,_)          , 0                         , 44 , 0  , 720  , 63 , 50 ), // #181
  INST(Fcomp            , FpuCom             , O_FPU(00,D8D8,3)          , 0                         , 45 , 0  , 727  , 64 , 0  ), // #182
  INST(Fcompp           , FpuOp              , O_FPU(00,DED9,_)          , 0                         , 37 , 0  , 733  , 30 , 0  ), // #183
  INST(Fcos             , FpuOp              , O_FPU(00,D9FF,_)          , 0                         , 35 , 0  , 740  , 30 , 0  ), // #184
  INST(Fdecstp          , FpuOp              , O_FPU(00,D9F6,_)          , 0                         , 35 , 0  , 745  , 30 , 0  ), // #185
  INST(Fdiv             , FpuArith           , O_FPU(00,F0F8,6)          , 0                         , 46 , 0  , 753  , 60 , 0  ), // #186
  INST(Fdivp            , FpuRDef            , O_FPU(00,DEF8,_)          , 0                         , 37 , 0  , 758  , 61 , 0  ), // #187
  INST(Fdivr            , FpuArith           , O_FPU(00,F8F0,7)          , 0                         , 47 , 0  , 764  , 60 , 0  ), // #188
  INST(Fdivrp           , FpuRDef            , O_FPU(00,DEF0,_)          , 0                         , 37 , 0  , 770  , 61 , 0  ), // #189
  INST(Femms            , X86Op              , O(000F00,0E,_,_,_,_,_,_  ), 0                         , 4  , 0  , 777  , 30 , 51 ), // #190
  INST(Ffree            , FpuR               , O_FPU(00,DDC0,_)          , 0                         , 48 , 0  , 783  , 63 , 0  ), // #191
  INST(Fiadd            , FpuM               , O_FPU(00,00DA,0)          , 0                         , 49 , 0  , 789  , 65 , 0  ), // #192
  INST(Ficom            , FpuM               , O_FPU(00,00DA,2)          , 0                         , 50 , 0  , 795  , 65 , 0  ), // #193
  INST(Ficomp           , FpuM               , O_FPU(00,00DA,3)          , 0                         , 51 , 0  , 801  , 65 , 0  ), // #194
  INST(Fidiv            , FpuM               , O_FPU(00,00DA,6)          , 0                         , 39 , 0  , 808  , 65 , 0  ), // #195
  INST(Fidivr           , FpuM               , O_FPU(00,00DA,7)          , 0                         , 52 , 0  , 814  , 65 , 0  ), // #196
  INST(Fild             , FpuM               , O_FPU(00,00DB,0)          , O_FPU(00,00DF,5)          , 49 , 8  , 821  , 66 , 0  ), // #197
  INST(Fimul            , FpuM               , O_FPU(00,00DA,1)          , 0                         , 53 , 0  , 826  , 65 , 0  ), // #198
  INST(Fincstp          , FpuOp              , O_FPU(00,D9F7,_)          , 0                         , 35 , 0  , 832  , 30 , 0  ), // #199
  INST(Finit            , FpuOp              , O_FPU(9B,DBE3,_)          , 0                         , 40 , 0  , 840  , 30 , 0  ), // #200
  INST(Fist             , FpuM               , O_FPU(00,00DB,2)          , 0                         , 50 , 0  , 846  , 65 , 0  ), // #201
  INST(Fistp            , FpuM               , O_FPU(00,00DB,3)          , O_FPU(00,00DF,7)          , 51 , 9  , 851  , 66 , 0  ), // #202
  INST(Fisttp           , FpuM               , O_FPU(00,00DB,1)          , O_FPU(00,00DD,1)          , 53 , 10 , 857  , 66 , 6  ), // #203
  INST(Fisub            , FpuM               , O_FPU(00,00DA,4)          , 0                         , 38 , 0  , 864  , 65 , 0  ), // #204
  INST(Fisubr           , FpuM               , O_FPU(00,00DA,5)          , 0                         , 54 , 0  , 870  , 65 , 0  ), // #205
  INST(Fld              , FpuFldFst          , O_FPU(00,00D9,0)          , O_FPU(00,00DB,5)          , 49 , 11 , 877  , 67 , 0  ), // #206
  INST(Fld1             , FpuOp              , O_FPU(00,D9E8,_)          , 0                         , 35 , 0  , 881  , 30 , 0  ), // #207
  INST(Fldcw            , X86M_Only          , O_FPU(00,00D9,5)          , 0                         , 54 , 0  , 886  , 68 , 0  ), // #208
  INST(Fldenv           , X86M_Only          , O_FPU(00,00D9,4)          , 0                         , 38 , 0  , 892  , 69 , 0  ), // #209
  INST(Fldl2e           , FpuOp              , O_FPU(00,D9EA,_)          , 0                         , 35 , 0  , 899  , 30 , 0  ), // #210
  INST(Fldl2t           , FpuOp              , O_FPU(00,D9E9,_)          , 0                         , 35 , 0  , 906  , 30 , 0  ), // #211
  INST(Fldlg2           , FpuOp              , O_FPU(00,D9EC,_)          , 0                         , 35 , 0  , 913  , 30 , 0  ), // #212
  INST(Fldln2           , FpuOp              , O_FPU(00,D9ED,_)          , 0                         , 35 , 0  , 920  , 30 , 0  ), // #213
  INST(Fldpi            , FpuOp              , O_FPU(00,D9EB,_)          , 0                         , 35 , 0  , 927  , 30 , 0  ), // #214
  INST(Fldz             , FpuOp              , O_FPU(00,D9EE,_)          , 0                         , 35 , 0  , 933  , 30 , 0  ), // #215
  INST(Fmul             , FpuArith           , O_FPU(00,C8C8,1)          , 0                         , 55 , 0  , 2163 , 60 , 0  ), // #216
  INST(Fmulp            , FpuRDef            , O_FPU(00,DEC8,_)          , 0                         , 37 , 0  , 938  , 61 , 0  ), // #217
  INST(Fnclex           , FpuOp              , O_FPU(00,DBE2,_)          , 0                         , 42 , 0  , 944  , 30 , 0  ), // #218
  INST(Fninit           , FpuOp              , O_FPU(00,DBE3,_)          , 0                         , 42 , 0  , 951  , 30 , 0  ), // #219
  INST(Fnop             , FpuOp              , O_FPU(00,D9D0,_)          , 0                         , 35 , 0  , 958  , 30 , 0  ), // #220
  INST(Fnsave           , X86M_Only          , O_FPU(00,00DD,6)          , 0                         , 39 , 0  , 963  , 69 , 0  ), // #221
  INST(Fnstcw           , X86M_Only          , O_FPU(00,00D9,7)          , 0                         , 52 , 0  , 970  , 68 , 0  ), // #222
  INST(Fnstenv          , X86M_Only          , O_FPU(00,00D9,6)          , 0                         , 39 , 0  , 977  , 69 , 0  ), // #223
  INST(Fnstsw           , FpuStsw            , O_FPU(00,00DD,7)          , O_FPU(00,DFE0,_)          , 52 , 12 , 985  , 70 , 0  ), // #224
  INST(Fpatan           , FpuOp              , O_FPU(00,D9F3,_)          , 0                         , 35 , 0  , 992  , 30 , 0  ), // #225
  INST(Fprem            , FpuOp              , O_FPU(00,D9F8,_)          , 0                         , 35 , 0  , 999  , 30 , 0  ), // #226
  INST(Fprem1           , FpuOp              , O_FPU(00,D9F5,_)          , 0                         , 35 , 0  , 1005 , 30 , 0  ), // #227
  INST(Fptan            , FpuOp              , O_FPU(00,D9F2,_)          , 0                         , 35 , 0  , 1012 , 30 , 0  ), // #228
  INST(Frndint          , FpuOp              , O_FPU(00,D9FC,_)          , 0                         , 35 , 0  , 1018 , 30 , 0  ), // #229
  INST(Frstor           , X86M_Only          , O_FPU(00,00DD,4)          , 0                         , 38 , 0  , 1026 , 69 , 0  ), // #230
  INST(Fsave            , X86M_Only          , O_FPU(9B,00DD,6)          , 0                         , 56 , 0  , 1033 , 69 , 0  ), // #231
  INST(Fscale           , FpuOp              , O_FPU(00,D9FD,_)          , 0                         , 35 , 0  , 1039 , 30 , 0  ), // #232
  INST(Fsin             , FpuOp              , O_FPU(00,D9FE,_)          , 0                         , 35 , 0  , 1046 , 30 , 0  ), // #233
  INST(Fsincos          , FpuOp              , O_FPU(00,D9FB,_)          , 0                         , 35 , 0  , 1051 , 30 , 0  ), // #234
  INST(Fsqrt            , FpuOp              , O_FPU(00,D9FA,_)          , 0                         , 35 , 0  , 1059 , 30 , 0  ), // #235
  INST(Fst              , FpuFldFst          , O_FPU(00,00D9,2)          , 0                         , 50 , 0  , 1065 , 71 , 0  ), // #236
  INST(Fstcw            , X86M_Only          , O_FPU(9B,00D9,7)          , 0                         , 57 , 0  , 1069 , 68 , 0  ), // #237
  INST(Fstenv           , X86M_Only          , O_FPU(9B,00D9,6)          , 0                         , 56 , 0  , 1075 , 69 , 0  ), // #238
  INST(Fstp             , FpuFldFst          , O_FPU(00,00D9,3)          , O(000000,DB,7,_,_,_,_,_  ), 51 , 13 , 1082 , 67 , 0  ), // #239
  INST(Fstsw            , FpuStsw            , O_FPU(9B,00DD,7)          , O_FPU(9B,DFE0,_)          , 57 , 14 , 1087 , 70 , 0  ), // #240
  INST(Fsub             , FpuArith           , O_FPU(00,E0E8,4)          , 0                         , 58 , 0  , 2241 , 60 , 0  ), // #241
  INST(Fsubp            , FpuRDef            , O_FPU(00,DEE8,_)          , 0                         , 37 , 0  , 1093 , 61 , 0  ), // #242
  INST(Fsubr            , FpuArith           , O_FPU(00,E8E0,5)          , 0                         , 59 , 0  , 2247 , 60 , 0  ), // #243
  INST(Fsubrp           , FpuRDef            , O_FPU(00,DEE0,_)          , 0                         , 37 , 0  , 1099 , 61 , 0  ), // #244
  INST(Ftst             , FpuOp              , O_FPU(00,D9E4,_)          , 0                         , 35 , 0  , 1106 , 30 , 0  ), // #245
  INST(Fucom            , FpuRDef            , O_FPU(00,DDE0,_)          , 0                         , 48 , 0  , 1111 , 61 , 0  ), // #246
  INST(Fucomi           , FpuR               , O_FPU(00,DBE8,_)          , 0                         , 42 , 0  , 1117 , 63 , 50 ), // #247
  INST(Fucomip          , FpuR               , O_FPU(00,DFE8,_)          , 0                         , 44 , 0  , 1124 , 63 , 50 ), // #248
  INST(Fucomp           , FpuRDef            , O_FPU(00,DDE8,_)          , 0                         , 48 , 0  , 1132 , 61 , 0  ), // #249
  INST(Fucompp          , FpuOp              , O_FPU(00,DAE9,_)          , 0                         , 41 , 0  , 1139 , 30 , 0  ), // #250
  INST(Fwait            , X86Op              , O_FPU(00,009B,_)          , 0                         , 60 , 0  , 1147 , 30 , 0  ), // #251
  INST(Fxam             , FpuOp              , O_FPU(00,D9E5,_)          , 0                         , 35 , 0  , 1153 , 30 , 0  ), // #252
  INST(Fxch             , FpuR               , O_FPU(00,D9C8,_)          , 0                         , 35 , 0  , 1158 , 61 , 0  ), // #253
  INST(Fxrstor          , X86M_Only          , O(000F00,AE,1,_,_,_,_,_  ), 0                         , 29 , 0  , 1163 , 69 , 52 ), // #254
  INST(Fxrstor64        , X86M_Only          , O(000F00,AE,1,_,1,_,_,_  ), 0                         , 28 , 0  , 1171 , 72 , 52 ), // #255
  INST(Fxsave           , X86M_Only          , O(000F00,AE,0,_,_,_,_,_  ), 0                         , 4  , 0  , 1181 , 69 , 52 ), // #256
  INST(Fxsave64         , X86M_Only          , O(000F00,AE,0,_,1,_,_,_  ), 0                         , 61 , 0  , 1188 , 72 , 52 ), // #257
  INST(Fxtract          , FpuOp              , O_FPU(00,D9F4,_)          , 0                         , 35 , 0  , 1197 , 30 , 0  ), // #258
  INST(Fyl2x            , FpuOp              , O_FPU(00,D9F1,_)          , 0                         , 35 , 0  , 1205 , 30 , 0  ), // #259
  INST(Fyl2xp1          , FpuOp              , O_FPU(00,D9F9,_)          , 0                         , 35 , 0  , 1211 , 30 , 0  ), // #260
  INST(Getsec           , X86Op              , O(000F00,37,_,_,_,_,_,_  ), 0                         , 4  , 0  , 1219 , 30 , 53 ), // #261
  INST(Gf2p8affineinvqb , ExtRmi             , O(660F3A,CF,_,_,_,_,_,_  ), 0                         , 8  , 0  , 5869 , 8  , 54 ), // #262
  INST(Gf2p8affineqb    , ExtRmi             , O(660F3A,CE,_,_,_,_,_,_  ), 0                         , 8  , 0  , 5887 , 8  , 54 ), // #263
  INST(Gf2p8mulb        , ExtRm              , O(660F38,CF,_,_,_,_,_,_  ), 0                         , 2  , 0  , 5902 , 5  , 54 ), // #264
  INST(Haddpd           , ExtRm              , O(660F00,7C,_,_,_,_,_,_  ), 0                         , 3  , 0  , 5913 , 5  , 6  ), // #265
  INST(Haddps           , ExtRm              , O(F20F00,7C,_,_,_,_,_,_  ), 0                         , 5  , 0  , 5921 , 5  , 6  ), // #266
  INST(Hlt              , X86Op              , O(000000,F4,_,_,_,_,_,_  ), 0                         , 0  , 0  , 1226 , 30 , 0  ), // #267
  INST(Hreset           , X86Op_Mod11RM_I8   , O(F30F3A,F0,0,_,_,_,_,_  ), 0                         , 62 , 0  , 1230 , 73 , 55 ), // #268
  INST(Hsubpd           , ExtRm              , O(660F00,7D,_,_,_,_,_,_  ), 0                         , 3  , 0  , 5929 , 5  , 6  ), // #269
  INST(Hsubps           , ExtRm              , O(F20F00,7D,_,_,_,_,_,_  ), 0                         , 5  , 0  , 5937 , 5  , 6  ), // #270
  INST(Idiv             , X86M_GPB_MulDiv    , O(000000,F6,7,_,x,_,_,_  ), 0                         , 27 , 0  , 809  , 54 , 1  ), // #271
  INST(Imul             , X86Imul            , O(000000,F6,5,_,x,_,_,_  ), 0                         , 63 , 0  , 827  , 74 , 1  ), // #272
  INST(In               , X86In              , O(000000,EC,_,_,_,_,_,_  ), O(000000,E4,_,_,_,_,_,_  ), 0  , 15 , 10462, 75 , 0  ), // #273
  INST(Inc              , X86IncDec          , O(000000,FE,0,_,x,_,_,_  ), O(000000,40,_,_,x,_,_,_  ), 0  , 16 , 1237 , 53 , 45 ), // #274
  INST(Incsspd          , X86M               , O(F30F00,AE,5,_,0,_,_,_  ), 0                         , 64 , 0  , 1241 , 76 , 56 ), // #275
  INST(Incsspq          , X86M               , O(F30F00,AE,5,_,1,_,_,_  ), 0                         , 65 , 0  , 1249 , 77 , 56 ), // #276
  INST(Ins              , X86Ins             , O(000000,6C,_,_,_,_,_,_  ), 0                         , 0  , 0  , 1916 , 78 , 0  ), // #277
  INST(Insertps         , ExtRmi             , O(660F3A,21,_,_,_,_,_,_  ), 0                         , 8  , 0  , 6073 , 38 , 12 ), // #278
  INST(Insertq          , ExtInsertq         , O(F20F00,79,_,_,_,_,_,_  ), O(F20F00,78,_,_,_,_,_,_  ), 5  , 17 , 1257 , 79 , 49 ), // #279
  INST(Int              , X86Int             , O(000000,CD,_,_,_,_,_,_  ), 0                         , 0  , 0  , 1022 , 80 , 0  ), // #280
  INST(Int3             , X86Op              , O(000000,CC,_,_,_,_,_,_  ), 0                         , 0  , 0  , 1265 , 30 , 0  ), // #281
  INST(Into             , X86Op              , O(000000,CE,_,_,_,_,_,_  ), 0                         , 0  , 0  , 1270 , 81 , 57 ), // #282
  INST(Invd             , X86Op              , O(000F00,08,_,_,_,_,_,_  ), 0                         , 4  , 0  , 10391, 30 , 43 ), // #283
  INST(Invept           , X86Rm_NoSize       , O(660F38,80,_,_,_,_,_,_  ), 0                         , 2  , 0  , 1275 , 82 , 58 ), // #284
  INST(Invlpg           , X86M_Only          , O(000F00,01,7,_,_,_,_,_  ), 0                         , 22 , 0  , 1282 , 69 , 43 ), // #285
  INST(Invlpga          , X86Op_xAddr        , O(000F01,DF,_,_,_,_,_,_  ), 0                         , 21 , 0  , 1289 , 83 , 22 ), // #286
  INST(Invpcid          , X86Rm_NoSize       , O(660F38,82,_,_,_,_,_,_  ), 0                         , 2  , 0  , 1297 , 82 , 43 ), // #287
  INST(Invvpid          , X86Rm_NoSize       , O(660F38,81,_,_,_,_,_,_  ), 0                         , 2  , 0  , 1305 , 82 , 58 ), // #288
  INST(Iret             , X86Op              , O(660000,CF,_,_,_,_,_,_  ), 0                         , 19 , 0  , 3226 , 84 , 1  ), // #289
  INST(Iretd            , X86Op              , O(000000,CF,_,_,_,_,_,_  ), 0                         , 0  , 0  , 1313 , 84 , 1  ), // #290
  INST(Iretq            , X86Op              , O(000000,CF,_,_,1,_,_,_  ), 0                         , 20 , 0  , 1319 , 85 , 1  ), // #291
  INST(Ja               , X86Jcc             , O(000F00,87,_,_,_,_,_,_  ), O(000000,77,_,_,_,_,_,_  ), 4  , 18 , 1325 , 86 , 59 ), // #292
  INST(Jae              , X86Jcc             , O(000F00,83,_,_,_,_,_,_  ), O(000000,73,_,_,_,_,_,_  ), 4  , 19 , 1328 , 86 , 60 ), // #293
  INST(Jb               , X86Jcc             , O(000F00,82,_,_,_,_,_,_  ), O(000000,72,_,_,_,_,_,_  ), 4  , 20 , 1332 , 86 , 60 ), // #294
  INST(Jbe              , X86Jcc             , O(000F00,86,_,_,_,_,_,_  ), O(000000,76,_,_,_,_,_,_  ), 4  , 21 , 1335 , 86 , 59 ), // #295
  INST(Jc               , X86Jcc             , O(000F00,82,_,_,_,_,_,_  ), O(000000,72,_,_,_,_,_,_  ), 4  , 20 , 1339 , 86 , 60 ), // #296
  INST(Je               , X86Jcc             , O(000F00,84,_,_,_,_,_,_  ), O(000000,74,_,_,_,_,_,_  ), 4  , 22 , 1342 , 86 , 61 ), // #297
  INST(Jecxz            , X86JecxzLoop       , 0                         , O(000000,E3,_,_,_,_,_,_  ), 0  , 23 , 1345 , 87 , 0  ), // #298
  INST(Jg               , X86Jcc             , O(000F00,8F,_,_,_,_,_,_  ), O(000000,7F,_,_,_,_,_,_  ), 4  , 24 , 1351 , 86 , 62 ), // #299
  INST(Jge              , X86Jcc             , O(000F00,8D,_,_,_,_,_,_  ), O(000000,7D,_,_,_,_,_,_  ), 4  , 25 , 1354 , 86 , 63 ), // #300
  INST(Jl               , X86Jcc             , O(000F00,8C,_,_,_,_,_,_  ), O(000000,7C,_,_,_,_,_,_  ), 4  , 26 , 1358 , 86 , 63 ), // #301
  INST(Jle              , X86Jcc             , O(000F00,8E,_,_,_,_,_,_  ), O(000000,7E,_,_,_,_,_,_  ), 4  , 27 , 1361 , 86 , 62 ), // #302
  INST(Jmp              , X86Jmp             , O(000000,FF,4,_,_,_,_,_  ), O(000000,EB,_,_,_,_,_,_  ), 9  , 28 , 1861 , 88 , 0  ), // #303
  INST(Jna              , X86Jcc             , O(000F00,86,_,_,_,_,_,_  ), O(000000,76,_,_,_,_,_,_  ), 4  , 21 , 1365 , 86 , 59 ), // #304
  INST(Jnae             , X86Jcc             , O(000F00,82,_,_,_,_,_,_  ), O(000000,72,_,_,_,_,_,_  ), 4  , 20 , 1369 , 86 , 60 ), // #305
  INST(Jnb              , X86Jcc             , O(000F00,83,_,_,_,_,_,_  ), O(000000,73,_,_,_,_,_,_  ), 4  , 19 , 1374 , 86 , 60 ), // #306
  INST(Jnbe             , X86Jcc             , O(000F00,87,_,_,_,_,_,_  ), O(000000,77,_,_,_,_,_,_  ), 4  , 18 , 1378 , 86 , 59 ), // #307
  INST(Jnc              , X86Jcc             , O(000F00,83,_,_,_,_,_,_  ), O(000000,73,_,_,_,_,_,_  ), 4  , 19 , 1383 , 86 , 60 ), // #308
  INST(Jne              , X86Jcc             , O(000F00,85,_,_,_,_,_,_  ), O(000000,75,_,_,_,_,_,_  ), 4  , 29 , 1387 , 86 , 61 ), // #309
  INST(Jng              , X86Jcc             , O(000F00,8E,_,_,_,_,_,_  ), O(000000,7E,_,_,_,_,_,_  ), 4  , 27 , 1391 , 86 , 62 ), // #310
  INST(Jnge             , X86Jcc             , O(000F00,8C,_,_,_,_,_,_  ), O(000000,7C,_,_,_,_,_,_  ), 4  , 26 , 1395 , 86 , 63 ), // #311
  INST(Jnl              , X86Jcc             , O(000F00,8D,_,_,_,_,_,_  ), O(000000,7D,_,_,_,_,_,_  ), 4  , 25 , 1400 , 86 , 63 ), // #312
  INST(Jnle             , X86Jcc             , O(000F00,8F,_,_,_,_,_,_  ), O(000000,7F,_,_,_,_,_,_  ), 4  , 24 , 1404 , 86 , 62 ), // #313
  INST(Jno              , X86Jcc             , O(000F00,81,_,_,_,_,_,_  ), O(000000,71,_,_,_,_,_,_  ), 4  , 30 , 1409 , 86 , 57 ), // #314
  INST(Jnp              , X86Jcc             , O(000F00,8B,_,_,_,_,_,_  ), O(000000,7B,_,_,_,_,_,_  ), 4  , 31 , 1413 , 86 , 64 ), // #315
  INST(Jns              , X86Jcc             , O(000F00,89,_,_,_,_,_,_  ), O(000000,79,_,_,_,_,_,_  ), 4  , 32 , 1417 , 86 , 65 ), // #316
  INST(Jnz              , X86Jcc             , O(000F00,85,_,_,_,_,_,_  ), O(000000,75,_,_,_,_,_,_  ), 4  , 29 , 1421 , 86 , 61 ), // #317
  INST(Jo               , X86Jcc             , O(000F00,80,_,_,_,_,_,_  ), O(000000,70,_,_,_,_,_,_  ), 4  , 33 , 1425 , 86 , 57 ), // #318
  INST(Jp               , X86Jcc             , O(000F00,8A,_,_,_,_,_,_  ), O(000000,7A,_,_,_,_,_,_  ), 4  , 34 , 1428 , 86 , 64 ), // #319
  INST(Jpe              , X86Jcc             , O(000F00,8A,_,_,_,_,_,_  ), O(000000,7A,_,_,_,_,_,_  ), 4  , 34 , 1431 , 86 , 64 ), // #320
  INST(Jpo              , X86Jcc             , O(000F00,8B,_,_,_,_,_,_  ), O(000000,7B,_,_,_,_,_,_  ), 4  , 31 , 1435 , 86 , 64 ), // #321
  INST(Js               , X86Jcc             , O(000F00,88,_,_,_,_,_,_  ), O(000000,78,_,_,_,_,_,_  ), 4  , 35 , 1439 , 86 , 65 ), // #322
  INST(Jz               , X86Jcc             , O(000F00,84,_,_,_,_,_,_  ), O(000000,74,_,_,_,_,_,_  ), 4  , 22 , 1442 , 86 , 61 ), // #323
  INST(Kaddb            , VexRvm             , V(660F00,4A,_,1,0,_,_,_  ), 0                         , 66 , 0  , 1445 , 89 , 66 ), // #324
  INST(Kaddd            , VexRvm             , V(660F00,4A,_,1,1,_,_,_  ), 0                         , 67 , 0  , 1451 , 89 , 67 ), // #325
  INST(Kaddq            , VexRvm             , V(000F00,4A,_,1,1,_,_,_  ), 0                         , 68 , 0  , 1457 , 89 , 67 ), // #326
  INST(Kaddw            , VexRvm             , V(000F00,4A,_,1,0,_,_,_  ), 0                         , 69 , 0  , 1463 , 89 , 66 ), // #327
  INST(Kandb            , VexRvm             , V(660F00,41,_,1,0,_,_,_  ), 0                         , 66 , 0  , 1469 , 89 , 66 ), // #328
  INST(Kandd            , VexRvm             , V(660F00,41,_,1,1,_,_,_  ), 0                         , 67 , 0  , 1475 , 89 , 67 ), // #329
  INST(Kandnb           , VexRvm             , V(660F00,42,_,1,0,_,_,_  ), 0                         , 66 , 0  , 1481 , 89 , 66 ), // #330
  INST(Kandnd           , VexRvm             , V(660F00,42,_,1,1,_,_,_  ), 0                         , 67 , 0  , 1488 , 89 , 67 ), // #331
  INST(Kandnq           , VexRvm             , V(000F00,42,_,1,1,_,_,_  ), 0                         , 68 , 0  , 1495 , 89 , 67 ), // #332
  INST(Kandnw           , VexRvm             , V(000F00,42,_,1,0,_,_,_  ), 0                         , 69 , 0  , 1502 , 89 , 68 ), // #333
  INST(Kandq            , VexRvm             , V(000F00,41,_,1,1,_,_,_  ), 0                         , 68 , 0  , 1509 , 89 , 67 ), // #334
  INST(Kandw            , VexRvm             , V(000F00,41,_,1,0,_,_,_  ), 0                         , 69 , 0  , 1515 , 89 , 68 ), // #335
  INST(Kmovb            , VexKmov            , V(660F00,90,_,0,0,_,_,_  ), V(660F00,92,_,0,0,_,_,_  ), 70 , 36 , 1521 , 90 , 66 ), // #336
  INST(Kmovd            , VexKmov            , V(660F00,90,_,0,1,_,_,_  ), V(F20F00,92,_,0,0,_,_,_  ), 71 , 37 , 8130 , 91 , 67 ), // #337
  INST(Kmovq            , VexKmov            , V(000F00,90,_,0,1,_,_,_  ), V(F20F00,92,_,0,1,_,_,_  ), 72 , 38 , 8141 , 92 , 67 ), // #338
  INST(Kmovw            , VexKmov            , V(000F00,90,_,0,0,_,_,_  ), V(000F00,92,_,0,0,_,_,_  ), 73 , 39 , 1527 , 93 , 68 ), // #339
  INST(Knotb            , VexRm              , V(660F00,44,_,0,0,_,_,_  ), 0                         , 70 , 0  , 1533 , 94 , 66 ), // #340
  INST(Knotd            , VexRm              , V(660F00,44,_,0,1,_,_,_  ), 0                         , 71 , 0  , 1539 , 94 , 67 ), // #341
  INST(Knotq            , VexRm              , V(000F00,44,_,0,1,_,_,_  ), 0                         , 72 , 0  , 1545 , 94 , 67 ), // #342
  INST(Knotw            , VexRm              , V(000F00,44,_,0,0,_,_,_  ), 0                         , 73 , 0  , 1551 , 94 , 68 ), // #343
  INST(Korb             , VexRvm             , V(660F00,45,_,1,0,_,_,_  ), 0                         , 66 , 0  , 1557 , 89 , 66 ), // #344
  INST(Kord             , VexRvm             , V(660F00,45,_,1,1,_,_,_  ), 0                         , 67 , 0  , 1562 , 89 , 67 ), // #345
  INST(Korq             , VexRvm             , V(000F00,45,_,1,1,_,_,_  ), 0                         , 68 , 0  , 1567 , 89 , 67 ), // #346
  INST(Kortestb         , VexRm              , V(660F00,98,_,0,0,_,_,_  ), 0                         , 70 , 0  , 1572 , 94 , 69 ), // #347
  INST(Kortestd         , VexRm              , V(660F00,98,_,0,1,_,_,_  ), 0                         , 71 , 0  , 1581 , 94 , 70 ), // #348
  INST(Kortestq         , VexRm              , V(000F00,98,_,0,1,_,_,_  ), 0                         , 72 , 0  , 1590 , 94 , 70 ), // #349
  INST(Kortestw         , VexRm              , V(000F00,98,_,0,0,_,_,_  ), 0                         , 73 , 0  , 1599 , 94 , 71 ), // #350
  INST(Korw             , VexRvm             , V(000F00,45,_,1,0,_,_,_  ), 0                         , 69 , 0  , 1608 , 89 , 68 ), // #351
  INST(Kshiftlb         , VexRmi             , V(660F3A,32,_,0,0,_,_,_  ), 0                         , 74 , 0  , 1613 , 95 , 66 ), // #352
  INST(Kshiftld         , VexRmi             , V(660F3A,33,_,0,0,_,_,_  ), 0                         , 74 , 0  , 1622 , 95 , 67 ), // #353
  INST(Kshiftlq         , VexRmi             , V(660F3A,33,_,0,1,_,_,_  ), 0                         , 75 , 0  , 1631 , 95 , 67 ), // #354
  INST(Kshiftlw         , VexRmi             , V(660F3A,32,_,0,1,_,_,_  ), 0                         , 75 , 0  , 1640 , 95 , 68 ), // #355
  INST(Kshiftrb         , VexRmi             , V(660F3A,30,_,0,0,_,_,_  ), 0                         , 74 , 0  , 1649 , 95 , 66 ), // #356
  INST(Kshiftrd         , VexRmi             , V(660F3A,31,_,0,0,_,_,_  ), 0                         , 74 , 0  , 1658 , 95 , 67 ), // #357
  INST(Kshiftrq         , VexRmi             , V(660F3A,31,_,0,1,_,_,_  ), 0                         , 75 , 0  , 1667 , 95 , 67 ), // #358
  INST(Kshiftrw         , VexRmi             , V(660F3A,30,_,0,1,_,_,_  ), 0                         , 75 , 0  , 1676 , 95 , 68 ), // #359
  INST(Ktestb           , VexRm              , V(660F00,99,_,0,0,_,_,_  ), 0                         , 70 , 0  , 1685 , 94 , 69 ), // #360
  INST(Ktestd           , VexRm              , V(660F00,99,_,0,1,_,_,_  ), 0                         , 71 , 0  , 1692 , 94 , 70 ), // #361
  INST(Ktestq           , VexRm              , V(000F00,99,_,0,1,_,_,_  ), 0                         , 72 , 0  , 1699 , 94 , 70 ), // #362
  INST(Ktestw           , VexRm              , V(000F00,99,_,0,0,_,_,_  ), 0                         , 73 , 0  , 1706 , 94 , 69 ), // #363
  INST(Kunpckbw         , VexRvm             , V(660F00,4B,_,1,0,_,_,_  ), 0                         , 66 , 0  , 1713 , 89 , 68 ), // #364
  INST(Kunpckdq         , VexRvm             , V(000F00,4B,_,1,1,_,_,_  ), 0                         , 68 , 0  , 1722 , 89 , 67 ), // #365
  INST(Kunpckwd         , VexRvm             , V(000F00,4B,_,1,0,_,_,_  ), 0                         , 69 , 0  , 1731 , 89 , 67 ), // #366
  INST(Kxnorb           , VexRvm             , V(660F00,46,_,1,0,_,_,_  ), 0                         , 66 , 0  , 1740 , 96 , 66 ), // #367
  INST(Kxnord           , VexRvm             , V(660F00,46,_,1,1,_,_,_  ), 0                         , 67 , 0  , 1747 , 96 , 67 ), // #368
  INST(Kxnorq           , VexRvm             , V(000F00,46,_,1,1,_,_,_  ), 0                         , 68 , 0  , 1754 , 96 , 67 ), // #369
  INST(Kxnorw           , VexRvm             , V(000F00,46,_,1,0,_,_,_  ), 0                         , 69 , 0  , 1761 , 96 , 68 ), // #370
  INST(Kxorb            , VexRvm             , V(660F00,47,_,1,0,_,_,_  ), 0                         , 66 , 0  , 1768 , 96 , 66 ), // #371
  INST(Kxord            , VexRvm             , V(660F00,47,_,1,1,_,_,_  ), 0                         , 67 , 0  , 1774 , 96 , 67 ), // #372
  INST(Kxorq            , VexRvm             , V(000F00,47,_,1,1,_,_,_  ), 0                         , 68 , 0  , 1780 , 96 , 67 ), // #373
  INST(Kxorw            , VexRvm             , V(000F00,47,_,1,0,_,_,_  ), 0                         , 69 , 0  , 1786 , 96 , 68 ), // #374
  INST(Lahf             , X86Op              , O(000000,9F,_,_,_,_,_,_  ), 0                         , 0  , 0  , 1792 , 97 , 72 ), // #375
  INST(Lar              , X86Rm              , O(000F00,02,_,_,_,_,_,_  ), 0                         , 4  , 0  , 1797 , 98 , 10 ), // #376
  INST(Lcall            , X86LcallLjmp       , O(000000,FF,3,_,_,_,_,_  ), O(000000,9A,_,_,_,_,_,_  ), 76 , 40 , 1801 , 99 , 1  ), // #377
  INST(Lddqu            , ExtRm              , O(F20F00,F0,_,_,_,_,_,_  ), 0                         , 5  , 0  , 6083 , 100, 6  ), // #378
  INST(Ldmxcsr          , X86M_Only          , O(000F00,AE,2,_,_,_,_,_  ), 0                         , 77 , 0  , 6090 , 101, 5  ), // #379
  INST(Lds              , X86Rm              , O(000000,C5,_,_,_,_,_,_  ), 0                         , 0  , 0  , 1807 , 102, 0  ), // #380
  INST(Ldtilecfg        , AmxCfg             , V(000F38,49,_,0,0,_,_,_  ), 0                         , 10 , 0  , 1811 , 103, 73 ), // #381
  INST(Lea              , X86Lea             , O(000000,8D,_,_,x,_,_,_  ), 0                         , 0  , 0  , 1821 , 104, 0  ), // #382
  INST(Leave            , X86Op              , O(000000,C9,_,_,_,_,_,_  ), 0                         , 0  , 0  , 1825 , 30 , 0  ), // #383
  INST(Les              , X86Rm              , O(000000,C4,_,_,_,_,_,_  ), 0                         , 0  , 0  , 1831 , 102, 0  ), // #384
  INST(Lfence           , X86Fence           , O(000F00,AE,5,_,_,_,_,_  ), 0                         , 78 , 0  , 1835 , 30 , 4  ), // #385
  INST(Lfs              , X86Rm              , O(000F00,B4,_,_,_,_,_,_  ), 0                         , 4  , 0  , 1842 , 105, 0  ), // #386
  INST(Lgdt             , X86M_Only          , O(000F00,01,2,_,_,_,_,_  ), 0                         , 77 , 0  , 1846 , 69 , 0  ), // #387
  INST(Lgs              , X86Rm              , O(000F00,B5,_,_,_,_,_,_  ), 0                         , 4  , 0  , 1851 , 105, 0  ), // #388
  INST(Lidt             , X86M_Only          , O(000F00,01,3,_,_,_,_,_  ), 0                         , 79 , 0  , 1855 , 69 , 0  ), // #389
  INST(Ljmp             , X86LcallLjmp       , O(000000,FF,5,_,_,_,_,_  ), O(000000,EA,_,_,_,_,_,_  ), 63 , 41 , 1860 , 106, 0  ), // #390
  INST(Lldt             , X86M_NoSize        , O(000F00,00,2,_,_,_,_,_  ), 0                         , 77 , 0  , 1865 , 107, 0  ), // #391
  INST(Llwpcb           , VexR_Wx            , V(XOP_M9,12,0,0,x,_,_,_  ), 0                         , 80 , 0  , 1870 , 108, 74 ), // #392
  INST(Lmsw             , X86M_NoSize        , O(000F00,01,6,_,_,_,_,_  ), 0                         , 81 , 0  , 1877 , 107, 0  ), // #393
  INST(Lods             , X86StrRm           , O(000000,AC,_,_,_,_,_,_  ), 0                         , 0  , 0  , 1882 , 109, 75 ), // #394
  INST(Loop             , X86JecxzLoop       , 0                         , O(000000,E2,_,_,_,_,_,_  ), 0  , 42 , 1887 , 110, 0  ), // #395
  INST(Loope            , X86JecxzLoop       , 0                         , O(000000,E1,_,_,_,_,_,_  ), 0  , 43 , 1892 , 110, 61 ), // #396
  INST(Loopne           , X86JecxzLoop       , 0                         , O(000000,E0,_,_,_,_,_,_  ), 0  , 44 , 1898 , 110, 61 ), // #397
  INST(Lsl              , X86Rm              , O(000F00,03,_,_,_,_,_,_  ), 0                         , 4  , 0  , 1905 , 111, 10 ), // #398
  INST(Lss              , X86Rm              , O(000F00,B2,_,_,_,_,_,_  ), 0                         , 4  , 0  , 6581 , 105, 0  ), // #399
  INST(Ltr              , X86M_NoSize        , O(000F00,00,3,_,_,_,_,_  ), 0                         , 79 , 0  , 1909 , 107, 0  ), // #400
  INST(Lwpins           , VexVmi4_Wx         , V(XOP_MA,12,0,0,x,_,_,_  ), 0                         , 82 , 0  , 1913 , 112, 74 ), // #401
  INST(Lwpval           , VexVmi4_Wx         , V(XOP_MA,12,1,0,x,_,_,_  ), 0                         , 83 , 0  , 1920 , 112, 74 ), // #402
  INST(Lzcnt            , X86Rm_Raw66H       , O(F30F00,BD,_,_,x,_,_,_  ), 0                         , 6  , 0  , 1927 , 22 , 76 ), // #403
  INST(Maskmovdqu       , ExtRm_ZDI          , O(660F00,F7,_,_,_,_,_,_  ), 0                         , 3  , 0  , 6099 , 113, 4  ), // #404
  INST(Maskmovq         , ExtRm_ZDI          , O(000F00,F7,_,_,_,_,_,_  ), 0                         , 4  , 0  , 8138 , 114, 77 ), // #405
  INST(Maxpd            , ExtRm              , O(660F00,5F,_,_,_,_,_,_  ), 0                         , 3  , 0  , 6133 , 5  , 4  ), // #406
  INST(Maxps            , ExtRm              , O(000F00,5F,_,_,_,_,_,_  ), 0                         , 4  , 0  , 6140 , 5  , 5  ), // #407
  INST(Maxsd            , ExtRm              , O(F20F00,5F,_,_,_,_,_,_  ), 0                         , 5  , 0  , 8157 , 6  , 4  ), // #408
  INST(Maxss            , ExtRm              , O(F30F00,5F,_,_,_,_,_,_  ), 0                         , 6  , 0  , 6154 , 7  , 5  ), // #409
  INST(Mcommit          , X86Op              , O(F30F01,FA,_,_,_,_,_,_  ), 0                         , 25 , 0  , 1933 , 30 , 78 ), // #410
  INST(Mfence           , X86Fence           , O(000F00,AE,6,_,_,_,_,_  ), 0                         , 81 , 0  , 1941 , 30 , 4  ), // #411
  INST(Minpd            , ExtRm              , O(660F00,5D,_,_,_,_,_,_  ), 0                         , 3  , 0  , 6183 , 5  , 4  ), // #412
  INST(Minps            , ExtRm              , O(000F00,5D,_,_,_,_,_,_  ), 0                         , 4  , 0  , 6190 , 5  , 5  ), // #413
  INST(Minsd            , ExtRm              , O(F20F00,5D,_,_,_,_,_,_  ), 0                         , 5  , 0  , 8221 , 6  , 4  ), // #414
  INST(Minss            , ExtRm              , O(F30F00,5D,_,_,_,_,_,_  ), 0                         , 6  , 0  , 6204 , 7  , 5  ), // #415
  INST(Monitor          , X86Op              , O(000F01,C8,_,_,_,_,_,_  ), 0                         , 21 , 0  , 3232 , 115, 79 ), // #416
  INST(Monitorx         , X86Op              , O(000F01,FA,_,_,_,_,_,_  ), 0                         , 21 , 0  , 1948 , 115, 80 ), // #417
  INST(Mov              , X86Mov             , 0                         , 0                         , 0  , 0  , 138  , 116, 0  ), // #418
  INST(Movabs           , X86Movabs          , 0                         , 0                         , 0  , 0  , 1957 , 117, 0  ), // #419
  INST(Movapd           , ExtMov             , O(660F00,28,_,_,_,_,_,_  ), O(660F00,29,_,_,_,_,_,_  ), 3  , 45 , 6235 , 118, 4  ), // #420
  INST(Movaps           , ExtMov             , O(000F00,28,_,_,_,_,_,_  ), O(000F00,29,_,_,_,_,_,_  ), 4  , 46 , 6243 , 118, 5  ), // #421
  INST(Movbe            , ExtMovbe           , O(000F38,F0,_,_,x,_,_,_  ), O(000F38,F1,_,_,x,_,_,_  ), 84 , 47 , 656  , 119, 81 ), // #422
  INST(Movd             , ExtMovd            , O(000F00,6E,_,_,_,_,_,_  ), O(000F00,7E,_,_,_,_,_,_  ), 4  , 48 , 8131 , 120, 82 ), // #423
  INST(Movddup          , ExtMov             , O(F20F00,12,_,_,_,_,_,_  ), 0                         , 5  , 0  , 6257 , 6  , 6  ), // #424
  INST(Movdir64b        , X86EnqcmdMovdir64b , O(660F38,F8,_,_,_,_,_,_  ), 0                         , 2  , 0  , 1964 , 121, 83 ), // #425
  INST(Movdiri          , X86MovntiMovdiri   , O(000F38,F9,_,_,_,_,_,_  ), 0                         , 84 , 0  , 1974 , 122, 84 ), // #426
  INST(Movdq2q          , ExtMov             , O(F20F00,D6,_,_,_,_,_,_  ), 0                         , 5  , 0  , 1982 , 123, 4  ), // #427
  INST(Movdqa           , ExtMov             , O(660F00,6F,_,_,_,_,_,_  ), O(660F00,7F,_,_,_,_,_,_  ), 3  , 49 , 6266 , 118, 4  ), // #428
  INST(Movdqu           , ExtMov             , O(F30F00,6F,_,_,_,_,_,_  ), O(F30F00,7F,_,_,_,_,_,_  ), 6  , 50 , 6103 , 118, 4  ), // #429
  INST(Movhlps          , ExtMov             , O(000F00,12,_,_,_,_,_,_  ), 0                         , 4  , 0  , 6341 , 124, 5  ), // #430
  INST(Movhpd           , ExtMov             , O(660F00,16,_,_,_,_,_,_  ), O(660F00,17,_,_,_,_,_,_  ), 3  , 51 , 6350 , 125, 4  ), // #431
  INST(Movhps           , ExtMov             , O(000F00,16,_,_,_,_,_,_  ), O(000F00,17,_,_,_,_,_,_  ), 4  , 52 , 6358 , 125, 5  ), // #432
  INST(Movlhps          , ExtMov             , O(000F00,16,_,_,_,_,_,_  ), 0                         , 4  , 0  , 6366 , 124, 5  ), // #433
  INST(Movlpd           , ExtMov             , O(660F00,12,_,_,_,_,_,_  ), O(660F00,13,_,_,_,_,_,_  ), 3  , 53 , 6375 , 125, 4  ), // #434
  INST(Movlps           , ExtMov             , O(000F00,12,_,_,_,_,_,_  ), O(000F00,13,_,_,_,_,_,_  ), 4  , 54 , 6383 , 125, 5  ), // #435
  INST(Movmskpd         , ExtMov             , O(660F00,50,_,_,_,_,_,_  ), 0                         , 3  , 0  , 6391 , 126, 4  ), // #436
  INST(Movmskps         , ExtMov             , O(000F00,50,_,_,_,_,_,_  ), 0                         , 4  , 0  , 6401 , 126, 5  ), // #437
  INST(Movntdq          , ExtMov             , 0                         , O(660F00,E7,_,_,_,_,_,_  ), 0  , 55 , 6411 , 127, 4  ), // #438
  INST(Movntdqa         , ExtMov             , O(660F38,2A,_,_,_,_,_,_  ), 0                         , 2  , 0  , 6420 , 100, 12 ), // #439
  INST(Movnti           , X86MovntiMovdiri   , O(000F00,C3,_,_,x,_,_,_  ), 0                         , 4  , 0  , 1990 , 122, 4  ), // #440
  INST(Movntpd          , ExtMov             , 0                         , O(660F00,2B,_,_,_,_,_,_  ), 0  , 56 , 6430 , 127, 4  ), // #441
  INST(Movntps          , ExtMov             , 0                         , O(000F00,2B,_,_,_,_,_,_  ), 0  , 57 , 6439 , 127, 5  ), // #442
  INST(Movntq           , ExtMov             , 0                         , O(000F00,E7,_,_,_,_,_,_  ), 0  , 58 , 1997 , 128, 77 ), // #443
  INST(Movntsd          , ExtMov             , 0                         , O(F20F00,2B,_,_,_,_,_,_  ), 0  , 59 , 2004 , 129, 49 ), // #444
  INST(Movntss          , ExtMov             , 0                         , O(F30F00,2B,_,_,_,_,_,_  ), 0  , 60 , 2012 , 130, 49 ), // #445
  INST(Movq             , ExtMovq            , O(000F00,6E,_,_,x,_,_,_  ), O(000F00,7E,_,_,x,_,_,_  ), 4  , 61 , 8142 , 131, 82 ), // #446
  INST(Movq2dq          , ExtRm              , O(F30F00,D6,_,_,_,_,_,_  ), 0                         , 6  , 0  , 2020 , 132, 4  ), // #447
  INST(Movs             , X86StrMm           , O(000000,A4,_,_,_,_,_,_  ), 0                         , 0  , 0  , 439  , 133, 75 ), // #448
  INST(Movsd            , ExtMov             , O(F20F00,10,_,_,_,_,_,_  ), O(F20F00,11,_,_,_,_,_,_  ), 5  , 62 , 6454 , 134, 4  ), // #449
  INST(Movshdup         , ExtRm              , O(F30F00,16,_,_,_,_,_,_  ), 0                         , 6  , 0  , 6461 , 5  , 6  ), // #450
  INST(Movsldup         , ExtRm              , O(F30F00,12,_,_,_,_,_,_  ), 0                         , 6  , 0  , 6471 , 5  , 6  ), // #451
  INST(Movss            , ExtMov             , O(F30F00,10,_,_,_,_,_,_  ), O(F30F00,11,_,_,_,_,_,_  ), 6  , 63 , 6481 , 135, 5  ), // #452
  INST(Movsx            , X86MovsxMovzx      , O(000F00,BE,_,_,x,_,_,_  ), 0                         , 4  , 0  , 2028 , 136, 0  ), // #453
  INST(Movsxd           , X86Rm              , O(000000,63,_,_,x,_,_,_  ), 0                         , 0  , 0  , 2034 , 137, 0  ), // #454
  INST(Movupd           , ExtMov             , O(660F00,10,_,_,_,_,_,_  ), O(660F00,11,_,_,_,_,_,_  ), 3  , 64 , 6488 , 118, 4  ), // #455
  INST(Movups           , ExtMov             , O(000F00,10,_,_,_,_,_,_  ), O(000F00,11,_,_,_,_,_,_  ), 4  , 65 , 6496 , 118, 5  ), // #456
  INST(Movzx            , X86MovsxMovzx      , O(000F00,B6,_,_,x,_,_,_  ), 0                         , 4  , 0  , 2041 , 136, 0  ), // #457
  INST(Mpsadbw          , ExtRmi             , O(660F3A,42,_,_,_,_,_,_  ), 0                         , 8  , 0  , 6504 , 8  , 12 ), // #458
  INST(Mul              , X86M_GPB_MulDiv    , O(000000,F6,4,_,x,_,_,_  ), 0                         , 9  , 0  , 828  , 54 , 1  ), // #459
  INST(Mulpd            , ExtRm              , O(660F00,59,_,_,_,_,_,_  ), 0                         , 3  , 0  , 6558 , 5  , 4  ), // #460
  INST(Mulps            , ExtRm              , O(000F00,59,_,_,_,_,_,_  ), 0                         , 4  , 0  , 6565 , 5  , 5  ), // #461
  INST(Mulsd            , ExtRm              , O(F20F00,59,_,_,_,_,_,_  ), 0                         , 5  , 0  , 6572 , 6  , 4  ), // #462
  INST(Mulss            , ExtRm              , O(F30F00,59,_,_,_,_,_,_  ), 0                         , 6  , 0  , 6579 , 7  , 5  ), // #463
  INST(Mulx             , VexRvm_ZDX_Wx      , V(F20F38,F6,_,0,x,_,_,_  ), 0                         , 85 , 0  , 2047 , 138, 85 ), // #464
  INST(Mwait            , X86Op              , O(000F01,C9,_,_,_,_,_,_  ), 0                         , 21 , 0  , 3241 , 139, 79 ), // #465
  INST(Mwaitx           , X86Op              , O(000F01,FB,_,_,_,_,_,_  ), 0                         , 21 , 0  , 2052 , 140, 80 ), // #466
  INST(Neg              , X86M_GPB           , O(000000,F6,3,_,x,_,_,_  ), 0                         , 76 , 0  , 2059 , 141, 1  ), // #467
  INST(Nop              , X86M_Nop           , O(000000,90,_,_,_,_,_,_  ), 0                         , 0  , 0  , 959  , 142, 0  ), // #468
  INST(Not              , X86M_GPB           , O(000000,F6,2,_,x,_,_,_  ), 0                         , 1  , 0  , 2063 , 141, 0  ), // #469
  INST(Or               , X86Arith           , O(000000,08,1,_,x,_,_,_  ), 0                         , 31 , 0  , 3237 , 143, 1  ), // #470
  INST(Orpd             , ExtRm              , O(660F00,56,_,_,_,_,_,_  ), 0                         , 3  , 0  , 10348, 11 , 4  ), // #471
  INST(Orps             , ExtRm              , O(000F00,56,_,_,_,_,_,_  ), 0                         , 4  , 0  , 10355, 11 , 5  ), // #472
  INST(Out              , X86Out             , O(000000,EE,_,_,_,_,_,_  ), O(000000,E6,_,_,_,_,_,_  ), 0  , 66 , 2067 , 144, 0  ), // #473
  INST(Outs             , X86Outs            , O(000000,6E,_,_,_,_,_,_  ), 0                         , 0  , 0  , 2071 , 145, 0  ), // #474
  INST(Pabsb            , ExtRm_P            , O(000F38,1C,_,_,_,_,_,_  ), 0                         , 84 , 0  , 6661 , 146, 86 ), // #475
  INST(Pabsd            , ExtRm_P            , O(000F38,1E,_,_,_,_,_,_  ), 0                         , 84 , 0  , 6668 , 146, 86 ), // #476
  INST(Pabsw            , ExtRm_P            , O(000F38,1D,_,_,_,_,_,_  ), 0                         , 84 , 0  , 6682 , 146, 86 ), // #477
  INST(Packssdw         , ExtRm_P            , O(000F00,6B,_,_,_,_,_,_  ), 0                         , 4  , 0  , 6689 , 146, 82 ), // #478
  INST(Packsswb         , ExtRm_P            , O(000F00,63,_,_,_,_,_,_  ), 0                         , 4  , 0  , 6699 , 146, 82 ), // #479
  INST(Packusdw         , ExtRm              , O(660F38,2B,_,_,_,_,_,_  ), 0                         , 2  , 0  , 6709 , 5  , 12 ), // #480
  INST(Packuswb         , ExtRm_P            , O(000F00,67,_,_,_,_,_,_  ), 0                         , 4  , 0  , 6719 , 146, 82 ), // #481
  INST(Paddb            , ExtRm_P            , O(000F00,FC,_,_,_,_,_,_  ), 0                         , 4  , 0  , 6729 , 146, 82 ), // #482
  INST(Paddd            , ExtRm_P            , O(000F00,FE,_,_,_,_,_,_  ), 0                         , 4  , 0  , 6736 , 146, 82 ), // #483
  INST(Paddq            , ExtRm_P            , O(000F00,D4,_,_,_,_,_,_  ), 0                         , 4  , 0  , 6743 , 146, 4  ), // #484
  INST(Paddsb           , ExtRm_P            , O(000F00,EC,_,_,_,_,_,_  ), 0                         , 4  , 0  , 6750 , 146, 82 ), // #485
  INST(Paddsw           , ExtRm_P            , O(000F00,ED,_,_,_,_,_,_  ), 0                         , 4  , 0  , 6758 , 146, 82 ), // #486
  INST(Paddusb          , ExtRm_P            , O(000F00,DC,_,_,_,_,_,_  ), 0                         , 4  , 0  , 6766 , 146, 82 ), // #487
  INST(Paddusw          , ExtRm_P            , O(000F00,DD,_,_,_,_,_,_  ), 0                         , 4  , 0  , 6775 , 146, 82 ), // #488
  INST(Paddw            , ExtRm_P            , O(000F00,FD,_,_,_,_,_,_  ), 0                         , 4  , 0  , 6784 , 146, 82 ), // #489
  INST(Palignr          , ExtRmi_P           , O(000F3A,0F,_,_,_,_,_,_  ), 0                         , 86 , 0  , 6791 , 147, 6  ), // #490
  INST(Pand             , ExtRm_P            , O(000F00,DB,_,_,_,_,_,_  ), 0                         , 4  , 0  , 6800 , 148, 82 ), // #491
  INST(Pandn            , ExtRm_P            , O(000F00,DF,_,_,_,_,_,_  ), 0                         , 4  , 0  , 6813 , 149, 82 ), // #492
  INST(Pause            , X86Op              , O(F30000,90,_,_,_,_,_,_  ), 0                         , 87 , 0  , 3195 , 30 , 0  ), // #493
  INST(Pavgb            , ExtRm_P            , O(000F00,E0,_,_,_,_,_,_  ), 0                         , 4  , 0  , 6843 , 146, 87 ), // #494
  INST(Pavgusb          , Ext3dNow           , O(000F0F,BF,_,_,_,_,_,_  ), 0                         , 88 , 0  , 2076 , 150, 51 ), // #495
  INST(Pavgw            , ExtRm_P            , O(000F00,E3,_,_,_,_,_,_  ), 0                         , 4  , 0  , 6850 , 146, 87 ), // #496
  INST(Pblendvb         , ExtRm_XMM0         , O(660F38,10,_,_,_,_,_,_  ), 0                         , 2  , 0  , 6906 , 15 , 12 ), // #497
  INST(Pblendw          , ExtRmi             , O(660F3A,0E,_,_,_,_,_,_  ), 0                         , 8  , 0  , 6916 , 8  , 12 ), // #498
  INST(Pclmulqdq        , ExtRmi             , O(660F3A,44,_,_,_,_,_,_  ), 0                         , 8  , 0  , 7009 , 8  , 88 ), // #499
  INST(Pcmpeqb          , ExtRm_P            , O(000F00,74,_,_,_,_,_,_  ), 0                         , 4  , 0  , 7041 , 149, 82 ), // #500
  INST(Pcmpeqd          , ExtRm_P            , O(000F00,76,_,_,_,_,_,_  ), 0                         , 4  , 0  , 7050 , 149, 82 ), // #501
  INST(Pcmpeqq          , ExtRm              , O(660F38,29,_,_,_,_,_,_  ), 0                         , 2  , 0  , 7059 , 151, 12 ), // #502
  INST(Pcmpeqw          , ExtRm_P            , O(000F00,75,_,_,_,_,_,_  ), 0                         , 4  , 0  , 7068 , 149, 82 ), // #503
  INST(Pcmpestri        , ExtRmi             , O(660F3A,61,_,_,_,_,_,_  ), 0                         , 8  , 0  , 7077 , 152, 89 ), // #504
  INST(Pcmpestrm        , ExtRmi             , O(660F3A,60,_,_,_,_,_,_  ), 0                         , 8  , 0  , 7088 , 153, 89 ), // #505
  INST(Pcmpgtb          , ExtRm_P            , O(000F00,64,_,_,_,_,_,_  ), 0                         , 4  , 0  , 7099 , 149, 82 ), // #506
  INST(Pcmpgtd          , ExtRm_P            , O(000F00,66,_,_,_,_,_,_  ), 0                         , 4  , 0  , 7108 , 149, 82 ), // #507
  INST(Pcmpgtq          , ExtRm              , O(660F38,37,_,_,_,_,_,_  ), 0                         , 2  , 0  , 7117 , 151, 44 ), // #508
  INST(Pcmpgtw          , ExtRm_P            , O(000F00,65,_,_,_,_,_,_  ), 0                         , 4  , 0  , 7126 , 149, 82 ), // #509
  INST(Pcmpistri        , ExtRmi             , O(660F3A,63,_,_,_,_,_,_  ), 0                         , 8  , 0  , 7135 , 154, 89 ), // #510
  INST(Pcmpistrm        , ExtRmi             , O(660F3A,62,_,_,_,_,_,_  ), 0                         , 8  , 0  , 7146 , 155, 89 ), // #511
  INST(Pconfig          , X86Op              , O(000F01,C5,_,_,_,_,_,_  ), 0                         , 21 , 0  , 2084 , 30 , 90 ), // #512
  INST(Pdep             , VexRvm_Wx          , V(F20F38,F5,_,0,x,_,_,_  ), 0                         , 85 , 0  , 2092 , 10 , 85 ), // #513
  INST(Pext             , VexRvm_Wx          , V(F30F38,F5,_,0,x,_,_,_  ), 0                         , 89 , 0  , 2097 , 10 , 85 ), // #514
  INST(Pextrb           , ExtExtract         , O(000F3A,14,_,_,_,_,_,_  ), 0                         , 86 , 0  , 7633 , 156, 12 ), // #515
  INST(Pextrd           , ExtExtract         , O(000F3A,16,_,_,_,_,_,_  ), 0                         , 86 , 0  , 7641 , 58 , 12 ), // #516
  INST(Pextrq           , ExtExtract         , O(000F3A,16,_,_,1,_,_,_  ), 0                         , 90 , 0  , 7649 , 157, 12 ), // #517
  INST(Pextrw           , ExtPextrw          , O(000F00,C5,_,_,_,_,_,_  ), O(000F3A,15,_,_,_,_,_,_  ), 4  , 67 , 7657 , 158, 91 ), // #518
  INST(Pf2id            , Ext3dNow           , O(000F0F,1D,_,_,_,_,_,_  ), 0                         , 88 , 0  , 2102 , 150, 51 ), // #519
  INST(Pf2iw            , Ext3dNow           , O(000F0F,1C,_,_,_,_,_,_  ), 0                         , 88 , 0  , 2108 , 150, 92 ), // #520
  INST(Pfacc            , Ext3dNow           , O(000F0F,AE,_,_,_,_,_,_  ), 0                         , 88 , 0  , 2114 , 150, 51 ), // #521
  INST(Pfadd            , Ext3dNow           , O(000F0F,9E,_,_,_,_,_,_  ), 0                         , 88 , 0  , 2120 , 150, 51 ), // #522
  INST(Pfcmpeq          , Ext3dNow           , O(000F0F,B0,_,_,_,_,_,_  ), 0                         , 88 , 0  , 2126 , 150, 51 ), // #523
  INST(Pfcmpge          , Ext3dNow           , O(000F0F,90,_,_,_,_,_,_  ), 0                         , 88 , 0  , 2134 , 150, 51 ), // #524
  INST(Pfcmpgt          , Ext3dNow           , O(000F0F,A0,_,_,_,_,_,_  ), 0                         , 88 , 0  , 2142 , 150, 51 ), // #525
  INST(Pfmax            , Ext3dNow           , O(000F0F,A4,_,_,_,_,_,_  ), 0                         , 88 , 0  , 2150 , 150, 51 ), // #526
  INST(Pfmin            , Ext3dNow           , O(000F0F,94,_,_,_,_,_,_  ), 0                         , 88 , 0  , 2156 , 150, 51 ), // #527
  INST(Pfmul            , Ext3dNow           , O(000F0F,B4,_,_,_,_,_,_  ), 0                         , 88 , 0  , 2162 , 150, 51 ), // #528
  INST(Pfnacc           , Ext3dNow           , O(000F0F,8A,_,_,_,_,_,_  ), 0                         , 88 , 0  , 2168 , 150, 92 ), // #529
  INST(Pfpnacc          , Ext3dNow           , O(000F0F,8E,_,_,_,_,_,_  ), 0                         , 88 , 0  , 2175 , 150, 92 ), // #530
  INST(Pfrcp            , Ext3dNow           , O(000F0F,96,_,_,_,_,_,_  ), 0                         , 88 , 0  , 2183 , 150, 51 ), // #531
  INST(Pfrcpit1         , Ext3dNow           , O(000F0F,A6,_,_,_,_,_,_  ), 0                         , 88 , 0  , 2189 , 150, 51 ), // #532
  INST(Pfrcpit2         , Ext3dNow           , O(000F0F,B6,_,_,_,_,_,_  ), 0                         , 88 , 0  , 2198 , 150, 51 ), // #533
  INST(Pfrcpv           , Ext3dNow           , O(000F0F,86,_,_,_,_,_,_  ), 0                         , 88 , 0  , 2207 , 150, 93 ), // #534
  INST(Pfrsqit1         , Ext3dNow           , O(000F0F,A7,_,_,_,_,_,_  ), 0                         , 88 , 0  , 2214 , 150, 51 ), // #535
  INST(Pfrsqrt          , Ext3dNow           , O(000F0F,97,_,_,_,_,_,_  ), 0                         , 88 , 0  , 2223 , 150, 51 ), // #536
  INST(Pfrsqrtv         , Ext3dNow           , O(000F0F,87,_,_,_,_,_,_  ), 0                         , 88 , 0  , 2231 , 150, 93 ), // #537
  INST(Pfsub            , Ext3dNow           , O(000F0F,9A,_,_,_,_,_,_  ), 0                         , 88 , 0  , 2240 , 150, 51 ), // #538
  INST(Pfsubr           , Ext3dNow           , O(000F0F,AA,_,_,_,_,_,_  ), 0                         , 88 , 0  , 2246 , 150, 51 ), // #539
  INST(Phaddd           , ExtRm_P            , O(000F38,02,_,_,_,_,_,_  ), 0                         , 84 , 0  , 7736 , 146, 86 ), // #540
  INST(Phaddsw          , ExtRm_P            , O(000F38,03,_,_,_,_,_,_  ), 0                         , 84 , 0  , 7753 , 146, 86 ), // #541
  INST(Phaddw           , ExtRm_P            , O(000F38,01,_,_,_,_,_,_  ), 0                         , 84 , 0  , 7822 , 146, 86 ), // #542
  INST(Phminposuw       , ExtRm              , O(660F38,41,_,_,_,_,_,_  ), 0                         , 2  , 0  , 7848 , 5  , 12 ), // #543
  INST(Phsubd           , ExtRm_P            , O(000F38,06,_,_,_,_,_,_  ), 0                         , 84 , 0  , 7869 , 146, 86 ), // #544
  INST(Phsubsw          , ExtRm_P            , O(000F38,07,_,_,_,_,_,_  ), 0                         , 84 , 0  , 7886 , 146, 86 ), // #545
  INST(Phsubw           , ExtRm_P            , O(000F38,05,_,_,_,_,_,_  ), 0                         , 84 , 0  , 7895 , 146, 86 ), // #546
  INST(Pi2fd            , Ext3dNow           , O(000F0F,0D,_,_,_,_,_,_  ), 0                         , 88 , 0  , 2253 , 150, 51 ), // #547
  INST(Pi2fw            , Ext3dNow           , O(000F0F,0C,_,_,_,_,_,_  ), 0                         , 88 , 0  , 2259 , 150, 92 ), // #548
  INST(Pinsrb           , ExtRmi             , O(660F3A,20,_,_,_,_,_,_  ), 0                         , 8  , 0  , 7912 , 159, 12 ), // #549
  INST(Pinsrd           , ExtRmi             , O(660F3A,22,_,_,_,_,_,_  ), 0                         , 8  , 0  , 7920 , 160, 12 ), // #550
  INST(Pinsrq           , ExtRmi             , O(660F3A,22,_,_,1,_,_,_  ), 0                         , 91 , 0  , 7928 , 161, 12 ), // #551
  INST(Pinsrw           , ExtRmi_P           , O(000F00,C4,_,_,_,_,_,_  ), 0                         , 4  , 0  , 7936 , 162, 87 ), // #552
  INST(Pmaddubsw        , ExtRm_P            , O(000F38,04,_,_,_,_,_,_  ), 0                         , 84 , 0  , 8106 , 146, 86 ), // #553
  INST(Pmaddwd          , ExtRm_P            , O(000F00,F5,_,_,_,_,_,_  ), 0                         , 4  , 0  , 8117 , 146, 82 ), // #554
  INST(Pmaxsb           , ExtRm              , O(660F38,3C,_,_,_,_,_,_  ), 0                         , 2  , 0  , 8148 , 11 , 12 ), // #555
  INST(Pmaxsd           , ExtRm              , O(660F38,3D,_,_,_,_,_,_  ), 0                         , 2  , 0  , 8156 , 11 , 12 ), // #556
  INST(Pmaxsw           , ExtRm_P            , O(000F00,EE,_,_,_,_,_,_  ), 0                         , 4  , 0  , 8172 , 148, 87 ), // #557
  INST(Pmaxub           , ExtRm_P            , O(000F00,DE,_,_,_,_,_,_  ), 0                         , 4  , 0  , 8180 , 148, 87 ), // #558
  INST(Pmaxud           , ExtRm              , O(660F38,3F,_,_,_,_,_,_  ), 0                         , 2  , 0  , 8188 , 11 , 12 ), // #559
  INST(Pmaxuw           , ExtRm              , O(660F38,3E,_,_,_,_,_,_  ), 0                         , 2  , 0  , 8204 , 11 , 12 ), // #560
  INST(Pminsb           , ExtRm              , O(660F38,38,_,_,_,_,_,_  ), 0                         , 2  , 0  , 8212 , 11 , 12 ), // #561
  INST(Pminsd           , ExtRm              , O(660F38,39,_,_,_,_,_,_  ), 0                         , 2  , 0  , 8220 , 11 , 12 ), // #562
  INST(Pminsw           , ExtRm_P            , O(000F00,EA,_,_,_,_,_,_  ), 0                         , 4  , 0  , 8236 , 148, 87 ), // #563
  INST(Pminub           , ExtRm_P            , O(000F00,DA,_,_,_,_,_,_  ), 0                         , 4  , 0  , 8244 , 148, 87 ), // #564
  INST(Pminud           , ExtRm              , O(660F38,3B,_,_,_,_,_,_  ), 0                         , 2  , 0  , 8252 , 11 , 12 ), // #565
  INST(Pminuw           , ExtRm              , O(660F38,3A,_,_,_,_,_,_  ), 0                         , 2  , 0  , 8268 , 11 , 12 ), // #566
  INST(Pmovmskb         , ExtRm_P            , O(000F00,D7,_,_,_,_,_,_  ), 0                         , 4  , 0  , 8346 , 163, 87 ), // #567
  INST(Pmovsxbd         , ExtRm              , O(660F38,21,_,_,_,_,_,_  ), 0                         , 2  , 0  , 8443 , 7  , 12 ), // #568
  INST(Pmovsxbq         , ExtRm              , O(660F38,22,_,_,_,_,_,_  ), 0                         , 2  , 0  , 8453 , 164, 12 ), // #569
  INST(Pmovsxbw         , ExtRm              , O(660F38,20,_,_,_,_,_,_  ), 0                         , 2  , 0  , 8463 , 6  , 12 ), // #570
  INST(Pmovsxdq         , ExtRm              , O(660F38,25,_,_,_,_,_,_  ), 0                         , 2  , 0  , 8473 , 6  , 12 ), // #571
  INST(Pmovsxwd         , ExtRm              , O(660F38,23,_,_,_,_,_,_  ), 0                         , 2  , 0  , 8483 , 6  , 12 ), // #572
  INST(Pmovsxwq         , ExtRm              , O(660F38,24,_,_,_,_,_,_  ), 0                         , 2  , 0  , 8493 , 7  , 12 ), // #573
  INST(Pmovzxbd         , ExtRm              , O(660F38,31,_,_,_,_,_,_  ), 0                         , 2  , 0  , 8580 , 7  , 12 ), // #574
  INST(Pmovzxbq         , ExtRm              , O(660F38,32,_,_,_,_,_,_  ), 0                         , 2  , 0  , 8590 , 164, 12 ), // #575
  INST(Pmovzxbw         , ExtRm              , O(660F38,30,_,_,_,_,_,_  ), 0                         , 2  , 0  , 8600 , 6  , 12 ), // #576
  INST(Pmovzxdq         , ExtRm              , O(660F38,35,_,_,_,_,_,_  ), 0                         , 2  , 0  , 8610 , 6  , 12 ), // #577
  INST(Pmovzxwd         , ExtRm              , O(660F38,33,_,_,_,_,_,_  ), 0                         , 2  , 0  , 8620 , 6  , 12 ), // #578
  INST(Pmovzxwq         , ExtRm              , O(660F38,34,_,_,_,_,_,_  ), 0                         , 2  , 0  , 8630 , 7  , 12 ), // #579
  INST(Pmuldq           , ExtRm              , O(660F38,28,_,_,_,_,_,_  ), 0                         , 2  , 0  , 8640 , 5  , 12 ), // #580
  INST(Pmulhrsw         , ExtRm_P            , O(000F38,0B,_,_,_,_,_,_  ), 0                         , 84 , 0  , 8648 , 146, 86 ), // #581
  INST(Pmulhrw          , Ext3dNow           , O(000F0F,B7,_,_,_,_,_,_  ), 0                         , 88 , 0  , 2265 , 150, 51 ), // #582
  INST(Pmulhuw          , ExtRm_P            , O(000F00,E4,_,_,_,_,_,_  ), 0                         , 4  , 0  , 8658 , 146, 87 ), // #583
  INST(Pmulhw           , ExtRm_P            , O(000F00,E5,_,_,_,_,_,_  ), 0                         , 4  , 0  , 8667 , 146, 82 ), // #584
  INST(Pmulld           , ExtRm              , O(660F38,40,_,_,_,_,_,_  ), 0                         , 2  , 0  , 8675 , 5  , 12 ), // #585
  INST(Pmullw           , ExtRm_P            , O(000F00,D5,_,_,_,_,_,_  ), 0                         , 4  , 0  , 8691 , 146, 82 ), // #586
  INST(Pmuludq          , ExtRm_P            , O(000F00,F4,_,_,_,_,_,_  ), 0                         , 4  , 0  , 8714 , 146, 4  ), // #587
  INST(Pop              , X86Pop             , O(000000,8F,0,_,_,_,_,_  ), O(000000,58,_,_,_,_,_,_  ), 0  , 68 , 2273 , 165, 0  ), // #588
  INST(Popa             , X86Op              , O(660000,61,_,_,_,_,_,_  ), 0                         , 19 , 0  , 2277 , 81 , 0  ), // #589
  INST(Popad            , X86Op              , O(000000,61,_,_,_,_,_,_  ), 0                         , 0  , 0  , 2282 , 81 , 0  ), // #590
  INST(Popcnt           , X86Rm_Raw66H       , O(F30F00,B8,_,_,x,_,_,_  ), 0                         , 6  , 0  , 2288 , 22 , 94 ), // #591
  INST(Popf             , X86Op              , O(660000,9D,_,_,_,_,_,_  ), 0                         , 19 , 0  , 2295 , 30 , 95 ), // #592
  INST(Popfd            , X86Op              , O(000000,9D,_,_,_,_,_,_  ), 0                         , 0  , 0  , 2300 , 81 , 95 ), // #593
  INST(Popfq            , X86Op              , O(000000,9D,_,_,_,_,_,_  ), 0                         , 0  , 0  , 2306 , 33 , 95 ), // #594
  INST(Por              , ExtRm_P            , O(000F00,EB,_,_,_,_,_,_  ), 0                         , 4  , 0  , 8759 , 148, 82 ), // #595
  INST(Prefetch         , X86M_Only          , O(000F00,0D,0,_,_,_,_,_  ), 0                         , 4  , 0  , 2312 , 31 , 51 ), // #596
  INST(Prefetchnta      , X86M_Only          , O(000F00,18,0,_,_,_,_,_  ), 0                         , 4  , 0  , 2321 , 31 , 77 ), // #597
  INST(Prefetcht0       , X86M_Only          , O(000F00,18,1,_,_,_,_,_  ), 0                         , 29 , 0  , 2333 , 31 , 77 ), // #598
  INST(Prefetcht1       , X86M_Only          , O(000F00,18,2,_,_,_,_,_  ), 0                         , 77 , 0  , 2344 , 31 , 77 ), // #599
  INST(Prefetcht2       , X86M_Only          , O(000F00,18,3,_,_,_,_,_  ), 0                         , 79 , 0  , 2355 , 31 , 77 ), // #600
  INST(Prefetchw        , X86M_Only          , O(000F00,0D,1,_,_,_,_,_  ), 0                         , 29 , 0  , 2366 , 31 , 96 ), // #601
  INST(Prefetchwt1      , X86M_Only          , O(000F00,0D,2,_,_,_,_,_  ), 0                         , 77 , 0  , 2376 , 31 , 97 ), // #602
  INST(Psadbw           , ExtRm_P            , O(000F00,F6,_,_,_,_,_,_  ), 0                         , 4  , 0  , 4272 , 146, 87 ), // #603
  INST(Pshufb           , ExtRm_P            , O(000F38,00,_,_,_,_,_,_  ), 0                         , 84 , 0  , 9085 , 146, 86 ), // #604
  INST(Pshufd           , ExtRmi             , O(660F00,70,_,_,_,_,_,_  ), 0                         , 3  , 0  , 9106 , 8  , 4  ), // #605
  INST(Pshufhw          , ExtRmi             , O(F30F00,70,_,_,_,_,_,_  ), 0                         , 6  , 0  , 9114 , 8  , 4  ), // #606
  INST(Pshuflw          , ExtRmi             , O(F20F00,70,_,_,_,_,_,_  ), 0                         , 5  , 0  , 9123 , 8  , 4  ), // #607
  INST(Pshufw           , ExtRmi_P           , O(000F00,70,_,_,_,_,_,_  ), 0                         , 4  , 0  , 2388 , 166, 77 ), // #608
  INST(Psignb           , ExtRm_P            , O(000F38,08,_,_,_,_,_,_  ), 0                         , 84 , 0  , 9132 , 146, 86 ), // #609
  INST(Psignd           , ExtRm_P            , O(000F38,0A,_,_,_,_,_,_  ), 0                         , 84 , 0  , 9140 , 146, 86 ), // #610
  INST(Psignw           , ExtRm_P            , O(000F38,09,_,_,_,_,_,_  ), 0                         , 84 , 0  , 9148 , 146, 86 ), // #611
  INST(Pslld            , ExtRmRi_P          , O(000F00,F2,_,_,_,_,_,_  ), O(000F00,72,6,_,_,_,_,_  ), 4  , 69 , 9156 , 167, 82 ), // #612
  INST(Pslldq           , ExtRmRi            , 0                         , O(660F00,73,7,_,_,_,_,_  ), 0  , 70 , 9163 , 168, 4  ), // #613
  INST(Psllq            , ExtRmRi_P          , O(000F00,F3,_,_,_,_,_,_  ), O(000F00,73,6,_,_,_,_,_  ), 4  , 71 , 9171 , 167, 82 ), // #614
  INST(Psllw            , ExtRmRi_P          , O(000F00,F1,_,_,_,_,_,_  ), O(000F00,71,6,_,_,_,_,_  ), 4  , 72 , 9202 , 167, 82 ), // #615
  INST(Psmash           , X86Op              , O(F30F01,FF,_,_,_,_,_,_  ), 0                         , 25 , 0  , 2395 , 33 , 98 ), // #616
  INST(Psrad            , ExtRmRi_P          , O(000F00,E2,_,_,_,_,_,_  ), O(000F00,72,4,_,_,_,_,_  ), 4  , 73 , 9209 , 167, 82 ), // #617
  INST(Psraw            , ExtRmRi_P          , O(000F00,E1,_,_,_,_,_,_  ), O(000F00,71,4,_,_,_,_,_  ), 4  , 74 , 9247 , 167, 82 ), // #618
  INST(Psrld            , ExtRmRi_P          , O(000F00,D2,_,_,_,_,_,_  ), O(000F00,72,2,_,_,_,_,_  ), 4  , 75 , 9254 , 167, 82 ), // #619
  INST(Psrldq           , ExtRmRi            , 0                         , O(660F00,73,3,_,_,_,_,_  ), 0  , 76 , 9261 , 168, 4  ), // #620
  INST(Psrlq            , ExtRmRi_P          , O(000F00,D3,_,_,_,_,_,_  ), O(000F00,73,2,_,_,_,_,_  ), 4  , 77 , 9269 , 167, 82 ), // #621
  INST(Psrlw            , ExtRmRi_P          , O(000F00,D1,_,_,_,_,_,_  ), O(000F00,71,2,_,_,_,_,_  ), 4  , 78 , 9300 , 167, 82 ), // #622
  INST(Psubb            , ExtRm_P            , O(000F00,F8,_,_,_,_,_,_  ), 0                         , 4  , 0  , 9307 , 149, 82 ), // #623
  INST(Psubd            , ExtRm_P            , O(000F00,FA,_,_,_,_,_,_  ), 0                         , 4  , 0  , 9314 , 149, 82 ), // #624
  INST(Psubq            , ExtRm_P            , O(000F00,FB,_,_,_,_,_,_  ), 0                         , 4  , 0  , 9321 , 149, 4  ), // #625
  INST(Psubsb           , ExtRm_P            , O(000F00,E8,_,_,_,_,_,_  ), 0                         , 4  , 0  , 9328 , 149, 82 ), // #626
  INST(Psubsw           , ExtRm_P            , O(000F00,E9,_,_,_,_,_,_  ), 0                         , 4  , 0  , 9336 , 149, 82 ), // #627
  INST(Psubusb          , ExtRm_P            , O(000F00,D8,_,_,_,_,_,_  ), 0                         , 4  , 0  , 9344 , 149, 82 ), // #628
  INST(Psubusw          , ExtRm_P            , O(000F00,D9,_,_,_,_,_,_  ), 0                         , 4  , 0  , 9353 , 149, 82 ), // #629
  INST(Psubw            , ExtRm_P            , O(000F00,F9,_,_,_,_,_,_  ), 0                         , 4  , 0  , 9362 , 149, 82 ), // #630
  INST(Pswapd           , Ext3dNow           , O(000F0F,BB,_,_,_,_,_,_  ), 0                         , 88 , 0  , 2402 , 150, 92 ), // #631
  INST(Ptest            , ExtRm              , O(660F38,17,_,_,_,_,_,_  ), 0                         , 2  , 0  , 9391 , 5  , 99 ), // #632
  INST(Ptwrite          , X86M               , O(F30F00,AE,4,_,_,_,_,_  ), 0                         , 92 , 0  , 2409 , 169, 100), // #633
  INST(Punpckhbw        , ExtRm_P            , O(000F00,68,_,_,_,_,_,_  ), 0                         , 4  , 0  , 9474 , 146, 82 ), // #634
  INST(Punpckhdq        , ExtRm_P            , O(000F00,6A,_,_,_,_,_,_  ), 0                         , 4  , 0  , 9485 , 146, 82 ), // #635
  INST(Punpckhqdq       , ExtRm              , O(660F00,6D,_,_,_,_,_,_  ), 0                         , 3  , 0  , 9496 , 5  , 4  ), // #636
  INST(Punpckhwd        , ExtRm_P            , O(000F00,69,_,_,_,_,_,_  ), 0                         , 4  , 0  , 9508 , 146, 82 ), // #637
  INST(Punpcklbw        , ExtRm_P            , O(000F00,60,_,_,_,_,_,_  ), 0                         , 4  , 0  , 9519 , 170, 82 ), // #638
  INST(Punpckldq        , ExtRm_P            , O(000F00,62,_,_,_,_,_,_  ), 0                         , 4  , 0  , 9530 , 170, 82 ), // #639
  INST(Punpcklqdq       , ExtRm              , O(660F00,6C,_,_,_,_,_,_  ), 0                         , 3  , 0  , 9541 , 5  , 4  ), // #640
  INST(Punpcklwd        , ExtRm_P            , O(000F00,61,_,_,_,_,_,_  ), 0                         , 4  , 0  , 9553 , 170, 82 ), // #641
  INST(Push             , X86Push            , O(000000,FF,6,_,_,_,_,_  ), O(000000,50,_,_,_,_,_,_  ), 32 , 79 , 2417 , 171, 0  ), // #642
  INST(Pusha            , X86Op              , O(660000,60,_,_,_,_,_,_  ), 0                         , 19 , 0  , 2422 , 81 , 0  ), // #643
  INST(Pushad           , X86Op              , O(000000,60,_,_,_,_,_,_  ), 0                         , 0  , 0  , 2428 , 81 , 0  ), // #644
  INST(Pushf            , X86Op              , O(660000,9C,_,_,_,_,_,_  ), 0                         , 19 , 0  , 2435 , 30 , 101), // #645
  INST(Pushfd           , X86Op              , O(000000,9C,_,_,_,_,_,_  ), 0                         , 0  , 0  , 2441 , 81 , 101), // #646
  INST(Pushfq           , X86Op              , O(000000,9C,_,_,_,_,_,_  ), 0                         , 0  , 0  , 2448 , 33 , 101), // #647
  INST(Pvalidate        , X86Op              , O(F20F01,FF,_,_,_,_,_,_  ), 0                         , 93 , 0  , 2455 , 30 , 102), // #648
  INST(Pxor             , ExtRm_P            , O(000F00,EF,_,_,_,_,_,_  ), 0                         , 4  , 0  , 9564 , 149, 82 ), // #649
  INST(Rcl              , X86Rot             , O(000000,D0,2,_,x,_,_,_  ), 0                         , 1  , 0  , 2465 , 172, 103), // #650
  INST(Rcpps            , ExtRm              , O(000F00,53,_,_,_,_,_,_  ), 0                         , 4  , 0  , 9692 , 5  , 5  ), // #651
  INST(Rcpss            , ExtRm              , O(F30F00,53,_,_,_,_,_,_  ), 0                         , 6  , 0  , 9699 , 7  , 5  ), // #652
  INST(Rcr              , X86Rot             , O(000000,D0,3,_,x,_,_,_  ), 0                         , 76 , 0  , 2469 , 172, 103), // #653
  INST(Rdfsbase         , X86M               , O(F30F00,AE,0,_,x,_,_,_  ), 0                         , 6  , 0  , 2473 , 173, 104), // #654
  INST(Rdgsbase         , X86M               , O(F30F00,AE,1,_,x,_,_,_  ), 0                         , 94 , 0  , 2482 , 173, 104), // #655
  INST(Rdmsr            , X86Op              , O(000F00,32,_,_,_,_,_,_  ), 0                         , 4  , 0  , 2491 , 174, 105), // #656
  INST(Rdpid            , X86R_Native        , O(F30F00,C7,7,_,_,_,_,_  ), 0                         , 95 , 0  , 2497 , 175, 106), // #657
  INST(Rdpkru           , X86Op              , O(000F01,EE,_,_,_,_,_,_  ), 0                         , 21 , 0  , 2503 , 174, 107), // #658
  INST(Rdpmc            , X86Op              , O(000F00,33,_,_,_,_,_,_  ), 0                         , 4  , 0  , 2510 , 174, 0  ), // #659
  INST(Rdpru            , X86Op              , O(000F01,FD,_,_,_,_,_,_  ), 0                         , 21 , 0  , 2516 , 174, 108), // #660
  INST(Rdrand           , X86M               , O(000F00,C7,6,_,x,_,_,_  ), 0                         , 81 , 0  , 2522 , 23 , 109), // #661
  INST(Rdseed           , X86M               , O(000F00,C7,7,_,x,_,_,_  ), 0                         , 22 , 0  , 2529 , 23 , 110), // #662
  INST(Rdsspd           , X86M               , O(F30F00,1E,1,_,_,_,_,_  ), 0                         , 94 , 0  , 2536 , 76 , 56 ), // #663
  INST(Rdsspq           , X86M               , O(F30F00,1E,1,_,_,_,_,_  ), 0                         , 94 , 0  , 2543 , 77 , 56 ), // #664
  INST(Rdtsc            , X86Op              , O(000F00,31,_,_,_,_,_,_  ), 0                         , 4  , 0  , 2550 , 28 , 111), // #665
  INST(Rdtscp           , X86Op              , O(000F01,F9,_,_,_,_,_,_  ), 0                         , 21 , 0  , 2556 , 174, 112), // #666
  INST(Ret              , X86Ret             , O(000000,C2,_,_,_,_,_,_  ), 0                         , 0  , 0  , 3072 , 176, 0  ), // #667
  INST(Retf             , X86Ret             , O(000000,CA,_,_,x,_,_,_  ), 0                         , 0  , 0  , 2563 , 177, 0  ), // #668
  INST(Rmpadjust        , X86Op              , O(F30F01,FE,_,_,_,_,_,_  ), 0                         , 25 , 0  , 2568 , 33 , 98 ), // #669
  INST(Rmpupdate        , X86Op              , O(F20F01,FE,_,_,_,_,_,_  ), 0                         , 93 , 0  , 2578 , 33 , 98 ), // #670
  INST(Rol              , X86Rot             , O(000000,D0,0,_,x,_,_,_  ), 0                         , 0  , 0  , 2588 , 172, 113), // #671
  INST(Ror              , X86Rot             , O(000000,D0,1,_,x,_,_,_  ), 0                         , 31 , 0  , 2592 , 172, 113), // #672
  INST(Rorx             , VexRmi_Wx          , V(F20F3A,F0,_,0,x,_,_,_  ), 0                         , 96 , 0  , 2596 , 178, 85 ), // #673
  INST(Roundpd          , ExtRmi             , O(660F3A,09,_,_,_,_,_,_  ), 0                         , 8  , 0  , 9794 , 8  , 12 ), // #674
  INST(Roundps          , ExtRmi             , O(660F3A,08,_,_,_,_,_,_  ), 0                         , 8  , 0  , 9803 , 8  , 12 ), // #675
  INST(Roundsd          , ExtRmi             , O(660F3A,0B,_,_,_,_,_,_  ), 0                         , 8  , 0  , 9812 , 37 , 12 ), // #676
  INST(Roundss          , ExtRmi             , O(660F3A,0A,_,_,_,_,_,_  ), 0                         , 8  , 0  , 9821 , 38 , 12 ), // #677
  INST(Rsm              , X86Op              , O(000F00,AA,_,_,_,_,_,_  ), 0                         , 4  , 0  , 2601 , 81 , 1  ), // #678
  INST(Rsqrtps          , ExtRm              , O(000F00,52,_,_,_,_,_,_  ), 0                         , 4  , 0  , 9918 , 5  , 5  ), // #679
  INST(Rsqrtss          , ExtRm              , O(F30F00,52,_,_,_,_,_,_  ), 0                         , 6  , 0  , 9927 , 7  , 5  ), // #680
  INST(Rstorssp         , X86M_Only          , O(F30F00,01,5,_,_,_,_,_  ), 0                         , 64 , 0  , 2605 , 32 , 24 ), // #681
  INST(Sahf             , X86Op              , O(000000,9E,_,_,_,_,_,_  ), 0                         , 0  , 0  , 2614 , 97 , 114), // #682
  INST(Sal              , X86Rot             , O(000000,D0,4,_,x,_,_,_  ), 0                         , 9  , 0  , 2619 , 172, 1  ), // #683
  INST(Sar              , X86Rot             , O(000000,D0,7,_,x,_,_,_  ), 0                         , 27 , 0  , 2623 , 172, 1  ), // #684
  INST(Sarx             , VexRmv_Wx          , V(F30F38,F7,_,0,x,_,_,_  ), 0                         , 89 , 0  , 2627 , 13 , 85 ), // #685
  INST(Saveprevssp      , X86Op              , O(F30F01,EA,_,_,_,_,_,_  ), 0                         , 25 , 0  , 2632 , 30 , 24 ), // #686
  INST(Sbb              , X86Arith           , O(000000,18,3,_,x,_,_,_  ), 0                         , 76 , 0  , 2644 , 179, 2  ), // #687
  INST(Scas             , X86StrRm           , O(000000,AE,_,_,_,_,_,_  ), 0                         , 0  , 0  , 2648 , 180, 37 ), // #688
  INST(Senduipi         , X86M_NoSize        , O(F30F00,C7,6,_,_,_,_,_  ), 0                         , 24 , 0  , 2653 , 77 , 25 ), // #689
  INST(Serialize        , X86Op              , O(000F01,E8,_,_,_,_,_,_  ), 0                         , 21 , 0  , 2662 , 30 , 115), // #690
  INST(Seta             , X86Set             , O(000F00,97,_,_,_,_,_,_  ), 0                         , 4  , 0  , 2672 , 181, 59 ), // #691
  INST(Setae            , X86Set             , O(000F00,93,_,_,_,_,_,_  ), 0                         , 4  , 0  , 2677 , 181, 60 ), // #692
  INST(Setb             , X86Set             , O(000F00,92,_,_,_,_,_,_  ), 0                         , 4  , 0  , 2683 , 181, 60 ), // #693
  INST(Setbe            , X86Set             , O(000F00,96,_,_,_,_,_,_  ), 0                         , 4  , 0  , 2688 , 181, 59 ), // #694
  INST(Setc             , X86Set             , O(000F00,92,_,_,_,_,_,_  ), 0                         , 4  , 0  , 2694 , 181, 60 ), // #695
  INST(Sete             , X86Set             , O(000F00,94,_,_,_,_,_,_  ), 0                         , 4  , 0  , 2699 , 181, 61 ), // #696
  INST(Setg             , X86Set             , O(000F00,9F,_,_,_,_,_,_  ), 0                         , 4  , 0  , 2704 , 181, 62 ), // #697
  INST(Setge            , X86Set             , O(000F00,9D,_,_,_,_,_,_  ), 0                         , 4  , 0  , 2709 , 181, 63 ), // #698
  INST(Setl             , X86Set             , O(000F00,9C,_,_,_,_,_,_  ), 0                         , 4  , 0  , 2715 , 181, 63 ), // #699
  INST(Setle            , X86Set             , O(000F00,9E,_,_,_,_,_,_  ), 0                         , 4  , 0  , 2720 , 181, 62 ), // #700
  INST(Setna            , X86Set             , O(000F00,96,_,_,_,_,_,_  ), 0                         , 4  , 0  , 2726 , 181, 59 ), // #701
  INST(Setnae           , X86Set             , O(000F00,92,_,_,_,_,_,_  ), 0                         , 4  , 0  , 2732 , 181, 60 ), // #702
  INST(Setnb            , X86Set             , O(000F00,93,_,_,_,_,_,_  ), 0                         , 4  , 0  , 2739 , 181, 60 ), // #703
  INST(Setnbe           , X86Set             , O(000F00,97,_,_,_,_,_,_  ), 0                         , 4  , 0  , 2745 , 181, 59 ), // #704
  INST(Setnc            , X86Set             , O(000F00,93,_,_,_,_,_,_  ), 0                         , 4  , 0  , 2752 , 181, 60 ), // #705
  INST(Setne            , X86Set             , O(000F00,95,_,_,_,_,_,_  ), 0                         , 4  , 0  , 2758 , 181, 61 ), // #706
  INST(Setng            , X86Set             , O(000F00,9E,_,_,_,_,_,_  ), 0                         , 4  , 0  , 2764 , 181, 62 ), // #707
  INST(Setnge           , X86Set             , O(000F00,9C,_,_,_,_,_,_  ), 0                         , 4  , 0  , 2770 , 181, 63 ), // #708
  INST(Setnl            , X86Set             , O(000F00,9D,_,_,_,_,_,_  ), 0                         , 4  , 0  , 2777 , 181, 63 ), // #709
  INST(Setnle           , X86Set             , O(000F00,9F,_,_,_,_,_,_  ), 0                         , 4  , 0  , 2783 , 181, 62 ), // #710
  INST(Setno            , X86Set             , O(000F00,91,_,_,_,_,_,_  ), 0                         , 4  , 0  , 2790 , 181, 57 ), // #711
  INST(Setnp            , X86Set             , O(000F00,9B,_,_,_,_,_,_  ), 0                         , 4  , 0  , 2796 , 181, 64 ), // #712
  INST(Setns            , X86Set             , O(000F00,99,_,_,_,_,_,_  ), 0                         , 4  , 0  , 2802 , 181, 65 ), // #713
  INST(Setnz            , X86Set             , O(000F00,95,_,_,_,_,_,_  ), 0                         , 4  , 0  , 2808 , 181, 61 ), // #714
  INST(Seto             , X86Set             , O(000F00,90,_,_,_,_,_,_  ), 0                         , 4  , 0  , 2814 , 181, 57 ), // #715
  INST(Setp             , X86Set             , O(000F00,9A,_,_,_,_,_,_  ), 0                         , 4  , 0  , 2819 , 181, 64 ), // #716
  INST(Setpe            , X86Set             , O(000F00,9A,_,_,_,_,_,_  ), 0                         , 4  , 0  , 2824 , 181, 64 ), // #717
  INST(Setpo            , X86Set             , O(000F00,9B,_,_,_,_,_,_  ), 0                         , 4  , 0  , 2830 , 181, 64 ), // #718
  INST(Sets             , X86Set             , O(000F00,98,_,_,_,_,_,_  ), 0                         , 4  , 0  , 2836 , 181, 65 ), // #719
  INST(Setssbsy         , X86Op              , O(F30F01,E8,_,_,_,_,_,_  ), 0                         , 25 , 0  , 2841 , 30 , 56 ), // #720
  INST(Setz             , X86Set             , O(000F00,94,_,_,_,_,_,_  ), 0                         , 4  , 0  , 2850 , 181, 61 ), // #721
  INST(Sfence           , X86Fence           , O(000F00,AE,7,_,_,_,_,_  ), 0                         , 22 , 0  , 2855 , 30 , 77 ), // #722
  INST(Sgdt             , X86M_Only          , O(000F00,01,0,_,_,_,_,_  ), 0                         , 4  , 0  , 2862 , 69 , 0  ), // #723
  INST(Sha1msg1         , ExtRm              , O(000F38,C9,_,_,_,_,_,_  ), 0                         , 84 , 0  , 2867 , 5  , 116), // #724
  INST(Sha1msg2         , ExtRm              , O(000F38,CA,_,_,_,_,_,_  ), 0                         , 84 , 0  , 2876 , 5  , 116), // #725
  INST(Sha1nexte        , ExtRm              , O(000F38,C8,_,_,_,_,_,_  ), 0                         , 84 , 0  , 2885 , 5  , 116), // #726
  INST(Sha1rnds4        , ExtRmi             , O(000F3A,CC,_,_,_,_,_,_  ), 0                         , 86 , 0  , 2895 , 8  , 116), // #727
  INST(Sha256msg1       , ExtRm              , O(000F38,CC,_,_,_,_,_,_  ), 0                         , 84 , 0  , 2905 , 5  , 116), // #728
  INST(Sha256msg2       , ExtRm              , O(000F38,CD,_,_,_,_,_,_  ), 0                         , 84 , 0  , 2916 , 5  , 116), // #729
  INST(Sha256rnds2      , ExtRm_XMM0         , O(000F38,CB,_,_,_,_,_,_  ), 0                         , 84 , 0  , 2927 , 15 , 116), // #730
  INST(Shl              , X86Rot             , O(000000,D0,4,_,x,_,_,_  ), 0                         , 9  , 0  , 2939 , 172, 1  ), // #731
  INST(Shld             , X86ShldShrd        , O(000F00,A4,_,_,x,_,_,_  ), 0                         , 4  , 0  , 8963 , 182, 1  ), // #732
  INST(Shlx             , VexRmv_Wx          , V(660F38,F7,_,0,x,_,_,_  ), 0                         , 97 , 0  , 2943 , 13 , 85 ), // #733
  INST(Shr              , X86Rot             , O(000000,D0,5,_,x,_,_,_  ), 0                         , 63 , 0  , 2948 , 172, 1  ), // #734
  INST(Shrd             , X86ShldShrd        , O(000F00,AC,_,_,x,_,_,_  ), 0                         , 4  , 0  , 2952 , 182, 1  ), // #735
  INST(Shrx             , VexRmv_Wx          , V(F20F38,F7,_,0,x,_,_,_  ), 0                         , 85 , 0  , 2957 , 13 , 85 ), // #736
  INST(Shufpd           , ExtRmi             , O(660F00,C6,_,_,_,_,_,_  ), 0                         , 3  , 0  , 10188, 8  , 4  ), // #737
  INST(Shufps           , ExtRmi             , O(000F00,C6,_,_,_,_,_,_  ), 0                         , 4  , 0  , 10196, 8  , 5  ), // #738
  INST(Sidt             , X86M_Only          , O(000F00,01,1,_,_,_,_,_  ), 0                         , 29 , 0  , 2962 , 69 , 0  ), // #739
  INST(Skinit           , X86Op_xAX          , O(000F01,DE,_,_,_,_,_,_  ), 0                         , 21 , 0  , 2967 , 52 , 117), // #740
  INST(Sldt             , X86M_NoMemSize     , O(000F00,00,0,_,_,_,_,_  ), 0                         , 4  , 0  , 2974 , 183, 0  ), // #741
  INST(Slwpcb           , VexR_Wx            , V(XOP_M9,12,1,0,x,_,_,_  ), 0                         , 11 , 0  , 2979 , 108, 74 ), // #742
  INST(Smsw             , X86M_NoMemSize     , O(000F00,01,4,_,_,_,_,_  ), 0                         , 98 , 0  , 2986 , 183, 0  ), // #743
  INST(Sqrtpd           , ExtRm              , O(660F00,51,_,_,_,_,_,_  ), 0                         , 3  , 0  , 10204, 5  , 4  ), // #744
  INST(Sqrtps           , ExtRm              , O(000F00,51,_,_,_,_,_,_  ), 0                         , 4  , 0  , 9919 , 5  , 5  ), // #745
  INST(Sqrtsd           , ExtRm              , O(F20F00,51,_,_,_,_,_,_  ), 0                         , 5  , 0  , 10220, 6  , 4  ), // #746
  INST(Sqrtss           , ExtRm              , O(F30F00,51,_,_,_,_,_,_  ), 0                         , 6  , 0  , 9928 , 7  , 5  ), // #747
  INST(Stac             , X86Op              , O(000F01,CB,_,_,_,_,_,_  ), 0                         , 21 , 0  , 2991 , 30 , 16 ), // #748
  INST(Stc              , X86Op              , O(000000,F9,_,_,_,_,_,_  ), 0                         , 0  , 0  , 2996 , 30 , 17 ), // #749
  INST(Std              , X86Op              , O(000000,FD,_,_,_,_,_,_  ), 0                         , 0  , 0  , 6946 , 30 , 18 ), // #750
  INST(Stgi             , X86Op              , O(000F01,DC,_,_,_,_,_,_  ), 0                         , 21 , 0  , 3000 , 30 , 117), // #751
  INST(Sti              , X86Op              , O(000000,FB,_,_,_,_,_,_  ), 0                         , 0  , 0  , 3005 , 30 , 23 ), // #752
  INST(Stmxcsr          , X86M_Only          , O(000F00,AE,3,_,_,_,_,_  ), 0                         , 79 , 0  , 10236, 101, 5  ), // #753
  INST(Stos             , X86StrMr           , O(000000,AA,_,_,_,_,_,_  ), 0                         , 0  , 0  , 3009 , 184, 75 ), // #754
  INST(Str              , X86M_NoMemSize     , O(000F00,00,1,_,_,_,_,_  ), 0                         , 29 , 0  , 3014 , 183, 0  ), // #755
  INST(Sttilecfg        , AmxCfg             , V(660F38,49,_,0,0,_,_,_  ), 0                         , 97 , 0  , 3018 , 103, 73 ), // #756
  INST(Stui             , X86Op              , O(F30F01,EF,_,_,_,_,_,_  ), 0                         , 25 , 0  , 3135 , 33 , 25 ), // #757
  INST(Sub              , X86Arith           , O(000000,28,5,_,x,_,_,_  ), 0                         , 63 , 0  , 866  , 179, 1  ), // #758
  INST(Subpd            , ExtRm              , O(660F00,5C,_,_,_,_,_,_  ), 0                         , 3  , 0  , 4848 , 5  , 4  ), // #759
  INST(Subps            , ExtRm              , O(000F00,5C,_,_,_,_,_,_  ), 0                         , 4  , 0  , 4860 , 5  , 5  ), // #760
  INST(Subsd            , ExtRm              , O(F20F00,5C,_,_,_,_,_,_  ), 0                         , 5  , 0  , 5536 , 6  , 4  ), // #761
  INST(Subss            , ExtRm              , O(F30F00,5C,_,_,_,_,_,_  ), 0                         , 6  , 0  , 5546 , 7  , 5  ), // #762
  INST(Swapgs           , X86Op              , O(000F01,F8,_,_,_,_,_,_  ), 0                         , 21 , 0  , 3028 , 33 , 0  ), // #763
  INST(Syscall          , X86Op              , O(000F00,05,_,_,_,_,_,_  ), 0                         , 4  , 0  , 3035 , 33 , 0  ), // #764
  INST(Sysenter         , X86Op              , O(000F00,34,_,_,_,_,_,_  ), 0                         , 4  , 0  , 3043 , 30 , 0  ), // #765
  INST(Sysexit          , X86Op              , O(000F00,35,_,_,_,_,_,_  ), 0                         , 4  , 0  , 3052 , 30 , 0  ), // #766
  INST(Sysexitq         , X86Op              , O(000F00,35,_,_,1,_,_,_  ), 0                         , 61 , 0  , 3060 , 30 , 0  ), // #767
  INST(Sysret           , X86Op              , O(000F00,07,_,_,_,_,_,_  ), 0                         , 4  , 0  , 3069 , 33 , 0  ), // #768
  INST(Sysretq          , X86Op              , O(000F00,07,_,_,1,_,_,_  ), 0                         , 61 , 0  , 3076 , 33 , 0  ), // #769
  INST(T1mskc           , VexVm_Wx           , V(XOP_M9,01,7,0,x,_,_,_  ), 0                         , 99 , 0  , 3084 , 14 , 11 ), // #770
  INST(Tdpbf16ps        , AmxRmv             , V(F30F38,5C,_,0,0,_,_,_  ), 0                         , 89 , 0  , 3091 , 185, 118), // #771
  INST(Tdpbssd          , AmxRmv             , V(F20F38,5E,_,0,0,_,_,_  ), 0                         , 85 , 0  , 3101 , 185, 119), // #772
  INST(Tdpbsud          , AmxRmv             , V(F30F38,5E,_,0,0,_,_,_  ), 0                         , 89 , 0  , 3109 , 185, 119), // #773
  INST(Tdpbusd          , AmxRmv             , V(660F38,5E,_,0,0,_,_,_  ), 0                         , 97 , 0  , 3117 , 185, 119), // #774
  INST(Tdpbuud          , AmxRmv             , V(000F38,5E,_,0,0,_,_,_  ), 0                         , 10 , 0  , 3125 , 185, 119), // #775
  INST(Test             , X86Test            , O(000000,84,_,_,x,_,_,_  ), O(000000,F6,_,_,x,_,_,_  ), 0  , 80 , 9392 , 186, 1  ), // #776
  INST(Testui           , X86Op              , O(F30F01,ED,_,_,_,_,_,_  ), 0                         , 25 , 0  , 3133 , 33 , 120), // #777
  INST(Tileloadd        , AmxRm              , V(F20F38,4B,_,0,0,_,_,_  ), 0                         , 85 , 0  , 3140 , 187, 73 ), // #778
  INST(Tileloaddt1      , AmxRm              , V(660F38,4B,_,0,0,_,_,_  ), 0                         , 97 , 0  , 3150 , 187, 73 ), // #779
  INST(Tilerelease      , VexOpMod           , V(000F38,49,0,0,0,_,_,_  ), 0                         , 10 , 0  , 3162 , 188, 73 ), // #780
  INST(Tilestored       , AmxMr              , V(F30F38,4B,_,0,0,_,_,_  ), 0                         , 89 , 0  , 3174 , 189, 73 ), // #781
  INST(Tilezero         , AmxR               , V(F20F38,49,_,0,0,_,_,_  ), 0                         , 85 , 0  , 3185 , 190, 73 ), // #782
  INST(Tpause           , X86R32_EDX_EAX     , O(660F00,AE,6,_,_,_,_,_  ), 0                         , 26 , 0  , 3194 , 191, 121), // #783
  INST(Tzcnt            , X86Rm_Raw66H       , O(F30F00,BC,_,_,x,_,_,_  ), 0                         , 6  , 0  , 3201 , 22 , 9  ), // #784
  INST(Tzmsk            , VexVm_Wx           , V(XOP_M9,01,4,0,x,_,_,_  ), 0                         , 100, 0  , 3207 , 14 , 11 ), // #785
  INST(Ucomisd          , ExtRm              , O(660F00,2E,_,_,_,_,_,_  ), 0                         , 3  , 0  , 10289, 6  , 41 ), // #786
  INST(Ucomiss          , ExtRm              , O(000F00,2E,_,_,_,_,_,_  ), 0                         , 4  , 0  , 10298, 7  , 42 ), // #787
  INST(Ud0              , X86Rm              , O(000F00,FF,_,_,_,_,_,_  ), 0                         , 4  , 0  , 3213 , 192, 0  ), // #788
  INST(Ud1              , X86Rm              , O(000F00,B9,_,_,_,_,_,_  ), 0                         , 4  , 0  , 3217 , 192, 0  ), // #789
  INST(Ud2              , X86Op              , O(000F00,0B,_,_,_,_,_,_  ), 0                         , 4  , 0  , 3221 , 30 , 0  ), // #790
  INST(Uiret            , X86Op              , O(F30F01,EC,_,_,_,_,_,_  ), 0                         , 25 , 0  , 3225 , 33 , 25 ), // #791
  INST(Umonitor         , X86R_FromM         , O(F30F00,AE,6,_,_,_,_,_  ), 0                         , 24 , 0  , 3231 , 193, 122), // #792
  INST(Umwait           , X86R32_EDX_EAX     , O(F20F00,AE,6,_,_,_,_,_  ), 0                         , 101, 0  , 3240 , 191, 121), // #793
  INST(Unpckhpd         , ExtRm              , O(660F00,15,_,_,_,_,_,_  ), 0                         , 3  , 0  , 10307, 5  , 4  ), // #794
  INST(Unpckhps         , ExtRm              , O(000F00,15,_,_,_,_,_,_  ), 0                         , 4  , 0  , 10317, 5  , 5  ), // #795
  INST(Unpcklpd         , ExtRm              , O(660F00,14,_,_,_,_,_,_  ), 0                         , 3  , 0  , 10327, 5  , 4  ), // #796
  INST(Unpcklps         , ExtRm              , O(000F00,14,_,_,_,_,_,_  ), 0                         , 4  , 0  , 10337, 5  , 5  ), // #797
  INST(V4fmaddps        , VexRm_T1_4X        , E(F20F38,9A,_,2,_,0,4,T4X), 0                         , 102, 0  , 3247 , 194, 123), // #798
  INST(V4fmaddss        , VexRm_T1_4X        , E(F20F38,9B,_,0,_,0,4,T4X), 0                         , 103, 0  , 3257 , 195, 123), // #799
  INST(V4fnmaddps       , VexRm_T1_4X        , E(F20F38,AA,_,2,_,0,4,T4X), 0                         , 102, 0  , 3267 , 194, 123), // #800
  INST(V4fnmaddss       , VexRm_T1_4X        , E(F20F38,AB,_,0,_,0,4,T4X), 0                         , 103, 0  , 3278 , 195, 123), // #801
  INST(Vaddpd           , VexRvm_Lx          , V(660F00,58,_,x,I,1,4,FV ), 0                         , 104, 0  , 3289 , 196, 124), // #802
  INST(Vaddps           , VexRvm_Lx          , V(000F00,58,_,x,I,0,4,FV ), 0                         , 105, 0  , 3296 , 197, 124), // #803
  INST(Vaddsd           , VexRvm             , V(F20F00,58,_,I,I,1,3,T1S), 0                         , 106, 0  , 3303 , 198, 125), // #804
  INST(Vaddss           , VexRvm             , V(F30F00,58,_,I,I,0,2,T1S), 0                         , 107, 0  , 3310 , 199, 125), // #805
  INST(Vaddsubpd        , VexRvm_Lx          , V(660F00,D0,_,x,I,_,_,_  ), 0                         , 70 , 0  , 3317 , 200, 126), // #806
  INST(Vaddsubps        , VexRvm_Lx          , V(F20F00,D0,_,x,I,_,_,_  ), 0                         , 108, 0  , 3327 , 200, 126), // #807
  INST(Vaesdec          , VexRvm_Lx          , V(660F38,DE,_,x,I,_,4,FVM), 0                         , 109, 0  , 3337 , 201, 127), // #808
  INST(Vaesdeclast      , VexRvm_Lx          , V(660F38,DF,_,x,I,_,4,FVM), 0                         , 109, 0  , 3345 , 201, 127), // #809
  INST(Vaesenc          , VexRvm_Lx          , V(660F38,DC,_,x,I,_,4,FVM), 0                         , 109, 0  , 3357 , 201, 127), // #810
  INST(Vaesenclast      , VexRvm_Lx          , V(660F38,DD,_,x,I,_,4,FVM), 0                         , 109, 0  , 3365 , 201, 127), // #811
  INST(Vaesimc          , VexRm              , V(660F38,DB,_,0,I,_,_,_  ), 0                         , 97 , 0  , 3377 , 202, 128), // #812
  INST(Vaeskeygenassist , VexRmi             , V(660F3A,DF,_,0,I,_,_,_  ), 0                         , 74 , 0  , 3385 , 203, 128), // #813
  INST(Valignd          , VexRvmi_Lx         , E(660F3A,03,_,x,_,0,4,FV ), 0                         , 110, 0  , 3402 , 204, 129), // #814
  INST(Valignq          , VexRvmi_Lx         , E(660F3A,03,_,x,_,1,4,FV ), 0                         , 111, 0  , 3410 , 205, 129), // #815
  INST(Vandnpd          , VexRvm_Lx          , V(660F00,55,_,x,I,1,4,FV ), 0                         , 104, 0  , 3418 , 206, 130), // #816
  INST(Vandnps          , VexRvm_Lx          , V(000F00,55,_,x,I,0,4,FV ), 0                         , 105, 0  , 3426 , 207, 130), // #817
  INST(Vandpd           , VexRvm_Lx          , V(660F00,54,_,x,I,1,4,FV ), 0                         , 104, 0  , 3434 , 208, 130), // #818
  INST(Vandps           , VexRvm_Lx          , V(000F00,54,_,x,I,0,4,FV ), 0                         , 105, 0  , 3441 , 209, 130), // #819
  INST(Vblendmpd        , VexRvm_Lx          , E(660F38,65,_,x,_,1,4,FV ), 0                         , 112, 0  , 3448 , 210, 129), // #820
  INST(Vblendmps        , VexRvm_Lx          , E(660F38,65,_,x,_,0,4,FV ), 0                         , 113, 0  , 3458 , 211, 129), // #821
  INST(Vblendpd         , VexRvmi_Lx         , V(660F3A,0D,_,x,I,_,_,_  ), 0                         , 74 , 0  , 3468 , 212, 126), // #822
  INST(Vblendps         , VexRvmi_Lx         , V(660F3A,0C,_,x,I,_,_,_  ), 0                         , 74 , 0  , 3477 , 212, 126), // #823
  INST(Vblendvpd        , VexRvmr_Lx         , V(660F3A,4B,_,x,0,_,_,_  ), 0                         , 74 , 0  , 3486 , 213, 126), // #824
  INST(Vblendvps        , VexRvmr_Lx         , V(660F3A,4A,_,x,0,_,_,_  ), 0                         , 74 , 0  , 3496 , 213, 126), // #825
  INST(Vbroadcastf128   , VexRm              , V(660F38,1A,_,1,0,_,_,_  ), 0                         , 114, 0  , 3506 , 214, 126), // #826
  INST(Vbroadcastf32x2  , VexRm_Lx           , E(660F38,19,_,x,_,0,3,T2 ), 0                         , 115, 0  , 3521 , 215, 131), // #827
  INST(Vbroadcastf32x4  , VexRm_Lx           , E(660F38,1A,_,x,_,0,4,T4 ), 0                         , 116, 0  , 3537 , 216, 68 ), // #828
  INST(Vbroadcastf32x8  , VexRm              , E(660F38,1B,_,2,_,0,5,T8 ), 0                         , 117, 0  , 3553 , 217, 66 ), // #829
  INST(Vbroadcastf64x2  , VexRm_Lx           , E(660F38,1A,_,x,_,1,4,T2 ), 0                         , 118, 0  , 3569 , 216, 131), // #830
  INST(Vbroadcastf64x4  , VexRm              , E(660F38,1B,_,2,_,1,5,T4 ), 0                         , 119, 0  , 3585 , 217, 68 ), // #831
  INST(Vbroadcasti128   , VexRm              , V(660F38,5A,_,1,0,_,_,_  ), 0                         , 114, 0  , 3601 , 214, 132), // #832
  INST(Vbroadcasti32x2  , VexRm_Lx           , E(660F38,59,_,x,_,0,3,T2 ), 0                         , 115, 0  , 3616 , 218, 131), // #833
  INST(Vbroadcasti32x4  , VexRm_Lx           , E(660F38,5A,_,x,_,0,4,T4 ), 0                         , 116, 0  , 3632 , 216, 129), // #834
  INST(Vbroadcasti32x8  , VexRm              , E(660F38,5B,_,2,_,0,5,T8 ), 0                         , 117, 0  , 3648 , 217, 66 ), // #835
  INST(Vbroadcasti64x2  , VexRm_Lx           , E(660F38,5A,_,x,_,1,4,T2 ), 0                         , 118, 0  , 3664 , 216, 131), // #836
  INST(Vbroadcasti64x4  , VexRm              , E(660F38,5B,_,2,_,1,5,T4 ), 0                         , 119, 0  , 3680 , 217, 68 ), // #837
  INST(Vbroadcastsd     , VexRm_Lx           , V(660F38,19,_,x,0,1,3,T1S), 0                         , 120, 0  , 3696 , 219, 133), // #838
  INST(Vbroadcastss     , VexRm_Lx           , V(660F38,18,_,x,0,0,2,T1S), 0                         , 121, 0  , 3709 , 220, 133), // #839
  INST(Vcmppd           , VexRvmi_Lx_KEvex   , V(660F00,C2,_,x,I,1,4,FV ), 0                         , 104, 0  , 3722 , 221, 124), // #840
  INST(Vcmpps           , VexRvmi_Lx_KEvex   , V(000F00,C2,_,x,I,0,4,FV ), 0                         , 105, 0  , 3729 , 222, 124), // #841
  INST(Vcmpsd           , VexRvmi_KEvex      , V(F20F00,C2,_,I,I,1,3,T1S), 0                         , 106, 0  , 3736 , 223, 125), // #842
  INST(Vcmpss           , VexRvmi_KEvex      , V(F30F00,C2,_,I,I,0,2,T1S), 0                         , 107, 0  , 3743 , 224, 125), // #843
  INST(Vcomisd          , VexRm              , V(660F00,2F,_,I,I,1,3,T1S), 0                         , 122, 0  , 3750 , 225, 134), // #844
  INST(Vcomiss          , VexRm              , V(000F00,2F,_,I,I,0,2,T1S), 0                         , 123, 0  , 3758 , 226, 134), // #845
  INST(Vcompresspd      , VexMr_Lx           , E(660F38,8A,_,x,_,1,3,T1S), 0                         , 124, 0  , 3766 , 227, 129), // #846
  INST(Vcompressps      , VexMr_Lx           , E(660F38,8A,_,x,_,0,2,T1S), 0                         , 125, 0  , 3778 , 227, 129), // #847
  INST(Vcvtdq2pd        , VexRm_Lx           , V(F30F00,E6,_,x,I,0,3,HV ), 0                         , 126, 0  , 3790 , 228, 124), // #848
  INST(Vcvtdq2ps        , VexRm_Lx           , V(000F00,5B,_,x,I,0,4,FV ), 0                         , 105, 0  , 3800 , 229, 124), // #849
  INST(Vcvtne2ps2bf16   , VexRvm_Lx          , E(F20F38,72,_,_,_,0,4,FV ), 0                         , 127, 0  , 3810 , 211, 135), // #850
  INST(Vcvtneps2bf16    , VexRm_Lx_Narrow    , E(F30F38,72,_,_,_,0,4,FV ), 0                         , 128, 0  , 3825 , 230, 135), // #851
  INST(Vcvtpd2dq        , VexRm_Lx_Narrow    , V(F20F00,E6,_,x,I,1,4,FV ), 0                         , 129, 0  , 3839 , 231, 124), // #852
  INST(Vcvtpd2ps        , VexRm_Lx_Narrow    , V(660F00,5A,_,x,I,1,4,FV ), 0                         , 104, 0  , 3849 , 231, 124), // #853
  INST(Vcvtpd2qq        , VexRm_Lx           , E(660F00,7B,_,x,_,1,4,FV ), 0                         , 130, 0  , 3859 , 232, 131), // #854
  INST(Vcvtpd2udq       , VexRm_Lx_Narrow    , E(000F00,79,_,x,_,1,4,FV ), 0                         , 131, 0  , 3869 , 233, 129), // #855
  INST(Vcvtpd2uqq       , VexRm_Lx           , E(660F00,79,_,x,_,1,4,FV ), 0                         , 130, 0  , 3880 , 232, 131), // #856
  INST(Vcvtph2ps        , VexRm_Lx           , V(660F38,13,_,x,0,0,3,HVM), 0                         , 132, 0  , 3891 , 234, 136), // #857
  INST(Vcvtps2dq        , VexRm_Lx           , V(660F00,5B,_,x,I,0,4,FV ), 0                         , 133, 0  , 3901 , 229, 124), // #858
  INST(Vcvtps2pd        , VexRm_Lx           , V(000F00,5A,_,x,I,0,3,HV ), 0                         , 134, 0  , 3911 , 235, 124), // #859
  INST(Vcvtps2ph        , VexMri_Lx          , V(660F3A,1D,_,x,0,0,3,HVM), 0                         , 135, 0  , 3921 , 236, 136), // #860
  INST(Vcvtps2qq        , VexRm_Lx           , E(660F00,7B,_,x,_,0,3,HV ), 0                         , 136, 0  , 3931 , 237, 131), // #861
  INST(Vcvtps2udq       , VexRm_Lx           , E(000F00,79,_,x,_,0,4,FV ), 0                         , 137, 0  , 3941 , 238, 129), // #862
  INST(Vcvtps2uqq       , VexRm_Lx           , E(660F00,79,_,x,_,0,3,HV ), 0                         , 136, 0  , 3952 , 237, 131), // #863
  INST(Vcvtqq2pd        , VexRm_Lx           , E(F30F00,E6,_,x,_,1,4,FV ), 0                         , 138, 0  , 3963 , 232, 131), // #864
  INST(Vcvtqq2ps        , VexRm_Lx_Narrow    , E(000F00,5B,_,x,_,1,4,FV ), 0                         , 131, 0  , 3973 , 233, 131), // #865
  INST(Vcvtsd2si        , VexRm_Wx           , V(F20F00,2D,_,I,x,x,3,T1F), 0                         , 139, 0  , 3983 , 239, 125), // #866
  INST(Vcvtsd2ss        , VexRvm             , V(F20F00,5A,_,I,I,1,3,T1S), 0                         , 106, 0  , 3993 , 198, 125), // #867
  INST(Vcvtsd2usi       , VexRm_Wx           , E(F20F00,79,_,I,_,x,3,T1F), 0                         , 140, 0  , 4003 , 240, 68 ), // #868
  INST(Vcvtsi2sd        , VexRvm_Wx          , V(F20F00,2A,_,I,x,x,2,T1W), 0                         , 141, 0  , 4014 , 241, 125), // #869
  INST(Vcvtsi2ss        , VexRvm_Wx          , V(F30F00,2A,_,I,x,x,2,T1W), 0                         , 142, 0  , 4024 , 241, 125), // #870
  INST(Vcvtss2sd        , VexRvm             , V(F30F00,5A,_,I,I,0,2,T1S), 0                         , 107, 0  , 4034 , 242, 125), // #871
  INST(Vcvtss2si        , VexRm_Wx           , V(F30F00,2D,_,I,x,x,2,T1F), 0                         , 143, 0  , 4044 , 243, 125), // #872
  INST(Vcvtss2usi       , VexRm_Wx           , E(F30F00,79,_,I,_,x,2,T1F), 0                         , 144, 0  , 4054 , 244, 68 ), // #873
  INST(Vcvttpd2dq       , VexRm_Lx_Narrow    , V(660F00,E6,_,x,I,1,4,FV ), 0                         , 104, 0  , 4065 , 245, 124), // #874
  INST(Vcvttpd2qq       , VexRm_Lx           , E(660F00,7A,_,x,_,1,4,FV ), 0                         , 130, 0  , 4076 , 246, 129), // #875
  INST(Vcvttpd2udq      , VexRm_Lx_Narrow    , E(000F00,78,_,x,_,1,4,FV ), 0                         , 131, 0  , 4087 , 247, 129), // #876
  INST(Vcvttpd2uqq      , VexRm_Lx           , E(660F00,78,_,x,_,1,4,FV ), 0                         , 130, 0  , 4099 , 246, 131), // #877
  INST(Vcvttps2dq       , VexRm_Lx           , V(F30F00,5B,_,x,I,0,4,FV ), 0                         , 145, 0  , 4111 , 248, 124), // #878
  INST(Vcvttps2qq       , VexRm_Lx           , E(660F00,7A,_,x,_,0,3,HV ), 0                         , 136, 0  , 4122 , 249, 131), // #879
  INST(Vcvttps2udq      , VexRm_Lx           , E(000F00,78,_,x,_,0,4,FV ), 0                         , 137, 0  , 4133 , 250, 129), // #880
  INST(Vcvttps2uqq      , VexRm_Lx           , E(660F00,78,_,x,_,0,3,HV ), 0                         , 136, 0  , 4145 , 249, 131), // #881
  INST(Vcvttsd2si       , VexRm_Wx           , V(F20F00,2C,_,I,x,x,3,T1F), 0                         , 139, 0  , 4157 , 251, 125), // #882
  INST(Vcvttsd2usi      , VexRm_Wx           , E(F20F00,78,_,I,_,x,3,T1F), 0                         , 140, 0  , 4168 , 252, 68 ), // #883
  INST(Vcvttss2si       , VexRm_Wx           , V(F30F00,2C,_,I,x,x,2,T1F), 0                         , 143, 0  , 4180 , 253, 125), // #884
  INST(Vcvttss2usi      , VexRm_Wx           , E(F30F00,78,_,I,_,x,2,T1F), 0                         , 144, 0  , 4191 , 254, 68 ), // #885
  INST(Vcvtudq2pd       , VexRm_Lx           , E(F30F00,7A,_,x,_,0,3,HV ), 0                         , 146, 0  , 4203 , 255, 129), // #886
  INST(Vcvtudq2ps       , VexRm_Lx           , E(F20F00,7A,_,x,_,0,4,FV ), 0                         , 147, 0  , 4214 , 238, 129), // #887
  INST(Vcvtuqq2pd       , VexRm_Lx           , E(F30F00,7A,_,x,_,1,4,FV ), 0                         , 138, 0  , 4225 , 232, 131), // #888
  INST(Vcvtuqq2ps       , VexRm_Lx_Narrow    , E(F20F00,7A,_,x,_,1,4,FV ), 0                         , 148, 0  , 4236 , 233, 131), // #889
  INST(Vcvtusi2sd       , VexRvm_Wx          , E(F20F00,7B,_,I,_,x,2,T1W), 0                         , 149, 0  , 4247 , 256, 68 ), // #890
  INST(Vcvtusi2ss       , VexRvm_Wx          , E(F30F00,7B,_,I,_,x,2,T1W), 0                         , 150, 0  , 4258 , 256, 68 ), // #891
  INST(Vdbpsadbw        , VexRvmi_Lx         , E(660F3A,42,_,x,_,0,4,FVM), 0                         , 151, 0  , 4269 , 257, 137), // #892
  INST(Vdivpd           , VexRvm_Lx          , V(660F00,5E,_,x,I,1,4,FV ), 0                         , 104, 0  , 4279 , 196, 124), // #893
  INST(Vdivps           , VexRvm_Lx          , V(000F00,5E,_,x,I,0,4,FV ), 0                         , 105, 0  , 4286 , 197, 124), // #894
  INST(Vdivsd           , VexRvm             , V(F20F00,5E,_,I,I,1,3,T1S), 0                         , 106, 0  , 4293 , 198, 125), // #895
  INST(Vdivss           , VexRvm             , V(F30F00,5E,_,I,I,0,2,T1S), 0                         , 107, 0  , 4300 , 199, 125), // #896
  INST(Vdpbf16ps        , VexRvm_Lx          , E(F30F38,52,_,_,_,0,4,FV ), 0                         , 128, 0  , 4307 , 211, 135), // #897
  INST(Vdppd            , VexRvmi_Lx         , V(660F3A,41,_,x,I,_,_,_  ), 0                         , 74 , 0  , 4317 , 258, 126), // #898
  INST(Vdpps            , VexRvmi_Lx         , V(660F3A,40,_,x,I,_,_,_  ), 0                         , 74 , 0  , 4323 , 212, 126), // #899
  INST(Verr             , X86M_NoSize        , O(000F00,00,4,_,_,_,_,_  ), 0                         , 98 , 0  , 4329 , 107, 10 ), // #900
  INST(Verw             , X86M_NoSize        , O(000F00,00,5,_,_,_,_,_  ), 0                         , 78 , 0  , 4334 , 107, 10 ), // #901
  INST(Vexp2pd          , VexRm              , E(660F38,C8,_,2,_,1,4,FV ), 0                         , 152, 0  , 4339 , 259, 138), // #902
  INST(Vexp2ps          , VexRm              , E(660F38,C8,_,2,_,0,4,FV ), 0                         , 153, 0  , 4347 , 260, 138), // #903
  INST(Vexpandpd        , VexRm_Lx           , E(660F38,88,_,x,_,1,3,T1S), 0                         , 124, 0  , 4355 , 261, 129), // #904
  INST(Vexpandps        , VexRm_Lx           , E(660F38,88,_,x,_,0,2,T1S), 0                         , 125, 0  , 4365 , 261, 129), // #905
  INST(Vextractf128     , VexMri             , V(660F3A,19,_,1,0,_,_,_  ), 0                         , 154, 0  , 4375 , 262, 126), // #906
  INST(Vextractf32x4    , VexMri_Lx          , E(660F3A,19,_,x,_,0,4,T4 ), 0                         , 155, 0  , 4388 , 263, 129), // #907
  INST(Vextractf32x8    , VexMri             , E(660F3A,1B,_,2,_,0,5,T8 ), 0                         , 156, 0  , 4402 , 264, 66 ), // #908
  INST(Vextractf64x2    , VexMri_Lx          , E(660F3A,19,_,x,_,1,4,T2 ), 0                         , 157, 0  , 4416 , 263, 131), // #909
  INST(Vextractf64x4    , VexMri             , E(660F3A,1B,_,2,_,1,5,T4 ), 0                         , 158, 0  , 4430 , 264, 68 ), // #910
  INST(Vextracti128     , VexMri             , V(660F3A,39,_,1,0,_,_,_  ), 0                         , 154, 0  , 4444 , 262, 132), // #911
  INST(Vextracti32x4    , VexMri_Lx          , E(660F3A,39,_,x,_,0,4,T4 ), 0                         , 155, 0  , 4457 , 263, 129), // #912
  INST(Vextracti32x8    , VexMri             , E(660F3A,3B,_,2,_,0,5,T8 ), 0                         , 156, 0  , 4471 , 264, 66 ), // #913
  INST(Vextracti64x2    , VexMri_Lx          , E(660F3A,39,_,x,_,1,4,T2 ), 0                         , 157, 0  , 4485 , 263, 131), // #914
  INST(Vextracti64x4    , VexMri             , E(660F3A,3B,_,2,_,1,5,T4 ), 0                         , 158, 0  , 4499 , 264, 68 ), // #915
  INST(Vextractps       , VexMri             , V(660F3A,17,_,0,I,I,2,T1S), 0                         , 159, 0  , 4513 , 265, 125), // #916
  INST(Vfixupimmpd      , VexRvmi_Lx         , E(660F3A,54,_,x,_,1,4,FV ), 0                         , 111, 0  , 4524 , 266, 129), // #917
  INST(Vfixupimmps      , VexRvmi_Lx         , E(660F3A,54,_,x,_,0,4,FV ), 0                         , 110, 0  , 4536 , 267, 129), // #918
  INST(Vfixupimmsd      , VexRvmi            , E(660F3A,55,_,I,_,1,3,T1S), 0                         , 160, 0  , 4548 , 268, 68 ), // #919
  INST(Vfixupimmss      , VexRvmi            , E(660F3A,55,_,I,_,0,2,T1S), 0                         , 161, 0  , 4560 , 269, 68 ), // #920
  INST(Vfmadd132pd      , VexRvm_Lx          , V(660F38,98,_,x,1,1,4,FV ), 0                         , 162, 0  , 4572 , 196, 139), // #921
  INST(Vfmadd132ps      , VexRvm_Lx          , V(660F38,98,_,x,0,0,4,FV ), 0                         , 163, 0  , 4584 , 197, 139), // #922
  INST(Vfmadd132sd      , VexRvm             , V(660F38,99,_,I,1,1,3,T1S), 0                         , 164, 0  , 4596 , 198, 140), // #923
  INST(Vfmadd132ss      , VexRvm             , V(660F38,99,_,I,0,0,2,T1S), 0                         , 121, 0  , 4608 , 199, 140), // #924
  INST(Vfmadd213pd      , VexRvm_Lx          , V(660F38,A8,_,x,1,1,4,FV ), 0                         , 162, 0  , 4620 , 196, 139), // #925
  INST(Vfmadd213ps      , VexRvm_Lx          , V(660F38,A8,_,x,0,0,4,FV ), 0                         , 163, 0  , 4632 , 197, 139), // #926
  INST(Vfmadd213sd      , VexRvm             , V(660F38,A9,_,I,1,1,3,T1S), 0                         , 164, 0  , 4644 , 198, 140), // #927
  INST(Vfmadd213ss      , VexRvm             , V(660F38,A9,_,I,0,0,2,T1S), 0                         , 121, 0  , 4656 , 199, 140), // #928
  INST(Vfmadd231pd      , VexRvm_Lx          , V(660F38,B8,_,x,1,1,4,FV ), 0                         , 162, 0  , 4668 , 196, 139), // #929
  INST(Vfmadd231ps      , VexRvm_Lx          , V(660F38,B8,_,x,0,0,4,FV ), 0                         , 163, 0  , 4680 , 197, 139), // #930
  INST(Vfmadd231sd      , VexRvm             , V(660F38,B9,_,I,1,1,3,T1S), 0                         , 164, 0  , 4692 , 198, 140), // #931
  INST(Vfmadd231ss      , VexRvm             , V(660F38,B9,_,I,0,0,2,T1S), 0                         , 121, 0  , 4704 , 199, 140), // #932
  INST(Vfmaddpd         , Fma4_Lx            , V(660F3A,69,_,x,x,_,_,_  ), 0                         , 74 , 0  , 4716 , 270, 141), // #933
  INST(Vfmaddps         , Fma4_Lx            , V(660F3A,68,_,x,x,_,_,_  ), 0                         , 74 , 0  , 4725 , 270, 141), // #934
  INST(Vfmaddsd         , Fma4               , V(660F3A,6B,_,0,x,_,_,_  ), 0                         , 74 , 0  , 4734 , 271, 141), // #935
  INST(Vfmaddss         , Fma4               , V(660F3A,6A,_,0,x,_,_,_  ), 0                         , 74 , 0  , 4743 , 272, 141), // #936
  INST(Vfmaddsub132pd   , VexRvm_Lx          , V(660F38,96,_,x,1,1,4,FV ), 0                         , 162, 0  , 4752 , 196, 139), // #937
  INST(Vfmaddsub132ps   , VexRvm_Lx          , V(660F38,96,_,x,0,0,4,FV ), 0                         , 163, 0  , 4767 , 197, 139), // #938
  INST(Vfmaddsub213pd   , VexRvm_Lx          , V(660F38,A6,_,x,1,1,4,FV ), 0                         , 162, 0  , 4782 , 196, 139), // #939
  INST(Vfmaddsub213ps   , VexRvm_Lx          , V(660F38,A6,_,x,0,0,4,FV ), 0                         , 163, 0  , 4797 , 197, 139), // #940
  INST(Vfmaddsub231pd   , VexRvm_Lx          , V(660F38,B6,_,x,1,1,4,FV ), 0                         , 162, 0  , 4812 , 196, 139), // #941
  INST(Vfmaddsub231ps   , VexRvm_Lx          , V(660F38,B6,_,x,0,0,4,FV ), 0                         , 163, 0  , 4827 , 197, 139), // #942
  INST(Vfmaddsubpd      , Fma4_Lx            , V(660F3A,5D,_,x,x,_,_,_  ), 0                         , 74 , 0  , 4842 , 270, 141), // #943
  INST(Vfmaddsubps      , Fma4_Lx            , V(660F3A,5C,_,x,x,_,_,_  ), 0                         , 74 , 0  , 4854 , 270, 141), // #944
  INST(Vfmsub132pd      , VexRvm_Lx          , V(660F38,9A,_,x,1,1,4,FV ), 0                         , 162, 0  , 4866 , 196, 139), // #945
  INST(Vfmsub132ps      , VexRvm_Lx          , V(660F38,9A,_,x,0,0,4,FV ), 0                         , 163, 0  , 4878 , 197, 139), // #946
  INST(Vfmsub132sd      , VexRvm             , V(660F38,9B,_,I,1,1,3,T1S), 0                         , 164, 0  , 4890 , 198, 140), // #947
  INST(Vfmsub132ss      , VexRvm             , V(660F38,9B,_,I,0,0,2,T1S), 0                         , 121, 0  , 4902 , 199, 140), // #948
  INST(Vfmsub213pd      , VexRvm_Lx          , V(660F38,AA,_,x,1,1,4,FV ), 0                         , 162, 0  , 4914 , 196, 139), // #949
  INST(Vfmsub213ps      , VexRvm_Lx          , V(660F38,AA,_,x,0,0,4,FV ), 0                         , 163, 0  , 4926 , 197, 139), // #950
  INST(Vfmsub213sd      , VexRvm             , V(660F38,AB,_,I,1,1,3,T1S), 0                         , 164, 0  , 4938 , 198, 140), // #951
  INST(Vfmsub213ss      , VexRvm             , V(660F38,AB,_,I,0,0,2,T1S), 0                         , 121, 0  , 4950 , 199, 140), // #952
  INST(Vfmsub231pd      , VexRvm_Lx          , V(660F38,BA,_,x,1,1,4,FV ), 0                         , 162, 0  , 4962 , 196, 139), // #953
  INST(Vfmsub231ps      , VexRvm_Lx          , V(660F38,BA,_,x,0,0,4,FV ), 0                         , 163, 0  , 4974 , 197, 139), // #954
  INST(Vfmsub231sd      , VexRvm             , V(660F38,BB,_,I,1,1,3,T1S), 0                         , 164, 0  , 4986 , 198, 140), // #955
  INST(Vfmsub231ss      , VexRvm             , V(660F38,BB,_,I,0,0,2,T1S), 0                         , 121, 0  , 4998 , 199, 140), // #956
  INST(Vfmsubadd132pd   , VexRvm_Lx          , V(660F38,97,_,x,1,1,4,FV ), 0                         , 162, 0  , 5010 , 196, 139), // #957
  INST(Vfmsubadd132ps   , VexRvm_Lx          , V(660F38,97,_,x,0,0,4,FV ), 0                         , 163, 0  , 5025 , 197, 139), // #958
  INST(Vfmsubadd213pd   , VexRvm_Lx          , V(660F38,A7,_,x,1,1,4,FV ), 0                         , 162, 0  , 5040 , 196, 139), // #959
  INST(Vfmsubadd213ps   , VexRvm_Lx          , V(660F38,A7,_,x,0,0,4,FV ), 0                         , 163, 0  , 5055 , 197, 139), // #960
  INST(Vfmsubadd231pd   , VexRvm_Lx          , V(660F38,B7,_,x,1,1,4,FV ), 0                         , 162, 0  , 5070 , 196, 139), // #961
  INST(Vfmsubadd231ps   , VexRvm_Lx          , V(660F38,B7,_,x,0,0,4,FV ), 0                         , 163, 0  , 5085 , 197, 139), // #962
  INST(Vfmsubaddpd      , Fma4_Lx            , V(660F3A,5F,_,x,x,_,_,_  ), 0                         , 74 , 0  , 5100 , 270, 141), // #963
  INST(Vfmsubaddps      , Fma4_Lx            , V(660F3A,5E,_,x,x,_,_,_  ), 0                         , 74 , 0  , 5112 , 270, 141), // #964
  INST(Vfmsubpd         , Fma4_Lx            , V(660F3A,6D,_,x,x,_,_,_  ), 0                         , 74 , 0  , 5124 , 270, 141), // #965
  INST(Vfmsubps         , Fma4_Lx            , V(660F3A,6C,_,x,x,_,_,_  ), 0                         , 74 , 0  , 5133 , 270, 141), // #966
  INST(Vfmsubsd         , Fma4               , V(660F3A,6F,_,0,x,_,_,_  ), 0                         , 74 , 0  , 5142 , 271, 141), // #967
  INST(Vfmsubss         , Fma4               , V(660F3A,6E,_,0,x,_,_,_  ), 0                         , 74 , 0  , 5151 , 272, 141), // #968
  INST(Vfnmadd132pd     , VexRvm_Lx          , V(660F38,9C,_,x,1,1,4,FV ), 0                         , 162, 0  , 5160 , 196, 139), // #969
  INST(Vfnmadd132ps     , VexRvm_Lx          , V(660F38,9C,_,x,0,0,4,FV ), 0                         , 163, 0  , 5173 , 197, 139), // #970
  INST(Vfnmadd132sd     , VexRvm             , V(660F38,9D,_,I,1,1,3,T1S), 0                         , 164, 0  , 5186 , 198, 140), // #971
  INST(Vfnmadd132ss     , VexRvm             , V(660F38,9D,_,I,0,0,2,T1S), 0                         , 121, 0  , 5199 , 199, 140), // #972
  INST(Vfnmadd213pd     , VexRvm_Lx          , V(660F38,AC,_,x,1,1,4,FV ), 0                         , 162, 0  , 5212 , 196, 139), // #973
  INST(Vfnmadd213ps     , VexRvm_Lx          , V(660F38,AC,_,x,0,0,4,FV ), 0                         , 163, 0  , 5225 , 197, 139), // #974
  INST(Vfnmadd213sd     , VexRvm             , V(660F38,AD,_,I,1,1,3,T1S), 0                         , 164, 0  , 5238 , 198, 140), // #975
  INST(Vfnmadd213ss     , VexRvm             , V(660F38,AD,_,I,0,0,2,T1S), 0                         , 121, 0  , 5251 , 199, 140), // #976
  INST(Vfnmadd231pd     , VexRvm_Lx          , V(660F38,BC,_,x,1,1,4,FV ), 0                         , 162, 0  , 5264 , 196, 139), // #977
  INST(Vfnmadd231ps     , VexRvm_Lx          , V(660F38,BC,_,x,0,0,4,FV ), 0                         , 163, 0  , 5277 , 197, 139), // #978
  INST(Vfnmadd231sd     , VexRvm             , V(660F38,BD,_,I,1,1,3,T1S), 0                         , 164, 0  , 5290 , 198, 140), // #979
  INST(Vfnmadd231ss     , VexRvm             , V(660F38,BD,_,I,0,0,2,T1S), 0                         , 121, 0  , 5303 , 199, 140), // #980
  INST(Vfnmaddpd        , Fma4_Lx            , V(660F3A,79,_,x,x,_,_,_  ), 0                         , 74 , 0  , 5316 , 270, 141), // #981
  INST(Vfnmaddps        , Fma4_Lx            , V(660F3A,78,_,x,x,_,_,_  ), 0                         , 74 , 0  , 5326 , 270, 141), // #982
  INST(Vfnmaddsd        , Fma4               , V(660F3A,7B,_,0,x,_,_,_  ), 0                         , 74 , 0  , 5336 , 271, 141), // #983
  INST(Vfnmaddss        , Fma4               , V(660F3A,7A,_,0,x,_,_,_  ), 0                         , 74 , 0  , 5346 , 272, 141), // #984
  INST(Vfnmsub132pd     , VexRvm_Lx          , V(660F38,9E,_,x,1,1,4,FV ), 0                         , 162, 0  , 5356 , 196, 139), // #985
  INST(Vfnmsub132ps     , VexRvm_Lx          , V(660F38,9E,_,x,0,0,4,FV ), 0                         , 163, 0  , 5369 , 197, 139), // #986
  INST(Vfnmsub132sd     , VexRvm             , V(660F38,9F,_,I,1,1,3,T1S), 0                         , 164, 0  , 5382 , 198, 140), // #987
  INST(Vfnmsub132ss     , VexRvm             , V(660F38,9F,_,I,0,0,2,T1S), 0                         , 121, 0  , 5395 , 199, 140), // #988
  INST(Vfnmsub213pd     , VexRvm_Lx          , V(660F38,AE,_,x,1,1,4,FV ), 0                         , 162, 0  , 5408 , 196, 139), // #989
  INST(Vfnmsub213ps     , VexRvm_Lx          , V(660F38,AE,_,x,0,0,4,FV ), 0                         , 163, 0  , 5421 , 197, 139), // #990
  INST(Vfnmsub213sd     , VexRvm             , V(660F38,AF,_,I,1,1,3,T1S), 0                         , 164, 0  , 5434 , 198, 140), // #991
  INST(Vfnmsub213ss     , VexRvm             , V(660F38,AF,_,I,0,0,2,T1S), 0                         , 121, 0  , 5447 , 199, 140), // #992
  INST(Vfnmsub231pd     , VexRvm_Lx          , V(660F38,BE,_,x,1,1,4,FV ), 0                         , 162, 0  , 5460 , 196, 139), // #993
  INST(Vfnmsub231ps     , VexRvm_Lx          , V(660F38,BE,_,x,0,0,4,FV ), 0                         , 163, 0  , 5473 , 197, 139), // #994
  INST(Vfnmsub231sd     , VexRvm             , V(660F38,BF,_,I,1,1,3,T1S), 0                         , 164, 0  , 5486 , 198, 140), // #995
  INST(Vfnmsub231ss     , VexRvm             , V(660F38,BF,_,I,0,0,2,T1S), 0                         , 121, 0  , 5499 , 199, 140), // #996
  INST(Vfnmsubpd        , Fma4_Lx            , V(660F3A,7D,_,x,x,_,_,_  ), 0                         , 74 , 0  , 5512 , 270, 141), // #997
  INST(Vfnmsubps        , Fma4_Lx            , V(660F3A,7C,_,x,x,_,_,_  ), 0                         , 74 , 0  , 5522 , 270, 141), // #998
  INST(Vfnmsubsd        , Fma4               , V(660F3A,7F,_,0,x,_,_,_  ), 0                         , 74 , 0  , 5532 , 271, 141), // #999
  INST(Vfnmsubss        , Fma4               , V(660F3A,7E,_,0,x,_,_,_  ), 0                         , 74 , 0  , 5542 , 272, 141), // #1000
  INST(Vfpclasspd       , VexRmi_Lx          , E(660F3A,66,_,x,_,1,4,FV ), 0                         , 111, 0  , 5552 , 273, 131), // #1001
  INST(Vfpclassps       , VexRmi_Lx          , E(660F3A,66,_,x,_,0,4,FV ), 0                         , 110, 0  , 5563 , 274, 131), // #1002
  INST(Vfpclasssd       , VexRmi_Lx          , E(660F3A,67,_,I,_,1,3,T1S), 0                         , 160, 0  , 5574 , 275, 66 ), // #1003
  INST(Vfpclassss       , VexRmi_Lx          , E(660F3A,67,_,I,_,0,2,T1S), 0                         , 161, 0  , 5585 , 276, 66 ), // #1004
  INST(Vfrczpd          , VexRm_Lx           , V(XOP_M9,81,_,x,0,_,_,_  ), 0                         , 80 , 0  , 5596 , 277, 142), // #1005
  INST(Vfrczps          , VexRm_Lx           , V(XOP_M9,80,_,x,0,_,_,_  ), 0                         , 80 , 0  , 5604 , 277, 142), // #1006
  INST(Vfrczsd          , VexRm              , V(XOP_M9,83,_,0,0,_,_,_  ), 0                         , 80 , 0  , 5612 , 278, 142), // #1007
  INST(Vfrczss          , VexRm              , V(XOP_M9,82,_,0,0,_,_,_  ), 0                         , 80 , 0  , 5620 , 279, 142), // #1008
  INST(Vgatherdpd       , VexRmvRm_VM        , V(660F38,92,_,x,1,_,_,_  ), E(660F38,92,_,x,_,1,3,T1S), 165, 81 , 5628 , 280, 143), // #1009
  INST(Vgatherdps       , VexRmvRm_VM        , V(660F38,92,_,x,0,_,_,_  ), E(660F38,92,_,x,_,0,2,T1S), 97 , 82 , 5639 , 281, 143), // #1010
  INST(Vgatherpf0dpd    , VexM_VM            , E(660F38,C6,1,2,_,1,3,T1S), 0                         , 166, 0  , 5650 , 282, 144), // #1011
  INST(Vgatherpf0dps    , VexM_VM            , E(660F38,C6,1,2,_,0,2,T1S), 0                         , 167, 0  , 5664 , 283, 144), // #1012
  INST(Vgatherpf0qpd    , VexM_VM            , E(660F38,C7,1,2,_,1,3,T1S), 0                         , 166, 0  , 5678 , 284, 144), // #1013
  INST(Vgatherpf0qps    , VexM_VM            , E(660F38,C7,1,2,_,0,2,T1S), 0                         , 167, 0  , 5692 , 284, 144), // #1014
  INST(Vgatherpf1dpd    , VexM_VM            , E(660F38,C6,2,2,_,1,3,T1S), 0                         , 168, 0  , 5706 , 282, 144), // #1015
  INST(Vgatherpf1dps    , VexM_VM            , E(660F38,C6,2,2,_,0,2,T1S), 0                         , 169, 0  , 5720 , 283, 144), // #1016
  INST(Vgatherpf1qpd    , VexM_VM            , E(660F38,C7,2,2,_,1,3,T1S), 0                         , 168, 0  , 5734 , 284, 144), // #1017
  INST(Vgatherpf1qps    , VexM_VM            , E(660F38,C7,2,2,_,0,2,T1S), 0                         , 169, 0  , 5748 , 284, 144), // #1018
  INST(Vgatherqpd       , VexRmvRm_VM        , V(660F38,93,_,x,1,_,_,_  ), E(660F38,93,_,x,_,1,3,T1S), 165, 83 , 5762 , 285, 143), // #1019
  INST(Vgatherqps       , VexRmvRm_VM        , V(660F38,93,_,x,0,_,_,_  ), E(660F38,93,_,x,_,0,2,T1S), 97 , 84 , 5773 , 286, 143), // #1020
  INST(Vgetexppd        , VexRm_Lx           , E(660F38,42,_,x,_,1,4,FV ), 0                         , 112, 0  , 5784 , 246, 129), // #1021
  INST(Vgetexpps        , VexRm_Lx           , E(660F38,42,_,x,_,0,4,FV ), 0                         , 113, 0  , 5794 , 250, 129), // #1022
  INST(Vgetexpsd        , VexRvm             , E(660F38,43,_,I,_,1,3,T1S), 0                         , 124, 0  , 5804 , 287, 68 ), // #1023
  INST(Vgetexpss        , VexRvm             , E(660F38,43,_,I,_,0,2,T1S), 0                         , 125, 0  , 5814 , 288, 68 ), // #1024
  INST(Vgetmantpd       , VexRmi_Lx          , E(660F3A,26,_,x,_,1,4,FV ), 0                         , 111, 0  , 5824 , 289, 129), // #1025
  INST(Vgetmantps       , VexRmi_Lx          , E(660F3A,26,_,x,_,0,4,FV ), 0                         , 110, 0  , 5835 , 290, 129), // #1026
  INST(Vgetmantsd       , VexRvmi            , E(660F3A,27,_,I,_,1,3,T1S), 0                         , 160, 0  , 5846 , 268, 68 ), // #1027
  INST(Vgetmantss       , VexRvmi            , E(660F3A,27,_,I,_,0,2,T1S), 0                         , 161, 0  , 5857 , 269, 68 ), // #1028
  INST(Vgf2p8affineinvqb, VexRvmi_Lx         , V(660F3A,CF,_,x,1,1,4,FV ), 0                         , 170, 0  , 5868 , 291, 145), // #1029
  INST(Vgf2p8affineqb   , VexRvmi_Lx         , V(660F3A,CE,_,x,1,1,4,FV ), 0                         , 170, 0  , 5886 , 291, 145), // #1030
  INST(Vgf2p8mulb       , VexRvm_Lx          , V(660F38,CF,_,x,0,0,4,FV ), 0                         , 163, 0  , 5901 , 292, 145), // #1031
  INST(Vhaddpd          , VexRvm_Lx          , V(660F00,7C,_,x,I,_,_,_  ), 0                         , 70 , 0  , 5912 , 200, 126), // #1032
  INST(Vhaddps          , VexRvm_Lx          , V(F20F00,7C,_,x,I,_,_,_  ), 0                         , 108, 0  , 5920 , 200, 126), // #1033
  INST(Vhsubpd          , VexRvm_Lx          , V(660F00,7D,_,x,I,_,_,_  ), 0                         , 70 , 0  , 5928 , 200, 126), // #1034
  INST(Vhsubps          , VexRvm_Lx          , V(F20F00,7D,_,x,I,_,_,_  ), 0                         , 108, 0  , 5936 , 200, 126), // #1035
  INST(Vinsertf128      , VexRvmi            , V(660F3A,18,_,1,0,_,_,_  ), 0                         , 154, 0  , 5944 , 293, 126), // #1036
  INST(Vinsertf32x4     , VexRvmi_Lx         , E(660F3A,18,_,x,_,0,4,T4 ), 0                         , 155, 0  , 5956 , 294, 129), // #1037
  INST(Vinsertf32x8     , VexRvmi            , E(660F3A,1A,_,2,_,0,5,T8 ), 0                         , 156, 0  , 5969 , 295, 66 ), // #1038
  INST(Vinsertf64x2     , VexRvmi_Lx         , E(660F3A,18,_,x,_,1,4,T2 ), 0                         , 157, 0  , 5982 , 294, 131), // #1039
  INST(Vinsertf64x4     , VexRvmi            , E(660F3A,1A,_,2,_,1,5,T4 ), 0                         , 158, 0  , 5995 , 295, 68 ), // #1040
  INST(Vinserti128      , VexRvmi            , V(660F3A,38,_,1,0,_,_,_  ), 0                         , 154, 0  , 6008 , 293, 132), // #1041
  INST(Vinserti32x4     , VexRvmi_Lx         , E(660F3A,38,_,x,_,0,4,T4 ), 0                         , 155, 0  , 6020 , 294, 129), // #1042
  INST(Vinserti32x8     , VexRvmi            , E(660F3A,3A,_,2,_,0,5,T8 ), 0                         , 156, 0  , 6033 , 295, 66 ), // #1043
  INST(Vinserti64x2     , VexRvmi_Lx         , E(660F3A,38,_,x,_,1,4,T2 ), 0                         , 157, 0  , 6046 , 294, 131), // #1044
  INST(Vinserti64x4     , VexRvmi            , E(660F3A,3A,_,2,_,1,5,T4 ), 0                         , 158, 0  , 6059 , 295, 68 ), // #1045
  INST(Vinsertps        , VexRvmi            , V(660F3A,21,_,0,I,0,2,T1S), 0                         , 159, 0  , 6072 , 296, 125), // #1046
  INST(Vlddqu           , VexRm_Lx           , V(F20F00,F0,_,x,I,_,_,_  ), 0                         , 108, 0  , 6082 , 297, 126), // #1047
  INST(Vldmxcsr         , VexM               , V(000F00,AE,2,0,I,_,_,_  ), 0                         , 171, 0  , 6089 , 298, 126), // #1048
  INST(Vmaskmovdqu      , VexRm_ZDI          , V(660F00,F7,_,0,I,_,_,_  ), 0                         , 70 , 0  , 6098 , 299, 126), // #1049
  INST(Vmaskmovpd       , VexRvmMvr_Lx       , V(660F38,2D,_,x,0,_,_,_  ), V(660F38,2F,_,x,0,_,_,_  ), 97 , 85 , 6110 , 300, 126), // #1050
  INST(Vmaskmovps       , VexRvmMvr_Lx       , V(660F38,2C,_,x,0,_,_,_  ), V(660F38,2E,_,x,0,_,_,_  ), 97 , 86 , 6121 , 300, 126), // #1051
  INST(Vmaxpd           , VexRvm_Lx          , V(660F00,5F,_,x,I,1,4,FV ), 0                         , 104, 0  , 6132 , 301, 124), // #1052
  INST(Vmaxps           , VexRvm_Lx          , V(000F00,5F,_,x,I,0,4,FV ), 0                         , 105, 0  , 6139 , 302, 124), // #1053
  INST(Vmaxsd           , VexRvm             , V(F20F00,5F,_,I,I,1,3,T1S), 0                         , 106, 0  , 6146 , 303, 124), // #1054
  INST(Vmaxss           , VexRvm             , V(F30F00,5F,_,I,I,0,2,T1S), 0                         , 107, 0  , 6153 , 242, 124), // #1055
  INST(Vmcall           , X86Op              , O(000F01,C1,_,_,_,_,_,_  ), 0                         , 21 , 0  , 6160 , 30 , 58 ), // #1056
  INST(Vmclear          , X86M_Only          , O(660F00,C7,6,_,_,_,_,_  ), 0                         , 26 , 0  , 6167 , 32 , 58 ), // #1057
  INST(Vmfunc           , X86Op              , O(000F01,D4,_,_,_,_,_,_  ), 0                         , 21 , 0  , 6175 , 30 , 58 ), // #1058
  INST(Vminpd           , VexRvm_Lx          , V(660F00,5D,_,x,I,1,4,FV ), 0                         , 104, 0  , 6182 , 301, 124), // #1059
  INST(Vminps           , VexRvm_Lx          , V(000F00,5D,_,x,I,0,4,FV ), 0                         , 105, 0  , 6189 , 302, 124), // #1060
  INST(Vminsd           , VexRvm             , V(F20F00,5D,_,I,I,1,3,T1S), 0                         , 106, 0  , 6196 , 303, 124), // #1061
  INST(Vminss           , VexRvm             , V(F30F00,5D,_,I,I,0,2,T1S), 0                         , 107, 0  , 6203 , 242, 124), // #1062
  INST(Vmlaunch         , X86Op              , O(000F01,C2,_,_,_,_,_,_  ), 0                         , 21 , 0  , 6210 , 30 , 58 ), // #1063
  INST(Vmload           , X86Op_xAX          , O(000F01,DA,_,_,_,_,_,_  ), 0                         , 21 , 0  , 6219 , 304, 22 ), // #1064
  INST(Vmmcall          , X86Op              , O(000F01,D9,_,_,_,_,_,_  ), 0                         , 21 , 0  , 6226 , 30 , 22 ), // #1065
  INST(Vmovapd          , VexRmMr_Lx         , V(660F00,28,_,x,I,1,4,FVM), V(660F00,29,_,x,I,1,4,FVM), 172, 87 , 6234 , 305, 124), // #1066
  INST(Vmovaps          , VexRmMr_Lx         , V(000F00,28,_,x,I,0,4,FVM), V(000F00,29,_,x,I,0,4,FVM), 173, 88 , 6242 , 305, 124), // #1067
  INST(Vmovd            , VexMovdMovq        , V(660F00,6E,_,0,0,0,2,T1S), V(660F00,7E,_,0,0,0,2,T1S), 174, 89 , 6250 , 306, 125), // #1068
  INST(Vmovddup         , VexRm_Lx           , V(F20F00,12,_,x,I,1,3,DUP), 0                         , 175, 0  , 6256 , 307, 124), // #1069
  INST(Vmovdqa          , VexRmMr_Lx         , V(660F00,6F,_,x,I,_,_,_  ), V(660F00,7F,_,x,I,_,_,_  ), 70 , 90 , 6265 , 308, 126), // #1070
  INST(Vmovdqa32        , VexRmMr_Lx         , E(660F00,6F,_,x,_,0,4,FVM), E(660F00,7F,_,x,_,0,4,FVM), 176, 91 , 6273 , 309, 129), // #1071
  INST(Vmovdqa64        , VexRmMr_Lx         , E(660F00,6F,_,x,_,1,4,FVM), E(660F00,7F,_,x,_,1,4,FVM), 177, 92 , 6283 , 309, 129), // #1072
  INST(Vmovdqu          , VexRmMr_Lx         , V(F30F00,6F,_,x,I,_,_,_  ), V(F30F00,7F,_,x,I,_,_,_  ), 178, 93 , 6293 , 308, 126), // #1073
  INST(Vmovdqu16        , VexRmMr_Lx         , E(F20F00,6F,_,x,_,1,4,FVM), E(F20F00,7F,_,x,_,1,4,FVM), 179, 94 , 6301 , 309, 137), // #1074
  INST(Vmovdqu32        , VexRmMr_Lx         , E(F30F00,6F,_,x,_,0,4,FVM), E(F30F00,7F,_,x,_,0,4,FVM), 180, 95 , 6311 , 309, 129), // #1075
  INST(Vmovdqu64        , VexRmMr_Lx         , E(F30F00,6F,_,x,_,1,4,FVM), E(F30F00,7F,_,x,_,1,4,FVM), 181, 96 , 6321 , 309, 129), // #1076
  INST(Vmovdqu8         , VexRmMr_Lx         , E(F20F00,6F,_,x,_,0,4,FVM), E(F20F00,7F,_,x,_,0,4,FVM), 182, 97 , 6331 , 309, 137), // #1077
  INST(Vmovhlps         , VexRvm             , V(000F00,12,_,0,I,0,_,_  ), 0                         , 73 , 0  , 6340 , 310, 125), // #1078
  INST(Vmovhpd          , VexRvmMr           , V(660F00,16,_,0,I,1,3,T1S), V(660F00,17,_,0,I,1,3,T1S), 122, 98 , 6349 , 311, 125), // #1079
  INST(Vmovhps          , VexRvmMr           , V(000F00,16,_,0,I,0,3,T2 ), V(000F00,17,_,0,I,0,3,T2 ), 183, 99 , 6357 , 311, 125), // #1080
  INST(Vmovlhps         , VexRvm             , V(000F00,16,_,0,I,0,_,_  ), 0                         , 73 , 0  , 6365 , 310, 125), // #1081
  INST(Vmovlpd          , VexRvmMr           , V(660F00,12,_,0,I,1,3,T1S), V(660F00,13,_,0,I,1,3,T1S), 122, 100, 6374 , 311, 125), // #1082
  INST(Vmovlps          , VexRvmMr           , V(000F00,12,_,0,I,0,3,T2 ), V(000F00,13,_,0,I,0,3,T2 ), 183, 101, 6382 , 311, 125), // #1083
  INST(Vmovmskpd        , VexRm_Lx           , V(660F00,50,_,x,I,_,_,_  ), 0                         , 70 , 0  , 6390 , 312, 126), // #1084
  INST(Vmovmskps        , VexRm_Lx           , V(000F00,50,_,x,I,_,_,_  ), 0                         , 73 , 0  , 6400 , 312, 126), // #1085
  INST(Vmovntdq         , VexMr_Lx           , V(660F00,E7,_,x,I,0,4,FVM), 0                         , 184, 0  , 6410 , 313, 124), // #1086
  INST(Vmovntdqa        , VexRm_Lx           , V(660F38,2A,_,x,I,0,4,FVM), 0                         , 109, 0  , 6419 , 314, 133), // #1087
  INST(Vmovntpd         , VexMr_Lx           , V(660F00,2B,_,x,I,1,4,FVM), 0                         , 172, 0  , 6429 , 313, 124), // #1088
  INST(Vmovntps         , VexMr_Lx           , V(000F00,2B,_,x,I,0,4,FVM), 0                         , 173, 0  , 6438 , 313, 124), // #1089
  INST(Vmovq            , VexMovdMovq        , V(660F00,6E,_,0,I,1,3,T1S), V(660F00,7E,_,0,I,1,3,T1S), 122, 102, 6447 , 315, 125), // #1090
  INST(Vmovsd           , VexMovssMovsd      , V(F20F00,10,_,I,I,1,3,T1S), V(F20F00,11,_,I,I,1,3,T1S), 106, 103, 6453 , 316, 125), // #1091
  INST(Vmovshdup        , VexRm_Lx           , V(F30F00,16,_,x,I,0,4,FVM), 0                         , 185, 0  , 6460 , 317, 124), // #1092
  INST(Vmovsldup        , VexRm_Lx           , V(F30F00,12,_,x,I,0,4,FVM), 0                         , 185, 0  , 6470 , 317, 124), // #1093
  INST(Vmovss           , VexMovssMovsd      , V(F30F00,10,_,I,I,0,2,T1S), V(F30F00,11,_,I,I,0,2,T1S), 107, 104, 6480 , 318, 125), // #1094
  INST(Vmovupd          , VexRmMr_Lx         , V(660F00,10,_,x,I,1,4,FVM), V(660F00,11,_,x,I,1,4,FVM), 172, 105, 6487 , 305, 124), // #1095
  INST(Vmovups          , VexRmMr_Lx         , V(000F00,10,_,x,I,0,4,FVM), V(000F00,11,_,x,I,0,4,FVM), 173, 106, 6495 , 305, 124), // #1096
  INST(Vmpsadbw         , VexRvmi_Lx         , V(660F3A,42,_,x,I,_,_,_  ), 0                         , 74 , 0  , 6503 , 212, 146), // #1097
  INST(Vmptrld          , X86M_Only          , O(000F00,C7,6,_,_,_,_,_  ), 0                         , 81 , 0  , 6512 , 32 , 58 ), // #1098
  INST(Vmptrst          , X86M_Only          , O(000F00,C7,7,_,_,_,_,_  ), 0                         , 22 , 0  , 6520 , 32 , 58 ), // #1099
  INST(Vmread           , X86Mr_NoSize       , O(000F00,78,_,_,_,_,_,_  ), 0                         , 4  , 0  , 6528 , 319, 58 ), // #1100
  INST(Vmresume         , X86Op              , O(000F01,C3,_,_,_,_,_,_  ), 0                         , 21 , 0  , 6535 , 30 , 58 ), // #1101
  INST(Vmrun            , X86Op_xAX          , O(000F01,D8,_,_,_,_,_,_  ), 0                         , 21 , 0  , 6544 , 304, 22 ), // #1102
  INST(Vmsave           , X86Op_xAX          , O(000F01,DB,_,_,_,_,_,_  ), 0                         , 21 , 0  , 6550 , 304, 22 ), // #1103
  INST(Vmulpd           , VexRvm_Lx          , V(660F00,59,_,x,I,1,4,FV ), 0                         , 104, 0  , 6557 , 196, 124), // #1104
  INST(Vmulps           , VexRvm_Lx          , V(000F00,59,_,x,I,0,4,FV ), 0                         , 105, 0  , 6564 , 197, 124), // #1105
  INST(Vmulsd           , VexRvm_Lx          , V(F20F00,59,_,I,I,1,3,T1S), 0                         , 106, 0  , 6571 , 198, 125), // #1106
  INST(Vmulss           , VexRvm_Lx          , V(F30F00,59,_,I,I,0,2,T1S), 0                         , 107, 0  , 6578 , 199, 125), // #1107
  INST(Vmwrite          , X86Rm_NoSize       , O(000F00,79,_,_,_,_,_,_  ), 0                         , 4  , 0  , 6585 , 320, 58 ), // #1108
  INST(Vmxon            , X86M_Only          , O(F30F00,C7,6,_,_,_,_,_  ), 0                         , 24 , 0  , 6593 , 32 , 58 ), // #1109
  INST(Vorpd            , VexRvm_Lx          , V(660F00,56,_,x,I,1,4,FV ), 0                         , 104, 0  , 6599 , 208, 130), // #1110
  INST(Vorps            , VexRvm_Lx          , V(000F00,56,_,x,I,0,4,FV ), 0                         , 105, 0  , 6605 , 209, 130), // #1111
  INST(Vp2intersectd    , VexRvm_Lx_2xK      , E(F20F38,68,_,_,_,0,4,FV ), 0                         , 127, 0  , 6611 , 321, 147), // #1112
  INST(Vp2intersectq    , VexRvm_Lx_2xK      , E(F20F38,68,_,_,_,1,4,FV ), 0                         , 186, 0  , 6625 , 322, 147), // #1113
  INST(Vp4dpwssd        , VexRm_T1_4X        , E(F20F38,52,_,2,_,0,4,T4X), 0                         , 102, 0  , 6639 , 194, 148), // #1114
  INST(Vp4dpwssds       , VexRm_T1_4X        , E(F20F38,53,_,2,_,0,4,T4X), 0                         , 102, 0  , 6649 , 194, 148), // #1115
  INST(Vpabsb           , VexRm_Lx           , V(660F38,1C,_,x,I,_,4,FVM), 0                         , 109, 0  , 6660 , 317, 149), // #1116
  INST(Vpabsd           , VexRm_Lx           , V(660F38,1E,_,x,I,0,4,FV ), 0                         , 163, 0  , 6667 , 317, 133), // #1117
  INST(Vpabsq           , VexRm_Lx           , E(660F38,1F,_,x,_,1,4,FV ), 0                         , 112, 0  , 6674 , 261, 129), // #1118
  INST(Vpabsw           , VexRm_Lx           , V(660F38,1D,_,x,I,_,4,FVM), 0                         , 109, 0  , 6681 , 317, 149), // #1119
  INST(Vpackssdw        , VexRvm_Lx          , V(660F00,6B,_,x,I,0,4,FV ), 0                         , 133, 0  , 6688 , 207, 149), // #1120
  INST(Vpacksswb        , VexRvm_Lx          , V(660F00,63,_,x,I,I,4,FVM), 0                         , 184, 0  , 6698 , 292, 149), // #1121
  INST(Vpackusdw        , VexRvm_Lx          , V(660F38,2B,_,x,I,0,4,FV ), 0                         , 163, 0  , 6708 , 207, 149), // #1122
  INST(Vpackuswb        , VexRvm_Lx          , V(660F00,67,_,x,I,I,4,FVM), 0                         , 184, 0  , 6718 , 292, 149), // #1123
  INST(Vpaddb           , VexRvm_Lx          , V(660F00,FC,_,x,I,I,4,FVM), 0                         , 184, 0  , 6728 , 292, 149), // #1124
  INST(Vpaddd           , VexRvm_Lx          , V(660F00,FE,_,x,I,0,4,FV ), 0                         , 133, 0  , 6735 , 207, 133), // #1125
  INST(Vpaddq           , VexRvm_Lx          , V(660F00,D4,_,x,I,1,4,FV ), 0                         , 104, 0  , 6742 , 206, 133), // #1126
  INST(Vpaddsb          , VexRvm_Lx          , V(660F00,EC,_,x,I,I,4,FVM), 0                         , 184, 0  , 6749 , 292, 149), // #1127
  INST(Vpaddsw          , VexRvm_Lx          , V(660F00,ED,_,x,I,I,4,FVM), 0                         , 184, 0  , 6757 , 292, 149), // #1128
  INST(Vpaddusb         , VexRvm_Lx          , V(660F00,DC,_,x,I,I,4,FVM), 0                         , 184, 0  , 6765 , 292, 149), // #1129
  INST(Vpaddusw         , VexRvm_Lx          , V(660F00,DD,_,x,I,I,4,FVM), 0                         , 184, 0  , 6774 , 292, 149), // #1130
  INST(Vpaddw           , VexRvm_Lx          , V(660F00,FD,_,x,I,I,4,FVM), 0                         , 184, 0  , 6783 , 292, 149), // #1131
  INST(Vpalignr         , VexRvmi_Lx         , V(660F3A,0F,_,x,I,I,4,FVM), 0                         , 187, 0  , 6790 , 291, 149), // #1132
  INST(Vpand            , VexRvm_Lx          , V(660F00,DB,_,x,I,_,_,_  ), 0                         , 70 , 0  , 6799 , 323, 146), // #1133
  INST(Vpandd           , VexRvm_Lx          , E(660F00,DB,_,x,_,0,4,FV ), 0                         , 188, 0  , 6805 , 324, 129), // #1134
  INST(Vpandn           , VexRvm_Lx          , V(660F00,DF,_,x,I,_,_,_  ), 0                         , 70 , 0  , 6812 , 325, 146), // #1135
  INST(Vpandnd          , VexRvm_Lx          , E(660F00,DF,_,x,_,0,4,FV ), 0                         , 188, 0  , 6819 , 326, 129), // #1136
  INST(Vpandnq          , VexRvm_Lx          , E(660F00,DF,_,x,_,1,4,FV ), 0                         , 130, 0  , 6827 , 327, 129), // #1137
  INST(Vpandq           , VexRvm_Lx          , E(660F00,DB,_,x,_,1,4,FV ), 0                         , 130, 0  , 6835 , 328, 129), // #1138
  INST(Vpavgb           , VexRvm_Lx          , V(660F00,E0,_,x,I,I,4,FVM), 0                         , 184, 0  , 6842 , 292, 149), // #1139
  INST(Vpavgw           , VexRvm_Lx          , V(660F00,E3,_,x,I,I,4,FVM), 0                         , 184, 0  , 6849 , 292, 149), // #1140
  INST(Vpblendd         , VexRvmi_Lx         , V(660F3A,02,_,x,0,_,_,_  ), 0                         , 74 , 0  , 6856 , 212, 132), // #1141
  INST(Vpblendmb        , VexRvm_Lx          , E(660F38,66,_,x,_,0,4,FVM), 0                         , 189, 0  , 6865 , 329, 137), // #1142
  INST(Vpblendmd        , VexRvm_Lx          , E(660F38,64,_,x,_,0,4,FV ), 0                         , 113, 0  , 6875 , 211, 129), // #1143
  INST(Vpblendmq        , VexRvm_Lx          , E(660F38,64,_,x,_,1,4,FV ), 0                         , 112, 0  , 6885 , 210, 129), // #1144
  INST(Vpblendmw        , VexRvm_Lx          , E(660F38,66,_,x,_,1,4,FVM), 0                         , 190, 0  , 6895 , 329, 137), // #1145
  INST(Vpblendvb        , VexRvmr_Lx         , V(660F3A,4C,_,x,0,_,_,_  ), 0                         , 74 , 0  , 6905 , 213, 146), // #1146
  INST(Vpblendw         , VexRvmi_Lx         , V(660F3A,0E,_,x,I,_,_,_  ), 0                         , 74 , 0  , 6915 , 212, 146), // #1147
  INST(Vpbroadcastb     , VexRm_Lx_Bcst      , V(660F38,78,_,x,0,0,0,T1S), E(660F38,7A,_,x,0,0,0,T1S), 191, 107, 6924 , 330, 150), // #1148
  INST(Vpbroadcastd     , VexRm_Lx_Bcst      , V(660F38,58,_,x,0,0,2,T1S), E(660F38,7C,_,x,0,0,0,T1S), 121, 108, 6937 , 331, 143), // #1149
  INST(Vpbroadcastmb2q  , VexRm_Lx           , E(F30F38,2A,_,x,_,1,_,_  ), 0                         , 192, 0  , 6950 , 332, 151), // #1150
  INST(Vpbroadcastmw2d  , VexRm_Lx           , E(F30F38,3A,_,x,_,0,_,_  ), 0                         , 193, 0  , 6966 , 332, 151), // #1151
  INST(Vpbroadcastq     , VexRm_Lx_Bcst      , V(660F38,59,_,x,0,1,3,T1S), E(660F38,7C,_,x,0,1,0,T1S), 120, 109, 6982 , 333, 143), // #1152
  INST(Vpbroadcastw     , VexRm_Lx_Bcst      , V(660F38,79,_,x,0,0,1,T1S), E(660F38,7B,_,x,0,0,0,T1S), 194, 110, 6995 , 334, 150), // #1153
  INST(Vpclmulqdq       , VexRvmi_Lx         , V(660F3A,44,_,x,I,_,4,FVM), 0                         , 187, 0  , 7008 , 335, 152), // #1154
  INST(Vpcmov           , VexRvrmRvmr_Lx     , V(XOP_M8,A2,_,x,x,_,_,_  ), 0                         , 195, 0  , 7019 , 270, 142), // #1155
  INST(Vpcmpb           , VexRvmi_Lx         , E(660F3A,3F,_,x,_,0,4,FVM), 0                         , 151, 0  , 7026 , 336, 137), // #1156
  INST(Vpcmpd           , VexRvmi_Lx         , E(660F3A,1F,_,x,_,0,4,FV ), 0                         , 110, 0  , 7033 , 337, 129), // #1157
  INST(Vpcmpeqb         , VexRvm_Lx_KEvex    , V(660F00,74,_,x,I,I,4,FV ), 0                         , 133, 0  , 7040 , 338, 149), // #1158
  INST(Vpcmpeqd         , VexRvm_Lx_KEvex    , V(660F00,76,_,x,I,0,4,FVM), 0                         , 184, 0  , 7049 , 339, 133), // #1159
  INST(Vpcmpeqq         , VexRvm_Lx_KEvex    , V(660F38,29,_,x,I,1,4,FVM), 0                         , 196, 0  , 7058 , 340, 133), // #1160
  INST(Vpcmpeqw         , VexRvm_Lx_KEvex    , V(660F00,75,_,x,I,I,4,FV ), 0                         , 133, 0  , 7067 , 338, 149), // #1161
  INST(Vpcmpestri       , VexRmi             , V(660F3A,61,_,0,I,_,_,_  ), 0                         , 74 , 0  , 7076 , 341, 153), // #1162
  INST(Vpcmpestrm       , VexRmi             , V(660F3A,60,_,0,I,_,_,_  ), 0                         , 74 , 0  , 7087 , 342, 153), // #1163
  INST(Vpcmpgtb         , VexRvm_Lx_KEvex    , V(660F00,64,_,x,I,I,4,FV ), 0                         , 133, 0  , 7098 , 338, 149), // #1164
  INST(Vpcmpgtd         , VexRvm_Lx_KEvex    , V(660F00,66,_,x,I,0,4,FVM), 0                         , 184, 0  , 7107 , 339, 133), // #1165
  INST(Vpcmpgtq         , VexRvm_Lx_KEvex    , V(660F38,37,_,x,I,1,4,FVM), 0                         , 196, 0  , 7116 , 340, 133), // #1166
  INST(Vpcmpgtw         , VexRvm_Lx_KEvex    , V(660F00,65,_,x,I,I,4,FV ), 0                         , 133, 0  , 7125 , 338, 149), // #1167
  INST(Vpcmpistri       , VexRmi             , V(660F3A,63,_,0,I,_,_,_  ), 0                         , 74 , 0  , 7134 , 343, 153), // #1168
  INST(Vpcmpistrm       , VexRmi             , V(660F3A,62,_,0,I,_,_,_  ), 0                         , 74 , 0  , 7145 , 344, 153), // #1169
  INST(Vpcmpq           , VexRvmi_Lx         , E(660F3A,1F,_,x,_,1,4,FV ), 0                         , 111, 0  , 7156 , 345, 129), // #1170
  INST(Vpcmpub          , VexRvmi_Lx         , E(660F3A,3E,_,x,_,0,4,FVM), 0                         , 151, 0  , 7163 , 336, 137), // #1171
  INST(Vpcmpud          , VexRvmi_Lx         , E(660F3A,1E,_,x,_,0,4,FV ), 0                         , 110, 0  , 7171 , 337, 129), // #1172
  INST(Vpcmpuq          , VexRvmi_Lx         , E(660F3A,1E,_,x,_,1,4,FV ), 0                         , 111, 0  , 7179 , 345, 129), // #1173
  INST(Vpcmpuw          , VexRvmi_Lx         , E(660F3A,3E,_,x,_,1,4,FVM), 0                         , 197, 0  , 7187 , 345, 137), // #1174
  INST(Vpcmpw           , VexRvmi_Lx         , E(660F3A,3F,_,x,_,1,4,FVM), 0                         , 197, 0  , 7195 , 345, 137), // #1175
  INST(Vpcomb           , VexRvmi            , V(XOP_M8,CC,_,0,0,_,_,_  ), 0                         , 195, 0  , 7202 , 258, 142), // #1176
  INST(Vpcomd           , VexRvmi            , V(XOP_M8,CE,_,0,0,_,_,_  ), 0                         , 195, 0  , 7209 , 258, 142), // #1177
  INST(Vpcompressb      , VexMr_Lx           , E(660F38,63,_,x,_,0,0,T1S), 0                         , 198, 0  , 7216 , 227, 154), // #1178
  INST(Vpcompressd      , VexMr_Lx           , E(660F38,8B,_,x,_,0,2,T1S), 0                         , 125, 0  , 7228 , 227, 129), // #1179
  INST(Vpcompressq      , VexMr_Lx           , E(660F38,8B,_,x,_,1,3,T1S), 0                         , 124, 0  , 7240 , 227, 129), // #1180
  INST(Vpcompressw      , VexMr_Lx           , E(660F38,63,_,x,_,1,1,T1S), 0                         , 199, 0  , 7252 , 227, 154), // #1181
  INST(Vpcomq           , VexRvmi            , V(XOP_M8,CF,_,0,0,_,_,_  ), 0                         , 195, 0  , 7264 , 258, 142), // #1182
  INST(Vpcomub          , VexRvmi            , V(XOP_M8,EC,_,0,0,_,_,_  ), 0                         , 195, 0  , 7271 , 258, 142), // #1183
  INST(Vpcomud          , VexRvmi            , V(XOP_M8,EE,_,0,0,_,_,_  ), 0                         , 195, 0  , 7279 , 258, 142), // #1184
  INST(Vpcomuq          , VexRvmi            , V(XOP_M8,EF,_,0,0,_,_,_  ), 0                         , 195, 0  , 7287 , 258, 142), // #1185
  INST(Vpcomuw          , VexRvmi            , V(XOP_M8,ED,_,0,0,_,_,_  ), 0                         , 195, 0  , 7295 , 258, 142), // #1186
  INST(Vpcomw           , VexRvmi            , V(XOP_M8,CD,_,0,0,_,_,_  ), 0                         , 195, 0  , 7303 , 258, 142), // #1187
  INST(Vpconflictd      , VexRm_Lx           , E(660F38,C4,_,x,_,0,4,FV ), 0                         , 113, 0  , 7310 , 346, 151), // #1188
  INST(Vpconflictq      , VexRm_Lx           , E(660F38,C4,_,x,_,1,4,FV ), 0                         , 112, 0  , 7322 , 346, 151), // #1189
  INST(Vpdpbusd         , VexRvm_Lx          , V(660F38,50,_,x,_,0,4,FV ), 0                         , 163, 0  , 7334 , 347, 155), // #1190
  INST(Vpdpbusds        , VexRvm_Lx          , V(660F38,51,_,x,_,0,4,FV ), 0                         , 163, 0  , 7343 , 347, 155), // #1191
  INST(Vpdpwssd         , VexRvm_Lx          , V(660F38,52,_,x,_,0,4,FV ), 0                         , 163, 0  , 7353 , 347, 155), // #1192
  INST(Vpdpwssds        , VexRvm_Lx          , V(660F38,53,_,x,_,0,4,FV ), 0                         , 163, 0  , 7362 , 347, 155), // #1193
  INST(Vperm2f128       , VexRvmi            , V(660F3A,06,_,1,0,_,_,_  ), 0                         , 154, 0  , 7372 , 348, 126), // #1194
  INST(Vperm2i128       , VexRvmi            , V(660F3A,46,_,1,0,_,_,_  ), 0                         , 154, 0  , 7383 , 348, 132), // #1195
  INST(Vpermb           , VexRvm_Lx          , E(660F38,8D,_,x,_,0,4,FVM), 0                         , 189, 0  , 7394 , 329, 156), // #1196
  INST(Vpermd           , VexRvm_Lx          , V(660F38,36,_,x,0,0,4,FV ), 0                         , 163, 0  , 7401 , 349, 143), // #1197
  INST(Vpermi2b         , VexRvm_Lx          , E(660F38,75,_,x,_,0,4,FVM), 0                         , 189, 0  , 7408 , 329, 156), // #1198
  INST(Vpermi2d         , VexRvm_Lx          , E(660F38,76,_,x,_,0,4,FV ), 0                         , 113, 0  , 7417 , 211, 129), // #1199
  INST(Vpermi2pd        , VexRvm_Lx          , E(660F38,77,_,x,_,1,4,FV ), 0                         , 112, 0  , 7426 , 210, 129), // #1200
  INST(Vpermi2ps        , VexRvm_Lx          , E(660F38,77,_,x,_,0,4,FV ), 0                         , 113, 0  , 7436 , 211, 129), // #1201
  INST(Vpermi2q         , VexRvm_Lx          , E(660F38,76,_,x,_,1,4,FV ), 0                         , 112, 0  , 7446 , 210, 129), // #1202
  INST(Vpermi2w         , VexRvm_Lx          , E(660F38,75,_,x,_,1,4,FVM), 0                         , 190, 0  , 7455 , 329, 137), // #1203
  INST(Vpermil2pd       , VexRvrmiRvmri_Lx   , V(660F3A,49,_,x,x,_,_,_  ), 0                         , 74 , 0  , 7464 , 350, 142), // #1204
  INST(Vpermil2ps       , VexRvrmiRvmri_Lx   , V(660F3A,48,_,x,x,_,_,_  ), 0                         , 74 , 0  , 7475 , 350, 142), // #1205
  INST(Vpermilpd        , VexRvmRmi_Lx       , V(660F38,0D,_,x,0,1,4,FV ), V(660F3A,05,_,x,0,1,4,FV ), 200, 111, 7486 , 351, 124), // #1206
  INST(Vpermilps        , VexRvmRmi_Lx       , V(660F38,0C,_,x,0,0,4,FV ), V(660F3A,04,_,x,0,0,4,FV ), 163, 112, 7496 , 351, 124), // #1207
  INST(Vpermpd          , VexRvmRmi_Lx       , E(660F38,16,_,x,1,1,4,FV ), V(660F3A,01,_,x,1,1,4,FV ), 201, 113, 7506 , 352, 143), // #1208
  INST(Vpermps          , VexRvm_Lx          , V(660F38,16,_,x,0,0,4,FV ), 0                         , 163, 0  , 7514 , 349, 143), // #1209
  INST(Vpermq           , VexRvmRmi_Lx       , E(660F38,36,_,x,_,1,4,FV ), V(660F3A,00,_,x,1,1,4,FV ), 112, 114, 7522 , 352, 143), // #1210
  INST(Vpermt2b         , VexRvm_Lx          , E(660F38,7D,_,x,_,0,4,FVM), 0                         , 189, 0  , 7529 , 329, 156), // #1211
  INST(Vpermt2d         , VexRvm_Lx          , E(660F38,7E,_,x,_,0,4,FV ), 0                         , 113, 0  , 7538 , 211, 129), // #1212
  INST(Vpermt2pd        , VexRvm_Lx          , E(660F38,7F,_,x,_,1,4,FV ), 0                         , 112, 0  , 7547 , 210, 129), // #1213
  INST(Vpermt2ps        , VexRvm_Lx          , E(660F38,7F,_,x,_,0,4,FV ), 0                         , 113, 0  , 7557 , 211, 129), // #1214
  INST(Vpermt2q         , VexRvm_Lx          , E(660F38,7E,_,x,_,1,4,FV ), 0                         , 112, 0  , 7567 , 210, 129), // #1215
  INST(Vpermt2w         , VexRvm_Lx          , E(660F38,7D,_,x,_,1,4,FVM), 0                         , 190, 0  , 7576 , 329, 137), // #1216
  INST(Vpermw           , VexRvm_Lx          , E(660F38,8D,_,x,_,1,4,FVM), 0                         , 190, 0  , 7585 , 329, 137), // #1217
  INST(Vpexpandb        , VexRm_Lx           , E(660F38,62,_,x,_,0,0,T1S), 0                         , 198, 0  , 7592 , 261, 154), // #1218
  INST(Vpexpandd        , VexRm_Lx           , E(660F38,89,_,x,_,0,2,T1S), 0                         , 125, 0  , 7602 , 261, 129), // #1219
  INST(Vpexpandq        , VexRm_Lx           , E(660F38,89,_,x,_,1,3,T1S), 0                         , 124, 0  , 7612 , 261, 129), // #1220
  INST(Vpexpandw        , VexRm_Lx           , E(660F38,62,_,x,_,1,1,T1S), 0                         , 199, 0  , 7622 , 261, 154), // #1221
  INST(Vpextrb          , VexMri             , V(660F3A,14,_,0,0,I,0,T1S), 0                         , 202, 0  , 7632 , 353, 157), // #1222
  INST(Vpextrd          , VexMri             , V(660F3A,16,_,0,0,0,2,T1S), 0                         , 159, 0  , 7640 , 265, 158), // #1223
  INST(Vpextrq          , VexMri             , V(660F3A,16,_,0,1,1,3,T1S), 0                         , 203, 0  , 7648 , 354, 158), // #1224
  INST(Vpextrw          , VexMri_Vpextrw     , V(660F3A,15,_,0,0,I,1,T1S), 0                         , 204, 0  , 7656 , 355, 157), // #1225
  INST(Vpgatherdd       , VexRmvRm_VM        , V(660F38,90,_,x,0,_,_,_  ), E(660F38,90,_,x,_,0,2,T1S), 97 , 115, 7664 , 281, 143), // #1226
  INST(Vpgatherdq       , VexRmvRm_VM        , V(660F38,90,_,x,1,_,_,_  ), E(660F38,90,_,x,_,1,3,T1S), 165, 116, 7675 , 280, 143), // #1227
  INST(Vpgatherqd       , VexRmvRm_VM        , V(660F38,91,_,x,0,_,_,_  ), E(660F38,91,_,x,_,0,2,T1S), 97 , 117, 7686 , 286, 143), // #1228
  INST(Vpgatherqq       , VexRmvRm_VM        , V(660F38,91,_,x,1,_,_,_  ), E(660F38,91,_,x,_,1,3,T1S), 165, 118, 7697 , 285, 143), // #1229
  INST(Vphaddbd         , VexRm              , V(XOP_M9,C2,_,0,0,_,_,_  ), 0                         , 80 , 0  , 7708 , 202, 142), // #1230
  INST(Vphaddbq         , VexRm              , V(XOP_M9,C3,_,0,0,_,_,_  ), 0                         , 80 , 0  , 7717 , 202, 142), // #1231
  INST(Vphaddbw         , VexRm              , V(XOP_M9,C1,_,0,0,_,_,_  ), 0                         , 80 , 0  , 7726 , 202, 142), // #1232
  INST(Vphaddd          , VexRvm_Lx          , V(660F38,02,_,x,I,_,_,_  ), 0                         , 97 , 0  , 7735 , 200, 146), // #1233
  INST(Vphadddq         , VexRm              , V(XOP_M9,CB,_,0,0,_,_,_  ), 0                         , 80 , 0  , 7743 , 202, 142), // #1234
  INST(Vphaddsw         , VexRvm_Lx          , V(660F38,03,_,x,I,_,_,_  ), 0                         , 97 , 0  , 7752 , 200, 146), // #1235
  INST(Vphaddubd        , VexRm              , V(XOP_M9,D2,_,0,0,_,_,_  ), 0                         , 80 , 0  , 7761 , 202, 142), // #1236
  INST(Vphaddubq        , VexRm              , V(XOP_M9,D3,_,0,0,_,_,_  ), 0                         , 80 , 0  , 7771 , 202, 142), // #1237
  INST(Vphaddubw        , VexRm              , V(XOP_M9,D1,_,0,0,_,_,_  ), 0                         , 80 , 0  , 7781 , 202, 142), // #1238
  INST(Vphaddudq        , VexRm              , V(XOP_M9,DB,_,0,0,_,_,_  ), 0                         , 80 , 0  , 7791 , 202, 142), // #1239
  INST(Vphadduwd        , VexRm              , V(XOP_M9,D6,_,0,0,_,_,_  ), 0                         , 80 , 0  , 7801 , 202, 142), // #1240
  INST(Vphadduwq        , VexRm              , V(XOP_M9,D7,_,0,0,_,_,_  ), 0                         , 80 , 0  , 7811 , 202, 142), // #1241
  INST(Vphaddw          , VexRvm_Lx          , V(660F38,01,_,x,I,_,_,_  ), 0                         , 97 , 0  , 7821 , 200, 146), // #1242
  INST(Vphaddwd         , VexRm              , V(XOP_M9,C6,_,0,0,_,_,_  ), 0                         , 80 , 0  , 7829 , 202, 142), // #1243
  INST(Vphaddwq         , VexRm              , V(XOP_M9,C7,_,0,0,_,_,_  ), 0                         , 80 , 0  , 7838 , 202, 142), // #1244
  INST(Vphminposuw      , VexRm              , V(660F38,41,_,0,I,_,_,_  ), 0                         , 97 , 0  , 7847 , 202, 126), // #1245
  INST(Vphsubbw         , VexRm              , V(XOP_M9,E1,_,0,0,_,_,_  ), 0                         , 80 , 0  , 7859 , 202, 142), // #1246
  INST(Vphsubd          , VexRvm_Lx          , V(660F38,06,_,x,I,_,_,_  ), 0                         , 97 , 0  , 7868 , 200, 146), // #1247
  INST(Vphsubdq         , VexRm              , V(XOP_M9,E3,_,0,0,_,_,_  ), 0                         , 80 , 0  , 7876 , 202, 142), // #1248
  INST(Vphsubsw         , VexRvm_Lx          , V(660F38,07,_,x,I,_,_,_  ), 0                         , 97 , 0  , 7885 , 200, 146), // #1249
  INST(Vphsubw          , VexRvm_Lx          , V(660F38,05,_,x,I,_,_,_  ), 0                         , 97 , 0  , 7894 , 200, 146), // #1250
  INST(Vphsubwd         , VexRm              , V(XOP_M9,E2,_,0,0,_,_,_  ), 0                         , 80 , 0  , 7902 , 202, 142), // #1251
  INST(Vpinsrb          , VexRvmi            , V(660F3A,20,_,0,0,I,0,T1S), 0                         , 202, 0  , 7911 , 356, 157), // #1252
  INST(Vpinsrd          , VexRvmi            , V(660F3A,22,_,0,0,0,2,T1S), 0                         , 159, 0  , 7919 , 357, 158), // #1253
  INST(Vpinsrq          , VexRvmi            , V(660F3A,22,_,0,1,1,3,T1S), 0                         , 203, 0  , 7927 , 358, 158), // #1254
  INST(Vpinsrw          , VexRvmi            , V(660F00,C4,_,0,0,I,1,T1S), 0                         , 205, 0  , 7935 , 359, 157), // #1255
  INST(Vplzcntd         , VexRm_Lx           , E(660F38,44,_,x,_,0,4,FV ), 0                         , 113, 0  , 7943 , 346, 151), // #1256
  INST(Vplzcntq         , VexRm_Lx           , E(660F38,44,_,x,_,1,4,FV ), 0                         , 112, 0  , 7952 , 360, 151), // #1257
  INST(Vpmacsdd         , VexRvmr            , V(XOP_M8,9E,_,0,0,_,_,_  ), 0                         , 195, 0  , 7961 , 361, 142), // #1258
  INST(Vpmacsdqh        , VexRvmr            , V(XOP_M8,9F,_,0,0,_,_,_  ), 0                         , 195, 0  , 7970 , 361, 142), // #1259
  INST(Vpmacsdql        , VexRvmr            , V(XOP_M8,97,_,0,0,_,_,_  ), 0                         , 195, 0  , 7980 , 361, 142), // #1260
  INST(Vpmacssdd        , VexRvmr            , V(XOP_M8,8E,_,0,0,_,_,_  ), 0                         , 195, 0  , 7990 , 361, 142), // #1261
  INST(Vpmacssdqh       , VexRvmr            , V(XOP_M8,8F,_,0,0,_,_,_  ), 0                         , 195, 0  , 8000 , 361, 142), // #1262
  INST(Vpmacssdql       , VexRvmr            , V(XOP_M8,87,_,0,0,_,_,_  ), 0                         , 195, 0  , 8011 , 361, 142), // #1263
  INST(Vpmacsswd        , VexRvmr            , V(XOP_M8,86,_,0,0,_,_,_  ), 0                         , 195, 0  , 8022 , 361, 142), // #1264
  INST(Vpmacssww        , VexRvmr            , V(XOP_M8,85,_,0,0,_,_,_  ), 0                         , 195, 0  , 8032 , 361, 142), // #1265
  INST(Vpmacswd         , VexRvmr            , V(XOP_M8,96,_,0,0,_,_,_  ), 0                         , 195, 0  , 8042 , 361, 142), // #1266
  INST(Vpmacsww         , VexRvmr            , V(XOP_M8,95,_,0,0,_,_,_  ), 0                         , 195, 0  , 8051 , 361, 142), // #1267
  INST(Vpmadcsswd       , VexRvmr            , V(XOP_M8,A6,_,0,0,_,_,_  ), 0                         , 195, 0  , 8060 , 361, 142), // #1268
  INST(Vpmadcswd        , VexRvmr            , V(XOP_M8,B6,_,0,0,_,_,_  ), 0                         , 195, 0  , 8071 , 361, 142), // #1269
  INST(Vpmadd52huq      , VexRvm_Lx          , E(660F38,B5,_,x,_,1,4,FV ), 0                         , 112, 0  , 8081 , 210, 159), // #1270
  INST(Vpmadd52luq      , VexRvm_Lx          , E(660F38,B4,_,x,_,1,4,FV ), 0                         , 112, 0  , 8093 , 210, 159), // #1271
  INST(Vpmaddubsw       , VexRvm_Lx          , V(660F38,04,_,x,I,I,4,FVM), 0                         , 109, 0  , 8105 , 292, 149), // #1272
  INST(Vpmaddwd         , VexRvm_Lx          , V(660F00,F5,_,x,I,I,4,FVM), 0                         , 184, 0  , 8116 , 292, 149), // #1273
  INST(Vpmaskmovd       , VexRvmMvr_Lx       , V(660F38,8C,_,x,0,_,_,_  ), V(660F38,8E,_,x,0,_,_,_  ), 97 , 119, 8125 , 300, 132), // #1274
  INST(Vpmaskmovq       , VexRvmMvr_Lx       , V(660F38,8C,_,x,1,_,_,_  ), V(660F38,8E,_,x,1,_,_,_  ), 165, 120, 8136 , 300, 132), // #1275
  INST(Vpmaxsb          , VexRvm_Lx          , V(660F38,3C,_,x,I,I,4,FVM), 0                         , 109, 0  , 8147 , 362, 149), // #1276
  INST(Vpmaxsd          , VexRvm_Lx          , V(660F38,3D,_,x,I,0,4,FV ), 0                         , 163, 0  , 8155 , 209, 133), // #1277
  INST(Vpmaxsq          , VexRvm_Lx          , E(660F38,3D,_,x,_,1,4,FV ), 0                         , 112, 0  , 8163 , 210, 129), // #1278
  INST(Vpmaxsw          , VexRvm_Lx          , V(660F00,EE,_,x,I,I,4,FVM), 0                         , 184, 0  , 8171 , 362, 149), // #1279
  INST(Vpmaxub          , VexRvm_Lx          , V(660F00,DE,_,x,I,I,4,FVM), 0                         , 184, 0  , 8179 , 362, 149), // #1280
  INST(Vpmaxud          , VexRvm_Lx          , V(660F38,3F,_,x,I,0,4,FV ), 0                         , 163, 0  , 8187 , 209, 133), // #1281
  INST(Vpmaxuq          , VexRvm_Lx          , E(660F38,3F,_,x,_,1,4,FV ), 0                         , 112, 0  , 8195 , 210, 129), // #1282
  INST(Vpmaxuw          , VexRvm_Lx          , V(660F38,3E,_,x,I,I,4,FVM), 0                         , 109, 0  , 8203 , 362, 149), // #1283
  INST(Vpminsb          , VexRvm_Lx          , V(660F38,38,_,x,I,I,4,FVM), 0                         , 109, 0  , 8211 , 362, 149), // #1284
  INST(Vpminsd          , VexRvm_Lx          , V(660F38,39,_,x,I,0,4,FV ), 0                         , 163, 0  , 8219 , 209, 133), // #1285
  INST(Vpminsq          , VexRvm_Lx          , E(660F38,39,_,x,_,1,4,FV ), 0                         , 112, 0  , 8227 , 210, 129), // #1286
  INST(Vpminsw          , VexRvm_Lx          , V(660F00,EA,_,x,I,I,4,FVM), 0                         , 184, 0  , 8235 , 362, 149), // #1287
  INST(Vpminub          , VexRvm_Lx          , V(660F00,DA,_,x,I,_,4,FVM), 0                         , 184, 0  , 8243 , 362, 149), // #1288
  INST(Vpminud          , VexRvm_Lx          , V(660F38,3B,_,x,I,0,4,FV ), 0                         , 163, 0  , 8251 , 209, 133), // #1289
  INST(Vpminuq          , VexRvm_Lx          , E(660F38,3B,_,x,_,1,4,FV ), 0                         , 112, 0  , 8259 , 210, 129), // #1290
  INST(Vpminuw          , VexRvm_Lx          , V(660F38,3A,_,x,I,_,4,FVM), 0                         , 109, 0  , 8267 , 362, 149), // #1291
  INST(Vpmovb2m         , VexRm_Lx           , E(F30F38,29,_,x,_,0,_,_  ), 0                         , 193, 0  , 8275 , 363, 137), // #1292
  INST(Vpmovd2m         , VexRm_Lx           , E(F30F38,39,_,x,_,0,_,_  ), 0                         , 193, 0  , 8284 , 363, 131), // #1293
  INST(Vpmovdb          , VexMr_Lx           , E(F30F38,31,_,x,_,0,2,QVM), 0                         , 206, 0  , 8293 , 364, 129), // #1294
  INST(Vpmovdw          , VexMr_Lx           , E(F30F38,33,_,x,_,0,3,HVM), 0                         , 207, 0  , 8301 , 365, 129), // #1295
  INST(Vpmovm2b         , VexRm_Lx           , E(F30F38,28,_,x,_,0,_,_  ), 0                         , 193, 0  , 8309 , 332, 137), // #1296
  INST(Vpmovm2d         , VexRm_Lx           , E(F30F38,38,_,x,_,0,_,_  ), 0                         , 193, 0  , 8318 , 332, 131), // #1297
  INST(Vpmovm2q         , VexRm_Lx           , E(F30F38,38,_,x,_,1,_,_  ), 0                         , 192, 0  , 8327 , 332, 131), // #1298
  INST(Vpmovm2w         , VexRm_Lx           , E(F30F38,28,_,x,_,1,_,_  ), 0                         , 192, 0  , 8336 , 332, 137), // #1299
  INST(Vpmovmskb        , VexRm_Lx           , V(660F00,D7,_,x,I,_,_,_  ), 0                         , 70 , 0  , 8345 , 312, 146), // #1300
  INST(Vpmovq2m         , VexRm_Lx           , E(F30F38,39,_,x,_,1,_,_  ), 0                         , 192, 0  , 8355 , 363, 131), // #1301
  INST(Vpmovqb          , VexMr_Lx           , E(F30F38,32,_,x,_,0,1,OVM), 0                         , 208, 0  , 8364 , 366, 129), // #1302
  INST(Vpmovqd          , VexMr_Lx           , E(F30F38,35,_,x,_,0,3,HVM), 0                         , 207, 0  , 8372 , 365, 129), // #1303
  INST(Vpmovqw          , VexMr_Lx           , E(F30F38,34,_,x,_,0,2,QVM), 0                         , 206, 0  , 8380 , 364, 129), // #1304
  INST(Vpmovsdb         , VexMr_Lx           , E(F30F38,21,_,x,_,0,2,QVM), 0                         , 206, 0  , 8388 , 364, 129), // #1305
  INST(Vpmovsdw         , VexMr_Lx           , E(F30F38,23,_,x,_,0,3,HVM), 0                         , 207, 0  , 8397 , 365, 129), // #1306
  INST(Vpmovsqb         , VexMr_Lx           , E(F30F38,22,_,x,_,0,1,OVM), 0                         , 208, 0  , 8406 , 366, 129), // #1307
  INST(Vpmovsqd         , VexMr_Lx           , E(F30F38,25,_,x,_,0,3,HVM), 0                         , 207, 0  , 8415 , 365, 129), // #1308
  INST(Vpmovsqw         , VexMr_Lx           , E(F30F38,24,_,x,_,0,2,QVM), 0                         , 206, 0  , 8424 , 364, 129), // #1309
  INST(Vpmovswb         , VexMr_Lx           , E(F30F38,20,_,x,_,0,3,HVM), 0                         , 207, 0  , 8433 , 365, 137), // #1310
  INST(Vpmovsxbd        , VexRm_Lx           , V(660F38,21,_,x,I,I,2,QVM), 0                         , 209, 0  , 8442 , 367, 133), // #1311
  INST(Vpmovsxbq        , VexRm_Lx           , V(660F38,22,_,x,I,I,1,OVM), 0                         , 210, 0  , 8452 , 368, 133), // #1312
  INST(Vpmovsxbw        , VexRm_Lx           , V(660F38,20,_,x,I,I,3,HVM), 0                         , 132, 0  , 8462 , 369, 149), // #1313
  INST(Vpmovsxdq        , VexRm_Lx           , V(660F38,25,_,x,I,0,3,HVM), 0                         , 132, 0  , 8472 , 369, 133), // #1314
  INST(Vpmovsxwd        , VexRm_Lx           , V(660F38,23,_,x,I,I,3,HVM), 0                         , 132, 0  , 8482 , 369, 133), // #1315
  INST(Vpmovsxwq        , VexRm_Lx           , V(660F38,24,_,x,I,I,2,QVM), 0                         , 209, 0  , 8492 , 367, 133), // #1316
  INST(Vpmovusdb        , VexMr_Lx           , E(F30F38,11,_,x,_,0,2,QVM), 0                         , 206, 0  , 8502 , 364, 129), // #1317
  INST(Vpmovusdw        , VexMr_Lx           , E(F30F38,13,_,x,_,0,3,HVM), 0                         , 207, 0  , 8512 , 365, 129), // #1318
  INST(Vpmovusqb        , VexMr_Lx           , E(F30F38,12,_,x,_,0,1,OVM), 0                         , 208, 0  , 8522 , 366, 129), // #1319
  INST(Vpmovusqd        , VexMr_Lx           , E(F30F38,15,_,x,_,0,3,HVM), 0                         , 207, 0  , 8532 , 365, 129), // #1320
  INST(Vpmovusqw        , VexMr_Lx           , E(F30F38,14,_,x,_,0,2,QVM), 0                         , 206, 0  , 8542 , 364, 129), // #1321
  INST(Vpmovuswb        , VexMr_Lx           , E(F30F38,10,_,x,_,0,3,HVM), 0                         , 207, 0  , 8552 , 365, 137), // #1322
  INST(Vpmovw2m         , VexRm_Lx           , E(F30F38,29,_,x,_,1,_,_  ), 0                         , 192, 0  , 8562 , 363, 137), // #1323
  INST(Vpmovwb          , VexMr_Lx           , E(F30F38,30,_,x,_,0,3,HVM), 0                         , 207, 0  , 8571 , 365, 137), // #1324
  INST(Vpmovzxbd        , VexRm_Lx           , V(660F38,31,_,x,I,I,2,QVM), 0                         , 209, 0  , 8579 , 367, 133), // #1325
  INST(Vpmovzxbq        , VexRm_Lx           , V(660F38,32,_,x,I,I,1,OVM), 0                         , 210, 0  , 8589 , 368, 133), // #1326
  INST(Vpmovzxbw        , VexRm_Lx           , V(660F38,30,_,x,I,I,3,HVM), 0                         , 132, 0  , 8599 , 369, 149), // #1327
  INST(Vpmovzxdq        , VexRm_Lx           , V(660F38,35,_,x,I,0,3,HVM), 0                         , 132, 0  , 8609 , 369, 133), // #1328
  INST(Vpmovzxwd        , VexRm_Lx           , V(660F38,33,_,x,I,I,3,HVM), 0                         , 132, 0  , 8619 , 369, 133), // #1329
  INST(Vpmovzxwq        , VexRm_Lx           , V(660F38,34,_,x,I,I,2,QVM), 0                         , 209, 0  , 8629 , 367, 133), // #1330
  INST(Vpmuldq          , VexRvm_Lx          , V(660F38,28,_,x,I,1,4,FV ), 0                         , 200, 0  , 8639 , 206, 133), // #1331
  INST(Vpmulhrsw        , VexRvm_Lx          , V(660F38,0B,_,x,I,I,4,FVM), 0                         , 109, 0  , 8647 , 292, 149), // #1332
  INST(Vpmulhuw         , VexRvm_Lx          , V(660F00,E4,_,x,I,I,4,FVM), 0                         , 184, 0  , 8657 , 292, 149), // #1333
  INST(Vpmulhw          , VexRvm_Lx          , V(660F00,E5,_,x,I,I,4,FVM), 0                         , 184, 0  , 8666 , 292, 149), // #1334
  INST(Vpmulld          , VexRvm_Lx          , V(660F38,40,_,x,I,0,4,FV ), 0                         , 163, 0  , 8674 , 207, 133), // #1335
  INST(Vpmullq          , VexRvm_Lx          , E(660F38,40,_,x,_,1,4,FV ), 0                         , 112, 0  , 8682 , 210, 131), // #1336
  INST(Vpmullw          , VexRvm_Lx          , V(660F00,D5,_,x,I,I,4,FVM), 0                         , 184, 0  , 8690 , 292, 149), // #1337
  INST(Vpmultishiftqb   , VexRvm_Lx          , E(660F38,83,_,x,_,1,4,FV ), 0                         , 112, 0  , 8698 , 210, 156), // #1338
  INST(Vpmuludq         , VexRvm_Lx          , V(660F00,F4,_,x,I,1,4,FV ), 0                         , 104, 0  , 8713 , 206, 133), // #1339
  INST(Vpopcntb         , VexRm_Lx           , E(660F38,54,_,x,_,0,4,FV ), 0                         , 113, 0  , 8722 , 261, 160), // #1340
  INST(Vpopcntd         , VexRm_Lx           , E(660F38,55,_,x,_,0,4,FVM), 0                         , 189, 0  , 8731 , 346, 161), // #1341
  INST(Vpopcntq         , VexRm_Lx           , E(660F38,55,_,x,_,1,4,FVM), 0                         , 190, 0  , 8740 , 360, 161), // #1342
  INST(Vpopcntw         , VexRm_Lx           , E(660F38,54,_,x,_,1,4,FV ), 0                         , 112, 0  , 8749 , 261, 160), // #1343
  INST(Vpor             , VexRvm_Lx          , V(660F00,EB,_,x,I,_,_,_  ), 0                         , 70 , 0  , 8758 , 323, 146), // #1344
  INST(Vpord            , VexRvm_Lx          , E(660F00,EB,_,x,_,0,4,FV ), 0                         , 188, 0  , 8763 , 324, 129), // #1345
  INST(Vporq            , VexRvm_Lx          , E(660F00,EB,_,x,_,1,4,FV ), 0                         , 130, 0  , 8769 , 328, 129), // #1346
  INST(Vpperm           , VexRvrmRvmr        , V(XOP_M8,A3,_,0,x,_,_,_  ), 0                         , 195, 0  , 8775 , 370, 142), // #1347
  INST(Vprold           , VexVmi_Lx          , E(660F00,72,1,x,_,0,4,FV ), 0                         , 211, 0  , 8782 , 371, 129), // #1348
  INST(Vprolq           , VexVmi_Lx          , E(660F00,72,1,x,_,1,4,FV ), 0                         , 212, 0  , 8789 , 372, 129), // #1349
  INST(Vprolvd          , VexRvm_Lx          , E(660F38,15,_,x,_,0,4,FV ), 0                         , 113, 0  , 8796 , 211, 129), // #1350
  INST(Vprolvq          , VexRvm_Lx          , E(660F38,15,_,x,_,1,4,FV ), 0                         , 112, 0  , 8804 , 210, 129), // #1351
  INST(Vprord           , VexVmi_Lx          , E(660F00,72,0,x,_,0,4,FV ), 0                         , 188, 0  , 8812 , 371, 129), // #1352
  INST(Vprorq           , VexVmi_Lx          , E(660F00,72,0,x,_,1,4,FV ), 0                         , 130, 0  , 8819 , 372, 129), // #1353
  INST(Vprorvd          , VexRvm_Lx          , E(660F38,14,_,x,_,0,4,FV ), 0                         , 113, 0  , 8826 , 211, 129), // #1354
  INST(Vprorvq          , VexRvm_Lx          , E(660F38,14,_,x,_,1,4,FV ), 0                         , 112, 0  , 8834 , 210, 129), // #1355
  INST(Vprotb           , VexRvmRmvRmi       , V(XOP_M9,90,_,0,x,_,_,_  ), V(XOP_M8,C0,_,0,x,_,_,_  ), 80 , 121, 8842 , 373, 142), // #1356
  INST(Vprotd           , VexRvmRmvRmi       , V(XOP_M9,92,_,0,x,_,_,_  ), V(XOP_M8,C2,_,0,x,_,_,_  ), 80 , 122, 8849 , 373, 142), // #1357
  INST(Vprotq           , VexRvmRmvRmi       , V(XOP_M9,93,_,0,x,_,_,_  ), V(XOP_M8,C3,_,0,x,_,_,_  ), 80 , 123, 8856 , 373, 142), // #1358
  INST(Vprotw           , VexRvmRmvRmi       , V(XOP_M9,91,_,0,x,_,_,_  ), V(XOP_M8,C1,_,0,x,_,_,_  ), 80 , 124, 8863 , 373, 142), // #1359
  INST(Vpsadbw          , VexRvm_Lx          , V(660F00,F6,_,x,I,I,4,FVM), 0                         , 184, 0  , 8870 , 201, 149), // #1360
  INST(Vpscatterdd      , VexMr_VM           , E(660F38,A0,_,x,_,0,2,T1S), 0                         , 125, 0  , 8878 , 374, 129), // #1361
  INST(Vpscatterdq      , VexMr_VM           , E(660F38,A0,_,x,_,1,3,T1S), 0                         , 124, 0  , 8890 , 375, 129), // #1362
  INST(Vpscatterqd      , VexMr_VM           , E(660F38,A1,_,x,_,0,2,T1S), 0                         , 125, 0  , 8902 , 376, 129), // #1363
  INST(Vpscatterqq      , VexMr_VM           , E(660F38,A1,_,x,_,1,3,T1S), 0                         , 124, 0  , 8914 , 377, 129), // #1364
  INST(Vpshab           , VexRvmRmv          , V(XOP_M9,98,_,0,x,_,_,_  ), 0                         , 80 , 0  , 8926 , 378, 142), // #1365
  INST(Vpshad           , VexRvmRmv          , V(XOP_M9,9A,_,0,x,_,_,_  ), 0                         , 80 , 0  , 8933 , 378, 142), // #1366
  INST(Vpshaq           , VexRvmRmv          , V(XOP_M9,9B,_,0,x,_,_,_  ), 0                         , 80 , 0  , 8940 , 378, 142), // #1367
  INST(Vpshaw           , VexRvmRmv          , V(XOP_M9,99,_,0,x,_,_,_  ), 0                         , 80 , 0  , 8947 , 378, 142), // #1368
  INST(Vpshlb           , VexRvmRmv          , V(XOP_M9,94,_,0,x,_,_,_  ), 0                         , 80 , 0  , 8954 , 378, 142), // #1369
  INST(Vpshld           , VexRvmRmv          , V(XOP_M9,96,_,0,x,_,_,_  ), 0                         , 80 , 0  , 8961 , 378, 142), // #1370
  INST(Vpshldd          , VexRvmi_Lx         , E(660F3A,71,_,x,_,0,4,FV ), 0                         , 110, 0  , 8968 , 204, 154), // #1371
  INST(Vpshldq          , VexRvmi_Lx         , E(660F3A,71,_,x,_,1,4,FV ), 0                         , 111, 0  , 8976 , 205, 154), // #1372
  INST(Vpshldvd         , VexRvm_Lx          , E(660F38,71,_,x,_,0,4,FV ), 0                         , 113, 0  , 8984 , 211, 154), // #1373
  INST(Vpshldvq         , VexRvm_Lx          , E(660F38,71,_,x,_,1,4,FV ), 0                         , 112, 0  , 8993 , 210, 154), // #1374
  INST(Vpshldvw         , VexRvm_Lx          , E(660F38,70,_,x,_,1,4,FVM), 0                         , 190, 0  , 9002 , 329, 154), // #1375
  INST(Vpshldw          , VexRvmi_Lx         , E(660F3A,70,_,x,_,1,4,FVM), 0                         , 197, 0  , 9011 , 257, 154), // #1376
  INST(Vpshlq           , VexRvmRmv          , V(XOP_M9,97,_,0,x,_,_,_  ), 0                         , 80 , 0  , 9019 , 378, 142), // #1377
  INST(Vpshlw           , VexRvmRmv          , V(XOP_M9,95,_,0,x,_,_,_  ), 0                         , 80 , 0  , 9026 , 378, 142), // #1378
  INST(Vpshrdd          , VexRvmi_Lx         , E(660F3A,73,_,x,_,0,4,FV ), 0                         , 110, 0  , 9033 , 204, 154), // #1379
  INST(Vpshrdq          , VexRvmi_Lx         , E(660F3A,73,_,x,_,1,4,FV ), 0                         , 111, 0  , 9041 , 205, 154), // #1380
  INST(Vpshrdvd         , VexRvm_Lx          , E(660F38,73,_,x,_,0,4,FV ), 0                         , 113, 0  , 9049 , 211, 154), // #1381
  INST(Vpshrdvq         , VexRvm_Lx          , E(660F38,73,_,x,_,1,4,FV ), 0                         , 112, 0  , 9058 , 210, 154), // #1382
  INST(Vpshrdvw         , VexRvm_Lx          , E(660F38,72,_,x,_,1,4,FVM), 0                         , 190, 0  , 9067 , 329, 154), // #1383
  INST(Vpshrdw          , VexRvmi_Lx         , E(660F3A,72,_,x,_,1,4,FVM), 0                         , 197, 0  , 9076 , 257, 154), // #1384
  INST(Vpshufb          , VexRvm_Lx          , V(660F38,00,_,x,I,I,4,FVM), 0                         , 109, 0  , 9084 , 292, 149), // #1385
  INST(Vpshufbitqmb     , VexRvm_Lx          , E(660F38,8F,_,x,0,0,4,FVM), 0                         , 189, 0  , 9092 , 379, 160), // #1386
  INST(Vpshufd          , VexRmi_Lx          , V(660F00,70,_,x,I,0,4,FV ), 0                         , 133, 0  , 9105 , 380, 133), // #1387
  INST(Vpshufhw         , VexRmi_Lx          , V(F30F00,70,_,x,I,I,4,FVM), 0                         , 185, 0  , 9113 , 381, 149), // #1388
  INST(Vpshuflw         , VexRmi_Lx          , V(F20F00,70,_,x,I,I,4,FVM), 0                         , 213, 0  , 9122 , 381, 149), // #1389
  INST(Vpsignb          , VexRvm_Lx          , V(660F38,08,_,x,I,_,_,_  ), 0                         , 97 , 0  , 9131 , 200, 146), // #1390
  INST(Vpsignd          , VexRvm_Lx          , V(660F38,0A,_,x,I,_,_,_  ), 0                         , 97 , 0  , 9139 , 200, 146), // #1391
  INST(Vpsignw          , VexRvm_Lx          , V(660F38,09,_,x,I,_,_,_  ), 0                         , 97 , 0  , 9147 , 200, 146), // #1392
  INST(Vpslld           , VexRvmVmi_Lx_MEvex , V(660F00,F2,_,x,I,0,4,128), V(660F00,72,6,x,I,0,4,FV ), 214, 125, 9155 , 382, 133), // #1393
  INST(Vpslldq          , VexVmi_Lx_MEvex    , V(660F00,73,7,x,I,I,4,FVM), 0                         , 215, 0  , 9162 , 383, 149), // #1394
  INST(Vpsllq           , VexRvmVmi_Lx_MEvex , V(660F00,F3,_,x,I,1,4,128), V(660F00,73,6,x,I,1,4,FV ), 216, 126, 9170 , 384, 133), // #1395
  INST(Vpsllvd          , VexRvm_Lx          , V(660F38,47,_,x,0,0,4,FV ), 0                         , 163, 0  , 9177 , 207, 143), // #1396
  INST(Vpsllvq          , VexRvm_Lx          , V(660F38,47,_,x,1,1,4,FV ), 0                         , 162, 0  , 9185 , 206, 143), // #1397
  INST(Vpsllvw          , VexRvm_Lx          , E(660F38,12,_,x,_,1,4,FVM), 0                         , 190, 0  , 9193 , 329, 137), // #1398
  INST(Vpsllw           , VexRvmVmi_Lx_MEvex , V(660F00,F1,_,x,I,I,4,128), V(660F00,71,6,x,I,I,4,FVM), 214, 127, 9201 , 385, 149), // #1399
  INST(Vpsrad           , VexRvmVmi_Lx_MEvex , V(660F00,E2,_,x,I,0,4,128), V(660F00,72,4,x,I,0,4,FV ), 214, 128, 9208 , 382, 133), // #1400
  INST(Vpsraq           , VexRvmVmi_Lx_MEvex , E(660F00,E2,_,x,_,1,4,128), E(660F00,72,4,x,_,1,4,FV ), 217, 129, 9215 , 386, 129), // #1401
  INST(Vpsravd          , VexRvm_Lx          , V(660F38,46,_,x,0,0,4,FV ), 0                         , 163, 0  , 9222 , 207, 143), // #1402
  INST(Vpsravq          , VexRvm_Lx          , E(660F38,46,_,x,_,1,4,FV ), 0                         , 112, 0  , 9230 , 210, 129), // #1403
  INST(Vpsravw          , VexRvm_Lx          , E(660F38,11,_,x,_,1,4,FVM), 0                         , 190, 0  , 9238 , 329, 137), // #1404
  INST(Vpsraw           , VexRvmVmi_Lx_MEvex , V(660F00,E1,_,x,I,I,4,128), V(660F00,71,4,x,I,I,4,FVM), 214, 130, 9246 , 385, 149), // #1405
  INST(Vpsrld           , VexRvmVmi_Lx_MEvex , V(660F00,D2,_,x,I,0,4,128), V(660F00,72,2,x,I,0,4,FV ), 214, 131, 9253 , 382, 133), // #1406
  INST(Vpsrldq          , VexVmi_Lx_MEvex    , V(660F00,73,3,x,I,I,4,FVM), 0                         , 218, 0  , 9260 , 383, 149), // #1407
  INST(Vpsrlq           , VexRvmVmi_Lx_MEvex , V(660F00,D3,_,x,I,1,4,128), V(660F00,73,2,x,I,1,4,FV ), 216, 132, 9268 , 384, 133), // #1408
  INST(Vpsrlvd          , VexRvm_Lx          , V(660F38,45,_,x,0,0,4,FV ), 0                         , 163, 0  , 9275 , 207, 143), // #1409
  INST(Vpsrlvq          , VexRvm_Lx          , V(660F38,45,_,x,1,1,4,FV ), 0                         , 162, 0  , 9283 , 206, 143), // #1410
  INST(Vpsrlvw          , VexRvm_Lx          , E(660F38,10,_,x,_,1,4,FVM), 0                         , 190, 0  , 9291 , 329, 137), // #1411
  INST(Vpsrlw           , VexRvmVmi_Lx_MEvex , V(660F00,D1,_,x,I,I,4,128), V(660F00,71,2,x,I,I,4,FVM), 214, 133, 9299 , 385, 149), // #1412
  INST(Vpsubb           , VexRvm_Lx          , V(660F00,F8,_,x,I,I,4,FVM), 0                         , 184, 0  , 9306 , 387, 149), // #1413
  INST(Vpsubd           , VexRvm_Lx          , V(660F00,FA,_,x,I,0,4,FV ), 0                         , 133, 0  , 9313 , 388, 133), // #1414
  INST(Vpsubq           , VexRvm_Lx          , V(660F00,FB,_,x,I,1,4,FV ), 0                         , 104, 0  , 9320 , 389, 133), // #1415
  INST(Vpsubsb          , VexRvm_Lx          , V(660F00,E8,_,x,I,I,4,FVM), 0                         , 184, 0  , 9327 , 387, 149), // #1416
  INST(Vpsubsw          , VexRvm_Lx          , V(660F00,E9,_,x,I,I,4,FVM), 0                         , 184, 0  , 9335 , 387, 149), // #1417
  INST(Vpsubusb         , VexRvm_Lx          , V(660F00,D8,_,x,I,I,4,FVM), 0                         , 184, 0  , 9343 , 387, 149), // #1418
  INST(Vpsubusw         , VexRvm_Lx          , V(660F00,D9,_,x,I,I,4,FVM), 0                         , 184, 0  , 9352 , 387, 149), // #1419
  INST(Vpsubw           , VexRvm_Lx          , V(660F00,F9,_,x,I,I,4,FVM), 0                         , 184, 0  , 9361 , 387, 149), // #1420
  INST(Vpternlogd       , VexRvmi_Lx         , E(660F3A,25,_,x,_,0,4,FV ), 0                         , 110, 0  , 9368 , 204, 129), // #1421
  INST(Vpternlogq       , VexRvmi_Lx         , E(660F3A,25,_,x,_,1,4,FV ), 0                         , 111, 0  , 9379 , 205, 129), // #1422
  INST(Vptest           , VexRm_Lx           , V(660F38,17,_,x,I,_,_,_  ), 0                         , 97 , 0  , 9390 , 277, 153), // #1423
  INST(Vptestmb         , VexRvm_Lx          , E(660F38,26,_,x,_,0,4,FVM), 0                         , 189, 0  , 9397 , 379, 137), // #1424
  INST(Vptestmd         , VexRvm_Lx          , E(660F38,27,_,x,_,0,4,FV ), 0                         , 113, 0  , 9406 , 390, 129), // #1425
  INST(Vptestmq         , VexRvm_Lx          , E(660F38,27,_,x,_,1,4,FV ), 0                         , 112, 0  , 9415 , 391, 129), // #1426
  INST(Vptestmw         , VexRvm_Lx          , E(660F38,26,_,x,_,1,4,FVM), 0                         , 190, 0  , 9424 , 379, 137), // #1427
  INST(Vptestnmb        , VexRvm_Lx          , E(F30F38,26,_,x,_,0,4,FVM), 0                         , 219, 0  , 9433 , 379, 137), // #1428
  INST(Vptestnmd        , VexRvm_Lx          , E(F30F38,27,_,x,_,0,4,FV ), 0                         , 128, 0  , 9443 , 390, 129), // #1429
  INST(Vptestnmq        , VexRvm_Lx          , E(F30F38,27,_,x,_,1,4,FV ), 0                         , 220, 0  , 9453 , 391, 129), // #1430
  INST(Vptestnmw        , VexRvm_Lx          , E(F30F38,26,_,x,_,1,4,FVM), 0                         , 221, 0  , 9463 , 379, 137), // #1431
  INST(Vpunpckhbw       , VexRvm_Lx          , V(660F00,68,_,x,I,I,4,FVM), 0                         , 184, 0  , 9473 , 292, 149), // #1432
  INST(Vpunpckhdq       , VexRvm_Lx          , V(660F00,6A,_,x,I,0,4,FV ), 0                         , 133, 0  , 9484 , 207, 133), // #1433
  INST(Vpunpckhqdq      , VexRvm_Lx          , V(660F00,6D,_,x,I,1,4,FV ), 0                         , 104, 0  , 9495 , 206, 133), // #1434
  INST(Vpunpckhwd       , VexRvm_Lx          , V(660F00,69,_,x,I,I,4,FVM), 0                         , 184, 0  , 9507 , 292, 149), // #1435
  INST(Vpunpcklbw       , VexRvm_Lx          , V(660F00,60,_,x,I,I,4,FVM), 0                         , 184, 0  , 9518 , 292, 149), // #1436
  INST(Vpunpckldq       , VexRvm_Lx          , V(660F00,62,_,x,I,0,4,FV ), 0                         , 133, 0  , 9529 , 207, 133), // #1437
  INST(Vpunpcklqdq      , VexRvm_Lx          , V(660F00,6C,_,x,I,1,4,FV ), 0                         , 104, 0  , 9540 , 206, 133), // #1438
  INST(Vpunpcklwd       , VexRvm_Lx          , V(660F00,61,_,x,I,I,4,FVM), 0                         , 184, 0  , 9552 , 292, 149), // #1439
  INST(Vpxor            , VexRvm_Lx          , V(660F00,EF,_,x,I,_,_,_  ), 0                         , 70 , 0  , 9563 , 325, 146), // #1440
  INST(Vpxord           , VexRvm_Lx          , E(660F00,EF,_,x,_,0,4,FV ), 0                         , 188, 0  , 9569 , 326, 129), // #1441
  INST(Vpxorq           , VexRvm_Lx          , E(660F00,EF,_,x,_,1,4,FV ), 0                         , 130, 0  , 9576 , 327, 129), // #1442
  INST(Vrangepd         , VexRvmi_Lx         , E(660F3A,50,_,x,_,1,4,FV ), 0                         , 111, 0  , 9583 , 266, 131), // #1443
  INST(Vrangeps         , VexRvmi_Lx         , E(660F3A,50,_,x,_,0,4,FV ), 0                         , 110, 0  , 9592 , 267, 131), // #1444
  INST(Vrangesd         , VexRvmi            , E(660F3A,51,_,I,_,1,3,T1S), 0                         , 160, 0  , 9601 , 268, 66 ), // #1445
  INST(Vrangess         , VexRvmi            , E(660F3A,51,_,I,_,0,2,T1S), 0                         , 161, 0  , 9610 , 269, 66 ), // #1446
  INST(Vrcp14pd         , VexRm_Lx           , E(660F38,4C,_,x,_,1,4,FV ), 0                         , 112, 0  , 9619 , 360, 129), // #1447
  INST(Vrcp14ps         , VexRm_Lx           , E(660F38,4C,_,x,_,0,4,FV ), 0                         , 113, 0  , 9628 , 346, 129), // #1448
  INST(Vrcp14sd         , VexRvm             , E(660F38,4D,_,I,_,1,3,T1S), 0                         , 124, 0  , 9637 , 392, 68 ), // #1449
  INST(Vrcp14ss         , VexRvm             , E(660F38,4D,_,I,_,0,2,T1S), 0                         , 125, 0  , 9646 , 393, 68 ), // #1450
  INST(Vrcp28pd         , VexRm              , E(660F38,CA,_,2,_,1,4,FV ), 0                         , 152, 0  , 9655 , 259, 138), // #1451
  INST(Vrcp28ps         , VexRm              , E(660F38,CA,_,2,_,0,4,FV ), 0                         , 153, 0  , 9664 , 260, 138), // #1452
  INST(Vrcp28sd         , VexRvm             , E(660F38,CB,_,I,_,1,3,T1S), 0                         , 124, 0  , 9673 , 287, 138), // #1453
  INST(Vrcp28ss         , VexRvm             , E(660F38,CB,_,I,_,0,2,T1S), 0                         , 125, 0  , 9682 , 288, 138), // #1454
  INST(Vrcpps           , VexRm_Lx           , V(000F00,53,_,x,I,_,_,_  ), 0                         , 73 , 0  , 9691 , 277, 126), // #1455
  INST(Vrcpss           , VexRvm             , V(F30F00,53,_,I,I,_,_,_  ), 0                         , 178, 0  , 9698 , 394, 126), // #1456
  INST(Vreducepd        , VexRmi_Lx          , E(660F3A,56,_,x,_,1,4,FV ), 0                         , 111, 0  , 9705 , 372, 131), // #1457
  INST(Vreduceps        , VexRmi_Lx          , E(660F3A,56,_,x,_,0,4,FV ), 0                         , 110, 0  , 9715 , 371, 131), // #1458
  INST(Vreducesd        , VexRvmi            , E(660F3A,57,_,I,_,1,3,T1S), 0                         , 160, 0  , 9725 , 395, 66 ), // #1459
  INST(Vreducess        , VexRvmi            , E(660F3A,57,_,I,_,0,2,T1S), 0                         , 161, 0  , 9735 , 396, 66 ), // #1460
  INST(Vrndscalepd      , VexRmi_Lx          , E(660F3A,09,_,x,_,1,4,FV ), 0                         , 111, 0  , 9745 , 289, 129), // #1461
  INST(Vrndscaleps      , VexRmi_Lx          , E(660F3A,08,_,x,_,0,4,FV ), 0                         , 110, 0  , 9757 , 290, 129), // #1462
  INST(Vrndscalesd      , VexRvmi            , E(660F3A,0B,_,I,_,1,3,T1S), 0                         , 160, 0  , 9769 , 268, 68 ), // #1463
  INST(Vrndscaless      , VexRvmi            , E(660F3A,0A,_,I,_,0,2,T1S), 0                         , 161, 0  , 9781 , 269, 68 ), // #1464
  INST(Vroundpd         , VexRmi_Lx          , V(660F3A,09,_,x,I,_,_,_  ), 0                         , 74 , 0  , 9793 , 397, 126), // #1465
  INST(Vroundps         , VexRmi_Lx          , V(660F3A,08,_,x,I,_,_,_  ), 0                         , 74 , 0  , 9802 , 397, 126), // #1466
  INST(Vroundsd         , VexRvmi            , V(660F3A,0B,_,I,I,_,_,_  ), 0                         , 74 , 0  , 9811 , 398, 126), // #1467
  INST(Vroundss         , VexRvmi            , V(660F3A,0A,_,I,I,_,_,_  ), 0                         , 74 , 0  , 9820 , 399, 126), // #1468
  INST(Vrsqrt14pd       , VexRm_Lx           , E(660F38,4E,_,x,_,1,4,FV ), 0                         , 112, 0  , 9829 , 360, 129), // #1469
  INST(Vrsqrt14ps       , VexRm_Lx           , E(660F38,4E,_,x,_,0,4,FV ), 0                         , 113, 0  , 9840 , 346, 129), // #1470
  INST(Vrsqrt14sd       , VexRvm             , E(660F38,4F,_,I,_,1,3,T1S), 0                         , 124, 0  , 9851 , 392, 68 ), // #1471
  INST(Vrsqrt14ss       , VexRvm             , E(660F38,4F,_,I,_,0,2,T1S), 0                         , 125, 0  , 9862 , 393, 68 ), // #1472
  INST(Vrsqrt28pd       , VexRm              , E(660F38,CC,_,2,_,1,4,FV ), 0                         , 152, 0  , 9873 , 259, 138), // #1473
  INST(Vrsqrt28ps       , VexRm              , E(660F38,CC,_,2,_,0,4,FV ), 0                         , 153, 0  , 9884 , 260, 138), // #1474
  INST(Vrsqrt28sd       , VexRvm             , E(660F38,CD,_,I,_,1,3,T1S), 0                         , 124, 0  , 9895 , 287, 138), // #1475
  INST(Vrsqrt28ss       , VexRvm             , E(660F38,CD,_,I,_,0,2,T1S), 0                         , 125, 0  , 9906 , 288, 138), // #1476
  INST(Vrsqrtps         , VexRm_Lx           , V(000F00,52,_,x,I,_,_,_  ), 0                         , 73 , 0  , 9917 , 277, 126), // #1477
  INST(Vrsqrtss         , VexRvm             , V(F30F00,52,_,I,I,_,_,_  ), 0                         , 178, 0  , 9926 , 394, 126), // #1478
  INST(Vscalefpd        , VexRvm_Lx          , E(660F38,2C,_,x,_,1,4,FV ), 0                         , 112, 0  , 9935 , 400, 129), // #1479
  INST(Vscalefps        , VexRvm_Lx          , E(660F38,2C,_,x,_,0,4,FV ), 0                         , 113, 0  , 9945 , 401, 129), // #1480
  INST(Vscalefsd        , VexRvm             , E(660F38,2D,_,I,_,1,3,T1S), 0                         , 124, 0  , 9955 , 402, 68 ), // #1481
  INST(Vscalefss        , VexRvm             , E(660F38,2D,_,I,_,0,2,T1S), 0                         , 125, 0  , 9965 , 403, 68 ), // #1482
  INST(Vscatterdpd      , VexMr_VM           , E(660F38,A2,_,x,_,1,3,T1S), 0                         , 124, 0  , 9975 , 375, 129), // #1483
  INST(Vscatterdps      , VexMr_VM           , E(660F38,A2,_,x,_,0,2,T1S), 0                         , 125, 0  , 9987 , 374, 129), // #1484
  INST(Vscatterpf0dpd   , VexM_VM            , E(660F38,C6,5,2,_,1,3,T1S), 0                         , 222, 0  , 9999 , 282, 144), // #1485
  INST(Vscatterpf0dps   , VexM_VM            , E(660F38,C6,5,2,_,0,2,T1S), 0                         , 223, 0  , 10014, 283, 144), // #1486
  INST(Vscatterpf0qpd   , VexM_VM            , E(660F38,C7,5,2,_,1,3,T1S), 0                         , 222, 0  , 10029, 284, 144), // #1487
  INST(Vscatterpf0qps   , VexM_VM            , E(660F38,C7,5,2,_,0,2,T1S), 0                         , 223, 0  , 10044, 284, 144), // #1488
  INST(Vscatterpf1dpd   , VexM_VM            , E(660F38,C6,6,2,_,1,3,T1S), 0                         , 224, 0  , 10059, 282, 144), // #1489
  INST(Vscatterpf1dps   , VexM_VM            , E(660F38,C6,6,2,_,0,2,T1S), 0                         , 225, 0  , 10074, 283, 144), // #1490
  INST(Vscatterpf1qpd   , VexM_VM            , E(660F38,C7,6,2,_,1,3,T1S), 0                         , 224, 0  , 10089, 284, 144), // #1491
  INST(Vscatterpf1qps   , VexM_VM            , E(660F38,C7,6,2,_,0,2,T1S), 0                         , 225, 0  , 10104, 284, 144), // #1492
  INST(Vscatterqpd      , VexMr_VM           , E(660F38,A3,_,x,_,1,3,T1S), 0                         , 124, 0  , 10119, 377, 129), // #1493
  INST(Vscatterqps      , VexMr_VM           , E(660F38,A3,_,x,_,0,2,T1S), 0                         , 125, 0  , 10131, 376, 129), // #1494
  INST(Vshuff32x4       , VexRvmi_Lx         , E(660F3A,23,_,x,_,0,4,FV ), 0                         , 110, 0  , 10143, 404, 129), // #1495
  INST(Vshuff64x2       , VexRvmi_Lx         , E(660F3A,23,_,x,_,1,4,FV ), 0                         , 111, 0  , 10154, 405, 129), // #1496
  INST(Vshufi32x4       , VexRvmi_Lx         , E(660F3A,43,_,x,_,0,4,FV ), 0                         , 110, 0  , 10165, 404, 129), // #1497
  INST(Vshufi64x2       , VexRvmi_Lx         , E(660F3A,43,_,x,_,1,4,FV ), 0                         , 111, 0  , 10176, 405, 129), // #1498
  INST(Vshufpd          , VexRvmi_Lx         , V(660F00,C6,_,x,I,1,4,FV ), 0                         , 104, 0  , 10187, 406, 124), // #1499
  INST(Vshufps          , VexRvmi_Lx         , V(000F00,C6,_,x,I,0,4,FV ), 0                         , 105, 0  , 10195, 407, 124), // #1500
  INST(Vsqrtpd          , VexRm_Lx           , V(660F00,51,_,x,I,1,4,FV ), 0                         , 104, 0  , 10203, 408, 124), // #1501
  INST(Vsqrtps          , VexRm_Lx           , V(000F00,51,_,x,I,0,4,FV ), 0                         , 105, 0  , 10211, 229, 124), // #1502
  INST(Vsqrtsd          , VexRvm             , V(F20F00,51,_,I,I,1,3,T1S), 0                         , 106, 0  , 10219, 198, 125), // #1503
  INST(Vsqrtss          , VexRvm             , V(F30F00,51,_,I,I,0,2,T1S), 0                         , 107, 0  , 10227, 199, 125), // #1504
  INST(Vstmxcsr         , VexM               , V(000F00,AE,3,0,I,_,_,_  ), 0                         , 226, 0  , 10235, 298, 126), // #1505
  INST(Vsubpd           , VexRvm_Lx          , V(660F00,5C,_,x,I,1,4,FV ), 0                         , 104, 0  , 10244, 196, 124), // #1506
  INST(Vsubps           , VexRvm_Lx          , V(000F00,5C,_,x,I,0,4,FV ), 0                         , 105, 0  , 10251, 197, 124), // #1507
  INST(Vsubsd           , VexRvm             , V(F20F00,5C,_,I,I,1,3,T1S), 0                         , 106, 0  , 10258, 198, 125), // #1508
  INST(Vsubss           , VexRvm             , V(F30F00,5C,_,I,I,0,2,T1S), 0                         , 107, 0  , 10265, 199, 125), // #1509
  INST(Vtestpd          , VexRm_Lx           , V(660F38,0F,_,x,0,_,_,_  ), 0                         , 97 , 0  , 10272, 277, 153), // #1510
  INST(Vtestps          , VexRm_Lx           , V(660F38,0E,_,x,0,_,_,_  ), 0                         , 97 , 0  , 10280, 277, 153), // #1511
  INST(Vucomisd         , VexRm              , V(660F00,2E,_,I,I,1,3,T1S), 0                         , 122, 0  , 10288, 225, 134), // #1512
  INST(Vucomiss         , VexRm              , V(000F00,2E,_,I,I,0,2,T1S), 0                         , 123, 0  , 10297, 226, 134), // #1513
  INST(Vunpckhpd        , VexRvm_Lx          , V(660F00,15,_,x,I,1,4,FV ), 0                         , 104, 0  , 10306, 206, 124), // #1514
  INST(Vunpckhps        , VexRvm_Lx          , V(000F00,15,_,x,I,0,4,FV ), 0                         , 105, 0  , 10316, 207, 124), // #1515
  INST(Vunpcklpd        , VexRvm_Lx          , V(660F00,14,_,x,I,1,4,FV ), 0                         , 104, 0  , 10326, 206, 124), // #1516
  INST(Vunpcklps        , VexRvm_Lx          , V(000F00,14,_,x,I,0,4,FV ), 0                         , 105, 0  , 10336, 207, 124), // #1517
  INST(Vxorpd           , VexRvm_Lx          , V(660F00,57,_,x,I,1,4,FV ), 0                         , 104, 0  , 10346, 389, 130), // #1518
  INST(Vxorps           , VexRvm_Lx          , V(000F00,57,_,x,I,0,4,FV ), 0                         , 105, 0  , 10353, 388, 130), // #1519
  INST(Vzeroall         , VexOp              , V(000F00,77,_,1,I,_,_,_  ), 0                         , 69 , 0  , 10360, 409, 126), // #1520
  INST(Vzeroupper       , VexOp              , V(000F00,77,_,0,I,_,_,_  ), 0                         , 73 , 0  , 10369, 409, 126), // #1521
  INST(Wbinvd           , X86Op              , O(000F00,09,_,_,_,_,_,_  ), 0                         , 4  , 0  , 10380, 30 , 0  ), // #1522
  INST(Wbnoinvd         , X86Op              , O(F30F00,09,_,_,_,_,_,_  ), 0                         , 6  , 0  , 10387, 30 , 162), // #1523
  INST(Wrfsbase         , X86M               , O(F30F00,AE,2,_,x,_,_,_  ), 0                         , 227, 0  , 10396, 173, 104), // #1524
  INST(Wrgsbase         , X86M               , O(F30F00,AE,3,_,x,_,_,_  ), 0                         , 228, 0  , 10405, 173, 104), // #1525
  INST(Wrmsr            , X86Op              , O(000F00,30,_,_,_,_,_,_  ), 0                         , 4  , 0  , 10414, 174, 105), // #1526
  INST(Wrssd            , X86Mr              , O(000F38,F6,_,_,_,_,_,_  ), 0                         , 84 , 0  , 10420, 410, 56 ), // #1527
  INST(Wrssq            , X86Mr              , O(000F38,F6,_,_,1,_,_,_  ), 0                         , 229, 0  , 10426, 411, 56 ), // #1528
  INST(Wrussd           , X86Mr              , O(660F38,F5,_,_,_,_,_,_  ), 0                         , 2  , 0  , 10432, 410, 56 ), // #1529
  INST(Wrussq           , X86Mr              , O(660F38,F5,_,_,1,_,_,_  ), 0                         , 230, 0  , 10439, 411, 56 ), // #1530
  INST(Xabort           , X86Op_Mod11RM_I8   , O(000000,C6,7,_,_,_,_,_  ), 0                         , 27 , 0  , 10446, 80 , 163), // #1531
  INST(Xadd             , X86Xadd            , O(000F00,C0,_,_,x,_,_,_  ), 0                         , 4  , 0  , 10453, 412, 38 ), // #1532
  INST(Xbegin           , X86JmpRel          , O(000000,C7,7,_,_,_,_,_  ), 0                         , 27 , 0  , 10458, 413, 163), // #1533
  INST(Xchg             , X86Xchg            , O(000000,86,_,_,x,_,_,_  ), 0                         , 0  , 0  , 462  , 414, 0  ), // #1534
  INST(Xend             , X86Op              , O(000F01,D5,_,_,_,_,_,_  ), 0                         , 21 , 0  , 10465, 30 , 163), // #1535
  INST(Xgetbv           , X86Op              , O(000F01,D0,_,_,_,_,_,_  ), 0                         , 21 , 0  , 10470, 174, 164), // #1536
  INST(Xlatb            , X86Op              , O(000000,D7,_,_,_,_,_,_  ), 0                         , 0  , 0  , 10477, 30 , 0  ), // #1537
  INST(Xor              , X86Arith           , O(000000,30,6,_,x,_,_,_  ), 0                         , 32 , 0  , 9565 , 179, 1  ), // #1538
  INST(Xorpd            , ExtRm              , O(660F00,57,_,_,_,_,_,_  ), 0                         , 3  , 0  , 10347, 151, 4  ), // #1539
  INST(Xorps            , ExtRm              , O(000F00,57,_,_,_,_,_,_  ), 0                         , 4  , 0  , 10354, 151, 5  ), // #1540
  INST(Xresldtrk        , X86Op              , O(F20F01,E9,_,_,_,_,_,_  ), 0                         , 93 , 0  , 10483, 30 , 165), // #1541
  INST(Xrstor           , X86M_Only_EDX_EAX  , O(000F00,AE,5,_,_,_,_,_  ), 0                         , 78 , 0  , 1164 , 415, 164), // #1542
  INST(Xrstor64         , X86M_Only_EDX_EAX  , O(000F00,AE,5,_,1,_,_,_  ), 0                         , 231, 0  , 1172 , 416, 164), // #1543
  INST(Xrstors          , X86M_Only_EDX_EAX  , O(000F00,C7,3,_,_,_,_,_  ), 0                         , 79 , 0  , 10493, 415, 166), // #1544
  INST(Xrstors64        , X86M_Only_EDX_EAX  , O(000F00,C7,3,_,1,_,_,_  ), 0                         , 232, 0  , 10501, 416, 166), // #1545
  INST(Xsave            , X86M_Only_EDX_EAX  , O(000F00,AE,4,_,_,_,_,_  ), 0                         , 98 , 0  , 1182 , 415, 164), // #1546
  INST(Xsave64          , X86M_Only_EDX_EAX  , O(000F00,AE,4,_,1,_,_,_  ), 0                         , 233, 0  , 1189 , 416, 164), // #1547
  INST(Xsavec           , X86M_Only_EDX_EAX  , O(000F00,C7,4,_,_,_,_,_  ), 0                         , 98 , 0  , 10511, 415, 167), // #1548
  INST(Xsavec64         , X86M_Only_EDX_EAX  , O(000F00,C7,4,_,1,_,_,_  ), 0                         , 233, 0  , 10518, 416, 167), // #1549
  INST(Xsaveopt         , X86M_Only_EDX_EAX  , O(000F00,AE,6,_,_,_,_,_  ), 0                         , 81 , 0  , 10527, 415, 168), // #1550
  INST(Xsaveopt64       , X86M_Only_EDX_EAX  , O(000F00,AE,6,_,1,_,_,_  ), 0                         , 234, 0  , 10536, 416, 168), // #1551
  INST(Xsaves           , X86M_Only_EDX_EAX  , O(000F00,C7,5,_,_,_,_,_  ), 0                         , 78 , 0  , 10547, 415, 166), // #1552
  INST(Xsaves64         , X86M_Only_EDX_EAX  , O(000F00,C7,5,_,1,_,_,_  ), 0                         , 231, 0  , 10554, 416, 166), // #1553
  INST(Xsetbv           , X86Op              , O(000F01,D1,_,_,_,_,_,_  ), 0                         , 21 , 0  , 10563, 174, 164), // #1554
  INST(Xsusldtrk        , X86Op              , O(F20F01,E8,_,_,_,_,_,_  ), 0                         , 93 , 0  , 10570, 30 , 165), // #1555
  INST(Xtest            , X86Op              , O(000F01,D6,_,_,_,_,_,_  ), 0                         , 21 , 0  , 10580, 30 , 169)  // #1556
  // ${InstInfo:End}
};
#undef NAME_DATA_INDEX
#undef INST

// ============================================================================
// [asmjit::x86::InstDB - Opcode Tables]
// ============================================================================

// ${MainOpcodeTable:Begin}
// ------------------- Automatically generated, do not edit -------------------
const uint32_t InstDB::_mainOpcodeTable[] = {
  O(000000,00,0,0,0,0,0,_  ), // #0 [ref=56x]
  O(000000,00,2,0,0,0,0,_  ), // #1 [ref=4x]
  O(660F38,00,0,0,0,0,0,_  ), // #2 [ref=43x]
  O(660F00,00,0,0,0,0,0,_  ), // #3 [ref=38x]
  O(000F00,00,0,0,0,0,0,_  ), // #4 [ref=231x]
  O(F20F00,00,0,0,0,0,0,_  ), // #5 [ref=24x]
  O(F30F00,00,0,0,0,0,0,_  ), // #6 [ref=29x]
  O(F30F38,00,0,0,0,0,0,_  ), // #7 [ref=2x]
  O(660F3A,00,0,0,0,0,0,_  ), // #8 [ref=22x]
  O(000000,00,4,0,0,0,0,_  ), // #9 [ref=5x]
  V(000F38,00,0,0,0,0,0,_  ), // #10 [ref=6x]
  V(XOP_M9,00,1,0,0,0,0,_  ), // #11 [ref=3x]
  V(XOP_M9,00,6,0,0,0,0,_  ), // #12 [ref=2x]
  V(XOP_M9,00,5,0,0,0,0,_  ), // #13 [ref=1x]
  V(XOP_M9,00,3,0,0,0,0,_  ), // #14 [ref=1x]
  V(XOP_M9,00,2,0,0,0,0,_  ), // #15 [ref=1x]
  V(000F38,00,3,0,0,0,0,_  ), // #16 [ref=1x]
  V(000F38,00,2,0,0,0,0,_  ), // #17 [ref=1x]
  V(000F38,00,1,0,0,0,0,_  ), // #18 [ref=1x]
  O(660000,00,0,0,0,0,0,_  ), // #19 [ref=7x]
  O(000000,00,0,0,1,0,0,_  ), // #20 [ref=3x]
  O(000F01,00,0,0,0,0,0,_  ), // #21 [ref=29x]
  O(000F00,00,7,0,0,0,0,_  ), // #22 [ref=5x]
  O(660F00,00,7,0,0,0,0,_  ), // #23 [ref=1x]
  O(F30F00,00,6,0,0,0,0,_  ), // #24 [ref=4x]
  O(F30F01,00,0,0,0,0,0,_  ), // #25 [ref=9x]
  O(660F00,00,6,0,0,0,0,_  ), // #26 [ref=3x]
  O(000000,00,7,0,0,0,0,_  ), // #27 [ref=5x]
  O(000F00,00,1,0,1,0,0,_  ), // #28 [ref=2x]
  O(000F00,00,1,0,0,0,0,_  ), // #29 [ref=6x]
  O(F20F38,00,0,0,0,0,0,_  ), // #30 [ref=2x]
  O(000000,00,1,0,0,0,0,_  ), // #31 [ref=3x]
  O(000000,00,6,0,0,0,0,_  ), // #32 [ref=3x]
  O(F30F00,00,7,0,0,0,0,3  ), // #33 [ref=1x]
  O(F30F00,00,7,0,0,0,0,2  ), // #34 [ref=1x]
  O_FPU(00,D900,_)          , // #35 [ref=29x]
  O_FPU(00,C000,0)          , // #36 [ref=1x]
  O_FPU(00,DE00,_)          , // #37 [ref=7x]
  O_FPU(00,0000,4)          , // #38 [ref=4x]
  O_FPU(00,0000,6)          , // #39 [ref=4x]
  O_FPU(9B,DB00,_)          , // #40 [ref=2x]
  O_FPU(00,DA00,_)          , // #41 [ref=5x]
  O_FPU(00,DB00,_)          , // #42 [ref=8x]
  O_FPU(00,D000,2)          , // #43 [ref=1x]
  O_FPU(00,DF00,_)          , // #44 [ref=2x]
  O_FPU(00,D800,3)          , // #45 [ref=1x]
  O_FPU(00,F000,6)          , // #46 [ref=1x]
  O_FPU(00,F800,7)          , // #47 [ref=1x]
  O_FPU(00,DD00,_)          , // #48 [ref=3x]
  O_FPU(00,0000,0)          , // #49 [ref=3x]
  O_FPU(00,0000,2)          , // #50 [ref=3x]
  O_FPU(00,0000,3)          , // #51 [ref=3x]
  O_FPU(00,0000,7)          , // #52 [ref=3x]
  O_FPU(00,0000,1)          , // #53 [ref=2x]
  O_FPU(00,0000,5)          , // #54 [ref=2x]
  O_FPU(00,C800,1)          , // #55 [ref=1x]
  O_FPU(9B,0000,6)          , // #56 [ref=2x]
  O_FPU(9B,0000,7)          , // #57 [ref=2x]
  O_FPU(00,E000,4)          , // #58 [ref=1x]
  O_FPU(00,E800,5)          , // #59 [ref=1x]
  O_FPU(00,0000,_)          , // #60 [ref=1x]
  O(000F00,00,0,0,1,0,0,_  ), // #61 [ref=3x]
  O(F30F3A,00,0,0,0,0,0,_  ), // #62 [ref=1x]
  O(000000,00,5,0,0,0,0,_  ), // #63 [ref=4x]
  O(F30F00,00,5,0,0,0,0,_  ), // #64 [ref=2x]
  O(F30F00,00,5,0,1,0,0,_  ), // #65 [ref=1x]
  V(660F00,00,0,1,0,0,0,_  ), // #66 [ref=7x]
  V(660F00,00,0,1,1,0,0,_  ), // #67 [ref=6x]
  V(000F00,00,0,1,1,0,0,_  ), // #68 [ref=7x]
  V(000F00,00,0,1,0,0,0,_  ), // #69 [ref=8x]
  V(660F00,00,0,0,0,0,0,_  ), // #70 [ref=15x]
  V(660F00,00,0,0,1,0,0,_  ), // #71 [ref=4x]
  V(000F00,00,0,0,1,0,0,_  ), // #72 [ref=4x]
  V(000F00,00,0,0,0,0,0,_  ), // #73 [ref=10x]
  V(660F3A,00,0,0,0,0,0,_  ), // #74 [ref=45x]
  V(660F3A,00,0,0,1,0,0,_  ), // #75 [ref=4x]
  O(000000,00,3,0,0,0,0,_  ), // #76 [ref=4x]
  O(000F00,00,2,0,0,0,0,_  ), // #77 [ref=5x]
  O(000F00,00,5,0,0,0,0,_  ), // #78 [ref=4x]
  O(000F00,00,3,0,0,0,0,_  ), // #79 [ref=5x]
  V(XOP_M9,00,0,0,0,0,0,_  ), // #80 [ref=32x]
  O(000F00,00,6,0,0,0,0,_  ), // #81 [ref=5x]
  V(XOP_MA,00,0,0,0,0,0,_  ), // #82 [ref=1x]
  V(XOP_MA,00,1,0,0,0,0,_  ), // #83 [ref=1x]
  O(000F38,00,0,0,0,0,0,_  ), // #84 [ref=24x]
  V(F20F38,00,0,0,0,0,0,_  ), // #85 [ref=6x]
  O(000F3A,00,0,0,0,0,0,_  ), // #86 [ref=4x]
  O(F30000,00,0,0,0,0,0,_  ), // #87 [ref=1x]
  O(000F0F,00,0,0,0,0,0,_  ), // #88 [ref=26x]
  V(F30F38,00,0,0,0,0,0,_  ), // #89 [ref=5x]
  O(000F3A,00,0,0,1,0,0,_  ), // #90 [ref=1x]
  O(660F3A,00,0,0,1,0,0,_  ), // #91 [ref=1x]
  O(F30F00,00,4,0,0,0,0,_  ), // #92 [ref=1x]
  O(F20F01,00,0,0,0,0,0,_  ), // #93 [ref=4x]
  O(F30F00,00,1,0,0,0,0,_  ), // #94 [ref=3x]
  O(F30F00,00,7,0,0,0,0,_  ), // #95 [ref=1x]
  V(F20F3A,00,0,0,0,0,0,_  ), // #96 [ref=1x]
  V(660F38,00,0,0,0,0,0,_  ), // #97 [ref=25x]
  O(000F00,00,4,0,0,0,0,_  ), // #98 [ref=4x]
  V(XOP_M9,00,7,0,0,0,0,_  ), // #99 [ref=1x]
  V(XOP_M9,00,4,0,0,0,0,_  ), // #100 [ref=1x]
  O(F20F00,00,6,0,0,0,0,_  ), // #101 [ref=1x]
  E(F20F38,00,0,2,0,0,4,T4X), // #102 [ref=4x]
  E(F20F38,00,0,0,0,0,4,T4X), // #103 [ref=2x]
  V(660F00,00,0,0,0,1,4,FV ), // #104 [ref=22x]
  V(000F00,00,0,0,0,0,4,FV ), // #105 [ref=16x]
  V(F20F00,00,0,0,0,1,3,T1S), // #106 [ref=10x]
  V(F30F00,00,0,0,0,0,2,T1S), // #107 [ref=10x]
  V(F20F00,00,0,0,0,0,0,_  ), // #108 [ref=4x]
  V(660F38,00,0,0,0,0,4,FVM), // #109 [ref=14x]
  E(660F3A,00,0,0,0,0,4,FV ), // #110 [ref=14x]
  E(660F3A,00,0,0,0,1,4,FV ), // #111 [ref=14x]
  E(660F38,00,0,0,0,1,4,FV ), // #112 [ref=29x]
  E(660F38,00,0,0,0,0,4,FV ), // #113 [ref=18x]
  V(660F38,00,0,1,0,0,0,_  ), // #114 [ref=2x]
  E(660F38,00,0,0,0,0,3,T2 ), // #115 [ref=2x]
  E(660F38,00,0,0,0,0,4,T4 ), // #116 [ref=2x]
  E(660F38,00,0,2,0,0,5,T8 ), // #117 [ref=2x]
  E(660F38,00,0,0,0,1,4,T2 ), // #118 [ref=2x]
  E(660F38,00,0,2,0,1,5,T4 ), // #119 [ref=2x]
  V(660F38,00,0,0,0,1,3,T1S), // #120 [ref=2x]
  V(660F38,00,0,0,0,0,2,T1S), // #121 [ref=14x]
  V(660F00,00,0,0,0,1,3,T1S), // #122 [ref=5x]
  V(000F00,00,0,0,0,0,2,T1S), // #123 [ref=2x]
  E(660F38,00,0,0,0,1,3,T1S), // #124 [ref=14x]
  E(660F38,00,0,0,0,0,2,T1S), // #125 [ref=14x]
  V(F30F00,00,0,0,0,0,3,HV ), // #126 [ref=1x]
  E(F20F38,00,0,0,0,0,4,FV ), // #127 [ref=2x]
  E(F30F38,00,0,0,0,0,4,FV ), // #128 [ref=3x]
  V(F20F00,00,0,0,0,1,4,FV ), // #129 [ref=1x]
  E(660F00,00,0,0,0,1,4,FV ), // #130 [ref=9x]
  E(000F00,00,0,0,0,1,4,FV ), // #131 [ref=3x]
  V(660F38,00,0,0,0,0,3,HVM), // #132 [ref=7x]
  V(660F00,00,0,0,0,0,4,FV ), // #133 [ref=11x]
  V(000F00,00,0,0,0,0,3,HV ), // #134 [ref=1x]
  V(660F3A,00,0,0,0,0,3,HVM), // #135 [ref=1x]
  E(660F00,00,0,0,0,0,3,HV ), // #136 [ref=4x]
  E(000F00,00,0,0,0,0,4,FV ), // #137 [ref=2x]
  E(F30F00,00,0,0,0,1,4,FV ), // #138 [ref=2x]
  V(F20F00,00,0,0,0,0,3,T1F), // #139 [ref=2x]
  E(F20F00,00,0,0,0,0,3,T1F), // #140 [ref=2x]
  V(F20F00,00,0,0,0,0,2,T1W), // #141 [ref=1x]
  V(F30F00,00,0,0,0,0,2,T1W), // #142 [ref=1x]
  V(F30F00,00,0,0,0,0,2,T1F), // #143 [ref=2x]
  E(F30F00,00,0,0,0,0,2,T1F), // #144 [ref=2x]
  V(F30F00,00,0,0,0,0,4,FV ), // #145 [ref=1x]
  E(F30F00,00,0,0,0,0,3,HV ), // #146 [ref=1x]
  E(F20F00,00,0,0,0,0,4,FV ), // #147 [ref=1x]
  E(F20F00,00,0,0,0,1,4,FV ), // #148 [ref=1x]
  E(F20F00,00,0,0,0,0,2,T1W), // #149 [ref=1x]
  E(F30F00,00,0,0,0,0,2,T1W), // #150 [ref=1x]
  E(660F3A,00,0,0,0,0,4,FVM), // #151 [ref=3x]
  E(660F38,00,0,2,0,1,4,FV ), // #152 [ref=3x]
  E(660F38,00,0,2,0,0,4,FV ), // #153 [ref=3x]
  V(660F3A,00,0,1,0,0,0,_  ), // #154 [ref=6x]
  E(660F3A,00,0,0,0,0,4,T4 ), // #155 [ref=4x]
  E(660F3A,00,0,2,0,0,5,T8 ), // #156 [ref=4x]
  E(660F3A,00,0,0,0,1,4,T2 ), // #157 [ref=4x]
  E(660F3A,00,0,2,0,1,5,T4 ), // #158 [ref=4x]
  V(660F3A,00,0,0,0,0,2,T1S), // #159 [ref=4x]
  E(660F3A,00,0,0,0,1,3,T1S), // #160 [ref=6x]
  E(660F3A,00,0,0,0,0,2,T1S), // #161 [ref=6x]
  V(660F38,00,0,0,1,1,4,FV ), // #162 [ref=20x]
  V(660F38,00,0,0,0,0,4,FV ), // #163 [ref=36x]
  V(660F38,00,0,0,1,1,3,T1S), // #164 [ref=12x]
  V(660F38,00,0,0,1,0,0,_  ), // #165 [ref=5x]
  E(660F38,00,1,2,0,1,3,T1S), // #166 [ref=2x]
  E(660F38,00,1,2,0,0,2,T1S), // #167 [ref=2x]
  E(660F38,00,2,2,0,1,3,T1S), // #168 [ref=2x]
  E(660F38,00,2,2,0,0,2,T1S), // #169 [ref=2x]
  V(660F3A,00,0,0,1,1,4,FV ), // #170 [ref=2x]
  V(000F00,00,2,0,0,0,0,_  ), // #171 [ref=1x]
  V(660F00,00,0,0,0,1,4,FVM), // #172 [ref=3x]
  V(000F00,00,0,0,0,0,4,FVM), // #173 [ref=3x]
  V(660F00,00,0,0,0,0,2,T1S), // #174 [ref=1x]
  V(F20F00,00,0,0,0,1,3,DUP), // #175 [ref=1x]
  E(660F00,00,0,0,0,0,4,FVM), // #176 [ref=1x]
  E(660F00,00,0,0,0,1,4,FVM), // #177 [ref=1x]
  V(F30F00,00,0,0,0,0,0,_  ), // #178 [ref=3x]
  E(F20F00,00,0,0,0,1,4,FVM), // #179 [ref=1x]
  E(F30F00,00,0,0,0,0,4,FVM), // #180 [ref=1x]
  E(F30F00,00,0,0,0,1,4,FVM), // #181 [ref=1x]
  E(F20F00,00,0,0,0,0,4,FVM), // #182 [ref=1x]
  V(000F00,00,0,0,0,0,3,T2 ), // #183 [ref=2x]
  V(660F00,00,0,0,0,0,4,FVM), // #184 [ref=32x]
  V(F30F00,00,0,0,0,0,4,FVM), // #185 [ref=3x]
  E(F20F38,00,0,0,0,1,4,FV ), // #186 [ref=1x]
  V(660F3A,00,0,0,0,0,4,FVM), // #187 [ref=2x]
  E(660F00,00,0,0,0,0,4,FV ), // #188 [ref=5x]
  E(660F38,00,0,0,0,0,4,FVM), // #189 [ref=7x]
  E(660F38,00,0,0,0,1,4,FVM), // #190 [ref=11x]
  V(660F38,00,0,0,0,0,0,T1S), // #191 [ref=1x]
  E(F30F38,00,0,0,0,1,0,_  ), // #192 [ref=5x]
  E(F30F38,00,0,0,0,0,0,_  ), // #193 [ref=5x]
  V(660F38,00,0,0,0,0,1,T1S), // #194 [ref=1x]
  V(XOP_M8,00,0,0,0,0,0,_  ), // #195 [ref=22x]
  V(660F38,00,0,0,0,1,4,FVM), // #196 [ref=2x]
  E(660F3A,00,0,0,0,1,4,FVM), // #197 [ref=4x]
  E(660F38,00,0,0,0,0,0,T1S), // #198 [ref=2x]
  E(660F38,00,0,0,0,1,1,T1S), // #199 [ref=2x]
  V(660F38,00,0,0,0,1,4,FV ), // #200 [ref=2x]
  E(660F38,00,0,0,1,1,4,FV ), // #201 [ref=1x]
  V(660F3A,00,0,0,0,0,0,T1S), // #202 [ref=2x]
  V(660F3A,00,0,0,1,1,3,T1S), // #203 [ref=2x]
  V(660F3A,00,0,0,0,0,1,T1S), // #204 [ref=1x]
  V(660F00,00,0,0,0,0,1,T1S), // #205 [ref=1x]
  E(F30F38,00,0,0,0,0,2,QVM), // #206 [ref=6x]
  E(F30F38,00,0,0,0,0,3,HVM), // #207 [ref=9x]
  E(F30F38,00,0,0,0,0,1,OVM), // #208 [ref=3x]
  V(660F38,00,0,0,0,0,2,QVM), // #209 [ref=4x]
  V(660F38,00,0,0,0,0,1,OVM), // #210 [ref=2x]
  E(660F00,00,1,0,0,0,4,FV ), // #211 [ref=1x]
  E(660F00,00,1,0,0,1,4,FV ), // #212 [ref=1x]
  V(F20F00,00,0,0,0,0,4,FVM), // #213 [ref=1x]
  V(660F00,00,0,0,0,0,4,128), // #214 [ref=6x]
  V(660F00,00,7,0,0,0,4,FVM), // #215 [ref=1x]
  V(660F00,00,0,0,0,1,4,128), // #216 [ref=2x]
  E(660F00,00,0,0,0,1,4,128), // #217 [ref=1x]
  V(660F00,00,3,0,0,0,4,FVM), // #218 [ref=1x]
  E(F30F38,00,0,0,0,0,4,FVM), // #219 [ref=1x]
  E(F30F38,00,0,0,0,1,4,FV ), // #220 [ref=1x]
  E(F30F38,00,0,0,0,1,4,FVM), // #221 [ref=1x]
  E(660F38,00,5,2,0,1,3,T1S), // #222 [ref=2x]
  E(660F38,00,5,2,0,0,2,T1S), // #223 [ref=2x]
  E(660F38,00,6,2,0,1,3,T1S), // #224 [ref=2x]
  E(660F38,00,6,2,0,0,2,T1S), // #225 [ref=2x]
  V(000F00,00,3,0,0,0,0,_  ), // #226 [ref=1x]
  O(F30F00,00,2,0,0,0,0,_  ), // #227 [ref=1x]
  O(F30F00,00,3,0,0,0,0,_  ), // #228 [ref=1x]
  O(000F38,00,0,0,1,0,0,_  ), // #229 [ref=1x]
  O(660F38,00,0,0,1,0,0,_  ), // #230 [ref=1x]
  O(000F00,00,5,0,1,0,0,_  ), // #231 [ref=2x]
  O(000F00,00,3,0,1,0,0,_  ), // #232 [ref=1x]
  O(000F00,00,4,0,1,0,0,_  ), // #233 [ref=2x]
  O(000F00,00,6,0,1,0,0,_  )  // #234 [ref=1x]
};
// ----------------------------------------------------------------------------
// ${MainOpcodeTable:End}

// ${AltOpcodeTable:Begin}
// ------------------- Automatically generated, do not edit -------------------
const uint32_t InstDB::_altOpcodeTable[] = {
  0                         , // #0 [ref=1410x]
  O(660F00,1B,_,_,_,_,_,_  ), // #1 [ref=1x]
  O(000F00,BA,4,_,x,_,_,_  ), // #2 [ref=1x]
  O(000F00,BA,7,_,x,_,_,_  ), // #3 [ref=1x]
  O(000F00,BA,6,_,x,_,_,_  ), // #4 [ref=1x]
  O(000F00,BA,5,_,x,_,_,_  ), // #5 [ref=1x]
  O(000000,48,_,_,x,_,_,_  ), // #6 [ref=1x]
  O(660F00,78,0,_,_,_,_,_  ), // #7 [ref=1x]
  O_FPU(00,00DF,5)          , // #8 [ref=1x]
  O_FPU(00,00DF,7)          , // #9 [ref=1x]
  O_FPU(00,00DD,1)          , // #10 [ref=1x]
  O_FPU(00,00DB,5)          , // #11 [ref=1x]
  O_FPU(00,DFE0,_)          , // #12 [ref=1x]
  O(000000,DB,7,_,_,_,_,_  ), // #13 [ref=1x]
  O_FPU(9B,DFE0,_)          , // #14 [ref=1x]
  O(000000,E4,_,_,_,_,_,_  ), // #15 [ref=1x]
  O(000000,40,_,_,x,_,_,_  ), // #16 [ref=1x]
  O(F20F00,78,_,_,_,_,_,_  ), // #17 [ref=1x]
  O(000000,77,_,_,_,_,_,_  ), // #18 [ref=2x]
  O(000000,73,_,_,_,_,_,_  ), // #19 [ref=3x]
  O(000000,72,_,_,_,_,_,_  ), // #20 [ref=3x]
  O(000000,76,_,_,_,_,_,_  ), // #21 [ref=2x]
  O(000000,74,_,_,_,_,_,_  ), // #22 [ref=2x]
  O(000000,E3,_,_,_,_,_,_  ), // #23 [ref=1x]
  O(000000,7F,_,_,_,_,_,_  ), // #24 [ref=2x]
  O(000000,7D,_,_,_,_,_,_  ), // #25 [ref=2x]
  O(000000,7C,_,_,_,_,_,_  ), // #26 [ref=2x]
  O(000000,7E,_,_,_,_,_,_  ), // #27 [ref=2x]
  O(000000,EB,_,_,_,_,_,_  ), // #28 [ref=1x]
  O(000000,75,_,_,_,_,_,_  ), // #29 [ref=2x]
  O(000000,71,_,_,_,_,_,_  ), // #30 [ref=1x]
  O(000000,7B,_,_,_,_,_,_  ), // #31 [ref=2x]
  O(000000,79,_,_,_,_,_,_  ), // #32 [ref=1x]
  O(000000,70,_,_,_,_,_,_  ), // #33 [ref=1x]
  O(000000,7A,_,_,_,_,_,_  ), // #34 [ref=2x]
  O(000000,78,_,_,_,_,_,_  ), // #35 [ref=1x]
  V(660F00,92,_,0,0,_,_,_  ), // #36 [ref=1x]
  V(F20F00,92,_,0,0,_,_,_  ), // #37 [ref=1x]
  V(F20F00,92,_,0,1,_,_,_  ), // #38 [ref=1x]
  V(000F00,92,_,0,0,_,_,_  ), // #39 [ref=1x]
  O(000000,9A,_,_,_,_,_,_  ), // #40 [ref=1x]
  O(000000,EA,_,_,_,_,_,_  ), // #41 [ref=1x]
  O(000000,E2,_,_,_,_,_,_  ), // #42 [ref=1x]
  O(000000,E1,_,_,_,_,_,_  ), // #43 [ref=1x]
  O(000000,E0,_,_,_,_,_,_  ), // #44 [ref=1x]
  O(660F00,29,_,_,_,_,_,_  ), // #45 [ref=1x]
  O(000F00,29,_,_,_,_,_,_  ), // #46 [ref=1x]
  O(000F38,F1,_,_,x,_,_,_  ), // #47 [ref=1x]
  O(000F00,7E,_,_,_,_,_,_  ), // #48 [ref=1x]
  O(660F00,7F,_,_,_,_,_,_  ), // #49 [ref=1x]
  O(F30F00,7F,_,_,_,_,_,_  ), // #50 [ref=1x]
  O(660F00,17,_,_,_,_,_,_  ), // #51 [ref=1x]
  O(000F00,17,_,_,_,_,_,_  ), // #52 [ref=1x]
  O(660F00,13,_,_,_,_,_,_  ), // #53 [ref=1x]
  O(000F00,13,_,_,_,_,_,_  ), // #54 [ref=1x]
  O(660F00,E7,_,_,_,_,_,_  ), // #55 [ref=1x]
  O(660F00,2B,_,_,_,_,_,_  ), // #56 [ref=1x]
  O(000F00,2B,_,_,_,_,_,_  ), // #57 [ref=1x]
  O(000F00,E7,_,_,_,_,_,_  ), // #58 [ref=1x]
  O(F20F00,2B,_,_,_,_,_,_  ), // #59 [ref=1x]
  O(F30F00,2B,_,_,_,_,_,_  ), // #60 [ref=1x]
  O(000F00,7E,_,_,x,_,_,_  ), // #61 [ref=1x]
  O(F20F00,11,_,_,_,_,_,_  ), // #62 [ref=1x]
  O(F30F00,11,_,_,_,_,_,_  ), // #63 [ref=1x]
  O(660F00,11,_,_,_,_,_,_  ), // #64 [ref=1x]
  O(000F00,11,_,_,_,_,_,_  ), // #65 [ref=1x]
  O(000000,E6,_,_,_,_,_,_  ), // #66 [ref=1x]
  O(000F3A,15,_,_,_,_,_,_  ), // #67 [ref=1x]
  O(000000,58,_,_,_,_,_,_  ), // #68 [ref=1x]
  O(000F00,72,6,_,_,_,_,_  ), // #69 [ref=1x]
  O(660F00,73,7,_,_,_,_,_  ), // #70 [ref=1x]
  O(000F00,73,6,_,_,_,_,_  ), // #71 [ref=1x]
  O(000F00,71,6,_,_,_,_,_  ), // #72 [ref=1x]
  O(000F00,72,4,_,_,_,_,_  ), // #73 [ref=1x]
  O(000F00,71,4,_,_,_,_,_  ), // #74 [ref=1x]
  O(000F00,72,2,_,_,_,_,_  ), // #75 [ref=1x]
  O(660F00,73,3,_,_,_,_,_  ), // #76 [ref=1x]
  O(000F00,73,2,_,_,_,_,_  ), // #77 [ref=1x]
  O(000F00,71,2,_,_,_,_,_  ), // #78 [ref=1x]
  O(000000,50,_,_,_,_,_,_  ), // #79 [ref=1x]
  O(000000,F6,_,_,x,_,_,_  ), // #80 [ref=1x]
  E(660F38,92,_,x,_,1,3,T1S), // #81 [ref=1x]
  E(660F38,92,_,x,_,0,2,T1S), // #82 [ref=1x]
  E(660F38,93,_,x,_,1,3,T1S), // #83 [ref=1x]
  E(660F38,93,_,x,_,0,2,T1S), // #84 [ref=1x]
  V(660F38,2F,_,x,0,_,_,_  ), // #85 [ref=1x]
  V(660F38,2E,_,x,0,_,_,_  ), // #86 [ref=1x]
  V(660F00,29,_,x,I,1,4,FVM), // #87 [ref=1x]
  V(000F00,29,_,x,I,0,4,FVM), // #88 [ref=1x]
  V(660F00,7E,_,0,0,0,2,T1S), // #89 [ref=1x]
  V(660F00,7F,_,x,I,_,_,_  ), // #90 [ref=1x]
  E(660F00,7F,_,x,_,0,4,FVM), // #91 [ref=1x]
  E(660F00,7F,_,x,_,1,4,FVM), // #92 [ref=1x]
  V(F30F00,7F,_,x,I,_,_,_  ), // #93 [ref=1x]
  E(F20F00,7F,_,x,_,1,4,FVM), // #94 [ref=1x]
  E(F30F00,7F,_,x,_,0,4,FVM), // #95 [ref=1x]
  E(F30F00,7F,_,x,_,1,4,FVM), // #96 [ref=1x]
  E(F20F00,7F,_,x,_,0,4,FVM), // #97 [ref=1x]
  V(660F00,17,_,0,I,1,3,T1S), // #98 [ref=1x]
  V(000F00,17,_,0,I,0,3,T2 ), // #99 [ref=1x]
  V(660F00,13,_,0,I,1,3,T1S), // #100 [ref=1x]
  V(000F00,13,_,0,I,0,3,T2 ), // #101 [ref=1x]
  V(660F00,7E,_,0,I,1,3,T1S), // #102 [ref=1x]
  V(F20F00,11,_,I,I,1,3,T1S), // #103 [ref=1x]
  V(F30F00,11,_,I,I,0,2,T1S), // #104 [ref=1x]
  V(660F00,11,_,x,I,1,4,FVM), // #105 [ref=1x]
  V(000F00,11,_,x,I,0,4,FVM), // #106 [ref=1x]
  E(660F38,7A,_,x,0,0,0,T1S), // #107 [ref=1x]
  E(660F38,7C,_,x,0,0,0,T1S), // #108 [ref=1x]
  E(660F38,7C,_,x,0,1,0,T1S), // #109 [ref=1x]
  E(660F38,7B,_,x,0,0,0,T1S), // #110 [ref=1x]
  V(660F3A,05,_,x,0,1,4,FV ), // #111 [ref=1x]
  V(660F3A,04,_,x,0,0,4,FV ), // #112 [ref=1x]
  V(660F3A,01,_,x,1,1,4,FV ), // #113 [ref=1x]
  V(660F3A,00,_,x,1,1,4,FV ), // #114 [ref=1x]
  E(660F38,90,_,x,_,0,2,T1S), // #115 [ref=1x]
  E(660F38,90,_,x,_,1,3,T1S), // #116 [ref=1x]
  E(660F38,91,_,x,_,0,2,T1S), // #117 [ref=1x]
  E(660F38,91,_,x,_,1,3,T1S), // #118 [ref=1x]
  V(660F38,8E,_,x,0,_,_,_  ), // #119 [ref=1x]
  V(660F38,8E,_,x,1,_,_,_  ), // #120 [ref=1x]
  V(XOP_M8,C0,_,0,x,_,_,_  ), // #121 [ref=1x]
  V(XOP_M8,C2,_,0,x,_,_,_  ), // #122 [ref=1x]
  V(XOP_M8,C3,_,0,x,_,_,_  ), // #123 [ref=1x]
  V(XOP_M8,C1,_,0,x,_,_,_  ), // #124 [ref=1x]
  V(660F00,72,6,x,I,0,4,FV ), // #125 [ref=1x]
  V(660F00,73,6,x,I,1,4,FV ), // #126 [ref=1x]
  V(660F00,71,6,x,I,I,4,FVM), // #127 [ref=1x]
  V(660F00,72,4,x,I,0,4,FV ), // #128 [ref=1x]
  E(660F00,72,4,x,_,1,4,FV ), // #129 [ref=1x]
  V(660F00,71,4,x,I,I,4,FVM), // #130 [ref=1x]
  V(660F00,72,2,x,I,0,4,FV ), // #131 [ref=1x]
  V(660F00,73,2,x,I,1,4,FV ), // #132 [ref=1x]
  V(660F00,71,2,x,I,I,4,FVM)  // #133 [ref=1x]
};
// ----------------------------------------------------------------------------
// ${AltOpcodeTable:End}

#undef O
#undef V
#undef E
#undef O_FPU

// ============================================================================
// [asmjit::x86::InstDB - CommonInfoTableA]
// ============================================================================

// ${InstCommonTable:Begin}
// ------------------- Automatically generated, do not edit -------------------
#define F(VAL) InstDB::kFlag##VAL
#define X(VAL) InstDB::kAvx512Flag##VAL
#define CONTROL(VAL) Inst::kControl##VAL
#define SINGLE_REG(VAL) InstDB::kSingleReg##VAL
const InstDB::CommonInfo InstDB::_commonInfoTable[] = {
  { 0                                                 , 0                             , 0  , 0 , CONTROL(None)   , SINGLE_REG(None)}, // #0 [ref=1x]
  { 0                                                 , 0                             , 376, 1 , CONTROL(None)   , SINGLE_REG(None)}, // #1 [ref=4x]
  { 0                                                 , 0                             , 377, 1 , CONTROL(None)   , SINGLE_REG(None)}, // #2 [ref=2x]
  { F(Lock)|F(XAcquire)|F(XRelease)                   , 0                             , 16 , 12, CONTROL(None)   , SINGLE_REG(None)}, // #3 [ref=2x]
  { 0                                                 , 0                             , 180, 2 , CONTROL(None)   , SINGLE_REG(None)}, // #4 [ref=2x]
  { F(Vec)                                            , 0                             , 79 , 1 , CONTROL(None)   , SINGLE_REG(None)}, // #5 [ref=54x]
  { F(Vec)                                            , 0                             , 106, 1 , CONTROL(None)   , SINGLE_REG(None)}, // #6 [ref=19x]
  { F(Vec)                                            , 0                             , 257, 1 , CONTROL(None)   , SINGLE_REG(None)}, // #7 [ref=16x]
  { F(Vec)                                            , 0                             , 215, 1 , CONTROL(None)   , SINGLE_REG(None)}, // #8 [ref=20x]
  { F(Lock)|F(XAcquire)|F(XRelease)                   , 0                             , 28 , 11, CONTROL(None)   , SINGLE_REG(RO)  }, // #9 [ref=1x]
  { F(Vex)                                            , 0                             , 272, 2 , CONTROL(None)   , SINGLE_REG(None)}, // #10 [ref=3x]
  { F(Vec)                                            , 0                             , 79 , 1 , CONTROL(None)   , SINGLE_REG(RO)  }, // #11 [ref=12x]
  { 0                                                 , 0                             , 378, 1 , CONTROL(None)   , SINGLE_REG(None)}, // #12 [ref=1x]
  { F(Vex)                                            , 0                             , 274, 2 , CONTROL(None)   , SINGLE_REG(None)}, // #13 [ref=5x]
  { F(Vex)                                            , 0                             , 180, 2 , CONTROL(None)   , SINGLE_REG(None)}, // #14 [ref=12x]
  { F(Vec)                                            , 0                             , 379, 1 , CONTROL(None)   , SINGLE_REG(None)}, // #15 [ref=4x]
  { 0                                                 , 0                             , 276, 2 , CONTROL(None)   , SINGLE_REG(None)}, // #16 [ref=3x]
  { F(Mib)                                            , 0                             , 380, 1 , CONTROL(None)   , SINGLE_REG(None)}, // #17 [ref=1x]
  { 0                                                 , 0                             , 381, 1 , CONTROL(None)   , SINGLE_REG(None)}, // #18 [ref=1x]
  { 0                                                 , 0                             , 278, 2 , CONTROL(None)   , SINGLE_REG(None)}, // #19 [ref=1x]
  { F(Mib)                                            , 0                             , 382, 1 , CONTROL(None)   , SINGLE_REG(None)}, // #20 [ref=1x]
  { 0                                                 , 0                             , 280, 2 , CONTROL(None)   , SINGLE_REG(None)}, // #21 [ref=1x]
  { 0                                                 , 0                             , 179, 3 , CONTROL(None)   , SINGLE_REG(None)}, // #22 [ref=35x]
  { 0                                                 , 0                             , 383, 1 , CONTROL(None)   , SINGLE_REG(None)}, // #23 [ref=3x]
  { 0                                                 , 0                             , 123, 4 , CONTROL(None)   , SINGLE_REG(None)}, // #24 [ref=1x]
  { F(Lock)|F(XAcquire)|F(XRelease)                   , 0                             , 123, 4 , CONTROL(None)   , SINGLE_REG(None)}, // #25 [ref=3x]
  { F(Rep)|F(RepIgnored)                              , 0                             , 282, 2 , CONTROL(Call)   , SINGLE_REG(None)}, // #26 [ref=1x]
  { 0                                                 , 0                             , 384, 1 , CONTROL(None)   , SINGLE_REG(None)}, // #27 [ref=1x]
  { 0                                                 , 0                             , 385, 1 , CONTROL(None)   , SINGLE_REG(None)}, // #28 [ref=2x]
  { 0                                                 , 0                             , 359, 1 , CONTROL(None)   , SINGLE_REG(None)}, // #29 [ref=1x]
  { 0                                                 , 0                             , 108, 1 , CONTROL(None)   , SINGLE_REG(None)}, // #30 [ref=83x]
  { 0                                                 , 0                             , 386, 1 , CONTROL(None)   , SINGLE_REG(None)}, // #31 [ref=11x]
  { 0                                                 , 0                             , 387, 1 , CONTROL(None)   , SINGLE_REG(None)}, // #32 [ref=6x]
  { 0                                                 , 0                             , 388, 1 , CONTROL(None)   , SINGLE_REG(None)}, // #33 [ref=13x]
  { 0                                                 , 0                             , 389, 1 , CONTROL(None)   , SINGLE_REG(None)}, // #34 [ref=1x]
  { 0                                                 , 0                             , 16 , 12, CONTROL(None)   , SINGLE_REG(None)}, // #35 [ref=1x]
  { F(Rep)                                            , 0                             , 127, 4 , CONTROL(None)   , SINGLE_REG(None)}, // #36 [ref=1x]
  { F(Vec)                                            , 0                             , 390, 1 , CONTROL(None)   , SINGLE_REG(None)}, // #37 [ref=2x]
  { F(Vec)                                            , 0                             , 391, 1 , CONTROL(None)   , SINGLE_REG(None)}, // #38 [ref=3x]
  { F(Lock)|F(XAcquire)|F(XRelease)                   , 0                             , 131, 4 , CONTROL(None)   , SINGLE_REG(None)}, // #39 [ref=1x]
  { F(Lock)|F(XAcquire)|F(XRelease)                   , 0                             , 392, 1 , CONTROL(None)   , SINGLE_REG(None)}, // #40 [ref=1x]
  { F(Lock)|F(XAcquire)|F(XRelease)                   , 0                             , 393, 1 , CONTROL(None)   , SINGLE_REG(None)}, // #41 [ref=1x]
  { 0                                                 , 0                             , 394, 1 , CONTROL(None)   , SINGLE_REG(None)}, // #42 [ref=1x]
  { 0                                                 , 0                             , 395, 1 , CONTROL(None)   , SINGLE_REG(None)}, // #43 [ref=1x]
  { 0                                                 , 0                             , 284, 2 , CONTROL(None)   , SINGLE_REG(None)}, // #44 [ref=1x]
  { F(Mmx)|F(Vec)                                     , 0                             , 396, 1 , CONTROL(None)   , SINGLE_REG(None)}, // #45 [ref=2x]
  { F(Mmx)|F(Vec)                                     , 0                             , 397, 1 , CONTROL(None)   , SINGLE_REG(None)}, // #46 [ref=2x]
  { F(Mmx)|F(Vec)                                     , 0                             , 398, 1 , CONTROL(None)   , SINGLE_REG(None)}, // #47 [ref=2x]
  { F(Vec)                                            , 0                             , 399, 1 , CONTROL(None)   , SINGLE_REG(None)}, // #48 [ref=2x]
  { F(Vec)                                            , 0                             , 400, 1 , CONTROL(None)   , SINGLE_REG(None)}, // #49 [ref=2x]
  { F(Vec)                                            , 0                             , 401, 1 , CONTROL(None)   , SINGLE_REG(None)}, // #50 [ref=2x]
  { 0                                                 , 0                             , 402, 1 , CONTROL(None)   , SINGLE_REG(None)}, // #51 [ref=1x]
  { 0                                                 , 0                             , 403, 1 , CONTROL(None)   , SINGLE_REG(None)}, // #52 [ref=2x]
  { F(Lock)|F(XAcquire)|F(XRelease)                   , 0                             , 286, 2 , CONTROL(None)   , SINGLE_REG(None)}, // #53 [ref=2x]
  { 0                                                 , 0                             , 39 , 4 , CONTROL(None)   , SINGLE_REG(None)}, // #54 [ref=3x]
  { F(Mmx)                                            , 0                             , 108, 1 , CONTROL(None)   , SINGLE_REG(None)}, // #55 [ref=1x]
  { 0                                                 , 0                             , 288, 2 , CONTROL(None)   , SINGLE_REG(None)}, // #56 [ref=2x]
  { 0                                                 , 0                             , 404, 1 , CONTROL(None)   , SINGLE_REG(None)}, // #57 [ref=1x]
  { F(Vec)                                            , 0                             , 405, 1 , CONTROL(None)   , SINGLE_REG(None)}, // #58 [ref=2x]
  { F(Vec)                                            , 0                             , 290, 2 , CONTROL(None)   , SINGLE_REG(None)}, // #59 [ref=1x]
  { F(FpuM32)|F(FpuM64)                               , 0                             , 182, 3 , CONTROL(None)   , SINGLE_REG(None)}, // #60 [ref=6x]
  { 0                                                 , 0                             , 292, 2 , CONTROL(None)   , SINGLE_REG(None)}, // #61 [ref=9x]
  { F(FpuM80)                                         , 0                             , 406, 1 , CONTROL(None)   , SINGLE_REG(None)}, // #62 [ref=2x]
  { 0                                                 , 0                             , 293, 1 , CONTROL(None)   , SINGLE_REG(None)}, // #63 [ref=13x]
  { F(FpuM32)|F(FpuM64)                               , 0                             , 294, 2 , CONTROL(None)   , SINGLE_REG(None)}, // #64 [ref=2x]
  { F(FpuM16)|F(FpuM32)                               , 0                             , 407, 1 , CONTROL(None)   , SINGLE_REG(None)}, // #65 [ref=9x]
  { F(FpuM16)|F(FpuM32)|F(FpuM64)                     , 0                             , 408, 1 , CONTROL(None)   , SINGLE_REG(None)}, // #66 [ref=3x]
  { F(FpuM32)|F(FpuM64)|F(FpuM80)                     , 0                             , 409, 1 , CONTROL(None)   , SINGLE_REG(None)}, // #67 [ref=2x]
  { F(FpuM16)                                         , 0                             , 410, 1 , CONTROL(None)   , SINGLE_REG(None)}, // #68 [ref=3x]
  { 0                                                 , 0                             , 411, 1 , CONTROL(None)   , SINGLE_REG(None)}, // #69 [ref=13x]
  { F(FpuM16)                                         , 0                             , 412, 1 , CONTROL(None)   , SINGLE_REG(None)}, // #70 [ref=2x]
  { F(FpuM32)|F(FpuM64)                               , 0                             , 295, 1 , CONTROL(None)   , SINGLE_REG(None)}, // #71 [ref=1x]
  { 0                                                 , 0                             , 413, 1 , CONTROL(None)   , SINGLE_REG(None)}, // #72 [ref=2x]
  { 0                                                 , 0                             , 414, 1 , CONTROL(None)   , SINGLE_REG(None)}, // #73 [ref=1x]
  { 0                                                 , 0                             , 39 , 10, CONTROL(None)   , SINGLE_REG(None)}, // #74 [ref=1x]
  { 0                                                 , 0                             , 415, 1 , CONTROL(None)   , SINGLE_REG(None)}, // #75 [ref=1x]
  { 0                                                 , 0                             , 416, 1 , CONTROL(None)   , SINGLE_REG(None)}, // #76 [ref=2x]
  { 0                                                 , 0                             , 343, 1 , CONTROL(None)   , SINGLE_REG(None)}, // #77 [ref=3x]
  { F(Rep)                                            , 0                             , 417, 1 , CONTROL(None)   , SINGLE_REG(None)}, // #78 [ref=1x]
  { F(Vec)                                            , 0                             , 296, 2 , CONTROL(None)   , SINGLE_REG(None)}, // #79 [ref=1x]
  { 0                                                 , 0                             , 418, 1 , CONTROL(None)   , SINGLE_REG(None)}, // #80 [ref=2x]
  { 0                                                 , 0                             , 419, 1 , CONTROL(None)   , SINGLE_REG(None)}, // #81 [ref=8x]
  { 0                                                 , 0                             , 298, 2 , CONTROL(None)   , SINGLE_REG(None)}, // #82 [ref=3x]
  { 0                                                 , 0                             , 300, 2 , CONTROL(None)   , SINGLE_REG(None)}, // #83 [ref=1x]
  { 0                                                 , 0                             , 108, 1 , CONTROL(Return) , SINGLE_REG(None)}, // #84 [ref=2x]
  { 0                                                 , 0                             , 388, 1 , CONTROL(Return) , SINGLE_REG(None)}, // #85 [ref=1x]
  { F(Rep)|F(RepIgnored)                              , 0                             , 302, 2 , CONTROL(Branch) , SINGLE_REG(None)}, // #86 [ref=30x]
  { F(Rep)|F(RepIgnored)                              , 0                             , 304, 2 , CONTROL(Branch) , SINGLE_REG(None)}, // #87 [ref=1x]
  { F(Rep)|F(RepIgnored)                              , 0                             , 306, 2 , CONTROL(Jump)   , SINGLE_REG(None)}, // #88 [ref=1x]
  { F(Vex)                                            , 0                             , 420, 1 , CONTROL(None)   , SINGLE_REG(None)}, // #89 [ref=19x]
  { F(Vex)                                            , 0                             , 308, 2 , CONTROL(None)   , SINGLE_REG(None)}, // #90 [ref=1x]
  { F(Vex)                                            , 0                             , 310, 2 , CONTROL(None)   , SINGLE_REG(None)}, // #91 [ref=1x]
  { F(Vex)                                            , 0                             , 312, 2 , CONTROL(None)   , SINGLE_REG(None)}, // #92 [ref=1x]
  { F(Vex)                                            , 0                             , 314, 2 , CONTROL(None)   , SINGLE_REG(None)}, // #93 [ref=1x]
  { F(Vex)                                            , 0                             , 421, 1 , CONTROL(None)   , SINGLE_REG(None)}, // #94 [ref=12x]
  { F(Vex)                                            , 0                             , 422, 1 , CONTROL(None)   , SINGLE_REG(None)}, // #95 [ref=8x]
  { F(Vex)                                            , 0                             , 420, 1 , CONTROL(None)   , SINGLE_REG(WO)  }, // #96 [ref=8x]
  { 0                                                 , 0                             , 423, 1 , CONTROL(None)   , SINGLE_REG(None)}, // #97 [ref=2x]
  { 0                                                 , 0                             , 316, 2 , CONTROL(None)   , SINGLE_REG(None)}, // #98 [ref=1x]
  { 0                                                 , 0                             , 318, 2 , CONTROL(Call)   , SINGLE_REG(None)}, // #99 [ref=1x]
  { F(Vec)                                            , 0                             , 224, 1 , CONTROL(None)   , SINGLE_REG(None)}, // #100 [ref=2x]
  { 0                                                 , 0                             , 424, 1 , CONTROL(None)   , SINGLE_REG(None)}, // #101 [ref=2x]
  { 0                                                 , 0                             , 320, 2 , CONTROL(None)   , SINGLE_REG(None)}, // #102 [ref=2x]
  { F(Vex)                                            , 0                             , 425, 1 , CONTROL(None)   , SINGLE_REG(None)}, // #103 [ref=2x]
  { 0                                                 , 0                             , 426, 1 , CONTROL(None)   , SINGLE_REG(None)}, // #104 [ref=1x]
  { 0                                                 , 0                             , 185, 3 , CONTROL(None)   , SINGLE_REG(None)}, // #105 [ref=3x]
  { 0                                                 , 0                             , 318, 2 , CONTROL(Jump)   , SINGLE_REG(None)}, // #106 [ref=1x]
  { 0                                                 , 0                             , 427, 1 , CONTROL(None)   , SINGLE_REG(None)}, // #107 [ref=5x]
  { F(Vex)                                            , 0                             , 428, 1 , CONTROL(None)   , SINGLE_REG(None)}, // #108 [ref=2x]
  { F(Rep)                                            , 0                             , 135, 4 , CONTROL(None)   , SINGLE_REG(None)}, // #109 [ref=1x]
  { 0                                                 , 0                             , 304, 2 , CONTROL(Branch) , SINGLE_REG(None)}, // #110 [ref=3x]
  { 0                                                 , 0                             , 322, 2 , CONTROL(None)   , SINGLE_REG(None)}, // #111 [ref=1x]
  { F(Vex)                                            , 0                             , 429, 1 , CONTROL(None)   , SINGLE_REG(None)}, // #112 [ref=2x]
  { F(Vec)                                            , 0                             , 430, 1 , CONTROL(None)   , SINGLE_REG(None)}, // #113 [ref=1x]
  { F(Mmx)                                            , 0                             , 431, 1 , CONTROL(None)   , SINGLE_REG(None)}, // #114 [ref=1x]
  { 0                                                 , 0                             , 432, 1 , CONTROL(None)   , SINGLE_REG(None)}, // #115 [ref=2x]
  { F(XRelease)                                       , 0                             , 0  , 16, CONTROL(None)   , SINGLE_REG(None)}, // #116 [ref=1x]
  { 0                                                 , 0                             , 49 , 9 , CONTROL(None)   , SINGLE_REG(None)}, // #117 [ref=1x]
  { F(Vec)                                            , 0                             , 79 , 2 , CONTROL(None)   , SINGLE_REG(None)}, // #118 [ref=6x]
  { 0                                                 , 0                             , 73 , 6 , CONTROL(None)   , SINGLE_REG(None)}, // #119 [ref=1x]
  { F(Mmx)|F(Vec)                                     , 0                             , 324, 2 , CONTROL(None)   , SINGLE_REG(None)}, // #120 [ref=1x]
  { 0                                                 , 0                             , 433, 1 , CONTROL(None)   , SINGLE_REG(None)}, // #121 [ref=1x]
  { 0                                                 , 0                             , 77 , 2 , CONTROL(None)   , SINGLE_REG(None)}, // #122 [ref=2x]
  { F(Mmx)|F(Vec)                                     , 0                             , 434, 1 , CONTROL(None)   , SINGLE_REG(None)}, // #123 [ref=1x]
  { F(Vec)                                            , 0                             , 291, 1 , CONTROL(None)   , SINGLE_REG(None)}, // #124 [ref=2x]
  { F(Vec)                                            , 0                             , 230, 2 , CONTROL(None)   , SINGLE_REG(None)}, // #125 [ref=4x]
  { F(Vec)                                            , 0                             , 435, 1 , CONTROL(None)   , SINGLE_REG(None)}, // #126 [ref=2x]
  { F(Vec)                                            , 0                             , 80 , 1 , CONTROL(None)   , SINGLE_REG(None)}, // #127 [ref=3x]
  { F(Mmx)                                            , 0                             , 436, 1 , CONTROL(None)   , SINGLE_REG(None)}, // #128 [ref=1x]
  { F(Vec)                                            , 0                             , 107, 1 , CONTROL(None)   , SINGLE_REG(None)}, // #129 [ref=1x]
  { F(Vec)                                            , 0                             , 233, 1 , CONTROL(None)   , SINGLE_REG(None)}, // #130 [ref=1x]
  { F(Mmx)|F(Vec)                                     , 0                             , 103, 5 , CONTROL(None)   , SINGLE_REG(None)}, // #131 [ref=1x]
  { F(Mmx)|F(Vec)                                     , 0                             , 437, 1 , CONTROL(None)   , SINGLE_REG(None)}, // #132 [ref=1x]
  { F(Rep)                                            , 0                             , 139, 4 , CONTROL(None)   , SINGLE_REG(None)}, // #133 [ref=1x]
  { F(Vec)                                            , 0                             , 106, 2 , CONTROL(None)   , SINGLE_REG(None)}, // #134 [ref=1x]
  { F(Vec)                                            , 0                             , 326, 2 , CONTROL(None)   , SINGLE_REG(None)}, // #135 [ref=1x]
  { 0                                                 , 0                             , 328, 2 , CONTROL(None)   , SINGLE_REG(None)}, // #136 [ref=2x]
  { 0                                                 , 0                             , 438, 1 , CONTROL(None)   , SINGLE_REG(None)}, // #137 [ref=1x]
  { F(Vex)                                            , 0                             , 330, 2 , CONTROL(None)   , SINGLE_REG(None)}, // #138 [ref=1x]
  { 0                                                 , 0                             , 439, 1 , CONTROL(None)   , SINGLE_REG(None)}, // #139 [ref=1x]
  { 0                                                 , 0                             , 440, 1 , CONTROL(None)   , SINGLE_REG(None)}, // #140 [ref=1x]
  { F(Lock)|F(XAcquire)|F(XRelease)                   , 0                             , 287, 1 , CONTROL(None)   , SINGLE_REG(None)}, // #141 [ref=2x]
  { 0                                                 , 0                             , 108, 5 , CONTROL(None)   , SINGLE_REG(None)}, // #142 [ref=1x]
  { F(Lock)|F(XAcquire)|F(XRelease)                   , 0                             , 16 , 12, CONTROL(None)   , SINGLE_REG(RO)  }, // #143 [ref=1x]
  { 0                                                 , 0                             , 441, 1 , CONTROL(None)   , SINGLE_REG(None)}, // #144 [ref=1x]
  { F(Rep)                                            , 0                             , 442, 1 , CONTROL(None)   , SINGLE_REG(None)}, // #145 [ref=1x]
  { F(Mmx)|F(Vec)                                     , 0                             , 332, 2 , CONTROL(None)   , SINGLE_REG(None)}, // #146 [ref=37x]
  { F(Mmx)|F(Vec)                                     , 0                             , 334, 2 , CONTROL(None)   , SINGLE_REG(None)}, // #147 [ref=1x]
  { F(Mmx)|F(Vec)                                     , 0                             , 332, 2 , CONTROL(None)   , SINGLE_REG(RO)  }, // #148 [ref=6x]
  { F(Mmx)|F(Vec)                                     , 0                             , 332, 2 , CONTROL(None)   , SINGLE_REG(WO)  }, // #149 [ref=16x]
  { F(Mmx)                                            , 0                             , 332, 1 , CONTROL(None)   , SINGLE_REG(None)}, // #150 [ref=26x]
  { F(Vec)                                            , 0                             , 79 , 1 , CONTROL(None)   , SINGLE_REG(WO)  }, // #151 [ref=4x]
  { F(Vec)                                            , 0                             , 443, 1 , CONTROL(None)   , SINGLE_REG(None)}, // #152 [ref=1x]
  { F(Vec)                                            , 0                             , 444, 1 , CONTROL(None)   , SINGLE_REG(None)}, // #153 [ref=1x]
  { F(Vec)                                            , 0                             , 445, 1 , CONTROL(None)   , SINGLE_REG(None)}, // #154 [ref=1x]
  { F(Vec)                                            , 0                             , 446, 1 , CONTROL(None)   , SINGLE_REG(None)}, // #155 [ref=1x]
  { F(Vec)                                            , 0                             , 447, 1 , CONTROL(None)   , SINGLE_REG(None)}, // #156 [ref=1x]
  { F(Vec)                                            , 0                             , 448, 1 , CONTROL(None)   , SINGLE_REG(None)}, // #157 [ref=1x]
  { F(Mmx)|F(Vec)                                     , 0                             , 336, 2 , CONTROL(None)   , SINGLE_REG(None)}, // #158 [ref=1x]
  { F(Vec)                                            , 0                             , 449, 1 , CONTROL(None)   , SINGLE_REG(None)}, // #159 [ref=1x]
  { F(Vec)                                            , 0                             , 450, 1 , CONTROL(None)   , SINGLE_REG(None)}, // #160 [ref=1x]
  { F(Vec)                                            , 0                             , 451, 1 , CONTROL(None)   , SINGLE_REG(None)}, // #161 [ref=1x]
  { F(Mmx)|F(Vec)                                     , 0                             , 452, 1 , CONTROL(None)   , SINGLE_REG(None)}, // #162 [ref=1x]
  { F(Mmx)|F(Vec)                                     , 0                             , 453, 1 , CONTROL(None)   , SINGLE_REG(None)}, // #163 [ref=1x]
  { F(Vec)                                            , 0                             , 260, 1 , CONTROL(None)   , SINGLE_REG(None)}, // #164 [ref=2x]
  { 0                                                 , 0                             , 143, 4 , CONTROL(None)   , SINGLE_REG(None)}, // #165 [ref=1x]
  { F(Mmx)                                            , 0                             , 334, 1 , CONTROL(None)   , SINGLE_REG(None)}, // #166 [ref=1x]
  { F(Mmx)|F(Vec)                                     , 0                             , 338, 2 , CONTROL(None)   , SINGLE_REG(None)}, // #167 [ref=8x]
  { F(Vec)                                            , 0                             , 454, 1 , CONTROL(None)   , SINGLE_REG(None)}, // #168 [ref=2x]
  { 0                                                 , 0                             , 455, 1 , CONTROL(None)   , SINGLE_REG(None)}, // #169 [ref=1x]
  { F(Mmx)|F(Vec)                                     , 0                             , 340, 2 , CONTROL(None)   , SINGLE_REG(None)}, // #170 [ref=3x]
  { 0                                                 , 0                             , 147, 4 , CONTROL(None)   , SINGLE_REG(None)}, // #171 [ref=1x]
  { 0                                                 , 0                             , 456, 1 , CONTROL(None)   , SINGLE_REG(None)}, // #172 [ref=8x]
  { 0                                                 , 0                             , 457, 1 , CONTROL(None)   , SINGLE_REG(None)}, // #173 [ref=4x]
  { 0                                                 , 0                             , 458, 1 , CONTROL(None)   , SINGLE_REG(None)}, // #174 [ref=8x]
  { 0                                                 , 0                             , 342, 2 , CONTROL(None)   , SINGLE_REG(None)}, // #175 [ref=1x]
  { F(Rep)|F(RepIgnored)                              , 0                             , 344, 2 , CONTROL(Return) , SINGLE_REG(None)}, // #176 [ref=1x]
  { 0                                                 , 0                             , 344, 2 , CONTROL(Return) , SINGLE_REG(None)}, // #177 [ref=1x]
  { F(Vex)                                            , 0                             , 346, 2 , CONTROL(None)   , SINGLE_REG(None)}, // #178 [ref=1x]
  { F(Lock)|F(XAcquire)|F(XRelease)                   , 0                             , 16 , 12, CONTROL(None)   , SINGLE_REG(WO)  }, // #179 [ref=3x]
  { F(Rep)                                            , 0                             , 151, 4 , CONTROL(None)   , SINGLE_REG(None)}, // #180 [ref=1x]
  { 0                                                 , 0                             , 459, 1 , CONTROL(None)   , SINGLE_REG(None)}, // #181 [ref=30x]
  { 0                                                 , 0                             , 188, 3 , CONTROL(None)   , SINGLE_REG(None)}, // #182 [ref=2x]
  { 0                                                 , 0                             , 460, 1 , CONTROL(None)   , SINGLE_REG(None)}, // #183 [ref=3x]
  { F(Rep)                                            , 0                             , 155, 4 , CONTROL(None)   , SINGLE_REG(None)}, // #184 [ref=1x]
  { F(Vex)                                            , 0                             , 461, 1 , CONTROL(None)   , SINGLE_REG(None)}, // #185 [ref=5x]
  { 0                                                 , 0                             , 66 , 7 , CONTROL(None)   , SINGLE_REG(None)}, // #186 [ref=1x]
  { F(Tsib)|F(Vex)                                    , 0                             , 462, 1 , CONTROL(None)   , SINGLE_REG(None)}, // #187 [ref=2x]
  { F(Vex)                                            , 0                             , 388, 1 , CONTROL(None)   , SINGLE_REG(None)}, // #188 [ref=1x]
  { F(Tsib)|F(Vex)                                    , 0                             , 463, 1 , CONTROL(None)   , SINGLE_REG(None)}, // #189 [ref=1x]
  { F(Vex)                                            , 0                             , 464, 1 , CONTROL(None)   , SINGLE_REG(None)}, // #190 [ref=1x]
  { 0                                                 , 0                             , 465, 1 , CONTROL(None)   , SINGLE_REG(None)}, // #191 [ref=2x]
  { 0                                                 , 0                             , 180, 1 , CONTROL(None)   , SINGLE_REG(None)}, // #192 [ref=2x]
  { 0                                                 , 0                             , 466, 1 , CONTROL(None)   , SINGLE_REG(None)}, // #193 [ref=1x]
  { F(Vec)|F(Evex)                                    , X(K)|X(Z)|X(T4X)              , 467, 1 , CONTROL(None)   , SINGLE_REG(None)}, // #194 [ref=4x]
  { F(Vec)|F(Evex)                                    , X(K)|X(Z)|X(T4X)              , 468, 1 , CONTROL(None)   , SINGLE_REG(None)}, // #195 [ref=2x]
  { F(Vec)|F(Vex)|F(Evex)|F(EvexCompat)               , X(K)|X(Z)|X(B64)|X(ER)|X(SAE) , 191, 3 , CONTROL(None)   , SINGLE_REG(None)}, // #196 [ref=22x]
  { F(Vec)|F(Vex)|F(Evex)|F(EvexCompat)               , X(K)|X(Z)|X(B32)|X(ER)|X(SAE) , 191, 3 , CONTROL(None)   , SINGLE_REG(None)}, // #197 [ref=22x]
  { F(Vec)|F(Vex)|F(Evex)|F(EvexCompat)               , X(K)|X(Z)|X(ER)|X(SAE)        , 469, 1 , CONTROL(None)   , SINGLE_REG(None)}, // #198 [ref=18x]
  { F(Vec)|F(Vex)|F(Evex)|F(EvexCompat)               , X(K)|X(Z)|X(ER)|X(SAE)        , 470, 1 , CONTROL(None)   , SINGLE_REG(None)}, // #199 [ref=17x]
  { F(Vec)|F(Vex)                                     , 0                             , 191, 2 , CONTROL(None)   , SINGLE_REG(None)}, // #200 [ref=15x]
  { F(Vec)|F(Vex)|F(Evex)|F(EvexCompat)               , 0                             , 191, 3 , CONTROL(None)   , SINGLE_REG(None)}, // #201 [ref=5x]
  { F(Vec)|F(Vex)                                     , 0                             , 79 , 1 , CONTROL(None)   , SINGLE_REG(None)}, // #202 [ref=17x]
  { F(Vec)|F(Vex)                                     , 0                             , 215, 1 , CONTROL(None)   , SINGLE_REG(None)}, // #203 [ref=1x]
  { F(Vec)|F(Evex)                                    , X(K)|X(Z)|X(B32)              , 194, 3 , CONTROL(None)   , SINGLE_REG(None)}, // #204 [ref=4x]
  { F(Vec)|F(Evex)                                    , X(K)|X(Z)|X(B64)              , 194, 3 , CONTROL(None)   , SINGLE_REG(None)}, // #205 [ref=4x]
  { F(Vec)|F(Vex)|F(Evex)|F(EvexCompat)               , X(K)|X(Z)|X(B64)              , 191, 3 , CONTROL(None)   , SINGLE_REG(None)}, // #206 [ref=10x]
  { F(Vec)|F(Vex)|F(Evex)|F(EvexCompat)               , X(K)|X(Z)|X(B32)              , 191, 3 , CONTROL(None)   , SINGLE_REG(None)}, // #207 [ref=12x]
  { F(Vec)|F(Vex)|F(Evex)|F(EvexCompat)               , X(K)|X(Z)|X(B64)              , 191, 3 , CONTROL(None)   , SINGLE_REG(RO)  }, // #208 [ref=2x]
  { F(Vec)|F(Vex)|F(Evex)|F(EvexCompat)               , X(K)|X(Z)|X(B32)              , 191, 3 , CONTROL(None)   , SINGLE_REG(RO)  }, // #209 [ref=6x]
  { F(Vec)|F(Evex)                                    , X(K)|X(Z)|X(B64)              , 191, 3 , CONTROL(None)   , SINGLE_REG(None)}, // #210 [ref=19x]
  { F(Vec)|F(Evex)                                    , X(K)|X(Z)|X(B32)              , 191, 3 , CONTROL(None)   , SINGLE_REG(None)}, // #211 [ref=12x]
  { F(Vec)|F(Vex)                                     , 0                             , 194, 2 , CONTROL(None)   , SINGLE_REG(None)}, // #212 [ref=6x]
  { F(Vec)|F(Vex)                                     , 0                             , 348, 2 , CONTROL(None)   , SINGLE_REG(None)}, // #213 [ref=3x]
  { F(Vec)|F(Vex)|F(EvexTransformable)                , 0                             , 471, 1 , CONTROL(None)   , SINGLE_REG(None)}, // #214 [ref=2x]
  { F(Vec)|F(Evex)                                    , X(K)|X(Z)                     , 472, 1 , CONTROL(None)   , SINGLE_REG(None)}, // #215 [ref=1x]
  { F(Vec)|F(Evex)                                    , X(K)|X(Z)                     , 473, 1 , CONTROL(None)   , SINGLE_REG(None)}, // #216 [ref=4x]
  { F(Vec)|F(Evex)                                    , X(K)|X(Z)                     , 474, 1 , CONTROL(None)   , SINGLE_REG(None)}, // #217 [ref=4x]
  { F(Vec)|F(Evex)                                    , X(K)|X(Z)                     , 475, 1 , CONTROL(None)   , SINGLE_REG(None)}, // #218 [ref=1x]
  { F(Vec)|F(Vex)|F(Evex)|F(EvexCompat)               , X(K)|X(Z)                     , 472, 1 , CONTROL(None)   , SINGLE_REG(None)}, // #219 [ref=1x]
  { F(Vec)|F(Vex)|F(Evex)|F(EvexCompat)               , X(K)|X(Z)                     , 476, 1 , CONTROL(None)   , SINGLE_REG(None)}, // #220 [ref=1x]
  { F(Vec)|F(Vex)|F(Evex)|F(EvexKReg)                 , X(K)|X(Z)|X(B64)|X(SAE)       , 197, 3 , CONTROL(None)   , SINGLE_REG(None)}, // #221 [ref=1x]
  { F(Vec)|F(Vex)|F(Evex)|F(EvexKReg)                 , X(K)|X(Z)|X(B32)|X(SAE)       , 197, 3 , CONTROL(None)   , SINGLE_REG(None)}, // #222 [ref=1x]
  { F(Vec)|F(Vex)|F(Evex)|F(EvexKReg)                 , X(K)|X(Z)|X(SAE)              , 477, 1 , CONTROL(None)   , SINGLE_REG(None)}, // #223 [ref=1x]
  { F(Vec)|F(Vex)|F(Evex)|F(EvexKReg)                 , X(K)|X(Z)|X(SAE)              , 478, 1 , CONTROL(None)   , SINGLE_REG(None)}, // #224 [ref=1x]
  { F(Vec)|F(Vex)|F(Evex)|F(EvexCompat)               , X(SAE)                        , 106, 1 , CONTROL(None)   , SINGLE_REG(None)}, // #225 [ref=2x]
  { F(Vec)|F(Vex)|F(Evex)|F(EvexCompat)               , X(SAE)                        , 257, 1 , CONTROL(None)   , SINGLE_REG(None)}, // #226 [ref=2x]
  { F(Vec)|F(Evex)                                    , X(K)|X(Z)                     , 200, 3 , CONTROL(None)   , SINGLE_REG(None)}, // #227 [ref=6x]
  { F(Vec)|F(Vex)|F(Evex)|F(EvexCompat)               , X(K)|X(Z)|X(B32)              , 203, 3 , CONTROL(None)   , SINGLE_REG(None)}, // #228 [ref=1x]
  { F(Vec)|F(Vex)|F(Evex)|F(EvexCompat)               , X(K)|X(Z)|X(B32)|X(ER)|X(SAE) , 206, 3 , CONTROL(None)   , SINGLE_REG(None)}, // #229 [ref=3x]
  { F(Vec)|F(Evex)                                    , X(K)|X(Z)|X(B32)              , 350, 2 , CONTROL(None)   , SINGLE_REG(None)}, // #230 [ref=1x]
  { F(Vec)|F(Vex)|F(Evex)|F(EvexCompat)               , X(K)|X(Z)|X(B64)|X(ER)|X(SAE) , 350, 2 , CONTROL(None)   , SINGLE_REG(None)}, // #231 [ref=2x]
  { F(Vec)|F(Evex)                                    , X(K)|X(Z)|X(B64)|X(ER)|X(SAE) , 206, 3 , CONTROL(None)   , SINGLE_REG(None)}, // #232 [ref=4x]
  { F(Vec)|F(Evex)                                    , X(K)|X(Z)|X(B64)|X(ER)|X(SAE) , 350, 2 , CONTROL(None)   , SINGLE_REG(None)}, // #233 [ref=3x]
  { F(Vec)|F(Vex)|F(Evex)|F(EvexCompat)               , X(K)|X(Z)|X(SAE)              , 203, 3 , CONTROL(None)   , SINGLE_REG(None)}, // #234 [ref=1x]
  { F(Vec)|F(Vex)|F(Evex)|F(EvexCompat)               , X(K)|X(Z)|X(B32)|X(ER)|X(SAE) , 203, 3 , CONTROL(None)   , SINGLE_REG(None)}, // #235 [ref=1x]
  { F(Vec)|F(Vex)|F(Evex)|F(EvexCompat)               , X(K)|X(Z)|X(SAE)              , 209, 3 , CONTROL(None)   , SINGLE_REG(None)}, // #236 [ref=1x]
  { F(Vec)|F(Evex)                                    , X(K)|X(Z)|X(B32)|X(ER)|X(SAE) , 203, 3 , CONTROL(None)   , SINGLE_REG(None)}, // #237 [ref=2x]
  { F(Vec)|F(Evex)                                    , X(K)|X(Z)|X(B32)|X(ER)|X(SAE) , 206, 3 , CONTROL(None)   , SINGLE_REG(None)}, // #238 [ref=2x]
  { F(Vec)|F(Vex)|F(Evex)|F(EvexCompat)               , X(ER)|X(SAE)                  , 399, 1 , CONTROL(None)   , SINGLE_REG(None)}, // #239 [ref=1x]
  { F(Vec)|F(Evex)                                    , X(ER)|X(SAE)                  , 399, 1 , CONTROL(None)   , SINGLE_REG(None)}, // #240 [ref=1x]
  { F(Vec)|F(Vex)|F(Evex)|F(EvexCompat)               , X(ER)|X(SAE)                  , 479, 1 , CONTROL(None)   , SINGLE_REG(None)}, // #241 [ref=2x]
  { F(Vec)|F(Vex)|F(Evex)|F(EvexCompat)               , X(K)|X(Z)|X(SAE)              , 470, 1 , CONTROL(None)   , SINGLE_REG(None)}, // #242 [ref=3x]
  { F(Vec)|F(Vex)|F(Evex)|F(EvexCompat)               , X(ER)|X(SAE)                  , 401, 1 , CONTROL(None)   , SINGLE_REG(None)}, // #243 [ref=1x]
  { F(Vec)|F(Evex)                                    , X(ER)|X(SAE)                  , 401, 1 , CONTROL(None)   , SINGLE_REG(None)}, // #244 [ref=1x]
  { F(Vec)|F(Vex)|F(Evex)|F(EvexCompat)               , X(K)|X(Z)|X(B64)|X(SAE)       , 350, 2 , CONTROL(None)   , SINGLE_REG(None)}, // #245 [ref=1x]
  { F(Vec)|F(Evex)                                    , X(K)|X(Z)|X(B64)|X(SAE)       , 206, 3 , CONTROL(None)   , SINGLE_REG(None)}, // #246 [ref=3x]
  { F(Vec)|F(Evex)                                    , X(K)|X(Z)|X(B64)|X(SAE)       , 350, 2 , CONTROL(None)   , SINGLE_REG(None)}, // #247 [ref=1x]
  { F(Vec)|F(Vex)|F(Evex)|F(EvexCompat)               , X(K)|X(Z)|X(B32)|X(SAE)       , 206, 3 , CONTROL(None)   , SINGLE_REG(None)}, // #248 [ref=1x]
  { F(Vec)|F(Evex)                                    , X(K)|X(Z)|X(B32)|X(SAE)       , 203, 3 , CONTROL(None)   , SINGLE_REG(None)}, // #249 [ref=2x]
  { F(Vec)|F(Evex)                                    , X(K)|X(Z)|X(B32)|X(SAE)       , 206, 3 , CONTROL(None)   , SINGLE_REG(None)}, // #250 [ref=2x]
  { F(Vec)|F(Vex)|F(Evex)|F(EvexCompat)               , X(SAE)                        , 399, 1 , CONTROL(None)   , SINGLE_REG(None)}, // #251 [ref=1x]
  { F(Vec)|F(Evex)                                    , X(SAE)                        , 399, 1 , CONTROL(None)   , SINGLE_REG(None)}, // #252 [ref=1x]
  { F(Vec)|F(Vex)|F(Evex)|F(EvexCompat)               , X(SAE)                        , 401, 1 , CONTROL(None)   , SINGLE_REG(None)}, // #253 [ref=1x]
  { F(Vec)|F(Evex)                                    , X(SAE)                        , 401, 1 , CONTROL(None)   , SINGLE_REG(None)}, // #254 [ref=1x]
  { F(Vec)|F(Evex)                                    , X(K)|X(Z)|X(B32)              , 203, 3 , CONTROL(None)   , SINGLE_REG(None)}, // #255 [ref=1x]
  { F(Vec)|F(Evex)                                    , X(ER)|X(SAE)                  , 479, 1 , CONTROL(None)   , SINGLE_REG(None)}, // #256 [ref=2x]
  { F(Vec)|F(Evex)                                    , X(K)|X(Z)                     , 194, 3 , CONTROL(None)   , SINGLE_REG(None)}, // #257 [ref=3x]
  { F(Vec)|F(Vex)                                     , 0                             , 194, 1 , CONTROL(None)   , SINGLE_REG(None)}, // #258 [ref=9x]
  { F(Vec)|F(Evex)                                    , X(K)|X(Z)|X(SAE)|X(B64)       , 83 , 1 , CONTROL(None)   , SINGLE_REG(None)}, // #259 [ref=3x]
  { F(Vec)|F(Evex)                                    , X(K)|X(Z)|X(SAE)|X(B32)       , 83 , 1 , CONTROL(None)   , SINGLE_REG(None)}, // #260 [ref=3x]
  { F(Vec)|F(Evex)                                    , X(K)|X(Z)                     , 206, 3 , CONTROL(None)   , SINGLE_REG(None)}, // #261 [ref=9x]
  { F(Vec)|F(Vex)|F(EvexTransformable)                , 0                             , 210, 1 , CONTROL(None)   , SINGLE_REG(None)}, // #262 [ref=2x]
  { F(Vec)|F(Evex)                                    , X(K)|X(Z)                     , 480, 1 , CONTROL(None)   , SINGLE_REG(None)}, // #263 [ref=4x]
  { F(Vec)|F(Evex)                                    , X(K)|X(Z)                     , 211, 1 , CONTROL(None)   , SINGLE_REG(None)}, // #264 [ref=4x]
  { F(Vec)|F(Vex)|F(Evex)|F(EvexCompat)               , 0                             , 405, 1 , CONTROL(None)   , SINGLE_REG(None)}, // #265 [ref=2x]
  { F(Vec)|F(Evex)                                    , X(K)|X(Z)|X(B64)|X(SAE)       , 194, 3 , CONTROL(None)   , SINGLE_REG(None)}, // #266 [ref=2x]
  { F(Vec)|F(Evex)                                    , X(K)|X(Z)|X(B32)|X(SAE)       , 194, 3 , CONTROL(None)   , SINGLE_REG(None)}, // #267 [ref=2x]
  { F(Vec)|F(Evex)                                    , X(K)|X(Z)|X(SAE)              , 481, 1 , CONTROL(None)   , SINGLE_REG(None)}, // #268 [ref=4x]
  { F(Vec)|F(Evex)                                    , X(K)|X(Z)|X(SAE)              , 482, 1 , CONTROL(None)   , SINGLE_REG(None)}, // #269 [ref=4x]
  { F(Vec)|F(Vex)                                     , 0                             , 159, 4 , CONTROL(None)   , SINGLE_REG(None)}, // #270 [ref=13x]
  { F(Vec)|F(Vex)                                     , 0                             , 352, 2 , CONTROL(None)   , SINGLE_REG(None)}, // #271 [ref=4x]
  { F(Vec)|F(Vex)                                     , 0                             , 354, 2 , CONTROL(None)   , SINGLE_REG(None)}, // #272 [ref=4x]
  { F(Vec)|F(Evex)                                    , X(K)|X(B64)                   , 483, 1 , CONTROL(None)   , SINGLE_REG(None)}, // #273 [ref=1x]
  { F(Vec)|F(Evex)                                    , X(K)|X(B32)                   , 483, 1 , CONTROL(None)   , SINGLE_REG(None)}, // #274 [ref=1x]
  { F(Vec)|F(Evex)                                    , X(K)                          , 484, 1 , CONTROL(None)   , SINGLE_REG(None)}, // #275 [ref=1x]
  { F(Vec)|F(Evex)                                    , X(K)                          , 485, 1 , CONTROL(None)   , SINGLE_REG(None)}, // #276 [ref=1x]
  { F(Vec)|F(Vex)                                     , 0                             , 206, 2 , CONTROL(None)   , SINGLE_REG(None)}, // #277 [ref=7x]
  { F(Vec)|F(Vex)                                     , 0                             , 106, 1 , CONTROL(None)   , SINGLE_REG(None)}, // #278 [ref=1x]
  { F(Vec)|F(Vex)                                     , 0                             , 257, 1 , CONTROL(None)   , SINGLE_REG(None)}, // #279 [ref=1x]
  { F(Vec)|F(Vsib)|F(Vex)|F(Evex)|F(EvexTwoOp)        , X(K)                          , 163, 4 , CONTROL(None)   , SINGLE_REG(None)}, // #280 [ref=2x]
  { F(Vec)|F(Vsib)|F(Vex)|F(Evex)|F(EvexTwoOp)        , X(K)                          , 113, 5 , CONTROL(None)   , SINGLE_REG(None)}, // #281 [ref=2x]
  { F(Vsib)|F(Evex)                                   , X(K)                          , 486, 1 , CONTROL(None)   , SINGLE_REG(None)}, // #282 [ref=4x]
  { F(Vsib)|F(Evex)                                   , X(K)                          , 487, 1 , CONTROL(None)   , SINGLE_REG(None)}, // #283 [ref=4x]
  { F(Vsib)|F(Evex)                                   , X(K)                          , 488, 1 , CONTROL(None)   , SINGLE_REG(None)}, // #284 [ref=8x]
  { F(Vec)|F(Vsib)|F(Vex)|F(Evex)|F(EvexTwoOp)        , X(K)                          , 118, 5 , CONTROL(None)   , SINGLE_REG(None)}, // #285 [ref=2x]
  { F(Vec)|F(Vsib)|F(Vex)|F(Evex)|F(EvexTwoOp)        , X(K)                          , 212, 3 , CONTROL(None)   , SINGLE_REG(None)}, // #286 [ref=2x]
  { F(Vec)|F(Evex)                                    , X(K)|X(Z)|X(SAE)              , 469, 1 , CONTROL(None)   , SINGLE_REG(None)}, // #287 [ref=3x]
  { F(Vec)|F(Evex)                                    , X(K)|X(Z)|X(SAE)              , 470, 1 , CONTROL(None)   , SINGLE_REG(None)}, // #288 [ref=3x]
  { F(Vec)|F(Evex)                                    , X(K)|X(Z)|X(B64)|X(SAE)       , 215, 3 , CONTROL(None)   , SINGLE_REG(None)}, // #289 [ref=2x]
  { F(Vec)|F(Evex)                                    , X(K)|X(Z)|X(B32)|X(SAE)       , 215, 3 , CONTROL(None)   , SINGLE_REG(None)}, // #290 [ref=2x]
  { F(Vec)|F(Vex)|F(Evex)|F(EvexCompat)               , X(K)|X(Z)                     , 194, 3 , CONTROL(None)   , SINGLE_REG(None)}, // #291 [ref=3x]
  { F(Vec)|F(Vex)|F(Evex)|F(EvexCompat)               , X(K)|X(Z)                     , 191, 3 , CONTROL(None)   , SINGLE_REG(None)}, // #292 [ref=22x]
  { F(Vec)|F(Vex)|F(EvexTransformable)                , 0                             , 356, 1 , CONTROL(None)   , SINGLE_REG(None)}, // #293 [ref=2x]
  { F(Vec)|F(Evex)                                    , X(K)|X(Z)                     , 356, 2 , CONTROL(None)   , SINGLE_REG(None)}, // #294 [ref=4x]
  { F(Vec)|F(Evex)                                    , X(K)|X(Z)                     , 489, 1 , CONTROL(None)   , SINGLE_REG(None)}, // #295 [ref=4x]
  { F(Vec)|F(Vex)|F(Evex)|F(EvexCompat)               , 0                             , 482, 1 , CONTROL(None)   , SINGLE_REG(None)}, // #296 [ref=1x]
  { F(Vec)|F(Vex)                                     , 0                             , 224, 2 , CONTROL(None)   , SINGLE_REG(None)}, // #297 [ref=1x]
  { F(Vex)                                            , 0                             , 424, 1 , CONTROL(None)   , SINGLE_REG(None)}, // #298 [ref=2x]
  { F(Vec)|F(Vex)                                     , 0                             , 430, 1 , CONTROL(None)   , SINGLE_REG(None)}, // #299 [ref=1x]
  { F(Vec)|F(Vex)                                     , 0                             , 167, 4 , CONTROL(None)   , SINGLE_REG(None)}, // #300 [ref=4x]
  { F(Vec)|F(Vex)|F(Evex)|F(EvexCompat)               , X(K)|X(Z)|X(B64)|X(SAE)       , 191, 3 , CONTROL(None)   , SINGLE_REG(None)}, // #301 [ref=2x]
  { F(Vec)|F(Vex)|F(Evex)|F(EvexCompat)               , X(K)|X(Z)|X(B32)|X(SAE)       , 191, 3 , CONTROL(None)   , SINGLE_REG(None)}, // #302 [ref=2x]
  { F(Vec)|F(Vex)|F(Evex)|F(EvexCompat)               , X(K)|X(Z)|X(SAE)              , 469, 1 , CONTROL(None)   , SINGLE_REG(None)}, // #303 [ref=2x]
  { 0                                                 , 0                             , 358, 2 , CONTROL(None)   , SINGLE_REG(None)}, // #304 [ref=3x]
  { F(Vec)|F(Vex)|F(Evex)|F(EvexCompat)               , X(K)|X(Z)                     , 79 , 6 , CONTROL(None)   , SINGLE_REG(None)}, // #305 [ref=4x]
  { F(Vec)|F(Vex)|F(Evex)|F(EvexCompat)               , 0                             , 360, 2 , CONTROL(None)   , SINGLE_REG(None)}, // #306 [ref=1x]
  { F(Vec)|F(Vex)|F(Evex)|F(EvexCompat)               , X(K)|X(Z)                     , 218, 3 , CONTROL(None)   , SINGLE_REG(None)}, // #307 [ref=1x]
  { F(Vec)|F(Vex)|F(EvexTransformable)                , 0                             , 79 , 4 , CONTROL(None)   , SINGLE_REG(None)}, // #308 [ref=2x]
  { F(Vec)|F(Evex)                                    , X(K)|X(Z)                     , 79 , 6 , CONTROL(None)   , SINGLE_REG(None)}, // #309 [ref=6x]
  { F(Vec)|F(Vex)|F(Evex)|F(EvexCompat)               , 0                             , 232, 1 , CONTROL(None)   , SINGLE_REG(None)}, // #310 [ref=2x]
  { F(Vec)|F(Vex)|F(Evex)|F(EvexCompat)               , 0                             , 362, 2 , CONTROL(None)   , SINGLE_REG(None)}, // #311 [ref=4x]
  { F(Vec)|F(Vex)                                     , 0                             , 490, 1 , CONTROL(None)   , SINGLE_REG(None)}, // #312 [ref=3x]
  { F(Vec)|F(Vex)|F(Evex)|F(EvexCompat)               , 0                             , 221, 3 , CONTROL(None)   , SINGLE_REG(None)}, // #313 [ref=3x]
  { F(Vec)|F(Vex)|F(Evex)|F(EvexCompat)               , 0                             , 224, 3 , CONTROL(None)   , SINGLE_REG(None)}, // #314 [ref=1x]
  { F(Vec)|F(Vex)|F(Evex)|F(EvexCompat)               , 0                             , 227, 3 , CONTROL(None)   , SINGLE_REG(None)}, // #315 [ref=1x]
  { F(Vec)|F(Vex)|F(Evex)|F(EvexCompat)               , X(K)|X(Z)                     , 230, 3 , CONTROL(None)   , SINGLE_REG(None)}, // #316 [ref=1x]
  { F(Vec)|F(Vex)|F(Evex)|F(EvexCompat)               , X(K)|X(Z)                     , 206, 3 , CONTROL(None)   , SINGLE_REG(None)}, // #317 [ref=5x]
  { F(Vec)|F(Vex)|F(Evex)|F(EvexCompat)               , X(K)|X(Z)                     , 233, 3 , CONTROL(None)   , SINGLE_REG(None)}, // #318 [ref=1x]
  { 0                                                 , 0                             , 364, 2 , CONTROL(None)   , SINGLE_REG(None)}, // #319 [ref=1x]
  { 0                                                 , 0                             , 366, 2 , CONTROL(None)   , SINGLE_REG(None)}, // #320 [ref=1x]
  { F(Vec)|F(Evex)                                    , X(B32)                        , 236, 3 , CONTROL(None)   , SINGLE_REG(None)}, // #321 [ref=1x]
  { F(Vec)|F(Evex)                                    , X(B64)                        , 236, 3 , CONTROL(None)   , SINGLE_REG(None)}, // #322 [ref=1x]
  { F(Vec)|F(Vex)|F(EvexTransformable)                , 0                             , 191, 2 , CONTROL(None)   , SINGLE_REG(RO)  }, // #323 [ref=2x]
  { F(Vec)|F(Evex)                                    , X(K)|X(Z)|X(B32)              , 191, 3 , CONTROL(None)   , SINGLE_REG(RO)  }, // #324 [ref=2x]
  { F(Vec)|F(Vex)|F(EvexTransformable)                , 0                             , 191, 2 , CONTROL(None)   , SINGLE_REG(WO)  }, // #325 [ref=2x]
  { F(Vec)|F(Evex)                                    , X(K)|X(Z)|X(B32)              , 191, 3 , CONTROL(None)   , SINGLE_REG(WO)  }, // #326 [ref=2x]
  { F(Vec)|F(Evex)                                    , X(K)|X(Z)|X(B64)              , 191, 3 , CONTROL(None)   , SINGLE_REG(WO)  }, // #327 [ref=2x]
  { F(Vec)|F(Evex)                                    , X(K)|X(Z)|X(B64)              , 191, 3 , CONTROL(None)   , SINGLE_REG(RO)  }, // #328 [ref=2x]
  { F(Vec)|F(Evex)                                    , X(K)|X(Z)                     , 191, 3 , CONTROL(None)   , SINGLE_REG(None)}, // #329 [ref=13x]
  { F(Vec)|F(Vex)|F(Evex)|F(EvexCompat)               , X(K)|X(Z)                     , 491, 1 , CONTROL(None)   , SINGLE_REG(None)}, // #330 [ref=1x]
  { F(Vec)|F(Vex)|F(Evex)|F(EvexCompat)               , X(K)|X(Z)                     , 492, 1 , CONTROL(None)   , SINGLE_REG(None)}, // #331 [ref=1x]
  { F(Vec)|F(Evex)                                    , 0                             , 493, 1 , CONTROL(None)   , SINGLE_REG(None)}, // #332 [ref=6x]
  { F(Vec)|F(Vex)|F(Evex)|F(EvexCompat)               , X(K)|X(Z)                     , 239, 3 , CONTROL(None)   , SINGLE_REG(None)}, // #333 [ref=1x]
  { F(Vec)|F(Vex)|F(Evex)|F(EvexCompat)               , X(K)|X(Z)                     , 494, 1 , CONTROL(None)   , SINGLE_REG(None)}, // #334 [ref=1x]
  { F(Vec)|F(Vex)|F(Evex)|F(EvexCompat)               , 0                             , 194, 3 , CONTROL(None)   , SINGLE_REG(None)}, // #335 [ref=1x]
  { F(Vec)|F(Evex)                                    , X(K)                          , 242, 3 , CONTROL(None)   , SINGLE_REG(WO)  }, // #336 [ref=2x]
  { F(Vec)|F(Evex)                                    , X(K)|X(B32)                   , 242, 3 , CONTROL(None)   , SINGLE_REG(WO)  }, // #337 [ref=2x]
  { F(Vec)|F(Vex)|F(Evex)|F(EvexKReg)                 , X(K)                          , 245, 3 , CONTROL(None)   , SINGLE_REG(WO)  }, // #338 [ref=4x]
  { F(Vec)|F(Vex)|F(Evex)|F(EvexKReg)                 , X(K)|X(B32)                   , 245, 3 , CONTROL(None)   , SINGLE_REG(WO)  }, // #339 [ref=2x]
  { F(Vec)|F(Vex)|F(Evex)|F(EvexKReg)                 , X(K)|X(B64)                   , 245, 3 , CONTROL(None)   , SINGLE_REG(WO)  }, // #340 [ref=2x]
  { F(Vec)|F(Vex)                                     , 0                             , 443, 1 , CONTROL(None)   , SINGLE_REG(None)}, // #341 [ref=1x]
  { F(Vec)|F(Vex)                                     , 0                             , 444, 1 , CONTROL(None)   , SINGLE_REG(None)}, // #342 [ref=1x]
  { F(Vec)|F(Vex)                                     , 0                             , 445, 1 , CONTROL(None)   , SINGLE_REG(None)}, // #343 [ref=1x]
  { F(Vec)|F(Vex)                                     , 0                             , 446, 1 , CONTROL(None)   , SINGLE_REG(None)}, // #344 [ref=1x]
  { F(Vec)|F(Evex)                                    , X(K)|X(B64)                   , 242, 3 , CONTROL(None)   , SINGLE_REG(WO)  }, // #345 [ref=4x]
  { F(Vec)|F(Evex)                                    , X(K)|X(Z)|X(B32)              , 206, 3 , CONTROL(None)   , SINGLE_REG(None)}, // #346 [ref=6x]
  { F(Vec)|F(Vex)|F(Evex)|F(PreferEvex)|F(EvexCompat) , X(K)|X(Z)|X(B32)              , 191, 3 , CONTROL(None)   , SINGLE_REG(None)}, // #347 [ref=4x]
  { F(Vec)|F(Vex)                                     , 0                             , 195, 1 , CONTROL(None)   , SINGLE_REG(None)}, // #348 [ref=2x]
  { F(Vec)|F(Vex)|F(Evex)|F(EvexCompat)               , X(K)|X(Z)|X(B32)              , 192, 2 , CONTROL(None)   , SINGLE_REG(None)}, // #349 [ref=2x]
  { F(Vec)|F(Vex)                                     , 0                             , 171, 4 , CONTROL(None)   , SINGLE_REG(None)}, // #350 [ref=2x]
  { F(Vec)|F(Vex)|F(Evex)|F(EvexCompat)               , X(K)|X(Z)|X(B64)              , 85 , 6 , CONTROL(None)   , SINGLE_REG(None)}, // #351 [ref=2x]
  { F(Vec)|F(Vex)|F(Evex)|F(EvexCompat)               , X(K)|X(Z)|X(B64)              , 175, 4 , CONTROL(None)   , SINGLE_REG(None)}, // #352 [ref=2x]
  { F(Vec)|F(Vex)|F(Evex)|F(EvexCompat)               , 0                             , 447, 1 , CONTROL(None)   , SINGLE_REG(None)}, // #353 [ref=1x]
  { F(Vec)|F(Vex)|F(Evex)|F(EvexCompat)               , 0                             , 448, 1 , CONTROL(None)   , SINGLE_REG(None)}, // #354 [ref=1x]
  { F(Vec)|F(Vex)|F(Evex)|F(EvexCompat)               , 0                             , 495, 1 , CONTROL(None)   , SINGLE_REG(None)}, // #355 [ref=1x]
  { F(Vec)|F(Vex)|F(Evex)|F(EvexCompat)               , X(K)|X(Z)                     , 496, 1 , CONTROL(None)   , SINGLE_REG(None)}, // #356 [ref=1x]
  { F(Vec)|F(Vex)|F(Evex)|F(EvexCompat)               , X(K)|X(Z)                     , 497, 1 , CONTROL(None)   , SINGLE_REG(None)}, // #357 [ref=1x]
  { F(Vec)|F(Vex)|F(Evex)|F(EvexCompat)               , X(K)|X(Z)                     , 498, 1 , CONTROL(None)   , SINGLE_REG(None)}, // #358 [ref=1x]
  { F(Vec)|F(Vex)|F(Evex)|F(EvexCompat)               , X(K)|X(Z)                     , 499, 1 , CONTROL(None)   , SINGLE_REG(None)}, // #359 [ref=1x]
  { F(Vec)|F(Evex)                                    , X(K)|X(Z)|X(B64)              , 206, 3 , CONTROL(None)   , SINGLE_REG(None)}, // #360 [ref=4x]
  { F(Vec)|F(Vex)                                     , 0                             , 348, 1 , CONTROL(None)   , SINGLE_REG(None)}, // #361 [ref=12x]
  { F(Vec)|F(Vex)|F(Evex)|F(EvexCompat)               , X(K)|X(Z)                     , 191, 3 , CONTROL(None)   , SINGLE_REG(RO)  }, // #362 [ref=8x]
  { F(Vec)|F(Evex)                                    , 0                             , 500, 1 , CONTROL(None)   , SINGLE_REG(None)}, // #363 [ref=4x]
  { F(Vec)|F(Evex)                                    , X(K)|X(Z)                     , 248, 3 , CONTROL(None)   , SINGLE_REG(None)}, // #364 [ref=6x]
  { F(Vec)|F(Evex)                                    , X(K)|X(Z)                     , 251, 3 , CONTROL(None)   , SINGLE_REG(None)}, // #365 [ref=9x]
  { F(Vec)|F(Evex)                                    , X(K)|X(Z)                     , 254, 3 , CONTROL(None)   , SINGLE_REG(None)}, // #366 [ref=3x]
  { F(Vec)|F(Vex)|F(Evex)|F(EvexCompat)               , X(K)|X(Z)                     , 257, 3 , CONTROL(None)   , SINGLE_REG(None)}, // #367 [ref=4x]
  { F(Vec)|F(Vex)|F(Evex)|F(EvexCompat)               , X(K)|X(Z)                     , 260, 3 , CONTROL(None)   , SINGLE_REG(None)}, // #368 [ref=2x]
  { F(Vec)|F(Vex)|F(Evex)|F(EvexCompat)               , X(K)|X(Z)                     , 203, 3 , CONTROL(None)   , SINGLE_REG(None)}, // #369 [ref=6x]
  { F(Vec)|F(Vex)                                     , 0                             , 159, 2 , CONTROL(None)   , SINGLE_REG(None)}, // #370 [ref=1x]
  { F(Vec)|F(Evex)                                    , X(K)|X(Z)|X(B32)              , 215, 3 , CONTROL(None)   , SINGLE_REG(None)}, // #371 [ref=3x]
  { F(Vec)|F(Evex)                                    , X(K)|X(Z)|X(B64)              , 215, 3 , CONTROL(None)   , SINGLE_REG(None)}, // #372 [ref=3x]
  { F(Vec)|F(Vex)                                     , 0                             , 368, 2 , CONTROL(None)   , SINGLE_REG(None)}, // #373 [ref=4x]
  { F(Vec)|F(Vsib)|F(Evex)                            , X(K)                          , 263, 3 , CONTROL(None)   , SINGLE_REG(None)}, // #374 [ref=2x]
  { F(Vec)|F(Vsib)|F(Evex)                            , X(K)                          , 370, 2 , CONTROL(None)   , SINGLE_REG(None)}, // #375 [ref=2x]
  { F(Vec)|F(Vsib)|F(Evex)                            , X(K)                          , 372, 2 , CONTROL(None)   , SINGLE_REG(None)}, // #376 [ref=2x]
  { F(Vec)|F(Vsib)|F(Evex)                            , X(K)                          , 266, 3 , CONTROL(None)   , SINGLE_REG(None)}, // #377 [ref=2x]
  { F(Vec)|F(Vex)                                     , 0                             , 374, 2 , CONTROL(None)   , SINGLE_REG(None)}, // #378 [ref=8x]
  { F(Vec)|F(Evex)                                    , X(K)                          , 269, 3 , CONTROL(None)   , SINGLE_REG(None)}, // #379 [ref=5x]
  { F(Vec)|F(Vex)|F(Evex)|F(EvexCompat)               , X(K)|X(Z)|X(B32)              , 215, 3 , CONTROL(None)   , SINGLE_REG(None)}, // #380 [ref=1x]
  { F(Vec)|F(Vex)|F(Evex)|F(EvexCompat)               , X(K)|X(Z)                     , 215, 3 , CONTROL(None)   , SINGLE_REG(None)}, // #381 [ref=2x]
  { F(Vec)|F(Vex)|F(Evex)|F(EvexCompat)               , X(K)|X(Z)|X(B32)              , 91 , 6 , CONTROL(None)   , SINGLE_REG(None)}, // #382 [ref=3x]
  { F(Vec)|F(Vex)|F(Evex)|F(EvexCompat)               , 0                             , 215, 3 , CONTROL(None)   , SINGLE_REG(None)}, // #383 [ref=2x]
  { F(Vec)|F(Vex)|F(Evex)|F(EvexCompat)               , X(K)|X(Z)|X(B64)              , 91 , 6 , CONTROL(None)   , SINGLE_REG(None)}, // #384 [ref=2x]
  { F(Vec)|F(Vex)|F(Evex)|F(EvexCompat)               , X(K)|X(Z)                     , 91 , 6 , CONTROL(None)   , SINGLE_REG(None)}, // #385 [ref=3x]
  { F(Vec)|F(Evex)                                    , X(K)|X(Z)|X(B64)              , 97 , 6 , CONTROL(None)   , SINGLE_REG(None)}, // #386 [ref=1x]
  { F(Vec)|F(Vex)|F(Evex)|F(EvexCompat)               , X(K)|X(Z)                     , 191, 3 , CONTROL(None)   , SINGLE_REG(WO)  }, // #387 [ref=6x]
  { F(Vec)|F(Vex)|F(Evex)|F(EvexCompat)               , X(K)|X(Z)|X(B32)              , 191, 3 , CONTROL(None)   , SINGLE_REG(WO)  }, // #388 [ref=2x]
  { F(Vec)|F(Vex)|F(Evex)|F(EvexCompat)               , X(K)|X(Z)|X(B64)              , 191, 3 , CONTROL(None)   , SINGLE_REG(WO)  }, // #389 [ref=2x]
  { F(Vec)|F(Evex)                                    , X(K)|X(B32)                   , 269, 3 , CONTROL(None)   , SINGLE_REG(None)}, // #390 [ref=2x]
  { F(Vec)|F(Evex)                                    , X(K)|X(B64)                   , 269, 3 , CONTROL(None)   , SINGLE_REG(None)}, // #391 [ref=2x]
  { F(Vec)|F(Evex)                                    , X(K)|X(Z)                     , 469, 1 , CONTROL(None)   , SINGLE_REG(None)}, // #392 [ref=2x]
  { F(Vec)|F(Evex)                                    , X(K)|X(Z)                     , 470, 1 , CONTROL(None)   , SINGLE_REG(None)}, // #393 [ref=2x]
  { F(Vec)|F(Vex)                                     , 0                             , 470, 1 , CONTROL(None)   , SINGLE_REG(None)}, // #394 [ref=2x]
  { F(Vec)|F(Evex)                                    , X(K)|X(Z)                     , 481, 1 , CONTROL(None)   , SINGLE_REG(None)}, // #395 [ref=1x]
  { F(Vec)|F(Evex)                                    , X(K)|X(Z)                     , 482, 1 , CONTROL(None)   , SINGLE_REG(None)}, // #396 [ref=1x]
  { F(Vec)|F(Vex)                                     , 0                             , 215, 2 , CONTROL(None)   , SINGLE_REG(None)}, // #397 [ref=2x]
  { F(Vec)|F(Vex)                                     , 0                             , 481, 1 , CONTROL(None)   , SINGLE_REG(None)}, // #398 [ref=1x]
  { F(Vec)|F(Vex)                                     , 0                             , 482, 1 , CONTROL(None)   , SINGLE_REG(None)}, // #399 [ref=1x]
  { F(Vec)|F(Evex)                                    , X(K)|X(Z)|X(B64)|X(ER)|X(SAE) , 191, 3 , CONTROL(None)   , SINGLE_REG(None)}, // #400 [ref=1x]
  { F(Vec)|F(Evex)                                    , X(K)|X(Z)|X(B32)|X(ER)|X(SAE) , 191, 3 , CONTROL(None)   , SINGLE_REG(None)}, // #401 [ref=1x]
  { F(Vec)|F(Evex)                                    , X(K)|X(Z)|X(ER)|X(SAE)        , 469, 1 , CONTROL(None)   , SINGLE_REG(None)}, // #402 [ref=1x]
  { F(Vec)|F(Evex)                                    , X(K)|X(Z)|X(ER)|X(SAE)        , 470, 1 , CONTROL(None)   , SINGLE_REG(None)}, // #403 [ref=1x]
  { F(Vec)|F(Evex)                                    , X(K)|X(Z)|X(B32)              , 195, 2 , CONTROL(None)   , SINGLE_REG(None)}, // #404 [ref=2x]
  { F(Vec)|F(Evex)                                    , X(K)|X(Z)|X(B64)              , 195, 2 , CONTROL(None)   , SINGLE_REG(None)}, // #405 [ref=2x]
  { F(Vec)|F(Vex)|F(Evex)|F(EvexCompat)               , X(K)|X(Z)|X(B32)              , 194, 3 , CONTROL(None)   , SINGLE_REG(None)}, // #406 [ref=1x]
  { F(Vec)|F(Vex)|F(Evex)|F(EvexCompat)               , X(K)|X(Z)|X(B64)              , 194, 3 , CONTROL(None)   , SINGLE_REG(None)}, // #407 [ref=1x]
  { F(Vec)|F(Vex)|F(Evex)|F(EvexCompat)               , X(K)|X(Z)|X(B64)|X(ER)|X(SAE) , 206, 3 , CONTROL(None)   , SINGLE_REG(None)}, // #408 [ref=1x]
  { F(Vec)|F(Vex)                                     , 0                             , 108, 1 , CONTROL(None)   , SINGLE_REG(None)}, // #409 [ref=2x]
  { 0                                                 , 0                             , 23 , 1 , CONTROL(None)   , SINGLE_REG(None)}, // #410 [ref=2x]
  { 0                                                 , 0                             , 61 , 1 , CONTROL(None)   , SINGLE_REG(None)}, // #411 [ref=2x]
  { F(Lock)|F(XAcquire)|F(XRelease)                   , 0                             , 58 , 4 , CONTROL(None)   , SINGLE_REG(None)}, // #412 [ref=1x]
  { 0                                                 , 0                             , 501, 1 , CONTROL(None)   , SINGLE_REG(None)}, // #413 [ref=1x]
  { F(Lock)|F(XAcquire)                               , 0                             , 58 , 8 , CONTROL(None)   , SINGLE_REG(RO)  }, // #414 [ref=1x]
  { 0                                                 , 0                             , 502, 1 , CONTROL(None)   , SINGLE_REG(None)}, // #415 [ref=6x]
  { 0                                                 , 0                             , 503, 1 , CONTROL(None)   , SINGLE_REG(None)}  // #416 [ref=6x]
};
#undef SINGLE_REG
#undef CONTROL
#undef X
#undef F
// ----------------------------------------------------------------------------
// ${InstCommonTable:End}

// ============================================================================
// [asmjit::x86::InstDB - CommonInfoTableB]
// ============================================================================

// ${InstCommonInfoTableB:Begin}
// ------------------- Automatically generated, do not edit -------------------
#define EXT(VAL) uint32_t(Features::k##VAL)
const InstDB::CommonInfoTableB InstDB::_commonInfoTableB[] = {
  { { 0 }, 0, 0 }, // #0 [ref=149x]
  { { 0 }, 1, 0 }, // #1 [ref=32x]
  { { 0 }, 2, 0 }, // #2 [ref=2x]
  { { EXT(ADX) }, 3, 0 }, // #3 [ref=1x]
  { { EXT(SSE2) }, 0, 0 }, // #4 [ref=65x]
  { { EXT(SSE) }, 0, 0 }, // #5 [ref=44x]
  { { EXT(SSE3) }, 0, 0 }, // #6 [ref=12x]
  { { EXT(ADX) }, 4, 0 }, // #7 [ref=1x]
  { { EXT(AESNI) }, 0, 0 }, // #8 [ref=6x]
  { { EXT(BMI) }, 1, 0 }, // #9 [ref=6x]
  { { 0 }, 5, 0 }, // #10 [ref=5x]
  { { EXT(TBM) }, 0, 0 }, // #11 [ref=9x]
  { { EXT(SSE4_1) }, 0, 0 }, // #12 [ref=47x]
  { { EXT(MPX) }, 0, 0 }, // #13 [ref=7x]
  { { 0 }, 6, 0 }, // #14 [ref=4x]
  { { EXT(BMI2) }, 1, 0 }, // #15 [ref=1x]
  { { EXT(SMAP) }, 7, 0 }, // #16 [ref=2x]
  { { 0 }, 8, 0 }, // #17 [ref=2x]
  { { 0 }, 9, 0 }, // #18 [ref=2x]
  { { EXT(CLDEMOTE) }, 0, 0 }, // #19 [ref=1x]
  { { EXT(CLFLUSH) }, 0, 0 }, // #20 [ref=1x]
  { { EXT(CLFLUSHOPT) }, 0, 0 }, // #21 [ref=1x]
  { { EXT(SVM) }, 0, 0 }, // #22 [ref=6x]
  { { 0 }, 10, 0 }, // #23 [ref=2x]
  { { EXT(CET_SS) }, 1, 0 }, // #24 [ref=3x]
  { { EXT(UINTR) }, 0, 0 }, // #25 [ref=4x]
  { { EXT(CLWB) }, 0, 0 }, // #26 [ref=1x]
  { { EXT(CLZERO) }, 0, 0 }, // #27 [ref=1x]
  { { 0 }, 3, 0 }, // #28 [ref=1x]
  { { EXT(CMOV) }, 11, 0 }, // #29 [ref=6x]
  { { EXT(CMOV) }, 12, 0 }, // #30 [ref=8x]
  { { EXT(CMOV) }, 13, 0 }, // #31 [ref=6x]
  { { EXT(CMOV) }, 14, 0 }, // #32 [ref=4x]
  { { EXT(CMOV) }, 15, 0 }, // #33 [ref=4x]
  { { EXT(CMOV) }, 16, 0 }, // #34 [ref=2x]
  { { EXT(CMOV) }, 17, 0 }, // #35 [ref=6x]
  { { EXT(CMOV) }, 18, 0 }, // #36 [ref=2x]
  { { 0 }, 19, 0 }, // #37 [ref=2x]
  { { EXT(I486) }, 1, 0 }, // #38 [ref=2x]
  { { EXT(CMPXCHG16B) }, 5, 0 }, // #39 [ref=1x]
  { { EXT(CMPXCHG8B) }, 5, 0 }, // #40 [ref=1x]
  { { EXT(SSE2) }, 1, 0 }, // #41 [ref=2x]
  { { EXT(SSE) }, 1, 0 }, // #42 [ref=2x]
  { { EXT(I486) }, 0, 0 }, // #43 [ref=4x]
  { { EXT(SSE4_2) }, 0, 0 }, // #44 [ref=2x]
  { { 0 }, 20, 0 }, // #45 [ref=2x]
  { { EXT(MMX) }, 0, 0 }, // #46 [ref=1x]
  { { EXT(CET_IBT) }, 0, 0 }, // #47 [ref=2x]
  { { EXT(ENQCMD) }, 0, 0 }, // #48 [ref=2x]
  { { EXT(SSE4A) }, 0, 0 }, // #49 [ref=4x]
  { { 0 }, 21, 0 }, // #50 [ref=4x]
  { { EXT(3DNOW) }, 0, 0 }, // #51 [ref=21x]
  { { EXT(FXSR) }, 0, 0 }, // #52 [ref=4x]
  { { EXT(SMX) }, 0, 0 }, // #53 [ref=1x]
  { { EXT(GFNI) }, 0, 0 }, // #54 [ref=3x]
  { { EXT(HRESET) }, 0, 0 }, // #55 [ref=1x]
  { { EXT(CET_SS) }, 0, 0 }, // #56 [ref=9x]
  { { 0 }, 16, 0 }, // #57 [ref=5x]
  { { EXT(VMX) }, 0, 0 }, // #58 [ref=12x]
  { { 0 }, 11, 0 }, // #59 [ref=8x]
  { { 0 }, 12, 0 }, // #60 [ref=12x]
  { { 0 }, 13, 0 }, // #61 [ref=10x]
  { { 0 }, 14, 0 }, // #62 [ref=8x]
  { { 0 }, 15, 0 }, // #63 [ref=8x]
  { { 0 }, 17, 0 }, // #64 [ref=8x]
  { { 0 }, 18, 0 }, // #65 [ref=4x]
  { { EXT(AVX512_DQ) }, 0, 0 }, // #66 [ref=23x]
  { { EXT(AVX512_BW) }, 0, 0 }, // #67 [ref=22x]
  { { EXT(AVX512_F) }, 0, 0 }, // #68 [ref=37x]
  { { EXT(AVX512_DQ) }, 1, 0 }, // #69 [ref=3x]
  { { EXT(AVX512_BW) }, 1, 0 }, // #70 [ref=4x]
  { { EXT(AVX512_F) }, 1, 0 }, // #71 [ref=1x]
  { { EXT(LAHFSAHF) }, 22, 0 }, // #72 [ref=1x]
  { { EXT(AMX_TILE) }, 0, 0 }, // #73 [ref=7x]
  { { EXT(LWP) }, 0, 0 }, // #74 [ref=4x]
  { { 0 }, 23, 0 }, // #75 [ref=3x]
  { { EXT(LZCNT) }, 1, 0 }, // #76 [ref=1x]
  { { EXT(MMX2) }, 0, 0 }, // #77 [ref=8x]
  { { EXT(MCOMMIT) }, 1, 0 }, // #78 [ref=1x]
  { { EXT(MONITOR) }, 0, 0 }, // #79 [ref=2x]
  { { EXT(MONITORX) }, 0, 0 }, // #80 [ref=2x]
  { { EXT(MOVBE) }, 0, 0 }, // #81 [ref=1x]
  { { EXT(MMX), EXT(SSE2) }, 0, 0 }, // #82 [ref=46x]
  { { EXT(MOVDIR64B) }, 0, 0 }, // #83 [ref=1x]
  { { EXT(MOVDIRI) }, 0, 0 }, // #84 [ref=1x]
  { { EXT(BMI2) }, 0, 0 }, // #85 [ref=7x]
  { { EXT(SSSE3) }, 0, 0 }, // #86 [ref=15x]
  { { EXT(MMX2), EXT(SSE2) }, 0, 0 }, // #87 [ref=10x]
  { { EXT(PCLMULQDQ) }, 0, 0 }, // #88 [ref=1x]
  { { EXT(SSE4_2) }, 1, 0 }, // #89 [ref=4x]
  { { EXT(PCONFIG) }, 0, 0 }, // #90 [ref=1x]
  { { EXT(MMX2), EXT(SSE2), EXT(SSE4_1) }, 0, 0 }, // #91 [ref=1x]
  { { EXT(3DNOW2) }, 0, 0 }, // #92 [ref=5x]
  { { EXT(GEODE) }, 0, 0 }, // #93 [ref=2x]
  { { EXT(POPCNT) }, 1, 0 }, // #94 [ref=1x]
  { { 0 }, 24, 0 }, // #95 [ref=3x]
  { { EXT(PREFETCHW) }, 1, 0 }, // #96 [ref=1x]
  { { EXT(PREFETCHWT1) }, 1, 0 }, // #97 [ref=1x]
  { { EXT(SNP) }, 20, 0 }, // #98 [ref=3x]
  { { EXT(SSE4_1) }, 1, 0 }, // #99 [ref=1x]
  { { EXT(PTWRITE) }, 0, 0 }, // #100 [ref=1x]
  { { 0 }, 25, 0 }, // #101 [ref=3x]
  { { EXT(SNP) }, 1, 0 }, // #102 [ref=1x]
  { { 0 }, 26, 0 }, // #103 [ref=2x]
  { { EXT(FSGSBASE) }, 0, 0 }, // #104 [ref=4x]
  { { EXT(MSR) }, 0, 0 }, // #105 [ref=2x]
  { { EXT(RDPID) }, 0, 0 }, // #106 [ref=1x]
  { { EXT(OSPKE) }, 0, 0 }, // #107 [ref=1x]
  { { EXT(RDPRU) }, 0, 0 }, // #108 [ref=1x]
  { { EXT(RDRAND) }, 1, 0 }, // #109 [ref=1x]
  { { EXT(RDSEED) }, 1, 0 }, // #110 [ref=1x]
  { { EXT(RDTSC) }, 0, 0 }, // #111 [ref=1x]
  { { EXT(RDTSCP) }, 0, 0 }, // #112 [ref=1x]
  { { 0 }, 27, 0 }, // #113 [ref=2x]
  { { EXT(LAHFSAHF) }, 28, 0 }, // #114 [ref=1x]
  { { EXT(SERIALIZE) }, 0, 0 }, // #115 [ref=1x]
  { { EXT(SHA) }, 0, 0 }, // #116 [ref=7x]
  { { EXT(SKINIT) }, 0, 0 }, // #117 [ref=2x]
  { { EXT(AMX_BF16) }, 0, 0 }, // #118 [ref=1x]
  { { EXT(AMX_INT8) }, 0, 0 }, // #119 [ref=4x]
  { { EXT(UINTR) }, 1, 0 }, // #120 [ref=1x]
  { { EXT(WAITPKG) }, 1, 0 }, // #121 [ref=2x]
  { { EXT(WAITPKG) }, 0, 0 }, // #122 [ref=1x]
  { { EXT(AVX512_4FMAPS) }, 0, 0 }, // #123 [ref=4x]
  { { EXT(AVX), EXT(AVX512_F), EXT(AVX512_VL) }, 0, 0 }, // #124 [ref=46x]
  { { EXT(AVX), EXT(AVX512_F) }, 0, 0 }, // #125 [ref=32x]
  { { EXT(AVX) }, 0, 0 }, // #126 [ref=37x]
  { { EXT(AESNI), EXT(AVX), EXT(AVX512_F), EXT(AVX512_VL), EXT(VAES) }, 0, 0 }, // #127 [ref=4x]
  { { EXT(AESNI), EXT(AVX) }, 0, 0 }, // #128 [ref=2x]
  { { EXT(AVX512_F), EXT(AVX512_VL) }, 0, 0 }, // #129 [ref=112x]
  { { EXT(AVX), EXT(AVX512_DQ), EXT(AVX512_VL) }, 0, 0 }, // #130 [ref=8x]
  { { EXT(AVX512_DQ), EXT(AVX512_VL) }, 0, 0 }, // #131 [ref=30x]
  { { EXT(AVX2) }, 0, 0 }, // #132 [ref=7x]
  { { EXT(AVX), EXT(AVX2), EXT(AVX512_F), EXT(AVX512_VL) }, 0, 0 }, // #133 [ref=39x]
  { { EXT(AVX), EXT(AVX512_F) }, 1, 0 }, // #134 [ref=4x]
  { { EXT(AVX512_BF16), EXT(AVX512_VL) }, 0, 0 }, // #135 [ref=3x]
  { { EXT(AVX512_F), EXT(AVX512_VL), EXT(F16C) }, 0, 0 }, // #136 [ref=2x]
  { { EXT(AVX512_BW), EXT(AVX512_VL) }, 0, 0 }, // #137 [ref=26x]
  { { EXT(AVX512_ERI) }, 0, 0 }, // #138 [ref=10x]
  { { EXT(AVX512_F), EXT(AVX512_VL), EXT(FMA) }, 0, 0 }, // #139 [ref=36x]
  { { EXT(AVX512_F), EXT(FMA) }, 0, 0 }, // #140 [ref=24x]
  { { EXT(FMA4) }, 0, 0 }, // #141 [ref=20x]
  { { EXT(XOP) }, 0, 0 }, // #142 [ref=55x]
  { { EXT(AVX2), EXT(AVX512_F), EXT(AVX512_VL) }, 0, 0 }, // #143 [ref=19x]
  { { EXT(AVX512_PFI) }, 0, 0 }, // #144 [ref=16x]
  { { EXT(AVX), EXT(AVX512_F), EXT(AVX512_VL), EXT(GFNI) }, 0, 0 }, // #145 [ref=3x]
  { { EXT(AVX), EXT(AVX2) }, 0, 0 }, // #146 [ref=17x]
  { { EXT(AVX512_VP2INTERSECT) }, 0, 0 }, // #147 [ref=2x]
  { { EXT(AVX512_4VNNIW) }, 0, 0 }, // #148 [ref=2x]
  { { EXT(AVX), EXT(AVX2), EXT(AVX512_BW), EXT(AVX512_VL) }, 0, 0 }, // #149 [ref=54x]
  { { EXT(AVX2), EXT(AVX512_BW), EXT(AVX512_VL) }, 0, 0 }, // #150 [ref=2x]
  { { EXT(AVX512_CDI), EXT(AVX512_VL) }, 0, 0 }, // #151 [ref=6x]
  { { EXT(AVX), EXT(AVX512_F), EXT(AVX512_VL), EXT(PCLMULQDQ), EXT(VPCLMULQDQ) }, 0, 0 }, // #152 [ref=1x]
  { { EXT(AVX) }, 1, 0 }, // #153 [ref=7x]
  { { EXT(AVX512_VBMI2), EXT(AVX512_VL) }, 0, 0 }, // #154 [ref=16x]
  { { EXT(AVX512_VL), EXT(AVX512_VNNI), EXT(AVX_VNNI) }, 0, 0 }, // #155 [ref=4x]
  { { EXT(AVX512_VBMI), EXT(AVX512_VL) }, 0, 0 }, // #156 [ref=4x]
  { { EXT(AVX), EXT(AVX512_BW) }, 0, 0 }, // #157 [ref=4x]
  { { EXT(AVX), EXT(AVX512_DQ) }, 0, 0 }, // #158 [ref=4x]
  { { EXT(AVX512_IFMA), EXT(AVX512_VL) }, 0, 0 }, // #159 [ref=2x]
  { { EXT(AVX512_BITALG), EXT(AVX512_VL) }, 0, 0 }, // #160 [ref=3x]
  { { EXT(AVX512_VL), EXT(AVX512_VPOPCNTDQ) }, 0, 0 }, // #161 [ref=2x]
  { { EXT(WBNOINVD) }, 0, 0 }, // #162 [ref=1x]
  { { EXT(RTM) }, 0, 0 }, // #163 [ref=3x]
  { { EXT(XSAVE) }, 0, 0 }, // #164 [ref=6x]
  { { EXT(TSXLDTRK) }, 0, 0 }, // #165 [ref=2x]
  { { EXT(XSAVES) }, 0, 0 }, // #166 [ref=4x]
  { { EXT(XSAVEC) }, 0, 0 }, // #167 [ref=2x]
  { { EXT(XSAVEOPT) }, 0, 0 }, // #168 [ref=2x]
  { { EXT(TSX) }, 1, 0 }  // #169 [ref=1x]
};
#undef EXT

#define FLAG(VAL) uint32_t(Status::k##VAL)
const InstDB::RWFlagsInfoTable InstDB::_rwFlagsInfoTable[] = {
  { 0, 0 }, // #0 [ref=1323x]
  { 0, FLAG(AF) | FLAG(CF) | FLAG(OF) | FLAG(PF) | FLAG(SF) | FLAG(ZF) }, // #1 [ref=84x]
  { FLAG(CF), FLAG(AF) | FLAG(CF) | FLAG(OF) | FLAG(PF) | FLAG(SF) | FLAG(ZF) }, // #2 [ref=2x]
  { FLAG(CF), FLAG(CF) }, // #3 [ref=2x]
  { FLAG(OF), FLAG(OF) }, // #4 [ref=1x]
  { 0, FLAG(ZF) }, // #5 [ref=7x]
  { 0, FLAG(AF) | FLAG(CF) | FLAG(OF) | FLAG(PF) | FLAG(SF) }, // #6 [ref=4x]
  { 0, FLAG(AC) }, // #7 [ref=2x]
  { 0, FLAG(CF) }, // #8 [ref=2x]
  { 0, FLAG(DF) }, // #9 [ref=2x]
  { 0, FLAG(IF) }, // #10 [ref=2x]
  { FLAG(CF) | FLAG(ZF), 0 }, // #11 [ref=14x]
  { FLAG(CF), 0 }, // #12 [ref=20x]
  { FLAG(ZF), 0 }, // #13 [ref=16x]
  { FLAG(OF) | FLAG(SF) | FLAG(ZF), 0 }, // #14 [ref=12x]
  { FLAG(OF) | FLAG(SF), 0 }, // #15 [ref=12x]
  { FLAG(OF), 0 }, // #16 [ref=7x]
  { FLAG(PF), 0 }, // #17 [ref=14x]
  { FLAG(SF), 0 }, // #18 [ref=6x]
  { FLAG(DF), FLAG(AF) | FLAG(CF) | FLAG(OF) | FLAG(PF) | FLAG(SF) | FLAG(ZF) }, // #19 [ref=2x]
  { 0, FLAG(AF) | FLAG(OF) | FLAG(PF) | FLAG(SF) | FLAG(ZF) }, // #20 [ref=5x]
  { 0, FLAG(CF) | FLAG(PF) | FLAG(ZF) }, // #21 [ref=4x]
  { FLAG(AF) | FLAG(CF) | FLAG(PF) | FLAG(SF) | FLAG(ZF), 0 }, // #22 [ref=1x]
  { FLAG(DF), 0 }, // #23 [ref=3x]
  { 0, FLAG(AF) | FLAG(CF) | FLAG(DF) | FLAG(IF) | FLAG(OF) | FLAG(PF) | FLAG(SF) | FLAG(ZF) }, // #24 [ref=3x]
  { FLAG(AF) | FLAG(CF) | FLAG(DF) | FLAG(IF) | FLAG(OF) | FLAG(PF) | FLAG(SF) | FLAG(ZF), 0 }, // #25 [ref=3x]
  { FLAG(CF) | FLAG(OF), FLAG(CF) | FLAG(OF) }, // #26 [ref=2x]
  { 0, FLAG(CF) | FLAG(OF) }, // #27 [ref=2x]
  { 0, FLAG(AF) | FLAG(CF) | FLAG(PF) | FLAG(SF) | FLAG(ZF) }  // #28 [ref=1x]
};
#undef FLAG
// ----------------------------------------------------------------------------
// ${InstCommonInfoTableB:End}

// ============================================================================
// [asmjit::Inst - NameData]
// ============================================================================

#ifndef ASMJIT_NO_TEXT
// ${NameData:Begin}
// ------------------- Automatically generated, do not edit -------------------
const char InstDB::_nameData[] =
  "\0" "aaa\0" "aad\0" "aam\0" "aas\0" "adc\0" "adcx\0" "adox\0" "arpl\0" "bextr\0" "blcfill\0" "blci\0" "blcic\0"
  "blcmsk\0" "blcs\0" "blsfill\0" "blsi\0" "blsic\0" "blsmsk\0" "blsr\0" "bndcl\0" "bndcn\0" "bndcu\0" "bndldx\0"
  "bndmk\0" "bndmov\0" "bndstx\0" "bound\0" "bsf\0" "bsr\0" "bswap\0" "bt\0" "btc\0" "btr\0" "bts\0" "bzhi\0" "cbw\0"
  "cdq\0" "cdqe\0" "clac\0" "clc\0" "cld\0" "cldemote\0" "clflush\0" "clflushopt\0" "clgi\0" "cli\0" "clrssbsy\0"
  "clts\0" "clui\0" "clwb\0" "clzero\0" "cmc\0" "cmova\0" "cmovae\0" "cmovc\0" "cmovg\0" "cmovge\0" "cmovl\0"
  "cmovle\0" "cmovna\0" "cmovnae\0" "cmovnc\0" "cmovng\0" "cmovnge\0" "cmovnl\0" "cmovnle\0" "cmovno\0" "cmovnp\0"
  "cmovns\0" "cmovnz\0" "cmovo\0" "cmovp\0" "cmovpe\0" "cmovpo\0" "cmovs\0" "cmovz\0" "cmp\0" "cmps\0" "cmpxchg\0"
  "cmpxchg16b\0" "cmpxchg8b\0" "cpuid\0" "cqo\0" "crc32\0" "cvtpd2pi\0" "cvtpi2pd\0" "cvtpi2ps\0" "cvtps2pi\0"
  "cvttpd2pi\0" "cvttps2pi\0" "cwd\0" "cwde\0" "daa\0" "das\0" "endbr32\0" "endbr64\0" "enqcmd\0" "enqcmds\0" "f2xm1\0"
  "fabs\0" "faddp\0" "fbld\0" "fbstp\0" "fchs\0" "fclex\0" "fcmovb\0" "fcmovbe\0" "fcmove\0" "fcmovnb\0" "fcmovnbe\0"
  "fcmovne\0" "fcmovnu\0" "fcmovu\0" "fcom\0" "fcomi\0" "fcomip\0" "fcomp\0" "fcompp\0" "fcos\0" "fdecstp\0" "fdiv\0"
  "fdivp\0" "fdivr\0" "fdivrp\0" "femms\0" "ffree\0" "fiadd\0" "ficom\0" "ficomp\0" "fidiv\0" "fidivr\0" "fild\0"
  "fimul\0" "fincstp\0" "finit\0" "fist\0" "fistp\0" "fisttp\0" "fisub\0" "fisubr\0" "fld\0" "fld1\0" "fldcw\0"
  "fldenv\0" "fldl2e\0" "fldl2t\0" "fldlg2\0" "fldln2\0" "fldpi\0" "fldz\0" "fmulp\0" "fnclex\0" "fninit\0" "fnop\0"
  "fnsave\0" "fnstcw\0" "fnstenv\0" "fnstsw\0" "fpatan\0" "fprem\0" "fprem1\0" "fptan\0" "frndint\0" "frstor\0"
  "fsave\0" "fscale\0" "fsin\0" "fsincos\0" "fsqrt\0" "fst\0" "fstcw\0" "fstenv\0" "fstp\0" "fstsw\0" "fsubp\0"
  "fsubrp\0" "ftst\0" "fucom\0" "fucomi\0" "fucomip\0" "fucomp\0" "fucompp\0" "fwait\0" "fxam\0" "fxch\0" "fxrstor\0"
  "fxrstor64\0" "fxsave\0" "fxsave64\0" "fxtract\0" "fyl2x\0" "fyl2xp1\0" "getsec\0" "hlt\0" "hreset\0" "inc\0"
  "incsspd\0" "incsspq\0" "insertq\0" "int3\0" "into\0" "invept\0" "invlpg\0" "invlpga\0" "invpcid\0" "invvpid\0"
  "iretd\0" "iretq\0" "ja\0" "jae\0" "jb\0" "jbe\0" "jc\0" "je\0" "jecxz\0" "jg\0" "jge\0" "jl\0" "jle\0" "jna\0"
  "jnae\0" "jnb\0" "jnbe\0" "jnc\0" "jne\0" "jng\0" "jnge\0" "jnl\0" "jnle\0" "jno\0" "jnp\0" "jns\0" "jnz\0" "jo\0"
  "jp\0" "jpe\0" "jpo\0" "js\0" "jz\0" "kaddb\0" "kaddd\0" "kaddq\0" "kaddw\0" "kandb\0" "kandd\0" "kandnb\0"
  "kandnd\0" "kandnq\0" "kandnw\0" "kandq\0" "kandw\0" "kmovb\0" "kmovw\0" "knotb\0" "knotd\0" "knotq\0" "knotw\0"
  "korb\0" "kord\0" "korq\0" "kortestb\0" "kortestd\0" "kortestq\0" "kortestw\0" "korw\0" "kshiftlb\0" "kshiftld\0"
  "kshiftlq\0" "kshiftlw\0" "kshiftrb\0" "kshiftrd\0" "kshiftrq\0" "kshiftrw\0" "ktestb\0" "ktestd\0" "ktestq\0"
  "ktestw\0" "kunpckbw\0" "kunpckdq\0" "kunpckwd\0" "kxnorb\0" "kxnord\0" "kxnorq\0" "kxnorw\0" "kxorb\0" "kxord\0"
  "kxorq\0" "kxorw\0" "lahf\0" "lar\0" "lcall\0" "lds\0" "ldtilecfg\0" "lea\0" "leave\0" "les\0" "lfence\0" "lfs\0"
  "lgdt\0" "lgs\0" "lidt\0" "ljmp\0" "lldt\0" "llwpcb\0" "lmsw\0" "lods\0" "loop\0" "loope\0" "loopne\0" "lsl\0"
  "ltr\0" "lwpins\0" "lwpval\0" "lzcnt\0" "mcommit\0" "mfence\0" "monitorx\0" "movabs\0" "movdir64b\0" "movdiri\0"
  "movdq2q\0" "movnti\0" "movntq\0" "movntsd\0" "movntss\0" "movq2dq\0" "movsx\0" "movsxd\0" "movzx\0" "mulx\0"
  "mwaitx\0" "neg\0" "not\0" "out\0" "outs\0" "pavgusb\0" "pconfig\0" "pdep\0" "pext\0" "pf2id\0" "pf2iw\0" "pfacc\0"
  "pfadd\0" "pfcmpeq\0" "pfcmpge\0" "pfcmpgt\0" "pfmax\0" "pfmin\0" "pfmul\0" "pfnacc\0" "pfpnacc\0" "pfrcp\0"
  "pfrcpit1\0" "pfrcpit2\0" "pfrcpv\0" "pfrsqit1\0" "pfrsqrt\0" "pfrsqrtv\0" "pfsub\0" "pfsubr\0" "pi2fd\0" "pi2fw\0"
  "pmulhrw\0" "pop\0" "popa\0" "popad\0" "popcnt\0" "popf\0" "popfd\0" "popfq\0" "prefetch\0" "prefetchnta\0"
  "prefetcht0\0" "prefetcht1\0" "prefetcht2\0" "prefetchw\0" "prefetchwt1\0" "pshufw\0" "psmash\0" "pswapd\0"
  "ptwrite\0" "push\0" "pusha\0" "pushad\0" "pushf\0" "pushfd\0" "pushfq\0" "pvalidate\0" "rcl\0" "rcr\0" "rdfsbase\0"
  "rdgsbase\0" "rdmsr\0" "rdpid\0" "rdpkru\0" "rdpmc\0" "rdpru\0" "rdrand\0" "rdseed\0" "rdsspd\0" "rdsspq\0" "rdtsc\0"
  "rdtscp\0" "retf\0" "rmpadjust\0" "rmpupdate\0" "rol\0" "ror\0" "rorx\0" "rsm\0" "rstorssp\0" "sahf\0" "sal\0"
  "sar\0" "sarx\0" "saveprevssp\0" "sbb\0" "scas\0" "senduipi\0" "serialize\0" "seta\0" "setae\0" "setb\0" "setbe\0"
  "setc\0" "sete\0" "setg\0" "setge\0" "setl\0" "setle\0" "setna\0" "setnae\0" "setnb\0" "setnbe\0" "setnc\0" "setne\0"
  "setng\0" "setnge\0" "setnl\0" "setnle\0" "setno\0" "setnp\0" "setns\0" "setnz\0" "seto\0" "setp\0" "setpe\0"
  "setpo\0" "sets\0" "setssbsy\0" "setz\0" "sfence\0" "sgdt\0" "sha1msg1\0" "sha1msg2\0" "sha1nexte\0" "sha1rnds4\0"
  "sha256msg1\0" "sha256msg2\0" "sha256rnds2\0" "shl\0" "shlx\0" "shr\0" "shrd\0" "shrx\0" "sidt\0" "skinit\0" "sldt\0"
  "slwpcb\0" "smsw\0" "stac\0" "stc\0" "stgi\0" "sti\0" "stos\0" "str\0" "sttilecfg\0" "swapgs\0" "syscall\0"
  "sysenter\0" "sysexit\0" "sysexitq\0" "sysret\0" "sysretq\0" "t1mskc\0" "tdpbf16ps\0" "tdpbssd\0" "tdpbsud\0"
  "tdpbusd\0" "tdpbuud\0" "testui\0" "tileloadd\0" "tileloaddt1\0" "tilerelease\0" "tilestored\0" "tilezero\0"
  "tpause\0" "tzcnt\0" "tzmsk\0" "ud0\0" "ud1\0" "ud2\0" "uiret\0" "umonitor\0" "umwait\0" "v4fmaddps\0" "v4fmaddss\0"
  "v4fnmaddps\0" "v4fnmaddss\0" "vaddpd\0" "vaddps\0" "vaddsd\0" "vaddss\0" "vaddsubpd\0" "vaddsubps\0" "vaesdec\0"
  "vaesdeclast\0" "vaesenc\0" "vaesenclast\0" "vaesimc\0" "vaeskeygenassist\0" "valignd\0" "valignq\0" "vandnpd\0"
  "vandnps\0" "vandpd\0" "vandps\0" "vblendmpd\0" "vblendmps\0" "vblendpd\0" "vblendps\0" "vblendvpd\0" "vblendvps\0"
  "vbroadcastf128\0" "vbroadcastf32x2\0" "vbroadcastf32x4\0" "vbroadcastf32x8\0" "vbroadcastf64x2\0"
  "vbroadcastf64x4\0" "vbroadcasti128\0" "vbroadcasti32x2\0" "vbroadcasti32x4\0" "vbroadcasti32x8\0"
  "vbroadcasti64x2\0" "vbroadcasti64x4\0" "vbroadcastsd\0" "vbroadcastss\0" "vcmppd\0" "vcmpps\0" "vcmpsd\0" "vcmpss\0"
  "vcomisd\0" "vcomiss\0" "vcompresspd\0" "vcompressps\0" "vcvtdq2pd\0" "vcvtdq2ps\0" "vcvtne2ps2bf16\0"
  "vcvtneps2bf16\0" "vcvtpd2dq\0" "vcvtpd2ps\0" "vcvtpd2qq\0" "vcvtpd2udq\0" "vcvtpd2uqq\0" "vcvtph2ps\0" "vcvtps2dq\0"
  "vcvtps2pd\0" "vcvtps2ph\0" "vcvtps2qq\0" "vcvtps2udq\0" "vcvtps2uqq\0" "vcvtqq2pd\0" "vcvtqq2ps\0" "vcvtsd2si\0"
  "vcvtsd2ss\0" "vcvtsd2usi\0" "vcvtsi2sd\0" "vcvtsi2ss\0" "vcvtss2sd\0" "vcvtss2si\0" "vcvtss2usi\0" "vcvttpd2dq\0"
  "vcvttpd2qq\0" "vcvttpd2udq\0" "vcvttpd2uqq\0" "vcvttps2dq\0" "vcvttps2qq\0" "vcvttps2udq\0" "vcvttps2uqq\0"
  "vcvttsd2si\0" "vcvttsd2usi\0" "vcvttss2si\0" "vcvttss2usi\0" "vcvtudq2pd\0" "vcvtudq2ps\0" "vcvtuqq2pd\0"
  "vcvtuqq2ps\0" "vcvtusi2sd\0" "vcvtusi2ss\0" "vdbpsadbw\0" "vdivpd\0" "vdivps\0" "vdivsd\0" "vdivss\0" "vdpbf16ps\0"
  "vdppd\0" "vdpps\0" "verr\0" "verw\0" "vexp2pd\0" "vexp2ps\0" "vexpandpd\0" "vexpandps\0" "vextractf128\0"
  "vextractf32x4\0" "vextractf32x8\0" "vextractf64x2\0" "vextractf64x4\0" "vextracti128\0" "vextracti32x4\0"
  "vextracti32x8\0" "vextracti64x2\0" "vextracti64x4\0" "vextractps\0" "vfixupimmpd\0" "vfixupimmps\0" "vfixupimmsd\0"
  "vfixupimmss\0" "vfmadd132pd\0" "vfmadd132ps\0" "vfmadd132sd\0" "vfmadd132ss\0" "vfmadd213pd\0" "vfmadd213ps\0"
  "vfmadd213sd\0" "vfmadd213ss\0" "vfmadd231pd\0" "vfmadd231ps\0" "vfmadd231sd\0" "vfmadd231ss\0" "vfmaddpd\0"
  "vfmaddps\0" "vfmaddsd\0" "vfmaddss\0" "vfmaddsub132pd\0" "vfmaddsub132ps\0" "vfmaddsub213pd\0" "vfmaddsub213ps\0"
  "vfmaddsub231pd\0" "vfmaddsub231ps\0" "vfmaddsubpd\0" "vfmaddsubps\0" "vfmsub132pd\0" "vfmsub132ps\0" "vfmsub132sd\0"
  "vfmsub132ss\0" "vfmsub213pd\0" "vfmsub213ps\0" "vfmsub213sd\0" "vfmsub213ss\0" "vfmsub231pd\0" "vfmsub231ps\0"
  "vfmsub231sd\0" "vfmsub231ss\0" "vfmsubadd132pd\0" "vfmsubadd132ps\0" "vfmsubadd213pd\0" "vfmsubadd213ps\0"
  "vfmsubadd231pd\0" "vfmsubadd231ps\0" "vfmsubaddpd\0" "vfmsubaddps\0" "vfmsubpd\0" "vfmsubps\0" "vfmsubsd\0"
  "vfmsubss\0" "vfnmadd132pd\0" "vfnmadd132ps\0" "vfnmadd132sd\0" "vfnmadd132ss\0" "vfnmadd213pd\0" "vfnmadd213ps\0"
  "vfnmadd213sd\0" "vfnmadd213ss\0" "vfnmadd231pd\0" "vfnmadd231ps\0" "vfnmadd231sd\0" "vfnmadd231ss\0" "vfnmaddpd\0"
  "vfnmaddps\0" "vfnmaddsd\0" "vfnmaddss\0" "vfnmsub132pd\0" "vfnmsub132ps\0" "vfnmsub132sd\0" "vfnmsub132ss\0"
  "vfnmsub213pd\0" "vfnmsub213ps\0" "vfnmsub213sd\0" "vfnmsub213ss\0" "vfnmsub231pd\0" "vfnmsub231ps\0"
  "vfnmsub231sd\0" "vfnmsub231ss\0" "vfnmsubpd\0" "vfnmsubps\0" "vfnmsubsd\0" "vfnmsubss\0" "vfpclasspd\0"
  "vfpclassps\0" "vfpclasssd\0" "vfpclassss\0" "vfrczpd\0" "vfrczps\0" "vfrczsd\0" "vfrczss\0" "vgatherdpd\0"
  "vgatherdps\0" "vgatherpf0dpd\0" "vgatherpf0dps\0" "vgatherpf0qpd\0" "vgatherpf0qps\0" "vgatherpf1dpd\0"
  "vgatherpf1dps\0" "vgatherpf1qpd\0" "vgatherpf1qps\0" "vgatherqpd\0" "vgatherqps\0" "vgetexppd\0" "vgetexpps\0"
  "vgetexpsd\0" "vgetexpss\0" "vgetmantpd\0" "vgetmantps\0" "vgetmantsd\0" "vgetmantss\0" "vgf2p8affineinvqb\0"
  "vgf2p8affineqb\0" "vgf2p8mulb\0" "vhaddpd\0" "vhaddps\0" "vhsubpd\0" "vhsubps\0" "vinsertf128\0" "vinsertf32x4\0"
  "vinsertf32x8\0" "vinsertf64x2\0" "vinsertf64x4\0" "vinserti128\0" "vinserti32x4\0" "vinserti32x8\0" "vinserti64x2\0"
  "vinserti64x4\0" "vinsertps\0" "vlddqu\0" "vldmxcsr\0" "vmaskmovdqu\0" "vmaskmovpd\0" "vmaskmovps\0" "vmaxpd\0"
  "vmaxps\0" "vmaxsd\0" "vmaxss\0" "vmcall\0" "vmclear\0" "vmfunc\0" "vminpd\0" "vminps\0" "vminsd\0" "vminss\0"
  "vmlaunch\0" "vmload\0" "vmmcall\0" "vmovapd\0" "vmovaps\0" "vmovd\0" "vmovddup\0" "vmovdqa\0" "vmovdqa32\0"
  "vmovdqa64\0" "vmovdqu\0" "vmovdqu16\0" "vmovdqu32\0" "vmovdqu64\0" "vmovdqu8\0" "vmovhlps\0" "vmovhpd\0" "vmovhps\0"
  "vmovlhps\0" "vmovlpd\0" "vmovlps\0" "vmovmskpd\0" "vmovmskps\0" "vmovntdq\0" "vmovntdqa\0" "vmovntpd\0" "vmovntps\0"
  "vmovq\0" "vmovsd\0" "vmovshdup\0" "vmovsldup\0" "vmovss\0" "vmovupd\0" "vmovups\0" "vmpsadbw\0" "vmptrld\0"
  "vmptrst\0" "vmread\0" "vmresume\0" "vmrun\0" "vmsave\0" "vmulpd\0" "vmulps\0" "vmulsd\0" "vmulss\0" "vmwrite\0"
  "vmxon\0" "vorpd\0" "vorps\0" "vp2intersectd\0" "vp2intersectq\0" "vp4dpwssd\0" "vp4dpwssds\0" "vpabsb\0" "vpabsd\0"
  "vpabsq\0" "vpabsw\0" "vpackssdw\0" "vpacksswb\0" "vpackusdw\0" "vpackuswb\0" "vpaddb\0" "vpaddd\0" "vpaddq\0"
  "vpaddsb\0" "vpaddsw\0" "vpaddusb\0" "vpaddusw\0" "vpaddw\0" "vpalignr\0" "vpand\0" "vpandd\0" "vpandn\0" "vpandnd\0"
  "vpandnq\0" "vpandq\0" "vpavgb\0" "vpavgw\0" "vpblendd\0" "vpblendmb\0" "vpblendmd\0" "vpblendmq\0" "vpblendmw\0"
  "vpblendvb\0" "vpblendw\0" "vpbroadcastb\0" "vpbroadcastd\0" "vpbroadcastmb2q\0" "vpbroadcastmw2d\0" "vpbroadcastq\0"
  "vpbroadcastw\0" "vpclmulqdq\0" "vpcmov\0" "vpcmpb\0" "vpcmpd\0" "vpcmpeqb\0" "vpcmpeqd\0" "vpcmpeqq\0" "vpcmpeqw\0"
  "vpcmpestri\0" "vpcmpestrm\0" "vpcmpgtb\0" "vpcmpgtd\0" "vpcmpgtq\0" "vpcmpgtw\0" "vpcmpistri\0" "vpcmpistrm\0"
  "vpcmpq\0" "vpcmpub\0" "vpcmpud\0" "vpcmpuq\0" "vpcmpuw\0" "vpcmpw\0" "vpcomb\0" "vpcomd\0" "vpcompressb\0"
  "vpcompressd\0" "vpcompressq\0" "vpcompressw\0" "vpcomq\0" "vpcomub\0" "vpcomud\0" "vpcomuq\0" "vpcomuw\0" "vpcomw\0"
  "vpconflictd\0" "vpconflictq\0" "vpdpbusd\0" "vpdpbusds\0" "vpdpwssd\0" "vpdpwssds\0" "vperm2f128\0" "vperm2i128\0"
  "vpermb\0" "vpermd\0" "vpermi2b\0" "vpermi2d\0" "vpermi2pd\0" "vpermi2ps\0" "vpermi2q\0" "vpermi2w\0" "vpermil2pd\0"
  "vpermil2ps\0" "vpermilpd\0" "vpermilps\0" "vpermpd\0" "vpermps\0" "vpermq\0" "vpermt2b\0" "vpermt2d\0" "vpermt2pd\0"
  "vpermt2ps\0" "vpermt2q\0" "vpermt2w\0" "vpermw\0" "vpexpandb\0" "vpexpandd\0" "vpexpandq\0" "vpexpandw\0"
  "vpextrb\0" "vpextrd\0" "vpextrq\0" "vpextrw\0" "vpgatherdd\0" "vpgatherdq\0" "vpgatherqd\0" "vpgatherqq\0"
  "vphaddbd\0" "vphaddbq\0" "vphaddbw\0" "vphaddd\0" "vphadddq\0" "vphaddsw\0" "vphaddubd\0" "vphaddubq\0"
  "vphaddubw\0" "vphaddudq\0" "vphadduwd\0" "vphadduwq\0" "vphaddw\0" "vphaddwd\0" "vphaddwq\0" "vphminposuw\0"
  "vphsubbw\0" "vphsubd\0" "vphsubdq\0" "vphsubsw\0" "vphsubw\0" "vphsubwd\0" "vpinsrb\0" "vpinsrd\0" "vpinsrq\0"
  "vpinsrw\0" "vplzcntd\0" "vplzcntq\0" "vpmacsdd\0" "vpmacsdqh\0" "vpmacsdql\0" "vpmacssdd\0" "vpmacssdqh\0"
  "vpmacssdql\0" "vpmacsswd\0" "vpmacssww\0" "vpmacswd\0" "vpmacsww\0" "vpmadcsswd\0" "vpmadcswd\0" "vpmadd52huq\0"
  "vpmadd52luq\0" "vpmaddubsw\0" "vpmaddwd\0" "vpmaskmovd\0" "vpmaskmovq\0" "vpmaxsb\0" "vpmaxsd\0" "vpmaxsq\0"
  "vpmaxsw\0" "vpmaxub\0" "vpmaxud\0" "vpmaxuq\0" "vpmaxuw\0" "vpminsb\0" "vpminsd\0" "vpminsq\0" "vpminsw\0"
  "vpminub\0" "vpminud\0" "vpminuq\0" "vpminuw\0" "vpmovb2m\0" "vpmovd2m\0" "vpmovdb\0" "vpmovdw\0" "vpmovm2b\0"
  "vpmovm2d\0" "vpmovm2q\0" "vpmovm2w\0" "vpmovmskb\0" "vpmovq2m\0" "vpmovqb\0" "vpmovqd\0" "vpmovqw\0" "vpmovsdb\0"
  "vpmovsdw\0" "vpmovsqb\0" "vpmovsqd\0" "vpmovsqw\0" "vpmovswb\0" "vpmovsxbd\0" "vpmovsxbq\0" "vpmovsxbw\0"
  "vpmovsxdq\0" "vpmovsxwd\0" "vpmovsxwq\0" "vpmovusdb\0" "vpmovusdw\0" "vpmovusqb\0" "vpmovusqd\0" "vpmovusqw\0"
  "vpmovuswb\0" "vpmovw2m\0" "vpmovwb\0" "vpmovzxbd\0" "vpmovzxbq\0" "vpmovzxbw\0" "vpmovzxdq\0" "vpmovzxwd\0"
  "vpmovzxwq\0" "vpmuldq\0" "vpmulhrsw\0" "vpmulhuw\0" "vpmulhw\0" "vpmulld\0" "vpmullq\0" "vpmullw\0"
  "vpmultishiftqb\0" "vpmuludq\0" "vpopcntb\0" "vpopcntd\0" "vpopcntq\0" "vpopcntw\0" "vpor\0" "vpord\0" "vporq\0"
  "vpperm\0" "vprold\0" "vprolq\0" "vprolvd\0" "vprolvq\0" "vprord\0" "vprorq\0" "vprorvd\0" "vprorvq\0" "vprotb\0"
  "vprotd\0" "vprotq\0" "vprotw\0" "vpsadbw\0" "vpscatterdd\0" "vpscatterdq\0" "vpscatterqd\0" "vpscatterqq\0"
  "vpshab\0" "vpshad\0" "vpshaq\0" "vpshaw\0" "vpshlb\0" "vpshld\0" "vpshldd\0" "vpshldq\0" "vpshldvd\0" "vpshldvq\0"
  "vpshldvw\0" "vpshldw\0" "vpshlq\0" "vpshlw\0" "vpshrdd\0" "vpshrdq\0" "vpshrdvd\0" "vpshrdvq\0" "vpshrdvw\0"
  "vpshrdw\0" "vpshufb\0" "vpshufbitqmb\0" "vpshufd\0" "vpshufhw\0" "vpshuflw\0" "vpsignb\0" "vpsignd\0" "vpsignw\0"
  "vpslld\0" "vpslldq\0" "vpsllq\0" "vpsllvd\0" "vpsllvq\0" "vpsllvw\0" "vpsllw\0" "vpsrad\0" "vpsraq\0" "vpsravd\0"
  "vpsravq\0" "vpsravw\0" "vpsraw\0" "vpsrld\0" "vpsrldq\0" "vpsrlq\0" "vpsrlvd\0" "vpsrlvq\0" "vpsrlvw\0" "vpsrlw\0"
  "vpsubb\0" "vpsubd\0" "vpsubq\0" "vpsubsb\0" "vpsubsw\0" "vpsubusb\0" "vpsubusw\0" "vpsubw\0" "vpternlogd\0"
  "vpternlogq\0" "vptest\0" "vptestmb\0" "vptestmd\0" "vptestmq\0" "vptestmw\0" "vptestnmb\0" "vptestnmd\0"
  "vptestnmq\0" "vptestnmw\0" "vpunpckhbw\0" "vpunpckhdq\0" "vpunpckhqdq\0" "vpunpckhwd\0" "vpunpcklbw\0"
  "vpunpckldq\0" "vpunpcklqdq\0" "vpunpcklwd\0" "vpxor\0" "vpxord\0" "vpxorq\0" "vrangepd\0" "vrangeps\0" "vrangesd\0"
  "vrangess\0" "vrcp14pd\0" "vrcp14ps\0" "vrcp14sd\0" "vrcp14ss\0" "vrcp28pd\0" "vrcp28ps\0" "vrcp28sd\0" "vrcp28ss\0"
  "vrcpps\0" "vrcpss\0" "vreducepd\0" "vreduceps\0" "vreducesd\0" "vreducess\0" "vrndscalepd\0" "vrndscaleps\0"
  "vrndscalesd\0" "vrndscaless\0" "vroundpd\0" "vroundps\0" "vroundsd\0" "vroundss\0" "vrsqrt14pd\0" "vrsqrt14ps\0"
  "vrsqrt14sd\0" "vrsqrt14ss\0" "vrsqrt28pd\0" "vrsqrt28ps\0" "vrsqrt28sd\0" "vrsqrt28ss\0" "vrsqrtps\0" "vrsqrtss\0"
  "vscalefpd\0" "vscalefps\0" "vscalefsd\0" "vscalefss\0" "vscatterdpd\0" "vscatterdps\0" "vscatterpf0dpd\0"
  "vscatterpf0dps\0" "vscatterpf0qpd\0" "vscatterpf0qps\0" "vscatterpf1dpd\0" "vscatterpf1dps\0" "vscatterpf1qpd\0"
  "vscatterpf1qps\0" "vscatterqpd\0" "vscatterqps\0" "vshuff32x4\0" "vshuff64x2\0" "vshufi32x4\0" "vshufi64x2\0"
  "vshufpd\0" "vshufps\0" "vsqrtpd\0" "vsqrtps\0" "vsqrtsd\0" "vsqrtss\0" "vstmxcsr\0" "vsubpd\0" "vsubps\0" "vsubsd\0"
  "vsubss\0" "vtestpd\0" "vtestps\0" "vucomisd\0" "vucomiss\0" "vunpckhpd\0" "vunpckhps\0" "vunpcklpd\0" "vunpcklps\0"
  "vxorpd\0" "vxorps\0" "vzeroall\0" "vzeroupper\0" "wbinvd\0" "wbnoinvd\0" "wrfsbase\0" "wrgsbase\0" "wrmsr\0"
  "wrssd\0" "wrssq\0" "wrussd\0" "wrussq\0" "xabort\0" "xadd\0" "xbegin\0" "xend\0" "xgetbv\0" "xlatb\0" "xresldtrk\0"
  "xrstors\0" "xrstors64\0" "xsavec\0" "xsavec64\0" "xsaveopt\0" "xsaveopt64\0" "xsaves\0" "xsaves64\0" "xsetbv\0"
  "xsusldtrk\0" "xtest";

const InstDB::InstNameIndex InstDB::instNameIndex[26] = {
  { Inst::kIdAaa          , Inst::kIdArpl          + 1 },
  { Inst::kIdBextr        , Inst::kIdBzhi          + 1 },
  { Inst::kIdCall         , Inst::kIdCwde          + 1 },
  { Inst::kIdDaa          , Inst::kIdDpps          + 1 },
  { Inst::kIdEmms         , Inst::kIdExtrq         + 1 },
  { Inst::kIdF2xm1        , Inst::kIdFyl2xp1       + 1 },
  { Inst::kIdGetsec       , Inst::kIdGf2p8mulb     + 1 },
  { Inst::kIdHaddpd       , Inst::kIdHsubps        + 1 },
  { Inst::kIdIdiv         , Inst::kIdIretq         + 1 },
  { Inst::kIdJa           , Inst::kIdJz            + 1 },
  { Inst::kIdKaddb        , Inst::kIdKxorw         + 1 },
  { Inst::kIdLahf         , Inst::kIdLzcnt         + 1 },
  { Inst::kIdMaskmovdqu   , Inst::kIdMwaitx        + 1 },
  { Inst::kIdNeg          , Inst::kIdNot           + 1 },
  { Inst::kIdOr           , Inst::kIdOuts          + 1 },
  { Inst::kIdPabsb        , Inst::kIdPxor          + 1 },
  { Inst::kIdNone         , Inst::kIdNone          + 1 },
  { Inst::kIdRcl          , Inst::kIdRstorssp      + 1 },
  { Inst::kIdSahf         , Inst::kIdSysretq       + 1 },
  { Inst::kIdT1mskc       , Inst::kIdTzmsk         + 1 },
  { Inst::kIdUcomisd      , Inst::kIdUnpcklps      + 1 },
  { Inst::kIdV4fmaddps    , Inst::kIdVzeroupper    + 1 },
  { Inst::kIdWbinvd       , Inst::kIdWrussq        + 1 },
  { Inst::kIdXabort       , Inst::kIdXtest         + 1 },
  { Inst::kIdNone         , Inst::kIdNone          + 1 },
  { Inst::kIdNone         , Inst::kIdNone          + 1 }
};
// ----------------------------------------------------------------------------
// ${NameData:End}
#endif // !ASMJIT_NO_TEXT

// ============================================================================
// [asmjit::x86::InstDB - InstSignature / OpSignature]
// ============================================================================

#ifndef ASMJIT_NO_VALIDATION
// ${InstSignatureTable:Begin}
// ------------------- Automatically generated, do not edit -------------------
#define ROW(count, x86, x64, implicit, o0, o1, o2, o3, o4, o5)  \
  { count, (x86 ? uint8_t(InstDB::kModeX86) : uint8_t(0)) |     \
           (x64 ? uint8_t(InstDB::kModeX64) : uint8_t(0)) ,     \
    implicit,                                                   \
    0,                                                          \
    { o0, o1, o2, o3, o4, o5 }                                  \
  }
const InstDB::InstSignature InstDB::_instSignatureTable[] = {
  ROW(2, 1, 1, 0, 1  , 2  , 0  , 0  , 0  , 0  ), // #0   {r8lo|r8hi|m8|mem, r8lo|r8hi}
  ROW(2, 1, 1, 0, 3  , 4  , 0  , 0  , 0  , 0  ), //      {r16|m16|mem|sreg, r16}
  ROW(2, 1, 1, 0, 5  , 6  , 0  , 0  , 0  , 0  ), //      {r32|m32|mem|sreg, r32}
  ROW(2, 0, 1, 0, 7  , 8  , 0  , 0  , 0  , 0  ), //      {r64|m64|mem|sreg|creg|dreg, r64}
  ROW(2, 1, 1, 0, 9  , 10 , 0  , 0  , 0  , 0  ), //      {r8lo|r8hi|m8, i8|u8}
  ROW(2, 1, 1, 0, 11 , 12 , 0  , 0  , 0  , 0  ), //      {r16|m16, i16|u16}
  ROW(2, 1, 1, 0, 13 , 14 , 0  , 0  , 0  , 0  ), //      {r32|m32, i32|u32}
  ROW(2, 0, 1, 0, 15 , 16 , 0  , 0  , 0  , 0  ), //      {r64|m64|mem, i32}
  ROW(2, 0, 1, 0, 8  , 17 , 0  , 0  , 0  , 0  ), //      {r64, i64|u64|m64|mem|sreg|creg|dreg}
  ROW(2, 1, 1, 0, 2  , 18 , 0  , 0  , 0  , 0  ), //      {r8lo|r8hi, m8|mem}
  ROW(2, 1, 1, 0, 4  , 19 , 0  , 0  , 0  , 0  ), //      {r16, m16|mem|sreg}
  ROW(2, 1, 1, 0, 6  , 20 , 0  , 0  , 0  , 0  ), //      {r32, m32|mem|sreg}
  ROW(2, 1, 1, 0, 21 , 22 , 0  , 0  , 0  , 0  ), //      {m16|mem, sreg}
  ROW(2, 1, 1, 0, 22 , 21 , 0  , 0  , 0  , 0  ), //      {sreg, m16|mem}
  ROW(2, 1, 0, 0, 6  , 23 , 0  , 0  , 0  , 0  ), //      {r32, creg|dreg}
  ROW(2, 1, 0, 0, 23 , 6  , 0  , 0  , 0  , 0  ), //      {creg|dreg, r32}
  ROW(2, 1, 1, 0, 9  , 10 , 0  , 0  , 0  , 0  ), // #16  {r8lo|r8hi|m8, i8|u8}
  ROW(2, 1, 1, 0, 11 , 12 , 0  , 0  , 0  , 0  ), //      {r16|m16, i16|u16}
  ROW(2, 1, 1, 0, 13 , 14 , 0  , 0  , 0  , 0  ), //      {r32|m32, i32|u32}
  ROW(2, 0, 1, 0, 15 , 24 , 0  , 0  , 0  , 0  ), //      {r64|m64|mem, i32|r64}
  ROW(2, 1, 1, 0, 25 , 26 , 0  , 0  , 0  , 0  ), //      {r16|m16|r32|m32|r64|m64|mem, i8}
  ROW(2, 1, 1, 0, 1  , 2  , 0  , 0  , 0  , 0  ), //      {r8lo|r8hi|m8|mem, r8lo|r8hi}
  ROW(2, 1, 1, 0, 27 , 4  , 0  , 0  , 0  , 0  ), //      {r16|m16|mem, r16}
  ROW(2, 1, 1, 0, 28 , 6  , 0  , 0  , 0  , 0  ), // #23  {r32|m32|mem, r32}
  ROW(2, 1, 1, 0, 2  , 18 , 0  , 0  , 0  , 0  ), //      {r8lo|r8hi, m8|mem}
  ROW(2, 1, 1, 0, 4  , 21 , 0  , 0  , 0  , 0  ), //      {r16, m16|mem}
  ROW(2, 1, 1, 0, 6  , 29 , 0  , 0  , 0  , 0  ), //      {r32, m32|mem}
  ROW(2, 0, 1, 0, 8  , 30 , 0  , 0  , 0  , 0  ), //      {r64, m64|mem}
  ROW(2, 1, 1, 0, 31 , 10 , 0  , 0  , 0  , 0  ), // #28  {r8lo|r8hi|m8|r16|m16|r32|m32|r64|m64|mem, i8|u8}
  ROW(2, 1, 1, 0, 11 , 12 , 0  , 0  , 0  , 0  ), //      {r16|m16, i16|u16}
  ROW(2, 1, 1, 0, 13 , 14 , 0  , 0  , 0  , 0  ), //      {r32|m32, i32|u32}
  ROW(2, 0, 1, 0, 8  , 32 , 0  , 0  , 0  , 0  ), //      {r64, u32|i32|r64|m64|mem}
  ROW(2, 0, 1, 0, 30 , 24 , 0  , 0  , 0  , 0  ), //      {m64|mem, i32|r64}
  ROW(2, 1, 1, 0, 1  , 2  , 0  , 0  , 0  , 0  ), //      {r8lo|r8hi|m8|mem, r8lo|r8hi}
  ROW(2, 1, 1, 0, 27 , 4  , 0  , 0  , 0  , 0  ), //      {r16|m16|mem, r16}
  ROW(2, 1, 1, 0, 28 , 6  , 0  , 0  , 0  , 0  ), //      {r32|m32|mem, r32}
  ROW(2, 1, 1, 0, 2  , 18 , 0  , 0  , 0  , 0  ), //      {r8lo|r8hi, m8|mem}
  ROW(2, 1, 1, 0, 4  , 21 , 0  , 0  , 0  , 0  ), //      {r16, m16|mem}
  ROW(2, 1, 1, 0, 6  , 29 , 0  , 0  , 0  , 0  ), //      {r32, m32|mem}
  ROW(2, 1, 1, 1, 33 , 1  , 0  , 0  , 0  , 0  ), // #39  {<ax>, r8lo|r8hi|m8|mem}
  ROW(3, 1, 1, 2, 34 , 33 , 27 , 0  , 0  , 0  ), //      {<dx>, <ax>, r16|m16|mem}
  ROW(3, 1, 1, 2, 35 , 36 , 28 , 0  , 0  , 0  ), //      {<edx>, <eax>, r32|m32|mem}
  ROW(3, 0, 1, 2, 37 , 38 , 15 , 0  , 0  , 0  ), //      {<rdx>, <rax>, r64|m64|mem}
  ROW(2, 1, 1, 0, 4  , 39 , 0  , 0  , 0  , 0  ), //      {r16, r16|m16|mem|i8|i16}
  ROW(2, 1, 1, 0, 6  , 40 , 0  , 0  , 0  , 0  ), //      {r32, r32|m32|mem|i8|i32}
  ROW(2, 0, 1, 0, 8  , 41 , 0  , 0  , 0  , 0  ), //      {r64, r64|m64|mem|i8|i32}
  ROW(3, 1, 1, 0, 4  , 27 , 42 , 0  , 0  , 0  ), //      {r16, r16|m16|mem, i8|i16|u16}
  ROW(3, 1, 1, 0, 6  , 28 , 43 , 0  , 0  , 0  ), //      {r32, r32|m32|mem, i8|i32|u32}
  ROW(3, 0, 1, 0, 8  , 15 , 44 , 0  , 0  , 0  ), //      {r64, r64|m64|mem, i8|i32}
  ROW(2, 0, 1, 0, 8  , 45 , 0  , 0  , 0  , 0  ), // #49  {r64, i64|u64}
  ROW(2, 0, 1, 0, 46 , 18 , 0  , 0  , 0  , 0  ), //      {al, m8|mem}
  ROW(2, 0, 1, 0, 47 , 21 , 0  , 0  , 0  , 0  ), //      {ax, m16|mem}
  ROW(2, 0, 1, 0, 48 , 29 , 0  , 0  , 0  , 0  ), //      {eax, m32|mem}
  ROW(2, 0, 1, 0, 49 , 30 , 0  , 0  , 0  , 0  ), //      {rax, m64|mem}
  ROW(2, 0, 1, 0, 18 , 46 , 0  , 0  , 0  , 0  ), //      {m8|mem, al}
  ROW(2, 0, 1, 0, 21 , 47 , 0  , 0  , 0  , 0  ), //      {m16|mem, ax}
  ROW(2, 0, 1, 0, 29 , 48 , 0  , 0  , 0  , 0  ), //      {m32|mem, eax}
  ROW(2, 0, 1, 0, 30 , 49 , 0  , 0  , 0  , 0  ), //      {m64|mem, rax}
  ROW(2, 1, 1, 0, 1  , 2  , 0  , 0  , 0  , 0  ), // #58  {r8lo|r8hi|m8|mem, r8lo|r8hi}
  ROW(2, 1, 1, 0, 27 , 4  , 0  , 0  , 0  , 0  ), //      {r16|m16|mem, r16}
  ROW(2, 1, 1, 0, 28 , 6  , 0  , 0  , 0  , 0  ), //      {r32|m32|mem, r32}
  ROW(2, 0, 1, 0, 15 , 8  , 0  , 0  , 0  , 0  ), // #61  {r64|m64|mem, r64}
  ROW(2, 1, 1, 0, 2  , 18 , 0  , 0  , 0  , 0  ), //      {r8lo|r8hi, m8|mem}
  ROW(2, 1, 1, 0, 4  , 21 , 0  , 0  , 0  , 0  ), //      {r16, m16|mem}
  ROW(2, 1, 1, 0, 6  , 29 , 0  , 0  , 0  , 0  ), //      {r32, m32|mem}
  ROW(2, 0, 1, 0, 8  , 30 , 0  , 0  , 0  , 0  ), //      {r64, m64|mem}
  ROW(2, 1, 1, 0, 9  , 10 , 0  , 0  , 0  , 0  ), // #66  {r8lo|r8hi|m8, i8|u8}
  ROW(2, 1, 1, 0, 11 , 12 , 0  , 0  , 0  , 0  ), //      {r16|m16, i16|u16}
  ROW(2, 1, 1, 0, 13 , 14 , 0  , 0  , 0  , 0  ), //      {r32|m32, i32|u32}
  ROW(2, 0, 1, 0, 15 , 24 , 0  , 0  , 0  , 0  ), //      {r64|m64|mem, i32|r64}
  ROW(2, 1, 1, 0, 1  , 2  , 0  , 0  , 0  , 0  ), //      {r8lo|r8hi|m8|mem, r8lo|r8hi}
  ROW(2, 1, 1, 0, 27 , 4  , 0  , 0  , 0  , 0  ), //      {r16|m16|mem, r16}
  ROW(2, 1, 1, 0, 28 , 6  , 0  , 0  , 0  , 0  ), //      {r32|m32|mem, r32}
  ROW(2, 1, 1, 0, 4  , 21 , 0  , 0  , 0  , 0  ), // #73  {r16, m16|mem}
  ROW(2, 1, 1, 0, 6  , 29 , 0  , 0  , 0  , 0  ), //      {r32, m32|mem}
  ROW(2, 0, 1, 0, 8  , 30 , 0  , 0  , 0  , 0  ), //      {r64, m64|mem}
  ROW(2, 1, 1, 0, 21 , 4  , 0  , 0  , 0  , 0  ), //      {m16|mem, r16}
  ROW(2, 1, 1, 0, 29 , 6  , 0  , 0  , 0  , 0  ), // #77  {m32|mem, r32}
  ROW(2, 0, 1, 0, 30 , 8  , 0  , 0  , 0  , 0  ), //      {m64|mem, r64}
  ROW(2, 1, 1, 0, 50 , 51 , 0  , 0  , 0  , 0  ), // #79  {xmm, xmm|m128|mem}
  ROW(2, 1, 1, 0, 52 , 50 , 0  , 0  , 0  , 0  ), // #80  {m128|mem, xmm}
  ROW(2, 1, 1, 0, 53 , 54 , 0  , 0  , 0  , 0  ), //      {ymm, ymm|m256|mem}
  ROW(2, 1, 1, 0, 55 , 53 , 0  , 0  , 0  , 0  ), //      {m256|mem, ymm}
  ROW(2, 1, 1, 0, 56 , 57 , 0  , 0  , 0  , 0  ), // #83  {zmm, zmm|m512|mem}
  ROW(2, 1, 1, 0, 58 , 56 , 0  , 0  , 0  , 0  ), //      {m512|mem, zmm}
  ROW(3, 1, 1, 0, 50 , 50 , 59 , 0  , 0  , 0  ), // #85  {xmm, xmm, xmm|m128|mem|i8|u8}
  ROW(3, 1, 1, 0, 50 , 52 , 10 , 0  , 0  , 0  ), //      {xmm, m128|mem, i8|u8}
  ROW(3, 1, 1, 0, 53 , 53 , 60 , 0  , 0  , 0  ), //      {ymm, ymm, ymm|m256|mem|i8|u8}
  ROW(3, 1, 1, 0, 53 , 55 , 10 , 0  , 0  , 0  ), //      {ymm, m256|mem, i8|u8}
  ROW(3, 1, 1, 0, 56 , 56 , 61 , 0  , 0  , 0  ), //      {zmm, zmm, zmm|m512|mem|i8|u8}
  ROW(3, 1, 1, 0, 56 , 58 , 10 , 0  , 0  , 0  ), //      {zmm, m512|mem, i8|u8}
  ROW(3, 1, 1, 0, 50 , 50 , 59 , 0  , 0  , 0  ), // #91  {xmm, xmm, i8|u8|xmm|m128|mem}
  ROW(3, 1, 1, 0, 53 , 53 , 59 , 0  , 0  , 0  ), //      {ymm, ymm, i8|u8|xmm|m128|mem}
  ROW(3, 1, 1, 0, 50 , 52 , 10 , 0  , 0  , 0  ), //      {xmm, m128|mem, i8|u8}
  ROW(3, 1, 1, 0, 53 , 55 , 10 , 0  , 0  , 0  ), //      {ymm, m256|mem, i8|u8}
  ROW(3, 1, 1, 0, 56 , 56 , 59 , 0  , 0  , 0  ), //      {zmm, zmm, xmm|m128|mem|i8|u8}
  ROW(3, 1, 1, 0, 56 , 58 , 10 , 0  , 0  , 0  ), //      {zmm, m512|mem, i8|u8}
  ROW(3, 1, 1, 0, 50 , 50 , 59 , 0  , 0  , 0  ), // #97  {xmm, xmm, xmm|m128|mem|i8|u8}
  ROW(3, 1, 1, 0, 50 , 52 , 10 , 0  , 0  , 0  ), //      {xmm, m128|mem, i8|u8}
  ROW(3, 1, 1, 0, 53 , 53 , 59 , 0  , 0  , 0  ), //      {ymm, ymm, xmm|m128|mem|i8|u8}
  ROW(3, 1, 1, 0, 53 , 55 , 10 , 0  , 0  , 0  ), //      {ymm, m256|mem, i8|u8}
  ROW(3, 1, 1, 0, 56 , 56 , 59 , 0  , 0  , 0  ), //      {zmm, zmm, xmm|m128|mem|i8|u8}
  ROW(3, 1, 1, 0, 56 , 58 , 10 , 0  , 0  , 0  ), //      {zmm, m512|mem, i8|u8}
  ROW(2, 1, 1, 0, 62 , 63 , 0  , 0  , 0  , 0  ), // #103 {mm, mm|m64|mem|r64}
  ROW(2, 1, 1, 0, 15 , 64 , 0  , 0  , 0  , 0  ), //      {m64|mem|r64, mm|xmm}
  ROW(2, 0, 1, 0, 50 , 15 , 0  , 0  , 0  , 0  ), //      {xmm, r64|m64|mem}
  ROW(2, 1, 1, 0, 50 , 65 , 0  , 0  , 0  , 0  ), // #106 {xmm, xmm|m64|mem}
  ROW(2, 1, 1, 0, 30 , 50 , 0  , 0  , 0  , 0  ), // #107 {m64|mem, xmm}
  ROW(0, 1, 1, 0, 0  , 0  , 0  , 0  , 0  , 0  ), // #108 {}
  ROW(1, 1, 1, 0, 66 , 0  , 0  , 0  , 0  , 0  ), //      {r16|m16|r32|m32|r64|m64}
  ROW(2, 1, 1, 0, 27 , 4  , 0  , 0  , 0  , 0  ), //      {r16|m16|mem, r16}
  ROW(2, 1, 1, 0, 28 , 6  , 0  , 0  , 0  , 0  ), //      {r32|m32|mem, r32}
  ROW(2, 1, 1, 0, 15 , 8  , 0  , 0  , 0  , 0  ), //      {r64|m64|mem, r64}
  ROW(3, 1, 1, 0, 50 , 67 , 50 , 0  , 0  , 0  ), // #113 {xmm, vm32x, xmm}
  ROW(3, 1, 1, 0, 53 , 68 , 53 , 0  , 0  , 0  ), //      {ymm, vm32y, ymm}
  ROW(2, 1, 1, 0, 50 , 67 , 0  , 0  , 0  , 0  ), //      {xmm, vm32x}
  ROW(2, 1, 1, 0, 53 , 68 , 0  , 0  , 0  , 0  ), //      {ymm, vm32y}
  ROW(2, 1, 1, 0, 56 , 69 , 0  , 0  , 0  , 0  ), //      {zmm, vm32z}
  ROW(3, 1, 1, 0, 50 , 70 , 50 , 0  , 0  , 0  ), // #118 {xmm, vm64x, xmm}
  ROW(3, 1, 1, 0, 53 , 71 , 53 , 0  , 0  , 0  ), //      {ymm, vm64y, ymm}
  ROW(2, 1, 1, 0, 50 , 70 , 0  , 0  , 0  , 0  ), //      {xmm, vm64x}
  ROW(2, 1, 1, 0, 53 , 71 , 0  , 0  , 0  , 0  ), //      {ymm, vm64y}
  ROW(2, 1, 1, 0, 56 , 72 , 0  , 0  , 0  , 0  ), //      {zmm, vm64z}
  ROW(2, 1, 1, 0, 25 , 10 , 0  , 0  , 0  , 0  ), // #123 {r16|m16|r32|m32|r64|m64|mem, i8|u8}
  ROW(2, 1, 1, 0, 27 , 4  , 0  , 0  , 0  , 0  ), //      {r16|m16|mem, r16}
  ROW(2, 1, 1, 0, 28 , 6  , 0  , 0  , 0  , 0  ), //      {r32|m32|mem, r32}
  ROW(2, 0, 1, 0, 15 , 8  , 0  , 0  , 0  , 0  ), //      {r64|m64|mem, r64}
  ROW(2, 1, 1, 2, 73 , 74 , 0  , 0  , 0  , 0  ), // #127 {<ds:[m8|memBase|zsi]>, <es:[m8|memBase|zdi]>}
  ROW(2, 1, 1, 2, 75 , 76 , 0  , 0  , 0  , 0  ), //      {<ds:[m16|memBase|zsi]>, <es:[m16|memBase|zdi]>}
  ROW(2, 1, 1, 2, 77 , 78 , 0  , 0  , 0  , 0  ), //      {<ds:[m32|memBase|zsi]>, <es:[m32|memBase|zdi]>}
  ROW(2, 0, 1, 2, 79 , 80 , 0  , 0  , 0  , 0  ), //      {<ds:[m64|memBase|zsi]>, <es:[m64|memBase|zdi]>}
  ROW(3, 1, 1, 1, 1  , 2  , 81 , 0  , 0  , 0  ), // #131 {r8lo|r8hi|m8|mem, r8lo|r8hi, <al>}
  ROW(3, 1, 1, 1, 27 , 4  , 33 , 0  , 0  , 0  ), //      {r16|m16|mem, r16, <ax>}
  ROW(3, 1, 1, 1, 28 , 6  , 36 , 0  , 0  , 0  ), //      {r32|m32|mem, r32, <eax>}
  ROW(3, 0, 1, 1, 15 , 8  , 38 , 0  , 0  , 0  ), //      {r64|m64|mem, r64, <rax>}
  ROW(2, 1, 1, 2, 81 , 82 , 0  , 0  , 0  , 0  ), // #135 {<al>, <ds:[m8|memBase|zsi|mem]>}
  ROW(2, 1, 1, 2, 33 , 83 , 0  , 0  , 0  , 0  ), //      {<ax>, <ds:[m16|memBase|zsi|mem]>}
  ROW(2, 1, 1, 2, 36 , 84 , 0  , 0  , 0  , 0  ), //      {<eax>, <ds:[m32|memBase|zsi|mem]>}
  ROW(2, 0, 1, 2, 38 , 85 , 0  , 0  , 0  , 0  ), //      {<rax>, <ds:[m64|memBase|zsi|mem]>}
  ROW(2, 1, 1, 2, 74 , 73 , 0  , 0  , 0  , 0  ), // #139 {<es:[m8|memBase|zdi]>, <ds:[m8|memBase|zsi]>}
  ROW(2, 1, 1, 2, 76 , 75 , 0  , 0  , 0  , 0  ), //      {<es:[m16|memBase|zdi]>, <ds:[m16|memBase|zsi]>}
  ROW(2, 1, 1, 2, 78 , 77 , 0  , 0  , 0  , 0  ), //      {<es:[m32|memBase|zdi]>, <ds:[m32|memBase|zsi]>}
  ROW(2, 0, 1, 2, 80 , 79 , 0  , 0  , 0  , 0  ), //      {<es:[m64|memBase|zdi]>, <ds:[m64|memBase|zsi]>}
  ROW(1, 1, 1, 0, 86 , 0  , 0  , 0  , 0  , 0  ), // #143 {r16|m16|r64|m64}
  ROW(1, 1, 0, 0, 13 , 0  , 0  , 0  , 0  , 0  ), //      {r32|m32}
  ROW(1, 1, 0, 0, 87 , 0  , 0  , 0  , 0  , 0  ), //      {ds|es|ss}
  ROW(1, 1, 1, 0, 88 , 0  , 0  , 0  , 0  , 0  ), //      {fs|gs}
  ROW(1, 1, 1, 0, 89 , 0  , 0  , 0  , 0  , 0  ), // #147 {r16|m16|r64|m64|i8|i16|i32}
  ROW(1, 1, 0, 0, 90 , 0  , 0  , 0  , 0  , 0  ), //      {r32|m32|i32|u32}
  ROW(1, 1, 0, 0, 91 , 0  , 0  , 0  , 0  , 0  ), //      {cs|ss|ds|es}
  ROW(1, 1, 1, 0, 88 , 0  , 0  , 0  , 0  , 0  ), //      {fs|gs}
  ROW(2, 1, 1, 2, 81 , 92 , 0  , 0  , 0  , 0  ), // #151 {<al>, <es:[m8|memBase|zdi|mem]>}
  ROW(2, 1, 1, 2, 33 , 93 , 0  , 0  , 0  , 0  ), //      {<ax>, <es:[m16|memBase|zdi|mem]>}
  ROW(2, 1, 1, 2, 36 , 94 , 0  , 0  , 0  , 0  ), //      {<eax>, <es:[m32|memBase|zdi|mem]>}
  ROW(2, 0, 1, 2, 38 , 95 , 0  , 0  , 0  , 0  ), //      {<rax>, <es:[m64|memBase|zdi|mem]>}
  ROW(2, 1, 1, 2, 92 , 81 , 0  , 0  , 0  , 0  ), // #155 {<es:[m8|memBase|zdi|mem]>, <al>}
  ROW(2, 1, 1, 2, 93 , 33 , 0  , 0  , 0  , 0  ), //      {<es:[m16|memBase|zdi|mem]>, <ax>}
  ROW(2, 1, 1, 2, 94 , 36 , 0  , 0  , 0  , 0  ), //      {<es:[m32|memBase|zdi|mem]>, <eax>}
  ROW(2, 0, 1, 2, 95 , 38 , 0  , 0  , 0  , 0  ), //      {<es:[m64|memBase|zdi|mem]>, <rax>}
  ROW(4, 1, 1, 0, 50 , 50 , 50 , 51 , 0  , 0  ), // #159 {xmm, xmm, xmm, xmm|m128|mem}
  ROW(4, 1, 1, 0, 50 , 50 , 52 , 50 , 0  , 0  ), //      {xmm, xmm, m128|mem, xmm}
  ROW(4, 1, 1, 0, 53 , 53 , 53 , 54 , 0  , 0  ), //      {ymm, ymm, ymm, ymm|m256|mem}
  ROW(4, 1, 1, 0, 53 , 53 , 55 , 53 , 0  , 0  ), //      {ymm, ymm, m256|mem, ymm}
  ROW(3, 1, 1, 0, 50 , 67 , 50 , 0  , 0  , 0  ), // #163 {xmm, vm32x, xmm}
  ROW(3, 1, 1, 0, 53 , 67 , 53 , 0  , 0  , 0  ), //      {ymm, vm32x, ymm}
  ROW(2, 1, 1, 0, 96 , 67 , 0  , 0  , 0  , 0  ), //      {xmm|ymm, vm32x}
  ROW(2, 1, 1, 0, 56 , 68 , 0  , 0  , 0  , 0  ), //      {zmm, vm32y}
  ROW(3, 1, 1, 0, 52 , 50 , 50 , 0  , 0  , 0  ), // #167 {m128|mem, xmm, xmm}
  ROW(3, 1, 1, 0, 55 , 53 , 53 , 0  , 0  , 0  ), //      {m256|mem, ymm, ymm}
  ROW(3, 1, 1, 0, 50 , 50 , 52 , 0  , 0  , 0  ), //      {xmm, xmm, m128|mem}
  ROW(3, 1, 1, 0, 53 , 53 , 55 , 0  , 0  , 0  ), //      {ymm, ymm, m256|mem}
  ROW(5, 1, 1, 0, 50 , 50 , 51 , 50 , 97 , 0  ), // #171 {xmm, xmm, xmm|m128|mem, xmm, i4|u4}
  ROW(5, 1, 1, 0, 50 , 50 , 50 , 52 , 97 , 0  ), //      {xmm, xmm, xmm, m128|mem, i4|u4}
  ROW(5, 1, 1, 0, 53 , 53 , 54 , 53 , 97 , 0  ), //      {ymm, ymm, ymm|m256|mem, ymm, i4|u4}
  ROW(5, 1, 1, 0, 53 , 53 , 53 , 55 , 97 , 0  ), //      {ymm, ymm, ymm, m256|mem, i4|u4}
  ROW(3, 1, 1, 0, 53 , 54 , 10 , 0  , 0  , 0  ), // #175 {ymm, ymm|m256|mem, i8|u8}
  ROW(3, 1, 1, 0, 53 , 53 , 54 , 0  , 0  , 0  ), //      {ymm, ymm, ymm|m256|mem}
  ROW(3, 1, 1, 0, 56 , 56 , 61 , 0  , 0  , 0  ), //      {zmm, zmm, zmm|m512|mem|i8|u8}
  ROW(3, 1, 1, 0, 56 , 58 , 10 , 0  , 0  , 0  ), //      {zmm, m512|mem, i8|u8}
  ROW(2, 1, 1, 0, 4  , 27 , 0  , 0  , 0  , 0  ), // #179 {r16, r16|m16|mem}
  ROW(2, 1, 1, 0, 6  , 28 , 0  , 0  , 0  , 0  ), // #180 {r32, r32|m32|mem}
  ROW(2, 0, 1, 0, 8  , 15 , 0  , 0  , 0  , 0  ), //      {r64, r64|m64|mem}
  ROW(1, 1, 1, 0, 98 , 0  , 0  , 0  , 0  , 0  ), // #182 {m32|m64}
  ROW(2, 1, 1, 0, 99 , 100, 0  , 0  , 0  , 0  ), //      {st0, st}
  ROW(2, 1, 1, 0, 100, 99 , 0  , 0  , 0  , 0  ), //      {st, st0}
  ROW(2, 1, 1, 0, 4  , 29 , 0  , 0  , 0  , 0  ), // #185 {r16, m32|mem}
  ROW(2, 1, 1, 0, 6  , 101, 0  , 0  , 0  , 0  ), //      {r32, m48|mem}
  ROW(2, 0, 1, 0, 8  , 102, 0  , 0  , 0  , 0  ), //      {r64, m80|mem}
  ROW(3, 1, 1, 0, 27 , 4  , 103, 0  , 0  , 0  ), // #188 {r16|m16|mem, r16, cl|i8|u8}
  ROW(3, 1, 1, 0, 28 , 6  , 103, 0  , 0  , 0  ), //      {r32|m32|mem, r32, cl|i8|u8}
  ROW(3, 0, 1, 0, 15 , 8  , 103, 0  , 0  , 0  ), //      {r64|m64|mem, r64, cl|i8|u8}
  ROW(3, 1, 1, 0, 50 , 50 , 51 , 0  , 0  , 0  ), // #191 {xmm, xmm, xmm|m128|mem}
  ROW(3, 1, 1, 0, 53 , 53 , 54 , 0  , 0  , 0  ), // #192 {ymm, ymm, ymm|m256|mem}
  ROW(3, 1, 1, 0, 56 , 56 , 57 , 0  , 0  , 0  ), //      {zmm, zmm, zmm|m512|mem}
  ROW(4, 1, 1, 0, 50 , 50 , 51 , 10 , 0  , 0  ), // #194 {xmm, xmm, xmm|m128|mem, i8|u8}
  ROW(4, 1, 1, 0, 53 , 53 , 54 , 10 , 0  , 0  ), // #195 {ymm, ymm, ymm|m256|mem, i8|u8}
  ROW(4, 1, 1, 0, 56 , 56 , 57 , 10 , 0  , 0  ), //      {zmm, zmm, zmm|m512|mem, i8|u8}
  ROW(4, 1, 1, 0, 104, 50 , 51 , 10 , 0  , 0  ), // #197 {xmm|k, xmm, xmm|m128|mem, i8|u8}
  ROW(4, 1, 1, 0, 105, 53 , 54 , 10 , 0  , 0  ), //      {ymm|k, ymm, ymm|m256|mem, i8|u8}
  ROW(4, 1, 1, 0, 106, 56 , 57 , 10 , 0  , 0  ), //      {k, zmm, zmm|m512|mem, i8|u8}
  ROW(2, 1, 1, 0, 51 , 50 , 0  , 0  , 0  , 0  ), // #200 {xmm|m128|mem, xmm}
  ROW(2, 1, 1, 0, 54 , 53 , 0  , 0  , 0  , 0  ), //      {ymm|m256|mem, ymm}
  ROW(2, 1, 1, 0, 57 , 56 , 0  , 0  , 0  , 0  ), //      {zmm|m512|mem, zmm}
  ROW(2, 1, 1, 0, 50 , 65 , 0  , 0  , 0  , 0  ), // #203 {xmm, xmm|m64|mem}
  ROW(2, 1, 1, 0, 53 , 51 , 0  , 0  , 0  , 0  ), //      {ymm, xmm|m128|mem}
  ROW(2, 1, 1, 0, 56 , 54 , 0  , 0  , 0  , 0  ), //      {zmm, ymm|m256|mem}
  ROW(2, 1, 1, 0, 50 , 51 , 0  , 0  , 0  , 0  ), // #206 {xmm, xmm|m128|mem}
  ROW(2, 1, 1, 0, 53 , 54 , 0  , 0  , 0  , 0  ), //      {ymm, ymm|m256|mem}
  ROW(2, 1, 1, 0, 56 , 57 , 0  , 0  , 0  , 0  ), //      {zmm, zmm|m512|mem}
  ROW(3, 1, 1, 0, 65 , 50 , 10 , 0  , 0  , 0  ), // #209 {xmm|m64|mem, xmm, i8|u8}
  ROW(3, 1, 1, 0, 51 , 53 , 10 , 0  , 0  , 0  ), // #210 {xmm|m128|mem, ymm, i8|u8}
  ROW(3, 1, 1, 0, 54 , 56 , 10 , 0  , 0  , 0  ), // #211 {ymm|m256|mem, zmm, i8|u8}
  ROW(3, 1, 1, 0, 50 , 107, 50 , 0  , 0  , 0  ), // #212 {xmm, vm64x|vm64y, xmm}
  ROW(2, 1, 1, 0, 50 , 107, 0  , 0  , 0  , 0  ), //      {xmm, vm64x|vm64y}
  ROW(2, 1, 1, 0, 53 , 72 , 0  , 0  , 0  , 0  ), //      {ymm, vm64z}
  ROW(3, 1, 1, 0, 50 , 51 , 10 , 0  , 0  , 0  ), // #215 {xmm, xmm|m128|mem, i8|u8}
  ROW(3, 1, 1, 0, 53 , 54 , 10 , 0  , 0  , 0  ), //      {ymm, ymm|m256|mem, i8|u8}
  ROW(3, 1, 1, 0, 56 , 57 , 10 , 0  , 0  , 0  ), //      {zmm, zmm|m512|mem, i8|u8}
  ROW(2, 1, 1, 0, 50 , 65 , 0  , 0  , 0  , 0  ), // #218 {xmm, xmm|m64|mem}
  ROW(2, 1, 1, 0, 53 , 54 , 0  , 0  , 0  , 0  ), //      {ymm, ymm|m256|mem}
  ROW(2, 1, 1, 0, 56 , 57 , 0  , 0  , 0  , 0  ), //      {zmm, zmm|m512|mem}
  ROW(2, 1, 1, 0, 52 , 50 , 0  , 0  , 0  , 0  ), // #221 {m128|mem, xmm}
  ROW(2, 1, 1, 0, 55 , 53 , 0  , 0  , 0  , 0  ), //      {m256|mem, ymm}
  ROW(2, 1, 1, 0, 58 , 56 , 0  , 0  , 0  , 0  ), //      {m512|mem, zmm}
  ROW(2, 1, 1, 0, 50 , 52 , 0  , 0  , 0  , 0  ), // #224 {xmm, m128|mem}
  ROW(2, 1, 1, 0, 53 , 55 , 0  , 0  , 0  , 0  ), //      {ymm, m256|mem}
  ROW(2, 1, 1, 0, 56 , 58 , 0  , 0  , 0  , 0  ), //      {zmm, m512|mem}
  ROW(2, 0, 1, 0, 15 , 50 , 0  , 0  , 0  , 0  ), // #227 {r64|m64|mem, xmm}
  ROW(2, 1, 1, 0, 50 , 108, 0  , 0  , 0  , 0  ), //      {xmm, xmm|m64|mem|r64}
  ROW(2, 1, 1, 0, 30 , 50 , 0  , 0  , 0  , 0  ), //      {m64|mem, xmm}
  ROW(2, 1, 1, 0, 30 , 50 , 0  , 0  , 0  , 0  ), // #230 {m64|mem, xmm}
  ROW(2, 1, 1, 0, 50 , 30 , 0  , 0  , 0  , 0  ), //      {xmm, m64|mem}
  ROW(3, 1, 1, 0, 50 , 50 , 50 , 0  , 0  , 0  ), // #232 {xmm, xmm, xmm}
  ROW(2, 1, 1, 0, 29 , 50 , 0  , 0  , 0  , 0  ), // #233 {m32|mem, xmm}
  ROW(2, 1, 1, 0, 50 , 29 , 0  , 0  , 0  , 0  ), //      {xmm, m32|mem}
  ROW(3, 1, 1, 0, 50 , 50 , 50 , 0  , 0  , 0  ), //      {xmm, xmm, xmm}
  ROW(4, 1, 1, 0, 106, 106, 50 , 51 , 0  , 0  ), // #236 {k, k, xmm, xmm|m128|mem}
  ROW(4, 1, 1, 0, 106, 106, 53 , 54 , 0  , 0  ), //      {k, k, ymm, ymm|m256|mem}
  ROW(4, 1, 1, 0, 106, 106, 56 , 57 , 0  , 0  ), //      {k, k, zmm, zmm|m512|mem}
  ROW(2, 1, 1, 0, 96 , 108, 0  , 0  , 0  , 0  ), // #239 {xmm|ymm, xmm|m64|mem|r64}
  ROW(2, 0, 1, 0, 56 , 8  , 0  , 0  , 0  , 0  ), //      {zmm, r64}
  ROW(2, 1, 1, 0, 56 , 65 , 0  , 0  , 0  , 0  ), //      {zmm, xmm|m64|mem}
  ROW(4, 1, 1, 0, 106, 50 , 51 , 10 , 0  , 0  ), // #242 {k, xmm, xmm|m128|mem, i8|u8}
  ROW(4, 1, 1, 0, 106, 53 , 54 , 10 , 0  , 0  ), //      {k, ymm, ymm|m256|mem, i8|u8}
  ROW(4, 1, 1, 0, 106, 56 , 57 , 10 , 0  , 0  ), //      {k, zmm, zmm|m512|mem, i8|u8}
  ROW(3, 1, 1, 0, 104, 50 , 51 , 0  , 0  , 0  ), // #245 {xmm|k, xmm, xmm|m128|mem}
  ROW(3, 1, 1, 0, 105, 53 , 54 , 0  , 0  , 0  ), //      {ymm|k, ymm, ymm|m256|mem}
  ROW(3, 1, 1, 0, 106, 56 , 57 , 0  , 0  , 0  ), //      {k, zmm, zmm|m512|mem}
  ROW(2, 1, 1, 0, 109, 50 , 0  , 0  , 0  , 0  ), // #248 {xmm|m32|mem, xmm}
  ROW(2, 1, 1, 0, 65 , 53 , 0  , 0  , 0  , 0  ), //      {xmm|m64|mem, ymm}
  ROW(2, 1, 1, 0, 51 , 56 , 0  , 0  , 0  , 0  ), //      {xmm|m128|mem, zmm}
  ROW(2, 1, 1, 0, 65 , 50 , 0  , 0  , 0  , 0  ), // #251 {xmm|m64|mem, xmm}
  ROW(2, 1, 1, 0, 51 , 53 , 0  , 0  , 0  , 0  ), //      {xmm|m128|mem, ymm}
  ROW(2, 1, 1, 0, 54 , 56 , 0  , 0  , 0  , 0  ), //      {ymm|m256|mem, zmm}
  ROW(2, 1, 1, 0, 110, 50 , 0  , 0  , 0  , 0  ), // #254 {xmm|m16|mem, xmm}
  ROW(2, 1, 1, 0, 109, 53 , 0  , 0  , 0  , 0  ), //      {xmm|m32|mem, ymm}
  ROW(2, 1, 1, 0, 65 , 56 , 0  , 0  , 0  , 0  ), //      {xmm|m64|mem, zmm}
  ROW(2, 1, 1, 0, 50 , 109, 0  , 0  , 0  , 0  ), // #257 {xmm, xmm|m32|mem}
  ROW(2, 1, 1, 0, 53 , 65 , 0  , 0  , 0  , 0  ), //      {ymm, xmm|m64|mem}
  ROW(2, 1, 1, 0, 56 , 51 , 0  , 0  , 0  , 0  ), //      {zmm, xmm|m128|mem}
  ROW(2, 1, 1, 0, 50 , 110, 0  , 0  , 0  , 0  ), // #260 {xmm, xmm|m16|mem}
  ROW(2, 1, 1, 0, 53 , 109, 0  , 0  , 0  , 0  ), //      {ymm, xmm|m32|mem}
  ROW(2, 1, 1, 0, 56 , 65 , 0  , 0  , 0  , 0  ), //      {zmm, xmm|m64|mem}
  ROW(2, 1, 1, 0, 67 , 50 , 0  , 0  , 0  , 0  ), // #263 {vm32x, xmm}
  ROW(2, 1, 1, 0, 68 , 53 , 0  , 0  , 0  , 0  ), //      {vm32y, ymm}
  ROW(2, 1, 1, 0, 69 , 56 , 0  , 0  , 0  , 0  ), //      {vm32z, zmm}
  ROW(2, 1, 1, 0, 70 , 50 , 0  , 0  , 0  , 0  ), // #266 {vm64x, xmm}
  ROW(2, 1, 1, 0, 71 , 53 , 0  , 0  , 0  , 0  ), //      {vm64y, ymm}
  ROW(2, 1, 1, 0, 72 , 56 , 0  , 0  , 0  , 0  ), //      {vm64z, zmm}
  ROW(3, 1, 1, 0, 106, 50 , 51 , 0  , 0  , 0  ), // #269 {k, xmm, xmm|m128|mem}
  ROW(3, 1, 1, 0, 106, 53 , 54 , 0  , 0  , 0  ), //      {k, ymm, ymm|m256|mem}
  ROW(3, 1, 1, 0, 106, 56 , 57 , 0  , 0  , 0  ), //      {k, zmm, zmm|m512|mem}
  ROW(3, 1, 1, 0, 6  , 6  , 28 , 0  , 0  , 0  ), // #272 {r32, r32, r32|m32|mem}
  ROW(3, 0, 1, 0, 8  , 8  , 15 , 0  , 0  , 0  ), //      {r64, r64, r64|m64|mem}
  ROW(3, 1, 1, 0, 6  , 28 , 6  , 0  , 0  , 0  ), // #274 {r32, r32|m32|mem, r32}
  ROW(3, 0, 1, 0, 8  , 15 , 8  , 0  , 0  , 0  ), //      {r64, r64|m64|mem, r64}
  ROW(2, 1, 0, 0, 111, 28 , 0  , 0  , 0  , 0  ), // #276 {bnd, r32|m32|mem}
  ROW(2, 0, 1, 0, 111, 15 , 0  , 0  , 0  , 0  ), //      {bnd, r64|m64|mem}
  ROW(2, 1, 1, 0, 111, 112, 0  , 0  , 0  , 0  ), // #278 {bnd, bnd|mem}
  ROW(2, 1, 1, 0, 113, 111, 0  , 0  , 0  , 0  ), //      {mem, bnd}
  ROW(2, 1, 0, 0, 4  , 29 , 0  , 0  , 0  , 0  ), // #280 {r16, m32|mem}
  ROW(2, 1, 0, 0, 6  , 30 , 0  , 0  , 0  , 0  ), //      {r32, m64|mem}
  ROW(1, 1, 0, 0, 114, 0  , 0  , 0  , 0  , 0  ), // #282 {rel16|r16|m16|r32|m32}
  ROW(1, 1, 1, 0, 115, 0  , 0  , 0  , 0  , 0  ), //      {rel32|r64|m64|mem}
  ROW(2, 1, 1, 0, 6  , 116, 0  , 0  , 0  , 0  ), // #284 {r32, r8lo|r8hi|m8|r16|m16|r32|m32}
  ROW(2, 0, 1, 0, 8  , 117, 0  , 0  , 0  , 0  ), //      {r64, r8lo|r8hi|m8|r64|m64}
  ROW(1, 1, 0, 0, 118, 0  , 0  , 0  , 0  , 0  ), // #286 {r16|r32}
  ROW(1, 1, 1, 0, 31 , 0  , 0  , 0  , 0  , 0  ), // #287 {r8lo|r8hi|m8|r16|m16|r32|m32|r64|m64|mem}
  ROW(2, 1, 0, 0, 119, 58 , 0  , 0  , 0  , 0  ), // #288 {es:[mem|m512|memBase], m512|mem}
  ROW(2, 0, 1, 0, 119, 58 , 0  , 0  , 0  , 0  ), //      {es:[mem|m512|memBase], m512|mem}
  ROW(3, 1, 1, 0, 50 , 10 , 10 , 0  , 0  , 0  ), // #290 {xmm, i8|u8, i8|u8}
  ROW(2, 1, 1, 0, 50 , 50 , 0  , 0  , 0  , 0  ), // #291 {xmm, xmm}
  ROW(0, 1, 1, 0, 0  , 0  , 0  , 0  , 0  , 0  ), // #292 {}
  ROW(1, 1, 1, 0, 100, 0  , 0  , 0  , 0  , 0  ), // #293 {st}
  ROW(0, 1, 1, 0, 0  , 0  , 0  , 0  , 0  , 0  ), // #294 {}
  ROW(1, 1, 1, 0, 120, 0  , 0  , 0  , 0  , 0  ), // #295 {m32|m64|st}
  ROW(2, 1, 1, 0, 50 , 50 , 0  , 0  , 0  , 0  ), // #296 {xmm, xmm}
  ROW(4, 1, 1, 0, 50 , 50 , 10 , 10 , 0  , 0  ), //      {xmm, xmm, i8|u8, i8|u8}
  ROW(2, 1, 0, 0, 6  , 52 , 0  , 0  , 0  , 0  ), // #298 {r32, m128|mem}
  ROW(2, 0, 1, 0, 8  , 52 , 0  , 0  , 0  , 0  ), //      {r64, m128|mem}
  ROW(2, 1, 0, 2, 36 , 121, 0  , 0  , 0  , 0  ), // #300 {<eax>, <ecx>}
  ROW(2, 0, 1, 2, 122, 121, 0  , 0  , 0  , 0  ), //      {<eax|rax>, <ecx>}
  ROW(1, 1, 1, 0, 123, 0  , 0  , 0  , 0  , 0  ), // #302 {rel8|rel32}
  ROW(1, 1, 0, 0, 124, 0  , 0  , 0  , 0  , 0  ), //      {rel16}
  ROW(2, 1, 0, 1, 125, 126, 0  , 0  , 0  , 0  ), // #304 {<cx|ecx>, rel8}
  ROW(2, 0, 1, 1, 127, 126, 0  , 0  , 0  , 0  ), //      {<ecx|rcx>, rel8}
  ROW(1, 1, 1, 0, 128, 0  , 0  , 0  , 0  , 0  ), // #306 {rel8|rel32|r64|m64|mem}
  ROW(1, 1, 0, 0, 129, 0  , 0  , 0  , 0  , 0  ), //      {rel16|r32|m32|mem}
  ROW(2, 1, 1, 0, 106, 130, 0  , 0  , 0  , 0  ), // #308 {k, k|m8|mem|r32}
  ROW(2, 1, 1, 0, 131, 106, 0  , 0  , 0  , 0  ), //      {m8|mem|r32, k}
  ROW(2, 1, 1, 0, 106, 132, 0  , 0  , 0  , 0  ), // #310 {k, k|m32|mem|r32}
  ROW(2, 1, 1, 0, 28 , 106, 0  , 0  , 0  , 0  ), //      {m32|mem|r32, k}
  ROW(2, 1, 1, 0, 106, 133, 0  , 0  , 0  , 0  ), // #312 {k, k|m64|mem|r64}
  ROW(2, 1, 1, 0, 15 , 106, 0  , 0  , 0  , 0  ), //      {m64|mem|r64, k}
  ROW(2, 1, 1, 0, 106, 134, 0  , 0  , 0  , 0  ), // #314 {k, k|m16|mem|r32}
  ROW(2, 1, 1, 0, 135, 106, 0  , 0  , 0  , 0  ), //      {m16|mem|r32, k}
  ROW(2, 1, 1, 0, 4  , 27 , 0  , 0  , 0  , 0  ), // #316 {r16, r16|m16|mem}
  ROW(2, 1, 1, 0, 6  , 135, 0  , 0  , 0  , 0  ), //      {r32, r32|m16|mem}
  ROW(2, 1, 0, 0, 136, 137, 0  , 0  , 0  , 0  ), // #318 {i16, i16|i32}
  ROW(1, 1, 1, 0, 138, 0  , 0  , 0  , 0  , 0  ), //      {m32|m48|m80|mem}
  ROW(2, 1, 0, 0, 4  , 29 , 0  , 0  , 0  , 0  ), // #320 {r16, m32|mem}
  ROW(2, 1, 0, 0, 6  , 101, 0  , 0  , 0  , 0  ), //      {r32, m48|mem}
  ROW(2, 1, 1, 0, 4  , 27 , 0  , 0  , 0  , 0  ), // #322 {r16, r16|m16|mem}
  ROW(2, 1, 1, 0, 139, 135, 0  , 0  , 0  , 0  ), //      {r32|r64, r32|m16|mem}
  ROW(2, 1, 1, 0, 64 , 28 , 0  , 0  , 0  , 0  ), // #324 {mm|xmm, r32|m32|mem}
  ROW(2, 1, 1, 0, 28 , 64 , 0  , 0  , 0  , 0  ), //      {r32|m32|mem, mm|xmm}
  ROW(2, 1, 1, 0, 50 , 109, 0  , 0  , 0  , 0  ), // #326 {xmm, xmm|m32|mem}
  ROW(2, 1, 1, 0, 29 , 50 , 0  , 0  , 0  , 0  ), //      {m32|mem, xmm}
  ROW(2, 1, 1, 0, 4  , 9  , 0  , 0  , 0  , 0  ), // #328 {r16, r8lo|r8hi|m8}
  ROW(2, 1, 1, 0, 139, 140, 0  , 0  , 0  , 0  ), //      {r32|r64, r8lo|r8hi|m8|r16|m16}
  ROW(4, 1, 1, 1, 6  , 6  , 28 , 35 , 0  , 0  ), // #330 {r32, r32, r32|m32|mem, <edx>}
  ROW(4, 0, 1, 1, 8  , 8  , 15 , 37 , 0  , 0  ), //      {r64, r64, r64|m64|mem, <rdx>}
  ROW(2, 1, 1, 0, 62 , 141, 0  , 0  , 0  , 0  ), // #332 {mm, mm|m64|mem}
  ROW(2, 1, 1, 0, 50 , 51 , 0  , 0  , 0  , 0  ), //      {xmm, xmm|m128|mem}
  ROW(3, 1, 1, 0, 62 , 141, 10 , 0  , 0  , 0  ), // #334 {mm, mm|m64|mem, i8|u8}
  ROW(3, 1, 1, 0, 50 , 51 , 10 , 0  , 0  , 0  ), //      {xmm, xmm|m128|mem, i8|u8}
  ROW(3, 1, 1, 0, 6  , 64 , 10 , 0  , 0  , 0  ), // #336 {r32, mm|xmm, i8|u8}
  ROW(3, 1, 1, 0, 21 , 50 , 10 , 0  , 0  , 0  ), //      {m16|mem, xmm, i8|u8}
  ROW(2, 1, 1, 0, 62 , 142, 0  , 0  , 0  , 0  ), // #338 {mm, i8|u8|mm|m64|mem}
  ROW(2, 1, 1, 0, 50 , 59 , 0  , 0  , 0  , 0  ), //      {xmm, i8|u8|xmm|m128|mem}
  ROW(2, 1, 1, 0, 62 , 143, 0  , 0  , 0  , 0  ), // #340 {mm, mm|m32|mem}
  ROW(2, 1, 1, 0, 50 , 51 , 0  , 0  , 0  , 0  ), //      {xmm, xmm|m128|mem}
  ROW(1, 1, 0, 0, 6  , 0  , 0  , 0  , 0  , 0  ), // #342 {r32}
  ROW(1, 0, 1, 0, 8  , 0  , 0  , 0  , 0  , 0  ), // #343 {r64}
  ROW(0, 1, 1, 0, 0  , 0  , 0  , 0  , 0  , 0  ), // #344 {}
  ROW(1, 1, 1, 0, 144, 0  , 0  , 0  , 0  , 0  ), //      {u16}
  ROW(3, 1, 1, 0, 6  , 28 , 10 , 0  , 0  , 0  ), // #346 {r32, r32|m32|mem, i8|u8}
  ROW(3, 0, 1, 0, 8  , 15 , 10 , 0  , 0  , 0  ), //      {r64, r64|m64|mem, i8|u8}
  ROW(4, 1, 1, 0, 50 , 50 , 51 , 50 , 0  , 0  ), // #348 {xmm, xmm, xmm|m128|mem, xmm}
  ROW(4, 1, 1, 0, 53 , 53 , 54 , 53 , 0  , 0  ), //      {ymm, ymm, ymm|m256|mem, ymm}
  ROW(2, 1, 1, 0, 50 , 145, 0  , 0  , 0  , 0  ), // #350 {xmm, xmm|m128|ymm|m256}
  ROW(2, 1, 1, 0, 53 , 57 , 0  , 0  , 0  , 0  ), //      {ymm, zmm|m512|mem}
  ROW(4, 1, 1, 0, 50 , 50 , 50 , 65 , 0  , 0  ), // #352 {xmm, xmm, xmm, xmm|m64|mem}
  ROW(4, 1, 1, 0, 50 , 50 , 30 , 50 , 0  , 0  ), //      {xmm, xmm, m64|mem, xmm}
  ROW(4, 1, 1, 0, 50 , 50 , 50 , 109, 0  , 0  ), // #354 {xmm, xmm, xmm, xmm|m32|mem}
  ROW(4, 1, 1, 0, 50 , 50 , 29 , 50 , 0  , 0  ), //      {xmm, xmm, m32|mem, xmm}
  ROW(4, 1, 1, 0, 53 , 53 , 51 , 10 , 0  , 0  ), // #356 {ymm, ymm, xmm|m128|mem, i8|u8}
  ROW(4, 1, 1, 0, 56 , 56 , 51 , 10 , 0  , 0  ), //      {zmm, zmm, xmm|m128|mem, i8|u8}
  ROW(1, 1, 0, 1, 36 , 0  , 0  , 0  , 0  , 0  ), // #358 {<eax>}
  ROW(1, 0, 1, 1, 38 , 0  , 0  , 0  , 0  , 0  ), // #359 {<rax>}
  ROW(2, 1, 1, 0, 28 , 50 , 0  , 0  , 0  , 0  ), // #360 {r32|m32|mem, xmm}
  ROW(2, 1, 1, 0, 50 , 28 , 0  , 0  , 0  , 0  ), //      {xmm, r32|m32|mem}
  ROW(2, 1, 1, 0, 30 , 50 , 0  , 0  , 0  , 0  ), // #362 {m64|mem, xmm}
  ROW(3, 1, 1, 0, 50 , 50 , 30 , 0  , 0  , 0  ), //      {xmm, xmm, m64|mem}
  ROW(2, 1, 0, 0, 28 , 6  , 0  , 0  , 0  , 0  ), // #364 {r32|m32|mem, r32}
  ROW(2, 0, 1, 0, 15 , 8  , 0  , 0  , 0  , 0  ), //      {r64|m64|mem, r64}
  ROW(2, 1, 0, 0, 6  , 28 , 0  , 0  , 0  , 0  ), // #366 {r32, r32|m32|mem}
  ROW(2, 0, 1, 0, 8  , 15 , 0  , 0  , 0  , 0  ), //      {r64, r64|m64|mem}
  ROW(3, 1, 1, 0, 50 , 50 , 59 , 0  , 0  , 0  ), // #368 {xmm, xmm, xmm|m128|mem|i8|u8}
  ROW(3, 1, 1, 0, 50 , 52 , 146, 0  , 0  , 0  ), //      {xmm, m128|mem, i8|u8|xmm}
  ROW(2, 1, 1, 0, 67 , 96 , 0  , 0  , 0  , 0  ), // #370 {vm32x, xmm|ymm}
  ROW(2, 1, 1, 0, 68 , 56 , 0  , 0  , 0  , 0  ), //      {vm32y, zmm}
  ROW(2, 1, 1, 0, 107, 50 , 0  , 0  , 0  , 0  ), // #372 {vm64x|vm64y, xmm}
  ROW(2, 1, 1, 0, 72 , 53 , 0  , 0  , 0  , 0  ), //      {vm64z, ymm}
  ROW(3, 1, 1, 0, 50 , 50 , 51 , 0  , 0  , 0  ), // #374 {xmm, xmm, xmm|m128|mem}
  ROW(3, 1, 1, 0, 50 , 52 , 50 , 0  , 0  , 0  ), //      {xmm, m128|mem, xmm}
  ROW(1, 1, 0, 1, 33 , 0  , 0  , 0  , 0  , 0  ), // #376 {<ax>}
  ROW(2, 1, 0, 1, 33 , 10 , 0  , 0  , 0  , 0  ), // #377 {<ax>, i8|u8}
  ROW(2, 1, 0, 0, 27 , 4  , 0  , 0  , 0  , 0  ), // #378 {r16|m16|mem, r16}
  ROW(3, 1, 1, 1, 50 , 51 , 147, 0  , 0  , 0  ), // #379 {xmm, xmm|m128|mem, <xmm0>}
  ROW(2, 1, 1, 0, 111, 148, 0  , 0  , 0  , 0  ), // #380 {bnd, mib}
  ROW(2, 1, 1, 0, 111, 113, 0  , 0  , 0  , 0  ), // #381 {bnd, mem}
  ROW(2, 1, 1, 0, 148, 111, 0  , 0  , 0  , 0  ), // #382 {mib, bnd}
  ROW(1, 1, 1, 0, 149, 0  , 0  , 0  , 0  , 0  ), // #383 {r16|r32|r64}
  ROW(1, 1, 1, 1, 33 , 0  , 0  , 0  , 0  , 0  ), // #384 {<ax>}
  ROW(2, 1, 1, 2, 35 , 36 , 0  , 0  , 0  , 0  ), // #385 {<edx>, <eax>}
  ROW(1, 1, 1, 0, 150, 0  , 0  , 0  , 0  , 0  ), // #386 {mem|m8|m16|m32|m48|m64|m80|m128|m256|m512|m1024}
  ROW(1, 1, 1, 0, 30 , 0  , 0  , 0  , 0  , 0  ), // #387 {m64|mem}
  ROW(0, 0, 1, 0, 0  , 0  , 0  , 0  , 0  , 0  ), // #388 {}
  ROW(1, 1, 1, 1, 151, 0  , 0  , 0  , 0  , 0  ), // #389 {<ds:[mem|m512|memBase|zax]>}
  ROW(3, 1, 1, 0, 50 , 65 , 10 , 0  , 0  , 0  ), // #390 {xmm, xmm|m64|mem, i8|u8}
  ROW(3, 1, 1, 0, 50 , 109, 10 , 0  , 0  , 0  ), // #391 {xmm, xmm|m32|mem, i8|u8}
  ROW(5, 0, 1, 4, 52 , 37 , 38 , 152, 153, 0  ), // #392 {m128|mem, <rdx>, <rax>, <rcx>, <rbx>}
  ROW(5, 1, 1, 4, 30 , 35 , 36 , 121, 154, 0  ), // #393 {m64|mem, <edx>, <eax>, <ecx>, <ebx>}
  ROW(4, 1, 1, 4, 36 , 154, 121, 35 , 0  , 0  ), // #394 {<eax>, <ebx>, <ecx>, <edx>}
  ROW(2, 0, 1, 2, 37 , 38 , 0  , 0  , 0  , 0  ), // #395 {<rdx>, <rax>}
  ROW(2, 1, 1, 0, 62 , 51 , 0  , 0  , 0  , 0  ), // #396 {mm, xmm|m128|mem}
  ROW(2, 1, 1, 0, 50 , 141, 0  , 0  , 0  , 0  ), // #397 {xmm, mm|m64|mem}
  ROW(2, 1, 1, 0, 62 , 65 , 0  , 0  , 0  , 0  ), // #398 {mm, xmm|m64|mem}
  ROW(2, 1, 1, 0, 139, 65 , 0  , 0  , 0  , 0  ), // #399 {r32|r64, xmm|m64|mem}
  ROW(2, 1, 1, 0, 50 , 155, 0  , 0  , 0  , 0  ), // #400 {xmm, r32|m32|mem|r64|m64}
  ROW(2, 1, 1, 0, 139, 109, 0  , 0  , 0  , 0  ), // #401 {r32|r64, xmm|m32|mem}
  ROW(2, 1, 1, 2, 34 , 33 , 0  , 0  , 0  , 0  ), // #402 {<dx>, <ax>}
  ROW(1, 1, 1, 1, 36 , 0  , 0  , 0  , 0  , 0  ), // #403 {<eax>}
  ROW(2, 1, 1, 0, 12 , 10 , 0  , 0  , 0  , 0  ), // #404 {i16|u16, i8|u8}
  ROW(3, 1, 1, 0, 28 , 50 , 10 , 0  , 0  , 0  ), // #405 {r32|m32|mem, xmm, i8|u8}
  ROW(1, 1, 1, 0, 102, 0  , 0  , 0  , 0  , 0  ), // #406 {m80|mem}
  ROW(1, 1, 1, 0, 156, 0  , 0  , 0  , 0  , 0  ), // #407 {m16|m32}
  ROW(1, 1, 1, 0, 157, 0  , 0  , 0  , 0  , 0  ), // #408 {m16|m32|m64}
  ROW(1, 1, 1, 0, 158, 0  , 0  , 0  , 0  , 0  ), // #409 {m32|m64|m80|st}
  ROW(1, 1, 1, 0, 21 , 0  , 0  , 0  , 0  , 0  ), // #410 {m16|mem}
  ROW(1, 1, 1, 0, 113, 0  , 0  , 0  , 0  , 0  ), // #411 {mem}
  ROW(1, 1, 1, 0, 159, 0  , 0  , 0  , 0  , 0  ), // #412 {ax|m16|mem}
  ROW(1, 0, 1, 0, 113, 0  , 0  , 0  , 0  , 0  ), // #413 {mem}
  ROW(2, 1, 1, 1, 10 , 36 , 0  , 0  , 0  , 0  ), // #414 {i8|u8, <eax>}
  ROW(2, 1, 1, 0, 160, 161, 0  , 0  , 0  , 0  ), // #415 {al|ax|eax, i8|u8|dx}
  ROW(1, 1, 1, 0, 6  , 0  , 0  , 0  , 0  , 0  ), // #416 {r32}
  ROW(2, 1, 1, 0, 162, 163, 0  , 0  , 0  , 0  ), // #417 {es:[m8|memBase|zdi|m16|m32], dx}
  ROW(1, 1, 1, 0, 10 , 0  , 0  , 0  , 0  , 0  ), // #418 {i8|u8}
  ROW(0, 1, 0, 0, 0  , 0  , 0  , 0  , 0  , 0  ), // #419 {}
  ROW(3, 1, 1, 0, 106, 106, 106, 0  , 0  , 0  ), // #420 {k, k, k}
  ROW(2, 1, 1, 0, 106, 106, 0  , 0  , 0  , 0  ), // #421 {k, k}
  ROW(3, 1, 1, 0, 106, 106, 10 , 0  , 0  , 0  ), // #422 {k, k, i8|u8}
  ROW(1, 1, 1, 1, 164, 0  , 0  , 0  , 0  , 0  ), // #423 {<ah>}
  ROW(1, 1, 1, 0, 29 , 0  , 0  , 0  , 0  , 0  ), // #424 {m32|mem}
  ROW(1, 0, 1, 0, 58 , 0  , 0  , 0  , 0  , 0  ), // #425 {m512|mem}
  ROW(2, 1, 1, 0, 149, 150, 0  , 0  , 0  , 0  ), // #426 {r16|r32|r64, mem|m8|m16|m32|m48|m64|m80|m128|m256|m512|m1024}
  ROW(1, 1, 1, 0, 27 , 0  , 0  , 0  , 0  , 0  ), // #427 {r16|m16|mem}
  ROW(1, 1, 1, 0, 139, 0  , 0  , 0  , 0  , 0  ), // #428 {r32|r64}
  ROW(3, 1, 1, 0, 139, 28 , 14 , 0  , 0  , 0  ), // #429 {r32|r64, r32|m32|mem, i32|u32}
  ROW(3, 1, 1, 1, 50 , 50 , 165, 0  , 0  , 0  ), // #430 {xmm, xmm, <ds:[mem|m128|memBase|zdi]>}
  ROW(3, 1, 1, 1, 62 , 62 , 166, 0  , 0  , 0  ), // #431 {mm, mm, <ds:[mem|m64|memBase|zdi]>}
  ROW(3, 1, 1, 3, 167, 121, 35 , 0  , 0  , 0  ), // #432 {<ds:[mem|memBase|zax]>, <ecx>, <edx>}
  ROW(2, 1, 1, 0, 119, 58 , 0  , 0  , 0  , 0  ), // #433 {es:[mem|m512|memBase], m512|mem}
  ROW(2, 1, 1, 0, 62 , 50 , 0  , 0  , 0  , 0  ), // #434 {mm, xmm}
  ROW(2, 1, 1, 0, 6  , 50 , 0  , 0  , 0  , 0  ), // #435 {r32, xmm}
  ROW(2, 1, 1, 0, 30 , 62 , 0  , 0  , 0  , 0  ), // #436 {m64|mem, mm}
  ROW(2, 1, 1, 0, 50 , 62 , 0  , 0  , 0  , 0  ), // #437 {xmm, mm}
  ROW(2, 0, 1, 0, 149, 28 , 0  , 0  , 0  , 0  ), // #438 {r16|r32|r64, r32|m32|mem}
  ROW(2, 1, 1, 2, 36 , 121, 0  , 0  , 0  , 0  ), // #439 {<eax>, <ecx>}
  ROW(3, 1, 1, 3, 36 , 121, 154, 0  , 0  , 0  ), // #440 {<eax>, <ecx>, <ebx>}
  ROW(2, 1, 1, 0, 168, 160, 0  , 0  , 0  , 0  ), // #441 {u8|dx, al|ax|eax}
  ROW(2, 1, 1, 0, 163, 169, 0  , 0  , 0  , 0  ), // #442 {dx, ds:[m8|memBase|zsi|m16|m32]}
  ROW(6, 1, 1, 3, 50 , 51 , 10 , 121, 36 , 35 ), // #443 {xmm, xmm|m128|mem, i8|u8, <ecx>, <eax>, <edx>}
  ROW(6, 1, 1, 3, 50 , 51 , 10 , 147, 36 , 35 ), // #444 {xmm, xmm|m128|mem, i8|u8, <xmm0>, <eax>, <edx>}
  ROW(4, 1, 1, 1, 50 , 51 , 10 , 121, 0  , 0  ), // #445 {xmm, xmm|m128|mem, i8|u8, <ecx>}
  ROW(4, 1, 1, 1, 50 , 51 , 10 , 147, 0  , 0  ), // #446 {xmm, xmm|m128|mem, i8|u8, <xmm0>}
  ROW(3, 1, 1, 0, 131, 50 , 10 , 0  , 0  , 0  ), // #447 {r32|m8|mem, xmm, i8|u8}
  ROW(3, 0, 1, 0, 15 , 50 , 10 , 0  , 0  , 0  ), // #448 {r64|m64|mem, xmm, i8|u8}
  ROW(3, 1, 1, 0, 50 , 131, 10 , 0  , 0  , 0  ), // #449 {xmm, r32|m8|mem, i8|u8}
  ROW(3, 1, 1, 0, 50 , 28 , 10 , 0  , 0  , 0  ), // #450 {xmm, r32|m32|mem, i8|u8}
  ROW(3, 0, 1, 0, 50 , 15 , 10 , 0  , 0  , 0  ), // #451 {xmm, r64|m64|mem, i8|u8}
  ROW(3, 1, 1, 0, 64 , 135, 10 , 0  , 0  , 0  ), // #452 {mm|xmm, r32|m16|mem, i8|u8}
  ROW(2, 1, 1, 0, 6  , 64 , 0  , 0  , 0  , 0  ), // #453 {r32, mm|xmm}
  ROW(2, 1, 1, 0, 50 , 10 , 0  , 0  , 0  , 0  ), // #454 {xmm, i8|u8}
  ROW(1, 1, 1, 0, 155, 0  , 0  , 0  , 0  , 0  ), // #455 {r32|m32|mem|r64|m64}
  ROW(2, 1, 1, 0, 31 , 103, 0  , 0  , 0  , 0  ), // #456 {r8lo|r8hi|m8|r16|m16|r32|m32|r64|m64|mem, cl|i8|u8}
  ROW(1, 0, 1, 0, 139, 0  , 0  , 0  , 0  , 0  ), // #457 {r32|r64}
  ROW(3, 1, 1, 3, 35 , 36 , 121, 0  , 0  , 0  ), // #458 {<edx>, <eax>, <ecx>}
  ROW(1, 1, 1, 0, 1  , 0  , 0  , 0  , 0  , 0  ), // #459 {r8lo|r8hi|m8|mem}
  ROW(1, 1, 1, 0, 170, 0  , 0  , 0  , 0  , 0  ), // #460 {r16|m16|mem|r32|r64}
  ROW(3, 0, 1, 0, 171, 171, 171, 0  , 0  , 0  ), // #461 {tmm, tmm, tmm}
  ROW(2, 0, 1, 0, 171, 113, 0  , 0  , 0  , 0  ), // #462 {tmm, tmem}
  ROW(2, 0, 1, 0, 113, 171, 0  , 0  , 0  , 0  ), // #463 {tmem, tmm}
  ROW(1, 0, 1, 0, 171, 0  , 0  , 0  , 0  , 0  ), // #464 {tmm}
  ROW(3, 1, 1, 2, 6  , 35 , 36 , 0  , 0  , 0  ), // #465 {r32, <edx>, <eax>}
  ROW(1, 1, 1, 0, 172, 0  , 0  , 0  , 0  , 0  ), // #466 {ds:[mem|memBase]}
  ROW(6, 1, 1, 0, 56 , 56 , 56 , 56 , 56 , 52 ), // #467 {zmm, zmm, zmm, zmm, zmm, m128|mem}
  ROW(6, 1, 1, 0, 50 , 50 , 50 , 50 , 50 , 52 ), // #468 {xmm, xmm, xmm, xmm, xmm, m128|mem}
  ROW(3, 1, 1, 0, 50 , 50 , 65 , 0  , 0  , 0  ), // #469 {xmm, xmm, xmm|m64|mem}
  ROW(3, 1, 1, 0, 50 , 50 , 109, 0  , 0  , 0  ), // #470 {xmm, xmm, xmm|m32|mem}
  ROW(2, 1, 1, 0, 53 , 52 , 0  , 0  , 0  , 0  ), // #471 {ymm, m128|mem}
  ROW(2, 1, 1, 0, 173, 65 , 0  , 0  , 0  , 0  ), // #472 {ymm|zmm, xmm|m64|mem}
  ROW(2, 1, 1, 0, 173, 52 , 0  , 0  , 0  , 0  ), // #473 {ymm|zmm, m128|mem}
  ROW(2, 1, 1, 0, 56 , 55 , 0  , 0  , 0  , 0  ), // #474 {zmm, m256|mem}
  ROW(2, 1, 1, 0, 174, 65 , 0  , 0  , 0  , 0  ), // #475 {xmm|ymm|zmm, xmm|m64|mem}
  ROW(2, 1, 1, 0, 174, 109, 0  , 0  , 0  , 0  ), // #476 {xmm|ymm|zmm, m32|mem|xmm}
  ROW(4, 1, 1, 0, 104, 50 , 65 , 10 , 0  , 0  ), // #477 {xmm|k, xmm, xmm|m64|mem, i8|u8}
  ROW(4, 1, 1, 0, 104, 50 , 109, 10 , 0  , 0  ), // #478 {xmm|k, xmm, xmm|m32|mem, i8|u8}
  ROW(3, 1, 1, 0, 50 , 50 , 155, 0  , 0  , 0  ), // #479 {xmm, xmm, r32|m32|mem|r64|m64}
  ROW(3, 1, 1, 0, 51 , 173, 10 , 0  , 0  , 0  ), // #480 {xmm|m128|mem, ymm|zmm, i8|u8}
  ROW(4, 1, 1, 0, 50 , 50 , 65 , 10 , 0  , 0  ), // #481 {xmm, xmm, xmm|m64|mem, i8|u8}
  ROW(4, 1, 1, 0, 50 , 50 , 109, 10 , 0  , 0  ), // #482 {xmm, xmm, xmm|m32|mem, i8|u8}
  ROW(3, 1, 1, 0, 106, 175, 10 , 0  , 0  , 0  ), // #483 {k, xmm|m128|ymm|m256|zmm|m512, i8|u8}
  ROW(3, 1, 1, 0, 106, 65 , 10 , 0  , 0  , 0  ), // #484 {k, xmm|m64|mem, i8|u8}
  ROW(3, 1, 1, 0, 106, 109, 10 , 0  , 0  , 0  ), // #485 {k, xmm|m32|mem, i8|u8}
  ROW(1, 1, 1, 0, 68 , 0  , 0  , 0  , 0  , 0  ), // #486 {vm32y}
  ROW(1, 1, 1, 0, 69 , 0  , 0  , 0  , 0  , 0  ), // #487 {vm32z}
  ROW(1, 1, 1, 0, 72 , 0  , 0  , 0  , 0  , 0  ), // #488 {vm64z}
  ROW(4, 1, 1, 0, 56 , 56 , 54 , 10 , 0  , 0  ), // #489 {zmm, zmm, ymm|m256|mem, i8|u8}
  ROW(2, 1, 1, 0, 6  , 96 , 0  , 0  , 0  , 0  ), // #490 {r32, xmm|ymm}
  ROW(2, 1, 1, 0, 174, 176, 0  , 0  , 0  , 0  ), // #491 {xmm|ymm|zmm, xmm|m8|mem|r32}
  ROW(2, 1, 1, 0, 174, 177, 0  , 0  , 0  , 0  ), // #492 {xmm|ymm|zmm, xmm|m32|mem|r32}
  ROW(2, 1, 1, 0, 174, 106, 0  , 0  , 0  , 0  ), // #493 {xmm|ymm|zmm, k}
  ROW(2, 1, 1, 0, 174, 178, 0  , 0  , 0  , 0  ), // #494 {xmm|ymm|zmm, xmm|m16|mem|r32}
  ROW(3, 1, 1, 0, 135, 50 , 10 , 0  , 0  , 0  ), // #495 {r32|m16|mem, xmm, i8|u8}
  ROW(4, 1, 1, 0, 50 , 50 , 131, 10 , 0  , 0  ), // #496 {xmm, xmm, r32|m8|mem, i8|u8}
  ROW(4, 1, 1, 0, 50 , 50 , 28 , 10 , 0  , 0  ), // #497 {xmm, xmm, r32|m32|mem, i8|u8}
  ROW(4, 0, 1, 0, 50 , 50 , 15 , 10 , 0  , 0  ), // #498 {xmm, xmm, r64|m64|mem, i8|u8}
  ROW(4, 1, 1, 0, 50 , 50 , 135, 10 , 0  , 0  ), // #499 {xmm, xmm, r32|m16|mem, i8|u8}
  ROW(2, 1, 1, 0, 106, 174, 0  , 0  , 0  , 0  ), // #500 {k, xmm|ymm|zmm}
  ROW(1, 1, 1, 0, 124, 0  , 0  , 0  , 0  , 0  ), // #501 {rel16|rel32}
  ROW(3, 1, 1, 2, 113, 35 , 36 , 0  , 0  , 0  ), // #502 {mem, <edx>, <eax>}
  ROW(3, 0, 1, 2, 113, 35 , 36 , 0  , 0  , 0  )  // #503 {mem, <edx>, <eax>}
};
#undef ROW

#define ROW(flags, mFlags, extFlags, regId) { uint32_t(flags), uint16_t(mFlags), uint8_t(extFlags), uint8_t(regId) }
#define F(VAL) InstDB::kOp##VAL
#define M(VAL) InstDB::kMemOp##VAL
const InstDB::OpSignature InstDB::_opSignatureTable[] = {
  ROW(0, 0, 0, 0xFF),
  ROW(F(GpbLo) | F(GpbHi) | F(Mem), M(M8) | M(Any), 0, 0x00),
  ROW(F(GpbLo) | F(GpbHi), 0, 0, 0x00),
  ROW(F(Gpw) | F(SReg) | F(Mem), M(M16) | M(Any), 0, 0x00),
  ROW(F(Gpw), 0, 0, 0x00),
  ROW(F(Gpd) | F(SReg) | F(Mem), M(M32) | M(Any), 0, 0x00),
  ROW(F(Gpd), 0, 0, 0x00),
  ROW(F(Gpq) | F(SReg) | F(CReg) | F(DReg) | F(Mem), M(M64) | M(Any), 0, 0x00),
  ROW(F(Gpq), 0, 0, 0x00),
  ROW(F(GpbLo) | F(GpbHi) | F(Mem), M(M8), 0, 0x00),
  ROW(F(I8) | F(U8), 0, 0, 0x00),
  ROW(F(Gpw) | F(Mem), M(M16), 0, 0x00),
  ROW(F(I16) | F(U16), 0, 0, 0x00),
  ROW(F(Gpd) | F(Mem), M(M32), 0, 0x00),
  ROW(F(I32) | F(U32), 0, 0, 0x00),
  ROW(F(Gpq) | F(Mem), M(M64) | M(Any), 0, 0x00),
  ROW(F(I32), 0, 0, 0x00),
  ROW(F(SReg) | F(CReg) | F(DReg) | F(Mem) | F(I64) | F(U64), M(M64) | M(Any), 0, 0x00),
  ROW(F(Mem), M(M8) | M(Any), 0, 0x00),
  ROW(F(SReg) | F(Mem), M(M16) | M(Any), 0, 0x00),
  ROW(F(SReg) | F(Mem), M(M32) | M(Any), 0, 0x00),
  ROW(F(Mem), M(M16) | M(Any), 0, 0x00),
  ROW(F(SReg), 0, 0, 0x00),
  ROW(F(CReg) | F(DReg), 0, 0, 0x00),
  ROW(F(Gpq) | F(I32), 0, 0, 0x00),
  ROW(F(Gpw) | F(Gpd) | F(Gpq) | F(Mem), M(M16) | M(M32) | M(M64) | M(Any), 0, 0x00),
  ROW(F(I8), 0, 0, 0x00),
  ROW(F(Gpw) | F(Mem), M(M16) | M(Any), 0, 0x00),
  ROW(F(Gpd) | F(Mem), M(M32) | M(Any), 0, 0x00),
  ROW(F(Mem), M(M32) | M(Any), 0, 0x00),
  ROW(F(Mem), M(M64) | M(Any), 0, 0x00),
  ROW(F(GpbLo) | F(GpbHi) | F(Gpw) | F(Gpd) | F(Gpq) | F(Mem), M(M8) | M(M16) | M(M32) | M(M64) | M(Any), 0, 0x00),
  ROW(F(Gpq) | F(Mem) | F(I32) | F(U32), M(M64) | M(Any), 0, 0x00),
  ROW(F(Gpw) | F(Implicit), 0, 0, 0x01),
  ROW(F(Gpw) | F(Implicit), 0, 0, 0x04),
  ROW(F(Gpd) | F(Implicit), 0, 0, 0x04),
  ROW(F(Gpd) | F(Implicit), 0, 0, 0x01),
  ROW(F(Gpq) | F(Implicit), 0, 0, 0x04),
  ROW(F(Gpq) | F(Implicit), 0, 0, 0x01),
  ROW(F(Gpw) | F(Mem) | F(I8) | F(I16), M(M16) | M(Any), 0, 0x00),
  ROW(F(Gpd) | F(Mem) | F(I8) | F(I32), M(M32) | M(Any), 0, 0x00),
  ROW(F(Gpq) | F(Mem) | F(I8) | F(I32), M(M64) | M(Any), 0, 0x00),
  ROW(F(I8) | F(I16) | F(U16), 0, 0, 0x00),
  ROW(F(I8) | F(I32) | F(U32), 0, 0, 0x00),
  ROW(F(I8) | F(I32), 0, 0, 0x00),
  ROW(F(I64) | F(U64), 0, 0, 0x00),
  ROW(F(GpbLo), 0, 0, 0x01),
  ROW(F(Gpw), 0, 0, 0x01),
  ROW(F(Gpd), 0, 0, 0x01),
  ROW(F(Gpq), 0, 0, 0x01),
  ROW(F(Xmm), 0, 0, 0x00),
  ROW(F(Xmm) | F(Mem), M(M128) | M(Any), 0, 0x00),
  ROW(F(Mem), M(M128) | M(Any), 0, 0x00),
  ROW(F(Ymm), 0, 0, 0x00),
  ROW(F(Ymm) | F(Mem), M(M256) | M(Any), 0, 0x00),
  ROW(F(Mem), M(M256) | M(Any), 0, 0x00),
  ROW(F(Zmm), 0, 0, 0x00),
  ROW(F(Zmm) | F(Mem), M(M512) | M(Any), 0, 0x00),
  ROW(F(Mem), M(M512) | M(Any), 0, 0x00),
  ROW(F(Xmm) | F(Mem) | F(I8) | F(U8), M(M128) | M(Any), 0, 0x00),
  ROW(F(Ymm) | F(Mem) | F(I8) | F(U8), M(M256) | M(Any), 0, 0x00),
  ROW(F(Zmm) | F(Mem) | F(I8) | F(U8), M(M512) | M(Any), 0, 0x00),
  ROW(F(Mm), 0, 0, 0x00),
  ROW(F(Gpq) | F(Mm) | F(Mem), M(M64) | M(Any), 0, 0x00),
  ROW(F(Xmm) | F(Mm), 0, 0, 0x00),
  ROW(F(Xmm) | F(Mem), M(M64) | M(Any), 0, 0x00),
  ROW(F(Gpw) | F(Gpd) | F(Gpq) | F(Mem), M(M16) | M(M32) | M(M64), 0, 0x00),
  ROW(F(Vm), M(Vm32x), 0, 0x00),
  ROW(F(Vm), M(Vm32y), 0, 0x00),
  ROW(F(Vm), M(Vm32z), 0, 0x00),
  ROW(F(Vm), M(Vm64x), 0, 0x00),
  ROW(F(Vm), M(Vm64y), 0, 0x00),
  ROW(F(Vm), M(Vm64z), 0, 0x00),
  ROW(F(Mem) | F(Implicit), M(M8) | M(BaseOnly) | M(Ds), 0, 0x40),
  ROW(F(Mem) | F(Implicit), M(M8) | M(BaseOnly) | M(Es), 0, 0x80),
  ROW(F(Mem) | F(Implicit), M(M16) | M(BaseOnly) | M(Ds), 0, 0x40),
  ROW(F(Mem) | F(Implicit), M(M16) | M(BaseOnly) | M(Es), 0, 0x80),
  ROW(F(Mem) | F(Implicit), M(M32) | M(BaseOnly) | M(Ds), 0, 0x40),
  ROW(F(Mem) | F(Implicit), M(M32) | M(BaseOnly) | M(Es), 0, 0x80),
  ROW(F(Mem) | F(Implicit), M(M64) | M(BaseOnly) | M(Ds), 0, 0x40),
  ROW(F(Mem) | F(Implicit), M(M64) | M(BaseOnly) | M(Es), 0, 0x80),
  ROW(F(GpbLo) | F(Implicit), 0, 0, 0x01),
  ROW(F(Mem) | F(Implicit), M(M8) | M(BaseOnly) | M(Ds) | M(Any), 0, 0x40),
  ROW(F(Mem) | F(Implicit), M(M16) | M(BaseOnly) | M(Ds) | M(Any), 0, 0x40),
  ROW(F(Mem) | F(Implicit), M(M32) | M(BaseOnly) | M(Ds) | M(Any), 0, 0x40),
  ROW(F(Mem) | F(Implicit), M(M64) | M(BaseOnly) | M(Ds) | M(Any), 0, 0x40),
  ROW(F(Gpw) | F(Gpq) | F(Mem), M(M16) | M(M64), 0, 0x00),
  ROW(F(SReg), 0, 0, 0x1A),
  ROW(F(SReg), 0, 0, 0x60),
  ROW(F(Gpw) | F(Gpq) | F(Mem) | F(I8) | F(I16) | F(I32), M(M16) | M(M64), 0, 0x00),
  ROW(F(Gpd) | F(Mem) | F(I32) | F(U32), M(M32), 0, 0x00),
  ROW(F(SReg), 0, 0, 0x1E),
  ROW(F(Mem) | F(Implicit), M(M8) | M(BaseOnly) | M(Es) | M(Any), 0, 0x80),
  ROW(F(Mem) | F(Implicit), M(M16) | M(BaseOnly) | M(Es) | M(Any), 0, 0x80),
  ROW(F(Mem) | F(Implicit), M(M32) | M(BaseOnly) | M(Es) | M(Any), 0, 0x80),
  ROW(F(Mem) | F(Implicit), M(M64) | M(BaseOnly) | M(Es) | M(Any), 0, 0x80),
  ROW(F(Xmm) | F(Ymm), 0, 0, 0x00),
  ROW(F(I4) | F(U4), 0, 0, 0x00),
  ROW(F(Mem), M(M32) | M(M64), 0, 0x00),
  ROW(F(St), 0, 0, 0x01),
  ROW(F(St), 0, 0, 0x00),
  ROW(F(Mem), M(M48) | M(Any), 0, 0x00),
  ROW(F(Mem), M(M80) | M(Any), 0, 0x00),
  ROW(F(GpbLo) | F(I8) | F(U8), 0, 0, 0x02),
  ROW(F(Xmm) | F(KReg), 0, 0, 0x00),
  ROW(F(Ymm) | F(KReg), 0, 0, 0x00),
  ROW(F(KReg), 0, 0, 0x00),
  ROW(F(Vm), M(Vm64x) | M(Vm64y), 0, 0x00),
  ROW(F(Gpq) | F(Xmm) | F(Mem), M(M64) | M(Any), 0, 0x00),
  ROW(F(Xmm) | F(Mem), M(M32) | M(Any), 0, 0x00),
  ROW(F(Xmm) | F(Mem), M(M16) | M(Any), 0, 0x00),
  ROW(F(Bnd), 0, 0, 0x00),
  ROW(F(Bnd) | F(Mem), M(Any), 0, 0x00),
  ROW(F(Mem), M(Any), 0, 0x00),
  ROW(F(Gpw) | F(Gpd) | F(Mem) | F(I32) | F(I64) | F(Rel32), M(M16) | M(M32), 0, 0x00),
  ROW(F(Gpq) | F(Mem) | F(I32) | F(I64) | F(Rel32), M(M64) | M(Any), 0, 0x00),
  ROW(F(GpbLo) | F(GpbHi) | F(Gpw) | F(Gpd) | F(Mem), M(M8) | M(M16) | M(M32), 0, 0x00),
  ROW(F(GpbLo) | F(GpbHi) | F(Gpq) | F(Mem), M(M8) | M(M64), 0, 0x00),
  ROW(F(Gpw) | F(Gpd), 0, 0, 0x00),
  ROW(F(Mem), M(M512) | M(BaseOnly) | M(Es) | M(Any), 0, 0x00),
  ROW(F(St) | F(Mem), M(M32) | M(M64), 0, 0x00),
  ROW(F(Gpd) | F(Implicit), 0, 0, 0x02),
  ROW(F(Gpd) | F(Gpq) | F(Implicit), 0, 0, 0x01),
  ROW(F(I32) | F(I64) | F(Rel8) | F(Rel32), 0, 0, 0x00),
  ROW(F(I32) | F(I64) | F(Rel32), 0, 0, 0x00),
  ROW(F(Gpw) | F(Gpd) | F(Implicit), 0, 0, 0x02),
  ROW(F(I32) | F(I64) | F(Rel8), 0, 0, 0x00),
  ROW(F(Gpd) | F(Gpq) | F(Implicit), 0, 0, 0x02),
  ROW(F(Gpq) | F(Mem) | F(I32) | F(I64) | F(Rel8) | F(Rel32), M(M64) | M(Any), 0, 0x00),
  ROW(F(Gpd) | F(Mem) | F(I32) | F(I64) | F(Rel32), M(M32) | M(Any), 0, 0x00),
  ROW(F(Gpd) | F(KReg) | F(Mem), M(M8) | M(Any), 0, 0x00),
  ROW(F(Gpd) | F(Mem), M(M8) | M(Any), 0, 0x00),
  ROW(F(Gpd) | F(KReg) | F(Mem), M(M32) | M(Any), 0, 0x00),
  ROW(F(Gpq) | F(KReg) | F(Mem), M(M64) | M(Any), 0, 0x00),
  ROW(F(Gpd) | F(KReg) | F(Mem), M(M16) | M(Any), 0, 0x00),
  ROW(F(Gpd) | F(Mem), M(M16) | M(Any), 0, 0x00),
  ROW(F(I16), 0, 0, 0x00),
  ROW(F(I16) | F(I32), 0, 0, 0x00),
  ROW(F(Mem), M(M32) | M(M48) | M(M80) | M(Any), 0, 0x00),
  ROW(F(Gpd) | F(Gpq), 0, 0, 0x00),
  ROW(F(GpbLo) | F(GpbHi) | F(Gpw) | F(Mem), M(M8) | M(M16), 0, 0x00),
  ROW(F(Mm) | F(Mem), M(M64) | M(Any), 0, 0x00),
  ROW(F(Mm) | F(Mem) | F(I8) | F(U8), M(M64) | M(Any), 0, 0x00),
  ROW(F(Mm) | F(Mem), M(M32) | M(Any), 0, 0x00),
  ROW(F(U16), 0, 0, 0x00),
  ROW(F(Xmm) | F(Ymm) | F(Mem), M(M128) | M(M256), 0, 0x00),
  ROW(F(Xmm) | F(I8) | F(U8), 0, 0, 0x00),
  ROW(F(Xmm) | F(Implicit), 0, 0, 0x01),
  ROW(F(Mem), M(Mib), 0, 0x00),
  ROW(F(Gpw) | F(Gpd) | F(Gpq), 0, 0, 0x00),
  ROW(F(Mem), M(M8) | M(M16) | M(M32) | M(M48) | M(M64) | M(M80) | M(M128) | M(M256) | M(M512) | M(M1024) | M(Any), 0, 0x00),
  ROW(F(Mem) | F(Implicit), M(M512) | M(BaseOnly) | M(Ds) | M(Any), 0, 0x01),
  ROW(F(Gpq) | F(Implicit), 0, 0, 0x02),
  ROW(F(Gpq) | F(Implicit), 0, 0, 0x08),
  ROW(F(Gpd) | F(Implicit), 0, 0, 0x08),
  ROW(F(Gpd) | F(Gpq) | F(Mem), M(M32) | M(M64) | M(Any), 0, 0x00),
  ROW(F(Mem), M(M16) | M(M32), 0, 0x00),
  ROW(F(Mem), M(M16) | M(M32) | M(M64), 0, 0x00),
  ROW(F(St) | F(Mem), M(M32) | M(M64) | M(M80), 0, 0x00),
  ROW(F(Gpw) | F(Mem), M(M16) | M(Any), 0, 0x01),
  ROW(F(GpbLo) | F(Gpw) | F(Gpd), 0, 0, 0x01),
  ROW(F(Gpw) | F(I8) | F(U8), 0, 0, 0x04),
  ROW(F(Mem), M(M8) | M(M16) | M(M32) | M(BaseOnly) | M(Es), 0, 0x80),
  ROW(F(Gpw), 0, 0, 0x04),
  ROW(F(GpbHi) | F(Implicit), 0, 0, 0x01),
  ROW(F(Mem) | F(Implicit), M(M128) | M(BaseOnly) | M(Ds) | M(Any), 0, 0x80),
  ROW(F(Mem) | F(Implicit), M(M64) | M(BaseOnly) | M(Ds) | M(Any), 0, 0x80),
  ROW(F(Mem) | F(Implicit), M(BaseOnly) | M(Ds) | M(Any), 0, 0x01),
  ROW(F(Gpw) | F(U8), 0, 0, 0x04),
  ROW(F(Mem), M(M8) | M(M16) | M(M32) | M(BaseOnly) | M(Ds), 0, 0x40),
  ROW(F(Gpw) | F(Gpd) | F(Gpq) | F(Mem), M(M16) | M(Any), 0, 0x00),
  ROW(F(Tmm), 0, 0, 0x00),
  ROW(F(Mem), M(BaseOnly) | M(Ds) | M(Any), 0, 0x00),
  ROW(F(Ymm) | F(Zmm), 0, 0, 0x00),
  ROW(F(Xmm) | F(Ymm) | F(Zmm), 0, 0, 0x00),
  ROW(F(Xmm) | F(Ymm) | F(Zmm) | F(Mem), M(M128) | M(M256) | M(M512), 0, 0x00),
  ROW(F(Gpd) | F(Xmm) | F(Mem), M(M8) | M(Any), 0, 0x00),
  ROW(F(Gpd) | F(Xmm) | F(Mem), M(M32) | M(Any), 0, 0x00),
  ROW(F(Gpd) | F(Xmm) | F(Mem), M(M16) | M(Any), 0, 0x00)
};
#undef M
#undef F
#undef ROW
// ----------------------------------------------------------------------------
// ${InstSignatureTable:End}
#endif // !ASMJIT_NO_VALIDATION

// ============================================================================
// [asmjit::x86::InstInternal - QueryRWInfo]
// ============================================================================

// ${InstRWInfoTable:Begin}
// ------------------- Automatically generated, do not edit -------------------
const uint8_t InstDB::rwInfoIndexA[Inst::_kIdCount] = {
  0, 0, 1, 1, 0, 2, 3, 2, 4, 4, 5, 6, 4, 4, 3, 4, 4, 4, 4, 7, 0, 2, 0, 4, 4, 4,
  4, 8, 0, 9, 9, 9, 9, 9, 0, 0, 0, 0, 9, 9, 9, 9, 9, 10, 10, 10, 11, 11, 12, 13,
  14, 9, 9, 0, 15, 16, 16, 16, 0, 0, 0, 17, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
  0, 0, 0, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
  3, 3, 3, 3, 3, 3, 3, 18, 0, 0, 19, 0, 0, 0, 0, 0, 20, 21, 0, 22, 23, 24, 7, 25,
  25, 25, 24, 26, 7, 24, 27, 28, 29, 30, 31, 32, 33, 25, 25, 7, 27, 28, 33, 34,
  0, 0, 0, 0, 35, 4, 4, 5, 6, 0, 0, 0, 0, 0, 36, 36, 0, 0, 37, 0, 0, 38, 0, 0,
  0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 38, 0, 38, 0, 0, 0, 0,
  0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 38, 0, 0,
  0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 38, 0, 38,
  0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 4, 4, 4, 0, 39, 4,
  4, 35, 40, 41, 0, 0, 0, 42, 0, 37, 0, 0, 0, 0, 43, 0, 44, 43, 43, 0, 0, 0, 0,
  0, 0, 0, 0, 0, 45, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
  0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 46, 47, 48, 49, 50, 51,
  52, 53, 0, 0, 0, 54, 55, 56, 57, 0, 0, 0, 0, 0, 0, 0, 0, 0, 54, 55, 56, 57, 0,
  0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 58, 0, 59, 0, 60, 0, 61, 0, 60, 0, 60, 0, 60,
  0, 0, 0, 0, 0, 62, 63, 63, 63, 58, 60, 0, 0, 0, 9, 0, 0, 4, 4, 5, 6, 0, 0, 4,
  4, 5, 6, 0, 0, 64, 65, 66, 66, 67, 47, 24, 36, 67, 52, 66, 66, 68, 69, 69, 70,
  71, 71, 72, 72, 59, 59, 67, 59, 59, 71, 71, 73, 48, 52, 74, 48, 7, 7, 47, 75,
  33, 66, 66, 75, 0, 35, 4, 4, 5, 6, 0, 76, 0, 0, 77, 0, 2, 4, 4, 78, 79, 9,
  9, 9, 3, 3, 4, 3, 3, 3, 3, 3, 3, 3, 3, 3, 0, 3, 3, 0, 3, 80, 3, 0, 0, 0, 3, 3,
  4, 3, 0, 0, 3, 3, 4, 3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 27, 27, 80, 80, 80, 80, 80,
  80, 80, 80, 80, 80, 27, 80, 80, 80, 27, 27, 80, 80, 80, 3, 3, 3, 81, 3, 3, 3,
  27, 27, 0, 0, 0, 0, 3, 3, 4, 4, 3, 3, 4, 4, 4, 4, 3, 3, 4, 4, 82, 83, 84, 24,
  24, 24, 83, 83, 84, 24, 24, 24, 83, 4, 3, 80, 3, 3, 4, 3, 3, 0, 0, 0, 9, 0,
  0, 0, 3, 0, 0, 0, 0, 0, 0, 0, 3, 3, 0, 0, 0, 0, 3, 3, 3, 3, 85, 3, 3, 0, 3, 3,
  3, 85, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 27, 86, 0, 3, 3, 4, 3, 87, 87, 4, 87, 0,
  0, 0, 0, 0, 0, 0, 3, 88, 7, 89, 88, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 90, 0, 0,
  0, 0, 0, 88, 88, 0, 0, 0, 0, 0, 0, 7, 89, 0, 0, 88, 88, 0, 0, 2, 91, 0, 0, 0,
  0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
  0, 0, 0, 0, 0, 0, 4, 4, 4, 0, 4, 4, 0, 88, 0, 0, 88, 0, 0, 0, 0, 0, 0, 0, 0,
  0, 7, 7, 26, 89, 0, 0, 0, 0, 0, 0, 92, 0, 0, 0, 2, 4, 4, 5, 6, 0, 0, 0, 0, 0,
  0, 0, 9, 0, 0, 0, 0, 0, 15, 0, 93, 93, 0, 94, 0, 0, 9, 9, 20, 21, 95, 95, 0,
  0, 0, 0, 4, 4, 4, 4, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 7, 0, 0, 0, 0,
  0, 0, 0, 0, 0, 0, 0, 0, 0, 96, 28, 97, 98, 97, 98, 96, 28, 97, 98, 97, 98, 99,
  100, 0, 0, 0, 0, 20, 21, 101, 101, 102, 9, 0, 75, 103, 103, 9, 103, 9, 102, 9,
  102, 0, 102, 9, 102, 9, 103, 28, 0, 28, 0, 0, 0, 33, 33, 103, 9, 103, 9, 9,
  102, 9, 102, 28, 28, 33, 33, 102, 9, 9, 103, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
  0, 104, 104, 9, 9, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
  0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
  0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
  0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
  0, 9, 9, 27, 105, 60, 60, 0, 0, 0, 0, 0, 0, 0, 0, 60, 106, 9, 9, 0, 0, 0, 0,
  0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 67, 0, 0, 0, 0, 0,
  0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 107, 107, 47, 108, 107, 107, 107, 107,
  107, 107, 107, 107, 0, 109, 109, 0, 71, 71, 110, 111, 67, 67, 67, 67, 112, 71,
  9, 9, 73, 107, 107, 0, 0, 0, 101, 0, 0, 0, 0, 0, 0, 0, 113, 0, 0, 0, 0, 0, 0,
  0, 9, 9, 9, 9, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
  0, 0, 0, 0, 0, 0, 0, 114, 33, 115, 115, 28, 116, 0, 0, 0, 0, 0, 0, 0, 0, 0,
  0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 101, 101, 101, 101, 0, 0, 0, 0, 0,
  0, 9, 9, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
  0, 0, 0, 0, 0, 9, 9, 9, 9, 0, 0, 0, 0, 60, 60, 106, 60, 7, 7, 7, 0, 7, 0, 7,
  7, 7, 7, 7, 7, 0, 7, 7, 81, 7, 0, 7, 0, 0, 7, 0, 0, 0, 0, 9, 9, 0, 0, 0, 0, 0,
  0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
  0, 0, 0, 117, 117, 118, 119, 115, 115, 115, 115, 82, 117, 120, 119, 118, 118,
  119, 120, 119, 118, 119, 121, 122, 102, 102, 102, 121, 118, 119, 120, 119, 118,
  119, 117, 119, 121, 122, 102, 102, 102, 121, 0, 0, 0, 0, 0, 0, 0, 0, 0, 9,
  9, 9, 9, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 67, 67, 123, 67,
  0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
  0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
  0, 0, 0, 0, 0, 0, 113, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
  0, 0, 0, 0, 9, 9, 0, 0, 104, 104, 0, 0, 9, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
  0, 0, 9, 9, 0, 0, 104, 104, 0, 0, 9, 0, 0, 0, 0, 0, 67, 67, 0, 0, 0, 0, 0, 0,
  0, 0, 67, 123, 0, 0, 0, 0, 0, 0, 9, 9, 0, 0, 0, 0, 0, 0, 0, 113, 113, 20, 21,
  0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 124, 125, 124, 125, 0, 126, 0, 127, 0,
  0, 0, 2, 4, 4, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0
};

const uint8_t InstDB::rwInfoIndexB[Inst::_kIdCount] = {
  0, 1, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2, 0, 3, 0, 0, 0,
  0, 0, 4, 0, 0, 0, 0, 0, 5, 5, 6, 6, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
  0, 7, 0, 0, 0, 0, 4, 8, 1, 0, 9, 0, 0, 0, 10, 10, 10, 0, 0, 11, 0, 0, 10, 12,
  0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
  0, 0, 0, 0, 0, 0, 5, 5, 0, 13, 14, 15, 16, 17, 0, 0, 18, 0, 0, 0, 0, 0, 0, 0,
  0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 19, 1, 1, 20, 21, 0, 0,
  0, 0, 5, 5, 0, 0, 0, 0, 0, 0, 22, 23, 0, 0, 24, 25, 26, 27, 0, 0, 25, 25, 25,
  25, 25, 25, 25, 25, 28, 29, 29, 28, 0, 0, 0, 24, 25, 24, 25, 0, 25, 24, 24, 24,
  24, 24, 24, 24, 0, 0, 30, 30, 30, 24, 24, 28, 0, 31, 10, 0, 0, 0, 0, 0, 0, 24,
  25, 0, 0, 0, 32, 33, 32, 34, 0, 0, 0, 0, 0, 10, 32, 0, 0, 0, 0, 35, 33, 32,
  35, 34, 24, 25, 24, 25, 0, 29, 29, 29, 29, 0, 0, 0, 25, 10, 10, 32, 32, 0, 0,
  0, 0, 5, 5, 0, 0, 0, 0, 0, 0, 0, 21, 36, 0, 20, 37, 38, 0, 39, 40, 0, 0, 0, 0,
  0, 10, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 8, 0, 0, 0, 0, 0,
  0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 41, 42, 43, 44, 41, 42, 41, 42, 43,
  44, 43, 44, 0, 0, 0, 0, 0, 0, 0, 0, 41, 42, 43, 0, 0, 0, 0, 44, 45, 46, 47,
  48, 45, 46, 47, 48, 0, 0, 0, 0, 49, 50, 51, 41, 42, 43, 44, 41, 42, 43, 44, 52,
  0, 24, 0, 53, 0, 54, 0, 0, 0, 0, 0, 10, 0, 10, 24, 55, 56, 55, 0, 0, 0, 0,
  0, 0, 55, 57, 57, 0, 58, 59, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 60, 60, 0, 0, 0, 0,
  0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
  0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 5, 61, 0, 0, 0, 0, 62, 0, 63, 20, 64, 20, 0, 0,
  0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 65, 0, 0, 0, 0, 0, 0, 6,
  5, 5, 0, 0, 0, 0, 66, 67, 0, 0, 0, 0, 68, 69, 0, 3, 3, 70, 22, 71, 72, 0, 0,
  0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
  0, 0, 73, 39, 74, 75, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
  0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 76, 0, 0, 0, 0, 0, 0, 0, 10,
  10, 10, 10, 10, 10, 10, 0, 0, 2, 2, 2, 77, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
  0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 64, 0, 0, 0, 0, 0, 0, 0, 0, 78, 0, 0, 0,
  0, 0, 0, 0, 0, 0, 0, 0, 79, 79, 80, 79, 80, 80, 80, 79, 79, 81, 82, 0, 83, 0,
  0, 0, 0, 0, 0, 84, 2, 2, 85, 86, 0, 0, 0, 11, 87, 0, 0, 4, 0, 0, 0, 88, 0, 89,
  89, 89, 89, 89, 89, 89, 89, 89, 89, 89, 89, 89, 89, 89, 89, 89, 89, 89, 89,
  89, 89, 89, 89, 89, 89, 89, 89, 89, 0, 89, 0, 32, 0, 0, 0, 5, 0, 0, 6, 0, 90,
  4, 0, 90, 4, 5, 5, 32, 19, 91, 79, 91, 0, 0, 0, 0, 0, 0, 0, 0, 0, 92, 0, 91, 93,
  0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 94, 94, 94, 94, 94, 0, 0, 0, 0, 0,
  0, 95, 96, 0, 0, 0, 0, 0, 0, 0, 0, 56, 96, 0, 0, 0, 0, 97, 98, 97, 98, 3, 3,
  99, 100, 3, 3, 3, 3, 3, 3, 0, 2, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 101, 101, 0, 0,
  0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 3, 3, 102, 103, 0, 0, 0, 0, 0, 0, 3, 0,
  0, 0, 0, 0, 0, 0, 0, 0, 104, 0, 0, 0, 0, 0, 0, 105, 0, 106, 107, 108, 0, 0, 0,
  0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 106, 107, 3, 3, 3, 99, 100, 3, 109,
  3, 55, 55, 0, 0, 0, 0, 110, 111, 112, 111, 112, 110, 111, 112, 111, 112, 22,
  113, 113, 114, 115, 113, 113, 116, 117, 113, 113, 116, 117, 113, 113, 116,
  117, 118, 118, 119, 120, 113, 113, 113, 113, 113, 113, 118, 118, 113, 113, 116,
  117, 113, 113, 116, 117, 113, 113, 116, 117, 113, 113, 113, 113, 113, 113, 118,
  118, 118, 118, 119, 120, 113, 113, 116, 117, 113, 113, 116, 117, 113, 113,
  116, 117, 118, 118, 119, 120, 113, 113, 116, 117, 113, 113, 116, 117, 113, 113,
  121, 122, 118, 118, 119, 120, 123, 123, 77, 124, 0, 0, 0, 0, 125, 126, 10,
  10, 10, 10, 10, 10, 10, 10, 126, 127, 0, 0, 128, 129, 84, 84, 128, 129, 3, 3,
  3, 3, 3, 3, 3, 130, 131, 132, 131, 132, 130, 131, 132, 131, 132, 100, 0, 53, 58,
  133, 133, 3, 3, 99, 100, 0, 134, 0, 3, 3, 99, 100, 0, 135, 0, 0, 0, 0, 0, 0,
  0, 0, 0, 0, 0, 0, 0, 136, 137, 137, 138, 139, 139, 0, 0, 0, 0, 0, 0, 0, 140,
  0, 0, 141, 0, 0, 3, 11, 134, 0, 0, 142, 135, 3, 3, 99, 100, 0, 11, 3, 3, 143,
  143, 144, 144, 0, 0, 0, 0, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
  3, 3, 3, 3, 3, 3, 3, 3, 3, 101, 3, 0, 0, 0, 0, 0, 0, 3, 118, 145, 145, 3, 3,
  3, 3, 66, 67, 3, 3, 3, 3, 68, 69, 145, 145, 145, 145, 145, 145, 109, 109, 0, 0,
  0, 0, 109, 109, 109, 109, 109, 109, 0, 0, 113, 113, 113, 113, 146, 146, 3, 3,
  3, 113, 3, 3, 113, 113, 118, 118, 147, 147, 147, 3, 147, 3, 113, 113, 113, 113,
  113, 3, 0, 0, 0, 0, 70, 22, 71, 148, 126, 125, 127, 126, 0, 0, 0, 3, 0, 3,
  0, 0, 0, 0, 0, 0, 3, 0, 0, 0, 0, 3, 0, 3, 3, 0, 149, 100, 99, 150, 0, 0, 151,
  151, 151, 151, 151, 151, 151, 151, 151, 151, 151, 151, 113, 113, 3, 3, 133, 133,
  3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 0, 0, 0, 0, 0, 0, 0, 0, 0,
  0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
  0, 0, 0, 0, 3, 3, 3, 3, 3, 3, 3, 3, 3, 0, 0, 0, 0, 3, 3, 3, 152, 84, 84, 3, 3,
  84, 84, 3, 3, 153, 153, 153, 153, 3, 0, 0, 0, 0, 153, 153, 153, 153, 153, 153,
  3, 3, 113, 113, 113, 3, 153, 153, 3, 3, 113, 113, 113, 3, 3, 145, 84, 84, 84,
  3, 3, 3, 154, 155, 154, 3, 3, 3, 154, 154, 154, 3, 3, 3, 154, 154, 155, 154,
  3, 3, 3, 154, 3, 3, 3, 3, 3, 3, 3, 3, 113, 113, 0, 145, 145, 145, 145, 145,
  145, 145, 145, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 128, 129, 0, 0, 128, 129,
  0, 0, 128, 129, 0, 129, 84, 84, 128, 129, 84, 84, 128, 129, 84, 84, 128, 129,
  0, 0, 128, 129, 0, 0, 128, 129, 0, 129, 3, 3, 99, 100, 0, 0, 10, 10, 10, 10,
  10, 10, 10, 10, 0, 0, 3, 3, 3, 3, 3, 3, 0, 0, 128, 129, 92, 3, 3, 99, 100, 0,
  0, 0, 0, 3, 3, 3, 3, 3, 3, 0, 0, 0, 0, 56, 56, 156, 0, 0, 0, 0, 0, 0, 0, 0, 0,
  80, 0, 0, 0, 0, 0, 157, 157, 157, 157, 158, 158, 158, 158, 158, 158, 158, 158,
  156, 0, 0
};

const InstDB::RWInfo InstDB::rwInfoA[] = {
  { InstDB::RWInfo::kCategoryGeneric   , 0 , { 0 , 0 , 0 , 0 , 0 , 0  } }, // #0 [ref=936x]
  { InstDB::RWInfo::kCategoryGeneric   , 0 , { 1 , 0 , 0 , 0 , 0 , 0  } }, // #1 [ref=2x]
  { InstDB::RWInfo::kCategoryGeneric   , 1 , { 2 , 3 , 0 , 0 , 0 , 0  } }, // #2 [ref=7x]
  { InstDB::RWInfo::kCategoryGeneric   , 2 , { 2 , 3 , 0 , 0 , 0 , 0  } }, // #3 [ref=96x]
  { InstDB::RWInfo::kCategoryGeneric   , 3 , { 4 , 5 , 0 , 0 , 0 , 0  } }, // #4 [ref=55x]
  { InstDB::RWInfo::kCategoryGeneric   , 4 , { 6 , 7 , 0 , 0 , 0 , 0  } }, // #5 [ref=6x]
  { InstDB::RWInfo::kCategoryGeneric   , 5 , { 8 , 9 , 0 , 0 , 0 , 0  } }, // #6 [ref=6x]
  { InstDB::RWInfo::kCategoryGeneric   , 3 , { 10, 5 , 0 , 0 , 0 , 0  } }, // #7 [ref=26x]
  { InstDB::RWInfo::kCategoryGeneric   , 7 , { 12, 13, 0 , 0 , 0 , 0  } }, // #8 [ref=1x]
  { InstDB::RWInfo::kCategoryGeneric   , 2 , { 11, 3 , 0 , 0 , 0 , 0  } }, // #9 [ref=64x]
  { InstDB::RWInfo::kCategoryGeneric   , 2 , { 5 , 3 , 0 , 0 , 0 , 0  } }, // #10 [ref=3x]
  { InstDB::RWInfo::kCategoryGeneric   , 8 , { 10, 3 , 0 , 0 , 0 , 0  } }, // #11 [ref=2x]
  { InstDB::RWInfo::kCategoryGeneric   , 9 , { 10, 5 , 0 , 0 , 0 , 0  } }, // #12 [ref=1x]
  { InstDB::RWInfo::kCategoryGeneric   , 8 , { 15, 5 , 0 , 0 , 0 , 0  } }, // #13 [ref=1x]
  { InstDB::RWInfo::kCategoryGeneric   , 0 , { 3 , 3 , 0 , 0 , 0 , 0  } }, // #14 [ref=1x]
  { InstDB::RWInfo::kCategoryGeneric   , 10, { 3 , 3 , 0 , 0 , 0 , 0  } }, // #15 [ref=2x]
  { InstDB::RWInfo::kCategoryGeneric   , 10, { 2 , 3 , 0 , 0 , 0 , 0  } }, // #16 [ref=3x]
  { InstDB::RWInfo::kCategoryGeneric   , 0 , { 16, 17, 0 , 0 , 0 , 0  } }, // #17 [ref=1x]
  { InstDB::RWInfo::kCategoryGeneric   , 1 , { 3 , 3 , 0 , 0 , 0 , 0  } }, // #18 [ref=1x]
  { InstDB::RWInfo::kCategoryGeneric   , 0 , { 20, 21, 0 , 0 , 0 , 0  } }, // #19 [ref=1x]
  { InstDB::RWInfo::kCategoryGeneric   , 4 , { 7 , 7 , 0 , 0 , 0 , 0  } }, // #20 [ref=4x]
  { InstDB::RWInfo::kCategoryGeneric   , 5 , { 9 , 9 , 0 , 0 , 0 , 0  } }, // #21 [ref=4x]
  { InstDB::RWInfo::kCategoryGeneric   , 0 , { 33, 34, 0 , 0 , 0 , 0  } }, // #22 [ref=1x]
  { InstDB::RWInfo::kCategoryGeneric   , 14, { 2 , 3 , 0 , 0 , 0 , 0  } }, // #23 [ref=1x]
  { InstDB::RWInfo::kCategoryGeneric   , 4 , { 10, 7 , 0 , 0 , 0 , 0  } }, // #24 [ref=10x]
  { InstDB::RWInfo::kCategoryGeneric   , 3 , { 35, 5 , 0 , 0 , 0 , 0  } }, // #25 [ref=5x]
  { InstDB::RWInfo::kCategoryGeneric   , 4 , { 36, 7 , 0 , 0 , 0 , 0  } }, // #26 [ref=2x]
  { InstDB::RWInfo::kCategoryGeneric   , 4 , { 35, 7 , 0 , 0 , 0 , 0  } }, // #27 [ref=11x]
  { InstDB::RWInfo::kCategoryGeneric   , 4 , { 11, 7 , 0 , 0 , 0 , 0  } }, // #28 [ref=9x]
  { InstDB::RWInfo::kCategoryGeneric   , 4 , { 37, 7 , 0 , 0 , 0 , 0  } }, // #29 [ref=1x]
  { InstDB::RWInfo::kCategoryGeneric   , 14, { 36, 3 , 0 , 0 , 0 , 0  } }, // #30 [ref=1x]
  { InstDB::RWInfo::kCategoryGeneric   , 14, { 37, 3 , 0 , 0 , 0 , 0  } }, // #31 [ref=1x]
  { InstDB::RWInfo::kCategoryGeneric   , 5 , { 36, 9 , 0 , 0 , 0 , 0  } }, // #32 [ref=1x]
  { InstDB::RWInfo::kCategoryGeneric   , 5 , { 11, 9 , 0 , 0 , 0 , 0  } }, // #33 [ref=8x]
  { InstDB::RWInfo::kCategoryGeneric   , 0 , { 38, 39, 0 , 0 , 0 , 0  } }, // #34 [ref=1x]
  { InstDB::RWInfo::kCategoryGeneric   , 15, { 1 , 40, 0 , 0 , 0 , 0  } }, // #35 [ref=3x]
  { InstDB::RWInfo::kCategoryGeneric   , 16, { 11, 43, 0 , 0 , 0 , 0  } }, // #36 [ref=3x]
  { InstDB::RWInfo::kCategoryGeneric   , 0 , { 4 , 5 , 0 , 0 , 0 , 0  } }, // #37 [ref=2x]
  { InstDB::RWInfo::kCategoryGeneric   , 0 , { 45, 46, 0 , 0 , 0 , 0  } }, // #38 [ref=6x]
  { InstDB::RWInfo::kCategoryGeneric   , 0 , { 0 , 50, 0 , 0 , 0 , 0  } }, // #39 [ref=1x]
  { InstDB::RWInfo::kCategoryImul      , 2 , { 0 , 0 , 0 , 0 , 0 , 0  } }, // #40 [ref=1x]
  { InstDB::RWInfo::kCategoryGeneric   , 0 , { 51, 52, 0 , 0 , 0 , 0  } }, // #41 [ref=1x]
  { InstDB::RWInfo::kCategoryGeneric   , 0 , { 54, 52, 0 , 0 , 0 , 0  } }, // #42 [ref=1x]
  { InstDB::RWInfo::kCategoryGeneric   , 13, { 3 , 5 , 0 , 0 , 0 , 0  } }, // #43 [ref=3x]
  { InstDB::RWInfo::kCategoryGeneric   , 0 , { 22, 29, 0 , 0 , 0 , 0  } }, // #44 [ref=1x]
  { InstDB::RWInfo::kCategoryGeneric   , 0 , { 55, 0 , 0 , 0 , 0 , 0  } }, // #45 [ref=1x]
  { InstDB::RWInfo::kCategoryGeneric   , 23, { 56, 40, 0 , 0 , 0 , 0  } }, // #46 [ref=1x]
  { InstDB::RWInfo::kCategoryGeneric   , 24, { 44, 9 , 0 , 0 , 0 , 0  } }, // #47 [ref=4x]
  { InstDB::RWInfo::kCategoryGeneric   , 25, { 35, 7 , 0 , 0 , 0 , 0  } }, // #48 [ref=3x]
  { InstDB::RWInfo::kCategoryGeneric   , 26, { 48, 13, 0 , 0 , 0 , 0  } }, // #49 [ref=1x]
  { InstDB::RWInfo::kCategoryGeneric   , 0 , { 56, 40, 0 , 0 , 0 , 0  } }, // #50 [ref=1x]
  { InstDB::RWInfo::kCategoryGeneric   , 0 , { 44, 9 , 0 , 0 , 0 , 0  } }, // #51 [ref=1x]
  { InstDB::RWInfo::kCategoryGeneric   , 0 , { 35, 7 , 0 , 0 , 0 , 0  } }, // #52 [ref=3x]
  { InstDB::RWInfo::kCategoryGeneric   , 0 , { 48, 13, 0 , 0 , 0 , 0  } }, // #53 [ref=1x]
  { InstDB::RWInfo::kCategoryGeneric   , 0 , { 40, 40, 0 , 0 , 0 , 0  } }, // #54 [ref=2x]
  { InstDB::RWInfo::kCategoryGeneric   , 0 , { 9 , 9 , 0 , 0 , 0 , 0  } }, // #55 [ref=2x]
  { InstDB::RWInfo::kCategoryGeneric   , 0 , { 7 , 7 , 0 , 0 , 0 , 0  } }, // #56 [ref=2x]
  { InstDB::RWInfo::kCategoryGeneric   , 0 , { 13, 13, 0 , 0 , 0 , 0  } }, // #57 [ref=2x]
  { InstDB::RWInfo::kCategoryGeneric   , 27, { 11, 3 , 0 , 0 , 0 , 0  } }, // #58 [ref=2x]
  { InstDB::RWInfo::kCategoryGeneric   , 13, { 10, 5 , 0 , 0 , 0 , 0  } }, // #59 [ref=5x]
  { InstDB::RWInfo::kCategoryGeneric   , 0 , { 2 , 3 , 0 , 0 , 0 , 0  } }, // #60 [ref=11x]
  { InstDB::RWInfo::kCategoryGeneric   , 8 , { 11, 3 , 0 , 0 , 0 , 0  } }, // #61 [ref=1x]
  { InstDB::RWInfo::kCategoryGeneric   , 0 , { 51, 20, 0 , 0 , 0 , 0  } }, // #62 [ref=1x]
  { InstDB::RWInfo::kCategoryGeneric   , 0 , { 58, 0 , 0 , 0 , 0 , 0  } }, // #63 [ref=3x]
  { InstDB::RWInfo::kCategoryMov       , 29, { 0 , 0 , 0 , 0 , 0 , 0  } }, // #64 [ref=1x]
  { InstDB::RWInfo::kCategoryMovabs    , 0 , { 0 , 0 , 0 , 0 , 0 , 0  } }, // #65 [ref=1x]
  { InstDB::RWInfo::kCategoryGeneric   , 30, { 10, 5 , 0 , 0 , 0 , 0  } }, // #66 [ref=6x]
  { InstDB::RWInfo::kCategoryGeneric   , 0 , { 11, 3 , 0 , 0 , 0 , 0  } }, // #67 [ref=14x]
  { InstDB::RWInfo::kCategoryGeneric   , 0 , { 36, 61, 0 , 0 , 0 , 0  } }, // #68 [ref=1x]
  { InstDB::RWInfo::kCategoryMovh64    , 12, { 0 , 0 , 0 , 0 , 0 , 0  } }, // #69 [ref=2x]
  { InstDB::RWInfo::kCategoryGeneric   , 0 , { 62, 7 , 0 , 0 , 0 , 0  } }, // #70 [ref=1x]
  { InstDB::RWInfo::kCategoryGeneric   , 12, { 35, 7 , 0 , 0 , 0 , 0  } }, // #71 [ref=7x]
  { InstDB::RWInfo::kCategoryGeneric   , 0 , { 56, 5 , 0 , 0 , 0 , 0  } }, // #72 [ref=2x]
  { InstDB::RWInfo::kCategoryGeneric   , 28, { 44, 9 , 0 , 0 , 0 , 0  } }, // #73 [ref=2x]
  { InstDB::RWInfo::kCategoryGeneric   , 0 , { 63, 20, 0 , 0 , 0 , 0  } }, // #74 [ref=1x]
  { InstDB::RWInfo::kCategoryGeneric   , 14, { 11, 3 , 0 , 0 , 0 , 0  } }, // #75 [ref=3x]
  { InstDB::RWInfo::kCategoryGeneric   , 0 , { 17, 29, 0 , 0 , 0 , 0  } }, // #76 [ref=1x]
  { InstDB::RWInfo::kCategoryGeneric   , 11, { 3 , 3 , 0 , 0 , 0 , 0  } }, // #77 [ref=1x]
  { InstDB::RWInfo::kCategoryGeneric   , 0 , { 52, 22, 0 , 0 , 0 , 0  } }, // #78 [ref=1x]
  { InstDB::RWInfo::kCategoryGeneric   , 0 , { 52, 66, 0 , 0 , 0 , 0  } }, // #79 [ref=1x]
  { InstDB::RWInfo::kCategoryGeneric   , 4 , { 26, 7 , 0 , 0 , 0 , 0  } }, // #80 [ref=18x]
  { InstDB::RWInfo::kCategoryGeneric   , 3 , { 69, 5 , 0 , 0 , 0 , 0  } }, // #81 [ref=2x]
  { InstDB::RWInfo::kCategoryVmov1_8   , 0 , { 0 , 0 , 0 , 0 , 0 , 0  } }, // #82 [ref=2x]
  { InstDB::RWInfo::kCategoryGeneric   , 5 , { 10, 9 , 0 , 0 , 0 , 0  } }, // #83 [ref=4x]
  { InstDB::RWInfo::kCategoryGeneric   , 27, { 10, 13, 0 , 0 , 0 , 0  } }, // #84 [ref=2x]
  { InstDB::RWInfo::kCategoryGeneric   , 0 , { 4 , 0 , 0 , 0 , 0 , 0  } }, // #85 [ref=2x]
  { InstDB::RWInfo::kCategoryGeneric   , 3 , { 5 , 5 , 0 , 0 , 0 , 0  } }, // #86 [ref=1x]
  { InstDB::RWInfo::kCategoryPunpcklxx , 34, { 0 , 0 , 0 , 0 , 0 , 0  } }, // #87 [ref=3x]
  { InstDB::RWInfo::kCategoryGeneric   , 10, { 2 , 71, 0 , 0 , 0 , 0  } }, // #88 [ref=8x]
  { InstDB::RWInfo::kCategoryGeneric   , 5 , { 37, 9 , 0 , 0 , 0 , 0  } }, // #89 [ref=3x]
  { InstDB::RWInfo::kCategoryGeneric   , 0 , { 16, 50, 0 , 0 , 0 , 0  } }, // #90 [ref=1x]
  { InstDB::RWInfo::kCategoryGeneric   , 0 , { 22, 21, 0 , 0 , 0 , 0  } }, // #91 [ref=1x]
  { InstDB::RWInfo::kCategoryGeneric   , 0 , { 63, 22, 0 , 0 , 0 , 0  } }, // #92 [ref=1x]
  { InstDB::RWInfo::kCategoryGeneric   , 8 , { 74, 3 , 0 , 0 , 0 , 0  } }, // #93 [ref=2x]
  { InstDB::RWInfo::kCategoryGeneric   , 8 , { 11, 43, 0 , 0 , 0 , 0  } }, // #94 [ref=1x]
  { InstDB::RWInfo::kCategoryGeneric   , 5 , { 53, 9 , 0 , 0 , 0 , 0  } }, // #95 [ref=2x]
  { InstDB::RWInfo::kCategoryGeneric   , 13, { 76, 5 , 0 , 0 , 0 , 0  } }, // #96 [ref=2x]
  { InstDB::RWInfo::kCategoryGeneric   , 13, { 11, 5 , 0 , 0 , 0 , 0  } }, // #97 [ref=4x]
  { InstDB::RWInfo::kCategoryGeneric   , 38, { 74, 77, 0 , 0 , 0 , 0  } }, // #98 [ref=4x]
  { InstDB::RWInfo::kCategoryGeneric   , 39, { 11, 7 , 0 , 0 , 0 , 0  } }, // #99 [ref=1x]
  { InstDB::RWInfo::kCategoryGeneric   , 40, { 11, 9 , 0 , 0 , 0 , 0  } }, // #100 [ref=1x]
  { InstDB::RWInfo::kCategoryGeneric   , 11, { 11, 3 , 0 , 0 , 0 , 0  } }, // #101 [ref=7x]
  { InstDB::RWInfo::kCategoryVmov2_1   , 41, { 0 , 0 , 0 , 0 , 0 , 0  } }, // #102 [ref=14x]
  { InstDB::RWInfo::kCategoryVmov1_2   , 14, { 0 , 0 , 0 , 0 , 0 , 0  } }, // #103 [ref=7x]
  { InstDB::RWInfo::kCategoryGeneric   , 45, { 74, 43, 0 , 0 , 0 , 0  } }, // #104 [ref=6x]
  { InstDB::RWInfo::kCategoryGeneric   , 5 , { 44, 9 , 0 , 0 , 0 , 0  } }, // #105 [ref=1x]
  { InstDB::RWInfo::kCategoryGeneric   , 18, { 2 , 3 , 0 , 0 , 0 , 0  } }, // #106 [ref=2x]
  { InstDB::RWInfo::kCategoryGeneric   , 52, { 11, 3 , 0 , 0 , 0 , 0  } }, // #107 [ref=12x]
  { InstDB::RWInfo::kCategoryVmovddup  , 34, { 0 , 0 , 0 , 0 , 0 , 0  } }, // #108 [ref=1x]
  { InstDB::RWInfo::kCategoryGeneric   , 12, { 35, 61, 0 , 0 , 0 , 0  } }, // #109 [ref=2x]
  { InstDB::RWInfo::kCategoryVmovmskpd , 0 , { 0 , 0 , 0 , 0 , 0 , 0  } }, // #110 [ref=1x]
  { InstDB::RWInfo::kCategoryVmovmskps , 0 , { 0 , 0 , 0 , 0 , 0 , 0  } }, // #111 [ref=1x]
  { InstDB::RWInfo::kCategoryGeneric   , 53, { 35, 7 , 0 , 0 , 0 , 0  } }, // #112 [ref=1x]
  { InstDB::RWInfo::kCategoryGeneric   , 2 , { 3 , 3 , 0 , 0 , 0 , 0  } }, // #113 [ref=4x]
  { InstDB::RWInfo::kCategoryGeneric   , 15, { 11, 40, 0 , 0 , 0 , 0  } }, // #114 [ref=1x]
  { InstDB::RWInfo::kCategoryGeneric   , 0 , { 11, 7 , 0 , 0 , 0 , 0  } }, // #115 [ref=6x]
  { InstDB::RWInfo::kCategoryGeneric   , 27, { 11, 13, 0 , 0 , 0 , 0  } }, // #116 [ref=1x]
  { InstDB::RWInfo::kCategoryGeneric   , 0 , { 35, 3 , 0 , 0 , 0 , 0  } }, // #117 [ref=4x]
  { InstDB::RWInfo::kCategoryVmov1_4   , 57, { 0 , 0 , 0 , 0 , 0 , 0  } }, // #118 [ref=6x]
  { InstDB::RWInfo::kCategoryVmov1_2   , 42, { 0 , 0 , 0 , 0 , 0 , 0  } }, // #119 [ref=9x]
  { InstDB::RWInfo::kCategoryVmov1_8   , 58, { 0 , 0 , 0 , 0 , 0 , 0  } }, // #120 [ref=3x]
  { InstDB::RWInfo::kCategoryVmov4_1   , 59, { 0 , 0 , 0 , 0 , 0 , 0  } }, // #121 [ref=4x]
  { InstDB::RWInfo::kCategoryVmov8_1   , 60, { 0 , 0 , 0 , 0 , 0 , 0  } }, // #122 [ref=2x]
  { InstDB::RWInfo::kCategoryGeneric   , 18, { 11, 3 , 0 , 0 , 0 , 0  } }, // #123 [ref=2x]
  { InstDB::RWInfo::kCategoryGeneric   , 17, { 44, 9 , 0 , 0 , 0 , 0  } }, // #124 [ref=2x]
  { InstDB::RWInfo::kCategoryGeneric   , 32, { 35, 7 , 0 , 0 , 0 , 0  } }, // #125 [ref=2x]
  { InstDB::RWInfo::kCategoryGeneric   , 11, { 2 , 2 , 0 , 0 , 0 , 0  } }, // #126 [ref=1x]
  { InstDB::RWInfo::kCategoryGeneric   , 52, { 2 , 2 , 0 , 0 , 0 , 0  } }  // #127 [ref=1x]
};

const InstDB::RWInfo InstDB::rwInfoB[] = {
  { InstDB::RWInfo::kCategoryGeneric   , 0 , { 0 , 0 , 0 , 0 , 0 , 0  } }, // #0 [ref=742x]
  { InstDB::RWInfo::kCategoryGeneric   , 0 , { 1 , 0 , 0 , 0 , 0 , 0  } }, // #1 [ref=5x]
  { InstDB::RWInfo::kCategoryGeneric   , 3 , { 10, 5 , 0 , 0 , 0 , 0  } }, // #2 [ref=7x]
  { InstDB::RWInfo::kCategoryGeneric   , 6 , { 11, 3 , 3 , 0 , 0 , 0  } }, // #3 [ref=186x]
  { InstDB::RWInfo::kCategoryGeneric   , 2 , { 11, 3 , 3 , 0 , 0 , 0  } }, // #4 [ref=5x]
  { InstDB::RWInfo::kCategoryGeneric   , 3 , { 4 , 5 , 0 , 0 , 0 , 0  } }, // #5 [ref=14x]
  { InstDB::RWInfo::kCategoryGeneric   , 3 , { 4 , 5 , 14, 0 , 0 , 0  } }, // #6 [ref=4x]
  { InstDB::RWInfo::kCategoryGeneric   , 0 , { 2 , 0 , 0 , 0 , 0 , 0  } }, // #7 [ref=1x]
  { InstDB::RWInfo::kCategoryGeneric   , 11, { 3 , 0 , 0 , 0 , 0 , 0  } }, // #8 [ref=2x]
  { InstDB::RWInfo::kCategoryGeneric   , 0 , { 18, 0 , 0 , 0 , 0 , 0  } }, // #9 [ref=1x]
  { InstDB::RWInfo::kCategoryGeneric   , 8 , { 3 , 0 , 0 , 0 , 0 , 0  } }, // #10 [ref=34x]
  { InstDB::RWInfo::kCategoryGeneric   , 12, { 7 , 0 , 0 , 0 , 0 , 0  } }, // #11 [ref=4x]
  { InstDB::RWInfo::kCategoryGeneric   , 0 , { 19, 0 , 0 , 0 , 0 , 0  } }, // #12 [ref=1x]
  { InstDB::RWInfo::kCategoryGeneric   , 4 , { 6 , 7 , 0 , 0 , 0 , 0  } }, // #13 [ref=1x]
  { InstDB::RWInfo::kCategoryGeneric   , 5 , { 8 , 9 , 0 , 0 , 0 , 0  } }, // #14 [ref=1x]
  { InstDB::RWInfo::kCategoryGeneric   , 11, { 2 , 3 , 22, 0 , 0 , 0  } }, // #15 [ref=1x]
  { InstDB::RWInfo::kCategoryGeneric   , 13, { 4 , 23, 18, 24, 25, 0  } }, // #16 [ref=1x]
  { InstDB::RWInfo::kCategoryGeneric   , 12, { 26, 27, 28, 29, 30, 0  } }, // #17 [ref=1x]
  { InstDB::RWInfo::kCategoryGeneric   , 0 , { 28, 31, 32, 16, 0 , 0  } }, // #18 [ref=1x]
  { InstDB::RWInfo::kCategoryGeneric   , 0 , { 28, 0 , 0 , 0 , 0 , 0  } }, // #19 [ref=2x]
  { InstDB::RWInfo::kCategoryGeneric   , 10, { 2 , 0 , 0 , 0 , 0 , 0  } }, // #20 [ref=4x]
  { InstDB::RWInfo::kCategoryGeneric   , 6 , { 41, 42, 3 , 0 , 0 , 0  } }, // #21 [ref=2x]
  { InstDB::RWInfo::kCategoryGeneric   , 17, { 44, 5 , 0 , 0 , 0 , 0  } }, // #22 [ref=4x]
  { InstDB::RWInfo::kCategoryGeneric   , 0 , { 4 , 0 , 0 , 0 , 0 , 0  } }, // #23 [ref=1x]
  { InstDB::RWInfo::kCategoryGeneric   , 18, { 3 , 0 , 0 , 0 , 0 , 0  } }, // #24 [ref=17x]
  { InstDB::RWInfo::kCategoryGeneric   , 0 , { 45, 0 , 0 , 0 , 0 , 0  } }, // #25 [ref=16x]
  { InstDB::RWInfo::kCategoryGeneric   , 19, { 46, 0 , 0 , 0 , 0 , 0  } }, // #26 [ref=1x]
  { InstDB::RWInfo::kCategoryGeneric   , 19, { 47, 0 , 0 , 0 , 0 , 0  } }, // #27 [ref=1x]
  { InstDB::RWInfo::kCategoryGeneric   , 20, { 3 , 0 , 0 , 0 , 0 , 0  } }, // #28 [ref=3x]
  { InstDB::RWInfo::kCategoryGeneric   , 0 , { 46, 0 , 0 , 0 , 0 , 0  } }, // #29 [ref=6x]
  { InstDB::RWInfo::kCategoryGeneric   , 18, { 11, 0 , 0 , 0 , 0 , 0  } }, // #30 [ref=3x]
  { InstDB::RWInfo::kCategoryGeneric   , 21, { 13, 0 , 0 , 0 , 0 , 0  } }, // #31 [ref=1x]
  { InstDB::RWInfo::kCategoryGeneric   , 8 , { 11, 0 , 0 , 0 , 0 , 0  } }, // #32 [ref=8x]
  { InstDB::RWInfo::kCategoryGeneric   , 21, { 48, 0 , 0 , 0 , 0 , 0  } }, // #33 [ref=2x]
  { InstDB::RWInfo::kCategoryGeneric   , 7 , { 49, 0 , 0 , 0 , 0 , 0  } }, // #34 [ref=2x]
  { InstDB::RWInfo::kCategoryGeneric   , 20, { 11, 0 , 0 , 0 , 0 , 0  } }, // #35 [ref=2x]
  { InstDB::RWInfo::kCategoryImul      , 22, { 0 , 0 , 0 , 0 , 0 , 0  } }, // #36 [ref=1x]
  { InstDB::RWInfo::kCategoryGeneric   , 0 , { 53, 0 , 0 , 0 , 0 , 0  } }, // #37 [ref=1x]
  { InstDB::RWInfo::kCategoryGeneric   , 0 , { 26, 0 , 0 , 0 , 0 , 0  } }, // #38 [ref=1x]
  { InstDB::RWInfo::kCategoryGeneric   , 5 , { 4 , 9 , 0 , 0 , 0 , 0  } }, // #39 [ref=2x]
  { InstDB::RWInfo::kCategoryGeneric   , 0 , { 4 , 5 , 0 , 0 , 0 , 0  } }, // #40 [ref=1x]
  { InstDB::RWInfo::kCategoryGeneric   , 0 , { 56, 40, 40, 0 , 0 , 0  } }, // #41 [ref=6x]
  { InstDB::RWInfo::kCategoryGeneric   , 0 , { 44, 9 , 9 , 0 , 0 , 0  } }, // #42 [ref=6x]
  { InstDB::RWInfo::kCategoryGeneric   , 0 , { 35, 7 , 7 , 0 , 0 , 0  } }, // #43 [ref=6x]
  { InstDB::RWInfo::kCategoryGeneric   , 0 , { 48, 13, 13, 0 , 0 , 0  } }, // #44 [ref=6x]
  { InstDB::RWInfo::kCategoryGeneric   , 0 , { 56, 40, 0 , 0 , 0 , 0  } }, // #45 [ref=2x]
  { InstDB::RWInfo::kCategoryGeneric   , 0 , { 44, 9 , 0 , 0 , 0 , 0  } }, // #46 [ref=2x]
  { InstDB::RWInfo::kCategoryGeneric   , 0 , { 35, 7 , 0 , 0 , 0 , 0  } }, // #47 [ref=2x]
  { InstDB::RWInfo::kCategoryGeneric   , 0 , { 48, 13, 0 , 0 , 0 , 0  } }, // #48 [ref=2x]
  { InstDB::RWInfo::kCategoryGeneric   , 0 , { 48, 40, 40, 0 , 0 , 0  } }, // #49 [ref=1x]
  { InstDB::RWInfo::kCategoryGeneric   , 0 , { 35, 9 , 9 , 0 , 0 , 0  } }, // #50 [ref=1x]
  { InstDB::RWInfo::kCategoryGeneric   , 0 , { 44, 13, 13, 0 , 0 , 0  } }, // #51 [ref=1x]
  { InstDB::RWInfo::kCategoryGeneric   , 0 , { 57, 0 , 0 , 0 , 0 , 0  } }, // #52 [ref=1x]
  { InstDB::RWInfo::kCategoryGeneric   , 28, { 9 , 0 , 0 , 0 , 0 , 0  } }, // #53 [ref=2x]
  { InstDB::RWInfo::kCategoryGeneric   , 16, { 43, 0 , 0 , 0 , 0 , 0  } }, // #54 [ref=1x]
  { InstDB::RWInfo::kCategoryGeneric   , 7 , { 13, 0 , 0 , 0 , 0 , 0  } }, // #55 [ref=5x]
  { InstDB::RWInfo::kCategoryGeneric   , 0 , { 3 , 0 , 0 , 0 , 0 , 0  } }, // #56 [ref=4x]
  { InstDB::RWInfo::kCategoryGeneric   , 5 , { 3 , 9 , 0 , 0 , 0 , 0  } }, // #57 [ref=2x]
  { InstDB::RWInfo::kCategoryGeneric   , 0 , { 5 , 5 , 59, 0 , 0 , 0  } }, // #58 [ref=2x]
  { InstDB::RWInfo::kCategoryGeneric   , 0 , { 7 , 7 , 59, 0 , 0 , 0  } }, // #59 [ref=1x]
  { InstDB::RWInfo::kCategoryGeneric   , 0 , { 19, 29, 60, 0 , 0 , 0  } }, // #60 [ref=2x]
  { InstDB::RWInfo::kCategoryGeneric   , 6 , { 64, 42, 3 , 0 , 0 , 0  } }, // #61 [ref=1x]
  { InstDB::RWInfo::kCategoryGeneric   , 6 , { 11, 11, 3 , 65, 0 , 0  } }, // #62 [ref=1x]
  { InstDB::RWInfo::kCategoryGeneric   , 0 , { 17, 29, 30, 0 , 0 , 0  } }, // #63 [ref=1x]
  { InstDB::RWInfo::kCategoryGeneric   , 10, { 3 , 0 , 0 , 0 , 0 , 0  } }, // #64 [ref=2x]
  { InstDB::RWInfo::kCategoryGeneric   , 2 , { 2 , 3 , 0 , 0 , 0 , 0  } }, // #65 [ref=1x]
  { InstDB::RWInfo::kCategoryGeneric   , 3 , { 5 , 5 , 0 , 67, 17, 60 } }, // #66 [ref=2x]
  { InstDB::RWInfo::kCategoryGeneric   , 3 , { 5 , 5 , 0 , 68, 17, 60 } }, // #67 [ref=2x]
  { InstDB::RWInfo::kCategoryGeneric   , 3 , { 5 , 5 , 0 , 67, 0 , 0  } }, // #68 [ref=2x]
  { InstDB::RWInfo::kCategoryGeneric   , 3 , { 5 , 5 , 0 , 68, 0 , 0  } }, // #69 [ref=2x]
  { InstDB::RWInfo::kCategoryGeneric   , 31, { 56, 5 , 0 , 0 , 0 , 0  } }, // #70 [ref=2x]
  { InstDB::RWInfo::kCategoryGeneric   , 32, { 35, 5 , 0 , 0 , 0 , 0  } }, // #71 [ref=2x]
  { InstDB::RWInfo::kCategoryGeneric   , 33, { 48, 3 , 0 , 0 , 0 , 0  } }, // #72 [ref=1x]
  { InstDB::RWInfo::kCategoryGeneric   , 15, { 4 , 40, 0 , 0 , 0 , 0  } }, // #73 [ref=1x]
  { InstDB::RWInfo::kCategoryGeneric   , 4 , { 4 , 7 , 0 , 0 , 0 , 0  } }, // #74 [ref=1x]
  { InstDB::RWInfo::kCategoryGeneric   , 27, { 2 , 13, 0 , 0 , 0 , 0  } }, // #75 [ref=1x]
  { InstDB::RWInfo::kCategoryGeneric   , 10, { 70, 0 , 0 , 0 , 0 , 0  } }, // #76 [ref=1x]
  { InstDB::RWInfo::kCategoryGeneric   , 4 , { 35, 7 , 0 , 0 , 0 , 0  } }, // #77 [ref=2x]
  { InstDB::RWInfo::kCategoryGeneric   , 10, { 65, 0 , 0 , 0 , 0 , 0  } }, // #78 [ref=1x]
  { InstDB::RWInfo::kCategoryGeneric   , 0 , { 11, 0 , 0 , 0 , 0 , 0  } }, // #79 [ref=6x]
  { InstDB::RWInfo::kCategoryGeneric   , 0 , { 16, 50, 29, 0 , 0 , 0  } }, // #80 [ref=5x]
  { InstDB::RWInfo::kCategoryGeneric   , 0 , { 44, 0 , 0 , 0 , 0 , 0  } }, // #81 [ref=1x]
  { InstDB::RWInfo::kCategoryGeneric   , 0 , { 35, 0 , 0 , 0 , 0 , 0  } }, // #82 [ref=1x]
  { InstDB::RWInfo::kCategoryGeneric   , 0 , { 16, 50, 67, 0 , 0 , 0  } }, // #83 [ref=1x]
  { InstDB::RWInfo::kCategoryGeneric   , 2 , { 11, 3 , 0 , 0 , 0 , 0  } }, // #84 [ref=16x]
  { InstDB::RWInfo::kCategoryGeneric   , 4 , { 36, 7 , 0 , 0 , 0 , 0  } }, // #85 [ref=1x]
  { InstDB::RWInfo::kCategoryGeneric   , 5 , { 37, 9 , 0 , 0 , 0 , 0  } }, // #86 [ref=1x]
  { InstDB::RWInfo::kCategoryGeneric   , 0 , { 72, 0 , 0 , 0 , 0 , 0  } }, // #87 [ref=1x]
  { InstDB::RWInfo::kCategoryGeneric   , 0 , { 7 , 0 , 0 , 0 , 0 , 0  } }, // #88 [ref=1x]
  { InstDB::RWInfo::kCategoryGeneric   , 31, { 73, 0 , 0 , 0 , 0 , 0  } }, // #89 [ref=30x]
  { InstDB::RWInfo::kCategoryGeneric   , 11, { 2 , 3 , 71, 0 , 0 , 0  } }, // #90 [ref=2x]
  { InstDB::RWInfo::kCategoryGeneric   , 35, { 11, 0 , 0 , 0 , 0 , 0  } }, // #91 [ref=3x]
  { InstDB::RWInfo::kCategoryGeneric   , 28, { 44, 0 , 0 , 0 , 0 , 0  } }, // #92 [ref=2x]
  { InstDB::RWInfo::kCategoryGeneric   , 16, { 74, 0 , 0 , 0 , 0 , 0  } }, // #93 [ref=1x]
  { InstDB::RWInfo::kCategoryGeneric   , 0 , { 75, 43, 43, 0 , 0 , 0  } }, // #94 [ref=5x]
  { InstDB::RWInfo::kCategoryGeneric   , 0 , { 74, 0 , 0 , 0 , 0 , 0  } }, // #95 [ref=1x]
  { InstDB::RWInfo::kCategoryGeneric   , 0 , { 9 , 60, 17, 0 , 0 , 0  } }, // #96 [ref=2x]
  { InstDB::RWInfo::kCategoryGeneric   , 13, { 75, 43, 43, 43, 43, 5  } }, // #97 [ref=2x]
  { InstDB::RWInfo::kCategoryGeneric   , 13, { 4 , 5 , 5 , 5 , 5 , 5  } }, // #98 [ref=2x]
  { InstDB::RWInfo::kCategoryGeneric   , 36, { 10, 5 , 7 , 0 , 0 , 0  } }, // #99 [ref=8x]
  { InstDB::RWInfo::kCategoryGeneric   , 37, { 10, 5 , 9 , 0 , 0 , 0  } }, // #100 [ref=9x]
  { InstDB::RWInfo::kCategoryGeneric   , 6 , { 11, 3 , 3 , 3 , 0 , 0  } }, // #101 [ref=3x]
  { InstDB::RWInfo::kCategoryGeneric   , 36, { 11, 5 , 7 , 0 , 0 , 0  } }, // #102 [ref=1x]
  { InstDB::RWInfo::kCategoryGeneric   , 37, { 11, 5 , 9 , 0 , 0 , 0  } }, // #103 [ref=1x]
  { InstDB::RWInfo::kCategoryVmov1_2   , 42, { 0 , 0 , 0 , 0 , 0 , 0  } }, // #104 [ref=1x]
  { InstDB::RWInfo::kCategoryGeneric   , 36, { 10, 78, 7 , 0 , 0 , 0  } }, // #105 [ref=1x]
  { InstDB::RWInfo::kCategoryGeneric   , 43, { 10, 61, 3 , 0 , 0 , 0  } }, // #106 [ref=2x]
  { InstDB::RWInfo::kCategoryGeneric   , 43, { 10, 78, 3 , 0 , 0 , 0  } }, // #107 [ref=2x]
  { InstDB::RWInfo::kCategoryGeneric   , 37, { 10, 61, 9 , 0 , 0 , 0  } }, // #108 [ref=1x]
  { InstDB::RWInfo::kCategoryGeneric   , 44, { 10, 5 , 5 , 0 , 0 , 0  } }, // #109 [ref=9x]
  { InstDB::RWInfo::kCategoryGeneric   , 46, { 10, 77, 0 , 0 , 0 , 0  } }, // #110 [ref=2x]
  { InstDB::RWInfo::kCategoryGeneric   , 46, { 10, 3 , 0 , 0 , 0 , 0  } }, // #111 [ref=4x]
  { InstDB::RWInfo::kCategoryGeneric   , 47, { 76, 43, 0 , 0 , 0 , 0  } }, // #112 [ref=4x]
  { InstDB::RWInfo::kCategoryGeneric   , 6 , { 2 , 3 , 3 , 0 , 0 , 0  } }, // #113 [ref=60x]
  { InstDB::RWInfo::kCategoryGeneric   , 36, { 4 , 61, 7 , 0 , 0 , 0  } }, // #114 [ref=1x]
  { InstDB::RWInfo::kCategoryGeneric   , 37, { 4 , 78, 9 , 0 , 0 , 0  } }, // #115 [ref=1x]
  { InstDB::RWInfo::kCategoryGeneric   , 36, { 6 , 7 , 7 , 0 , 0 , 0  } }, // #116 [ref=11x]
  { InstDB::RWInfo::kCategoryGeneric   , 37, { 8 , 9 , 9 , 0 , 0 , 0  } }, // #117 [ref=11x]
  { InstDB::RWInfo::kCategoryGeneric   , 48, { 11, 3 , 3 , 3 , 0 , 0  } }, // #118 [ref=15x]
  { InstDB::RWInfo::kCategoryGeneric   , 49, { 35, 7 , 7 , 7 , 0 , 0  } }, // #119 [ref=4x]
  { InstDB::RWInfo::kCategoryGeneric   , 50, { 44, 9 , 9 , 9 , 0 , 0  } }, // #120 [ref=4x]
  { InstDB::RWInfo::kCategoryGeneric   , 36, { 26, 7 , 7 , 0 , 0 , 0  } }, // #121 [ref=1x]
  { InstDB::RWInfo::kCategoryGeneric   , 37, { 53, 9 , 9 , 0 , 0 , 0  } }, // #122 [ref=1x]
  { InstDB::RWInfo::kCategoryGeneric   , 14, { 35, 3 , 0 , 0 , 0 , 0  } }, // #123 [ref=2x]
  { InstDB::RWInfo::kCategoryGeneric   , 5 , { 35, 9 , 0 , 0 , 0 , 0  } }, // #124 [ref=1x]
  { InstDB::RWInfo::kCategoryGeneric   , 8 , { 2 , 3 , 2 , 0 , 0 , 0  } }, // #125 [ref=2x]
  { InstDB::RWInfo::kCategoryGeneric   , 0 , { 2 , 3 , 2 , 0 , 0 , 0  } }, // #126 [ref=4x]
  { InstDB::RWInfo::kCategoryGeneric   , 18, { 4 , 3 , 4 , 0 , 0 , 0  } }, // #127 [ref=2x]
  { InstDB::RWInfo::kCategoryGeneric   , 36, { 10, 61, 7 , 0 , 0 , 0  } }, // #128 [ref=11x]
  { InstDB::RWInfo::kCategoryGeneric   , 37, { 10, 78, 9 , 0 , 0 , 0  } }, // #129 [ref=13x]
  { InstDB::RWInfo::kCategoryGeneric   , 44, { 76, 77, 5 , 0 , 0 , 0  } }, // #130 [ref=2x]
  { InstDB::RWInfo::kCategoryGeneric   , 44, { 11, 3 , 5 , 0 , 0 , 0  } }, // #131 [ref=4x]
  { InstDB::RWInfo::kCategoryGeneric   , 51, { 74, 43, 77, 0 , 0 , 0  } }, // #132 [ref=4x]
  { InstDB::RWInfo::kCategoryVmaskmov  , 0 , { 0 , 0 , 0 , 0 , 0 , 0  } }, // #133 [ref=4x]
  { InstDB::RWInfo::kCategoryGeneric   , 12, { 35, 0 , 0 , 0 , 0 , 0  } }, // #134 [ref=2x]
  { InstDB::RWInfo::kCategoryGeneric   , 0 , { 22, 0 , 0 , 0 , 0 , 0  } }, // #135 [ref=2x]
  { InstDB::RWInfo::kCategoryGeneric   , 0 , { 10, 61, 61, 0 , 0 , 0  } }, // #136 [ref=1x]
  { InstDB::RWInfo::kCategoryGeneric   , 12, { 10, 7 , 7 , 0 , 0 , 0  } }, // #137 [ref=2x]
  { InstDB::RWInfo::kCategoryGeneric   , 0 , { 10, 7 , 7 , 0 , 0 , 0  } }, // #138 [ref=1x]
  { InstDB::RWInfo::kCategoryGeneric   , 12, { 10, 61, 7 , 0 , 0 , 0  } }, // #139 [ref=2x]
  { InstDB::RWInfo::kCategoryGeneric   , 0 , { 10, 61, 7 , 0 , 0 , 0  } }, // #140 [ref=1x]
  { InstDB::RWInfo::kCategoryGeneric   , 0 , { 10, 78, 9 , 0 , 0 , 0  } }, // #141 [ref=1x]
  { InstDB::RWInfo::kCategoryGeneric   , 0 , { 79, 0 , 0 , 0 , 0 , 0  } }, // #142 [ref=1x]
  { InstDB::RWInfo::kCategoryGeneric   , 54, { 35, 11, 3 , 3 , 0 , 0  } }, // #143 [ref=2x]
  { InstDB::RWInfo::kCategoryGeneric   , 13, { 74, 43, 43, 43, 43, 5  } }, // #144 [ref=2x]
  { InstDB::RWInfo::kCategoryGeneric   , 6 , { 35, 3 , 3 , 0 , 0 , 0  } }, // #145 [ref=17x]
  { InstDB::RWInfo::kCategoryGeneric   , 51, { 76, 77, 77, 0 , 0 , 0  } }, // #146 [ref=2x]
  { InstDB::RWInfo::kCategoryGeneric   , 22, { 11, 3 , 3 , 0 , 0 , 0  } }, // #147 [ref=4x]
  { InstDB::RWInfo::kCategoryGeneric   , 7 , { 48, 5 , 0 , 0 , 0 , 0  } }, // #148 [ref=1x]
  { InstDB::RWInfo::kCategoryGeneric   , 55, { 10, 5 , 40, 0 , 0 , 0  } }, // #149 [ref=1x]
  { InstDB::RWInfo::kCategoryGeneric   , 56, { 10, 5 , 13, 0 , 0 , 0  } }, // #150 [ref=1x]
  { InstDB::RWInfo::kCategoryGeneric   , 44, { 10, 5 , 5 , 5 , 0 , 0  } }, // #151 [ref=12x]
  { InstDB::RWInfo::kCategoryGeneric   , 61, { 10, 5 , 5 , 5 , 0 , 0  } }, // #152 [ref=1x]
  { InstDB::RWInfo::kCategoryGeneric   , 62, { 10, 5 , 5 , 0 , 0 , 0  } }, // #153 [ref=12x]
  { InstDB::RWInfo::kCategoryGeneric   , 22, { 11, 3 , 5 , 0 , 0 , 0  } }, // #154 [ref=9x]
  { InstDB::RWInfo::kCategoryGeneric   , 63, { 11, 3 , 0 , 0 , 0 , 0  } }, // #155 [ref=2x]
  { InstDB::RWInfo::kCategoryGeneric   , 0 , { 60, 17, 29, 0 , 0 , 0  } }, // #156 [ref=2x]
  { InstDB::RWInfo::kCategoryGeneric   , 8 , { 3 , 60, 17, 0 , 0 , 0  } }, // #157 [ref=4x]
  { InstDB::RWInfo::kCategoryGeneric   , 8 , { 11, 60, 17, 0 , 0 , 0  } }  // #158 [ref=8x]
};

const InstDB::RWInfoOp InstDB::rwInfoOp[] = {
  { 0x0000000000000000u, 0x0000000000000000u, 0xFF, { 0 }, 0 }, // #0 [ref=15529x]
  { 0x0000000000000003u, 0x0000000000000003u, 0x00, { 0 }, OpRWInfo::kRW | OpRWInfo::kRegPhysId }, // #1 [ref=10x]
  { 0x0000000000000000u, 0x0000000000000000u, 0xFF, { 0 }, OpRWInfo::kRW | OpRWInfo::kZExt }, // #2 [ref=214x]
  { 0x0000000000000000u, 0x0000000000000000u, 0xFF, { 0 }, OpRWInfo::kRead }, // #3 [ref=987x]
  { 0x000000000000FFFFu, 0x000000000000FFFFu, 0xFF, { 0 }, OpRWInfo::kRW | OpRWInfo::kZExt }, // #4 [ref=92x]
  { 0x000000000000FFFFu, 0x0000000000000000u, 0xFF, { 0 }, OpRWInfo::kRead }, // #5 [ref=305x]
  { 0x00000000000000FFu, 0x00000000000000FFu, 0xFF, { 0 }, OpRWInfo::kRW }, // #6 [ref=18x]
  { 0x00000000000000FFu, 0x0000000000000000u, 0xFF, { 0 }, OpRWInfo::kRead }, // #7 [ref=186x]
  { 0x000000000000000Fu, 0x000000000000000Fu, 0xFF, { 0 }, OpRWInfo::kRW }, // #8 [ref=18x]
  { 0x000000000000000Fu, 0x0000000000000000u, 0xFF, { 0 }, OpRWInfo::kRead }, // #9 [ref=136x]
  { 0x0000000000000000u, 0x000000000000FFFFu, 0xFF, { 0 }, OpRWInfo::kWrite | OpRWInfo::kZExt }, // #10 [ref=160x]
  { 0x0000000000000000u, 0x0000000000000000u, 0xFF, { 0 }, OpRWInfo::kWrite | OpRWInfo::kZExt }, // #11 [ref=420x]
  { 0x0000000000000003u, 0x0000000000000003u, 0xFF, { 0 }, OpRWInfo::kRW }, // #12 [ref=1x]
  { 0x0000000000000003u, 0x0000000000000000u, 0xFF, { 0 }, OpRWInfo::kRead }, // #13 [ref=34x]
  { 0x000000000000FFFFu, 0x0000000000000000u, 0x00, { 0 }, OpRWInfo::kRead | OpRWInfo::kRegPhysId }, // #14 [ref=4x]
  { 0x0000000000000000u, 0x0000000000000000u, 0xFF, { 0 }, OpRWInfo::kWrite | OpRWInfo::kZExt | OpRWInfo::kMemBaseWrite | OpRWInfo::kMemIndexWrite }, // #15 [ref=1x]
  { 0x0000000000000000u, 0x000000000000000Fu, 0x02, { 0 }, OpRWInfo::kWrite | OpRWInfo::kZExt | OpRWInfo::kRegPhysId }, // #16 [ref=9x]
  { 0x000000000000000Fu, 0x0000000000000000u, 0x00, { 0 }, OpRWInfo::kRead | OpRWInfo::kRegPhysId }, // #17 [ref=23x]
  { 0x00000000000000FFu, 0x00000000000000FFu, 0x00, { 0 }, OpRWInfo::kRW | OpRWInfo::kZExt | OpRWInfo::kRegPhysId }, // #18 [ref=2x]
  { 0x0000000000000000u, 0x0000000000000000u, 0x00, { 0 }, OpRWInfo::kRead | OpRWInfo::kMemPhysId }, // #19 [ref=3x]
  { 0x0000000000000000u, 0x0000000000000000u, 0x06, { 0 }, OpRWInfo::kRead | OpRWInfo::kMemBaseRW | OpRWInfo::kMemBasePostModify | OpRWInfo::kMemPhysId }, // #20 [ref=3x]
  { 0x0000000000000000u, 0x0000000000000000u, 0x07, { 0 }, OpRWInfo::kRead | OpRWInfo::kMemBaseRW | OpRWInfo::kMemBasePostModify | OpRWInfo::kMemPhysId }, // #21 [ref=2x]
  { 0x0000000000000000u, 0x0000000000000000u, 0x00, { 0 }, OpRWInfo::kRead | OpRWInfo::kRegPhysId }, // #22 [ref=7x]
  { 0x00000000000000FFu, 0x00000000000000FFu, 0x02, { 0 }, OpRWInfo::kRW | OpRWInfo::kZExt | OpRWInfo::kRegPhysId }, // #23 [ref=1x]
  { 0x00000000000000FFu, 0x0000000000000000u, 0x01, { 0 }, OpRWInfo::kRead | OpRWInfo::kRegPhysId }, // #24 [ref=1x]
  { 0x00000000000000FFu, 0x0000000000000000u, 0x03, { 0 }, OpRWInfo::kRead | OpRWInfo::kRegPhysId }, // #25 [ref=1x]
  { 0x00000000000000FFu, 0x00000000000000FFu, 0xFF, { 0 }, OpRWInfo::kRW | OpRWInfo::kZExt }, // #26 [ref=21x]
  { 0x000000000000000Fu, 0x000000000000000Fu, 0x02, { 0 }, OpRWInfo::kRW | OpRWInfo::kZExt | OpRWInfo::kRegPhysId }, // #27 [ref=1x]
  { 0x000000000000000Fu, 0x000000000000000Fu, 0x00, { 0 }, OpRWInfo::kRW | OpRWInfo::kZExt | OpRWInfo::kRegPhysId }, // #28 [ref=4x]
  { 0x000000000000000Fu, 0x0000000000000000u, 0x01, { 0 }, OpRWInfo::kRead | OpRWInfo::kRegPhysId }, // #29 [ref=13x]
  { 0x000000000000000Fu, 0x0000000000000000u, 0x03, { 0 }, OpRWInfo::kRead | OpRWInfo::kRegPhysId }, // #30 [ref=2x]
  { 0x0000000000000000u, 0x000000000000000Fu, 0x03, { 0 }, OpRWInfo::kWrite | OpRWInfo::kZExt | OpRWInfo::kRegPhysId }, // #31 [ref=1x]
  { 0x000000000000000Fu, 0x000000000000000Fu, 0x01, { 0 }, OpRWInfo::kRW | OpRWInfo::kZExt | OpRWInfo::kRegPhysId }, // #32 [ref=1x]
  { 0x0000000000000000u, 0x00000000000000FFu, 0x02, { 0 }, OpRWInfo::kWrite | OpRWInfo::kZExt | OpRWInfo::kRegPhysId }, // #33 [ref=1x]
  { 0x00000000000000FFu, 0x0000000000000000u, 0x00, { 0 }, OpRWInfo::kRead | OpRWInfo::kRegPhysId }, // #34 [ref=1x]
  { 0x0000000000000000u, 0x00000000000000FFu, 0xFF, { 0 }, OpRWInfo::kWrite | OpRWInfo::kZExt }, // #35 [ref=80x]
  { 0x0000000000000000u, 0x00000000000000FFu, 0xFF, { 0 }, OpRWInfo::kWrite }, // #36 [ref=6x]
  { 0x0000000000000000u, 0x000000000000000Fu, 0xFF, { 0 }, OpRWInfo::kWrite }, // #37 [ref=6x]
  { 0x0000000000000000u, 0x0000000000000003u, 0x02, { 0 }, OpRWInfo::kWrite | OpRWInfo::kRegPhysId }, // #38 [ref=1x]
  { 0x0000000000000003u, 0x0000000000000000u, 0x00, { 0 }, OpRWInfo::kRead | OpRWInfo::kRegPhysId }, // #39 [ref=1x]
  { 0x0000000000000001u, 0x0000000000000000u, 0xFF, { 0 }, OpRWInfo::kRead }, // #40 [ref=28x]
  { 0x0000000000000000u, 0x0000000000000000u, 0x02, { 0 }, OpRWInfo::kRW | OpRWInfo::kRegPhysId | OpRWInfo::kZExt }, // #41 [ref=2x]
  { 0x0000000000000000u, 0x0000000000000000u, 0x00, { 0 }, OpRWInfo::kRW | OpRWInfo::kRegPhysId | OpRWInfo::kZExt }, // #42 [ref=3x]
  { 0xFFFFFFFFFFFFFFFFu, 0x0000000000000000u, 0xFF, { 0 }, OpRWInfo::kRead }, // #43 [ref=45x]
  { 0x0000000000000000u, 0x000000000000000Fu, 0xFF, { 0 }, OpRWInfo::kWrite | OpRWInfo::kZExt }, // #44 [ref=30x]
  { 0x00000000000003FFu, 0x00000000000003FFu, 0xFF, { 0 }, OpRWInfo::kRW | OpRWInfo::kZExt }, // #45 [ref=22x]
  { 0x00000000000003FFu, 0x0000000000000000u, 0xFF, { 0 }, OpRWInfo::kRead }, // #46 [ref=13x]
  { 0x0000000000000000u, 0x00000000000003FFu, 0xFF, { 0 }, OpRWInfo::kWrite | OpRWInfo::kZExt }, // #47 [ref=1x]
  { 0x0000000000000000u, 0x0000000000000003u, 0xFF, { 0 }, OpRWInfo::kWrite | OpRWInfo::kZExt }, // #48 [ref=15x]
  { 0x0000000000000000u, 0x0000000000000003u, 0x00, { 0 }, OpRWInfo::kWrite | OpRWInfo::kRegPhysId | OpRWInfo::kZExt }, // #49 [ref=2x]
  { 0x0000000000000000u, 0x000000000000000Fu, 0x00, { 0 }, OpRWInfo::kWrite | OpRWInfo::kZExt | OpRWInfo::kRegPhysId }, // #50 [ref=8x]
  { 0x0000000000000000u, 0x0000000000000000u, 0x00, { 0 }, OpRWInfo::kWrite | OpRWInfo::kRegPhysId | OpRWInfo::kZExt }, // #51 [ref=2x]
  { 0x0000000000000003u, 0x0000000000000000u, 0x02, { 0 }, OpRWInfo::kRead | OpRWInfo::kRegPhysId }, // #52 [ref=4x]
  { 0x000000000000000Fu, 0x000000000000000Fu, 0xFF, { 0 }, OpRWInfo::kRW | OpRWInfo::kZExt }, // #53 [ref=4x]
  { 0x0000000000000000u, 0x0000000000000000u, 0x07, { 0 }, OpRWInfo::kWrite | OpRWInfo::kZExt | OpRWInfo::kMemPhysId }, // #54 [ref=1x]
  { 0x0000000000000000u, 0x0000000000000000u, 0x01, { 0 }, OpRWInfo::kRead | OpRWInfo::kRegPhysId }, // #55 [ref=1x]
  { 0x0000000000000000u, 0x0000000000000001u, 0xFF, { 0 }, OpRWInfo::kWrite | OpRWInfo::kZExt }, // #56 [ref=14x]
  { 0x0000000000000000u, 0x0000000000000001u, 0x00, { 0 }, OpRWInfo::kWrite | OpRWInfo::kRegPhysId }, // #57 [ref=1x]
  { 0x0000000000000000u, 0x0000000000000000u, 0x01, { 0 }, OpRWInfo::kRW | OpRWInfo::kRegPhysId | OpRWInfo::kZExt }, // #58 [ref=3x]
  { 0x0000000000000000u, 0x0000000000000000u, 0x07, { 0 }, OpRWInfo::kRW | OpRWInfo::kZExt | OpRWInfo::kMemPhysId }, // #59 [ref=3x]
  { 0x000000000000000Fu, 0x0000000000000000u, 0x02, { 0 }, OpRWInfo::kRead | OpRWInfo::kRegPhysId }, // #60 [ref=22x]
  { 0x000000000000FF00u, 0x0000000000000000u, 0xFF, { 0 }, OpRWInfo::kRead }, // #61 [ref=23x]
  { 0x0000000000000000u, 0x000000000000FF00u, 0xFF, { 0 }, OpRWInfo::kWrite }, // #62 [ref=1x]
  { 0x0000000000000000u, 0x0000000000000000u, 0x07, { 0 }, OpRWInfo::kWrite | OpRWInfo::kZExt | OpRWInfo::kMemBaseRW | OpRWInfo::kMemBasePostModify | OpRWInfo::kMemPhysId }, // #63 [ref=2x]
  { 0x0000000000000000u, 0x0000000000000000u, 0x02, { 0 }, OpRWInfo::kWrite | OpRWInfo::kRegPhysId | OpRWInfo::kZExt }, // #64 [ref=1x]
  { 0x0000000000000000u, 0x0000000000000000u, 0x02, { 0 }, OpRWInfo::kRead | OpRWInfo::kRegPhysId }, // #65 [ref=2x]
  { 0x0000000000000000u, 0x0000000000000000u, 0x06, { 0 }, OpRWInfo::kRead | OpRWInfo::kMemPhysId }, // #66 [ref=1x]
  { 0x0000000000000000u, 0x000000000000000Fu, 0x01, { 0 }, OpRWInfo::kWrite | OpRWInfo::kZExt | OpRWInfo::kRegPhysId }, // #67 [ref=5x]
  { 0x0000000000000000u, 0x000000000000FFFFu, 0x00, { 0 }, OpRWInfo::kWrite | OpRWInfo::kZExt | OpRWInfo::kRegPhysId }, // #68 [ref=4x]
  { 0x0000000000000000u, 0x0000000000000007u, 0xFF, { 0 }, OpRWInfo::kWrite | OpRWInfo::kZExt }, // #69 [ref=2x]
  { 0x0000000000000000u, 0x0000000000000000u, 0x04, { 0 }, OpRWInfo::kWrite | OpRWInfo::kZExt | OpRWInfo::kRegPhysId }, // #70 [ref=1x]
  { 0x0000000000000001u, 0x0000000000000000u, 0x01, { 0 }, OpRWInfo::kRead | OpRWInfo::kRegPhysId }, // #71 [ref=10x]
  { 0x0000000000000001u, 0x0000000000000000u, 0x00, { 0 }, OpRWInfo::kRead | OpRWInfo::kRegPhysId }, // #72 [ref=1x]
  { 0x0000000000000000u, 0x0000000000000001u, 0xFF, { 0 }, OpRWInfo::kWrite }, // #73 [ref=30x]
  { 0x0000000000000000u, 0xFFFFFFFFFFFFFFFFu, 0xFF, { 0 }, OpRWInfo::kWrite | OpRWInfo::kZExt }, // #74 [ref=20x]
  { 0xFFFFFFFFFFFFFFFFu, 0xFFFFFFFFFFFFFFFFu, 0xFF, { 0 }, OpRWInfo::kRW | OpRWInfo::kZExt }, // #75 [ref=7x]
  { 0x0000000000000000u, 0x00000000FFFFFFFFu, 0xFF, { 0 }, OpRWInfo::kWrite | OpRWInfo::kZExt }, // #76 [ref=10x]
  { 0x00000000FFFFFFFFu, 0x0000000000000000u, 0xFF, { 0 }, OpRWInfo::kRead }, // #77 [ref=16x]
  { 0x000000000000FFF0u, 0x0000000000000000u, 0xFF, { 0 }, OpRWInfo::kRead }, // #78 [ref=18x]
  { 0x0000000000000000u, 0x0000000000000000u, 0x00, { 0 }, OpRWInfo::kRW | OpRWInfo::kZExt | OpRWInfo::kRegPhysId }  // #79 [ref=1x]
};

const InstDB::RWInfoRm InstDB::rwInfoRm[] = {
  { InstDB::RWInfoRm::kCategoryNone      , 0x00, 0 , 0, 0 }, // #0 [ref=1894x]
  { InstDB::RWInfoRm::kCategoryConsistent, 0x03, 0 , InstDB::RWInfoRm::kFlagAmbiguous, 0 }, // #1 [ref=8x]
  { InstDB::RWInfoRm::kCategoryConsistent, 0x02, 0 , 0, 0 }, // #2 [ref=190x]
  { InstDB::RWInfoRm::kCategoryFixed     , 0x02, 16, 0, 0 }, // #3 [ref=122x]
  { InstDB::RWInfoRm::kCategoryFixed     , 0x02, 8 , 0, 0 }, // #4 [ref=66x]
  { InstDB::RWInfoRm::kCategoryFixed     , 0x02, 4 , 0, 0 }, // #5 [ref=36x]
  { InstDB::RWInfoRm::kCategoryConsistent, 0x04, 0 , 0, 0 }, // #6 [ref=270x]
  { InstDB::RWInfoRm::kCategoryFixed     , 0x01, 2 , 0, 0 }, // #7 [ref=9x]
  { InstDB::RWInfoRm::kCategoryFixed     , 0x00, 0 , 0, 0 }, // #8 [ref=63x]
  { InstDB::RWInfoRm::kCategoryFixed     , 0x03, 0 , 0, 0 }, // #9 [ref=1x]
  { InstDB::RWInfoRm::kCategoryConsistent, 0x01, 0 , InstDB::RWInfoRm::kFlagAmbiguous, 0 }, // #10 [ref=21x]
  { InstDB::RWInfoRm::kCategoryConsistent, 0x01, 0 , 0, 0 }, // #11 [ref=14x]
  { InstDB::RWInfoRm::kCategoryFixed     , 0x00, 8 , 0, 0 }, // #12 [ref=22x]
  { InstDB::RWInfoRm::kCategoryFixed     , 0x00, 16, 0, 0 }, // #13 [ref=21x]
  { InstDB::RWInfoRm::kCategoryConsistent, 0x02, 0 , InstDB::RWInfoRm::kFlagAmbiguous, 0 }, // #14 [ref=15x]
  { InstDB::RWInfoRm::kCategoryFixed     , 0x02, 1 , 0, 0 }, // #15 [ref=5x]
  { InstDB::RWInfoRm::kCategoryFixed     , 0x00, 64, 0, 0 }, // #16 [ref=5x]
  { InstDB::RWInfoRm::kCategoryFixed     , 0x01, 4 , 0, 0 }, // #17 [ref=6x]
  { InstDB::RWInfoRm::kCategoryNone      , 0x00, 0 , InstDB::RWInfoRm::kFlagAmbiguous, 0 }, // #18 [ref=26x]
  { InstDB::RWInfoRm::kCategoryFixed     , 0x00, 10, 0, 0 }, // #19 [ref=2x]
  { InstDB::RWInfoRm::kCategoryNone      , 0x01, 0 , InstDB::RWInfoRm::kFlagAmbiguous, 0 }, // #20 [ref=5x]
  { InstDB::RWInfoRm::kCategoryFixed     , 0x00, 2 , 0, 0 }, // #21 [ref=3x]
  { InstDB::RWInfoRm::kCategoryConsistent, 0x06, 0 , 0, 0 }, // #22 [ref=14x]
  { InstDB::RWInfoRm::kCategoryFixed     , 0x03, 1 , 0, 0 }, // #23 [ref=1x]
  { InstDB::RWInfoRm::kCategoryFixed     , 0x03, 4 , 0, 0 }, // #24 [ref=4x]
  { InstDB::RWInfoRm::kCategoryFixed     , 0x03, 8 , 0, 0 }, // #25 [ref=3x]
  { InstDB::RWInfoRm::kCategoryFixed     , 0x03, 2 , 0, 0 }, // #26 [ref=1x]
  { InstDB::RWInfoRm::kCategoryFixed     , 0x02, 2 , 0, 0 }, // #27 [ref=6x]
  { InstDB::RWInfoRm::kCategoryFixed     , 0x00, 4 , 0, 0 }, // #28 [ref=6x]
  { InstDB::RWInfoRm::kCategoryNone      , 0x03, 0 , InstDB::RWInfoRm::kFlagAmbiguous, 0 }, // #29 [ref=1x]
  { InstDB::RWInfoRm::kCategoryFixed     , 0x03, 16, 0, 0 }, // #30 [ref=6x]
  { InstDB::RWInfoRm::kCategoryFixed     , 0x01, 1 , 0, 0 }, // #31 [ref=32x]
  { InstDB::RWInfoRm::kCategoryFixed     , 0x01, 8 , 0, 0 }, // #32 [ref=4x]
  { InstDB::RWInfoRm::kCategoryFixed     , 0x01, 2 , 0, Features::kSSE4_1 }, // #33 [ref=1x]
  { InstDB::RWInfoRm::kCategoryNone      , 0x02, 0 , 0, 0 }, // #34 [ref=4x]
  { InstDB::RWInfoRm::kCategoryFixed     , 0x01, 2 , InstDB::RWInfoRm::kFlagAmbiguous, 0 }, // #35 [ref=3x]
  { InstDB::RWInfoRm::kCategoryFixed     , 0x04, 8 , 0, 0 }, // #36 [ref=34x]
  { InstDB::RWInfoRm::kCategoryFixed     , 0x04, 4 , 0, 0 }, // #37 [ref=37x]
  { InstDB::RWInfoRm::kCategoryFixed     , 0x00, 32, 0, 0 }, // #38 [ref=4x]
  { InstDB::RWInfoRm::kCategoryFixed     , 0x02, 8 , InstDB::RWInfoRm::kFlagAmbiguous, 0 }, // #39 [ref=1x]
  { InstDB::RWInfoRm::kCategoryFixed     , 0x02, 4 , InstDB::RWInfoRm::kFlagAmbiguous, 0 }, // #40 [ref=1x]
  { InstDB::RWInfoRm::kCategoryHalf      , 0x02, 0 , 0, 0 }, // #41 [ref=14x]
  { InstDB::RWInfoRm::kCategoryHalf      , 0x01, 0 , 0, 0 }, // #42 [ref=10x]
  { InstDB::RWInfoRm::kCategoryConsistent, 0x04, 0 , InstDB::RWInfoRm::kFlagAmbiguous, 0 }, // #43 [ref=4x]
  { InstDB::RWInfoRm::kCategoryFixed     , 0x04, 16, 0, 0 }, // #44 [ref=27x]
  { InstDB::RWInfoRm::kCategoryFixed     , 0x02, 64, 0, 0 }, // #45 [ref=6x]
  { InstDB::RWInfoRm::kCategoryFixed     , 0x01, 16, 0, 0 }, // #46 [ref=6x]
  { InstDB::RWInfoRm::kCategoryFixed     , 0x01, 32, 0, 0 }, // #47 [ref=4x]
  { InstDB::RWInfoRm::kCategoryConsistent, 0x0C, 0 , 0, 0 }, // #48 [ref=15x]
  { InstDB::RWInfoRm::kCategoryFixed     , 0x0C, 8 , 0, 0 }, // #49 [ref=4x]
  { InstDB::RWInfoRm::kCategoryFixed     , 0x0C, 4 , 0, 0 }, // #50 [ref=4x]
  { InstDB::RWInfoRm::kCategoryFixed     , 0x04, 32, 0, 0 }, // #51 [ref=6x]
  { InstDB::RWInfoRm::kCategoryConsistent, 0x03, 0 , 0, 0 }, // #52 [ref=13x]
  { InstDB::RWInfoRm::kCategoryFixed     , 0x03, 8 , InstDB::RWInfoRm::kFlagAmbiguous, 0 }, // #53 [ref=1x]
  { InstDB::RWInfoRm::kCategoryConsistent, 0x08, 0 , 0, 0 }, // #54 [ref=2x]
  { InstDB::RWInfoRm::kCategoryFixed     , 0x04, 1 , 0, 0 }, // #55 [ref=1x]
  { InstDB::RWInfoRm::kCategoryFixed     , 0x04, 2 , 0, 0 }, // #56 [ref=1x]
  { InstDB::RWInfoRm::kCategoryQuarter   , 0x01, 0 , 0, 0 }, // #57 [ref=6x]
  { InstDB::RWInfoRm::kCategoryEighth    , 0x01, 0 , 0, 0 }, // #58 [ref=3x]
  { InstDB::RWInfoRm::kCategoryQuarter   , 0x02, 0 , 0, 0 }, // #59 [ref=4x]
  { InstDB::RWInfoRm::kCategoryEighth    , 0x02, 0 , 0, 0 }, // #60 [ref=2x]
  { InstDB::RWInfoRm::kCategoryFixed     , 0x0C, 16, 0, 0 }, // #61 [ref=1x]
  { InstDB::RWInfoRm::kCategoryFixed     , 0x06, 16, 0, 0 }, // #62 [ref=12x]
  { InstDB::RWInfoRm::kCategoryConsistent, 0x02, 0 , 0, Features::kAVX512_BW }  // #63 [ref=2x]
};
// ----------------------------------------------------------------------------
// ${InstRWInfoTable:End}

// ============================================================================
// [asmjit::x86::InstDB - Unit]
// ============================================================================

#if defined(ASMJIT_TEST)
UNIT(x86_inst_db) {
  INFO("Checking validity of Inst enums");

  // Cross-validate prefixes.
  EXPECT(Inst::kOptionRex  == 0x40000000u, "REX prefix must be at 0x40000000");
  EXPECT(Inst::kOptionVex3 == 0x00000400u, "VEX3 prefix must be at 0x00000400");
  EXPECT(Inst::kOptionEvex == 0x00001000u, "EVEX prefix must be at 0x00001000");

  // These could be combined together to form a valid REX prefix, they must match.
  EXPECT(uint32_t(Inst::kOptionOpCodeB) == uint32_t(Opcode::kB), "Opcode::kB must match Inst::kOptionOpCodeB");
  EXPECT(uint32_t(Inst::kOptionOpCodeX) == uint32_t(Opcode::kX), "Opcode::kX must match Inst::kOptionOpCodeX");
  EXPECT(uint32_t(Inst::kOptionOpCodeR) == uint32_t(Opcode::kR), "Opcode::kR must match Inst::kOptionOpCodeR");
  EXPECT(uint32_t(Inst::kOptionOpCodeW) == uint32_t(Opcode::kW), "Opcode::kW must match Inst::kOptionOpCodeW");

  uint32_t rex_rb = (Opcode::kR >> Opcode::kREX_Shift) | (Opcode::kB >> Opcode::kREX_Shift) | 0x40;
  uint32_t rex_rw = (Opcode::kR >> Opcode::kREX_Shift) | (Opcode::kW >> Opcode::kREX_Shift) | 0x40;

  EXPECT(rex_rb == 0x45, "Opcode::kR|B must form a valid REX prefix (0x45) if combined with 0x40");
  EXPECT(rex_rw == 0x4C, "Opcode::kR|W must form a valid REX prefix (0x4C) if combined with 0x40");
}
#endif

ASMJIT_END_SUB_NAMESPACE

#endif // !ASMJIT_NO_X86
