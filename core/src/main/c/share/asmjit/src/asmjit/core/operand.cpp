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

#include "../core/api-build_p.h"
#include "../core/operand.h"

ASMJIT_BEGIN_NAMESPACE

// ============================================================================
// [asmjit::Operand - Unit]
// ============================================================================

#if defined(ASMJIT_TEST)
UNIT(operand) {
  INFO("Checking operand sizes");
  EXPECT(sizeof(Operand) == 16);
  EXPECT(sizeof(BaseReg) == 16);
  EXPECT(sizeof(BaseMem) == 16);
  EXPECT(sizeof(Imm)     == 16);
  EXPECT(sizeof(Label)   == 16);

  INFO("Checking basic functionality of Operand");
  Operand a, b;
  Operand dummy;

  EXPECT(a.isNone() == true);
  EXPECT(a.isReg() == false);
  EXPECT(a.isMem() == false);
  EXPECT(a.isImm() == false);
  EXPECT(a.isLabel() == false);
  EXPECT(a == b);
  EXPECT(a._data[0] == 0);
  EXPECT(a._data[1] == 0);

  INFO("Checking basic functionality of Label");
  Label label;
  EXPECT(label.isValid() == false);
  EXPECT(label.id() == Globals::kInvalidId);

  INFO("Checking basic functionality of BaseReg");
  EXPECT(BaseReg().isReg() == true);
  EXPECT(BaseReg().isValid() == false);
  EXPECT(BaseReg()._data[0] == 0);
  EXPECT(BaseReg()._data[1] == 0);
  EXPECT(dummy.as<BaseReg>().isValid() == false);

  // Create some register (not specific to any architecture).
  uint32_t rSig = Operand::kOpReg | (1 << Operand::kSignatureRegTypeShift ) |
                                    (2 << Operand::kSignatureRegGroupShift) |
                                    (8 << Operand::kSignatureSizeShift    ) ;
  BaseReg r1 = BaseReg::fromSignatureAndId(rSig, 5);

  EXPECT(r1.isValid()   == true);
  EXPECT(r1.isReg()     == true);
  EXPECT(r1.isReg(1)    == true);
  EXPECT(r1.isPhysReg() == true);
  EXPECT(r1.isVirtReg() == false);
  EXPECT(r1.signature() == rSig);
  EXPECT(r1.type()      == 1);
  EXPECT(r1.group()     == 2);
  EXPECT(r1.size()      == 8);
  EXPECT(r1.id()        == 5);
  EXPECT(r1.isReg(1, 5) == true); // RegType and Id.
  EXPECT(r1._data[0]    == 0);
  EXPECT(r1._data[1]    == 0);

  // The same type of register having different id.
  BaseReg r2(r1, 6);
  EXPECT(r2.isValid()   == true);
  EXPECT(r2.isReg()     == true);
  EXPECT(r2.isReg(1)    == true);
  EXPECT(r2.isPhysReg() == true);
  EXPECT(r2.isVirtReg() == false);
  EXPECT(r2.signature() == rSig);
  EXPECT(r2.type()      == r1.type());
  EXPECT(r2.group()     == r1.group());
  EXPECT(r2.size()      == r1.size());
  EXPECT(r2.id()        == 6);
  EXPECT(r2.isReg(1, 6) == true);

  r1.reset();
  EXPECT(!r1.isReg());
  EXPECT(!r1.isValid());

  INFO("Checking basic functionality of BaseMem");
  BaseMem m;
  EXPECT(m.isMem());
  EXPECT(m == BaseMem());
  EXPECT(m.hasBase() == false);
  EXPECT(m.hasIndex() == false);
  EXPECT(m.hasOffset() == false);
  EXPECT(m.isOffset64Bit() == true);
  EXPECT(m.offset() == 0);

  m.setOffset(-1);
  EXPECT(m.offsetLo32() == -1);
  EXPECT(m.offset() == -1);

  int64_t x = int64_t(0xFF00FF0000000001u);
  int32_t xHi = int32_t(0xFF00FF00u);

  m.setOffset(x);
  EXPECT(m.offset() == x);
  EXPECT(m.offsetLo32() == 1);
  EXPECT(m.offsetHi32() == xHi);

  INFO("Checking basic functionality of Imm");
  Imm immValue(-42);
  EXPECT(immValue.type() == Imm::kTypeInteger);
  EXPECT(Imm(-1).value() == -1);
  EXPECT(imm(-1).value() == -1);
  EXPECT(immValue.value() == -42);
  EXPECT(imm(0xFFFFFFFF).value() == int64_t(0xFFFFFFFF));

  Imm immDouble(0.4);
  EXPECT(immDouble.type() == Imm::kTypeDouble);
  EXPECT(immDouble.valueAs<double>() == 0.4);
  EXPECT(immDouble == imm(0.4));

}
#endif

ASMJIT_END_NAMESPACE
