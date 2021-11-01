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

#include <asmjit/asmjit.h>
#include "./broken.h"

using namespace asmjit;

// ============================================================================
// [DumpCpu]
// ============================================================================

struct DumpCpuFeature {
  uint32_t feature;
  const char* name;
};

static const char* archToString(uint32_t arch) noexcept {
  switch (arch & ~Environment::kArchBigEndianMask) {
    case Environment::kArchX86      : return "X86";
    case Environment::kArchX64      : return "X64";
    case Environment::kArchARM      : return "ARM";
    case Environment::kArchThumb    : return "Thumb";
    case Environment::kArchAArch64  : return "AArch64";
    case Environment::kArchMIPS32_LE: return "MIPS";
    case Environment::kArchMIPS64_LE: return "MIPS64";
    default: return "Unknown";
  }
}

static void dumpCpu(void) noexcept {
  const CpuInfo& cpu = CpuInfo::host();

  // --------------------------------------------------------------------------
  // [CPU Information]
  // --------------------------------------------------------------------------

  INFO("Host CPU:");
  INFO("  Vendor                  : %s", cpu.vendor());
  INFO("  Brand                   : %s", cpu.brand());
  INFO("  Model ID                : %u", cpu.modelId());
  INFO("  Brand ID                : %u", cpu.brandId());
  INFO("  Family ID               : %u", cpu.familyId());
  INFO("  Stepping                : %u", cpu.stepping());
  INFO("  Processor Type          : %u", cpu.processorType());
  INFO("  Max logical Processors  : %u", cpu.maxLogicalProcessors());
  INFO("  Cache-Line Size         : %u", cpu.cacheLineSize());
  INFO("  HW-Thread Count         : %u", cpu.hwThreadCount());
  INFO("");

  // --------------------------------------------------------------------------
  // [CPU Features]
  // --------------------------------------------------------------------------

#ifndef ASMJIT_NO_LOGGING
  INFO("CPU Features:");
  BaseFeatures::Iterator it(cpu.features().iterator());
  while (it.hasNext()) {
    uint32_t featureId = uint32_t(it.next());
    StringTmp<64> featureString;
    Formatter::formatFeature(featureString, cpu.arch(), featureId);
    INFO("  %s\n", featureString.data());
  };
  INFO("");
#endif // !ASMJIT_NO_LOGGING
}

// ============================================================================
// [DumpSizeOf]
// ============================================================================

#define DUMP_TYPE(...) \
  INFO("  %-26s: %u", #__VA_ARGS__, uint32_t(sizeof(__VA_ARGS__)))

static void dumpSizeOf(void) noexcept {
  INFO("Size of C++ types:");
    DUMP_TYPE(int8_t);
    DUMP_TYPE(int16_t);
    DUMP_TYPE(int32_t);
    DUMP_TYPE(int64_t);
    DUMP_TYPE(int);
    DUMP_TYPE(long);
    DUMP_TYPE(size_t);
    DUMP_TYPE(intptr_t);
    DUMP_TYPE(float);
    DUMP_TYPE(double);
    DUMP_TYPE(void*);
  INFO("");

  INFO("Size of base classes:");
    DUMP_TYPE(BaseAssembler);
    DUMP_TYPE(BaseEmitter);
    DUMP_TYPE(CodeBuffer);
    DUMP_TYPE(CodeHolder);
    DUMP_TYPE(ConstPool);
    DUMP_TYPE(LabelEntry);
    DUMP_TYPE(RelocEntry);
    DUMP_TYPE(Section);
    DUMP_TYPE(String);
    DUMP_TYPE(Target);
    DUMP_TYPE(Zone);
    DUMP_TYPE(ZoneAllocator);
    DUMP_TYPE(ZoneBitVector);
    DUMP_TYPE(ZoneHashNode);
    DUMP_TYPE(ZoneHash<ZoneHashNode>);
    DUMP_TYPE(ZoneList<int>);
    DUMP_TYPE(ZoneVector<int>);
  INFO("");

  INFO("Size of operand classes:");
    DUMP_TYPE(Operand);
    DUMP_TYPE(BaseReg);
    DUMP_TYPE(BaseMem);
    DUMP_TYPE(Imm);
    DUMP_TYPE(Label);
  INFO("");

  INFO("Size of function classes:");
    DUMP_TYPE(CallConv);
    DUMP_TYPE(FuncFrame);
    DUMP_TYPE(FuncValue);
    DUMP_TYPE(FuncDetail);
    DUMP_TYPE(FuncSignature);
    DUMP_TYPE(FuncArgsAssignment);
  INFO("");

#if !defined(ASMJIT_NO_BUILDER)
  INFO("Size of builder classes:");
    DUMP_TYPE(BaseBuilder);
    DUMP_TYPE(BaseNode);
    DUMP_TYPE(InstNode);
    DUMP_TYPE(InstExNode);
    DUMP_TYPE(AlignNode);
    DUMP_TYPE(LabelNode);
    DUMP_TYPE(EmbedDataNode);
    DUMP_TYPE(EmbedLabelNode);
    DUMP_TYPE(ConstPoolNode);
    DUMP_TYPE(CommentNode);
    DUMP_TYPE(SentinelNode);
  INFO("");
#endif

#if !defined(ASMJIT_NO_COMPILER)
  INFO("Size of compiler classes:");
    DUMP_TYPE(BaseCompiler);
    DUMP_TYPE(FuncNode);
    DUMP_TYPE(FuncRetNode);
    DUMP_TYPE(InvokeNode);
  INFO("");
#endif

#if !defined(ASMJIT_NO_X86)
  INFO("Size of x86-specific classes:");
    DUMP_TYPE(x86::Assembler);
    #if !defined(ASMJIT_NO_BUILDER)
    DUMP_TYPE(x86::Builder);
    #endif
    #if !defined(ASMJIT_NO_COMPILER)
    DUMP_TYPE(x86::Compiler);
    #endif
    DUMP_TYPE(x86::InstDB::InstInfo);
    DUMP_TYPE(x86::InstDB::CommonInfo);
    DUMP_TYPE(x86::InstDB::OpSignature);
    DUMP_TYPE(x86::InstDB::InstSignature);
  INFO("");
#endif
}

#undef DUMP_TYPE

// ============================================================================
// [Main]
// ============================================================================

static void onBeforeRun(void) noexcept {
  dumpCpu();
  dumpSizeOf();
}

int main(int argc, const char* argv[]) {
#if defined(ASMJIT_BUILD_DEBUG)
  const char buildType[] = "Debug";
#else
  const char buildType[] = "Release";
#endif

  INFO("AsmJit Unit-Test v%u.%u.%u [Arch=%s] [Mode=%s]\n\n",
    unsigned((ASMJIT_LIBRARY_VERSION >> 16)       ),
    unsigned((ASMJIT_LIBRARY_VERSION >>  8) & 0xFF),
    unsigned((ASMJIT_LIBRARY_VERSION      ) & 0xFF),
    archToString(Environment::kArchHost),
    buildType
  );

  return BrokenAPI::run(argc, argv, onBeforeRun);
}
