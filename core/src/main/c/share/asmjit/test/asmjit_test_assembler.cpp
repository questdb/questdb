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

#include <asmjit/core.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "asmjit_test_assembler.h"
#include "cmdline.h"

using namespace asmjit;

#if !defined(ASMJIT_NO_X86)
bool testX86Assembler(const TestSettings& settings) noexcept;
bool testX64Assembler(const TestSettings& settings) noexcept;
#endif

int main(int argc, char* argv[]) {
  CmdLine cmdLine(argc, argv);

  TestSettings settings {};
  settings.quiet = cmdLine.hasArg("--quiet");

  printf("AsmJit Assembler Test-Suite v%u.%u.%u:\n\n",
    unsigned((ASMJIT_LIBRARY_VERSION >> 16)       ),
    unsigned((ASMJIT_LIBRARY_VERSION >>  8) & 0xFF),
    unsigned((ASMJIT_LIBRARY_VERSION      ) & 0xFF));

  printf("Usage:\n");
  printf("  --help        Show usage only\n");
  printf("  --arch=<ARCH> Select architecture to run ('all' by default)\n");
  printf("  --quiet       Show only assembling errors [%s]\n", settings.quiet ? "x" : " ");
  printf("\n");

  if (cmdLine.hasArg("--help"))
    return 0;

  const char* arch = cmdLine.valueOf("--arch", "all");
  bool x86Failed = false;
  bool x64Failed = false;

#if !defined(ASMJIT_NO_X86)
  if ((strcmp(arch, "all") == 0 || strcmp(arch, "x86") == 0))
    x86Failed = !testX86Assembler(settings);

  if ((strcmp(arch, "all") == 0 || strcmp(arch, "x64") == 0))
    x64Failed = !testX64Assembler(settings);
#endif

  bool failed = x86Failed || x64Failed;

  if (failed) {
    if (x86Failed) printf("** X86 test suite failed **\n");
    if (x64Failed) printf("** X64 test suite failed **\n");
    printf("** FAILURE **\n");
  }
  else {
    printf("** SUCCESS **\n");
  }

  return failed ? 1 : 0;
}
