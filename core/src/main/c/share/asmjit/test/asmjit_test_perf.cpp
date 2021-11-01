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

#include "cmdline.h"

using namespace asmjit;

#if !defined(ASMJIT_NO_X86)
void benchmarkX86Emitters(uint32_t numIterations, bool testX86, bool testX64) noexcept;
#endif

int main(int argc, char* argv[]) {
  CmdLine cmdLine(argc, argv);
  uint32_t numIterations = 20000;

  printf("AsmJit Performance Suite v%u.%u.%u:\n\n",
    unsigned((ASMJIT_LIBRARY_VERSION >> 16)       ),
    unsigned((ASMJIT_LIBRARY_VERSION >>  8) & 0xFF),
    unsigned((ASMJIT_LIBRARY_VERSION      ) & 0xFF));

  printf("Usage:\n");
  printf("  --help        Show usage only\n");
  printf("  --quick       Decrease the number of iterations to make tests quicker\n");
  printf("  --arch=<ARCH> Select architecture to run ('all' by default)\n");
  printf("\n");

  if (cmdLine.hasArg("--help"))
    return 0;

  if (cmdLine.hasArg("--quick"))
    numIterations = 1000;

  const char* arch = cmdLine.valueOf("--arch", "all");

#if !defined(ASMJIT_NO_X86)
  bool testX86 = strcmp(arch, "all") == 0 || strcmp(arch, "x86") == 0;
  bool testX64 = strcmp(arch, "all") == 0 || strcmp(arch, "x64") == 0;

  if (testX86 || testX64)
    benchmarkX86Emitters(numIterations, testX86, testX64);
#endif

  return 0;
}
