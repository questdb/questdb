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

#ifndef ASMJIT_TEST_PERF_H_INCLUDED
#define ASMJIT_TEST_PERF_H_INCLUDED

#include <asmjit/core.h>
#include "performancetimer.h"

class MyErrorHandler : public asmjit::ErrorHandler {
  void handleError(asmjit::Error err, const char* message, asmjit::BaseEmitter* origin) {
    (void)err;
    (void)origin;
    printf("ERROR: %s\n", message);
    abort();
  }
};

template<typename EmitterT, typename FuncT>
static void bench(asmjit::CodeHolder& code, uint32_t arch, uint32_t numIterations, const char* testName, const FuncT& func) noexcept {
  EmitterT emitter;
  MyErrorHandler eh;

  const char* archName =
    arch == asmjit::Environment::kArchX86 ? "X86" :
    arch == asmjit::Environment::kArchX64 ? "X64" : "???";

  const char* emitterName =
    emitter.isAssembler() ? "Assembler" :
    emitter.isCompiler()  ? "Compiler"  :
    emitter.isBuilder()   ? "Builder"   : "Unknown";

  uint64_t codeSize = 0;
  asmjit::Environment env(arch);

  PerformanceTimer timer;
  double duration = std::numeric_limits<double>::infinity();

  for (uint32_t r = 0; r < numIterations; r++) {
    codeSize = 0;
    code.init(env);
    code.setErrorHandler(&eh);
    code.attach(&emitter);

    timer.start();
    func(emitter);
    timer.stop();

    codeSize += code.codeSize();

    code.reset();
    duration = asmjit::Support::min(duration, timer.duration());
  }

  printf("  [%s] %-9s %-16s | CodeSize:%5llu [B] | Time:%8.4f [ms]", archName, emitterName, testName, (unsigned long long)codeSize, duration);
  if (codeSize)
    printf(" | Speed:%8.3f [MB/s]", mbps(duration, codeSize));
  printf("\n");
}

#endif // ASMJIT_TEST_PERF_H_INCLUDED
