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

#include <memory>
#include <vector>
#include <chrono>

#include "cmdline.h"
#include "performancetimer.h"

#include "asmjit_test_compiler.h"

#if !defined(ASMJIT_NO_X86) && ASMJIT_ARCH_X86
#include <asmjit/x86.h>
void compiler_add_x86_tests(TestApp& app);
#endif

#if defined(ASMJIT_BUILD_ARM) && ASMJIT_ARCH_ARM == 64
#include <asmjit/a64.h>
void compiler_add_a64_tests(TestApp& app);
#endif

#if !defined(ASMJIT_NO_X86) && ASMJIT_ARCH_X86
  #define ASMJIT_HAVE_WORKING_JIT
#endif

#if defined(ASMJIT_BUILD_ARM) && ASMJIT_ARCH_ARM == 64
  #define ASMJIT_HAVE_WORKING_JIT
#endif

using namespace asmjit;

// ============================================================================
// [TestApp]
// ============================================================================

static const char* archAsString(uint32_t arch) {
  switch (arch) {
    case Environment::kArchX86: return "X86";
    case Environment::kArchX64: return "X64";
    case Environment::kArchARM: return "A32";
    case Environment::kArchThumb: return "T32";
    case Environment::kArchAArch64: return "A64";
    default: return "Unknown";
  }
}

int TestApp::handleArgs(int argc, const char* const* argv) {
  CmdLine cmd(argc, argv);

  if (cmd.hasArg("--verbose")) _verbose = true;
  if (cmd.hasArg("--dump-asm")) _dumpAsm = true;
  if (cmd.hasArg("--dump-hex")) _dumpHex = true;

  return 0;
}

void TestApp::showInfo() {
  printf("AsmJit Compiler Test-Suite v%u.%u.%u (Arch=%s):\n",
    unsigned((ASMJIT_LIBRARY_VERSION >> 16)       ),
    unsigned((ASMJIT_LIBRARY_VERSION >>  8) & 0xFF),
    unsigned((ASMJIT_LIBRARY_VERSION      ) & 0xFF),
    archAsString(Environment::kArchHost));
  printf("  [%s] Verbose (use --verbose to turn verbose output ON)\n", _verbose ? "x" : " ");
  printf("  [%s] DumpAsm (use --dump-asm to turn assembler dumps ON)\n", _dumpAsm ? "x" : " ");
  printf("  [%s] DumpHex (use --dump-hex to dump binary in hexadecimal)\n", _dumpHex ? "x" : " ");
  printf("\n");
}

int TestApp::run() {
#ifndef ASMJIT_HAVE_WORKING_JIT
  return 0;
#else
#ifndef ASMJIT_NO_LOGGING
  uint32_t kFormatFlags = FormatOptions::kFlagMachineCode   |
                          FormatOptions::kFlagExplainImms   |
                          FormatOptions::kFlagRegCasts      |
                          FormatOptions::kFlagAnnotations   |
                          FormatOptions::kFlagDebugPasses   |
                          FormatOptions::kFlagDebugRA       ;

  FileLogger fileLogger(stdout);
  fileLogger.addFlags(kFormatFlags);

  StringLogger stringLogger;
  stringLogger.addFlags(kFormatFlags);
#endif

  double compileTime = 0;
  double finalizeTime = 0;

  for (std::unique_ptr<TestCase>& test : _tests) {
    JitRuntime runtime;
    CodeHolder code;
    SimpleErrorHandler errorHandler;

    PerformanceTimer perfTimer;

    code.init(runtime.environment());
    code.setErrorHandler(&errorHandler);

#ifndef ASMJIT_NO_LOGGING
    if (_verbose) {
      code.setLogger(&fileLogger);
    }
    else {
      stringLogger.clear();
      code.setLogger(&stringLogger);
    }
#endif

    printf("[Test] %s", test->name());

#ifndef ASMJIT_NO_LOGGING
    if (_verbose) printf("\n");
#endif

#if !defined(ASMJIT_NO_X86) && ASMJIT_ARCH_X86
    x86::Compiler cc(&code);
#endif

#if defined(ASMJIT_BUILD_ARM) && ASMJIT_ARCH_ARM == 64
    arm::Compiler cc(&code);
#endif

    perfTimer.start();
    test->compile(cc);
    perfTimer.stop();
    compileTime += perfTimer.duration();

    void* func = nullptr;
    Error err = errorHandler._err;

    if (!err) {
      perfTimer.start();
      err = cc.finalize();
      perfTimer.stop();
      finalizeTime += perfTimer.duration();
    }

#ifndef ASMJIT_NO_LOGGING
    if (_dumpAsm) {
      if (!_verbose) printf("\n");

      String sb;
      Formatter::formatNodeList(sb, kFormatFlags, &cc);
      printf("%s", sb.data());
    }
#endif

    if (err == kErrorOk)
      err = runtime.add(&func, &code);

    if (err == kErrorOk && _dumpHex) {
      String sb;
      sb.appendHex((void*)func, code.codeSize());
      printf("\n (HEX: %s)\n", sb.data());
    }

    if (_verbose)
      fflush(stdout);

    if (err == kErrorOk) {
      _outputSize += code.codeSize();

      StringTmp<128> result;
      StringTmp<128> expect;

      if (test->run(func, result, expect)) {
        if (!_verbose) printf(" [OK]\n");
      }
      else {
        if (!_verbose) printf(" [FAILED]\n");

#ifndef ASMJIT_NO_LOGGING
        if (!_verbose) printf("%s", stringLogger.data());
#endif

        printf("[Status]\n");
        printf("  Returned: %s\n", result.data());
        printf("  Expected: %s\n", expect.data());

        _nFailed++;
      }

      if (_dumpAsm)
        printf("\n");

      runtime.release(func);
    }
    else {
      if (!_verbose) printf(" [FAILED]\n");

#ifndef ASMJIT_NO_LOGGING
      if (!_verbose) printf("%s", stringLogger.data());
#endif

      printf("[Status]\n");
      printf("  ERROR 0x%08X: %s\n", unsigned(err), errorHandler._message.data());

      _nFailed++;
    }
  }

  printf("\n");
  printf("Summary:\n");
  printf("  OutputSize: %zu bytes\n", _outputSize);
  printf("  CompileTime: %.2f ms\n", compileTime);
  printf("  FinalizeTime: %.2f ms\n", finalizeTime);
  printf("\n");

  if (_nFailed == 0)
    printf("** SUCCESS: All %u tests passed **\n", unsigned(_tests.size()));
  else
    printf("** FAILURE: %u of %u tests failed **\n", _nFailed, unsigned(_tests.size()));

  return _nFailed == 0 ? 0 : 1;
#endif
}

// ============================================================================
// [Main]
// ============================================================================

int main(int argc, char* argv[]) {
  TestApp app;

  app.handleArgs(argc, argv);
  app.showInfo();

#if !defined(ASMJIT_NO_X86) && ASMJIT_ARCH_X86
  compiler_add_x86_tests(app);
#endif

#if defined(ASMJIT_BUILD_ARM) && ASMJIT_ARCH_ARM == 64
  compiler_add_a64_tests(app);
#endif

  return app.run();
}
