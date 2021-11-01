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

#ifndef ASMJIT_TEST_ASSEMBLER_H_INCLUDED
#define ASMJIT_TEST_ASSEMBLER_H_INCLUDED

#include <asmjit/core.h>
#include <stdio.h>

struct TestSettings {
  bool quiet;
};

template<typename AssemblerType>
class AssemblerTester {
public:
  asmjit::Environment env {};
  asmjit::CodeHolder code {};
  AssemblerType assembler {};
  const TestSettings& settings;

  size_t passed {};
  size_t count {};

  AssemblerTester(uint32_t arch, const TestSettings& settings) noexcept
    : env(arch),
      settings(settings) {
    prepare();
  }

  void printHeader(const char* archName) noexcept {
    printf("%s assembler tests:\n", archName);
  }

  void printSummary() noexcept {
    printf("  Passed: %zu / %zu tests\n\n", passed, count);
  }

  bool didPass() const noexcept { return passed == count; }

  void prepare() noexcept {
    code.reset();
    code.init(env, 0);
    code.attach(&assembler);
  }

  ASMJIT_NOINLINE bool testInstruction(const char* expectedOpcode, const char* s, uint32_t err) noexcept {
    count++;

    if (err) {
      printf("  !! %s\n"
             "    <%s>\n", s, asmjit::DebugUtils::errorAsString(err));
      prepare();
      return false;
    }

    asmjit::String encodedOpcode;
    asmjit::Section* text = code.textSection();

    encodedOpcode.appendHex(text->data(), text->bufferSize());
    if (encodedOpcode != expectedOpcode) {
      printf("  !! [%s] <- %s\n"
             "     [%s] (Expected)\n", encodedOpcode.data(), s, expectedOpcode);
      prepare();
      return false;
    }

    if (!settings.quiet)
      printf("  OK [%s] <- %s\n", encodedOpcode.data(), s);

    passed++;
    prepare();
    return true;
  }
};

#endif // ASMJIT_TEST_ASSEMBLER_H_INCLUDED
