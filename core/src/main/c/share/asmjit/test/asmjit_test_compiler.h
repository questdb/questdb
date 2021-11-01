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

#ifndef ASMJIT_TEST_COMPILER_H_INCLUDED
#define ASMJIT_TEST_COMPILER_H_INCLUDED

#include <asmjit/core.h>

#include <memory>
#include <vector>

// ============================================================================
// [SimpleErrorHandler]
// ============================================================================

class SimpleErrorHandler : public asmjit::ErrorHandler {
public:
  SimpleErrorHandler()
    : _err(asmjit::kErrorOk) {}

  virtual void handleError(asmjit::Error err, const char* message, asmjit::BaseEmitter* origin) {
    asmjit::DebugUtils::unused(origin);
    _err = err;
    _message.assign(message);
  }

  asmjit::Error _err;
  asmjit::String _message;
};

// ============================================================================
// [TestCase]
// ============================================================================

//! A test case interface for testing AsmJit's Compiler.
class TestCase {
public:
  TestCase(const char* name = nullptr) {
    if (name)
      _name.assign(name);
  }

  virtual ~TestCase() {}

  inline const char* name() const { return _name.data(); }

  virtual void compile(asmjit::BaseCompiler& cc) = 0;
  virtual bool run(void* func, asmjit::String& result, asmjit::String& expect) = 0;

  asmjit::String _name;
};

// ============================================================================
// [TestApp]
// ============================================================================

class TestApp {
public:
  std::vector<std::unique_ptr<TestCase>> _tests;

  unsigned _nFailed = 0;
  size_t _outputSize = 0;

  bool _verbose = false;
  bool _dumpAsm = false;
  bool _dumpHex = false;

  TestApp() noexcept {}
  ~TestApp() noexcept {}

  void add(TestCase* test) noexcept {
    _tests.push_back(std::unique_ptr<TestCase>(test));
  }

  template<class T>
  inline void addT() { T::add(*this); }

  int handleArgs(int argc, const char* const* argv);
  void showInfo();
  int run();
};

#endif // ASMJIT_TEST_COMPILER_H_INCLUDED
