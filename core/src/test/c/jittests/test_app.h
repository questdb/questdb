/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2026 QuestDB
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 ******************************************************************************/

#ifndef QUESTDB_TEST_APP_H
#define QUESTDB_TEST_APP_H

// Altered source of asmjit_test_compiler.h
// from AsmJit - Machine code generation for C++

#include <asmjit/core.h>
#include <asmjit/x86.h>
#include <memory>
#include <vector>

#include "src/main/c/share/jit/x86.h"
#include "src/main/c/share/jit/avx2.h"

class SimpleErrorHandler : public asmjit::ErrorHandler {
public:
    SimpleErrorHandler()
    : _err(asmjit::Error::kOk) {}

    void handle_error(asmjit::Error err, const char* message, asmjit::BaseEmitter* origin) override {
        (void)origin;
        _err = err;
        _message.assign(message);
    }

    asmjit::Error _err;
    asmjit::String _message;
};

//! A test case interface for testing AsmJit's Compiler.
class TestCase {
public:
    explicit TestCase(const char* name = nullptr) {
        if (name)
            _name.assign(name);
    }

    virtual ~TestCase() = default;

    inline const char* name() const { return _name.data(); }

    virtual void compile(asmjit::BaseCompiler& cc) = 0;
    virtual bool run(void* func, asmjit::String& result, asmjit::String& expect) = 0;

    asmjit::String _name;
};

class TestApp {
public:
    std::vector<std::unique_ptr<TestCase>> _tests;

    unsigned _nFailed = 0;
    size_t _outputSize = 0;

    bool _verbose = false;
    bool _dumpAsm = false;
    bool _dumpHex = false;

    TestApp() noexcept = default;
    ~TestApp() noexcept = default;

    void add(TestCase* test) noexcept {
        _tests.push_back(std::unique_ptr<TestCase>(test));
    }

    template<class T>
    inline void addT() { T::add(*this); }

    int handleArgs(int argc, const char* const* argv);
    void showInfo();
    int run();
};

#endif //QUESTDB_TEST_APP_H
