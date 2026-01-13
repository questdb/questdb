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
#include "test_app.h"

// Altered source of asmjit_test_compiler.cpp
// from AsmJit - Machine code generation for C++

#include <asmjit/core.h>

#include <cstdio>
#include <cstring>
#include <cmath>

#include <memory>
#include <chrono>
#include <bitset>
#include <iostream>

#include "cmdline.h"
#include "performancetimer.h"

using namespace asmjit;

static const char *archAsString(Arch arch)
{
    switch (arch)
    {
    case Arch::kX86:
        return "X86";
    case Arch::kX64:
        return "X64";
    case Arch::kARM:
        return "A32";
    case Arch::kThumb:
        return "T32";
    case Arch::kAArch64:
        return "A64";
    default:
        return "Unknown";
    }
}

int TestApp::handleArgs(int argc, const char *const *argv)
{
    CmdLine cmd(argc, argv);

    if (cmd.has_arg("--verbose"))
        _verbose = true;
    if (cmd.has_arg("--dump-asm"))
        _dumpAsm = true;
    if (cmd.has_arg("--dump-hex"))
        _dumpHex = true;

    return 0;
}

void TestApp::showInfo()
{
    printf("AsmJit Compiler Test-Suite v%u.%u.%u (Arch=%s):\n",
           unsigned((ASMJIT_LIBRARY_VERSION >> 16)),
           unsigned((ASMJIT_LIBRARY_VERSION >> 8) & 0xFF),
           unsigned((ASMJIT_LIBRARY_VERSION) & 0xFF),
           archAsString(Environment::host().arch()));
    printf("  [%s] Verbose (use --verbose to turn verbose output ON)\n", _verbose ? "x" : " ");
    printf("  [%s] DumpAsm (use --dump-asm to turn assembler dumps ON)\n", _dumpAsm ? "x" : " ");
    printf("  [%s] DumpHex (use --dump-hex to dump binary in hexadecimal)\n", _dumpHex ? "x" : " ");
    printf("\n");
}

int TestApp::run()
{
    FormatFlags kFormatFlags = FormatFlags::kMachineCode |
                               FormatFlags::kExplainImms |
                               FormatFlags::kRegCasts |
                               FormatFlags::kPositions;

    FileLogger fileLogger(stdout);
    fileLogger.add_flags(kFormatFlags);

    StringLogger stringLogger;
    stringLogger.add_flags(kFormatFlags);

    double compileTime = 0;
    double finalizeTime = 0;

    for (std::unique_ptr<TestCase> &test : _tests)
    {
        JitRuntime runtime;
        CodeHolder code;
        SimpleErrorHandler errorHandler;

        PerformanceTimer perfTimer;

        code.init(runtime.environment());
        code.set_error_handler(&errorHandler);

        if (_verbose)
        {
            code.set_logger(&fileLogger);
        }
        else
        {
            stringLogger.clear();
            code.set_logger(&stringLogger);
        }

        printf("[Test] %s", test->name());

        if (_verbose)
            printf("\n");

        x86::Compiler cc(&code);

        perfTimer.start();
        test->compile(cc);
        perfTimer.stop();
        compileTime += perfTimer.duration();

        void *func = nullptr;
        Error err = errorHandler._err;

        if (err == Error::kOk)
        {
            perfTimer.start();
            err = cc.finalize();
            perfTimer.stop();
            finalizeTime += perfTimer.duration();
        }

        if (_dumpAsm)
        {
            if (!_verbose)
                printf("\n");

            String sb;
            FormatOptions formatOptions;
            formatOptions.set_flags(kFormatFlags);
            Formatter::format_node_list(sb, formatOptions, &cc);
            printf("%s", sb.data());
        }

        if (err == Error::kOk)
            err = runtime.add(&func, &code);

        if (err == Error::kOk && _dumpHex)
        {
            String sb;
            sb.append_hex((void *)func, code.code_size());
            printf("\n (HEX: %s)\n", sb.data());
        }

        if (_verbose)
            fflush(stdout);

        if (err == Error::kOk)
        {
            _outputSize += code.code_size();

            StringTmp<128> result;
            StringTmp<128> expect;

            if (test->run(func, result, expect))
            {
                if (!_verbose)
                    printf(" [OK]\n");
            }
            else
            {
                if (!_verbose)
                    printf(" [FAILED]\n");

                if (!_verbose)
                    printf("%s", stringLogger.data());

                printf("[Status]\n");
                printf("  Returned: %s\n", result.data());
                printf("  Expected: %s\n", expect.data());

                _nFailed++;
            }

            if (_dumpAsm)
                printf("\n");

            runtime.release(func);
        }
        else
        {
            if (!_verbose)
                printf(" [FAILED]\n");

            if (!_verbose)
                printf("%s", stringLogger.data());

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
}

////
class Test_Int32Not : public TestCase
{
public:
    Test_Int32Not() : TestCase("Int32Not") {}

    static void add(TestApp &app)
    {
        app.add(new Test_Int32Not());
    }

    void compile(BaseCompiler &c) override
    {
        auto &cc = dynamic_cast<x86::Compiler &>(c);
        auto* func = cc.add_func(FuncSignature::build<int32_t, int32_t>(CallConvId::kCDecl));

        x86::Gp a = cc.new_gp32("a");
        func->set_arg(0, a);

        x86::Gp r = questdb::x86::int32_not(cc, a);
        cc.ret(r);
        cc.end_func();
    }

    bool run(void *_func, String &result, String &expect) override
    {
        typedef int32_t (*Func)(int32_t);
        Func func = ptr_as_func<Func>(_func);

        int32_t resultRet = 0;
        int32_t expectRet = 0;

        resultRet = func(0);
        expectRet = ~0;

        result.assign_format("ret={%d}", resultRet);
        expect.assign_format("ret={%d}", expectRet);
        if (resultRet != expectRet)
            return false;

        resultRet = func(1);
        expectRet = ~1;

        result.assign_format("ret={%d}", resultRet);
        expect.assign_format("ret={%d}", expectRet);
        if (resultRet != expectRet)
            return false;

        resultRet = func(42);
        expectRet = ~42;

        result.assign_format("ret={%d}", resultRet);
        expect.assign_format("ret={%d}", expectRet);
        if (resultRet != expectRet)
            return false;

        return true;
    }
};

class Test_Int32And : public TestCase
{
public:
    Test_Int32And() : TestCase("Int32And") {}

    static void add(TestApp &app)
    {
        app.add(new Test_Int32And());
    }

    void compile(BaseCompiler &c) override
    {
        auto &cc = dynamic_cast<x86::Compiler &>(c);
        auto* func = cc.add_func(FuncSignature::build<int32_t, int32_t, int32_t>(CallConvId::kCDecl));

        x86::Gp a = cc.new_gp32("a");
        func->set_arg(0, a);
        x86::Gp b = cc.new_gp32("b");
        func->set_arg(1, b);

        x86::Gp r = questdb::x86::int32_and(cc, a, b);

        cc.ret(r);
        cc.end_func();
    }

    bool run(void *_func, String &result, String &expect) override
    {
        typedef int32_t (*Func)(int32_t, int32_t);
        Func func = ptr_as_func<Func>(_func);

        int32_t resultRet = 0;
        int32_t expectRet = 0;

        resultRet = func(0, 0);
        expectRet = 0 & 0;

        result.assign_format("ret={%d}", resultRet);
        expect.assign_format("ret={%d}", expectRet);
        if (resultRet != expectRet)
            return false;

        resultRet = func(0, 1);
        expectRet = 0 & 1;

        result.assign_format("ret={%d}", resultRet);
        expect.assign_format("ret={%d}", expectRet);
        if (resultRet != expectRet)
            return false;

        resultRet = func(1, 1);
        expectRet = 1 & 1;

        result.assign_format("ret={%d}", resultRet);
        expect.assign_format("ret={%d}", expectRet);
        if (resultRet != expectRet)
            return false;

        resultRet = func(42, 24);
        expectRet = 42 & 24;

        result.assign_format("ret={%d}", resultRet);
        expect.assign_format("ret={%d}", expectRet);
        if (resultRet != expectRet)
            return false;

        return true;
    }
};

class Test_Int32Or : public TestCase
{
public:
    Test_Int32Or() : TestCase("Int32Or") {}

    static void add(TestApp &app)
    {
        app.add(new Test_Int32Or());
    }

    void compile(BaseCompiler &c) override
    {
        auto &cc = dynamic_cast<x86::Compiler &>(c);
        auto* func = cc.add_func(FuncSignature::build<int32_t, int32_t, int32_t>(CallConvId::kCDecl));

        x86::Gp a = cc.new_gp32("a");
        func->set_arg(0, a);
        x86::Gp b = cc.new_gp32("b");
        func->set_arg(1, b);

        x86::Gp r = questdb::x86::int32_or(cc, a, b);
        cc.ret(r);
        cc.end_func();
    }

    bool run(void *_func, String &result, String &expect) override
    {
        typedef int32_t (*Func)(int32_t, int32_t);
        Func func = ptr_as_func<Func>(_func);

        int32_t resultRet = 0;
        int32_t expectRet = 0;

        resultRet = func(0, 0);
        expectRet = 0 | 0;

        result.assign_format("ret={%d}", resultRet);
        expect.assign_format("ret={%d}", expectRet);
        if (resultRet != expectRet)
            return false;

        resultRet = func(0, 1);
        expectRet = 0 | 1;

        result.assign_format("ret={%d}", resultRet);
        expect.assign_format("ret={%d}", expectRet);
        if (resultRet != expectRet)
            return false;

        resultRet = func(1, 1);
        expectRet = 1 | 1;

        result.assign_format("ret={%d}", resultRet);
        expect.assign_format("ret={%d}", expectRet);
        if (resultRet != expectRet)
            return false;

        resultRet = func(42, 24);
        expectRet = 42 | 24;

        result.assign_format("ret={%d}", resultRet);
        expect.assign_format("ret={%d}", expectRet);
        if (resultRet != expectRet)
            return false;

        return true;
    }
};

class Test_Int32toInt64 : public TestCase
{
public:
    Test_Int32toInt64() : TestCase("Int32toInt64") {}

    static void add(TestApp &app)
    {
        app.add(new Test_Int32toInt64());
    }

    void compile(BaseCompiler &c) override
    {
        auto &cc = dynamic_cast<x86::Compiler &>(c);
        auto* func = cc.add_func(FuncSignature::build<int64_t, int32_t>(CallConvId::kCDecl));

        x86::Gp a = cc.new_gp32("a");
        func->set_arg(0, a);

        x86::Gp r = questdb::x86::int32_to_int64(cc, a, true);
        cc.ret(r);
        cc.end_func();
    }

    bool run(void *_func, String &result, String &expect) override
    {
        typedef int64_t (*Func)(int32_t);
        Func func = ptr_as_func<Func>(_func);

        int64_t r1 = func(42);
        int64_t r2 = func(-42);
        int64_t r3 = func(0);
        int64_t r4 = func(std::numeric_limits<int32_t>::max());
        int64_t r5 = func(std::numeric_limits<int32_t>::min());

        int64_t e1 = 42;
        int64_t e2 = -42;
        int64_t e3 = 0;
        int64_t e4 = std::numeric_limits<int32_t>::max();
        int64_t e5 = std::numeric_limits<int64_t>::min();

        result.assign_format("ret={%d}, {%d}, {%d}, {%d}, {%lld}", r1, r2, r3, r4, r5);
        expect.assign_format("eet={%d}, {%d}, {%d}, {%d}, {%lld}", e1, e2, e3, e4, e5);

        return (r1 == e1) && (r2 == e2) && (r3 == e3) && (r4 == e4) && (r5 == e5);
    }
};

class Test_Int32toFloat : public TestCase
{
public:
    Test_Int32toFloat() : TestCase("Int32toFloat") {}

    static void add(TestApp &app)
    {
        app.add(new Test_Int32toFloat());
    }

    void compile(BaseCompiler &c) override
    {
        auto &cc = dynamic_cast<x86::Compiler &>(c);
        auto* func = cc.add_func(FuncSignature::build<float, int32_t>(CallConvId::kCDecl));

        x86::Gp a = cc.new_gp32("a");
        func->set_arg(0, a);

        x86::Vec r = questdb::x86::int32_to_float(cc, a, true);
        cc.ret(r);
        cc.end_func();
    }

    bool run(void *_func, String &result, String &expect) override
    {
        typedef float (*Func)(int32_t);
        Func func = ptr_as_func<Func>(_func);

        float r1 = func(42);
        float r2 = func(-42);
        float r3 = func(0);
        float r4 = func(std::numeric_limits<int32_t>::max());
        float r5 = func(std::numeric_limits<int32_t>::min());

        float e1 = 42;
        float e2 = -42;
        float e3 = 0;
        float e4 = (float)std::numeric_limits<int32_t>::max();
        float e5 = std::numeric_limits<float>::quiet_NaN();

        result.assign_format("ret={%g}, {%g}, {%g}, {%g}, {%g}", r1, r2, r3, r4, r5);
        expect.assign_format("eet={%g}, {%g}, {%g}, {%g}, {%g}", e1, e2, e3, e4, e5);

        return (r1 == e1) && (r2 == e2) && (r3 == e3) && (r4 == e4) && (std::isnan(r5) && std::isnan(e5));
    }
};

class Test_Int32toDouble : public TestCase
{
public:
    Test_Int32toDouble() : TestCase("Int32toDouble") {}

    static void add(TestApp &app)
    {
        app.add(new Test_Int32toDouble());
    }

    void compile(BaseCompiler &c) override
    {
        auto &cc = dynamic_cast<x86::Compiler &>(c);
        auto* func = cc.add_func(FuncSignature::build<double, int32_t>(CallConvId::kCDecl));

        x86::Gp a = cc.new_gp32("a");
        func->set_arg(0, a);

        x86::Vec r = questdb::x86::int32_to_double(cc, a, true);
        cc.ret(r);
        cc.end_func();
    }

    bool run(void *_func, String &result, String &expect) override
    {
        typedef double (*Func)(int32_t);
        Func func = ptr_as_func<Func>(_func);

        double r1 = func(42);
        double r2 = func(-42);
        double r3 = func(0);
        double r4 = func(std::numeric_limits<int32_t>::max());
        double r5 = func(std::numeric_limits<int32_t>::min());

        double e1 = 42;
        double e2 = -42;
        double e3 = 0;
        double e4 = (double)std::numeric_limits<int32_t>::max();
        double e5 = std::numeric_limits<double>::quiet_NaN();

        result.assign_format("ret={%g}, {%g}, {%g}, {%g}, {%g}", r1, r2, r3, r4, r5);
        expect.assign_format("eet={%g}, {%g}, {%g}, {%g}, {%g}", e1, e2, e3, e4, e5);

        return (r1 == e1) && (r2 == e2) && (r3 == e3) && (r4 == e4) && (std::isnan(r5) && std::isnan(e5));
    }
};

class Test_Int64toFloat : public TestCase
{
public:
    Test_Int64toFloat() : TestCase("Int64toFloat") {}

    static void add(TestApp &app)
    {
        app.add(new Test_Int64toFloat());
    }

    void compile(BaseCompiler &c) override
    {
        auto &cc = dynamic_cast<x86::Compiler &>(c);
        auto* func = cc.add_func(FuncSignature::build<float, int64_t>(CallConvId::kCDecl));

        x86::Gp a = cc.new_gp64("a");
        func->set_arg(0, a);

        x86::Vec r = questdb::x86::int64_to_float(cc, a, true);
        cc.ret(r);
        cc.end_func();
    }

    bool run(void *_func, String &result, String &expect) override
    {
        typedef float (*Func)(int64_t);
        Func func = ptr_as_func<Func>(_func);

        float r1 = func(42);
        float r2 = func(-42);
        float r3 = func(0);
        float r4 = func(std::numeric_limits<int64_t>::max());
        float r5 = func(std::numeric_limits<int64_t>::min());

        float e1 = 42;
        float e2 = -42;
        float e3 = 0;
        float e4 = (float)std::numeric_limits<int64_t>::max();
        double e5 = std::numeric_limits<double>::quiet_NaN();

        result.assign_format("ret={%g}, {%g}, {%g}, {%g}, {%g}", r1, r2, r3, r4, r5);
        expect.assign_format("eet={%g}, {%g}, {%g}, {%g}, {%g}", e1, e2, e3, e4, e5);

        return (r1 == e1) && (r2 == e2) && (r3 == e3) && (r4 == e4) && (std::isnan(r5) && std::isnan(e5));
    }
};

class Test_Int64toDouble : public TestCase
{
public:
    Test_Int64toDouble() : TestCase("Int64toDouble") {}

    static void add(TestApp &app)
    {
        app.add(new Test_Int64toDouble());
    }

    void compile(BaseCompiler &c) override
    {
        auto &cc = dynamic_cast<x86::Compiler &>(c);
        auto* func = cc.add_func(FuncSignature::build<double, int64_t>(CallConvId::kCDecl));

        x86::Gp a = cc.new_gp64("a");
        func->set_arg(0, a);

        x86::Vec r = questdb::x86::int64_to_double(cc, a, true);
        cc.ret(r);
        cc.end_func();
    }

    bool run(void *_func, String &result, String &expect) override
    {
        typedef double (*Func)(int64_t);
        Func func = ptr_as_func<Func>(_func);

        double r1 = func(42);
        double r2 = func(-42);
        double r3 = func(0);
        double r4 = func(std::numeric_limits<int64_t>::max());
        double r5 = func(std::numeric_limits<int64_t>::min());

        double e1 = 42;
        double e2 = -42;
        double e3 = 0;
        double e4 = std::numeric_limits<int64_t>::max();
        double e5 = std::numeric_limits<double>::quiet_NaN();

        result.assign_format("ret={%g}, {%g}, {%g}, {%lf}, {%g}", r1, r2, r3, r4, r5);
        expect.assign_format("eet={%g}, {%g}, {%g}, {%lf}, {%g}", e1, e2, e3, e4, e5);

        return (r1 == e1) && (r2 == e2) && (r3 == e3) && (r4 == e4) && (std::isnan(r5) && std::isnan(e5));
    }
};

class Test_FloatToDouble : public TestCase
{
public:
    Test_FloatToDouble() : TestCase("FloatToDouble") {}

    static void add(TestApp &app)
    {
        app.add(new Test_FloatToDouble());
    }

    void compile(BaseCompiler &c) override
    {
        auto &cc = dynamic_cast<x86::Compiler &>(c);
        auto* func = cc.add_func(FuncSignature::build<double, float>(CallConvId::kCDecl));

        x86::Vec a = cc.new_xmm_ss("a");
        func->set_arg(0, a);

        x86::Vec r = questdb::x86::float_to_double(cc, a);
        cc.ret(r);
        cc.end_func();
    }

    bool run(void *_func, String &result, String &expect) override
    {
        typedef double (*Func)(float);
        Func func = ptr_as_func<Func>(_func);

        double r1 = func(std::numeric_limits<float>::max());
        double r2 = func(std::numeric_limits<float>::min());
        double r3 = func(0);
        double r4 = func(std::numeric_limits<float>::quiet_NaN());

        double e1 = std::numeric_limits<float>::max();
        double e2 = std::numeric_limits<float>::min();
        double e3 = 0;
        double e4 = std::numeric_limits<float>::quiet_NaN();

        result.assign_format("ret={%g}, {%g}, {%g}, {%lf}, {%g}", r1, r2, r3, r4);
        expect.assign_format("eet={%g}, {%g}, {%g}, {%lf}, {%g}", e1, e2, e3, e4);

        return (r1 == e1) && (r2 == e2) && (r3 == e3) && (std::isnan(r4) && std::isnan(e4));
    }
};

class Test_Int64Neg : public TestCase
{
public:
    Test_Int64Neg() : TestCase("Int64Neg") {}

    static void add(TestApp &app)
    {
        app.add(new Test_Int64Neg());
    }

    void compile(BaseCompiler &c) override
    {
        auto &cc = dynamic_cast<x86::Compiler &>(c);
        auto* func = cc.add_func(FuncSignature::build<int64_t, int64_t>(CallConvId::kCDecl));

        x86::Gp a = cc.new_gp64("a");
        func->set_arg(0, a);

        x86::Gp r = questdb::x86::int64_neg(cc, a, true);
        cc.ret(r);
        cc.end_func();
    }

    bool run(void *_func, String &result, String &expect) override
    {
        typedef int64_t (*Func)(int64_t);
        Func func = ptr_as_func<Func>(_func);

        int64_t resultRet = 0;
        int64_t expectRet = 0;

        resultRet = func(0);
        expectRet = 0;

        result.assign_format("ret={%lld}", resultRet);
        expect.assign_format("ret={%lld}", expectRet);
        if (resultRet != expectRet)
            return false;

        resultRet = func(42);
        expectRet = -42;

        result.assign_format("ret={%lld}", resultRet);
        expect.assign_format("ret={%lld}", expectRet);
        if (resultRet != expectRet)
            return false;

        resultRet = func(LONG_NULL);
        expectRet = LONG_NULL;
        //        expectRet = -LONG_NULL;

        result.assign_format("ret={%lld}", resultRet);
        expect.assign_format("ret={%lld}", expectRet);
        if (resultRet != expectRet)
            return false;

        resultRet = func(std::numeric_limits<int64_t>::max());
        expectRet = -std::numeric_limits<int64_t>::max();

        result.assign_format("ret={%lld}", resultRet);
        expect.assign_format("ret={%lld}", expectRet);
        if (resultRet != expectRet)
            return false;

        return true;
    }
};

class Test_Int64Add : public TestCase
{
public:
    Test_Int64Add() : TestCase("Int64Add") {}

    static void add(TestApp &app)
    {
        app.add(new Test_Int64Add());
    }

    void compile(BaseCompiler &c) override
    {
        auto &cc = dynamic_cast<x86::Compiler &>(c);
        auto* func = cc.add_func(FuncSignature::build<int64_t, int64_t, int64_t>(CallConvId::kCDecl));

        x86::Gp a = cc.new_gp64("a");
        func->set_arg(0, a);
        x86::Gp b = cc.new_gp64("b");
        func->set_arg(1, b);

        x86::Gp r = questdb::x86::int64_add(cc, a, b, true);
        cc.ret(r);
        cc.end_func();
    }

    bool run(void *_func, String &result, String &expect) override
    {
        typedef int64_t (*Func)(int64_t, int64_t);
        Func func = ptr_as_func<Func>(_func);

        int64_t resultRet = 0;
        int64_t expectRet = 0;

        resultRet = func(42, -13);
        expectRet = 42 + -13;

        result.assign_format("ret={%lld}", resultRet);
        expect.assign_format("ret={%lld}", expectRet);
        if (resultRet != expectRet)
            return false;

        resultRet = func(42, LONG_NULL);
        expectRet = LONG_NULL;
        //        expectRet = 42 + LONG_NULL;

        result.assign_format("ret={%lld}", resultRet);
        expect.assign_format("ret={%lld}", expectRet);
        if (resultRet != expectRet)
            return false;

        resultRet = func(LONG_NULL, 42);
        expectRet = LONG_NULL;
        //        expectRet = LONG_NULL + 42;

        result.assign_format("ret={%lld}", resultRet);
        expect.assign_format("ret={%lld}", expectRet);
        if (resultRet != expectRet)
            return false;

        resultRet = func(std::numeric_limits<int64_t>::min(), std::numeric_limits<int64_t>::min());
        expectRet = LONG_NULL;
        //        expectRet = std::numeric_limits<int64_t>::min() + std::numeric_limits<int64_t>::min();

        result.assign_format("ret={%lld}", resultRet);
        expect.assign_format("ret={%lld}", expectRet);
        if (resultRet != expectRet)
            return false;

        resultRet = func(std::numeric_limits<int64_t>::min(), std::numeric_limits<int64_t>::max());
        expectRet = LONG_NULL;
        //        expectRet = std::numeric_limits<int64_t>::min() + std::numeric_limits<int64_t>::max();

        result.assign_format("ret={%lld}", resultRet);
        expect.assign_format("ret={%lld}", expectRet);
        if (resultRet != expectRet)
            return false;

        resultRet = func(std::numeric_limits<int64_t>::max(), std::numeric_limits<int64_t>::max());
        // overflow test
        expectRet = std::numeric_limits<int64_t>::max() + std::numeric_limits<int64_t>::max();

        result.assign_format("ret={%lld}", resultRet);
        expect.assign_format("ret={%lld}", expectRet);
        if (resultRet != expectRet)
            return false;

        return true;
    }
};

class Test_Int64Sub : public TestCase
{
public:
    Test_Int64Sub() : TestCase("Int64Sub") {}

    static void add(TestApp &app)
    {
        app.add(new Test_Int64Sub());
    }

    void compile(BaseCompiler &c) override
    {
        auto &cc = dynamic_cast<x86::Compiler &>(c);
        auto* func = cc.add_func(FuncSignature::build<int64_t, int64_t, int64_t>(CallConvId::kCDecl));

        x86::Gp a = cc.new_gp64("a");
        func->set_arg(0, a);
        x86::Gp b = cc.new_gp64("b");
        func->set_arg(1, b);

        x86::Gp r = questdb::x86::int64_sub(cc, a, b, true);
        cc.ret(r);
        cc.end_func();
    }

    bool run(void *_func, String &result, String &expect) override
    {
        typedef int64_t (*Func)(int64_t, int64_t);
        Func func = ptr_as_func<Func>(_func);

        int64_t resultRet = 0;
        int64_t expectRet = 0;

        resultRet = func(42, -13);
        expectRet = 42 - -13;

        result.assign_format("ret={%lld}", resultRet);
        expect.assign_format("ret={%lld}", expectRet);
        if (resultRet != expectRet)
            return false;

        resultRet = func(42, LONG_NULL);
        expectRet = LONG_NULL;
        //        expectRet = 42 - LONG_NULL;

        result.assign_format("ret={%lld}", resultRet);
        expect.assign_format("ret={%lld}", expectRet);
        if (resultRet != expectRet)
            return false;

        resultRet = func(LONG_NULL, 42);
        expectRet = LONG_NULL;
        //        expectRet = LONG_NULL - 42;

        result.assign_format("ret={%lld}", resultRet);
        expect.assign_format("ret={%lld}", expectRet);
        if (resultRet != expectRet)
            return false;

        resultRet = func(std::numeric_limits<int64_t>::min(), std::numeric_limits<int64_t>::min());
        expectRet = LONG_NULL;
        //        expectRet = std::numeric_limits<int64_t>::min() - std::numeric_limits<int64_t>::min();

        result.assign_format("ret={%lld}", resultRet);
        expect.assign_format("ret={%lld}", expectRet);
        if (resultRet != expectRet)
            return false;

        resultRet = func(std::numeric_limits<int64_t>::min(), std::numeric_limits<int64_t>::max());
        expectRet = LONG_NULL;
        //        expectRet = std::numeric_limits<int64_t>::min() - std::numeric_limits<int64_t>::max();

        result.assign_format("ret={%lld}", resultRet);
        expect.assign_format("ret={%lld}", expectRet);
        if (resultRet != expectRet)
            return false;

        resultRet = func(std::numeric_limits<int64_t>::max(), std::numeric_limits<int64_t>::max());
        expectRet = std::numeric_limits<int64_t>::max() - std::numeric_limits<int64_t>::max();

        result.assign_format("ret={%lld}", resultRet);
        expect.assign_format("ret={%lld}", expectRet);
        if (resultRet != expectRet)
            return false;

        return true;
    }
};

class Test_Int64Mul : public TestCase
{
public:
    Test_Int64Mul() : TestCase("Int64Mul") {}

    static void add(TestApp &app)
    {
        app.add(new Test_Int64Mul());
    }

    void compile(BaseCompiler &c) override
    {
        auto &cc = dynamic_cast<x86::Compiler &>(c);
        auto* func = cc.add_func(FuncSignature::build<int64_t, int64_t, int64_t>(CallConvId::kCDecl));

        x86::Gp a = cc.new_gp64("a");
        func->set_arg(0, a);
        x86::Gp b = cc.new_gp64("b");
        func->set_arg(1, b);

        x86::Gp r = questdb::x86::int64_mul(cc, a, b, true);
        cc.ret(r);
        cc.end_func();
    }

    bool run(void *_func, String &result, String &expect) override
    {
        typedef int64_t (*Func)(int64_t, int64_t);
        Func func = ptr_as_func<Func>(_func);

        int64_t resultRet = 0;
        int64_t expectRet = 0;

        resultRet = func(-3985256597569472057ll, 3);
        // overflow test
        expectRet = -3985256597569472057ll * 3;

        result.assign_format("ret={%lld}", resultRet);
        expect.assign_format("ret={%lld}", expectRet);
        if (resultRet != expectRet)
            return false;

        resultRet = func(42, -13);
        expectRet = 42 * -13;

        result.assign_format("ret={%lld}", resultRet);
        expect.assign_format("ret={%lld}", expectRet);
        if (resultRet != expectRet)
            return false;

        resultRet = func(42, LONG_NULL);
        expectRet = LONG_NULL;
        //        expectRet = 42 * LONG_NULL;

        result.assign_format("ret={%lld}", resultRet);
        expect.assign_format("ret={%lld}", expectRet);
        if (resultRet != expectRet)
            return false;

        resultRet = func(LONG_NULL, 42);
        expectRet = LONG_NULL;
        //        expectRet = LONG_NULL * 42;

        result.assign_format("ret={%lld}", resultRet);
        expect.assign_format("ret={%lld}", expectRet);
        if (resultRet != expectRet)
            return false;

        resultRet = func(std::numeric_limits<int64_t>::min(), std::numeric_limits<int64_t>::min());
        expectRet = LONG_NULL;
        //        expectRet = std::numeric_limits<int64_t>::min() * std::numeric_limits<int64_t>::min();

        result.assign_format("ret={%lld}", resultRet);
        expect.assign_format("ret={%lld}", expectRet);
        if (resultRet != expectRet)
            return false;

        resultRet = func(std::numeric_limits<int64_t>::min(), std::numeric_limits<int64_t>::max());
        expectRet = LONG_NULL;
        //        expectRet = std::numeric_limits<int64_t>::min() * std::numeric_limits<int64_t>::max();

        result.assign_format("ret={%lld}", resultRet);
        expect.assign_format("ret={%lld}", expectRet);
        if (resultRet != expectRet)
            return false;

        resultRet = func(std::numeric_limits<int64_t>::max(), std::numeric_limits<int64_t>::max());
        // overflow test
        expectRet = std::numeric_limits<int64_t>::max() * std::numeric_limits<int64_t>::max();

        result.assign_format("ret={%lld}", resultRet);
        expect.assign_format("ret={%lld}", expectRet);
        if (resultRet != expectRet)
            return false;

        return true;
    }
};

class Test_Int64Div : public TestCase
{
public:
    Test_Int64Div() : TestCase("Int64Div") {}

    static void add(TestApp &app)
    {
        app.add(new Test_Int64Div());
    }

    void compile(BaseCompiler &c) override
    {
        auto &cc = dynamic_cast<x86::Compiler &>(c);
        auto* func = cc.add_func(FuncSignature::build<int64_t, int64_t, int64_t>(CallConvId::kCDecl));

        x86::Gp a = cc.new_gp64("a");
        func->set_arg(0, a);
        x86::Gp b = cc.new_gp64("b");
        func->set_arg(1, b);

        x86::Gp r = questdb::x86::int64_div(cc, a, b, true);
        cc.ret(r);
        cc.end_func();
    }

    bool run(void *_func, String &result, String &expect) override
    {
        typedef int64_t (*Func)(int64_t, int64_t);
        Func func = ptr_as_func<Func>(_func);

        int64_t resultRet = 0;
        int64_t expectRet = 0;

        resultRet = func(42, -13);
        expectRet = 42 / -13;

        result.assign_format("ret={%lld}", resultRet);
        expect.assign_format("ret={%lld}", expectRet);
        if (resultRet != expectRet)
            return false;

        resultRet = func(42, LONG_NULL);
        expectRet = LONG_NULL;
        //        expectRet = 42 / LONG_NULL;

        result.assign_format("ret={%lld}", resultRet);
        expect.assign_format("ret={%lld}", expectRet);
        if (resultRet != expectRet)
            return false;

        resultRet = func(LONG_NULL, 42);
        expectRet = LONG_NULL;
        //        expectRet = LONG_NULL / 42;

        result.assign_format("ret={%lld}", resultRet);
        expect.assign_format("ret={%lld}", expectRet);
        if (resultRet != expectRet)
            return false;

        resultRet = func(std::numeric_limits<int64_t>::min(), std::numeric_limits<int64_t>::min());
        expectRet = LONG_NULL;
        //        expectRet = LONG_NULL / LONG_NULL;

        result.assign_format("ret={%lld}", resultRet);
        expect.assign_format("ret={%lld}", expectRet);
        if (resultRet != expectRet)
            return false;

        resultRet = func(std::numeric_limits<int64_t>::min(), std::numeric_limits<int64_t>::max());
        expectRet = LONG_NULL;
        //        expectRet = std::numeric_limits<int64_t>::min() / std::numeric_limits<int64_t>::max();

        result.assign_format("ret={%lld}", resultRet);
        expect.assign_format("ret={%lld}", expectRet);
        if (resultRet != expectRet)
            return false;

        resultRet = func(std::numeric_limits<int64_t>::max(), std::numeric_limits<int64_t>::max());
        expectRet = std::numeric_limits<int64_t>::max() / std::numeric_limits<int64_t>::max();

        result.assign_format("ret={%lld}", resultRet);
        expect.assign_format("ret={%lld}", expectRet);
        if (resultRet != expectRet)
            return false;

        return true;
    }
};

class Test_Int32Neg : public TestCase
{
public:
    Test_Int32Neg() : TestCase("Int32Neg") {}

    static void add(TestApp &app)
    {
        app.add(new Test_Int32Neg());
    }

    void compile(BaseCompiler &c) override
    {
        auto &cc = dynamic_cast<x86::Compiler &>(c);
        auto* func = cc.add_func(FuncSignature::build<int32_t, int32_t>(CallConvId::kCDecl));

        x86::Gp a = cc.new_gp32("a");
        func->set_arg(0, a);

        x86::Gp r = questdb::x86::int32_neg(cc, a, true);
        cc.ret(r);
        cc.end_func();
    }

    bool run(void *_func, String &result, String &expect) override
    {
        typedef int32_t (*Func)(int32_t);
        Func func = ptr_as_func<Func>(_func);

        int32_t resultRet = 0;
        int32_t expectRet = 0;

        resultRet = func(0);
        expectRet = 0;

        result.assign_format("ret={%d}", resultRet);
        expect.assign_format("ret={%d}", expectRet);
        if (resultRet != expectRet)
            return false;

        resultRet = func(42);
        expectRet = -42;

        result.assign_format("ret={%d}", resultRet);
        expect.assign_format("ret={%d}", expectRet);
        if (resultRet != expectRet)
            return false;

        resultRet = func(INT_NULL);
        expectRet = INT_NULL;
        //        expectRet = -INT_NULL;

        result.assign_format("ret={%d}", resultRet);
        expect.assign_format("ret={%d}", expectRet);
        if (resultRet != expectRet)
            return false;

        resultRet = func(std::numeric_limits<int32_t>::max());
        expectRet = -std::numeric_limits<int32_t>::max();

        result.assign_format("ret={%d}", resultRet);
        expect.assign_format("ret={%d}", expectRet);
        if (resultRet != expectRet)
            return false;

        return true;
    }
};

class Test_Int32Add : public TestCase
{
public:
    Test_Int32Add() : TestCase("Int32Add") {}

    static void add(TestApp &app)
    {
        app.add(new Test_Int32Add());
    }

    void compile(BaseCompiler &c) override
    {
        auto &cc = dynamic_cast<x86::Compiler &>(c);
        auto* func = cc.add_func(FuncSignature::build<int32_t, int32_t, int32_t>(CallConvId::kCDecl));

        x86::Gp a = cc.new_gp32("a");
        func->set_arg(0, a);
        x86::Gp b = cc.new_gp32("b");
        func->set_arg(1, b);

        x86::Gp r = questdb::x86::int32_add(cc, a, b, true);
        cc.ret(r);
        cc.end_func();
    }

    bool run(void *_func, String &result, String &expect) override
    {
        typedef int32_t (*Func)(int32_t, int32_t);
        Func func = ptr_as_func<Func>(_func);

        int32_t resultRet = 0;
        int32_t expectRet = 0;

        resultRet = func(42, -13);
        expectRet = 42 + -13;

        result.assign_format("ret={%d}", resultRet);
        expect.assign_format("ret={%d}", expectRet);
        if (resultRet != expectRet)
            return false;

        resultRet = func(42, INT_NULL);
        expectRet = INT_NULL;
        //                expectRet = 42 + INT_NULL;

        result.assign_format("ret={%d}", resultRet);
        expect.assign_format("ret={%d}", expectRet);
        if (resultRet != expectRet)
            return false;

        resultRet = func(INT_NULL, 42);
        expectRet = INT_NULL;
        //                expectRet = INT_NULL + 42;

        result.assign_format("ret={%d}", resultRet);
        expect.assign_format("ret={%d}", expectRet);
        if (resultRet != expectRet)
            return false;

        resultRet = func(std::numeric_limits<int32_t>::min(), std::numeric_limits<int32_t>::min());
        expectRet = INT_NULL;
        //                expectRet = std::numeric_limits<int32_t>::min() + std::numeric_limits<int32_t>::min();

        result.assign_format("ret={%d}", resultRet);
        expect.assign_format("ret={%d}", expectRet);
        if (resultRet != expectRet)
            return false;

        resultRet = func(std::numeric_limits<int32_t>::min(), std::numeric_limits<int32_t>::max());
        expectRet = INT_NULL;
        //                expectRet = std::numeric_limits<int32_t>::min() + std::numeric_limits<int32_t>::max();

        result.assign_format("ret={%d}", resultRet);
        expect.assign_format("ret={%d}", expectRet);
        if (resultRet != expectRet)
            return false;

        resultRet = func(std::numeric_limits<int32_t>::max(), std::numeric_limits<int32_t>::max());
        expectRet = std::numeric_limits<int32_t>::max() + std::numeric_limits<int32_t>::max();

        result.assign_format("ret={%d}", resultRet);
        expect.assign_format("ret={%d}", expectRet);
        if (resultRet != expectRet)
            return false;

        return true;
    }
};

class Test_Int32Sub : public TestCase
{
public:
    Test_Int32Sub() : TestCase("Int32Sub") {}

    static void add(TestApp &app)
    {
        app.add(new Test_Int32Sub());
    }

    void compile(BaseCompiler &c) override
    {
        auto &cc = dynamic_cast<x86::Compiler &>(c);
        auto* func = cc.add_func(FuncSignature::build<int32_t, int32_t, int32_t>(CallConvId::kCDecl));

        x86::Gp a = cc.new_gp32("a");
        func->set_arg(0, a);
        x86::Gp b = cc.new_gp32("b");
        func->set_arg(1, b);

        x86::Gp r = questdb::x86::int32_sub(cc, a, b, true);
        cc.ret(r);
        cc.end_func();
    }

    bool run(void *_func, String &result, String &expect) override
    {
        typedef int32_t (*Func)(int32_t, int32_t);
        Func func = ptr_as_func<Func>(_func);

        int32_t resultRet = 0;
        int32_t expectRet = 0;

        resultRet = func(42, -13);
        expectRet = 42 - -13;

        result.assign_format("ret={%d}", resultRet);
        expect.assign_format("ret={%d}", expectRet);
        if (resultRet != expectRet)
            return false;

        resultRet = func(42, INT_NULL);
        expectRet = INT_NULL;
        //                expectRet = 42 - INT_NULL;

        result.assign_format("ret={%d}", resultRet);
        expect.assign_format("ret={%d}", expectRet);
        if (resultRet != expectRet)
            return false;

        resultRet = func(INT_NULL, 42);
        expectRet = INT_NULL;
        //                expectRet = INT_NULL - 42;

        result.assign_format("ret={%d}", resultRet);
        expect.assign_format("ret={%d}", expectRet);
        if (resultRet != expectRet)
            return false;

        resultRet = func(std::numeric_limits<int32_t>::min(), std::numeric_limits<int32_t>::min());
        expectRet = INT_NULL;
        //                expectRet = std::numeric_limits<int32_t>::min() - std::numeric_limits<int32_t>::min();

        result.assign_format("ret={%d}", resultRet);
        expect.assign_format("ret={%d}", expectRet);
        if (resultRet != expectRet)
            return false;

        resultRet = func(std::numeric_limits<int32_t>::min(), std::numeric_limits<int32_t>::max());
        expectRet = INT_NULL;
        //                expectRet = std::numeric_limits<int32_t>::min() - std::numeric_limits<int32_t>::max();

        result.assign_format("ret={%d}", resultRet);
        expect.assign_format("ret={%d}", expectRet);
        if (resultRet != expectRet)
            return false;

        resultRet = func(std::numeric_limits<int32_t>::max(), std::numeric_limits<int32_t>::max());
        expectRet = std::numeric_limits<int32_t>::max() - std::numeric_limits<int32_t>::max();

        result.assign_format("ret={%d}", resultRet);
        expect.assign_format("ret={%d}", expectRet);
        if (resultRet != expectRet)
            return false;

        return true;
    }
};

class Test_Int32Mul : public TestCase
{
public:
    Test_Int32Mul() : TestCase("Int32Mul") {}

    static void add(TestApp &app)
    {
        app.add(new Test_Int32Mul());
    }

    void compile(BaseCompiler &c) override
    {
        auto &cc = dynamic_cast<x86::Compiler &>(c);
        auto* func = cc.add_func(FuncSignature::build<int32_t, int32_t, int32_t>(CallConvId::kCDecl));

        x86::Gp a = cc.new_gp32("a");
        func->set_arg(0, a);
        x86::Gp b = cc.new_gp32("b");
        func->set_arg(1, b);

        x86::Gp r = questdb::x86::int32_mul(cc, a, b, true);
        cc.ret(r);
        cc.end_func();
    }

    bool run(void *_func, String &result, String &expect) override
    {
        typedef int32_t (*Func)(int32_t, int32_t);
        Func func = ptr_as_func<Func>(_func);

        int32_t resultRet = 0;
        int32_t expectRet = 0;

        resultRet = func(-847531048, 3);
        expectRet = static_cast<int32_t>(-847531048 * 3ll);

        result.assign_format("ret={%d}", resultRet);
        expect.assign_format("ret={%d}", expectRet);
        if (resultRet != expectRet)
            return false;

        resultRet = func(42, -13);
        expectRet = 42 * -13;

        result.assign_format("ret={%d}", resultRet);
        expect.assign_format("ret={%d}", expectRet);
        if (resultRet != expectRet)
            return false;

        resultRet = func(42, INT_NULL);
        expectRet = INT_NULL;
        //                expectRet = 42 * INT_NULL;

        result.assign_format("ret={%d}", resultRet);
        expect.assign_format("ret={%d}", expectRet);
        if (resultRet != expectRet)
            return false;

        resultRet = func(INT_NULL, 42);
        expectRet = INT_NULL;
        //                expectRet = INT_NULL * 42;

        result.assign_format("ret={%d}", resultRet);
        expect.assign_format("ret={%d}", expectRet);
        if (resultRet != expectRet)
            return false;

        resultRet = func(std::numeric_limits<int32_t>::min(), std::numeric_limits<int32_t>::min());
        expectRet = INT_NULL;
        //                expectRet = std::numeric_limits<int32_t>::min() * std::numeric_limits<int32_t>::min();

        result.assign_format("ret={%d}", resultRet);
        expect.assign_format("ret={%d}", expectRet);
        if (resultRet != expectRet)
            return false;

        resultRet = func(std::numeric_limits<int32_t>::min(), std::numeric_limits<int32_t>::max());
        expectRet = INT_NULL;
        //                expectRet = std::numeric_limits<int32_t>::min() * std::numeric_limits<int32_t>::max();

        result.assign_format("ret={%d}", resultRet);
        expect.assign_format("ret={%d}", expectRet);
        if (resultRet != expectRet)
            return false;

        resultRet = func(std::numeric_limits<int32_t>::max(), std::numeric_limits<int32_t>::max());

        // overflow test
        expectRet = std::numeric_limits<int32_t>::max() * std::numeric_limits<int32_t>::max();

        result.assign_format("ret={%d}", resultRet);
        expect.assign_format("ret={%d}", expectRet);
        if (resultRet != expectRet)
            return false;

        return true;
    }
};

class Test_Int32Div : public TestCase
{
public:
    Test_Int32Div() : TestCase("Int32Div") {}

    static void add(TestApp &app)
    {
        app.add(new Test_Int32Div());
    }

    void compile(BaseCompiler &c) override
    {
        auto &cc = dynamic_cast<x86::Compiler &>(c);
        auto* func = cc.add_func(FuncSignature::build<int32_t, int32_t, int32_t>(CallConvId::kCDecl));

        x86::Gp a = cc.new_gp32("a");
        func->set_arg(0, a);
        x86::Gp b = cc.new_gp32("b");
        func->set_arg(1, b);

        x86::Gp r = questdb::x86::int32_div(cc, a, b, true);
        cc.ret(r);
        cc.end_func();
    }

    bool run(void *_func, String &result, String &expect) override
    {
        typedef int32_t (*Func)(int32_t, int32_t);
        Func func = ptr_as_func<Func>(_func);

        int32_t resultRet = 0;
        int32_t expectRet = 0;

        resultRet = func(42, -13);
        expectRet = 42 / -13;

        result.assign_format("ret={%d}", resultRet);
        expect.assign_format("ret={%d}", expectRet);
        if (resultRet != expectRet)
            return false;

        resultRet = func(42, INT_NULL);
        expectRet = INT_NULL;
        //                expectRet = 42 / INT_NULL;

        result.assign_format("ret={%d}", resultRet);
        expect.assign_format("ret={%d}", expectRet);
        if (resultRet != expectRet)
            return false;

        resultRet = func(INT_NULL, 42);
        expectRet = INT_NULL;
        //                expectRet = INT_NULL / 42;

        result.assign_format("ret={%d}", resultRet);
        expect.assign_format("ret={%d}", expectRet);
        if (resultRet != expectRet)
            return false;

        resultRet = func(std::numeric_limits<int32_t>::min(), std::numeric_limits<int32_t>::min());
        expectRet = INT_NULL;
        //                expectRet = INT_NULL / INT_NULL;

        result.assign_format("ret={%d}", resultRet);
        expect.assign_format("ret={%d}", expectRet);
        if (resultRet != expectRet)
            return false;

        resultRet = func(std::numeric_limits<int32_t>::min(), std::numeric_limits<int32_t>::max());
        expectRet = INT_NULL;
        //                expectRet = std::numeric_limits<int32_t>::min() / std::numeric_limits<int32_t>::max();

        result.assign_format("ret={%d}", resultRet);
        expect.assign_format("ret={%d}", expectRet);
        if (resultRet != expectRet)
            return false;

        resultRet = func(std::numeric_limits<int32_t>::max(), std::numeric_limits<int32_t>::max());
        expectRet = std::numeric_limits<int32_t>::max() / std::numeric_limits<int32_t>::max();

        result.assign_format("ret={%d}", resultRet);
        expect.assign_format("ret={%d}", expectRet);
        if (resultRet != expectRet)
            return false;

        return true;
    }
};

class Test_Int32EqNull : public TestCase
{
public:
    Test_Int32EqNull() : TestCase("Int32EqNull") {}

    static void add(TestApp &app)
    {
        app.add(new Test_Int32EqNull());
    }

    void compile(BaseCompiler &c) override
    {
        auto &cc = dynamic_cast<x86::Compiler &>(c);
        auto* func = cc.add_func(FuncSignature::build<int32_t, int32_t, int32_t>(CallConvId::kCDecl));

        x86::Gp a = cc.new_gp32("a");
        func->set_arg(0, a);
        x86::Gp b = cc.new_gp32("b");
        func->set_arg(1, b);

        x86::Gp r = questdb::x86::int32_eq(cc, a, b);
        cc.ret(r);
        cc.end_func();
    }

    bool run(void *_func, String &result, String &expect) override
    {
        typedef int32_t (*Func)(int32_t, int32_t);
        Func func = ptr_as_func<Func>(_func);

        int32_t resultRet = 0;
        int32_t expectRet = 0;

        resultRet = func(42, -13);
        expectRet = 0;

        result.assign_format("ret={%d}", resultRet);
        expect.assign_format("ret={%d}", expectRet);
        if (resultRet != expectRet)
            return false;

        resultRet = func(42, INT_NULL);
        expectRet = 0;

        result.assign_format("ret={%d}", resultRet);
        expect.assign_format("ret={%d}", expectRet);
        if (resultRet != expectRet)
            return false;

        resultRet = func(INT_NULL, 42);
        expectRet = 0;

        result.assign_format("ret={%d}", resultRet);
        expect.assign_format("ret={%d}", expectRet);
        if (resultRet != expectRet)
            return false;

        resultRet = func(INT_NULL, INT_NULL);
        expectRet = 1;

        result.assign_format("ret={%d}", resultRet);
        expect.assign_format("ret={%d}", expectRet);
        if (resultRet != expectRet)
            return false;

        return true;
    }
};

class Test_Float32CmpVec : public TestCase
{
public:
    Test_Float32CmpVec() : TestCase("Float32CmpVec") {}

    static void add(TestApp &app)
    {
        app.add(new Test_Float32CmpVec());
    }

    void compile(BaseCompiler &c) override
    {
        auto &cc = dynamic_cast<x86::Compiler &>(c);
        auto* func = cc.add_func(FuncSignature::build<void, float *, float *, int32_t *>(CallConvId::kCDecl));

        x86::Gp a_ptr = cc.new_gp64("a_ptr");
        func->set_arg(0, a_ptr);
        x86::Gp b_ptr = cc.new_gp64("b_ptr");
        func->set_arg(1, b_ptr);

        x86::Gp c_ptr = cc.new_gp64("c_ptr");
        func->set_arg(2, c_ptr);

        x86::Mem am = ymmword_ptr(a_ptr);
        x86::Mem bm = ymmword_ptr(b_ptr);
        x86::Mem cm = ymmword_ptr(c_ptr);

        x86::Vec adata = cc.new_ymm();
        x86::Vec bdata = cc.new_ymm();

        cc.vmovupd(adata, am);
        cc.vmovupd(bdata, bm);

        x86::Vec r = questdb::avx2::cmp_eq(cc, data_type_t::f32, adata, bdata);
        cc.vmovdqu(cm, r);
        cc.ret();

        cc.end_func();
    }

    bool run(void *_func, String &result, String &expect) override
    {
        typedef void (*Func)(float *, float *, int32_t *);
        Func func = ptr_as_func<Func>(_func);

        float a[8] = {22.0, 22.0, 22.0, 22.0, 22.0, 22.0, 22.0, 22.0};
        float b[8] = {22.0, 22.1, 22.01, 22.001, 22.0001, 22.00001, 22.00001, 22.0000000000001};

        int32_t c[8] = {0};
        int32_t e[8] = {-1, 0, 0, 0, 0, 0, 0, -1};

        func(reinterpret_cast<float *>(&a), reinterpret_cast<float *>(&b), reinterpret_cast<int32_t *>(&c));

        result.assign_format("ret=[{%d}, {%d}, {%d}, {%d}]", c[0], c[1], c[2], c[3]);
        expect.assign_format("ret=[{%d}, {%d}, {%d}, {%d}]", e[0], e[1], e[2], e[3]);

        for (int i = 0; i < 4; ++i)
        {
            if (c[i] != e[i])
                return false;
        }
        return true;
    }
};

class Test_Float64CmpVec : public TestCase
{
public:
    Test_Float64CmpVec() : TestCase("Float64CmpVec") {}

    static void add(TestApp &app)
    {
        app.add(new Test_Float64CmpVec());
    }

    void compile(BaseCompiler &c) override
    {
        auto &cc = dynamic_cast<x86::Compiler &>(c);
        auto* func = cc.add_func(FuncSignature::build<void, double *, double *, int64_t *>(CallConvId::kCDecl));

        x86::Gp a_ptr = cc.new_gp64("a_ptr");
        func->set_arg(0, a_ptr);
        x86::Gp b_ptr = cc.new_gp64("b_ptr");
        func->set_arg(1, b_ptr);

        x86::Gp c_ptr = cc.new_gp64("c_ptr");
        func->set_arg(2, c_ptr);

        x86::Mem am = ymmword_ptr(a_ptr);
        x86::Mem bm = ymmword_ptr(b_ptr);
        x86::Mem cm = ymmword_ptr(c_ptr);

        x86::Vec adata = cc.new_ymm();
        x86::Vec bdata = cc.new_ymm();

        cc.vmovupd(adata, am);
        cc.vmovupd(bdata, bm);

        x86::Vec r = questdb::avx2::cmp_eq(cc, data_type_t::f64, adata, bdata);
        cc.vmovdqu(cm, r);
        cc.ret();

        cc.end_func();
    }

    bool run(void *_func, String &result, String &expect) override
    {
        typedef void (*Func)(double *, double *, int64_t *);
        Func func = ptr_as_func<Func>(_func);

        double a[4] = {22.0, 22.0, 22.0, 22.0};
        double b[4] = {22.0, 22.1, 22.01, 22.0000000000001};

        int64_t c[4] = {0};
        int64_t e[4] = {-1, 0, 0, -1};

        func(reinterpret_cast<double *>(&a), reinterpret_cast<double *>(&b), reinterpret_cast<int64_t *>(&c));

        result.assign_format("ret=[{%lld}, {%lld}, {%lld}, {%lld}]", c[0], c[1], c[2], c[3]);
        expect.assign_format("ret=[{%lld}, {%lld}, {%lld}, {%lld}]", e[0], e[1], e[2], e[3]);

        for (int i = 0; i < 4; ++i)
        {
            if (c[i] != e[i])
                return false;
        }
        return true;
    }
};

class Test_Float64VecDiv : public TestCase
{
public:
    Test_Float64VecDiv() : TestCase("Float64VecDiv") {}

    static void add(TestApp &app)
    {
        app.add(new Test_Float64VecDiv());
    }

    void compile(BaseCompiler &c) override
    {
        auto &cc = dynamic_cast<x86::Compiler &>(c);
        auto* func = cc.add_func(FuncSignature::build<void, double *, double *, double *>(CallConvId::kCDecl));

        x86::Gp a_ptr = cc.new_gp64("a_ptr");
        func->set_arg(0, a_ptr);
        x86::Gp b_ptr = cc.new_gp64("b_ptr");
        func->set_arg(1, b_ptr);
        x86::Gp c_ptr = cc.new_gp64("c_ptr");
        func->set_arg(2, c_ptr);

        x86::Mem am = ymmword_ptr(a_ptr);
        x86::Mem bm = ymmword_ptr(b_ptr);
        x86::Mem cm = ymmword_ptr(c_ptr);

        x86::Vec adata = cc.new_ymm();
        x86::Vec bdata = cc.new_ymm();

        cc.vmovupd(adata, am);
        cc.vmovupd(bdata, bm);

        x86::Vec r = questdb::avx2::div(cc, data_type_t::f64, adata, bdata, false);
        cc.vmovupd(cm, r);

        cc.ret();
        cc.end_func();
    }

    bool run(void *_func, String &result, String &expect) override
    {
        typedef void (*Func)(double *, double *, double *);
        Func func = ptr_as_func<Func>(_func);

        double a[4] = {0.2845577791213847, 4.0, 6.0, 8.0};
        double b[4] = {0.2845577791213847, 2.0, 2.0, 0.0 / 0.0};
        double c[4] = {0.0, 0.0, 0.0, 0.0};
        double e[4] = {1.0, 2.0, 3.0, 2.0};

        func(reinterpret_cast<double *>(&a), reinterpret_cast<double *>(&b), reinterpret_cast<double *>(&c));

        result.assign_format("ret=[{%f}, {%f}, {%f}, {%f}]", c[0], c[1], c[2], c[3]);
        expect.assign_format("ret=[{%f}, {%f}, {%f}, {%f}]", e[0], e[1], e[2], e[3]);

        for (int i = 0; i < 4; ++i)
        {
            if (c[i] != e[i] && !std::isnan(c[i]))
                return false;
        }

        return true;
    }
};

class Test_CvtInt64ToFloat64 : public TestCase
{
public:
    Test_CvtInt64ToFloat64() : TestCase("CvtInt64ToFloat64") {}

    static void add(TestApp &app)
    {
        app.add(new Test_CvtInt64ToFloat64());
    }

    void compile(BaseCompiler &c) override
    {
        auto &cc = dynamic_cast<x86::Compiler &>(c);
        auto* func = cc.add_func(FuncSignature::build<void, int64_t *, double *>(CallConvId::kCDecl));

        x86::Gp a_ptr = cc.new_gp64("a_ptr");
        func->set_arg(0, a_ptr);
        x86::Gp b_ptr = cc.new_gp64("b_ptr");
        func->set_arg(1, b_ptr);

        x86::Mem am = ymmword_ptr(a_ptr);
        x86::Mem bm = ymmword_ptr(b_ptr);

        x86::Vec adata = cc.new_ymm();

        cc.vmovdqu(adata, am);
        x86::Vec r = questdb::avx2::cvt_ltod(cc, adata, true);
        cc.vmovupd(bm, r);

        cc.ret();
        cc.end_func();
    }

    bool run(void *_func, String &result, String &expect) override
    {
        typedef void (*Func)(int64_t *, double *);
        Func func = ptr_as_func<Func>(_func);

        int64_t a[4] = {5, 44, LONG_NULL, 88};
        double c[4] = {0.0};
        double e[4] = {5.0, 44.0, 0.0 / 0, 88.0};

        func(reinterpret_cast<int64_t *>(&a), reinterpret_cast<double *>(&c));

        result.assign_format("ret=[{%f}, {%f}, {%f}, {%f}]", c[0], c[1], c[2], c[3]);
        expect.assign_format("ret=[{%f}, {%f}, {%f}, {%f}]", e[0], e[1], e[2], e[3]);

        for (int i = 0; i < 4; ++i)
        {
            if (c[i] != e[i] && !std::isnan(c[i]))
                return false;
        }
        return true;
    }
};

class Test_VecInt64Add : public TestCase
{
public:
    Test_VecInt64Add() : TestCase("VecInt64Add") {}

    static void add(TestApp &app)
    {
        app.add(new Test_VecInt64Add());
    }

    void compile(BaseCompiler &c) override
    {
        auto &cc = dynamic_cast<x86::Compiler &>(c);
        auto* func = cc.add_func(FuncSignature::build<void, int64_t *, int64_t *>(CallConvId::kCDecl));

        x86::Gp a_ptr = cc.new_gp64("a_ptr");
        func->set_arg(0, a_ptr);
        x86::Gp b_ptr = cc.new_gp64("b_ptr");
        func->set_arg(1, b_ptr);

        x86::Mem am = ymmword_ptr(a_ptr);
        x86::Mem bm = ymmword_ptr(b_ptr);

        x86::Vec adata = cc.new_ymm();
        x86::Vec bdata = cc.new_ymm();

        cc.vmovdqu(adata, am);
        cc.vmovdqu(bdata, bm);
        x86::Vec r = questdb::avx2::add(cc, data_type_t::i64, adata, bdata, true);
        cc.vmovdqu(bm, r);

        cc.ret();
        cc.end_func();
    }

    bool run(void *_func, String &result, String &expect) override
    {
        typedef void (*Func)(int64_t *, int64_t *);
        Func func = ptr_as_func<Func>(_func);

        int64_t a[4] = {22, 44, LONG_NULL, 88};
        int64_t c[4] = {2, 11, LONG_NULL, LONG_NULL};
        int64_t e[4] = {24, 55, LONG_NULL, LONG_NULL};

        func(reinterpret_cast<int64_t *>(&a), reinterpret_cast<int64_t *>(&c));

        result.assign_format("ret=[{%lld}, {%lld}, {%lld}, {%lld}]", c[0], c[1], c[2], c[3]);
        expect.assign_format("ret=[{%lld}, {%lld}, {%lld}, {%lld}]", e[0], e[1], e[2], e[3]);

        for (int i = 0; i < 4; ++i)
        {
            if (c[i] != e[i])
                return false;
        }
        return true;
    }
};

class Test_VecInt64GeZero : public TestCase
{
public:
    Test_VecInt64GeZero() : TestCase("VecInt64GeZero") {}

    static void add(TestApp &app)
    {
        app.add(new Test_VecInt64GeZero());
    }

    void compile(BaseCompiler &c) override
    {
        auto &cc = dynamic_cast<x86::Compiler &>(c);
        auto* func = cc.add_func(FuncSignature::build<void, int64_t *, int64_t *>(CallConvId::kCDecl));

        x86::Gp a_ptr = cc.new_gp64("a_ptr");
        func->set_arg(0, a_ptr);
        x86::Gp b_ptr = cc.new_gp64("b_ptr");
        func->set_arg(1, b_ptr);

        x86::Mem am = ymmword_ptr(a_ptr);
        x86::Mem bm = ymmword_ptr(b_ptr);

        x86::Vec adata = cc.new_ymm();
        x86::Vec bdata = cc.new_ymm();

        cc.vmovdqu(adata, am);
        cc.vmovdqu(bdata, bm);
        x86::Vec r = questdb::avx2::cmp_ge(cc, data_type_t::i64, adata, bdata, true);
        cc.vmovdqu(bm, r);

        cc.ret();
        cc.end_func();
    }

    bool run(void *_func, String &result, String &expect) override
    {
        typedef void (*Func)(int64_t *, int64_t *);
        Func func = ptr_as_func<Func>(_func);

        int64_t a[4] = {-22, 44, LONG_NULL, 88};
        int64_t c[4] = {0, 0, 0, 0};
        int64_t e[4] = {0, -1, 0, -1};

        func(reinterpret_cast<int64_t *>(&a), reinterpret_cast<int64_t *>(&c));

        result.assign_format("ret=[{%lld}, {%lld}, {%lld}, {%lld}]", c[0], c[1], c[2], c[3]);
        expect.assign_format("ret=[{%lld}, {%lld}, {%lld}, {%lld}]", e[0], e[1], e[2], e[3]);

        for (int i = 0; i < 4; ++i)
        {
            if (c[i] != e[i])
                return false;
        }
        return true;
    }
};

class Test_VecInt64LeZero : public TestCase
{
public:
    Test_VecInt64LeZero() : TestCase("VecInt64LeZero") {}

    static void add(TestApp &app)
    {
        app.add(new Test_VecInt64LeZero());
    }

    void compile(BaseCompiler &c) override
    {
        auto &cc = dynamic_cast<x86::Compiler &>(c);
        auto* func = cc.add_func(FuncSignature::build<void, int64_t *, int64_t *>(CallConvId::kCDecl));

        x86::Gp a_ptr = cc.new_gp64("a_ptr");
        func->set_arg(0, a_ptr);
        x86::Gp b_ptr = cc.new_gp64("b_ptr");
        func->set_arg(1, b_ptr);

        x86::Mem am = ymmword_ptr(a_ptr);
        x86::Mem bm = ymmword_ptr(b_ptr);

        x86::Vec adata = cc.new_ymm();
        x86::Vec bdata = cc.new_ymm();

        cc.vmovdqu(adata, am);
        cc.vmovdqu(bdata, bm);
        x86::Vec r = questdb::avx2::cmp_le(cc, data_type_t::i64, adata, bdata, true);
        cc.vmovdqu(bm, r);

        cc.ret();
        cc.end_func();
    }

    bool run(void *_func, String &result, String &expect) override
    {
        typedef void (*Func)(int64_t *, int64_t *);
        Func func = ptr_as_func<Func>(_func);

        int64_t a[4] = {-22, 44, LONG_NULL, 0};
        int64_t c[4] = {0, 0, 0, 0};
        int64_t e[4] = {-1, 0, 0, -1};

        func(reinterpret_cast<int64_t *>(&a), reinterpret_cast<int64_t *>(&c));

        result.assign_format("ret=[{%lld}, {%lld}, {%lld}, {%lld}]", c[0], c[1], c[2], c[3]);
        expect.assign_format("ret=[{%lld}, {%lld}, {%lld}, {%lld}]", e[0], e[1], e[2], e[3]);

        for (int i = 0; i < 4; ++i)
        {
            if (c[i] != e[i])
                return false;
        }
        return true;
    }
};

class Test_Compress256 : public TestCase
{
public:
    Test_Compress256() : TestCase("Compress256") {}

    static void add(TestApp &app)
    {
        app.add(new Test_Compress256());
    }

    void compile(BaseCompiler &c) override
    {
        auto &cc = dynamic_cast<x86::Compiler &>(c);
        auto* func = cc.add_func(FuncSignature::build<int32_t, int64_t *, int64_t *>(CallConvId::kCDecl));

        x86::Gp a_ptr = cc.new_gp64("a_ptr");
        func->set_arg(0, a_ptr);
        x86::Gp b_ptr = cc.new_gp64("b_ptr");
        func->set_arg(1, b_ptr);

        x86::Mem am = ymmword_ptr(a_ptr);
        x86::Mem bm = ymmword_ptr(b_ptr);

        x86::Vec adata = cc.new_ymm();
        x86::Vec bdata = cc.new_ymm();

        cc.vmovdqu(adata, am);
        cc.vmovdqu(bdata, bm);

        x86::Vec r = questdb::avx2::cmp_eq(cc, data_type_t::i64, adata, bdata);
        adata = questdb::avx2::compress_register(cc, adata, r);

        cc.vmovdqu(bm, adata);
        x86::Gp mask2 = questdb::avx2::to_bits4(cc, r);
        cc.ret(mask2.r32());
        cc.end_func();
    }

    bool run(void *_func, String &result, String &expect) override
    {
        typedef int32_t (*Func)(int64_t *, int64_t *);
        Func func = ptr_as_func<Func>(_func);

        int64_t a[4] = {9, 12, 22, 37};
        int64_t c[4] = {-1, 12, 2, 37};

        int64_t e[4] = {12, 37, 0, 0};

        int32_t mask = func(reinterpret_cast<int64_t *>(&a), reinterpret_cast<int64_t *>(&c));

        result.assign_format("ret=[{%lld}, {%lld}, {%lld}, {%lld}]", c[0], c[1], c[2], c[3]);
        expect.assign_format("ret=[{%lld}, {%lld}, {%lld}, {%lld}]", e[0], e[1], e[2], e[3]);
        // i < 2 coz garbage in the rest positions
        for (int i = 0; i < 2; ++i)
        {
            if (c[i] != e[i])
                return false;
        }
        return true;
    }
};

class Test_Compress256Ints : public TestCase
{
public:
    Test_Compress256Ints() : TestCase("Compress256Ints") {}

    static void add(TestApp &app)
    {
        app.add(new Test_Compress256Ints());
    }

    void compile(BaseCompiler &c) override
    {
        auto &cc = dynamic_cast<x86::Compiler &>(c);
        auto* func = cc.add_func(FuncSignature::build<int32_t, int32_t *, int32_t *>(CallConvId::kCDecl));

        x86::Gp a_ptr = cc.new_gp64("a_ptr");
        func->set_arg(0, a_ptr);
        x86::Gp b_ptr = cc.new_gp64("b_ptr");
        func->set_arg(1, b_ptr);

        x86::Mem am = ymmword_ptr(a_ptr);
        x86::Mem bm = ymmword_ptr(b_ptr);

        x86::Vec adata = cc.new_ymm();
        x86::Vec bdata = cc.new_ymm();

        cc.vmovdqu(adata, am);
        cc.vmovdqu(bdata, bm);

        x86::Vec r = questdb::avx2::cmp_eq(cc, data_type_t::i32, adata, bdata);
        x86::Gp bits = questdb::avx2::to_bits8(cc, r);
        x86::Vec res = questdb::avx2::compress_register(cc, adata, r);

        cc.vmovdqu(bm, res);
        cc.ret(bits.r32());
        cc.end_func();
    }

    bool run(void *_func, String &result, String &expect) override
    {
        typedef int32_t (*Func)(int32_t *, int32_t *);
        Func func = ptr_as_func<Func>(_func);

        int32_t a[8] = {0, 1, 2, 3, 4, 5, 6, 7};
        int32_t c[8] = {0, -1, 2, 3, -4, 5, -6, 7};

        int32_t e[8] = {0, 2, 3, 5, 7, 0, 0, 0};

        int32_t mask = func(reinterpret_cast<int32_t *>(&a), reinterpret_cast<int32_t *>(&c));

        result.assign_format("ret=[{%d}, {%d}, {%d}, {%d}, {%d}, {%d}, {%d}, {%d}]", c[0], c[1], c[2], c[3], c[4], c[5], c[6], c[7]);
        expect.assign_format("ret=[{%d}, {%d}, {%d}, {%d}, {%d}, {%d}, {%d}, {%d}]", e[0], e[1], e[2], e[3], e[4], e[5], e[6], e[7]);

        for (int i = 0; i < 8; ++i)
        {
            if (c[i] != e[i])
                return false;
        }
        return true;
    }
};

void compiler_add_x86_tests(TestApp &app)
{
    app.addT<Test_Int32Not>();
    app.addT<Test_Int32And>();
    app.addT<Test_Int32Or>();
    app.addT<Test_Int32toInt64>();
    app.addT<Test_Int32toFloat>();
    app.addT<Test_Int32toDouble>();
    app.addT<Test_Int64toFloat>();
    app.addT<Test_Int64toDouble>();
    app.addT<Test_FloatToDouble>();
    app.addT<Test_Int64Neg>();
    app.addT<Test_Int64Add>();
    app.addT<Test_Int64Sub>();
    app.addT<Test_Int64Mul>();
    app.addT<Test_Int64Div>();
    app.addT<Test_Int32Neg>();
    app.addT<Test_Int32Add>();
    app.addT<Test_Int32Sub>();
    app.addT<Test_Int32Mul>();
    app.addT<Test_Int32Div>();
    app.addT<Test_Float32CmpVec>();
    app.addT<Test_Float64VecDiv>();
    app.addT<Test_CvtInt64ToFloat64>();
    app.addT<Test_VecInt64Add>();
    app.addT<Test_VecInt64GeZero>();
    app.addT<Test_VecInt64LeZero>();
    app.addT<Test_Float64CmpVec>();
    app.addT<Test_Int32EqNull>();
    app.addT<Test_Compress256>();
    app.addT<Test_Compress256Ints>();
}

int main(int argc, char *argv[])
{
    TestApp app;

    app.handleArgs(argc, argv);
    app.showInfo();

    compiler_add_x86_tests(app);

    return app.run();
}
