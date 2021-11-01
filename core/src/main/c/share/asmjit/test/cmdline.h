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

#ifndef CMDLINE_H_INCLUDED
#define CMDLINE_H_INCLUDED

#include <stdint.h>
#include <stdlib.h>
#include <string.h>

// ============================================================================
// [CmdLine]
// ============================================================================

class CmdLine {
public:
  int _argc;
  const char* const* _argv;

  CmdLine(int argc, const char* const* argv)
    : _argc(argc),
      _argv(argv) {}

  bool hasArg(const char* key) const {
    for (int i = 1; i < _argc; i++)
      if (strcmp(key, _argv[i]) == 0)
        return true;
    return false;
  }

  const char* valueOf(const char* key, const char* defaultValue) const {
    size_t keySize = strlen(key);
    for (int i = 1; i < _argc; i++) {
      const char* val = _argv[i];
      if (strlen(val) >= keySize + 1 && val[keySize] == '=' && memcmp(val, key, keySize) == 0)
        return val + keySize + 1;
    }

    return defaultValue;
  }

  int valueAsInt(const char* key, int defaultValue) const {
    const char* val = valueOf(key, nullptr);
    if (val == nullptr || val[0] == '\0')
      return defaultValue;

    return atoi(val);
  }

  unsigned valueAsUInt(const char* key, unsigned defaultValue) const {
    const char* val = valueOf(key, nullptr);
    if (val == nullptr || val[0] == '\0')
      return defaultValue;

    int v = atoi(val);
    if (v < 0)
      return defaultValue;
    else
      return unsigned(v);
  }
};

#endif // CMDLINE_H_INCLUDED
