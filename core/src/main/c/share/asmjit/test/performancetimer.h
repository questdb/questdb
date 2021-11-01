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

#ifndef PERFORMANCETIMER_H_INCLUDED
#define PERFORMANCETIMER_H_INCLUDED

#include <asmjit/core.h>
#include <chrono>

class PerformanceTimer {
public:
  typedef std::chrono::high_resolution_clock::time_point TimePoint;

  TimePoint _startTime {};
  TimePoint _endTime {};

  inline void start() {
    _startTime = std::chrono::high_resolution_clock::now();
  }

  inline void stop() {
    _endTime = std::chrono::high_resolution_clock::now();
  }

  inline double duration() const {
    std::chrono::duration<double> elapsed = _endTime - _startTime;
    return elapsed.count() * 1000;
  }
};

static inline double mbps(double duration, uint64_t outputSize) noexcept {
  if (duration == 0)
    return 0.0;

  double bytesTotal = double(outputSize);
  return (bytesTotal * 1000) / (duration * 1024 * 1024);
}

#endif // PERFORMANCETIMER_H_INCLUDED
