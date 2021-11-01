// Broken - Lightweight unit testing for C++
//
// This is free and unencumbered software released into the public domain.
//
// Anyone is free to copy, modify, publish, use, compile, sell, or
// distribute this software, either in source code form or as a compiled
// binary, for any purpose, commercial or non-commercial, and by any
// means.
//
// In jurisdictions that recognize copyright laws, the author or authors
// of this software dedicate any and all copyright interest in the
// software to the public domain. We make this dedication for the benefit
// of the public at large and to the detriment of our heirs and
// successors. We intend this dedication to be an overt act of
// relinquishment in perpetuity of all present and future rights to this
// software under copyright law.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
// EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
// MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
// IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
// OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
// ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
// OTHER DEALINGS IN THE SOFTWARE.
//
// For more information, please refer to <http://unlicense.org>

#include "./broken.h"
#include <stdarg.h>

// ============================================================================
// [Broken - Global]
// ============================================================================

// Zero initialized globals.
struct BrokenGlobal {
  // Application arguments.
  int _argc;
  const char** _argv;

  // Output file.
  FILE* _file;

  // Unit tests.
  BrokenAPI::Unit* _unitList;
  BrokenAPI::Unit* _unitRunning;

  bool hasArg(const char* a) const noexcept {
    for (int i = 1; i < _argc; i++)
      if (strcmp(_argv[i], a) == 0)
        return true;
    return false;
  }

  inline FILE* file() const noexcept { return _file ? _file : stdout; }
};
static BrokenGlobal _brokenGlobal;

// ============================================================================
// [Broken - API]
// ============================================================================

// Get whether the string `a` starts with string `b`.
static bool BrokenAPI_startsWith(const char* a, const char* b) noexcept {
  for (size_t i = 0; ; i++) {
    if (b[i] == '\0') return true;
    if (a[i] != b[i]) return false;
  }
}

//! Compares names and priority of two unit tests.
static int BrokenAPI_compareUnits(const BrokenAPI::Unit* a, const BrokenAPI::Unit* b) noexcept {
  if (a->priority == b->priority)
    return strcmp(a->name, b->name);
  else
    return a->priority > b->priority ? 1 : -1;
}

// Get whether the strings `a` and `b` are equal, ignoring case and treating
// `-` as `_`.
static bool BrokenAPI_matchesFilter(const char* a, const char* b) noexcept {
  for (size_t i = 0; ; i++) {
    int ca = (unsigned char)a[i];
    int cb = (unsigned char)b[i];

    // If filter is defined as wildcard the rest automatically matches.
    if (cb == '*')
      return true;

    if (ca == '-') ca = '_';
    if (cb == '-') cb = '_';

    if (ca >= 'A' && ca <= 'Z') ca += 'a' - 'A';
    if (cb >= 'A' && cb <= 'Z') cb += 'a' - 'A';

    if (ca != cb)
      return false;

    if (ca == '\0')
      return true;
  }
}

static bool BrokenAPI_canRun(BrokenAPI::Unit* unit) noexcept {
  BrokenGlobal& global = _brokenGlobal;

  int i, argc = global._argc;
  const char** argv = global._argv;

  const char* unitName = unit->name;
  bool hasFilter = false;

  for (i = 1; i < argc; i++) {
    const char* arg = argv[i];

    if (BrokenAPI_startsWith(arg, "--run-") && strcmp(arg, "--run-all") != 0) {
      hasFilter = true;

      if (BrokenAPI_matchesFilter(unitName, arg + 6))
        return true;
    }
  }

  // If no filter has been specified the default is to run.
  return !hasFilter;
}

static void BrokenAPI_runUnit(BrokenAPI::Unit* unit) noexcept {
  BrokenAPI::info("Running %s", unit->name);

  _brokenGlobal._unitRunning = unit;
  unit->entry();
  _brokenGlobal._unitRunning = NULL;
}

static void BrokenAPI_runAll() noexcept {
  BrokenAPI::Unit* unit = _brokenGlobal._unitList;

  bool hasUnits = unit != NULL;
  size_t count = 0;
  int currentPriority = 0;

  while (unit != NULL) {
    if (BrokenAPI_canRun(unit)) {
      if (currentPriority != unit->priority) {
        if (count)
          INFO("");
        INFO("[[Priority=%d]]", unit->priority);
      }

      currentPriority = unit->priority;
      BrokenAPI_runUnit(unit);
      count++;
    }
    unit = unit->next;
  }

  if (count) {
    INFO("\nSuccess:");
    INFO("  All tests passed!");
  }
  else {
    INFO("\nWarning:");
    INFO("  No units %s!", hasUnits ? "matched the filter" : "defined");
  }
}

static void BrokenAPI_listAll() noexcept {
  BrokenAPI::Unit* unit = _brokenGlobal._unitList;

  if (unit != NULL) {
    INFO("Units:");
    do {
      INFO("  %s [priority=%d]", unit->name, unit->priority);
      unit = unit->next;
    } while (unit != NULL);
  }
  else {
    INFO("Warning:");
    INFO("  No units defined!");
  }
}

bool BrokenAPI::hasArg(const char* name) noexcept {
  return _brokenGlobal.hasArg(name);
}

void BrokenAPI::add(Unit* unit) noexcept {
  Unit** pPrev = &_brokenGlobal._unitList;
  Unit* current = *pPrev;

  // C++ static initialization doesn't guarantee anything. We sort all units by
  // name so the execution will always happen in deterministic order.
  while (current != NULL) {
    if (BrokenAPI_compareUnits(current, unit) >= 0)
      break;

    pPrev = &current->next;
    current = *pPrev;
  }

  *pPrev = unit;
  unit->next = current;
}

void BrokenAPI::setOutputFile(FILE* file) noexcept {
  BrokenGlobal& global = _brokenGlobal;

  global._file = file;
}

int BrokenAPI::run(int argc, const char* argv[], Entry onBeforeRun, Entry onAfterRun) {
  BrokenGlobal& global = _brokenGlobal;

  global._argc = argc;
  global._argv = argv;

  if (global.hasArg("--help")) {
    INFO("Options:");
    INFO("  --help    - print this usage");
    INFO("  --list    - list all tests");
    INFO("  --run-... - run a test(s), trailing wildcards supported");
    INFO("  --run-all - run all tests (default)");
    return 0;
  }

  if (global.hasArg("--list")) {
    BrokenAPI_listAll();
    return 0;
  }

  if (onBeforeRun)
    onBeforeRun();

  // We don't care about filters here, it's implemented by `runAll`.
  BrokenAPI_runAll();

  if (onAfterRun)
    onAfterRun();

  return 0;
}

static void BrokenAPI_printMessage(const char* prefix, const char* fmt, va_list ap) noexcept {
  BrokenGlobal& global = _brokenGlobal;
  FILE* dst = global.file();

  if (!fmt || fmt[0] == '\0') {
    fprintf(dst, "\n");
  }
  else {
    // This looks scary, but we really want to use only a single call to vfprintf()
    // in multithreaded code. So we change the format a bit if necessary.
    enum : unsigned { kBufferSize = 512 };
    char staticBuffer[512];

    size_t fmtSize = strlen(fmt);
    size_t prefixSize = strlen(prefix);

    char* fmtBuf = staticBuffer;
    if (fmtSize > kBufferSize - 2 - prefixSize)
      fmtBuf = static_cast<char*>(malloc(fmtSize + prefixSize + 2));

    if (!fmtBuf) {
      fprintf(dst, "%sCannot allocate buffer for vfprintf()\n", prefix);
    }
    else {
      memcpy(fmtBuf, prefix, prefixSize);
      memcpy(fmtBuf + prefixSize, fmt, fmtSize);

      fmtSize += prefixSize;
      if (fmtBuf[fmtSize - 1] != '\n')
        fmtBuf[fmtSize++] = '\n';
      fmtBuf[fmtSize] = '\0';

      vfprintf(dst, fmtBuf, ap);

      if (fmtBuf != staticBuffer)
        free(fmtBuf);
    }
  }

  fflush(dst);
}

void BrokenAPI::info(const char* fmt, ...) noexcept {
  BrokenGlobal& global = _brokenGlobal;

  va_list ap;
  va_start(ap, fmt);
  BrokenAPI_printMessage(global._unitRunning ? "  " : "", fmt, ap);
  va_end(ap);
}

void BrokenAPI::fail(const char* file, int line, const char* expression, const char* fmt, ...) noexcept {
  BrokenGlobal& global = _brokenGlobal;
  FILE* dst = global.file();

  fprintf(dst, "  FAILED: %s\n", expression);

  if (fmt) {
    va_list ap;
    va_start(ap, fmt);
    BrokenAPI_printMessage("  REASON: ", fmt, ap);
    va_end(ap);
  }

  fprintf(dst, "  SOURCE: %s (Line: %d)\n", file, line);
  fflush(dst);

  exit(1);
}
