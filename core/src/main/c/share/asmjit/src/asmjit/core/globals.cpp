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

#include "../core/api-build_p.h"
#include "../core/globals.h"
#include "../core/support.h"

ASMJIT_BEGIN_NAMESPACE

// ============================================================================
// [asmjit::DebugUtils]
// ============================================================================

ASMJIT_FAVOR_SIZE const char* DebugUtils::errorAsString(Error err) noexcept {
#ifndef ASMJIT_NO_TEXT
  // @EnumStringBegin{"enum": "ErrorCode", "output": "sError", "strip": "kError"}@
  static const char sErrorString[] =
    "Ok\0"
    "OutOfMemory\0"
    "InvalidArgument\0"
    "InvalidState\0"
    "InvalidArch\0"
    "NotInitialized\0"
    "AlreadyInitialized\0"
    "FeatureNotEnabled\0"
    "TooManyHandles\0"
    "TooLarge\0"
    "NoCodeGenerated\0"
    "InvalidDirective\0"
    "InvalidLabel\0"
    "TooManyLabels\0"
    "LabelAlreadyBound\0"
    "LabelAlreadyDefined\0"
    "LabelNameTooLong\0"
    "InvalidLabelName\0"
    "InvalidParentLabel\0"
    "NonLocalLabelCannotHaveParent\0"
    "InvalidSection\0"
    "TooManySections\0"
    "InvalidSectionName\0"
    "TooManyRelocations\0"
    "InvalidRelocEntry\0"
    "RelocOffsetOutOfRange\0"
    "InvalidAssignment\0"
    "InvalidInstruction\0"
    "InvalidRegType\0"
    "InvalidRegGroup\0"
    "InvalidPhysId\0"
    "InvalidVirtId\0"
    "InvalidElementIndex\0"
    "InvalidPrefixCombination\0"
    "InvalidLockPrefix\0"
    "InvalidXAcquirePrefix\0"
    "InvalidXReleasePrefix\0"
    "InvalidRepPrefix\0"
    "InvalidRexPrefix\0"
    "InvalidExtraReg\0"
    "InvalidKMaskUse\0"
    "InvalidKZeroUse\0"
    "InvalidBroadcast\0"
    "InvalidEROrSAE\0"
    "InvalidAddress\0"
    "InvalidAddressIndex\0"
    "InvalidAddressScale\0"
    "InvalidAddress64Bit\0"
    "InvalidAddress64BitZeroExtension\0"
    "InvalidDisplacement\0"
    "InvalidSegment\0"
    "InvalidImmediate\0"
    "InvalidOperandSize\0"
    "AmbiguousOperandSize\0"
    "OperandSizeMismatch\0"
    "InvalidOption\0"
    "OptionAlreadyDefined\0"
    "InvalidTypeId\0"
    "InvalidUseOfGpbHi\0"
    "InvalidUseOfGpq\0"
    "InvalidUseOfF80\0"
    "NotConsecutiveRegs\0"
    "IllegalVirtReg\0"
    "TooManyVirtRegs\0"
    "NoMorePhysRegs\0"
    "OverlappedRegs\0"
    "OverlappingStackRegWithRegArg\0"
    "ExpressionLabelNotBound\0"
    "ExpressionOverflow\0"
    "FailedToOpenAnonymousMemory\0"
    "<Unknown>\0";

  static const uint16_t sErrorIndex[] = {
    0, 3, 15, 31, 44, 56, 71, 90, 108, 123, 132, 148, 165, 178, 192, 210, 230,
    247, 264, 283, 313, 328, 344, 363, 382, 400, 422, 440, 459, 474, 490, 504,
    518, 538, 563, 581, 603, 625, 642, 659, 675, 691, 707, 724, 739, 754, 774,
    794, 814, 847, 867, 882, 899, 918, 939, 959, 973, 994, 1008, 1026, 1042,
    1058, 1077, 1092, 1108, 1123, 1138, 1168, 1192, 1211, 1239
  };
  // @EnumStringEnd@

  return sErrorString + sErrorIndex[Support::min<Error>(err, kErrorCount)];
#else
  DebugUtils::unused(err);
  static const char noMessage[] = "";
  return noMessage;
#endif
}

ASMJIT_FAVOR_SIZE void DebugUtils::debugOutput(const char* str) noexcept {
#if defined(_WIN32)
  ::OutputDebugStringA(str);
#else
  ::fputs(str, stderr);
#endif
}

ASMJIT_FAVOR_SIZE void DebugUtils::assertionFailed(const char* file, int line, const char* msg) noexcept {
  char str[1024];

  snprintf(str, 1024,
    "[asmjit] Assertion failed at %s (line %d):\n"
    "[asmjit] %s\n", file, line, msg);

  debugOutput(str);
  ::abort();
}

ASMJIT_END_NAMESPACE
