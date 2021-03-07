;*************************  debugbreak64.asm  **********************************
; Author:           Agner Fog
; Date created:     2011-07-09
; Last modified:    2011-07-09
; Source URL:       www.agner.org/optimize
; Project:          asmlib.zip
; Language:         assembly, NASM/YASM syntax, 32 bit
;
; C++ prototype:
; extern "C" void A_DebugBreak(void);
;
; Description:
; Makes a debug breakpoint. Works only when running under a debugger
;
;
; Copyright (c) 2011 GNU General Public License www.gnu.org/licenses
;******************************************************************************
;
; C++ prototype:
; extern "C" void A_DebugBreak(void);

global A_DebugBreak


SECTION .text

A_DebugBreak:
        int3
        nop
        ret
;A_DebugBreak ENDP
