;          ROUND32.ASM

; Author:           Agner Fog
; Date created:     2003
; Last modified:    2008-10-16
; Description:
; Round function

; Copyright (c) 2009 GNU General Public License www.gnu.org/licenses
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

global _RoundD
global _RoundF

SECTION .text  align=16

; ********** round function **********
; C++ prototype:
; extern "C" int RoundD (double x);
; extern "C" int RoundF (float  x);

; This function converts a single or double precision floating point number 
; to an integer, rounding to nearest or even. Does not check for overflow.
; This function is much faster than the default conversion method in C++
; which uses truncation.

_RoundD:
        fld     qword [esp+4]          ; Load x
        push    eax                    ; Make temporary space on stack
        fistp   dword [esp]            ; Round. Store in temporary stack space
        pop     eax                    ; Read from temporary stack space
        ret
;_RoundD  ENDP

_RoundF:
        fld     dword [esp+4]
        push    eax
        fistp   dword [esp]
        pop     eax
        ret
;_RoundF ENDP
