;          ROUND64.ASM 

; Author:           Agner Fog
; Date created:     2007-06-15
; Last modified:    2008-10-16
; Description:
; Round function

; Copyright (c) 2009 GNU General Public License www.gnu.org/licenses
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

default rel

global RoundD
global RoundF


SECTION .text  align=16

; ********** round function **********
; C++ prototype:
; extern "C" int RoundD (double x);
; extern "C" int RoundF (float  x);

; This function converts a single or double precision floating point number 
; to an integer, rounding to nearest or even. Does not check for overflow.
; This function is much faster than the default conversion method in C++
; which uses truncation.

RoundD:
        cvtsd2si eax, xmm0             ; Round xmm0 to eax
        ret
;RoundD  ENDP

RoundF:
        cvtss2si eax, xmm0             ; Round xmm0 to eax
        ret
;RoundF ENDP
