;*************************  cpuid32.asm  *********************************
; Author:           Agner Fog
; Date created:     2008-12-14
; Last modified:    2011-07-01
; Source URL:       www.agner.org/optimize
; Project:          asmlib.zip
; Description:
; This function calls the CPUID instruction.
;
; Copyright (c) 2011 GNU General Public License www.gnu.org/licenses
;******************************************************************************

global _cpuid_ex

SECTION .text  align=16

; ********** cpuid_ex function **********
; C++ prototype:
; extern "C" void cpuid_ex (int abcd[4], int eax, int ecx);
; Input: a = eax, c = ecx
; Output: abcd[0] = eax, abcd[1] = ebx, abcd[2] = ecx, abcd[3] = edx


_cpuid_ex:
        push    ebx
        push    edi
        mov     edi, [esp+12]          ; abcd out
        mov     eax, [esp+16]          ; eax in
        mov     ecx, [esp+20]          ; ecx in
        cpuid                          ; input eax, ecx. output eax, ebx, ecx, edx
        mov     [edi],    eax
        mov     [edi+4],  ebx
        mov     [edi+8],  ecx
        mov     [edi+12], edx
        pop     edi
        pop     ebx
        ret
;_cpuid_ex END
