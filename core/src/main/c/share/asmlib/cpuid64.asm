;*************************  cpuid64.asm  *********************************
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

default rel

global cpuid_ex

SECTION .text  align=16

; ********** cpuid_ex function **********
; C++ prototype:
; extern "C" void cpuid_ex (int abcd[4], int a, int c);
; Input: a = eax, c = ecx
; Output: abcd[0] = eax, abcd[1] = ebx, abcd[2] = ecx, abcd[3] = edx


cpuid_ex:

%IFDEF   WINDOWS
; parameters: rcx = abcd, edx = a, r8d = c
        push    rbx
        xchg    rcx, r8
        mov     eax, edx
        cpuid                          ; input eax, ecx. output eax, ebx, ecx, edx
        mov     [r8],    eax
        mov     [r8+4],  ebx
        mov     [r8+8],  ecx
        mov     [r8+12], edx
        pop     rbx
%ENDIF        
%IFDEF   UNIX
; parameters: rdi = abcd, esi = a, edx = c
        push    rbx
        mov     eax, esi
        mov     ecx, edx
        cpuid                          ; input eax, ecx. output eax, ebx, ecx, edx
        mov     [rdi],    eax
        mov     [rdi+4],  ebx
        mov     [rdi+8],  ecx
        mov     [rdi+12], edx
        pop     rbx
%ENDIF        
        ret
;cpuid_ex END
