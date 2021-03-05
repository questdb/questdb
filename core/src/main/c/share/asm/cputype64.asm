;*************************  cputype64.asm  **********************************
; Author:           Agner Fog
; Date created:     2011-07-09
; Last modified:    2011-07-09
; Source URL:       www.agner.org/optimize
; Project:          asmlib.zip
; Language:         assembly, NASM/YASM syntax, 64 bit
;
; C++ prototype:
; extern "C" void CpuType(int * vendor, int * family, int * model);
;
; Description:
; This function finds the vendor, family and model number of the CPU
; and returns the values through the pointers. If a pointer is zero
; then the value is not returned.
;
; Vendor: 
; 0 = unknown
; 1 = Intel
; 2 = AMD
; 3 = VIA/Centaur
; 4 = Cyrix
; 5 = NexGen
;
; Family: This is the sum of the family and extended family fields of the cpuid
; Model:  This is the model + (extended model << 8)
;
; Copyright (c) 2011 GNU General Public License www.gnu.org/licenses
;******************************************************************************
;
; C++ prototype:
; extern "C" void CpuType(int * vendor, int * family, int * model);

global CpuType


SECTION .text

CpuType:
        push    rbx
%ifdef  UNIX
        mov     r8, rdx
%endif
%ifdef  WINDOWS        
        push    rsi
        push    rdi
        mov     rdi, rcx
        mov     rsi, rdx
%endif
        
; parameters
; vendor  rdi
; family  rsi
; model   r8

        xor     r9d,  r9d              ; vendor
        xor     r10d, r10d             ; family
        xor     r11d, r11d             ; model

        xor     eax, eax
        cpuid                          ; get vendor
        ; ecx = last  4 characters of vendor string
        ; ebx = first 4 characters of vendor string
        cmp     ecx, 'ntel'            ; 'GenuineIntel'
        je      C110
        cmp     ecx, 'cAMD'            ; 'AuthenticAMD'
        je      C120
        cmp     ebx, 'Cent'            ; 'CentaurHauls'
        je      C130
        cmp     ebx, 'VIA '            ; 'VIA VIA VIA '
        je      C130
        cmp     ebx, 'Cyri'            ; 'CyrixInstead'
        je      C140
        cmp     ebx, 'NexG'            ; 'NexGenDriven'
        je      C150
        jmp     C200                   ; other
C110:   or      r9d, 1
        jmp     C200
C120:   or      r9d, 2
        jmp     C200
C130:   or      r9d, 3
        jmp     C200
C140:   or      r9d, 4
        jmp     C200
C150:   or      r9d, 5
        ;jmp     C200
C200:   

        ; Get family and model
        mov     eax, 1
        cpuid                          
        mov     ebx, eax
        mov     r10d, eax
        shr     ebx, 8
        and     ebx, 0FH               ; Family
        shr     r10d, 20
        and     r10d, 0FFH             ; Extended family
        add     r10d, ebx              ; Family + extended family
        
        mov     r11d, eax
        shr     r11d, 4
        and     r11d, 0FH              ; Model
        shr     eax, 12
        and     eax, 0F0H              ; Extended model
        or      r11d, eax              ; extended model | Model
        
C300:   ; return r9d = vendor, r10d = family, r11d = model
        test    rdi, rdi
        jz      C310
        mov     [rdi], r9d
C310:   test    rsi, rsi
        jz      C320
        mov     [rsi], r10d
C320:   test    r8, r8
        jz      C330
        mov     [r8], r11d
C330:   xor     eax, eax
        ; return
%ifdef  WINDOWS 
        pop     rdi
        pop     rsi
%endif
        pop     rbx
        ret
;CpuType ENDP
