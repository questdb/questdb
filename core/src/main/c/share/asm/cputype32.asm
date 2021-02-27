;*************************  cputype32.asm  **********************************
; Author:           Agner Fog
; Date created:     2011-07-09
; Last modified:    2011-07-09
; Source URL:       www.agner.org/optimize
; Project:          asmlib.zip
; Language:         assembly, NASM/YASM syntax, 32 bit
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

global _CpuType


SECTION .text

_CpuType:
        push    ebx
        push    esi
        push    edi
        
; parameters
%define vendor  esp+16
%define family  esp+20
%define model   esp+24

        xor     esi, esi               ; vendor
        xor     edi, edi               ; family

        ; detect if CPUID instruction supported by microprocessor:
        pushfd
        pop     eax
        btc     eax, 21                ; check if CPUID bit can toggle
        push    eax
        popfd
        pushfd
        pop     ebx
        xor     ebx, eax
        bt      ebx, 21
        jc      C900                   ; CPUID not supported
        
        xor     eax, eax
        cpuid                          ; get number of CPUID functions
        
        ; get vendor
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
C110:   or      esi, 1
        jmp     C200
C120:   or      esi, 2
        jmp     C200
C130:   or      esi, 3
        jmp     C200
C140:   or      esi, 4
        jmp     C200
C150:   or      esi, 5
        ;jmp     C200
C200:   
        test    eax, eax
        jz      C900                   ; function 1 not supported

        ; Get family and model
        mov     eax, 1
        cpuid                          
        mov     ebx, eax
        mov     edi, eax
        shr     ebx, 8
        and     ebx, 0FH               ; Family
        shr     edi, 20
        and     edi, 0FFH              ; Extended family
        add     edi, ebx               ; Family + extended family
        
        mov     ebx, eax
        shr     ebx, 4
        and     ebx, 0FH               ; Model
        mov     ecx, eax
        shr     ecx, 12
        and     ecx, 0F0H              ; Extended model
        or      ebx, ecx               ; extended model - Model
        
C300:   ; return esi = vendor, edi = family, ebx = model
        mov     eax, [vendor]
        test    eax, eax
        jz      C310
        mov     [eax], esi
C310:   mov     eax, [family]
        test    eax, eax
        jz      C320
        mov     [eax], edi
C320:   mov     eax, [model]
        test    eax, eax
        jz      C330
        mov     [eax], ebx
C330:   xor     eax, eax
        ; return
        pop     edi
        pop     esi
        pop     ebx
        ret
        
C900:   ; no cpuid
        xor     ebx, ebx
        jmp     C300
;_CpuType ENDP
