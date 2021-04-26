;*************************  memcmp64.asm  *************************************
; Author:           Agner Fog
; Date created:     2013-10-03
; Last modified:    2016-11-08
; Description:
; Faster version of the standard memcmp function:
;
; int A_memcmp (const void * ptr1, const void * ptr2, size_t count);
;
; Compares two memory blocks of size num.
; The return value is zero if the two memory blocks ptr1 and ptr2 are equal
; The return value is positive if the first differing byte of ptr1 is bigger 
; than ptr2 when compared as unsigned bytes.
; The return value is negative if the first differing byte of ptr1 is smaller 
; than ptr2 when compared as unsigned bytes.
;
; Overriding standard function memcmp:
; The alias ?OVR_memcmp is changed to _memcmp in the object file if
; it is desired to override the standard library function memcmp.
;
; Optimization:
; Uses the largest vector registers available
;
; The latest version of this file is available at:
; www.agner.org/optimize/asmexamples.zip
; Copyright (c) 2013-2016 GNU General Public License www.gnu.org/licenses
;******************************************************************************

global A_memcmp              ; Function memcmp
global ?OVR_memcmp           ; ?OVR_ removed if standard function memcmp overridden
; Direct entries to CPU-specific versions
global memcmpSSE2            ; SSE2 version
global memcmpAVX2            ; AVX2 version
global memcmpAVX512F         ; AVX512F version
global memcmpAVX512BW        ; AVX512BW version

; Imported from instrset64.asm
extern InstructionSet                 ; Instruction set for CPU dispatcher

default rel

; define registers used for parameters
%IFDEF  WINDOWS
%define par1   rcx                     ; function parameter 1
%define par2   rdx                     ; function parameter 2
%define par3   r8                      ; function parameter 3
%define par4   r9                      ; scratch register
%define par4d  r9d                     ; scratch register
%ENDIF
%IFDEF  UNIX
%define par1   rdi                     ; function parameter 1
%define par2   rsi                     ; function parameter 2
%define par3   rdx                     ; function parameter 3
%define par4   rcx                     ; scratch register
%define par4d  ecx                     ; scratch register
%ENDIF



SECTION .text  align=16

; extern "C" int A_memcmp (const void * ptr1, const void * ptr2, size_t count);
; Function entry:
A_memcmp:
?OVR_memcmp:
        jmp     qword [memcmpDispatch] ; Go to appropriate version, depending on instruction set


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;
;   AVX512BW Version. Use zmm register
;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

; register aliases:
; par1   rcx or rdi  = function parameter 1 = ptr1
; par2   rdx or rsi  = function parameter 2 = ptr2
; par3   r8  or rdx  = function parameter 3 = count
; par4   r9  or rcx  = scratch register

align 16
memcmpAVX512BW:
memcmpAVX512BW@:                       ; internal reference
        cmp     par3, 40H
        jbe     L820
        cmp     par3, 80H
        jbe     L800

        ; count >= 80H
L010:   ; entry from memcmpAVX512F
        vmovdqu64 zmm16, [par1]
        vmovdqu64 zmm17, [par2]
        vpcmpd  k1, zmm16, zmm17, 4    ; compare first 40H bytes for dwords not equal
        kortestw k1, k1
        jnz     L500                   ; difference found

        ; find 40H boundaries
        lea     par4, [par1 + par3]    ; end of string 1
        mov     rax, par1
        add     par1, 40H
        and     par1, -40H             ; first aligned boundary for par1
        sub     rax, par1              ; -offset
        sub     par2, rax              ; same offset to par2
        mov     rax, par4
        and     par4, -40H             ; last aligned boundary for par1

        sub     par1, par4             ; par1 = -size of aligned blocks
        sub     par2, par1

L100:   ; main loop
        vmovdqa64 zmm16, [par4 + par1]
        vmovdqu64 zmm17, [par2 + par1]
        vpcmpd  k1, zmm16, zmm17, 4    ; compare first 40H bytes for not equal
        kortestw k1, k1
        jnz     L500                   ; difference found
        add     par1, 40H
        jnz     L100

        ; remaining 0-3FH bytes. Overlap with previous block
        add     par2, rax
        sub     par2, par4
        vmovdqu64 zmm16, [rax-40H]
        vmovdqu64 zmm17, [par2-40H]
        vpcmpd  k1, zmm16, zmm17, 4    ; compare first 40H bytes for not equal
        kortestw k1, k1
        jnz     L500                   ; difference found

        ; finished. no difference found
        xor     eax, eax
        ; vzeroupper not needed when using zmm16-31
        ret

L500:   ; the two strings are different
        vpcompressd zmm16{k1}{z},zmm16 ; get first differing dword to position 0
        vpcompressd zmm17{k1}{z},zmm17 ; get first differing dword to position 0
        vmovd   eax, xmm16
        vmovd   edx, xmm17
        mov     ecx, eax
        xor     ecx, edx               ; difference
        bsf     ecx, ecx               ; position of lowest differing bit
        and     ecx, -8                ; round down to byte boundary
        shr     eax, cl                ; first differing byte in al
        shr     edx, cl                ; first differing byte in dl
        movzx   eax, al                ; zero-extend bytes
        movzx   edx, dl
        sub     eax, edx               ; signed difference between unsigned bytes
        ; vzeroupper not needed when using zmm16-31
        ret

align   16
L800:   ; size = 41H - 80H
        vmovdqu64 zmm16, [par1]
        vmovdqu64 zmm17, [par2]
        vpcmpd  k1, zmm16, zmm17, 4    ; compare first 40H bytes for not equal
        kortestw k1, k1
        jnz     L500                   ; difference found
        add     par1, 40H
        add     par2, 40H
        sub     par3, 40H

L820:   ; size = 00H - 40H
        ; (this is the only part that requires AVX512BW)
        or      rax, -1                ; generate masks
        bzhi    rax, rax, par3
        kmovq   k1, rax
        vmovdqu8 zmm16{k1}{z}, [par1]
        vmovdqu8 zmm17{k1}{z}, [par2]
        vpcmpd  k1, zmm16, zmm17, 4    ; compare
        kortestw k1, k1
        jnz     L500                   ; difference found
        xor     eax, eax               ; no difference found
        ; vzeroupper not needed when using zmm16-31
        ret

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;
;   AVX512F Version. Use zmm register
;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

align 16
memcmpAVX512F:
memcmpAVX512F@:                       ; internal reference
        cmp     par3, 80H             ; size
        jae     L010                  ; continue in memcmpAVX512BW
        jmp     memcmpAVX2@           ; continue in memcmpAVX2 if less than 80H bytes


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;
;   AVX2 Version. Use ymm register
;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

align 16
memcmpAVX2:
memcmpAVX2@:   ; internal reference

        add     par1, par3                       ; use negative index from end of memory block
        add     par2, par3
        neg     par3
        jz      A900
        mov     par4d, 0FFFFH 
        cmp     par3, -32
        ja      A100
        
A000:   ; loop comparing 32 bytes
        vmovdqu   ymm1, [par1+par3]
        vpcmpeqb  ymm0, ymm1, [par2+par3]        ; compare 32 bytes
        vpmovmskb eax, ymm0                      ; get byte mask
        xor     eax, -1                          ; not eax would not set flags
        jnz     A700                             ; difference found
        add     par3, 32
        jz      A900                             ; finished, equal
        cmp     par3, -32
        jna     A000                             ; next 32 bytes
        vzeroupper                               ; end ymm state
        
A100:   ; less than 32 bytes left
        cmp     par3, -16
        ja      A200
        movdqu  xmm1, [par1+par3]
        movdqu  xmm2, [par2+par3]
        pcmpeqb xmm1, xmm2                       ; compare 16 bytes
        pmovmskb eax, xmm1                       ; get byte mask
        xor     eax, par4d                       ; invert lower 16 bits
        jnz     A701                             ; difference found
        add     par3, 16
        jz      A901                             ; finished, equal
        
A200:   ; less than 16 bytes left
        cmp     par3, -8
        ja      A300
        ; compare 8 bytes
        movq    xmm1, [par1+par3]
        movq    xmm2, [par2+par3]
        pcmpeqb xmm1, xmm2                       ; compare 8 bytes
        pmovmskb eax, xmm1                       ; get byte mask
        xor     eax, par4d
        jnz     A701                             ; difference found
        add     par3, 8
        jz      A901 
        
A300:   ; less than 8 bytes left
        cmp     par3, -4
        ja      A400
        ; compare 4 bytes
        movd    xmm1, [par1+par3]
        movd    xmm2, [par2+par3]
        pcmpeqb xmm1, xmm2                       ; compare 4 bytes
        pmovmskb eax, xmm1                       ; get byte mask
        xor     eax, par4d                         ; not ax
        jnz     A701                             ; difference found
        add     par3, 4
        jz      A901 

A400:   ; less than 4 bytes left
        cmp     par3, -2
        ja      A500
        movzx   eax, word [par1+par3]
        movzx   par4d, word [par2+par3]
        sub     eax, par4d
        jnz     A800                             ; difference in byte 0 or 1
        add     par3, 2
        jz      A901 
        
A500:   ; less than 2 bytes left
        test    par3, par3
        jz      A901                             ; no bytes left
        
A600:   ; one byte left
        movzx   eax, byte [par1+par3]
        movzx   par4d, byte [par2+par3]
        sub     eax, par4d                         ; return result
        ret

A700:   ; difference found. find position
        vzeroupper
A701:   
        bsf     eax, eax
        add     par3, rax
        movzx   eax, byte [par1+par3]
        movzx   par4d, byte [par2+par3]
        sub     eax, par4d                         ; return result
        ret

A800:   ; difference in byte 0 or 1
        neg     al
        sbb     par3, -1                           ; add 1 to par3 if al == 0
        movzx   eax, byte [par1+par3]
        movzx   par4d, byte [par2+par3]
        sub     eax, par4d                         ; return result
        ret

A900:   ; equal
        vzeroupper
A901:   xor     eax, eax        
        ret

        
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;
;   SSE2 version. Use xmm register
;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

memcmpSSE2:
memcmpSSE2@:   ; internal reference

        add     par1, par3                         ; use negative index from end of memory block
        add     par2, par3
        neg     par3
        jz      S900 
        mov     par4d, 0FFFFH
        cmp     par3, -16
        ja      S200
        
S100:   ; loop comparing 16 bytes
        movdqu  xmm1, [par1+par3]
        movdqu  xmm2, [par2+par3]
        pcmpeqb xmm1, xmm2                       ; compare 16 bytes
        pmovmskb eax, xmm1                       ; get byte mask
        xor     eax, par4d                         ; not ax
        jnz     S700                             ; difference found
        add     par3, 16
        jz      S900                             ; finished, equal
        cmp     par3, -16
        jna     S100                             ; next 16 bytes
        
S200:   ; less than 16 bytes left
        cmp     par3, -8
        ja      S300
        ; compare 8 bytes
        movq    xmm1, [par1+par3]
        movq    xmm2, [par2+par3]
        pcmpeqb xmm1, xmm2                       ; compare 8 bytes
        pmovmskb eax, xmm1                       ; get byte mask
        xor     eax, par4d                         ; not ax
        jnz     S700                             ; difference found
        add     par3, 8
        jz      S900 
        
S300:   ; less than 8 bytes left
        cmp     par3, -4
        ja      S400
        ; compare 4 bytes
        movd    xmm1, [par1+par3]
        movd    xmm2, [par2+par3]
        pcmpeqb xmm1, xmm2                       ; compare 4 bytes
        pmovmskb eax, xmm1                       ; get byte mask
        xor     eax, par4d                         ; not ax
        jnz     S700                             ; difference found
        add     par3, 4
        jz      S900 

S400:   ; less than 4 bytes left
        cmp     par3, -2
        ja      S500
        movzx   eax, word [par1+par3]
        movzx   par4d, word [par2+par3]
        sub     eax, par4d
        jnz     S800                             ; difference in byte 0 or 1
        add     par3, 2
        jz      S900 
        
S500:   ; less than 2 bytes left
        test    par3, par3
        jz      S900                             ; no bytes left
        
        ; one byte left
        movzx   eax, byte [par1+par3]
        movzx   par4d, byte [par2+par3]
        sub     eax, par4d                         ; return result
        ret

S700:   ; difference found. find position
        bsf     eax, eax
        add     par3, rax
        movzx   eax, byte [par1+par3]
        movzx   par4d, byte [par2+par3]
        sub     eax, par4d                         ; return result
        ret

S800:   ; difference in byte 0 or 1
        neg     al
        sbb     par3, -1                          ; add 1 to par3 if al == 0
S820:   movzx   eax, byte [par1+par3]
        movzx   par4d, byte [par2+par3]
        sub     eax, par4d                         ; return result
        ret

S900:   ; equal
        xor     eax, eax        
        ret


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;
; CPU dispatching for memcmp. This is executed only once
;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
        
memcmpCPUDispatch:
        push    par1
        push    par2
        push    par3        
        call    InstructionSet         ; get supported instruction set
        ; SSE2 always supported
        lea     par4, [memcmpSSE2@]
        cmp     eax, 13                ; check AVX2
        jb      Q100
        ; AVX2 supported
        lea     par4, [memcmpAVX2@]        
        cmp     eax, 15                ; check AVX512F
        jb      Q100
        ; AVX512F supported
        lea     par4, [memcmpAVX512F@]
        cmp     eax, 16                ; check AVX512BW
        jb      Q100
        ; AVX512BW supported
        lea     par4, [memcmpAVX512BW@]        

Q100:   ; save pointer
        mov     qword [memcmpDispatch], par4
; Continue in appropriate version of memcmp
        pop     par3
        pop     par2
        pop     par1
        jmp     par4


SECTION .data
align 16


; Pointer to appropriate version.
; This initially points to memcmpCPUDispatch. memcmpCPUDispatch will
; change this to the appropriate version of memcmp, so that
; memcmpCPUDispatch is only executed once:
memcmpDispatch DQ memcmpCPUDispatch

