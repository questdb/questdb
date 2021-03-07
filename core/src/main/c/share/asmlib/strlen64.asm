;**************************  strlen64.asm  **********************************
; Author:           Agner Fog
; Date created:     2008-07-19
; Last modified:    2008-10-16
; Description:
; Faster version of the standard strlen function:
; size_t strlen(const char * str);
; Finds the length of a zero-terminated string of bytes, optimized for speed.
;
; Overriding standard function strlen:
; The alias ?OVR_strlen is changed to _strlen in the object file if
; it is desired to override the standard library function strlen.
;
; Calling conventions: 
; Stack alignment is not required. No shadow space or red zone used.
; Called internally from strcpy and strcat without stack aligned.
;
; Optimization:
; Uses XMM registers to read 16 bytes at a time, aligned.
; Misaligned parts of the string are read from the nearest 16-bytes boundary
; and the irrelevant part masked out. It may read both before the begin of 
; the string and after the end, but will never load any unnecessary cache 
; line and never trigger a page fault for reading from non-existing memory 
; pages because it never reads past the nearest following 16-bytes boundary.
; It may, though, trigger any debug watch within the same 16-bytes boundary.
;
; The latest version of this file is available at:
; www.agner.org/optimize/asmexamples.zip
; Copyright (c) 2009 GNU General Public License www.gnu.org/licenses
;******************************************************************************

default rel

global A_strlen              ; Function A_strlen
global ?OVR_strlen           ; ?OVR removed if standard function strlen overridden


SECTION .text  align=16

; extern "C" int strlen (const char * s);

; 64-bit Windows version:
A_strlen:
?OVR_strlen:

%IFDEF  WINDOWS
        mov      rax,  rcx             ; get pointer to string from rcx
        mov      r8,   rcx             ; copy pointer
%define Rscopy   r8                    ; Copy of s

%ELSE   ; Unix
        mov      rax,  rdi             ; get pointer to string from rdi
        mov      ecx,  edi             ; copy pointer (lower 32 bits)
%define Rscopy   rdi                   ; Copy of s
%ENDIF
        
        ; rax = s, ecx = 32 bits of s
        pxor     xmm0, xmm0            ; set to zero
        and      ecx,  0FH             ; lower 4 bits indicate misalignment
        and      rax,  -10H            ; align pointer by 16
        movdqa   xmm1, [rax]           ; read from nearest preceding boundary
        pcmpeqb  xmm1, xmm0            ; compare 16 bytes with zero
        pmovmskb edx,  xmm1            ; get one bit for each byte result
        shr      edx,  cl              ; shift out false bits
        shl      edx,  cl              ; shift back again
        bsf      edx,  edx             ; find first 1-bit
        jnz      L2                    ; found
        
        ; Main loop, search 16 bytes at a time
L1:     add      rax,  10H             ; increment pointer by 16
        movdqa   xmm1, [rax]           ; read 16 bytes aligned
        pcmpeqb  xmm1, xmm0            ; compare 16 bytes with zero
        pmovmskb edx,  xmm1            ; get one bit for each byte result
        bsf      edx,  edx             ; find first 1-bit
        ; (moving the bsf out of the loop and using test here would be faster for long strings on old processors,
        ;  but we are assuming that most strings are short, and newer processors have higher priority)
        jz       L1                    ; loop if not found
        
L2:     ; Zero-byte found. Compute string length        
        sub      rax,  Rscopy          ; subtract start address
        add      rax,  rdx             ; add byte index
        ret
        
;A_strlen ENDP
