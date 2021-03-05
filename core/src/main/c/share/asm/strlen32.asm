;**************************  strlen32.asm  **********************************
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
; Position-independent code is generated if POSITIONINDEPENDENT is defined.
;
; Internal calls: The parameter on the stack is left unchanged for the sake
; of calls from strcpy and strcat.
;
; Optimization:
; Uses XMM registers to read 16 bytes at a time, aligned.
; Misaligned parts of the string are read from the nearest 16-bytes boundary
; and the irrelevant part masked out. It may read both before the begin of 
; the string and after the end, but will never load any unnecessary cache 
; line and never trigger a page fault for reading from non-existing memory 
; pages because it never reads past the nearest following 16-bytes boundary.
; It may, though, trigger any debug watch within the same 16-bytes boundary.
; CPU dispatching included for 386 and SSE2 instruction sets.
;
; The latest version of this file is available at:
; www.agner.org/optimize/asmexamples.zip
; Copyright (c) 2009 GNU General Public License www.gnu.org/licenses
;******************************************************************************

global _A_strlen             ; Function _A_strlen
global ?OVR_strlen           ; ?OVR removed if standard function strlen overridden

; Imported from instrset32.asm
extern _InstructionSet                 ; Instruction set for CPU dispatcher


SECTION .text  align=16

; extern "C" int strlen (const char * s);
_A_strlen:
?OVR_strlen:

%IFNDEF POSITIONINDEPENDENT
        jmp     [strlenDispatch]       ; Go to appropriate version, depending on instruction set

%ELSE   ; Position-independent code

        call    get_thunk_eax          ; get reference point for position-independent code
RP:                                    ; reference point eax = offset RP
A020:                                  ; Go here after CPU dispatching

        ; Make the following instruction with address relative to RP:
        cmp     dword [eax-RP+strlenCPUVersion], 1
        jb      strlenCPUDispatch      ; First time: strlenCPUVersion = 0, go to dispatcher
        je      strlen386              ; strlenCPUVersion = 1, go to 80386 version
%ENDIF

; SSE2 version
strlenSSE2:
        mov      eax,  [esp+4]         ; get pointer to string
        mov      ecx,  eax             ; copy pointer
        pxor     xmm0, xmm0            ; set to zero
        and      ecx,  0FH             ; lower 4 bits indicate misalignment
        and      eax,  -10H            ; align pointer by 16
        movdqa   xmm1, [eax]           ; read from nearest preceding boundary
        pcmpeqb  xmm1, xmm0            ; compare 16 bytes with zero
        pmovmskb edx,  xmm1            ; get one bit for each byte result
        shr      edx,  cl              ; shift out false bits
        shl      edx,  cl              ; shift back again
        bsf      edx,  edx             ; find first 1-bit
        jnz      A200                  ; found
        
        ; Main loop, search 16 bytes at a time
A100:   add      eax,  10H             ; increment pointer by 16
        movdqa   xmm1, [eax]           ; read 16 bytes aligned
        pcmpeqb  xmm1, xmm0            ; compare 16 bytes with zero
        pmovmskb edx,  xmm1            ; get one bit for each byte result
        bsf      edx,  edx             ; find first 1-bit
        ; (moving the bsf out of the loop and using test here would be faster for long strings on old processors,
        ;  but we are assuming that most strings are short, and newer processors have higher priority)
        jz       A100                  ; loop if not found
        
A200:   ; Zero-byte found. Compute string length        
        sub      eax,  [esp+4]         ; subtract start address
        add      eax,  edx             ; add byte index
        ret

strlen386: ; 80386 version
        push    ebx
        mov     ecx, [esp+8]           ; get pointer to string
        mov     eax, ecx               ; copy pointer
        and     ecx, 3                 ; lower 2 bits of address, check alignment
        jz      L2                     ; string is aligned by 4. Go to loop
        and     eax, -4                ; align pointer by 4
        mov     ebx, [eax]             ; read from nearest preceding boundary
        shl     ecx, 3                 ; mul by 8 = displacement in bits
        mov     edx, -1
        shl     edx, cl                ; make byte mask
        not     edx                    ; mask = 0FFH for false bytes
        or      ebx, edx               ; mask out false bytes

        ; check first four bytes for zero
        lea     ecx, [ebx-01010101H]   ; subtract 1 from each byte
        not     ebx                    ; invert all bytes
        and     ecx, ebx               ; and these two
        and     ecx, 80808080H         ; test all sign bits
        jnz     L3                     ; zero-byte found
        
        ; Main loop, read 4 bytes aligned
L1:     add     eax, 4                 ; increment pointer by 4
L2:     mov     ebx, [eax]             ; read 4 bytes of string
        lea     ecx, [ebx-01010101H]   ; subtract 1 from each byte
        not     ebx                    ; invert all bytes
        and     ecx, ebx               ; and these two
        and     ecx, 80808080H         ; test all sign bits
        jz      L1                     ; no zero bytes, continue loop
        
L3:     bsf     ecx, ecx               ; find right-most 1-bit
        shr     ecx, 3                 ; divide by 8 = byte index
        sub     eax, [esp+8]           ; subtract start address
        add     eax, ecx               ; add index to byte
        pop     ebx
        ret
        
        
; CPU dispatching for strlen. This is executed only once
strlenCPUDispatch:
%IFNDEF POSITIONINDEPENDENT
        pushad
        call    _InstructionSet
        ; Point to generic version of strlen
        mov     dword [strlenDispatch], strlen386
        cmp     eax, 4                 ; check SSE2
        jb      M100
        ; SSE2 supported
        ; Point to SSE2 version of strlen
        mov     dword [strlenDispatch], strlenSSE2
M100:   popad
        ; Continue in appropriate version of strlen
        jmp     dword [strlenDispatch]

%ELSE   ; Position-independent version
        pushad
        
        ; Make the following instruction with address relative to RP:
        lea     ebx, [eax-RP+strlenCPUVersion]
        ; Now ebx points to strlenCPUVersion.

        call    _InstructionSet        

        mov     byte [ebx], 1          ; Indicate generic version
        cmp     eax, 4                 ; check SSE2
        jb      M100
        ; SSE2 supported
        mov     byte [ebx], 2          ; Indicate SSE2 or later version
M100:   popad
        jmp     A020                   ; Go back and dispatch
        
get_thunk_eax: ; load caller address into ebx for position-independent code
        mov eax, [esp]
        ret       
        
%ENDIF        
        
SECTION .data
align 16
%IFNDEF POSITIONINDEPENDENT
; Pointer to appropriate version.
; This initially points to strlenCPUDispatch. strlenCPUDispatch will
; change this to the appropriate version of strlen, so that
; strlenCPUDispatch is only executed once:
strlenDispatch: DD strlenCPUDispatch
%ELSE    ; position-independent
; CPU version: 0=unknown, 1=80386, 2=SSE2
strlenCPUVersion: DD 0
; Fix potential problem in Mac linker
        DD      0, 0
%ENDIF
