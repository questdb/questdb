;*************************  strtouplow64.asm  ************************************
; Author:           Agner Fog
; Date created:     2011-07-17
; Last modified:    2013-09-11

; Description:
; A_strtolower converts a sting to lower case
; A_strtoupper converts a sting to upper case
; Only characters a-z or A-Z are converted, other characters are ignored.
; The functions save time by ignoring locale-specific characters or UTF-8 
; characters so it doesn't have to look up each character in a table.
;
; Function prototypes:
; extern "C" void A_strtolower(char * string);
; extern "C" void A_strtoupper(char * string);
;
; Note that these functions may read up to 15 bytes beyond the end of the strings.
; This is rarely a problem but it can in principle generate a protection violation
; if a string is placed at the end of the data segment.
;
; CPU dispatching included for SSE2 and SSE4.2 instruction sets.
;
; Copyright (c) 2011 GNU General Public License www.gnu.org/licenses
;******************************************************************************

default rel

section .data
align 16

azlow:   db 'azazazazazazazaz'         ; define range for lower case
azhigh:  db 'AZAZAZAZAZAZAZAZ'         ; define range for upper case
casebit: times 16 db 20h               ; bit to change when changing case

%ifdef WINDOWS
%define par1  rcx                      ; register for parameter 1
%else ; UNIX
%define par1  rdi
%endif

section .text

global A_strtolower
global A_strtoupper
global strtolowerGeneric
global strtoupperGeneric
global strtolowerSSE42
global strtoupperSSE42

; Imported from instrset64.asm:
extern InstructionSet                 ; Instruction set for CPU dispatcher

; function dispatching

A_strtolower: 
        jmp     near [strtolowerDispatch] ; Go to appropriate version, depending on instruction set

A_strtoupper: 
        jmp     near [strtoupperDispatch] ; Go to appropriate version, depending on instruction set


; SSE4.2 version
strtoupperSSE42:
        movdqa  xmm1, [azlow]          ; define range a-z
		jmp     strupperlower
strtolowerSSE42:
        movdqa  xmm1, [azhigh]         ; define range A-Z
strupperlower:
        ; common code for strtoupper and strtolower
        movdqa  xmm3, [casebit]        ; bit to change
next:   ; loop
        movdqu  xmm2, [par1]           ; read 16 bytes from string
        pcmpistrm xmm1, xmm2, 01000100b; find bytes in range A-Z or a-z, return mask in xmm0
        jz      last                   ; string ends in this paragraph
        pand    xmm0, xmm3             ; mask AND case bit
        pxor    xmm2, xmm0             ; change case bit in masked bytes of string
        movdqu  [par1], xmm2           ; write changed value
		add     par1, 16
		jmp     next                   ; next 16 bytes

last:   ; Write last 0-15 bytes
        ; While we can read past the end of the string if precautions are made, we cannot write
		; past the end of the string, even if the value is unchanged, because the value may have
		; been changed in the meantime by another thread
        jnc     finish                 ; nothing changed, no need to write
        pand    xmm3, xmm0             ; mask and case bit
        pxor    xmm2, xmm3             ; change case bit

%if 0   ; Method with maskmovdqu is elegant, but slow because maskmovdqu uses nontemporal (uncached) write
        push    rdi
		mov     rdi, par1
		maskmovdqu xmm2, xmm0
		pop     rdi
finish: ret

%else   ; less elegant alternative, but probably faster if data needed again soon
        ; write 8-4-2-1 bytes, if necessary
		pmovmskb eax, xmm0             ; create bit mask
		cmp     eax, 10000000b
		jb      L10
		; there are at least 8 bytes to write
		movq    [par1], xmm2
		psrldq  xmm2, 8
		add     par1, 8
		shr     eax, 8
L10:    cmp     eax, 1000b
        jb      L20
		; there are at least 4 bytes to write
		movd    [par1], xmm2
		psrldq  xmm2, 4
		add     par1, 4
		shr     eax, 4
L20:    movd    edx, xmm2              ; use edx for last 3 bytes
        cmp     eax, 10b
		jb      L30
		; there are at least 2 bytes to write
		mov     [par1], dx
		shr     edx, 16
		add     par1, 2
		shr     eax, 2
L30:    cmp     eax, 1
        jb      finish
		; there is one more byte to write
		mov     [par1], dl
finish: ret
%endif

; SSE2 version
strtolowerGeneric:
A100:   ; loop
        mov     al, [par1]
		test    al, al
		jz      A900                   ; end of string
		sub     al, 'A'
		cmp     al, 'Z' - 'A'
		jbe     A200                   ; is upper case
		inc     par1
		jmp     A100                   ; loop to next character
A200:   ; convert to lower case
        add     al, 'a'
		mov     [par1], al
		inc     par1
		jmp     A100
A900:   ret
;strtolowerGeneric end

strtoupperGeneric:
B100:   ; loop
        mov     al, [par1]
		test    al, al
		jz      B900                   ; end of string
		sub     al, 'a'
		cmp     al, 'z' - 'a'
		jbe     B200                   ; is lower case
		inc     par1
		jmp     B100                   ; loop to next character
B200:   ; convert to upper case
        add     al, 'A'
		mov     [par1], al
		inc     edx
		jmp     B100
B900:   ret
;strtoupperGeneric end


; CPU dispatching for strtolower. This is executed only once
strtolowerCPUDispatch:
        ; get supported instruction set
        push    par1
        call    InstructionSet
        pop     par1
        ; Point to generic version
        lea     rdx, [strtolowerGeneric]
        cmp     eax, 10                ; check SSE4.2
        jb      Q100
        ; SSE4.2 supported
        ; Point to SSE4.2 version
        lea     rdx, [strtolowerSSE42]
Q100:   mov     [strtolowerDispatch], rdx
        ; Continue in appropriate version
        jmp     rdx

; CPU dispatching for strtoupper. This is executed only once
strtoupperCPUDispatch:
        ; get supported instruction set
        push    par1
        call    InstructionSet
        pop     par1
        ; Point to generic version
        lea     rdx, [strtoupperGeneric]
        cmp     eax, 10                ; check SSE4.2
        jb      Q200
        ; SSE4.2 supported
        ; Point to SSE4.2 version
        lea     rdx, [strtoupperSSE42]
Q200:   mov     [strtoupperDispatch], rdx
        ; Continue in appropriate version
        jmp     rdx


SECTION .data

; Pointer to appropriate version. Initially points to dispatcher
strtolowerDispatch DQ strtolowerCPUDispatch
strtoupperDispatch DQ strtoupperCPUDispatch

; Append 16 bytes to end of last data section to allow reading past end of strings:
; (We might use names .bss$zzz etc. under Windows to make it is placed
; last, but the assembler gives sections with unknown names wrong attributes.
; Here, we are just relying on library data being placed after main data.
; This can be verified by making a link map file)
SECTION .bss
        dq      0, 0
