;*************************  strtouplow32.asm  ************************************
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
; CPU dispatching included for 386 and SSE4.2 instruction sets.
;
; Copyright (c) 2011 GNU General Public License www.gnu.org/licenses
;******************************************************************************

section .data
align  16

azlow:   db 'azazazazazazazaz'         ; define range for lower case
azhigh:  db 'AZAZAZAZAZAZAZAZ'         ; define range for upper case
casebit: times 16 db 20h               ; bit to change when changing case

section .text

global _A_strtolower
global _A_strtoupper
global _strtolowerGeneric
global _strtoupperGeneric
global _strtolowerSSE42
global _strtoupperSSE42

; Imported from instrset32.asm:
extern _InstructionSet                 ; Instruction set for CPU dispatcher

; function dispatching

%IFNDEF POSITIONINDEPENDENT
_A_strtolower: 
        jmp     near [strtolowerDispatch] ; Go to appropriate version, depending on instruction set

_A_strtoupper: 
        jmp     near [strtoupperDispatch] ; Go to appropriate version, depending on instruction set

%ELSE   ; Position-independent code

_A_strtolower:
        call    get_thunk_edx          ; get reference point for position-independent code
RP1:                                   ; reference point edx = offset RP1
; Make the following instruction with address relative to RP1:
        jmp     dword [edx+strtolowerDispatch-RP1]

_A_strtoupper:
        call    get_thunk_edx          ; get reference point for position-independent code
RP2:                                   ; reference point edx = offset RP2
; Make the following instruction with address relative to RP2:
        jmp     dword [edx+strtoupperDispatch-RP2]

%ENDIF


_strtoupperSSE42:
%IFNDEF POSITIONINDEPENDENT
        movdqa  xmm1, [azlow]          ; define range a-z
        movdqa  xmm3, [casebit]        ; bit to change
%ELSE
        call    get_thunk_edx          ; get reference point for position-independent code
RP11:
        movdqa  xmm1, [edx+azlow-RP11] ; same, with relative address
        movdqa  xmm3, [edx+casebit-RP11]
%ENDIF
		jmp     strupperlower
		
_strtolowerSSE42:
%IFNDEF POSITIONINDEPENDENT
        movdqa  xmm1, [azhigh]         ; define range A-Z
        movdqa  xmm3, [casebit]        ; bit to change
%ELSE
        call    get_thunk_edx          ; get reference point for position-independent code
RP12:
        movdqa  xmm1, [edx+azhigh-RP12]; same, with relative address
        movdqa  xmm3, [edx+casebit-RP12]
%ENDIF

        
strupperlower:
        ; common code for strtoupper and strtolower
        mov     edx,  [esp+4]          ; string
next:   ; loop
        movdqu  xmm2, [edx]            ; read 16 bytes from string
        pcmpistrm xmm1, xmm2, 01000100b; find bytes in range A-Z or a-z, return mask in xmm0
        jz      last                   ; string ends in this paragraph
        pand    xmm0, xmm3             ; mask AND case bit
        pxor    xmm2, xmm0             ; change case bit in masked bytes of string
        movdqu  [edx], xmm2            ; write changed value
		add     edx, 16
		jmp     next                   ; next 16 bytes

last:   ; Write last 0-15 bytes
        ; While we can read past the end of the string if precautions are made, we cannot write
		; past the end of the string, even if the value is unchanged, because the value may have
		; been changed in the meantime by another thread
        jnc     finish                 ; nothing changed, no need to write
        pand    xmm3, xmm0             ; mask and case bit
        pxor    xmm2, xmm3             ; change case bit

%if 0   ; Method with maskmovdqu is elegant, but slow because maskmovdqu uses nontemporal (uncached) write
        push    edi
		mov     edi, edx
		maskmovdqu xmm2, xmm0
		pop     edi
finish: ret

%else   ; less elegant alternative, but probably faster if data needed again soon
        ; write 8-4-2-1 bytes, if necessary
		pmovmskb eax, xmm0             ; create bit mask
		cmp     eax, 10000000b
		jb      L10
		; there are at least 8 bytes to write
		movq    [edx], xmm2
		psrldq  xmm2, 8
		add     edx, 8
		shr     eax, 8
L10:    cmp     eax, 1000b
        jb      L20
		; there are at least 4 bytes to write
		movd    [edx], xmm2
		psrldq  xmm2, 4
		add     edx, 4
		shr     eax, 4
L20:    movd    ecx, xmm2              ; use ecx for last 3 bytes
        cmp     eax, 10b
		jb      L30
		; there are at least 2 bytes to write
		mov     [edx], cx
		shr     ecx, 16
		add     edx, 2
		shr     eax, 2
L30:    cmp     eax, 1
        jb      finish
		; there is one more byte to write
		mov     [edx], cl
finish: ret
%endif

; 386 version
_strtolowerGeneric:
        mov     edx,  [esp+4]          ; string
A100:   ; loop
        mov     al, [edx]
		test    al, al
		jz      A900                   ; end of string
		sub     al, 'A'
		cmp     al, 'Z' - 'A'
		jbe     A200                   ; is upper case
		inc     edx
		jmp     A100                   ; loop to next character
A200:   ; convert to lower case
        add     al, 'a'
		mov     [edx], al
		inc     edx
		jmp     A100
A900:   ret
;_strtolowerGeneric end

_strtoupperGeneric:
        mov     edx,  [esp+4]          ; string
B100:   ; loop
        mov     al, [edx]
		test    al, al
		jz      B900                   ; end of string
		sub     al, 'a'
		cmp     al, 'z' - 'a'
		jbe     B200                   ; is lower case
		inc     edx
		jmp     B100                   ; loop to next character
B200:   ; convert to upper case
        add     al, 'A'
		mov     [edx], al
		inc     edx
		jmp     B100
B900:   ret
;_strtoupperGeneric end

%IFDEF  POSITIONINDEPENDENT
get_thunk_edx: ; load caller address into edx for position-independent code
        mov edx, [esp]
        ret
%ENDIF

; CPU dispatching for strtolower. This is executed only once
strtolowerCPUDispatch:
%IFNDEF POSITIONINDEPENDENT
        ; get supported instruction set
        call    _InstructionSet
        ; Point to generic version
        mov     ecx, _strtolowerGeneric
        cmp     eax, 10                ; check SSE4.2
        jb      Q100
        ; SSE4.2 supported
        ; Point to SSE4.2 version
        mov     ecx, _strtolowerSSE42
Q100:   mov     [strtolowerDispatch], ecx
        ; Continue in appropriate version
        jmp     ecx

%ELSE   ; Position-independent version
        ; get supported instruction set
        call    _InstructionSet
        call    get_thunk_edx
RP21:    ; reference point edx
        ; Point to generic version
        lea     ecx, [edx+_strtolowerGeneric-RP21]
        cmp     eax, 10                ; check SSE4.2
        jb      Q100
        ; SSE4.2 supported
        ; Point to SSE4.2 version
        lea     ecx, [edx+_strtolowerSSE42-RP21]
Q100:   mov     [edx+strtolowerDispatch-RP21], ecx
        ; Continue in appropriate version
        jmp     ecx
%ENDIF

; CPU dispatching for strtoupper. This is executed only once
strtoupperCPUDispatch:
%IFNDEF POSITIONINDEPENDENT
        ; get supported instruction set
        call    _InstructionSet
        ; Point to generic version
        mov     ecx, _strtoupperGeneric
        cmp     eax, 10                ; check SSE4.2
        jb      Q200
        ; SSE4.2 supported
        ; Point to SSE4.2 version 
        mov     ecx, _strtoupperSSE42
Q200:   mov     [strtoupperDispatch], ecx
        ; Continue in appropriate version
        jmp     ecx

%ELSE   ; Position-independent version
        ; get supported instruction set
        call    _InstructionSet
        call    get_thunk_edx
RP22:    ; reference point edx
        ; Point to generic version 
        lea     ecx, [edx+_strtoupperGeneric-RP22]
        cmp     eax, 10                ; check SSE4.2
        jb      Q200
        ; SSE4.2 supported
        ; Point to SSE4.2 version
        lea     ecx, [edx+_strtoupperSSE42-RP22]
Q200:   mov     [edx+strtoupperDispatch-RP22], ecx
        ; Continue in appropriate version
        jmp     ecx
%ENDIF


SECTION .data

; Pointer to appropriate version. Initially points to dispatcher
strtolowerDispatch DD strtolowerCPUDispatch
strtoupperDispatch DD strtoupperCPUDispatch

%IFDEF POSITIONINDEPENDENT
; Fix problem in Mac linker
        DD      0,0,0,0
%ENDIF


SECTION .bss
; Append 16 bytes to end of last data section to allow reading past end of strings:
; (We might use names .bss$zzz etc. under Windows to make it is placed
; last, but the assembler gives sections with unknown names wrong attributes.
; Here, we are just relying on library data being placed after main data.
; This can be verified by making a link map file)
        resq 4
