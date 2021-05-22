;*************************  strstr64.asm  ************************************
; Author:           Agner Fog
; Date created:     2011-07-14
; Last modified:    2011-07-14

; Description:
; Faster version of the standard strstr function:
; char * A_strstr(char * haystack, const char * needle);
; Searches for substring needle in string haystack. Return value is pointer to 
; first occurrence of needle, or NULL if not found. The strings must be zero-terminated.
;
; Note that this function may read up to 15 bytes beyond the end of the strings.
; This is rarely a problem but it can in principle generate a protection violation
; if a string is placed at the end of the data segment. Avoiding this would be complicated
; and make the function much slower: For every unaligned 16-bytes read we would have to
; check if it crosses a page boundary (4 kbytes), and if so check if the string ends
; before the page boundary. Only if the string does not end before the page boundary
; can we read into the next memory page.
;
; Overriding standard function strstr:
; The alias ?OVR_strstr is changed to _strstr in the object file if
; it is desired to override the standard library function strstr.
; Overriding is disabled because the function may read beyond the end of a 
; string, while the standard strstr function is guaranteed to work in all cases.
;
; Position-independent code is generated if POSITIONINDEPENDENT is defined.
;
; CPU dispatching included for 386 and SSE4.2 instruction sets.
;
; Copyright (c) 2011 GNU General Public License www.gnu.org/licenses
;******************************************************************************
default rel

%define ALLOW_OVERRIDE 0               ; Set to one if override of standard function desired

global A_strstr             ; Function A_strstr

; Direct entries to CPU-specific versions
global strstrGeneric            ; Generic version for processors without SSE4.2
global strstrSSE42          ; Version for processors with SSE4.2

; Imported from instrset64.asm:
extern InstructionSet                 ; Instruction set for CPU dispatcher

section .text

; strstr function

%if ALLOW_OVERRIDE
global ?OVR_strstr
?OVR_strstr:
%endif

A_strstr: ; function dispatching
        jmp     near [strstrDispatch] ; Go to appropriate version, depending on instruction set

; define register use
%ifdef  WINDOWS
%define par1      rcx                  ; parameter 1, pointer to haystack
%define par2      rdx                  ; parameter 2, pointer to needle
%define bitindex  r8d                  ; bit index in eax mask 
%define bitindexr r8                   ; bit index in eax mask 
%define phay      r9                   ; pointer to match in haystack
%define pnee      r10                  ; pointer to match in needle
%define tempb     r8b                  ; temporary byte
%else
%define par1      rdi                  ; parameter 1, pointer to haystack
%define par2      rsi                  ; parameter 2, pointer to needle
%define bitindex  ecx                  ; bit index in eax mask 
%define bitindexr rcx                  ; bit index in eax mask 
%define phay      r9                   ; pointer to match in haystack
%define pnee      rdx                  ; pointer to match in needle
%define tempb     cl                   ; temporary byte
%endif

align 16
strstrSSE42: ; SSE4.2 version
        movdqu  xmm1, [par2]           ; needle

;align 8
haystacknext:   
        ; [par1] = haystack
        pcmpistrm xmm1, [par1], 00001100b ; unsigned byte search, equal ordered, return mask in xmm0
		jc      matchbegin             ; found beginning of a match
		jz      nomatch                ; end of haystack found, no match
		add     par1, 16
		jmp     haystacknext

matchbegin:
		jz      foundshort             ; haystack ends here, a short match is found
		movd    eax, xmm0              ; bit mask of possible matches
nextindexbit:
        bsf     bitindex, eax          ; index of first bit in mask of possible matches

		; compare strings for full match
		lea     phay, [par1+bitindexr] ; haystack + index
		mov     pnee, par2             ; needle

compareloop: ; compare loop for long match
        movdqu  xmm2, [pnee]           ; paragraph of needle
        pcmpistrm xmm2, [phay], 00001100B ; unsigned bytes, equal ordered, modifies xmm0
        ; (can't use "equal each, masked" because it inverts when past end of needle, but not when past end of both)

        jno     longmatchfail          ; difference found after extending partial match
		js      longmatchsuccess       ; end of needle found, and no difference
		add     pnee, 16
		add     phay, 16
		jmp     compareloop            ; loop to next 16 bytes

longmatchfail:
        ; remove index bit of first partial match
		btr     eax, bitindex
		test    eax, eax
		jnz     nextindexbit           ; mask contains more index bits, loop to next bit in eax mask
		; mask exhausted for possible matches, continue to next haystack paragraph
		add     par1, 16
		jmp     haystacknext           ; loop to next paragraph of haystack

longmatchsuccess: ; match found over more than one paragraph
		lea     rax, [par1+bitindexr]  ; haystack + index to begin of long match
		ret

foundshort: ; match found within single paragraph 
        movd    eax, xmm0              ; bit mask of matches
        bsf     eax, eax               ; index of first match
		add     rax, par1              ; pointer to first match
		ret

nomatch: ; needle not found, return 0
        xor     eax, eax
		ret

;strstrSSE42: endp


align 16
strstrGeneric: ; generic version
		
		mov     ax, [par2]
		test    al, al
		jz      _Found                 ; a zero-length needle is always found
		test    ah, ah
		jz      _SingleCharNeedle		
		
_SearchLoop: ; search for first character match
        mov     tempb, [par1]
        test    tempb, tempb
        jz      _NotFound              ; end of haystack reached without finding
        cmp     al, tempb
        je      _FirstCharMatch        ; first character match
_IncompleteMatch:
        inc     par1
        jmp     _SearchLoop            ; loop through haystack
		
_FirstCharMatch:
        mov     phay, par1             ; begin of match position
        mov     pnee, par2
_MatchLoop:
        inc     phay
        inc     pnee
        mov     al, [pnee]
        test    al, al
        jz      _Found                 ; end of needle. match ok
        cmp     al, [phay] 
        je      _MatchLoop
        ; match failed, recover and continue
		mov     al, [par2]
		jmp     _IncompleteMatch
		
_NotFound: ; needle not found. return 0
        xor     eax, eax
        ret
		
_Found: ; needle found. return pointer to position in haystack
        mov     rax, par1
        ret		
		
_SingleCharNeedle: ; Needle is a single character
        mov     tempb, byte [par1]
        test    tempb, tempb
        jz      _NotFound              ; end of haystack reached without finding
        cmp     al, tempb
        je      _Found
        inc     par1
        jmp     _SingleCharNeedle      ; loop through haystack


; CPU dispatching for strstr. This is executed only once
strstrCPUDispatch:
        ; get supported instruction set
        push    par1
        push    par2
        call    InstructionSet
        pop     par2
        pop     par1
        ; Point to generic version of strstr
        lea     r9, [strstrGeneric]
        cmp     eax, 10                ; check SSE4.2
        jb      Q100
        ; SSE4.2 supported
        ; Point to SSE4.2 version of strstr
        lea     r9, [strstrSSE42]
Q100:   mov     [strstrDispatch], r9
        ; Continue in appropriate version of strstr
        jmp     r9

SECTION .data

; Pointer to appropriate version. Initially points to dispatcher
strstrDispatch DQ strstrCPUDispatch

; Append 16 bytes to end of last data section to allow reading past end of strings:
; (We might use names .bss$zzz etc. under Windows to make it is placed
; last, but the assembler gives sections with unknown names wrong attributes.
; Here, we are just relying on library data being placed after main data.
; This can be verified by making a link map file)
SECTION .bss
        dq      0, 0
