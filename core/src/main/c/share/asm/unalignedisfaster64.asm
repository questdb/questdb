;*************************  unalignedisfaster64.asm  ******************************
; Author:           Agner Fog
; Date created:     2011-07-09
; Last modified:    2013-08-30
; Source URL:       www.agner.org/optimize
; Project:          asmlib.zip
; Language:         assembly, NASM/YASM syntax, 64 bit
;
; C++ prototype:
; extern "C" int UnalignedIsFaster(void);
;
; Description:
; This function finds out if unaligned 16-bytes memory read is
; faster than aligned read followed by an alignment shift (PALIGNR) on the
; current CPU.
;
; Return value:
; 0:   Unaligned read is probably slower than alignment shift
; 1:   Unknown or equal
; 2:   Unaligned read is probably faster than alignment shift
;
;
; C++ prototype:
; extern "C" int Store256BitIsFaster(void);
;
; Description:
; This function finds out if a 32-bytes memory write is
; faster than two 16-bytes writes on the current CPU.
;
; Return value:
; 0:   32-bytes memory write is slower or AVX not supported
; 1:   Unknown
; 2:   32-bytes memory write is faster
;
; Copyright (c) 2011 - 2013 GNU General Public License www.gnu.org/licenses
;******************************************************************************
;
; C++ prototype:
; extern "C" int UnalignedIsFaster(void);

%include "piccall.asi"

global UnalignedIsFaster
global Store256BitIsFaster
extern CpuType
extern InstructionSet


SECTION .text

UnalignedIsFaster:

%ifdef  UNIX
        push    0                      ; vendor
        mov     rdi, rsp
        push    0                      ; family
        mov     rsi, rsp
        push    0                      ; model
        mov     rdx, rsp
%else   ; WINDOWS
        push    0                      ; vendor
        mov     rcx, rsp
        push    0                      ; family
        mov     rdx, rsp
        push    0                      ; model
        mov     r8,  rsp
%endif
        callW   CpuType                ; get vendor, family, model
        pop     rdx                    ; model
        pop     rcx                    ; family
        pop     r8                     ; vendor
        xor     eax, eax               ; return value
        dec     r8d
        jz      Intel
        dec     r8d
        jz      AMD
        dec     r8d
        jz      VIA
        ; unknown vendor
        inc     eax
        jmp     Uend

Intel:  ; Unaligned read is faster on Intel Nehalem and later, but not Atom
        ; Nehalem  = family 6, model 1AH
        ; Atom     = family 6, model 1CH
		; Silvermont = family 6, model 37H (palignr may be slightly faster than unaligned read in some cases. no big difference)
        ; Netburst = family 0FH
        ; Future models are likely to be family 6, mayby > 6, model > 1C
        cmp     ecx, 6
        jb      Uend                   ; old Pentium 1, etc
        cmp     ecx, 0FH
        je      Uend                   ; old Netburst architecture
        cmp     edx, 1AH
        jb      Uend                   ; earlier than Nehalem
        cmp     edx, 1CH
        je      Uend                   ; Intel Atom
        or      eax, 2                 ; Intel Nehalem and later, except Atom
        jmp     Uend

AMD:    ; AMD processors:
        ; The PALIGNR instruction is slow on AMD Bobcat but fast on Jaguar
        ; K10/Opteron = family 10H     ; Use unaligned
        ; Bobcat = family 14H          ; PALIGNR is very slow. Use unaligned
        ; Piledriver = family 15H      ; Use unaligned
        ; Jaguar = family 16H          ; PALIGNR is fast. Use aligned (aligned is faster in most cases, but not all)
        cmp     ecx, 10H               ; AMD K8 or earlier: use aligned
        jb      Uend
        cmp     ecx, 16H               ; Jaguar: use aligned
        je      Uend
        or      eax, 2                 ; AMD K10 or later: use unaligned
        jmp     Uend

VIA:    ; Unaligned read is not faster than PALIGNR on VIA Nano 2000 and 3000
        cmp     ecx, 0FH
        jna     Uend                   ; VIA Nano
        inc     eax                    ; Future versions: unknown
       ;jmp     Uend

Uend:   ret

;UnalignedIsFaster ENDP


Store256BitIsFaster:
        callW   InstructionSet
        cmp     eax, 11                ; AVX supported
        jb      S90
%ifdef  UNIX
        push    0                      ; vendor
        mov     rdi, rsp
        push    0                      ; family
        mov     rsi, rsp
        push    0                      ; model
        mov     rdx, rsp
%else   ; WINDOWS
        push    0                      ; vendor
        mov     rcx, rsp
        push    0                      ; family
        mov     rdx, rsp
        push    0                      ; model
        mov     r8,  rsp
%endif
        callW   CpuType                ; get vendor, family, model
        pop     rdx                    ; model
        pop     rcx                    ; family
        pop     rax                    ; vendor

        cmp     eax, 1                 ; Intel
        je      S_Intel
        cmp     eax, 2                 ; AMD
        je      S_AMD
        cmp     eax, 3
        je      S_VIA
        jmp     S91                    ; other vendor, not known

S_Intel:cmp     ecx, 6
        jne     S92                    ; unknown family. possibly future model
        ; model 2AH Sandy Bridge
        ; model 3AH Ivy Bridge
        ; model 3CH Haswell
        ; Sandy Bridge and Ivy Bridge are slightly faster with 128 than with 256 bit moves on large data blocks
        ; Haswell is much faster with 256 bit moves
        cmp     edx, 3AH
        jbe     S90
        jmp     S92

S_AMD:  ; AMD
        cmp     ecx, 15H               ; family 15h = Bulldozer, Piledriver
        ja      S92                    ; assume future AMD families are faster
                                       ; family 16H = Jaguar. 256 bit write is slightly faster
        ; model 1 = Bulldozer is a little slower on 256 bit write
        ; model 2 = Piledriver is terribly slow on 256 bit write
        ; model 30h = Steamroller is reasonable on 256 bit write
        cmp     edx, 30h
        jb      S90
        jmp     S91                    ; Steamroller: moderate

S_VIA:  jmp     S91                    ; don't know

S90:    xor     eax, eax               ; return 0
        ret

S91:    mov     eax, 1                 ; return 1
        ret

S92:    mov     eax, 2                 ; return 2
        ret

; Store256BitIsFaster ENDP