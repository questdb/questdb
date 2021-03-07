;*************************  substring64.asm  **********************************
; Author:           Agner Fog
; Date created:     2011-07-18
; Last modified:    2011-07-18
; Source URL:       www.agner.org/optimize
; Project:          asmlib.zip
; Description:
; Makes a substring of a zero-terminated ASCII string
;
; C++ prototype:
; extern "C"
; size_t A_substring(char * dest, const char * source, size_t pos, size_t len);
; Makes a substring from source, starting at position pos (zero-based) and length
; len and stores it in the array dest. It is the responsibility of the programmer
; that the size of the dest array is at least len + 1.
; The return value is the actual length of the substring. This may be less than 
; len if the length of source is less than pos + len.
;
; Copyright (c) 2011 GNU General Public License www.gnu.org/licenses/gpl.html
;******************************************************************************

global A_substring                      ; Function _A_substring

extern A_strlen
extern A_memcpy

SECTION .text

; extern "C"
; size_t A_substring(char * dest, const char * source, size_t pos, size_t len);

%ifdef WINDOWS
%define par1    rcx                    ; dest
%define par2    rdx                    ; source
%define par3    r8                     ; pos
%define par4    r9                     ; len
%else   ; UNIX
%define par1    rdi
%define par2    rsi
%define par3    rdx
%define par4    rcx
%endif

A_substring:
        push    par1
        push    par2
        push    par3
        push    par4
        mov     par1, par2
        call    A_strlen               ; rax = strlen(source)
        pop     par4
        pop     par3
        pop     par2
        pop     par1        
        sub     rax, par3              ; max length = strlen(source) - pos
        jbe     empty                  ; strlen(source) <= pos. Return empty string
        cmp     rax, par4
        cmova   rax, par4              ; min(len, maxlen)
        add     par2, par3             ; source + pos = source for memcpy
        mov     par3, rax              ; length for memcpy
        push    rax                    ; new length
        call    A_memcpy
        pop     rcx                    ; new length = return value, rax = dest
        mov     byte [rcx+rax], 0      ; terminating zero
        mov     rax, rcx               ; return new length
        ret
        
empty:  ; return empty string
        xor     eax, eax               ; return 0
        mov     byte [par1], al
        ret
        
;A_substring END
