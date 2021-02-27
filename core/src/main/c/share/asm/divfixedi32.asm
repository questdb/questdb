;*************************  divfixedi32.asm  *********************************
; Author:           Agner Fog
; Date created:     2011-07-22
; Last modified:    2011-07-22
;
; Function prototypes:
; void setdivisori32(int buffer[2], int d);
; int dividefixedi32(const int buffer[2], int x);
; void setdivisoru32(uint32_t buffer[2], uint32_t d);
; uint32_t dividefixedu32(const uint32_t buffer[2], uint32_t x);
;
; Description:
; Functions for fast repeated integer division by the same divisor, signed 
; and unsigned 32-bit integer versions. The divisor must be positive.
;
; The setdivisor functions calculate the reciprocal divisor and shift counts,
; the dividefixed functions do the division by multiplication and shift.
;
; The methods used are described by:
; T. Granlund and P. L. Montgomery: Division by Invariant Integers Using Multiplication,
; Proceedings of the SIGPLAN 1994 Conference on Programming Language Design and Implementation.
; http://citeseerx.ist.psu.edu/viewdoc/summary?doi=10.1.1.1.2556
;
; Mathematical formula, unsigned division:
; x = dividend
; d = divisor
; n = integer size, bits
; L = ceil(log2(d))
; m = 1 + 2^n * (2^L-d) / d       [2^L should overflow to 0 if L = n]
; sh1 = min(L,1)
; sh2 = max(L-1,0)
; t = m*x >> n                    [high part of unsigned multiplication]
; x/d = (((x-t) >> sh1) + t) >> sh2
;
; Mathematical formula, signed division:
; x = dividend
; d = abs(divisor)
; n = integer size, bits
; L = ceil(log2(d))
; L = max(L,1)
; m = 1 + 2^(n+L-1)/d - 2^n       [division should overflow to 0 if d = 1]
; sh1 = L-1
; q = x + (m*x >> n)              [high part of signed multiplication]
; q = (q >> sh1) - (x<0 ? -1 : 0)
; if (divisor < 0) q = -q         [negative divisor not supported in present implementation]
; x/d = q
;
; Copyright (c) 2011 GNU General Public License www.gnu.org/licenses
;******************************************************************************

section .text

; extern "C" void setdivisori32(int buffer[2], int d);
; 32 bit signed 

global _setdivisori32
_setdivisori32:
        push    ebx
        mov     ebx, [esp+12]          ; d
        dec     ebx
        mov     ecx, -1                ; value for bsr if ebx = 0 (assuming bsr leaves dest unchanged if src = 0, this works on both Intel, AMD and VIA processors)
        bsr     ecx, ebx               ; floor(log2(d-1))
        inc     ebx
        js      H120                   ; d < 0. Generate error
        inc     ecx                    ; L = ceil(log2(d))        
        sub     ecx, 1                 ; shift count = L - 1
        adc     ecx, 0                 ; avoid negative shift count
        xor     eax, eax
        mov     edx, 1
        cmp     ebx, edx
        je      H110                   ; avoid division overflow when d = 1
        shl     edx, cl
        div     ebx
H110:   inc     eax
        mov     ebx, [esp+8]           ; buffer
        mov     [ebx], eax             ; multiplier
        mov     [ebx+4], ecx           ; shift count
        pop     ebx
        ret
        
H120:   ; d <= 0 not supported. Generate error
        mov     edx, 1
        div     edx
        ud2

        
; extern "C" int dividefixedi32(int buffer[2], int x);
global _dividefixedi32
_dividefixedi32:
        push    ebx
        mov     eax, [esp+12]          ; x
        mov     ecx, [esp+8]           ; buffer
        mov     ebx, eax
        imul    dword [ecx]            ; m
        lea     eax, [edx+ebx]
        mov     ecx, [ecx+4]           ; shift count
        sar     eax, cl
        sar     ebx, 31                ; sign(x)
        sub     eax, ebx
        pop     ebx
        ret


;extern "C" void setdivisoru32(int buffer[2], int d);
; 32 bit unsigned 

global _setdivisoru32
_setdivisoru32:
        push    ebx
        mov     ebx, [esp+12]          ; d
        dec     ebx
        mov     ecx, -1                ; value for bsr if ebx = 0
        bsr     ecx, ebx               ; floor(log2(d-1))
        inc     ebx
        inc     ecx                    ; L = ceil(log2(d))
        mov     edx, 1
        shl     edx, cl                ; 2^L
        cmp     cl, 20h
        adc     edx, -1                ; fix cl overflow, must give edx = 0
        sub     edx, ebx
        xor     eax, eax
        div     ebx
        inc     eax
        mov     ebx, [esp+8]           ; buffer
        mov     [ebx], eax             ; multiplier
        sub     ecx, 1
        setae   dl
        movzx   edx, dl                ; shift1
        seta    al
        neg     al
        and     al,cl
        movzx   eax, al                ; shift 2
        shl     eax, 8
        or      eax, edx
        mov     [ebx+4], eax           ; shift 1 and shift 2
        pop     ebx
        ret
        
;extern "C" int dividefixedu32(int buffer[2], int x);
global _dividefixedu32       ; unsigned
_dividefixedu32:
        mov     eax, [esp+8]           ; x
        mov     ecx, [esp+4]           ; buffer
        mul     dword [ecx]            ; m
        mov     eax, [esp+8]           ; x
        sub     eax, edx               ; x-t
        mov     ecx, [ecx+4]           ; shift 1 and shift 2
        shr     eax, cl
        add     eax, edx
        shr     ecx, 8
        shr     eax, cl
        ret
