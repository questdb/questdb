;*************************  divfixedi64.asm  *********************************
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

%IFDEF  WINDOWS
%define par1   rcx                     ; function parameter 1
%define par2   edx                     ; function parameter 2
%define buf    r9                      ; copy of function parameter 1: buffer
%define rx     r8
%define rxd    r8d                     ; d or x
%ELSE   ; UNIX
%define par1   rdi                     ; function parameter 1
%define par2   esi                     ; function parameter 2
%define buf    rdi                     ; function parameter 1: buffer
%define rx     rsi
%define rxd    esi                     ; d or x
%ENDIF


section .text

; extern "C" void setdivisori32(int buffer[2], int d);
; 32 bit signed 

global setdivisori32
setdivisori32:
%IFDEF  WINDOWS
        mov     rxd, edx               ; x
        mov     buf, rcx               ; buffer
%ENDIF        
        dec     rxd                    ; rxd = r8d or esi
        mov     ecx, -1                ; value for bsr if rxd = 0 (assuming bsr leaves dest unchanged if src = 0, this works on both Intel, AMD and VIA processors)
        bsr     ecx, rxd               ; floor(log2(d-1))
        inc     rxd
        js      H120                   ; d < 0. Generate error
        inc     ecx                    ; L = ceil(log2(d))        
        sub     ecx, 1                 ; shift count = L - 1
        adc     ecx, 0                 ; avoid negative shift count
        xor     eax, eax
        mov     edx, 1
        cmp     rxd, edx
        je      H110                   ; avoid overflow when d = 1
        shl     edx, cl
        div     rxd
H110:   inc     eax
        mov     [buf], eax             ; multiplier
        mov     [buf+4], ecx           ; shift count
        ret
        
H120:   ; d <= 0 not supported. Generate error
        mov     edx, 1
        div     edx                    ; will overflow
        ud2

        
; extern "C" int dividefixedi32(int buffer[2], int x);
global dividefixedi32
dividefixedi32:
%IFDEF  WINDOWS
        mov     eax, edx
        mov     rxd, edx               ; x
        mov     buf, rcx               ; buffer
%ELSE
        mov     eax, esi
%ENDIF        
        imul    dword [buf]            ; m
        lea     eax, [rdx+rx]          ; rx = r8 or rsi
        mov     ecx, [buf+4]           ; shift count
        sar     eax, cl
        sar     rxd, 31                ; sign(x)
        sub     eax, rxd
        ret


;extern "C" void setdivisoru32(int buffer[2], int d);
; 32 bit unsigned 

global setdivisoru32
setdivisoru32:
%IFDEF  WINDOWS
        mov     rxd, edx               ; x
        mov     buf, rcx               ; buffer
%ENDIF        
        dec     rxd                    ; rxd = r8d or esi
        mov     ecx, -1                ; value for bsr if r8d = 0
        bsr     ecx, rxd               ; floor(log2(d-1))
        inc     rxd
        inc     ecx                    ; L = ceil(log2(d))
        mov     edx, 1
        shl     rdx, cl                ; 2^L (64 bit shift because cl may be 32)
        sub     edx, rxd
        xor     eax, eax
        div     rxd
        inc     eax
        mov     [buf], eax             ; multiplier
        sub     ecx, 1
        setae   dl
        movzx   edx, dl                ; shift1
        seta    al
        neg     al
        and     al,cl
        movzx   eax, al                ; shift 2
        shl     eax, 8
        or      eax, edx
        mov     [buf+4], eax           ; shift 1 and shift 2
        ret
        
;extern "C" int dividefixedu32(int buffer[2], int x);
global dividefixedu32       ; unsigned
dividefixedu32:
%IFDEF  WINDOWS
        mov     eax, edx
        mov     rxd, edx               ; x
        mov     buf, rcx               ; buffer
%ELSE
        mov     eax, esi
%ENDIF        
        mul     dword [buf]            ; m
        sub     rxd, edx               ; x-t
        mov     ecx, [buf+4]           ; shift 1 and shift 2
        shr     rxd, cl
        lea     eax, [rx+rdx]
        shr     ecx, 8
        shr     eax, cl
        ret
