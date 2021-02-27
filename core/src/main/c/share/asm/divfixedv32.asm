;*************************  divfixedv32.asm  *********************************
; Author:           Agner Fog
; Date created:     2011-07-25
; Last modified:    2012-03-10
;
; Function prototypes:
; void setdivisorV8i16(__m128i buf[2], int16_t d);
; void setdivisorV8u16(__m128i buf[2], uint16_t d);
; void setdivisorV4i32(__m128i buf[2], int32_t d);
; void setdivisorV4u32(__m128i buf[2], uint32_t d);
;
; __m128i dividefixedV8i16(const __m128i buf[2], __m128i x);
; __m128i dividefixedV8u16(const __m128i buf[2], __m128i x);;
; __m128i dividefixedV4i32(const __m128i buf[2], __m128i x);
; __m128i dividefixedV4u32(const __m128i buf[2], __m128i x);
;
; Alternative versions for VectorClass.h:
; (These versions pack all parameters into a single register)
; __m128i setdivisor8s(int16_t d);
; __m128i setdivisor8us(uint16_t d);
; __m128i setdivisor4i(int32_t d);
; __m128i setdivisor4ui(uint32_t d);
;
; Description:
; Functions for integer vector division by the same divisor, signed 
; and unsigned 16-bit and 32-bit integer versions.
;
; The setdivisor functions calculate the reciprocal divisor and shift counts,
; the dividefixed functions do the division by multiplication and shift of the 
; vector elements of packed 16-bit or 32-bit signed or unsigned integers. 
;
; The divisor must be positive. A zero divisor generated a divide by zero error.
; A negative divisor generates a division overflow error. To divide by a negative
; divisor, change the sign of the divisor and the result.
;
; The methods used are described in this article:
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
; Copyright (c) 2011 - 2012 GNU General Public License www.gnu.org/licenses
;******************************************************************************

; Imported from instrset32.asm:
extern _InstructionSet                 ; Instruction set for CPU dispatcher

section .text  align = 16

;******************************************************************************
;                    16 bit signed integers
;******************************************************************************

; extern "C" __m128i setdivisor8s(int16_t d);
; vector of 8 x 16 bit signed integers

global _setdivisor8s
_setdivisor8s:
        push    ebx
        movsx   ebx, word [esp+8]      ; d
        dec     ebx
        mov     ecx, -1                ; value for bsr if ebx = 0
        bsr     ecx, ebx               ; floor(log2(d-1))
        inc     ebx
        js      H120                   ; Generate error if d < 0. (error for d=0 will come in the div instruction)
        inc     ecx                    ; L = ceil(log2(d))        
        sub     ecx, 1                 ; shift count = L - 1
        adc     ecx, 0                 ; avoid negative shift count
        xor     eax, eax
        mov     edx, 1
        cmp     ebx, edx
        je      H110                   ; avoid division overflow when d = 1
        shl     edx, cl
        div     bx                     ; 2^(16+L-1)/d
H110:   inc     eax
        movd    xmm0, eax
        pshuflw xmm0, xmm0, 0          ; broadcast into lower 4 words
        movd    xmm1, ecx              ; shift count
        punpcklqdq xmm0, xmm1          ; insert shift count into upper half
        pop     ebx
        ret
        
H120:   ; d < 0 not supported. Generate error
        mov     edx, 1
        div     edx
        ud2
; _setdivisor8s end

; extern "C" void setdivisorV8i16(__m128i buf[2], int16_t d);
; vector of 8 x 16 bit signed integers

global _setdivisorV8i16
_setdivisorV8i16:
        mov     eax, dword [esp+8]     ; d
        push    eax
        call    _setdivisor8s
        pop     ecx
        mov     eax, dword [esp+4]     ; buf
        punpcklqdq xmm0, xmm0          ; copy multiplier into upper 4 words        
        movdqa  [eax], xmm0            ; multiplier
        movdqa  [eax+16], xmm1         ; shift count is still in xmm1
        ret
; _setdivisorV8i16 end

        
; extern "C" int dividefixedV8i16(const __m128i buf[2], __m128i x);
global _dividefixedV8i16

align 16
_dividefixedV8i16:
        mov     ecx, [esp+4]           ; buffer
        movdqa  xmm1, xmm0             ; x
        pmulhw  xmm0, [ecx]            ; multiply high signed words
        paddw   xmm0, xmm1
        movd    xmm2, [ecx+16]         ; shift count
        psraw   xmm0, xmm2             ; shift right arithmetic
        psraw   xmm1, 15               ; sign of x
        psubw   xmm0, xmm1
        ret
;_dividefixedV8i16 end



;******************************************************************************
;                    16 bit unsigned integers
;******************************************************************************

; extern "C" __m128i setdivisor8us(uint16_t d);
; vector of 8 x 16 bit unsigned integers

global _setdivisor8us
_setdivisor8us:
        push    ebx
        movzx   ebx, word [esp+8]      ; d
        dec     ebx
        mov     ecx, -1                ; value for bsr if ebx = 0
        bsr     ecx, ebx               ; floor(log2(d-1))
        inc     ebx
        inc     ecx                    ; L = ceil(log2(d))
        mov     edx, 1
        shl     edx, cl                ; 2^L  [32-bit shift to allow overflow]
        sub     edx, ebx
        xor     eax, eax
        div     bx
        inc     eax
        movd    xmm0, eax
        pshuflw xmm0, xmm0, 0          ; broadcast into lower 4 words
        sub     ecx, 1
        setae   dl
        movzx   edx, dl                ; shift1
        seta    al
        neg     al
        and     al,cl
        movzx   eax, al                ; shift 2
        movd    xmm1, edx              ; shift 1
        movd    xmm2, eax              ; shift 2
        punpckldq  xmm1, xmm2          ; combine into two dwords
        punpcklqdq xmm0, xmm1          ; multipliers, shift1, shift2
        pop     ebx
        ret
; _setdivisor8us

;extern "C" void setdivisorV8u16(__m128i buf[2], uint16_t d);
; 8 x 16 bit unsigned 

global _setdivisorV8u16
_setdivisorV8u16:
        mov     eax, dword [esp+8]    ; d
        push    eax
        call    _setdivisor8us
        pop     ecx
        mov     eax, dword [esp+4]     ; buf
        punpcklqdq xmm0, xmm0          ; copy multiplier into upper 4 words        
        movdqa  [eax], xmm0            ; multiplier
        movdqa  [eax+16], xmm1         ; shift counts are still in xmm1
        ret
; _setdivisorV8u16 end

        
;extern "C" __m128i dividefixedV8u16(const __m128i buf[2], __m128i x);
global _dividefixedV8u16

align 16
_dividefixedV8u16:
        mov     ecx, [esp+4]           ; buffer
        movdqa  xmm1, xmm0             ; x
        pmulhuw xmm0, [ecx]            ; multiply high unsigned words
        psubw   xmm1, xmm0
        movd    xmm2, [ecx+16]         ; shift1
        psrlw   xmm1, xmm2
        paddw   xmm0, xmm1
        movd    xmm2, [ecx+20]         ; shift2
        psrlw   xmm0, xmm2
        ret
; _dividefixedV8u16 end



;******************************************************************************
;                    32 bit signed integers
;******************************************************************************

; extern "C" __m128i setdivisor4i(int32_t d);
; vector of 4 x 32 bit signed integers

align 16
global _setdivisor4i
_setdivisor4i:
        push    ebx
        mov     ebx, [esp+8]           ; d
        dec     ebx
        mov     ecx, -1                ; value for bsr if ebx = 0
        bsr     ecx, ebx               ; floor(log2(d-1))
        inc     ebx
        js      K120                   ; Generate error if d < 0. (error for d=0 will come in the div instruction)
        inc     ecx                    ; L = ceil(log2(d))        
        sub     ecx, 1                 ; shift count = L - 1
        adc     ecx, 0                 ; avoid negative shift count
        xor     eax, eax
        mov     edx, 1
        cmp     ebx, edx
        je      K110                   ; avoid division overflow when d = 1
        shl     edx, cl
        div     ebx                    ; 2^(16+L-1)/d
K110:   inc     eax
        movd    xmm0, eax              ; multiplier
        pshufd  xmm0, xmm0, 0          ; broadcast into 4 dwords
        movd    xmm1, ecx              ; shift count
        punpcklqdq xmm0, xmm1          ; insert shift count into upper half
        pop     ebx
        ret
        
K120:   ; d < 0 not supported. Generate error
        mov     edx, 1
        div     edx
        ud2
; _setdivisor4i end


; extern "C" void setdivisorV4i32(__m128i buf[2], int32_t d);
; vector of 4 x 32 bit signed integers

global _setdivisorV4i32
_setdivisorV4i32:
        mov     eax, dword [esp+8]     ; d
        push    eax
        call    _setdivisor4i
        pop     ecx
        mov     eax, dword [esp+4]     ; buf
        punpcklqdq xmm0, xmm0          ; copy multiplier into upper 4 words        
        movdqa  [eax], xmm0            ; multiplier
        movdqa  [eax+16], xmm1         ; shift counts are still in xmm1
        ret
; _setdivisorV4i32 end

        
; extern "C" int dividefixedV4i32(const __m128i buf[2], __m128i x);
global _dividefixedV4i32

; Direct entries to CPU-specific versions
global _dividefixedV4i32SSE2                
global _dividefixedV4i32SSE41

align 8
_dividefixedV4i32: ; function dispatching

%IFNDEF POSITIONINDEPENDENT
        jmp     near [dividefixedV4i32Dispatch] ; Go to appropriate version, depending on instruction set
%ELSE   ; Position-independent code
        call    get_thunk_edx          ; get reference point for position-independent code
RP1:                                   ; reference point edx = offset RP1
; Make the following instruction with address relative to RP1:
        jmp     near [edx+dividefixedV4i32Dispatch-RP1]
%ENDIF

align 16
_dividefixedV4i32SSE41: 
        mov     ecx, [esp+4]           ; buffer
        movdqa  xmm1, xmm0             ; x
        movdqa  xmm2, xmm0             ; x        
        movdqa  xmm3, [ecx]            ; multiplier
        pmuldq  xmm0, xmm3             ; 32 x 32 -> 64 bit unsigned multiplication of x[0] and x[2]
        psrlq   xmm0, 32               ; high dword of result 0 and 2
        psrlq   xmm1, 32               ; get x[1] and x[3] into position for multiplication
        pmuldq  xmm1, xmm3             ; 32 x 32 -> 64 bit unsigned multiplication of x[1] and x[3]
        pcmpeqd xmm3, xmm3
        psllq   xmm3, 32               ; generate mask of dword 1 and 3
        pand    xmm1, xmm3             ; high dword of result 1 and 3
        por     xmm0, xmm1             ; combine all four results into one vector
        paddd   xmm0, xmm2
        movd    xmm3, [ecx+16]         ; shift count
        psrad   xmm0, xmm3             ; shift right arithmetic
        psrad   xmm2, 31               ; sign of x
        psubd   xmm0, xmm2
        ret
;_dividefixedV4i32SSE41 end


_dividefixedV4i32SSE2:
; I have tried to change sign and use pmuludq, but get rounding error (gives 9/10 = 1).
; This solution, with 4 separate multiplications, is probably faster anyway despite store forwarding stall
        push    ebp
        mov     ebp, esp
        sub     esp, 16
        and     esp, -16               ; make aligned stack space
        movdqa  [esp], xmm0            ; store x
        movdqa  xmm2, xmm0             ; x        
        mov     ecx, [ebp+8]           ; buffer
        mov     ecx, [ecx]             ; multiplier
        ; do four signed high multiplications
        mov     eax, [esp]
        imul    ecx
        mov     [esp], edx
        mov     eax, [esp+4]
        imul    ecx
        mov     [esp+4], edx
        mov     eax, [esp+8]
        imul    ecx
        mov     [esp+8], edx
        mov     eax, [esp+12]
        imul    ecx
        mov     [esp+12], edx
        movdqa  xmm0, [esp]            ; x*m vector
        mov     ecx, [ebp+8]           ; buffer
        paddd   xmm0, xmm2
        movd    xmm3, [ecx+16]         ; shift count
        psrad   xmm0, xmm3             ; shift right arithmetic
        psrad   xmm2, 31               ; sign of x
        psubd   xmm0, xmm2
        mov     esp, ebp
        pop     ebp        
        ret
;_dividefixedV4i32SSE2 end


; ********************************************************************************
; CPU dispatching for _dividefixedV4i32. This is executed only once
; ********************************************************************************

dividefixedV4i32CPUDispatch:
%IFNDEF POSITIONINDEPENDENT
        ; get supported instruction set
        call    _InstructionSet
        ; Point to generic version
        mov     ecx, _dividefixedV4i32SSE2
        cmp     eax, 8                ; check if PMULDQ supported
        jb      Q100
        ; SSE4.1 supported
        ; Point to SSE4.1 version of strstr
        mov     ecx, _dividefixedV4i32SSE41
Q100:   mov     [dividefixedV4i32Dispatch], ecx
        ; Continue in appropriate version 
        jmp     ecx

%ELSE   ; Position-independent version
        ; get supported instruction set
        call    _InstructionSet
        call    get_thunk_edx
RP10:   ; reference point edx
        ; Point to generic version
        lea     ecx, [edx+_dividefixedV4i32SSE2-RP10]
        cmp     eax, 8                ; check if PMULDQ supported
        jb      Q100
        ; SSE4.1 supported
        ; Point to SSE4.1 version of strstr
        lea     ecx, [edx+_dividefixedV4i32SSE41-RP10]
Q100:   mov     [edx+dividefixedV4i32Dispatch-RP10], ecx
        ; Continue in appropriate version
        jmp     ecx
        
get_thunk_edx: ; load caller address into edx for position-independent code
        mov edx, [esp]
        ret        
%ENDIF

SECTION .data

; Pointer to appropriate versions. Initially point to dispatcher
dividefixedV4i32Dispatch DD dividefixedV4i32CPUDispatch

section .text



;******************************************************************************
;                    32 bit unsigned integers
;******************************************************************************

; extern "C" __m128i setdivisor4ui(uint32_t d);
; vector of 4 x 32 bit unsigned integers

align 16
global _setdivisor4ui
_setdivisor4ui:
        push    ebx
        mov     ebx, [esp+8]           ; d
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
        movd    xmm0, eax
        pshufd  xmm0, xmm0, 0          ; broadcast into 4 dwords
        sub     ecx, 1
        setae   dl
        movzx   edx, dl                ; shift1
        seta    al
        neg     al
        and     al,cl
        movzx   eax, al                ; shift 2
        movd    xmm1, edx              ; shift 1
        movd    xmm2, eax              ; shift 2
        punpckldq  xmm1, xmm2          ; combine into two dwords
        punpcklqdq xmm0, xmm1          ; multipliers, shift1, shift2
        pop     ebx
        ret
; _setdivisor4ui end

;extern "C" void setdivisorV4u32(__m128i buf[2], uint32_t d);
; 4 x 32 bit unsigned 

global _setdivisorV4u32
_setdivisorV4u32:
        mov     eax, dword [esp+8]     ; d
        push    eax
        call    _setdivisor4ui
        pop     ecx
        mov     eax, dword [esp+4]     ; buf
        punpcklqdq xmm0, xmm0          ; copy multiplier into upper 4 words        
        movdqa  [eax], xmm0            ; multiplier
        movdqa  [eax+16], xmm1         ; shift counts are still in xmm1
        ret
; _setdivisorV4u32 end
        
;extern "C" __m128i dividefixedV4u32(const __m128i buf[2], __m128i x);
global _dividefixedV4u32

align 16
_dividefixedV4u32:
        mov     ecx, [esp+4]           ; buffer
        movdqa  xmm1, xmm0             ; x
        movdqa  xmm2, xmm0             ; x
        movdqa  xmm3, [ecx]            ; multiplier
        pmuludq xmm0, xmm3             ; 32 x 32 -> 64 bit unsigned multiplication of x[0] and x[2]
        psrlq   xmm0, 32               ; high dword of result 0 and 2
        psrlq   xmm1, 32               ; get x[1] and x[3] into position for multiplication
        pmuludq xmm1, xmm3             ; 32 x 32 -> 64 bit unsigned multiplication of x[1] and x[3]
        pcmpeqd xmm3, xmm3
        psllq   xmm3, 32               ; generate mask of dword 1 and 3
        pand    xmm1, xmm3             ; high dword of result 1 and 3
        por     xmm0, xmm1             ; combine all four results into one vector
        psubd   xmm2, xmm0
        movd    xmm3, [ecx+16]         ; shift1
        psrld   xmm2, xmm3
        paddd   xmm0, xmm2
        movd    xmm3, [ecx+20]         ; shift2
        psrld   xmm0, xmm3
        ret
;_dividefixedV4u32 end
