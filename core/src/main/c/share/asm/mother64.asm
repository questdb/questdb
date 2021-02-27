; ----------------------------- MOTHER64.ASM -----------------------------
; Author:           Agner Fog
; Date created:     1998
; Last modified:    2013-12-15
; Source URL:       www.agner.org/optimize
; Project:          asmlib.zip
; Language:         assembly, NASM/YASM syntax, 64 bit
; Description:
; Mother-of-All random number generator by Agner Fog
; 64-bit mode version for x86-64 compatible microprocessors.
;
;  This is a multiply-with-carry type of random number generator
;  invented by George Marsaglia.  The algorithm is:             
;  S = 2111111111*X[n-4] + 1492*X[n-3] + 1776*X[n-2] + 5115*X[n-1] + C
;  X[n] = S modulo 2^32
;  C = floor(S / 2^32) 
;
; C++ prototypes:
; extern "C" void         MotRandomInit(void * Pthis, int seed);      // Initialization
; extern "C" int          MotIRandom(void * Pthis, int min, int max); // Get integer random number in desired interval
; extern "C" double       MotRandom(void * Pthis);                    // Get floating point random number
; extern "C" unsigned int MotBRandom(void * Pthis);                   // Output random bits
;
; Copyright (c) 2008-2013 GNU General Public License www.gnu.org/licenses
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

default rel

; structure definition and constants:
%INCLUDE "randomah.asi"

; publics:
global MotherBRandom, MotBRandom, ?Windows_MotBRandom
global MotherRandom, MotRandom, MotherIRandom, MotIRandom
global MotherRandomInit, MotRandomInit
%IFDEF WINDOWS
global MotherRandomInitD, MotherRandomD, MotherIRandomD, MotherBRandomD
%ENDIF


section .data
align 16

; Data for single instance of random number generator
MotherInstance: ISTRUC CRandomMotherA
IEND
; Size of structure
MotherSize equ $-MotherInstance


SECTION .CODE ALIGN=16   ; code segment

; Single threaded version:
; extern "C" unsigned int MotherBRandom(); // Output random bits

MotherBRandom: ; PROC                         ; entry for both Windows and Linux call
%IFDEF WINDOWS
MotherBRandomD:
%ENDIF
        lea     rcx, [MotherInstance]         ; Point to instance
        jmp     ?Windows_MotBRandom
;MotherBRandom ENDP       

; Thread-safe version:
; extern "C" unsigned int MotBRandom(void * Pthis); // Output random bits

MotBRandom: ; PROC 
%IFDEF UNIX
        mov     rcx, rdi                    ; translate calling convention
%ENDIF
?Windows_MotBRandom:
        and     rcx, -16                    ; align
        movdqa  xmm1, oword [rcx+CRandomMotherA.M3]  ; load M3,M2,M1,M0
        mov     eax,  [rcx+CRandomMotherA.M0]              ; Retrieve previous random number
        movdqa  xmm2, xmm1                                 ; copy
        movdqa  xmm3, oword [rcx+CRandomMotherA.MF3] ; factors
        psrlq   xmm2, 32                                   ; move M2,M0 down
        movq    qword [rcx+CRandomMotherA.M4], xmm1    ; M4=M3, M3=M2
        movhps  qword [rcx+CRandomMotherA.M2], xmm1    ; M2=M1, M1=M0
        pmuludq xmm1, xmm3                                 ; M3*MF3, M1*MF1
        psrlq   xmm3, 32                                   ; move MF2,MF0 down
        pmuludq xmm2, xmm3                                 ; M2*MF2, M0*MF0
        paddq   xmm1, xmm2                                 ; P2+P3, P0+P1
        movhlps xmm2, xmm1                                 ; Get high qword
        paddq   xmm1, xmm2                                 ; P0+P1+P2+P3
        paddq   xmm1, oword [rcx+CRandomMotherA.MC]    ; +carry
        movq    qword [rcx+CRandomMotherA.M0], xmm1    ; Store new M0 and carry
        ; convert to double precision float
        psllq   xmm1, 32                                   ; Discard carry bits
        psrlq   xmm1, 12                                   ; Get bits into mantissa position
        por     xmm1, oword [rcx+CRandomMotherA.one] ; Add exponent bits to get number in interval [1,2)
        movq    [rcx+CRandomMotherA.RanP1], xmm1           ; Store floating point number
        ret
        
;MotBRandom ENDP

        
; Single threaded version:
; extern "C" unsigned int MotherRandom();  // Get floating point random number

MotherRandom:
%IFDEF UNIX
        lea     rdi, [MotherInstance]         ; Point to instance
%ENDIF
%IFDEF WINDOWS
MotherRandomD:
        lea     rcx, [MotherInstance]         ; Point to instance
%ENDIF

; Thread-safe version:
; extern "C" double MotRandom(void * Pthis);  // Get floating point random number
MotRandom:
%IFDEF UNIX
        mov     rcx, rdi                                   ; translate calling convention
%ENDIF
        and     rcx, -16                    ; align
        ; get previously prepared random number
        movsd   xmm0, [rcx+CRandomMotherA.RanP1]
        subsd   xmm0, [rcx+CRandomMotherA.one]

        ; make new random number ready for next time
        call    ?Windows_MotBRandom
        ret
;MotherRandom ENDP


; Single threaded version:
; extern "C" unsigned int MotherIRandom(int min, int max); // Get integer random number in desired interval

MotherIRandom: ; PROC
%IFDEF UNIX
        mov     r8d, esi                    ; max
        mov     edx, edi                    ; min
        lea     rcx, [MotherInstance]       ; Pthis = point to instance
        jmp     ?Windows_MotIRandom
%ENDIF
%IFDEF WINDOWS
MotherIRandomD:
        mov     r8d, edx                    ; max
        mov     edx, ecx                    ; min
        lea     rcx, [MotherInstance]       ; Pthis = point to instance
        jmp     ?Windows_MotIRandom
%ENDIF
; MotherIRandom ENDP       

; Thread-safe version:
; extern "C" int MotIRandom(void * Pthis, int min, int max); // Get integer random number in desired interval
MotIRandom:
%IFDEF UNIX
        ; translate calling convention
        mov     r8d, edx                    ; max
        mov     edx, esi                    ; min
        mov     rcx, rdi                    ; Pthis
%ENDIF
        
?Windows_MotIRandom: ;   LABEL NEAR         ; entry for Windows call
        and     rcx, -16                    ; align
        push    r8
        push    rdx
        call    ?Windows_MotBRandom         ; make random number
        pop     rcx                         ; min
        pop     r8                          ; max
        sub     r8d, ecx
        js      short rerror                ; max < min
        inc     r8d                         ; interval = max - min + 1
        mul     r8d                         ; multiply random number eax by interval and truncate
        lea     eax, [rdx+rcx]              ; add min to interval*BRandom >> 32
        ret                                 ; ret 8 if not _cdecl calling

rerror: mov     eax, 80000000h              ; error exit   
        ret                                 ; ret 8 if not _cdecl calling
;MotIRandom ENDP


; Single threaded version:
; extern "C" unsigned int MotherRandomInit(int seed);  // Initialization

MotherRandomInit: ; PROC
%IFDEF UNIX
        mov     edx, edi                    ; seed
        lea     rcx, [MotherInstance]       ; Pthis = point to instance
        jmp     ?Windows_MotRandomInit
%ENDIF
%IFDEF WINDOWS
MotherRandomInitD:
        mov     edx, ecx                    ; seed
        lea     rcx, [MotherInstance]       ; Pthis = point to instance
        jmp     ?Windows_MotRandomInit
%ENDIF
;MotherRandomInit ENDP       

; Thread-safe version:
; extern "C" void MotRandomInit(void * Pthis, int seed);  // Initialization
MotRandomInit: ; PROC
%IFDEF UNIX
        ; translate calling convention
        mov     edx, esi                    ; seed
        mov     rcx, rdi                    ; Pthis
%ENDIF
        
?Windows_MotRandomInit: ;   LABEL NEAR         ; entry for Windows call
        and     rcx, -16                    ; align
        ; clear my buffer
        push    rdi
        push    rcx
        mov     rdi, rcx                    ; Pthis
        add     rdi, 16
        mov     ecx, (MotherSize - 16) / 4
        xor     eax, eax
        cld
        rep     stosd
        pop     rcx
        
        ; insert constants
        mov     dword [rcx+CRandomMotherA.one+4], 3FF00000H  ; high dword of 1.0       
        mov     dword [rcx+CRandomMotherA.MF0], 5115             ; factors
        mov     dword [rcx+CRandomMotherA.MF1], 1776
        mov     dword [rcx+CRandomMotherA.MF2], 1492
        mov     dword [rcx+CRandomMotherA.MF3], 2111111111
        
        ; initialize from seed
        mov     eax, edx                                   ; seed        
        ; make random numbers and put them into buffer
        mov     edx, 29943829
        imul    eax, edx
        dec     eax
        mov     [rcx+CRandomMotherA.M0], eax
        imul    eax, edx
        dec     eax
        mov     [rcx+CRandomMotherA.M1], eax
        imul    eax, edx
        dec     eax
        mov     [rcx+CRandomMotherA.M2], eax
        imul    eax, edx
        dec     eax
        mov     [rcx+CRandomMotherA.M3], eax
        imul    eax, edx
        dec     eax
        mov     [rcx+CRandomMotherA.MC], eax

        ; randomize some more
        mov     edi, 20                                    ; loop counter
r90:    call    ?Windows_MotBRandom                        ; (rcx and rdi unchanged)
        dec     edi
        jnz     r90
        pop     rdi
        ret
;MotRandomInit ENDP

 ;       END
