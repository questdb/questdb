; ----------------------------- MERSENNE32.ASM ---------------------------
; Author:           Agner Fog
; Date created:     1998
; Last modified:    2013-09-13
; Source URL:       www.agner.org/optimize
; Project:          asmlib.zip
; Language:         assembly, NASM/YASM syntax, 32 bit
; Description:
; Random Number generator 'Mersenne Twister' type MT11213A (or MT19937)
;
;  This random number generator is described in the article by
;  M. Matsumoto & T. Nishimura, in:
;  ACM Transactions on Modeling and Computer Simulation,
;  vol. 8, no. 1, 1998, pp. 3-30. See also:
;  http://www.math.sci.hiroshima-u.ac.jp/~m-mat/MT/emt.html
;
;  Initialization:
;  MersRandomInit must be called before the first call to any of the other
;  random number functions. The seed is any 32-bit integer.
;  You may use MersRandomInitByArray instead if you want more
;  than 32 bits for seed. length is the number of integers in seeds[].
;  length must be > 0, there is no upper limit for length.
;
;  Generating random numbers:
;  MersRandom returns a floating point number in the interval 0 <= x < 1 with
;  a resolution of 32 bits.
;  MersIRandom returns an integer in the interval defined by min and max with
;  a resolution of 32 bits.
;  MersIRandomX returns an integer in the interval defined by min and max with
;  exactly equal probabilities of all values in the interval.
;  MersBRandom returns 32 random bits.
;
;  Error conditions:
;  If MersRandomInit or MersRandomInitByArray has not been called then MersRandom
;  and MersBRandom keep returning 0, and MersIRandom and MersIRandomX return min.
;  MersIRandom and MersIRandomX return a large negative number if max < min.
;
;  C++ prototypes in randoma.h, 32-bit Windows:
;
;  Thread-safe static link versions for Mersenne Twister
;  extern "C" void   MersRandomInit(void * Pthis, int seed);         // Re-seed
;  extern "C" void   MersRandomInitByArray(void * Pthis, unsigned int seeds[], int length); // Seed by more than 32 bits
;  extern "C" int    MersIRandom (void * Pthis, int min, int max);   // Output random integer
;  extern "C" int    MersIRandomX(void * Pthis, int min, int max);   // Output random integer, exact
;  extern "C" double MersRandom(void * Pthis);                       // Output random float
;  extern "C" unsigned int MersBRandom(void * Pthis);                // Output random bits
;
;  Single-threaded static link versions for Mersenne Twister, Windows only
;  extern "C" void   MersenneRandomInit(int seed);                   // Re-seed
;  extern "C" void   MersenneRandomInitByArray(unsigned int seeds[], int length); // Seed by more than 32 bits
;  extern "C" int    MersenneIRandom (int min, int max);             // Output random integer
;  extern "C" int    MersenneIRandomX(int min, int max);             // Output random integer, exact
;  extern "C" double MersenneRandom();                               // Output random float
;  extern "C" unsigned int MersenneBRandom();                        // Output random bits
;
;  Single threaded dynamic link versions for Mersenne Twister, Windows only
;  extern "C" void   __stdcall MersenneRandomInitD(int seed);              // Re-seed
;  extern "C" void   __stdcall MersenneRandomInitByArrayD(unsigned int seeds[], int length); // Seed by more than 32 bits
;  extern "C" int    __stdcall MersenneIRandomD (int min, int max);  // Output random integer
;  extern "C" int    __stdcall MersenneIRandomXD(int min, int max);  // Output random integer, exact
;  extern "C" double __stdcall MersenneRandomD();                    // Output random float
;  extern "C" unsigned int __stdcall MersenneBRandomD();             // Output random bits
;
;
; Copyright (c) 2008-2013 GNU General Public License www.gnu.org/licenses
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

; structure definition and constants:
%INCLUDE "asm/randomah.asi"

global _MersRandomInit, _MersRandomInitByArray
global _MersBRandom, _MersRandom, _MersIRandom, _MersIRandomX
global _MersenneRandomInitByArray,_MersenneRandomInit
global _MersenneRandom, _MersenneIRandom, _MersenneIRandomX, _MersenneBRandom
%IFDEF WINDOWS
global _MersenneRandomInitByArrayD@8, _MersenneRandomInitD@4
global _MersenneRandomD@0, _MersenneIRandomD@8, _MersenneIRandomXD@8, _MersenneBRandomD@0
%ENDIF


SECTION .data
align 16
; Data for single instance of random number generator
MersenneInstance: 
ISTRUC CRandomMersenneA
IEND
; Size of structure
MersenneSize equ $ - MersenneInstance


SECTION .CODE ALIGN=16

extern _InstructionSet


; ---------------------------------------------------------------
;  Thread-safe static link versions for Mersenne Twister
; ---------------------------------------------------------------

;  extern "C" void MersRandomInit(void * Pthis, int seed); // Re-seed

_MersRandomInit: ; PROC NEAR
        mov     ecx, [esp+4]                               ; Pthis
        mov     eax, [esp+8]                               ; seed
        and     ecx, -16                                   ; align buffer
        
MersRandomInit_reg:                                        ; Entry for register parameters, used internally
        call    Mers_init0                                 ; initialize mt buffer with seeds
        
        ; Number of premade numbers that are lost in the initialization when the  
        ; SSE2 implementation makes up to 4 premade numbers at a time:
%IF MERS_N & 3        
   PREMADELOST equ (MERS_N & 3)
%ELSE
   PREMADELOST equ 4
%ENDIF
        ; We want the C++ and the assembly implementation to give exactly the same
        ; sequence. The C++ version discards 37 random numbers after initialization.
        ; The assembly version generates a sequence that is PREMADELOST + 1 numbers
        ; behind. Therefore we discard the first 37 + PREMADELOST + 1 numbers if
        ; SSE2 is supported, otherwise 37 + 1.
        
        push    edi
        mov     edi, 37+PREMADELOST+1
        cmp     dword [ecx+CRandomMersenneA.Instset], 4    ; can we use XMM registers and SSE2 ?
        jae     M110
        sub     edi, PREMADELOST       ; SSE2 not supported
        mov     dword [ecx+CRandomMersenneA.PreInx], 0     ; reset index to premade list
M110:   ; loop
M120:   call    MersBRandom_reg
        dec     edi
        jnz     M120
        pop     edi
        ret
;_MersRandomInit ENDP
        

Mers_init0:                                                ; make random seeds from eax and put them into MT buffer
; Input parameters: 
; eax: seed
; ecx points to CRandomMersenneA

        push    ebx
        push    edi
        mov     ebx, eax                                   ; seed
        
        ; clear my buffer
        push    ecx
        mov     edi, ecx                                   ; Pthis
        add     edi, 16                                    ; skip alignment filler
        mov     ecx, (MersenneSize - 16) / 4
        xor     eax, eax
        cld
        rep     stosd
        pop     ecx                                        ; Pthis
        
        ; initialize CRandomMersenneA structure
        mov     dword [ecx+CRandomMersenneA.PreInx], 4*4
        push    ecx
        call    _InstructionSet                            ; detect instruction set
        pop     ecx
        mov     [ecx+CRandomMersenneA.Instset], eax
        mov     eax, MERS_B
        mov     [ecx+CRandomMersenneA.TMB], eax
        mov     [ecx+CRandomMersenneA.TMB+4], eax
        mov     [ecx+CRandomMersenneA.TMB+8], eax
        mov     [ecx+CRandomMersenneA.TMB+12], eax
        mov     eax, MERS_C
        mov     [ecx+CRandomMersenneA.TMC], eax
        mov     [ecx+CRandomMersenneA.TMC+4], eax
        mov     [ecx+CRandomMersenneA.TMC+8], eax
        mov     [ecx+CRandomMersenneA.TMC+12], eax
        mov     eax, 3FF00000H                             ; upper dword of 1.0, double precision
        mov     dword [ecx+CRandomMersenneA.one+4], eax
        mov     dword [ecx+CRandomMersenneA.one+12], eax        
        mov     dword [ecx+CRandomMersenneA.LMASK], LOWER_MASK
        mov     dword [ecx+CRandomMersenneA.UMASK], UPPER_MASK
        mov     dword [ecx+CRandomMersenneA.MATA],  MERS_A

        ; put random numbers into MT buffer
        xor     edi, edi        
M210:   mov     [ecx+edi*4+CRandomMersenneA.MT], ebx
        mov     edx, ebx
        shr     ebx, 30
        xor     ebx, edx
        imul    ebx, 1812433253
        inc     edi
        add     ebx, edi        
        cmp     edi, MERS_N
        jb      M210
        
        ; Set index MTI to end of list, (scaled by 4)
        ; Round up to multiple of 4 to avoid alignment error
        mov     dword [ecx+CRandomMersenneA.MTI], ((MERS_N+3) & -4) * 4
        
        pop     edi
        pop     ebx       
        ret      
;Mers_init0   ENDP


;  extern "C" void   MersRandomInitByArray(void * Pthis, unsigned int seeds[], int length); // Seed by more than 32 bits
_MersRandomInitByArray: ; PROC NEAR

        push    ebx
        push    esi
        push    edi
        push    ebp
        mov     ecx, [esp+20]                              ; Pthis
        mov     ebx, [esp+24]                              ; seeds
        mov     ebp, [esp+28]                              ; length
        and     ecx, -16                                   ; align buffer
        
MersRandomInitByArray_reg:                                 ; Entry for register parameters, used internally

        push    ebp                                        ; save length
        mov     eax, 19650218
        call    Mers_init0                                 ; init0(19650218);
        
        test    ebp, ebp
        jle     M380                                       ; error: length <= 0
        xor     edi, edi                                   ; j = 0
        lea     esi, [edi+1]                               ; i = 1
        cmp     ebp, MERS_N
        ja      M310
        mov     ebp, MERS_N                                ; k = max (MERS_N,length)
M310:                                                    

        ; for (; k; k--) {
M320:   mov     eax, [ecx+esi*4-4+CRandomMersenneA.MT]     ; mt[i-1]
        mov     edx, eax
        shr     eax, 30
        xor     eax, edx                                   ; mt[i-1] ^ (mt[i-1] >> 30)
        imul    eax, 1664525                               ; * 1664525
        xor     eax, [ecx+esi*4+CRandomMersenneA.MT]       ; ^ mt[i]
        add     eax, [ebx+edi*4]                           ; + seeds[j]
        add     eax, edi                                   ; + j
        mov     [ecx+esi*4+CRandomMersenneA.MT], eax       ; save in mt[i]
        inc     esi                                        ; i++
        inc     edi                                        ; j++
        cmp     esi, MERS_N
        jb      M330                                       ; if (i>=MERS_N)
        mov     eax, [ecx+(MERS_N-1)*4+CRandomMersenneA.MT]; mt[0] = mt[MERS_N-1];
        mov     [ecx+CRandomMersenneA.MT], eax
        mov     esi, 1                                     ; i=1;
M330:
        cmp     edi, [esp]                                 ; length
        jb      M340          ; if (j>=length)
        xor     edi, edi                                   ; j = 0;
M340:
        dec     ebp                                        ; k--
        jnz     M320                                       ; first k loop
M350:
        mov     ebp, MERS_N-1                              ; k
M360:   mov     eax, [ecx+esi*4-4+CRandomMersenneA.MT]     ; mt[i-1]
        mov     edx, eax
        shr     eax, 30
        xor     eax, edx                                   ; mt[i-1] ^ (mt[i-1] >> 30)
        imul    eax, 1566083941                            ; * 1566083941
        xor     eax, [ecx+esi*4+CRandomMersenneA.MT]       ; ^ mt[i]
        sub     eax, esi                                   ; - i
        mov     [ecx+esi*4+CRandomMersenneA.MT], eax       ; save in mt[i]
        inc     esi                                        ; i++
        cmp     esi, MERS_N
        jb      M370                                       ; if (i>=MERS_N)
        mov     eax, [ecx+(MERS_N-1)*4+CRandomMersenneA.MT]; mt[0] = mt[MERS_N-1];
        mov     [ecx+CRandomMersenneA.MT], eax
        mov     esi, 1                                     ; i=1;
M370:
        dec     ebp                                        ; k--
        jnz     M360                                       ; second k loop
        mov     dword [ecx+CRandomMersenneA.MT], 80000000H ; mt[0] = 0x80000000
M380:
        mov     dword [ecx+CRandomMersenneA.MTI], 0
        mov     dword [ecx+CRandomMersenneA.PreInx], 0

; discard first MERS_N random numbers + PREMADELOST+1 to compensate for lag
        mov     edi, MERS_N + PREMADELOST+1
        CMP     dword [ecx+CRandomMersenneA.Instset], 4    ; can we use XMM registers and SSE2 ?
        jae     M390
        sub     edi, PREMADELOST                           ; SSE2 not supported
        mov     dword [ecx+CRandomMersenneA.PreInx], 0     ; reset index to premade list
M390:   ; loop
M391:   call    MersBRandom_reg
        dec     edi
        jnz     M391

        pop     ecx                                        ; remove local copy of length
        pop     ebp                                        ; restore registers
        pop     edi
        pop     esi
        pop     ebx
        ret
;_MersRandomInitByArray ENDP

;  extern "C" unsigned int MersBRandom(void * Pthis);      // Output random bits

_MersBRandom: ; PROC NEAR                                  ; generate random bits
        mov     ecx, [esp+4]                               ; Pthis
        and     ecx, -16                                   ; align buffer

MersBRandom_reg:                                           ; Entry for register parameters, used internally

        cmp     dword [ecx+CRandomMersenneA.Instset], 4    ; can we use XMM registers and SSE2 ?
        jb      M500

        ; this version uses XMM registers and SSE2 instructions:
        mov     edx, [ecx+CRandomMersenneA.PreInx]         ; index into premade numbers
        mov     eax, [ecx+edx*1+CRandomMersenneA.PreInt]   ; fetch premade random number
        add     edx, 4
        mov     [ecx+CRandomMersenneA.PreInx], edx
        cmp     edx, 4*4
        jnb     M410
        ret                                                ; return premade number

M410:
; PREMADE list is empty. Make 4 more numbers ready for next call:
        mov     edx, [ecx+CRandomMersenneA.MTI]            ; fetch 4 numbers from MT buffer
        movdqa  xmm0, oword [ecx+edx*1+CRandomMersenneA.MT]
        
%IF TEMPERING                                              ; optional tempering algorithm
        movdqa  xmm1, xmm0
        psrld   xmm0, MERS_U
        pxor    xmm0, xmm1
        movdqa  xmm1, xmm0        
        pslld   xmm0, MERS_S
        pand    xmm0, oword [ecx+CRandomMersenneA.TMB]
        pxor    xmm0, xmm1
        movdqa  xmm1, xmm0        
        pslld   xmm0, MERS_T
        pand    xmm0, oword [ecx+CRandomMersenneA.TMC]
        pxor    xmm0, xmm1
        movdqa  xmm1, xmm0        
        psrld   xmm0, MERS_L
        pxor    xmm0, xmm1
%ENDIF   ; tempering

        ; save four premade integers
        movdqa  oword [ecx+CRandomMersenneA.PreInt], xmm0
        ; premake four floating point numbers
        pxor    xmm1, xmm1
        pxor    xmm2, xmm2
        punpckldq xmm1, xmm0                               ; get first two numbers into bits 32-63 and 96-127
        punpckhdq xmm2, xmm0                               ; get next  two numbers into bits 32-63 and 96-127
        psrlq   xmm1, 12                                   ; get bits into mantissa position
        psrlq   xmm2, 12                                   ; get bits into mantissa position
        por     xmm1, oword [ecx+CRandomMersenneA.one]     ; set exponent for interval [1,2)
        por     xmm2, oword [ecx+CRandomMersenneA.one]     ; set exponent for interval [1,2)
        movdqa  oword [ecx+CRandomMersenneA.PreFlt], xmm1  ; store two premade numbers
        movdqa  oword [ecx+CRandomMersenneA.PreFlt+16],xmm2; store two more premade numbers        
        mov     dword [ecx+CRandomMersenneA.PreInx], 0     ; index to premade numbers 
        add     edx, 4*4                                   ; increment MTI index into MT buffer by 4
        mov     [ecx+CRandomMersenneA.MTI], edx
        cmp     edx, MERS_N*4
        jae     M420
        ret                                                ; return random number in eax

; MT buffer exhausted. Make MERS_N new numbers ready for next time
M420:                                                      ; eax is the random number to return
%IF     MERS_N & 3                                         ; if MERS_N is not divisible by 4
        NVALID = MERS_N & 3                                ; only NVALID of the 4 premade numbers are valid
        ; Move premade numbers (4-NVALID) positions forward
        movdqa  xmm0, [ecx+CRandomMersenneA.PreInt]
        movdqa  xmm1, [ecx+CRandomMersenneA.PreFlt]
        movdqa  xmm2, [ecx+CRandomMersenneA.PreFlt+16]
        movdqu  [ecx+CRandomMersenneA.PreInt + (4-NVALID)*4], xmm0
        movdqu  [ecx+CRandomMersenneA.PreFlt + (4-NVALID)*8], xmm1
%IF NVALID == 3        
        movq    [ecx+CRandomMersenneA.PreFlt+16 + 8], xmm2
%ENDIF        
        ; save index to first valid premade number
        mov     [ecx+CRandomMersenneA.PreInx], (4-NVALID)*4  
%ENDIF
        
        ; MT buffer is empty. Fill it up
        push    ebx
        movd    xmm3, [ecx+CRandomMersenneA.UMASK]         ; load constants
        movd    xmm4, [ecx+CRandomMersenneA.LMASK]
        movd    xmm5, [ecx+CRandomMersenneA.MATA]
        pshufd  xmm3, xmm3, 0                              ; broadcast constants
        pshufd  xmm4, xmm4, 0
        pshufd  xmm5, xmm5, 0
        xor     ebx,  ebx                                  ; kk = 0
        mov     edx,  MERS_M*4                             ; km
        
; change ecx from pointing to CRandomMersenneA to pointing to CRandomMersenneA.MT
        add     ecx, CRandomMersenneA.MT

M430:   ; kk loop
        movdqa  xmm2, oword [ecx+ebx]                      ; mt[kk]
        movd    xmm6, [ecx+ebx+16]
        movdqa  xmm1, oword [ecx+ebx]                      ; mt[kk]        
        movss   xmm2, xmm6                                 ; faster than movdqu xmm2, [ebx+4] ?
        pshufd  xmm2, xmm2, 00111001B                      ; mt[kk+1]        
        movdqu  xmm0, oword [ecx+edx]                      ; mt[km]
        ;movq   xmm0, qword [ecx+edx]                      ; mt[km]
        ;movhps xmm0, qword [ecx+edx+8]                    ; this is faster than movdqu on older processors
        pand    xmm1, xmm3                                 ; mt[kk] & UPPER_MASK
        pand    xmm2, xmm4                                 ; mt[kk+1] & LOWER_MASK
        por     xmm1, xmm2                                 ; y        
        movdqa  xmm2, xmm1                                 ; y
        pslld   xmm1, 31                                   ; copy bit 0 into all bits
        psrad   xmm1, 31                                   ; -(y & 1)
        pand    xmm1, xmm5                                 ; & MERS_A
        psrld   xmm2, 1                                    ; y >> 1
        pxor    xmm0, xmm1
        pxor    xmm0, xmm2
        movdqa  oword [ecx+ebx], xmm0                      ; result into mt[kk]
        cmp     ebx, (MERS_N-4)*4
        jae     M440                                       ; exit loop when kk past end of buffer
        add     ebx, 16                                    ; kk += 4
        add     edx, 16                                    ; km += 4
        cmp     edx, (MERS_N-4)*4
        jbe     M430                                       ; skip unless km wraparound
        sub     edx, MERS_N*4                              ; km wraparound
        movdqu  xmm0, oword [ecx+(MERS_N-4)*4]             ; copy end to before begin for km wraparound
        movdqa  oword [ecx-4*4], xmm0        
        movdqa  xmm0, oword [ecx]                          ; copy begin to after end for kk wraparound
        movdqu  oword [ecx+MERS_N*4], xmm0
        jmp     M430

M440:   ; loop finished. discard excess part of last result

; change ecx back to pointing to CRandomMersenneA
        sub     ecx, CRandomMersenneA.MT        
        mov     dword [ecx+CRandomMersenneA.MTI], 0
        pop     ebx
        ret                                                ; random number is still in eax
        
; Generic version        
; this version is for old processors without XMM support:
M500:
        mov     edx, [ecx+CRandomMersenneA.MTI]
        cmp     edx, MERS_N*4
        jnb     short M520                                 ; buffer is empty, fill it   
M510:   mov     eax, [ecx+edx*1+CRandomMersenneA.MT]
        add     edx, 4
        mov     [ecx+CRandomMersenneA.MTI], edx
        
%IF TEMPERING   
        mov     edx, eax
        shr     eax, MERS_U
        xor     eax, edx
        mov     edx, eax
        shl     eax, MERS_S
        and     eax, MERS_B
        xor     eax, edx
        mov     edx, eax
        shl     eax, MERS_T
        and     eax, MERS_C
        xor     eax, edx
        mov     edx, eax
        shr     eax, MERS_L
        xor     eax, edx
%ENDIF   ; tempering

        mov     edx, [ecx+CRandomMersenneA.PreInt]         ; previously premade number
        mov     [ecx+CRandomMersenneA.PreInt], eax         ; store number for next call
        shl     eax, 20                                    ; convert to float
        mov     dword [ecx+CRandomMersenneA.PreFlt], eax
        mov     eax, [ecx+CRandomMersenneA.PreInt]
        shr     eax, 12
        or      eax, 3FF00000H
        mov     dword [ecx+CRandomMersenneA.PreFlt+4], eax
        mov     eax, edx                                   ; return value is premade integer
        ret

        ; fill buffer with random numbers
M520:   push    ebx
        push    esi
        xor     esi, esi                                   ; kk
        mov     ebx, MERS_M*4                              ; km
; change ecx from pointing to CRandomMersenneA to pointing to CRandomMersenneA.MT
        add     ecx, CRandomMersenneA.MT
        
        ; kk loop
M530:   mov     eax, [ecx+esi]
        mov     edx, [ecx+esi+4]
        and     eax, UPPER_MASK
        and     edx, LOWER_MASK
        or      eax, edx
        shr     eax, 1
        sbb     edx, edx
        and     edx, MERS_A
        xor     eax, edx
        xor     eax, [ecx+ebx]
        mov     [ecx+esi], eax
        add     ebx, 4
        cmp     ebx, MERS_N*4
        jb      short M540
        ; copy begin of table to after end to simplify kk+1 wraparound
        mov     eax, [ecx]
        mov     [ecx+ebx], eax 
        xor     ebx, ebx
M540:   add     esi, 4
        cmp     esi, MERS_N*4
        jb      M530                                       ; loop end        
        
; change ecx back to pointing to CRandomMersenneA
        sub     ecx, CRandomMersenneA.MT        
        xor     edx, edx
        mov     [ecx+CRandomMersenneA.MTI], edx
        pop     esi
        pop     ebx
        jmp     M510        
        
;_MersBRandom ENDP

;  extern "C" double MersRandom(void * Pthis); // Output random float

_MersRandom:; PROC NEAR                                    ; generate random float with 32 bits resolution
        mov     ecx, [esp+4]                               ; Pthis
        and     ecx, -16                                   ; align buffer
        mov     edx, [ecx+CRandomMersenneA.PreInx]         ; index into premade numbers
        fld     qword [ecx+edx*2+CRandomMersenneA.PreFlt]  ; fetch premade floating point random number
        fsub    qword [ecx+CRandomMersenneA.one]           ; subtract 1.0
        jmp     MersBRandom_reg                            ; random bits
;_MersRandom ENDP


;  extern "C" int MersIRandom (void * Pthis, int min, int max);  // Output random integer

_MersIRandom: ; PROC   NEAR
        mov     ecx, [esp+4]                               ; Pthis
        and     ecx, -16                                   ; align buffer
        call    MersBRandom_reg                            ; random bits
        mov     edx, [esp+12]                              ; max
        mov     ecx, [esp+8]                               ; min
        sub     edx, ecx
        js      short M720                                 ; max < min
        add     edx, 1                                     ; max - min + 1
        mul     edx                                        ; multiply random number by interval and truncate
        lea     eax, [edx+ecx]                             ; add min
        ret
M720:   mov     eax, 80000000H                             ; error exit
        ret
;_MersIRandom ENDP


;  extern "C" int MersIRandomX (void * Pthis, int min, int max);        // Output random integer

_MersIRandomX: ; PROC   NEAR
        push    edi
        mov     ecx, [esp+8]                               ; Pthis
        mov     edx, [esp+12]                              ; min
        mov     edi, [esp+16]                              ; max
        and     ecx, -16                                   ; align buffer
        sub     edi, edx                                   ; max - min
        jle     short M830                                 ; max <= min (signed)
        inc     edi                                        ; interval = max - min + 1
        
        ; if (interval != LastInterval) {
        cmp     edi, [ecx+CRandomMersenneA.LastInterval]
        je      M810
        ; RLimit = uint32(((uint64)1 << 32) / interval) * interval - 1;}
        xor     eax, eax                                   ; 0
        lea     edx, [eax+1]                               ; 1
        div     edi                                        ; (would give overflow if interval = 1)
        mul     edi
        dec     eax
        mov     [ecx+CRandomMersenneA.RLimit], eax
        mov     [ecx+CRandomMersenneA.LastInterval], edi
M810:
M820:   ; do { // Rejection loop
        call    MersBRandom_reg                            ; random bits (ecx is preserved)
        ; longran  = (uint64)BRandom() * interval;
        mul     edi
        ; } while (remainder > RLimit);
        cmp     eax, [ecx+CRandomMersenneA.RLimit]
        ja      M820
        
        ; return (int32)iran + min
        mov     eax, [esp+12]                              ; min
        add     eax, edx
        pop     edi
        ret
        
M830:   jl      M840
        ; max = min. Return min
        mov     eax, edx
        pop     edi
        ret                                                ; max = min exit
        
M840:   ; max < min: error
        mov     eax, 80000000H                             ; error exit
        pop     edi
        ret
;_MersIRandomX ENDP


; -------------------------------------------------------------------------
;  Single-threaded static link versions of Mersenne Twister
; -------------------------------------------------------------------------

%IFDEF POSITIONINDEPENDENT
; Get ecx = eip for self-relative addressing
GetThunkECX:
        mov     ecx, [esp]
        ret
        
; Get address of MersenneInstance into ecx, position independent
; This works only in YASM, not in NASM:
%macro GetMersenneInstanceAddress  0
        call    GetThunkECX
        add     ecx, MersenneInstance - $
%endmacro

%ELSE

; Get address of MersenneInstance into ecx, position dependent
; This works only in YASM, not in NASM:
%macro GetMersenneInstanceAddress  0
        mov     ecx, MersenneInstance
%endmacro

%ENDIF

;  extern "C" void   MersenneRandomInitByArray(unsigned int seeds[], int length); // Seed by more than 32 bits
_MersenneRandomInitByArray: ; PROC NEAR
        push    ebx
        push    esi
        push    edi
        push    ebp
        mov     ebx, [esp+20]                              ; seeds
        mov     ebp, [esp+24]                              ; length
        GetMersenneInstanceAddress                         ; Macro different for position-dependent and -independent version
        jmp     MersRandomInitByArray_reg                  ; jump to function in mersenne32.asm
;_MersenneRandomInitByArray ENDP        


;  extern "C" void   MersenneRandomInit(int seed);  // Re-seed
_MersenneRandomInit: ; PROC NEAR
        mov     eax, [esp+4]                               ; seed
        GetMersenneInstanceAddress
        jmp     MersRandomInit_reg                         ; jump to function in mersenne32.asm
;_MersenneRandomInit ENDP


;  extern "C" double MersenneRandom(); // Output random float
_MersenneRandom: ; PROC NEAR                               ; generate random float with 32 bits resolution
        GetMersenneInstanceAddress
        mov     edx, [ecx+CRandomMersenneA.PreInx]         ; index into premade numbers
        fld     qword [ecx+edx*2+CRandomMersenneA.PreFlt]  ; fetch premade floating point random number
        fsub    qword [ecx+CRandomMersenneA.one]           ; subtract 1.0
        jmp     MersBRandom_reg                            ; random bits
;_MersenneRandom ENDP


;  extern "C" int MersenneIRandom (int min, int max); // Output random integer
_MersenneIRandom: ; PROC   NEAR
        GetMersenneInstanceAddress
        call    MersBRandom_reg                            ; random bits
        mov     edx, [esp+8]                               ; max
        mov     ecx, [esp+4]                               ; min
        sub     edx, ecx
        js      short S410                                 ; max < min
        add     edx, 1                                     ; max - min + 1
        mul     edx                                        ; multiply random number by interval and truncate
        lea     eax, [edx+ecx]                             ; add min
        ret
S410:   mov     eax, 80000000H                             ; error exit
        ret
;_MersenneIRandom ENDP


;  extern "C" int MersenneIRandomX(int min, int max); // Output random integer, exact

_MersenneIRandomX: ; PROC   NEAR
        push    edi
        GetMersenneInstanceAddress
        mov     edx, [esp+8]                               ; min
        mov     edi, [esp+12]                              ; max
        sub     edi, edx                                   ; max - min
        jle     short S530                                 ; max <= min (signed)
        inc     edi                                        ; interval = max - min + 1
        cmp     edi, [ecx+CRandomMersenneA.LastInterval]
        je      S510
        xor     eax, eax                                   ; 0
        lea     edx, [eax+1]                               ; 1
        div     edi                                        ; (would give overflow if interval = 1)
        mul     edi
        dec     eax
        mov     [ecx+CRandomMersenneA.RLimit], eax
        mov     [ecx+CRandomMersenneA.LastInterval], edi
S510:
S520:   call    MersBRandom_reg                            ; random bits (ecx is preserved)
        mul     edi
        cmp     eax, [ecx+CRandomMersenneA.RLimit]
        ja      S520        
        mov     eax, [esp+8]                               ; min
        add     eax, edx
        pop     edi
        ret     
        
S530:   jl      S540
        ; max = min. Return min
        mov     eax, edx
        pop     edi
        ret                                                ; max = min exit
        
S540:   ; max < min: error
        mov     eax, 80000000H                             ; error exit
        pop     edi
        ret     
;_MersenneIRandomX ENDP


;  extern "C" unsigned int MersenneBRandom();              // Output random bits
_MersenneBRandom: ; PROC NEAR                              ; generate random float with 32 bits resolution
        GetMersenneInstanceAddress
        jmp     MersBRandom_reg                            ; random bits
;_MersenneBRandom ENDP


; -----------------------------------------------------------------
;  Single-threaded DLL versions for Mersenne Twister, Windows only
; -----------------------------------------------------------------
%IFDEF WINDOWS

;  extern "C" void __stdcall MersenneRandomInitByArrayD(unsigned int seeds[], int length); // Seed by more than 32 bits
_MersenneRandomInitByArrayD@8: ; PROC NEAR
        ; translate __cdecl to __stdcall calling
        mov     eax, [esp+4]                               ; seeds
        mov     edx, [esp+8]                               ; length
        push    edx                                       
        push    eax
        call    _MersenneRandomInitByArray
        pop     ecx
        pop     ecx
        ret     8
;_MersenneRandomInitByArrayD@8 ENDP        


;  extern "C" void __stdcall MersenneRandomInitD(int seed); // Re-seed
_MersenneRandomInitD@4: ; PROC NEAR
        ; remove parameter from stack
        pop     edx                                        ; return address
        pop     eax                                        ; seed
        push    edx                                        ; put return address back in        
        mov     ecx, MersenneInstance
        ; eax = seed, ecx = Pthis
        jmp     MersRandomInit_reg                         ; jump to function in mersenne32.asm
;_MersenneRandomInitD@4 ENDP


;  extern "C" double __stdcall MersenneRandomD(); // Output random float
_MersenneRandomD@0: ; PROC NEAR                            ; generate random float with 32 bits resolution
        mov     ecx, MersenneInstance
        mov     edx, [ecx+CRandomMersenneA.PreInx]         ; index into premade numbers
        fld     qword [ecx+edx*2+CRandomMersenneA.PreFlt]  ; fetch premade floating point random number
        fsub    qword [ecx+CRandomMersenneA.one]           ; subtract 1.0
        jmp     MersBRandom_reg                            ; random bits
;_MersenneRandomD@0 ENDP


;  extern "C" int __stdcall MersenneIRandomD (int min, int max); // Output random integer
_MersenneIRandomD@8: ; PROC   NEAR
        mov     ecx, MersenneInstance
        call    MersBRandom_reg                            ; random bits
        mov     edx, [esp+8]                               ; max
        mov     ecx, [esp+4]                               ; min
        sub     edx, ecx
        js      short S710                                 ; max < min
        add     edx, 1                                     ; max - min + 1
        mul     edx                                        ; multiply random number by interval and truncate
        lea     eax, [edx+ecx]                             ; add min
        ret     8
S710:   mov     eax, 80000000H                             ; error exit
        ret     8
;_MersenneIRandomD@8 ENDP


;  extern "C" int __stdcall MersenneIRandomXD(int min, int max); // Output random integer, exact

_MersenneIRandomXD@8: ; PROC   NEAR
        push    edi
        mov     ecx, MersenneInstance
        mov     edx, [esp+8]                               ; min
        mov     edi, [esp+12]                              ; max
        sub     edi, edx                                   ; max - min
        jle     short S830                                 ; max <= min (signed)
        inc     edi                                        ; interval = max - min + 1
        cmp     edi, [ecx+CRandomMersenneA.LastInterval]
        je      S810
        xor     eax, eax                                   ; 0
        lea     edx, [eax+1]                               ; 1
        div     edi                                        ; (would give overflow if interval = 1)
        mul     edi
        dec     eax
        mov     [ecx+CRandomMersenneA.RLimit], eax
        mov     [ecx+CRandomMersenneA.LastInterval], edi
S810:
S820:   call    MersBRandom_reg                            ; random bits (ecx is preserved)
        mul     edi
        cmp     eax, [ecx+CRandomMersenneA.RLimit]
        ja      S820        
        mov     eax, [esp+8]                               ; min
        add     eax, edx
        pop     edi
        ret     8
        
S830:   jl      S840
        ; max = min. Return min
        mov     eax, edx
        pop     edi
        ret     8                                          ; max = min exit
        
S840:   ; max < min: error
        mov     eax, 80000000H                             ; error exit
        pop     edi
        ret     8
;_MersenneIRandomXD@8 ENDP


;  extern "C" unsigned int __stdcall MersenneBRandomD();   // Output random bits
_MersenneBRandomD@0: ; PROC NEAR                           ; generate random float with 32 bits resolution
        mov     ecx, MersenneInstance
        jmp     MersBRandom_reg                            ; random bits
;_MersenneBRandomD@0 ENDP

%ENDIF ; WINDOWS
