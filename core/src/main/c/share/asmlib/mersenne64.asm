; ----------------------------- MERSENNE64.ASM ---------------------------
; Author:           Agner Fog
; Date created:     1998
; Last modified:    2013-09-13
; Source URL:       www.agner.org/optimize
; Project:          asmlib.zip
; Language:         assembly, NASM/YASM syntax, 64 bit
; Description:
; Random Number generator 'Mersenne Twister' type MT11213A (or MT19937)
;
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
;  C++ prototypes in randoma.h:
;  Thread-safe versions:
;  extern "C" void   MersRandomInit(void * Pthis, int seed);         // Re-seed
;  extern "C" void   MersRandomInitByArray(void * Pthis, unsigned int seeds[], int length); // Seed by more than 32 bits
;  extern "C" int    MersIRandom (void * Pthis, int min, int max);   // Output random integer
;  extern "C" int    MersIRandomX(void * Pthis, int min, int max);   // Output random integer, exact
;  extern "C" double MersRandom(void * Pthis);                       // Output random float
;  extern "C" unsigned int MersBRandom(void * Pthis);                // Output random bits
;
;  Single-threaded versions:
;  extern "C" void   MersenneRandomInit(int seed);                   // Re-seed
;  extern "C" void   MersenneRandomInitByArray(unsigned int seeds[], int length); // Seed by more than 32 bits
;  extern "C" int    MersenneIRandom (int min, int max);             // Output random integer
;  extern "C" int    MersenneIRandomX(int min, int max);             // Output random integer, exact
;  extern "C" double MersenneRandom();                               // Output random float
;  extern "C" unsigned int MersenneBRandom();                        // Output random bits
;
; Copyright (c) 2008-2013 GNU General Public License www.gnu.org/licenses
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

default rel

; structure definition and constants:
%INCLUDE "randomah.asi"

global MersenneRandomInit, MersenneRandomInitD, MersRandomInit
global MersenneRandomInitByArray, MersenneRandomInitByArrayD, MersRandomInitByArray
global MersenneBRandom, MersenneBRandomD, MersBRandom
global MersenneRandom, MersenneRandomD, MersRandom
global MersenneIRandom, MersenneIRandomD, MersIRandom
global MersenneIRandomX, MersenneIRandomXD, MersIRandomX


section .data
align 16

; Data for single instance of random number generator
MersenneInstance: ISTRUC CRandomMersenneA
IEND
; Size of structure
MersenneSize equ $ - MersenneInstance


SECTION .CODE  ALIGN=16

MersenneRandomInit: ; PROC
%IFDEF UNIX
        mov     edx, edi                                   ; seed
        lea     rcx, [MersenneInstance]                    ; Pthis = point to instance
        jmp     ?Windows_MersRandomInit
%ENDIF
%IFDEF WINDOWS
MersenneRandomInitD:                                       ; alias
        mov     edx, ecx                                   ; seed
        lea     rcx, [MersenneInstance]                    ; Pthis = point to instance
        ;jmp     ?Windows_MersRandomInit
%ENDIF
;MersenneRandomInit ENDP

        
; Thread-safe version:
;  extern "C" void MersRandomInit(void * Pthis, int seed); // Re-seed
MersRandomInit: ;   PROC
%IFDEF UNIX
        ; translate calling convention
        mov     edx, esi                                   ; seed
        mov     rcx, rdi                                   ; Pthis
%ENDIF
        ; parameters: rcx = Pthis, edx = seed
        and     rcx, -16                                   ; align buffer
        ?Windows_MersRandomInit:
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
        
        push    rbx
        mov     ebx, 37+PREMADELOST+1
        ; CMP     dword [rcx+CRandomMersenneA.Instset], 4  ; can we use XMM registers and SSE2 ?
        ; jae     M110
        ; sub     ebx, PREMADELOST                         ; SSE2 not supported
        ; mov     dword [rcx+CRandomMersenneA.PreInx], 0   ; reset index to premade list
M110:   ; loop
M120:   call    ?Windows_MersBRandom
        dec     ebx
        jnz     M120
        pop     rbx
        ret
;MersRandomInit ENDP
        

Mers_init0:                                                ; make random seeds from eax and put them into MT buffer
; Input parameters: 
; rcx points to CRandomMersenneA
; edx: seed
; rcx unchanged by procedure

        push    rdi
        ; clear my buffer
        push    rcx
        mov     rdi, rcx                                   ; Pthis
        add     rdi, 16
        mov     ecx, (MersenneSize - 16) / 4
        xor     eax, eax
        cld
        rep     stosd
        pop     rcx                                        ; Pthis
        mov     edi, edx                                   ; seed
        
        ; initialize CRandomMersenneA structure
        mov     dword [rcx+CRandomMersenneA.PreInx], 4*4
        mov     dword [rcx+CRandomMersenneA.Instset], 4
        mov     eax, MERS_B
        mov     [rcx+CRandomMersenneA.TMB], eax
        mov     [rcx+CRandomMersenneA.TMB+4], eax
        mov     [rcx+CRandomMersenneA.TMB+8], eax
        mov     [rcx+CRandomMersenneA.TMB+12], eax
        mov     eax, MERS_C
        mov     [rcx+CRandomMersenneA.TMC], eax
        mov     [rcx+CRandomMersenneA.TMC+4], eax
        mov     [rcx+CRandomMersenneA.TMC+8], eax
        mov     [rcx+CRandomMersenneA.TMC+12], eax
        mov     eax, 3FF00000H                             ; upper dword of 1.0, double precision
        mov     [rcx+CRandomMersenneA.one+4], eax
        mov     [rcx+CRandomMersenneA.one+12], eax        
        mov     dword [rcx+CRandomMersenneA.LMASK], LOWER_MASK
        mov     dword [rcx+CRandomMersenneA.UMASK], UPPER_MASK
        mov     dword [rcx+CRandomMersenneA.MATA],  MERS_A

        ; put random numbers into MT buffer
        xor     eax, eax
M210:   mov     [rcx+rax*4+CRandomMersenneA.MT], edi
        mov     edx, edi
        shr     edi, 30
        xor     edi, edx
        imul    edi, 1812433253
        inc     eax
        add     edi, eax
        cmp     eax, MERS_N
        jb      M210
        
        ; Set index MTI to end of list, (scaled by 4)
        ; Round up to multiple of 4 to avoid alignment error
        mov     dword [rcx+CRandomMersenneA.MTI], ((MERS_N+3) & (-4)) * 4
        
        pop     rdi
        ret      


; Single threaded version:
; extern "C" void MersenneRandomInitByArray(unsigned int seeds[], int length);

MersenneRandomInitByArray: ; PROC                          ; entry for Linux call
%IFDEF UNIX
        mov     r8d, esi                                   ; length
        mov     rdx, rdi                                   ; seeds
        lea     rcx, [MersenneInstance]                    ; Pthis = point to instance
        jmp     ?Windows_MersRandomInitByArray
%ENDIF
%IFDEF WINDOWS
MersenneRandomInitByArrayD: ; LABEL NEAR                   ; alias
        mov     r8d, edx                                   ; length
        mov     rdx, rcx                                   ; seeds
        lea     rcx, [MersenneInstance]                    ; Pthis = point to instance
        jmp     ?Windows_MersRandomInitByArray
%ENDIF        
;MersenneRandomInitByArray ENDP       

; Thread-safe version:
; extern "C" int MersRandomInitByArray(void * Pthis, unsigned int seeds[], int length);
MersRandomInitByArray: ; PROC
%IFDEF UNIX
        ; translate calling convention
        mov     r8d, edx                                   ; length
        mov     rdx, rsi                                   ; seeds
        mov     rcx, rdi                                   ; Pthis
%ENDIF
        
?Windows_MersRandomInitByArray:
; parameters: rcx = Pthis, rdx = seeds, r8d = length

        and     rcx, -16                                   ; align buffer
        push    rbx
        push    rsi
        push    rdi
        push    rbp
        mov     rbx, rdx                                   ; seeds
        mov     ebp, r8d                                   ; length
        
        mov     edx, 19650218
        call    Mers_init0                                 ; init0(19650218); (rcx unchanged)
        
        mov     r8d, ebp                                   ; r8d = length, ebp = k
        test    ebp, ebp
        jle     M380                                       ; error: length <= 0
        xor     edi, edi                                   ; j = 0
        lea     esi, [rdi+1]                               ; i = 1
        cmp     ebp, MERS_N
        ja      M310
        mov     ebp, MERS_N                                ; k = max (MERS_N,length)
M310:

        ; for (; k; k--) {
M320:   mov     eax, [rcx+rsi*4-4+CRandomMersenneA.MT]     ; mt[i-1]
        mov     edx, eax
        shr     eax, 30
        xor     eax, edx                                   ; mt[i-1] ^ (mt[i-1] >> 30)
        imul    eax, 1664525                               ; * 1664525
        xor     eax, [rcx+rsi*4+CRandomMersenneA.MT]       ; ^ mt[i]
        add     eax, [rbx+rdi*4]                           ; + seeds[j]
        add     eax, edi                                   ; + j
        mov     [rcx+rsi*4+CRandomMersenneA.MT], eax       ; save in mt[i]
        inc     esi                                        ; i++
        inc     edi                                        ; j++
        cmp     esi, MERS_N
        jb      M330                                       ; if (i>=MERS_N)
        mov     eax, [rcx+(MERS_N-1)*4+CRandomMersenneA.MT]; mt[0] = mt[MERS_N-1];
        mov     [rcx+CRandomMersenneA.MT], eax
        mov     esi, 1                                     ; i=1;
M330:
        cmp     edi, r8d                                   ; length
        jb      M340                                       ; if (j>=length)
        xor     edi, edi                                   ; j = 0;
M340:
        dec     ebp                                        ; k--
        jnz     M320                                       ; first k loop
M350:
        mov     ebp, MERS_N-1                              ; k
M360:   mov     eax, [rcx+rsi*4-4+CRandomMersenneA.MT]     ; mt[i-1]
        mov     edx, eax
        shr     eax, 30
        xor     eax, edx                                   ; mt[i-1] ^ (mt[i-1] >> 30)
        imul    eax, 1566083941                            ; * 1566083941
        xor     eax, [rcx+rsi*4+CRandomMersenneA.MT]       ; ^ mt[i]
        sub     eax, esi                                   ; - i
        mov     [rcx+rsi*4+CRandomMersenneA.MT], eax       ; save in mt[i]
        inc     esi                                        ; i++
        cmp     esi, MERS_N
        jb      M370                                       ; if (i>=MERS_N)
        mov     eax, [rcx+(MERS_N-1)*4+CRandomMersenneA.MT]; mt[0] = mt[MERS_N-1];
        mov     [rcx+CRandomMersenneA.MT], eax
        mov     esi, 1                                     ; i=1;
M370:
        dec     ebp                                        ; k--
        jnz     M360                                       ; second k loop
        mov     dword [rcx+CRandomMersenneA.MT], 80000000H ; mt[0] = 0x80000000
M380:
        mov     dword [rcx+CRandomMersenneA.MTI], 0
        mov     dword [rcx+CRandomMersenneA.PreInx], 0

; discard first MERS_N random numbers + PREMADELOST+1 to compensate for lag
        mov     edi, MERS_N + PREMADELOST+1
M391:   call    ?Windows_MersBRandom
        dec     edi
        jnz     M391

        pop     rbp                                        ; restore registers
        pop     rdi
        pop     rsi
        pop     rbx
        ret
;MersRandomInitByArray ENDP


; Single threaded version:
; extern "C" unsigned int MersenneBRandom(); // Output random bits

MersenneBRandom: ; PROC                                    ; entry for both Windows and Linux call
%IFDEF WINDOWS
MersenneBRandomD: ; LABEL NEAR                             ; alias
%ENDIF
        lea     rcx, [MersenneInstance]                    ; Point to instance
        jmp     ?Windows_MersBRandom
;MersenneBRandom ENDP       

; Thread-safe version:
; extern "C" unsigned int MersBRandom(void * Pthis);       // Output random bits

MersBRandom: ; PROC
%IFDEF UNIX
        mov     rcx, rdi                                   ; translate calling convention
%ENDIF

?Windows_MersBRandom: ; LABEL NEAR                         ; Label used internally
        and     rcx, -16                                   ; align buffer
        mov     edx, [rcx+CRandomMersenneA.PreInx]         ; index into premade numbers
        mov     eax, [rcx+rdx*1+CRandomMersenneA.PreInt]   ; fetch premade random number
        add     edx, 4
        mov     [rcx+CRandomMersenneA.PreInx], edx
        cmp     edx, 4*4
        jnb     M410
        ret                                                ; return premade number

M410:
; PREMADE list is empty. Make 4 more numbers ready for next call:
        mov     edx, [rcx+CRandomMersenneA.MTI]            ; fetch 4 numbers from MT buffer
        movdqa  xmm0, oword [rcx+rdx*1+CRandomMersenneA.MT]
        
%IF TEMPERING                                              ; optional tempering algorithm
        movdqa  xmm1, xmm0
        psrld   xmm0, MERS_U
        pxor    xmm0, xmm1
        movdqa  xmm1, xmm0        
        pslld   xmm0, MERS_S
        pand    xmm0, oword [rcx+CRandomMersenneA.TMB]
        pxor    xmm0, xmm1
        movdqa  xmm1, xmm0        
        pslld   xmm0, MERS_T
        pand    xmm0, oword [rcx+CRandomMersenneA.TMC]
        pxor    xmm0, xmm1
        movdqa  xmm1, xmm0        
        psrld   xmm0, MERS_L
        pxor    xmm0, xmm1
%ENDIF   ; tempering

        ; save four premade integers
        movdqa  oword [rcx+CRandomMersenneA.PreInt], xmm0
        ; premake four floating point numbers
        pxor    xmm1, xmm1
        pxor    xmm2, xmm2
        punpckldq xmm1, xmm0                               ; get first two numbers into bits 32-63 and 96-127
        punpckhdq xmm2, xmm0                               ; get next  two numbers into bits 32-63 and 96-127
        psrlq   xmm1, 12                                   ; get bits into mantissa position
        psrlq   xmm2, 12                                   ; get bits into mantissa position
        por     xmm1,oword[rcx+CRandomMersenneA.one]       ; set exponent for interval [1,2)
        por     xmm2,oword[rcx+CRandomMersenneA.one]       ; set exponent for interval [1,2)
        movdqa  oword [rcx+CRandomMersenneA.PreFlt], xmm1  ; store two premade numbers
        movdqa  oword [rcx+CRandomMersenneA.PreFlt+16],xmm2; store two more premade numbers        
        mov     dword [rcx+CRandomMersenneA.PreInx], 0     ; index to premade numbers 
        add     edx, 4*4                                   ; increment MTI index into MT buffer by 4
        mov     [rcx+CRandomMersenneA.MTI], edx
        cmp     edx, MERS_N*4
        jae     M420
        ret                                                ; return random number in eax

; MT buffer exhausted. Make MERS_N new numbers ready for next time
M420:                                                      ; eax is the random number to return
%IF     MERS_N & 3                                         ; if MERS_N is not divisible by 4
        NVALID equ MERS_N & 3                              ; only NVALID of the 4 premade numbers are valid
        ; Move premade numbers (4-NVALID) positions forward
        movdqa  xmm0, [rcx+CRandomMersenneA.PreInt]
        movdqa  xmm1, [rcx+CRandomMersenneA.PreFlt]
        movdqa  xmm2, [rcx+CRandomMersenneA.PreFlt+16]
        movdqu  [rcx+CRandomMersenneA.PreInt + (4-NVALID)*4], xmm0
        movdqu  [rcx+CRandomMersenneA.PreFlt + (4-NVALID)*8], xmm1
%IF NVALID == 3        
        movq    [rcx+CRandomMersenneA.PreFlt+16 + 8], xmm2
%ENDIF        
        ; save index to first valid premade number
        mov     [rcx+CRandomMersenneA.PreInx], (4-NVALID)*4  
%ENDIF
        
; MT buffer is empty. Fill it up
        push    rbx
        movd    xmm3, [rcx+CRandomMersenneA.UMASK]         ; load constants
        movd    xmm4, [rcx+CRandomMersenneA.LMASK]
        movd    xmm5, [rcx+CRandomMersenneA.MATA]
        pshufd  xmm3, xmm3, 0                              ; broadcast constants
        pshufd  xmm4, xmm4, 0
        pshufd  xmm5, xmm5, 0
        xor     rbx,  rbx                                  ; kk = 0
        mov     edx,  MERS_M*4                             ; km
        
; change rcx from pointing to CRandomMersenneA to pointing to CRandomMersenneA.MT
        add     rcx, CRandomMersenneA.MT

M430:   ; kk loop
        movdqa  xmm2, [rcx+rbx]                            ; mt[kk]
        movd    xmm0, dword [rcx+rbx+16]
        movdqa  xmm1, [rcx+rbx]                            ; mt[kk]        
        movss   xmm2, xmm0                                 ; faster than movdqu xmm2,[]
        pshufd  xmm2, xmm2, 00111001B                      ; mt[kk+1]
        movdqu  xmm0, oword [rcx+rdx]                      ; mt[km]        
        ;movq   xmm0, qword [rcx+rdx]                      ; mt[km]
        ;movhps xmm0, qword [rcx+rdx+8]                    ; faster than movdqu on older processors        
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
        movdqa  [rcx+rbx], xmm0                            ; result into mt[kk]
        cmp     ebx, (MERS_N-4)*4
        jae     M440                                       ; exit loop when kk past end of buffer
        add     ebx, 16                                    ; kk += 4
        add     rdx, 16                                    ; km += 4 (signed)
        cmp     edx, (MERS_N-4)*4
        jbe     M430                                       ; skip unless km wraparound
        sub     rdx, MERS_N*4                              ; km wraparound (signed)
        movdqu  xmm0, [rcx+(MERS_N-4)*4]                   ; copy end to before begin for km wraparound
        movdqa  [rcx-4*4], xmm0        
        movdqa  xmm0, [rcx]                                ; copy begin to after end for kk wraparound
        movdqu  [rcx+MERS_N*4], xmm0
        jmp     M430

M440:   ; loop finished. discard excess part of last result

; change ecx back to pointing to CRandomMersenneA
        sub     rcx, CRandomMersenneA.MT        

        mov     dword [rcx+CRandomMersenneA.MTI], 0
        pop     rbx
        ret                                                ; random number is still in eax
        
;MersBRandom ENDP


; Single threaded version:
; extern "C" unsigned int MersenneRandom();  // Get floating point random number

MersenneRandom: ; PROC                                     ; entry for both Windows and Linux call
%IFDEF WINDOWS
MersenneRandomD:                                           ; alias
        lea     rcx, [MersenneInstance]                    ; Point to instance
        ; continue in next function
%ENDIF
%IFDEF UNIX
        lea     rdi, [MersenneInstance]                    ; Point to instance
        ; continue in next function
%ENDIF

; Thread-safe version:
; extern "C" double MersRandom(void * Pthis);  // Get floating point random number
MersRandom: 
%IFDEF UNIX
        mov     rcx, rdi                                   ; translate calling convention
%ENDIF
        mov     edx, [rcx+CRandomMersenneA.PreInx]         ; index into premade numbers
        movsd   xmm0, [rcx+rdx*2+CRandomMersenneA.PreFlt]  ; fetch premade floating point random number
        subsd   xmm0, [rcx+CRandomMersenneA.one]           ; subtract 1.0
        movsd   [rcx+CRandomMersenneA.TmpFlt], xmm0        ; store random number
        call    ?Windows_MersBRandom                       ; prepare next random number
        movsd   xmm0, [rcx+CRandomMersenneA.TmpFlt]        ; recall random number
        ret        
;MersenneRandom ENDP       



; Single threaded version:
; extern "C" unsigned int MersenneIRandom(int min, int max); // Get integer random number in desired interval

MersenneIRandom: ; PROC 
%IFDEF UNIX
        push    rsi                                        ; max
        push    rdi                                        ; min
        lea     rcx, [MersenneInstance]                    ; Pthis = point to instance
        jmp     MersIRandom_max_min_on_stack
%ENDIF
%IFDEF WINDOWS
MersenneIRandomD:                                          ; Alias
        push    rdx                                        ; max
        push    rcx                                        ; min
        lea     rcx, [MersenneInstance]                    ; Pthis = point to instance
        jmp     MersIRandom_max_min_on_stack
%ENDIF
;MersenneIRandom ENDP       

; Thread-safe version:
; extern "C" int MersIRandom(void * Pthis, int min, int max); // Get integer random number in desired interval
MersIRandom: ; PROC
%IFDEF UNIX
        ; translate calling convention
        mov     r8d, edx                                   ; max
        mov     edx, esi                                   ; min
        mov     rcx, rdi                                   ; Pthis
%ENDIF
        push    r8                                         ; max
        push    rdx                                        ; min
MersIRandom_max_min_on_stack:
        
        call    ?Windows_MersBRandom                       ; random bits
        pop     rcx                                        ; min
        pop     rdx                                        ; max
        sub     edx, ecx
        js      short M720                                 ; max < min
        add     edx, 1                                     ; interval = max - min + 1
        mul     edx                                        ; multiply random number by interval and truncate
        lea     eax, [rdx+rcx]                             ; add min
        ret
M720:   mov     eax, 80000000H                             ; error exit
        ret
;MersIRandom ENDP


; Single threaded version:
; extern "C" unsigned int MersenneIRandomX(int min, int max); // Get integer random number in desired interval

MersenneIRandomX: ; PROC
%IFDEF UNIX
        mov     r8d, esi                                   ; max
        mov     edx, edi                                   ; min
        lea     rcx, [MersenneInstance]                    ; Pthis = point to instance
        jmp     ?Windows_MersIRandomX
%ENDIF
%IFDEF WINDOWS
MersenneIRandomXD:                                         ; alias
        mov     r8d, edx                                   ; max
        mov     edx, ecx                                   ; min
        lea     rcx, [MersenneInstance]                    ; Pthis = point to instance
        jmp     ?Windows_MersIRandomX
%ENDIF
;MersenneIRandomX ENDP       

; Thread-safe version:
; extern "C" int MersIRandomX(void * Pthis, int min, int max); // Get integer random number in desired interval
MersIRandomX: ; PROC
%IFDEF UNIX
        ; translate calling convention
        mov     r8d, edx                                   ; max
        mov     edx, esi                                   ; min
        mov     rcx, rdi                                   ; Pthis
%ENDIF
        
?Windows_MersIRandomX:
; parameters: rcx = Pthis, edx = min, r8d = max

        and     rcx, -16                                   ; align buffer
        push    rdi
        mov     edi, r8d                                   ; max

        sub     edi, edx                                   ; max - min
        jle     short M830                                 ; max <= min (signed)
        inc     edi                                        ; interval = max - min + 1
        push    rdx                                        ; save min
        
        ; if (interval != LastInterval) {
        cmp     edi, [rcx+CRandomMersenneA.LastInterval]
        je      M810
        ; RLimit = uint32(((uint64)1 << 32) / interval) * interval - 1;}
        xor     eax, eax                                   ; 0
        lea     edx, [rax+1]                               ; 1
        div     edi                                        ; (would give overflow if interval = 1)
        mul     edi
        dec     eax
        mov     [rcx+CRandomMersenneA.RLimit], eax
        mov     [rcx+CRandomMersenneA.LastInterval], edi
M810:
M820:   ; do { // Rejection loop
        call    ?Windows_MersBRandom                       ; random bits (rcx is preserved)
        ; longran  = (uint64)BRandom() * interval;
        mul     edi
        ; } while (remainder > RLimit);
        cmp     eax, [rcx+CRandomMersenneA.RLimit]
        ja      M820
        
        ; return (int32)iran + min
        pop     rax                                        ; min
        add     eax, edx
        pop     rdi
        ret
        
M830:   jl      M840
        ; max = min. Return min
        mov     eax, edx
        pop     rdi
        ret                                                ; max = min exit
        
M840:   ; max < min: error
        mov     eax, 80000000H                             ; error exit
        pop     rdi
        ret
;MersIRandomX ENDP
