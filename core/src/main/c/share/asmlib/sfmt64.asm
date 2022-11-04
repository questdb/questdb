; ----------------------------- SFMT64.ASM ---------------------------
; Author:        Agner Fog
; Date created:  2008-11-01
; Last modified: 2013-12-15
; Project:       randoma library of random number generators
; Source URL:    www.agner.org/random
; Description:
; Random number generator of type SIMD-oriented Fast Mersenne Twister (SFMT)
; (Mutsuo Saito and Makoto Matsumoto: "SIMD-oriented Fast Mersenne Twister:
; a 128-bit Pseudorandom Number Generator", Monte Carlo and Quasi-Monte 
; Carlo Methods 2006, Springer, 2008, pp. 607-622).
;
; 64-bit mode version for x86-64 compatible microprocessors.
; Copyright (c) 2008-2013 GNU General Public License www.gnu.org/licenses
; ----------------------------------------------------------------------

default rel

global SFMTRandomInit, SFMTRandomInitByArray, SFMTBRandom, SFMTRandom
global SFMTRandomL, SFMTIRandom, SFMTIRandomX, SFMTgenRandomInit
global SFMTgenRandomInitByArray, SFMTgenRandom, SFMTgenRandomL, SFMTgenIRandom
global SFMTgenIRandomX, SFMTgenBRandom
%IFDEF WINDOWS
global SFMTgenRandomInitD, SFMTgenRandomInitByArrayD, SFMTgenIRandomD
global SFMTgenIRandomXD, SFMTgenRandomD, SFMTgenBRandomD
%ENDIF


extern InstructionSet

; structure definition and constants:
%INCLUDE "../randomah.asi"


section .data
align 16
; Data for single instance of random number generator
SFMTInstance: ISTRUC CRandomSFMTA
; Size of structure
IEND
SFMTSize equ $-SFMTInstance


align 16
; Initialization constants for Mother-Of-All:
InitMother DD 2111111111, 0, 1492, 0, 1776, 0, 5115, 0
; Initialization Mask for SFMT:
InitMask   DD SFMT_MASK
; Period certification vector for SFMT:
InitParity DD SFMT_PARITY


SECTION .CODE align=16   ; code segment


; ---------------------------------------------------------------
;  Thread-safe static link versions for SFMT
; ---------------------------------------------------------------

; extern "C" void SFMTRandomInit(void * Pthis, int ThisSize, int seed, int IncludeMother = 0);
; Parameters:
; par1  = Pthis
; par2d = ThisSize
; par3d = seed
; par4d = IncludeMother

SFMTRandomInit:
        cmp     par2d, SFMTSize
        jb      Error                                      ; Error exit if buffer too small
        push    rbx

        ; Align by 16. Will overlap part of Fill if Pthis unaligned        
        and     par1, -16

        xor     eax, eax
        test    par4d, par4d                               ; IncludeMother
        setnz   al                                         ; convert any nonzero value to 1
        ; Store USEMOTHER
        mov     [par1+CRandomSFMTA.USEMOTHER], eax
        
        mov     eax, par3d                                 ; seed
        xor     ebx, ebx                                   ; loop counter i
        jmp     L002                                       ; go into seeding loop

L001:   ; seeding loop for SFMT
        ; y = factor * (y ^ (y >> 30)) + (++i);
        call    InitSubf0                                  ; randomization subfunction
L002:   mov     [par1+rbx*4+CRandomSFMTA.STATE],eax        ; initialize state
        cmp     ebx, SFMT_N*4 - 1
        jb      L001

        ; Put 5 more values into Mother-Of-All generator
        call    InitSubf0
        mov     [par1+CRandomSFMTA.M0], eax
        call    InitSubf0
        mov     [par1+CRandomSFMTA.M1], eax
        call    InitSubf0
        mov     [par1+CRandomSFMTA.M2], eax
        call    InitSubf0
        mov     [par1+CRandomSFMTA.M3], eax
        call    InitSubf0
        mov     [par1+CRandomSFMTA.MC], eax
        
        ; more initialization and period certification
        call    InitAndPeriod
        
        pop     rbx
        ret
;SFMTRandomInit ENDP
        
Error:                                                     ; Error exit
        xor     eax, eax
        div     eax                                        ; Divide by 0
        ret
        
; Subfunction used by SFMTRandomInit
InitSubf0: ; private
; y = 1812433253 * (y ^ (y >> 30)) + (++i);
; input parameters:
; eax = y
; ebx = i
; output:
; eax = new y
; ebx = i+1
; edx modified
        mov     edx, eax
        shr     eax, 30
        xor     eax, edx
        imul    eax, 1812433253
        inc     ebx
        add     eax, ebx
        ret
;InitSubf0 endp 
       
; Subfunction used by SFMTRandomInitByArray
InitSubf1: ; private
; r = 1664525U * (r ^ (r >> 27));
; input parameters:
; eax = r
; output:
; eax = new r
; r10 modified
        mov     r10d, eax
        shr     eax,  27
        xor     eax,  r10d
        imul    eax,  1664525
        ret
;InitSubf1 endp

; Subfunction used by SFMTRandomInitByArray
InitSubf2: ; private
; r = 1566083941U * (r ^ (r >> 27));
; input parameters:
; eax = r
; output:
; eax = new r
; r10 modified
        mov     r10d, eax
        shr     eax,  27
        xor     eax,  r10d
        imul    eax,  1566083941
        ret
;InitSubf2 endp


; Subfunction for initialization and period certification, except seeding
; par1 = aligned pointer to CRandomSFMTA
InitAndPeriod: ; private
        push    rbx
        
        ; initialize constants for Mother-Of-All
        movaps  xmm0, oword [InitMother]
        movaps  oword [par1+CRandomSFMTA.MF3], xmm0
        movaps  xmm0, oword [InitMother+16]
        movaps  oword [par1+CRandomSFMTA.MF1], xmm0
        
        ; initialize constants for SFMT
        movaps  xmm0, oword [InitMask]
        movaps  oword [par1+CRandomSFMTA.AMASK], xmm0

        ; initialize various variables
        xor     eax, eax
        mov     dword [par1+CRandomSFMTA.one], eax
        mov     dword [par1+4+CRandomSFMTA.one], 3FF00000H
        mov     dword [par1+CRandomSFMTA.LASTINTERVAL], eax        
        
        ; get instruction set
        push    par1
        call    InstructionSet
        pop     par1
        mov     [par1+CRandomSFMTA.Instset], eax
        
        ; Period certification
        ; Compute parity of STATE[0-4] & InitParity
        movaps  xmm1, oword [par1+CRandomSFMTA.STATE]
        andps   xmm1, oword [InitParity]
        movhlps xmm2, xmm1                                 ; high qword
        xorps   xmm1, xmm2                                 ; xor two qwords
        pshufd  xmm2, xmm1, 1                              ; high dword
        xorps   xmm1, xmm2                                 ; xor two dwords
        movd    eax,  xmm1                                 ; do rest of xor in eax
        mov     edx,  eax
        shr     eax,  16
        xor     eax,  edx                                  ; xor two words
        xor     al,   ah                                   ; xor two bytes
        jpo     L008                                       ; parity odd: period OK
        
        ; parity even: period not OK
        ; Find a nonzero dword in period certification vector
        xor     ebx, ebx                                   ; loop counter
        lea     rdx, [InitParity]
L005:   mov     eax, [rdx+rbx*4]                           ; InitParity[i]
        test    eax, eax
        jnz     L006
        inc     ebx
        ; assume that there is a nonzero dword in InitParity
        jmp     L005                                       ; loop until nonzero found
        
L006:   ; find first nonzero bit in eax
        bsf     edx, eax
        ; flip the corresponding bit in STATE
        btc     [par1+rbx*4+CRandomSFMTA.STATE], edx

L008:   cmp     dword [par1+CRandomSFMTA.USEMOTHER], 0
        je      L009
        call    Mother_Next                                ; Make first random number ready

L009:   ; Generate first random numbers and set IX = 0
        call    SFMT_Generate
        pop     rbx
        ret
;InitAndPeriod   endp


;  extern "C" void SFMTRandomInitByArray
; (void * Pthis, int ThisSize, int const seeds[], int NumSeeds, int IncludeMother = 0);
; // Seed by more than 32 bits
SFMTRandomInitByArray:
; Parameters
; par1  = Pthis
; par2d = ThisSize
; par3  = seeds
; par4d = NumSeeds
; par5d = IncludeMother

; define constants:
SFMT_SIZE equ SFMT_N*4                                     ; number of 32-bit integers in state

%IF SFMT_SIZE >= 623
   SFMT_LAG equ 11
%ELIF SFMT_SIZE >= 68
   SFMT_LAG equ  7
%ELIF SFMT_SIZE >= 39
   SFMT_LAG equ  5
%ELSE
   SFMT_LAG equ  3
%ENDIF

SFMT_MID equ ((SFMT_SIZE - SFMT_LAG) / 2)

        xor     eax, eax
        cmp     par5d, eax                                 ; IncludeMother (parameter is on stack if windows)
        setnz   al                                         ; convert any nonzero value to 1

        push    rbx
        push    rbp
        
        cmp     par2d, SFMTSize                            ; ThisSize
        jb      Error                                      ; Error exit if buffer too small

        ; Align by 16. Will overlap part of Fill if Pthis unaligned        
        and     par1, -16        

        ; Store USEMOTHER
        mov     [par1+CRandomSFMTA.USEMOTHER], eax 

; 1. loop: Fill state vector with random numbers from NumSeeds
; r = NumSeeds;
; for (i = 0; i < SFMT_N*4; i++) {
;    r = factor * (r ^ (r >> 30)) + i;
;    sta[i] = r;}

        mov     eax, par4d                                 ; r = NumSeeds
        xor     ebx, ebx                                   ; i
L100:   mov     par2d, eax
        shr     eax, 30
        xor     eax, par2d
        imul    eax, 1812433253
        add     eax, ebx
        mov     [par1+rbx*4+CRandomSFMTA.STATE], eax
        inc     ebx
        cmp     ebx, SFMT_SIZE
        jb      L100        

        ; count = max(NumSeeds,size-1)
        mov     eax,  SFMT_SIZE - 1
        mov     r11d, par4d                                 ; NumSeeds
        cmp     r11d, eax
        cmovb   r11d, eax
        
; 2. loop: Fill state vector with random numbers from seeds[]
; for (i = 1, j = 0; j < count; j++) {
;    r = func1(sta[i] ^ sta[(i + mid) % size] ^ sta[(i + size - 1) % size]);
;    sta[(i + mid) % size] += r;
;    if (j < NumSeeds) r += seeds[j]
;    r += i;
;    sta[(i + mid + lag) % size] += r;
;    sta[i] = r;
;    i = (i + 1) % size;
; }
        ; register use:
        ; par1  = Pthis
        ; par2  = j
        ; par3  = seeds
        ; par4  = NumSeeds
        ; eax   = r
        ; ebx   = i
        ; ebp   = (i + mid) % size, (i + mid + lag) % size
        ; r10   = (i + size - 1) % size
        ; r11   = count

        xor     par2d, par2d           ; j = 0
        lea     ebx, [par2+1]          ; i = 1

L101:   ; r = sta[i] ^ sta[(i + mid) % size] ^ sta[(i + size - 1) % size];
        mov     eax,  [par1+rbx*4+CRandomSFMTA.STATE]      ; sta[i]
        lea     ebp,  [rbx+SFMT_MID]
        cmp     ebp,  SFMT_SIZE
        jb      L102
        sub     ebp,  SFMT_SIZE
L102:   xor     eax,  [par1+rbp*4+CRandomSFMTA.STATE]      ; sta[(i + mid) % size]
        lea     r10d, [rbx+SFMT_SIZE-1]
        cmp     r10d, SFMT_SIZE
        jb      L103
        sub     r10d, SFMT_SIZE
L103:   xor     eax,  [par1+r10*4+CRandomSFMTA.STATE]      ; sta[(i + size - 1) % size]

        ; r = func1(r) = (r ^ (r >> 27)) * 1664525U;
        call    InitSubf1
        
        ; sta[(i + mid) % size] += r;
        add     [par1+rbp*4+CRandomSFMTA.STATE], eax
        
        ; if (j < NumSeeds) r += seeds[j]
        cmp     par2d, par4d
        jnb     L104
        add     eax, [par3+par2*4]        
L104:
        ; r += i;
        add     eax, ebx
        
        ; sta[(i + mid + lag) % size] += r;
        lea     ebp, [rbx+SFMT_MID+SFMT_LAG]
        cmp     ebp, SFMT_SIZE
        jb      L105
        sub     ebp, SFMT_SIZE
L105:   add     [par1+rbp*4+CRandomSFMTA.STATE], eax
        
        ;sta[i] = r;
        mov     [par1+rbx*4+CRandomSFMTA.STATE], eax
        
        ; i = (i + 1) % size;
        inc     ebx
        cmp     ebx, SFMT_SIZE
        jb      L106
        sub     ebx, SFMT_SIZE
L106:
        ; j++, loop while j < count
        inc     par2d
        cmp     par2d, r11d
        jb      L101
        
; 3. loop: Randomize some more
; for (j = 0; j < size; j++) {
;   r = func2(sta[i] + sta[(i + mid) % size] + sta[(i + size - 1) % size]);
;   sta[(i + mid) % size] ^= r;
;   r -= i;
;   sta[(i + mid + lag) % size] ^= r;
;   sta[i] = r;
;   i = (i + 1) % size;
; }
        ; j = 0
        xor     par2d, par2d

L110:    ; r = sta[i] + sta[(i + mid) % size] + sta[(i + size - 1) % size]
        mov     eax,  [par1+rbx*4+CRandomSFMTA.STATE]      ; sta[i]
        lea     ebp,  [rbx+SFMT_MID]
        cmp     ebp,  SFMT_SIZE
        jb      L111
        sub     ebp,  SFMT_SIZE
L111:   add     eax,  [par1+rbp*4+CRandomSFMTA.STATE]      ; sta[(i + mid) % size]
        lea     r10d, [rbx+SFMT_SIZE-1]
        cmp     r10d, SFMT_SIZE
        jb      L112
        sub     r10d, SFMT_SIZE
L112:   add     eax,  [par1+r10*4+CRandomSFMTA.STATE]      ; sta[(i + size - 1) % size]

        ; r = func2(r) = (x ^ (x >> 27)) * 1566083941U;
        call    InitSubf2
        
        ; sta[(i + mid) % size] ^= r;
        xor     [par1+rbp*4+CRandomSFMTA.STATE], eax
        
        ; r -= i;
        sub     eax, ebx
        
        ; sta[(i + mid + lag) % size] ^= r;
        lea     ebp, [rbx+SFMT_MID+SFMT_LAG]
        cmp     ebp, SFMT_SIZE
        jb      L113
        sub     ebp, SFMT_SIZE
L113:   xor     [par1+rbp*4+CRandomSFMTA.STATE], eax

        ; sta[i] = r;
        mov     [par1+rbx*4+CRandomSFMTA.STATE], eax
        
        ; i = (i + 1) % size;
        inc     ebx
        cmp     ebx, SFMT_SIZE
        jb      L114
        sub     ebx, SFMT_SIZE
L114:
        ; j++, loop while j < size
        inc     par2d
        cmp     par2d, SFMT_SIZE
        jb      L110
    
        ; if (UseMother) {
        cmp     dword [par1+CRandomSFMTA.USEMOTHER], 0
        jz      L120
        
; 4. loop: Initialize MotherState
; for (j = 0; j < 5; j++) {
;    r = func2(r) + j;
;    MotherState[j] = r + sta[2*j];
; }
        call    InitSubf2
        mov     par2d, [par1+CRandomSFMTA.STATE]
        add     par2d, eax
        mov     [par1+CRandomSFMTA.M0], par2d
        call    InitSubf2
        inc     eax
        mov     par2d, [par1+8+CRandomSFMTA.STATE]
        add     par2d, eax
        mov     [par1+CRandomSFMTA.M1], par2d
        call    InitSubf2
        add     eax, 2
        mov     par2d, [par1+16+CRandomSFMTA.STATE]
        add     par2d, eax        
        mov     [par1+CRandomSFMTA.M2], par2d
        call    InitSubf2
        add     eax, 3
        mov     par2d, [par1+24+CRandomSFMTA.STATE]
        add     par2d, eax        
        mov     [par1+CRandomSFMTA.M3], par2d
        call    InitSubf2
        add     eax, 4
        mov     par2d, [par1+32+CRandomSFMTA.STATE]
        add     par2d, eax        
        mov     [par1+CRandomSFMTA.MC], par2d
        
L120:    ; More initialization and period certification
        call    InitAndPeriod
        
        pop     rbp
        pop     rbx
        ret
;SFMTRandomInitByArray ENDP


Mother_Next: ; private
; Internal procedure: advance Mother-Of-All generator
; The random value is in M0
; par1 = aligned pointer to structure CRandomSFMTA
; eax, par1, xmm0 unchanged

        movdqa  xmm1, oword [par1+CRandomSFMTA.M3]         ; load M3,M2
        movdqa  xmm2, oword [par1+CRandomSFMTA.M1]         ; load M1,M0
        movhps  qword [par1+CRandomSFMTA.M3], xmm1         ; M3=M2
        movq    qword [par1+CRandomSFMTA.M2], xmm2         ; M2=M1
        movhps  qword [par1+CRandomSFMTA.M1], xmm2         ; M1=M0
        pmuludq xmm1, oword [par1+CRandomSFMTA.MF3]        ; M3*MF3, M2*MF2
        pmuludq xmm2, oword [par1+CRandomSFMTA.MF1]        ; M1*MF1, M0*MF0
        paddq   xmm1, xmm2                                 ; P3+P1, P2+P0
        movhlps xmm2, xmm1                                 ; Get high qword
        movq    xmm3, qword [par1+CRandomSFMTA.MC]         ; +carry
        paddq   xmm1, xmm3
        paddq   xmm1, xmm2                                 ; P0+P1+P2+P3
        movq    qword [par1+CRandomSFMTA.M0], xmm1         ; Store new M0 and carry
        ret
;Mother_Next endp


align 16
SFMT_Generate: ; private
; void CRandomSFMT::Generate() {
; Fill state array with new random numbers

        push    rbx
        
        ; register use
        ; par1 = Pthis (rcx or rdi)
        ; edx  = i*16 + offset state
        ; eax, ebx = loop end
        ; xmm1 = r1
        ; xmm2 = r2 = r
        ; xmm0, xmm3 = scratch
        
        ; r1 = state[SFMT_N*16 - 2];
        ; r2 = state[SFMT_N*16 - 1];
        movdqa  xmm1, oword [par1+(SFMT_N-2)*16+CRandomSFMTA.STATE]
        movdqa  xmm2, oword [par1+(SFMT_N-1)*16+CRandomSFMTA.STATE]
        mov     edx, CRandomSFMTA.STATE
        
;static inline __m128i sfmt_recursion(__m128i const &a, __m128i const &b, 
;__m128i const &c, __m128i const &d, __m128i const &mask) {
;    __m128i a1, b1, c1, d1, z1, z2;
;    b1 = _mm_srli_epi32(b, SFMT_SR1);
;    a1 = _mm_slli_si128(a, SFMT_SL2);
;    c1 = _mm_srli_si128(c, SFMT_SR2);
;    d1 = _mm_slli_epi32(d, SFMT_SL1);
;    b1 = _mm_and_si128(b1, mask);
;    z1 = _mm_xor_si128(a, a1);
;    z2 = _mm_xor_si128(b1, d1);
;    z1 = _mm_xor_si128(z1, c1);
;    z2 = _mm_xor_si128(z1, z2);
;    return z2;}

; for (i = 0; i < SFMT_N - SFMT_M; i++) {
;    r = sfmt_recursion(state[i], state[i + SFMT_M], r1, r2, mask);
;    state[i] = r;
;    r1 = r2;
;    r2 = r;
; }

        mov      eax, (SFMT_N-SFMT_M)*16 + CRandomSFMTA.STATE ; first loop end
        mov      ebx, SFMT_N*16 + CRandomSFMTA.STATE          ; second loop end

; first i loop from 0 to SFMT_N - SFMT_M
align 8
L201:   movdqa   xmm0, oword [par1+rdx+SFMT_M*16]          ; b
        psrld    xmm0, SFMT_SR1                            ; b1
        pand     xmm0, oword [par1+CRandomSFMTA.AMASK]     ; b1
        movdqa   xmm3, oword [par1+rdx]                    ; a
        pxor     xmm0, xmm3
        pslldq   xmm3, SFMT_SL2                            ; a1
        psrldq   xmm1, SFMT_SR2                            ; c1, c = r1
        pxor     xmm0, xmm3
        pxor     xmm0, xmm1
        movdqa   xmm1, xmm2                                ; r1 = r2
        pslld    xmm2, SFMT_SL1                            ; d1, d = r2
        pxor     xmm2, xmm0                                ; r2 = r
        ; state[i] = r;
        movdqa   oword [par1+rdx], xmm2
        
        ; i++ while i < SFMT_N - SFMT_M
        add      edx, 16
        cmp      edx, eax
        jb       L201
        
;align 16
L202:   ; second i loop from SFMT_N - SFMT_M + 1 to SFMT_N
        movdqa   xmm0, oword [par1+rdx+(SFMT_M-SFMT_N)*16]; b
        psrld    xmm0, SFMT_SR1                            ; b1
        pand     xmm0, oword [par1+CRandomSFMTA.AMASK]     ; b1
        movdqa   xmm3, oword [par1+rdx]                    ; a
        pxor     xmm0, xmm3
        pslldq   xmm3, SFMT_SL2                            ; a1
        psrldq   xmm1, SFMT_SR2                            ; c1, c = r1
        pxor     xmm0, xmm3
        pxor     xmm0, xmm1
        movdqa   xmm1, xmm2                                ; r1 = r2
        pslld    xmm2, SFMT_SL1                            ; d1, d = r2
        pxor     xmm2, xmm0                                ; r2 = r
        ; state[i] = r;
        movdqa   oword [par1+rdx], xmm2
        
        ; i++ while i < SFMT_N
        add      edx, 16
        cmp      edx, ebx
        jb       L202
        
        ; Check if initialized
L208:   cmp     dword [par1+CRandomSFMTA.AMASK], SFMT_MASK1
        jne     Error                                      ; Make error if not initialized

        ; ix = 0;
        mov      dword [par1+CRandomSFMTA.IX], 0 ; point to start of STATE buffer
        pop      rbx
        ret
;SFMT_Generate endp


;  extern "C" unsigned int SFMTBRandom(void * Pthis);                     // Output random bits

SFMTBRandom:                                               ; generate random bits
        ; Align Pthis by 16. Will overlap part of Fill1 if Pthis unaligned        
        and     par1, -16        

SFMTBRandom_reg:                                           ; Entry for register parameters, used internally

; if (ix >= SFMT_N*4) Generate();
        mov     edx, [par1+CRandomSFMTA.IX]
        cmp     edx, SFMT_N*16
        jnb     NeedGenerate
        
; y = ((uint32_t*)state)[ix++];
        mov     eax, dword [par1+rdx+CRandomSFMTA.STATE]
        add     edx, 4
        mov     [par1+CRandomSFMTA.IX], edx

AfterGenerate:
; if (UseMother) y += MotherBits();
        cmp     dword [par1+CRandomSFMTA.USEMOTHER], 0
        jz      NoMother
        
        ; add mother bits
        add     eax,  [par1+CRandomSFMTA.M0]               ; Add Mother random number        
        call    Mother_Next                                ; Make next Mother random number ready
        
NoMother: ; return y;
        ret
        
NeedGenerate: 
        call    SFMT_Generate                              ; generate SFMT_N*4 random dwords
        mov     eax, [par1+CRandomSFMTA.STATE]
        mov     dword [par1+CRandomSFMTA.IX], 4
        jmp     AfterGenerate
        
;SFMTBRandom ENDP


;  extern "C" double SFMTRandom  (void * Pthis); // Output random float
SFMTRandom:                                                ; generate random float with 52 bits resolution
        ; Align Pthis by 16. Will overlap part of Fill1 if Pthis unaligned        
        and     par1, -16
        
SFMTRandom_reg:                                            ; internal entry point        

; check if there are at least 64 random bits in state buffer
; if (ix >= SFMT_N*4-1) Generate();
        mov     edx, [par1+CRandomSFMTA.IX]
        cmp     edx, SFMT_N*16-4
        jnb     L303

L301:   ; read 64 random bits
        movq    xmm0, qword [par1+rdx+CRandomSFMTA.STATE]
        add     edx, 8
        mov     [par1+CRandomSFMTA.IX], edx

        ; combine with Mother-Of-All generator?
        cmp     dword [par1+CRandomSFMTA.USEMOTHER], 0
        jz      L302 ; ConvertToFloat
        
        ; add mother bits
        movq    xmm1, qword [par1+CRandomSFMTA.M0]         ; Mother random number MC and M0
        pshuflw xmm1, xmm1, 01001011B                      ; Put M0 before MC, and swap the words in MC
        paddq   xmm0, xmm1                                 ; Add SFMT and Mother outputs
        call    Mother_Next                                ; Make next Mother random number ready
        
L302:   ; ConvertToFloat
        psrlq	xmm0, 12			                       ; align with mantissa field of double precision float
        movsd   xmm1, [par1+CRandomSFMTA.one]              ; 1.0 double precision
        por     xmm0, xmm1                                 ; insert exponent to get 1.0 <= x < 2.0
        subsd   xmm0, xmm1                                 ; subtract 1.0 to get 0.0 <= x < 1.0
        ret                                                ; return value        
        
L303:   ; NeedGenerateR
        call    SFMT_Generate                              ; generate SFMT_N*4 random dwords
        xor     edx, edx
        jmp     L301

;SFMTRandom ENDP


; extern "C" long double SFMTRandomL (void * Pthis);
SFMTRandomL:                                               ; generate random float with 63 bits resolution
        ; Align Pthis by 16.
        and     par1, -16
        
SFMTRandomL_reg:                                           ; internal entry point        

; check if there are at least 64 random bits in state buffer
; if (ix >= SFMT_N*4-1) Generate();
        mov     edx, [par1+CRandomSFMTA.IX]
        cmp     edx, SFMT_N*16-4
        jnb     L403

L401:   ; read 64 random bits
        movq    xmm0, qword [par1+rdx+CRandomSFMTA.STATE]
        add     edx, 8
        mov     [par1+CRandomSFMTA.IX], edx

        ; combine with Mother-Of-All generator?
        cmp     dword [par1+CRandomSFMTA.USEMOTHER], 0
        jz      L402
                
        ; add mother bits
        movq    xmm1, qword [par1+CRandomSFMTA.M0]        ; Mother random number MC and M0
        pshuflw xmm1, xmm1, 01001011B                     ; Put M0 before MC, and swap the words in MC
        paddq   xmm0, xmm1                                ; Add SFMT and Mother outputs
        call    Mother_Next                               ; Make next Mother random number ready
        
L402:   ;ConvertToFloat
        sub     rsp, 16                                   ; make space for long double
        psrlq	xmm0, 1                                   ; align with mantissa field of long double
        pcmpeqw xmm1, xmm1                                ; all 1's
        psllq   xmm1, 63                                  ; create a 1 in bit 63
        por     xmm0, xmm1                                ; bit 63 is always 1 in long double
        movq    qword [rsp], xmm0                         ; store mantissa
        mov     dword [rsp+8], 3FFFH                      ; exponent
        fld     tword [rsp]                               ; load long double
        fsub    qword [par1+CRandomSFMTA.one]             ; subtract 1.0 to get 0.0 <= x < 1.0
        pcmpeqw xmm0, xmm0                                ; make a NAN for compilers that don't support long double
        add     rsp, 16
        ret                                               ; return value in st(0)
        
L403:   ;NeedGenerateR
        call    SFMT_Generate                             ; generate SFMT_N*4 random dwords
        xor     edx, edx
        jmp     L401        
;SFMTRandomL ENDP


;  extern "C" int SFMTIRandom (void * Pthis, int min, int max);  // Output random integer

SFMTIRandom:
; par1  = Pthis
; par2d = min
; par3d = max

        ; Align Pthis by 16.
        and     par1, -16        
        push    par2                                       ; save min, max
        push    par3
        call    SFMTBRandom_reg                            ; random bits
        pop     rdx                                        ; max
        pop     rcx                                        ; min        
        sub     edx, ecx
        jl      short WrongInterval                        ; max < min
        inc     edx                                        ; max - min + 1
        mul     edx                                        ; multiply random number by interval and truncate
        lea     eax, [rdx+rcx]                             ; add min to high dword of product
        ret
WrongInterval:
        mov     eax, 80000000H                             ; error exit
        ret
;SFMTIRandom ENDP


;  extern "C" int SFMTIRandomX (void * Pthis, int min, int max); // Output random integer

SFMTIRandomX:
; par1  = Pthis
; par2d = min
; par3d = max

        push    rbx
        ; Align Pthis by 16.
        and     par1, -16        

        mov     ebx, par3d 
        sub     ebx, par2d                                 ; max - min
        jle     short M30                                  ; max <= min (signed)
        inc     ebx                                        ; interval = max - min + 1
        
        ; if (interval != LastInterval) {
        cmp     ebx, [par1+CRandomSFMTA.LASTINTERVAL]
        je      M10
        ; need to calculate new rejection limit
        ; RLimit = uint32(((uint64)1 << 32) / interval) * interval - 1;}
        xor     eax, eax                                   ; 0
        lea     edx, [eax+1]                               ; 1
        div     ebx                                        ; (would give overflow if interval = 1)
        mul     ebx
        dec     eax
        mov     [par1+CRandomSFMTA.RLIMIT], eax       
        mov     [par1+CRandomSFMTA.LASTINTERVAL], ebx
M10:    mov     ebx, par2d                                 ; save min

M20:    ; do { // Rejection loop
        call    SFMTBRandom_reg                            ; random bits (par1 is preserved)
        ; longran  = (uint64)BRandom() * interval;
        mul     dword [par1+CRandomSFMTA.LASTINTERVAL]
        ; } while (remainder > RLimit);
        cmp     eax, [par1+CRandomSFMTA.RLIMIT]
        ja      M20
        
        ; return (int32)iran + min
        lea     eax, [rbx+rdx]
        pop     rbx
        ret
        
M30:    jl      M40
        ; max = min. Return min
        mov     eax, par2d
        pop     rbx
        ret                                                ; max = min exit
        
M40:    ; max < min: error
        mov     eax, 80000000H                             ; error exit
        pop     rbx
        ret
;SFMTIRandomX ENDP



; -------------------------------------------------------------------------
;  Single-threaded static link versions for SFMT generator
; -------------------------------------------------------------------------

;  extern "C" void SFMTgenRandomInit(int seed, int IncludeMother = 0); 
SFMTgenRandomInit:
%IFDEF WINDOWS
SFMTgenRandomInitD:
%ENDIF
; par1d = seed
; par2d = IncludeMother

        ; set up parameters for call SFMTRandomInit
        mov     par4d, par2d                               ; IncludeMother
        mov     par3d, par1d                               ; seed
        mov     par2d, SFMTSize                            ; ThisSize
        lea     par1,  [SFMTInstance]                      ; Get address of SFMTInstance into par1
        jmp     SFMTRandomInit
;SFMTgenRandomInit ENDP


;  extern "C" void SFMTgenRandomInitByArray(int const seeds[], int NumSeeds, int IncludeMother = 0);
SFMTgenRandomInitByArray:
; par1  = seeds
; par2d = NumSeeds
; par3d = IncludeMother

        ; set up parameters for call SFMTRandomInitByArray
%IFDEF   WINDOWS
SFMTgenRandomInitByArrayD:
        push    par3                                       ; IncludeMother on stack
        sub     rsp, 32                                    ; empty shadow space
        mov     par4d, par2d                               ; NumSeeds
        mov     par3,  par1                                ; seeds
        mov     par2d, SFMTSize                            ; ThisSize
        lea     par1,  [SFMTInstance]                      ; Get address of SFMTInstance into par1
        call	SFMTRandomInitByArray
        add     rsp, 40
        ret
%ELSE    ; UNIX
        mov     par5d, par3d                               ; IncludeMother in register
        mov     par4d, par2d                               ; NumSeeds
        mov     par3,  par1                                ; seeds
        mov     par2d, SFMTSize                            ; ThisSize
        lea     par1,  [SFMTInstance]                      ; Get address of SFMTInstance into par1
        jmp     SFMTRandomInitByArray
%ENDIF        
;SFMTgenRandomInitByArray ENDP  


;  extern "C" double SFMTgenRandom();
SFMTgenRandom:                                             ; generate random float with 52 bits resolution
%IFDEF WINDOWS
SFMTgenRandomD:
%ENDIF
        lea     par1, [SFMTInstance]                       ; Get address of SFMTInstance into par1
        jmp     SFMTRandom_reg                             ; random bits
;SFMTgenRandom ENDP


;  extern "C" double SFMTgenRandom();
SFMTgenRandomL:                                            ; generate random float with 63 bits resolution
        lea     par1, [SFMTInstance]                       ; Get address of SFMTInstance into par1
        jmp     SFMTRandomL_reg                            ; random bits
;SFMTgenRandomL ENDP


;  extern "C" int SFMTgenIRandom (int min, int max);
SFMTgenIRandom:
%IFDEF WINDOWS
SFMTgenIRandomD:   
%ENDIF
        mov     par3d, par2d
        mov     par2d, par1d
        lea     par1, [SFMTInstance]                       ; Get address of SFMTInstance into par1
        jmp     SFMTIRandom				                   ; continue in _SFMTIRandom
;SFMTgenIRandom ENDP


;  extern "C" int SFMTgenIRandomX (int min, int max);
SFMTgenIRandomX:
%IFDEF WINDOWS
SFMTgenIRandomXD:
%ENDIF
        mov     par3d, par2d
        mov     par2d, par1d
        lea     par1, [SFMTInstance]                       ; Get address of SFMTInstance into par1
        jmp	    SFMTIRandomX                               ; continue in _SFMTIRandomX
;SFMTgenIRandomX ENDP


;  extern "C" uint32_t SFMTgenBRandom();
SFMTgenBRandom:                                            ; generate random float with 32 bits resolution
%IFDEF WINDOWS
SFMTgenBRandomD:
%ENDIF
        lea     par1, [SFMTInstance]                       ; Get address of SFMTInstance into par1
        jmp     SFMTBRandom_reg                            ; random bits
;SFMTgenBRandom ENDP

;END
