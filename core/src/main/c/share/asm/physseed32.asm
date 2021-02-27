;*************************  physseed32.asm  **********************************
; Author:           Agner Fog
; Date created:     2010-08-03
; Last modified:    2016-11-03
; Source URL:       www.agner.org/optimize
; Project:          asmlib.zip
; C++ prototype:
; extern "C" int PhysicalSeed(int seeds[], int NumSeeds);
;
; Description:
; Generates a non-deterministic random seed from a physical random number generator 
; which is available on some processors. 
; Uses the time stamp counter (which is less random) if no physical random number
; generator is available.
; The code is not optimized for speed because it is typically called only once.
;
; Parameters:
; int seeds[]       An array which will be filled with random numbers
; int NumSeeds      Indicates the desired number of 32-bit random numbers
;
; Return value:     0   Failure. No suitable instruction available (processor older than Pentium)
;                   1   No physical random number generator. Used time stamp counter instead
;                   2   Success. VIA physical random number generator used
;                   3   Success. Intel physical random number generator used
;                   4   Success. Intel physical seed generator used
; 
; The return value will indicate the availability of a physical random number generator
; even if NumSeeds = 0.
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

%define NUM_TRIES   20                 ; max number of tries for rdseed and rdrand instructions

%define TESTING     0                  ; 1 for test only

global _PhysicalSeed

; Direct entries to CPU-specific versions
global _PhysicalSeedNone
global _PhysicalSeedRDTSC
global _PhysicalSeedVIA
global _PhysicalSeedRDRand
global _PhysicalSeedRDSeed


SECTION .text  align=16

_PhysicalSeed:

%IFNDEF POSITIONINDEPENDENT

        jmp     near [PhysicalSeedDispatch] ; Go to appropriate version, depending on instructions available

%ELSE   ; Position-independent code

        call    get_thunk_edx          ; get reference point for position-independent code
RP1:    ; Make the following instruction with address relative to RP1:
        jmp     near [edx+PhysicalSeedDispatch-RP1]

%ENDIF

_PhysicalSeedRDSeed:
        push    ebx
        mov     edx, [esp+8]           ; seeds
        mov     ecx, [esp+12]          ; NumSeeds
        jecxz   S300
        ; do 32 bits at a time
S100:   mov     ebx, NUM_TRIES
S110:   rdseed  eax
        ;db 0Fh, 0C7h, 0F8h             ; rdseed eax
        jc      S120
        ; failed. try again
        dec     ebx
        jz      S900
        jmp     S110
S120:   mov     [edx], eax
        add     edx, 4
        dec     ecx
        jnz     S100                   ; loop 32 bits
S300:   mov     eax, 4                 ; return value
        pop     ebx
        ret        
S900:   ; failure 
        xor     eax, eax               ; return 0
        pop     ebx
        ret

_PhysicalSeedRDRand:
        push    ebx
        mov     edx, [esp+8]           ; seeds
        mov     ecx, [esp+12]          ; NumSeeds
        jecxz   R300
        ; do 32 bits at a time
R100:   mov     ebx, NUM_TRIES
R110:   rdrand  eax
        ;db 0Fh, 0C7h, 0F0h             ; rdrand eax
        jc      R120
        ; failed. try again
        dec     ebx
        jz      R900
        jmp     R110
R120:   mov     [edx], eax
        add     edx, 4
        dec     ecx
        jnz     R100                   ; loop 32 bits
R300:   mov     eax, 3                 ; return value
        pop     ebx
        ret        
R900:   ; failure 
        xor     eax, eax               ; return 0
        pop     ebx
        ret


_PhysicalSeedVIA:
;       VIA XSTORE  supported
        push    ebx
        push    esi
        push    edi
        mov     edi, [esp+16]          ; seeds
        mov     ecx, [esp+20]          ; NumSeeds
        mov     ebx, ecx
        and     ecx, -2                ; round down to nearest even
        jz      T200                   ; NumSeeds <= 1
        ; make an even number of random dwords
        shl     ecx, 2                 ; number of bytes (divisible by 8)
        mov     edx, 3                 ; quality factor
%if     TESTING
        mov     eax, 1
        rep     stosb
%ELSE        
        db 0F3H, 00FH, 0A7H, 0C0H      ; rep xstore instuction
%ENDIF
T200:
        test    ebx, 1
        jz      T300
        ; NumSeeds is odd. Make 8 bytes in temporary buffer and store 4 of the bytes
        mov     esi, edi               ; current output pointer
        push    ebp
        mov     ebp, esp
        sub     esp, 8                 ; make temporary space on stack
        and     esp, -8                ; align by 8
        mov     edi, esp
        mov     ecx, 4                 ; Will generate 4 or 8 bytes, depending on CPU
        mov     edx, 3                 ; quality factor
%if     TESTING
        mov     eax, 1
        rep     stosb
%ELSE
        db 0F3H, 00FH, 0A7H, 0C0H      ; rep xstore instuction
%ENDIF
        mov     eax, [esp]
        mov     [esi], eax             ; store the last 4 bytes
        mov     esp, ebp
        pop     ebp
T300:
        mov     eax, 2                 ; return value
        pop     edi
        pop     esi
        pop     ebx
        ret


_PhysicalSeedRDTSC:
        push    ebx
        xor     eax, eax
        cpuid                          ; serialize
        rdtsc                          ; get time stamp counter
        mov     ebx, [esp+8]           ; seeds
        mov     ecx, [esp+12]          ; NumSeeds
        test    ecx, ecx
        jz      U300                   ; zero seeds
        js      U900                   ; failure
        mov     [ebx], eax             ; store time stamp counter as seeds[0]
        add     ebx, 4
        dec     ecx
        jz      U300
        mov     [ebx], edx             ; store upper part of time stamp counter as seeds[1]
        add     ebx, 4
        dec     ecx
        jz      U300
        xor     eax, eax
U100:   mov     [ebx], eax             ; store 0 for the rest
        add     ebx, 4
        dec     ecx
        jnz     U100
U300:   mov     eax, 1                 ; return value        
        pop     ebx
        ret
U900:   ; failure         
        xor     eax, eax               ; return 0
        pop     ebx
        ret


_PhysicalSeedNone:                     ; no possible generation
        mov     edx, [esp+4]           ; seeds
        mov     ecx, [esp+8]           ; NumSeeds
        xor     eax, eax
        jecxz   N200
N100:   mov     [edx], eax
        add     edx, 4
        dec     ecx
        jnz     N100
N200:   ret                            ; return 0


PhysicalSeedDispatcher:
        push    ebx
        pushfd
        pop     eax
        btc     eax, 21                ; check if CPUID bit can toggle
        push    eax
        popfd
        pushfd
        pop     ebx
        xor     ebx, eax
        bt      ebx, 21
        jc      FAILURE                ; CPUID not supported

        xor     eax, eax               ; 0
        cpuid                          ; get number of CPUID functions
        test    eax, eax
        jz      FAILURE                ; function 1 not supported

        ; test if RDSEED supported
        xor     eax, eax
        cpuid
        cmp     eax, 7
        jb      P200                   ; RDSEED not supported
        mov     eax, 7
        xor     ecx, ecx
        cpuid
        bt      ebx, 18
        jc      USE_RDSEED 

P200:   ; test if RDRAND supported
        mov     eax, 1
        cpuid
        bt      ecx, 30
        jc      USE_RDRAND

        ; test if VIA xstore instruction supported
        mov     eax, 0C0000000H
        push    eax
        cpuid
        pop     ebx
        cmp     eax, ebx
        jna     P300                   ; not a VIA processor
        lea     eax, [ebx+1]
        cpuid
        bt      edx, 3
        jc      VIA_METHOD

P300:   ; test if RDTSC supported
        mov     eax, 1
        cpuid
        bt      edx, 4
        jc      USE_RDTSC              ; XSTORE instruction not supported or not enabled
        
FAILURE: ; No useful instruction supported
        mov     edx, _PhysicalSeedNone
        jmp     P800

USE_RDRAND:     ; Use RDRAND instruction        
        mov     edx, _PhysicalSeedRDRand
        jmp     P800

USE_RDSEED:     ; Use RDSEED instruction (not tested yet)
        mov     edx, _PhysicalSeedRDSeed
        jmp     P800

VIA_METHOD:     ; Use VIA xstore instructions   
        mov     edx, _PhysicalSeedVIA
        jmp     P800
        
USE_RDTSC:
        mov     edx, _PhysicalSeedRDTSC
        ;jmp     P800
        
P800:   mov     [PhysicalSeedDispatch], edx
        pop     ebx
        jmp     edx                    ; continue in dispatched version
        
        
%IFDEF  POSITIONINDEPENDENT
get_thunk_edx: ; load caller address into edx for position-independent code
        mov edx, [esp]
        ret
%ENDIF        
        

; -----------------------------------------------------------------
;  DLL version, Windows only
; -----------------------------------------------------------------
%IFDEF WINDOWS

global _PhysicalSeedD@8
_PhysicalSeedD@8:
        ; translate __cdecl to __stdcall calling
        mov     eax, [esp+4]
        mov     edx, [esp+8]
        push    edx                                       
        push    eax
        call    _PhysicalSeed
        pop     ecx
        pop     ecx
        ret     8

%ENDIF ; WINDOWS


; -----------------------------------------------------------------
;  Data section for dispatcher
; -----------------------------------------------------------------

SECTION .data

; Pointer to appropriate versions. Initially point to dispatcher
PhysicalSeedDispatch  DD PhysicalSeedDispatcher

%IFDEF POSITIONINDEPENDENT
; Fix potential problem in Mac linker
        DD      0, 0
%ENDIF
