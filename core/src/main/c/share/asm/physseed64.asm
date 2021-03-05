;*************************  physseed64.asm  **********************************
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

default rel

%define NUM_TRIES   20                 ; max number of tries for rdseed and rdrand instructions

%define TESTING     0                  ; 1 for test only

global PhysicalSeed

; Direct entries to CPU-specific versions
global PhysicalSeedNone
global PhysicalSeedRDTSC
global PhysicalSeedVIA
global PhysicalSeedRDRand
global PhysicalSeedRDSeed

; ***************************************************************************
; Define registers used for function parameters, used in 64-bit mode only
; ***************************************************************************
 
%IFDEF WINDOWS
  %define par1     rcx
  %define par2     rdx
  %define par3     r8
  %define par1d    ecx
  %define par2d    edx
  %define par3d    r8d
%ENDIF
  
%IFDEF UNIX
  %define par1     rdi
  %define par2     rsi
  %define par3     rdx
  %define par1d    edi
  %define par2d    esi
  %define par3d    edx
%ENDIF 


SECTION .text  align=16

%IFDEF WINDOWS
global PhysicalSeedD@8                 ; DLL version
PhysicalSeedD@8:
%ENDIF

PhysicalSeed:
        jmp     [PhysicalSeedDispatch] ; Go to appropriate version, depending on instructions available


PhysicalSeedRDSeed:
        push    rbx
        test    par2d, par2d           ; NumSeeds
        jz      S300 
        js      S900
        mov     par3d, par2d           ; NumSeeds
        shr     par3d, 1
        jz      S150
        ; do 64 bits at a time
S100:   mov     ebx, NUM_TRIES
S110:   rdseed  rax
        ;db 48h, 0Fh, 0C7h, 0F8h        ; rdseed rax
        jc      S120
        ; failed. try again
        dec     ebx
        jz      S900
        jmp     S110
S120:   mov     [par1], rax
        add     par1, 8
        dec     par3d
        jnz     S100                   ; loop 64 bits
S150:
        and     par2d, 1
        jz      S300
        ; an odd 32 bit remains
S200:   mov     ebx, NUM_TRIES
S210:   rdseed rax
        ;db 0Fh, 0C7h, 0F8h             ; rdseed eax
        jc      S220
        ; failed. try again
        dec     ebx
        jz      S900
        jmp     S210
S220:   mov     [par1], eax
S300:   mov     eax, 4                 ; return value
        pop     rbx
        ret
S900:   ; failure 
        xor     eax, eax               ; return 0
        pop     rbx
        ret
                

PhysicalSeedRDRand:
        push    rbx
        test    par2d, par2d           ; NumSeeds
        jz      R300
        js      R900         
        mov     par3d, par2d           ; NumSeeds
        shr     par3d, 1               ; NumSeeds/2
        jz      R150
        ; do 64 bits at a time
R100:   mov     ebx, NUM_TRIES
R110:   rdrand  rax
        ;db 48h, 0Fh, 0C7h, 0F0h        ; rdrand rax
        jc      R120
        ; failed. try again
        dec     ebx
        jz      R900
        jmp     R110
R120:   mov     [par1], rax
        add     par1, 8
        dec     par3d
        jnz     R100                   ; loop 64 bits
R150:
        and     par2d, 1
        jz      R300
        ; an odd 32 bit remains
R200:   mov     ebx, NUM_TRIES
R210:   rdrand  eax
        ;db 0Fh, 0C7h, 0F0h             ; rdrand eax
        jc      R220
        ; failed. try again
        dec     ebx
        jz      R900
        jmp     R210
R220:   mov     [par1], eax
R300:   mov     eax, 4                 ; return value
        pop     rbx
        ret
R900:   ; failure 
        xor     eax, eax               ; return 0
        pop     rbx
        ret


PhysicalSeedVIA:
;       VIA XSTORE  supported
        push    rbx
%IFDEF WINDOWS
        push    rsi
        push    rdi
        mov     rdi, rcx               ; seeds
        mov     esi, edx               ; NumSeeds
%ENDIF        
        mov     ecx, esi               ; NumSeeds
        and     ecx, -2                ; round down to nearest even
        jz      T200                   ; NumSeeds <= 1
        ; make an even number of random dwords
        shl     ecx, 2                 ; number of bytes (divisible by 8)
        mov     edx, 3                 ; quality factor
%if     TESTING
        mov     eax, 1
        rep stosb
%ELSE        
        db 0F3H, 00FH, 0A7H, 0C0H      ; rep xstore instuction
%ENDIF
T200:        
        test    esi, 1
        jz      T300
        ; NumSeeds is odd. Make 8 bytes in temporary buffer and store 4 of the bytes
        mov     rbx, rdi               ; current output pointer
        mov     ecx, 4                 ; Will generate 4 or 8 bytes, depending on CPU
        mov     edx, 3                 ; quality factor
        push    rcx                    ; make temporary space on stack
        mov     rdi, rsp               ; point to buffer on stack
%if     TESTING
        mov     eax, 1
        rep stosb
%ELSE        
        db 0F3H, 00FH, 0A7H, 0C0H      ; rep xstore instuction
%ENDIF
        pop     rax
        mov     [rbx], eax             ; store the last 4 bytes
T300:
        mov     eax, 2                 ; return value        
%IFDEF WINDOWS
        pop     rdi
        pop     rsi
%ENDIF  
        pop     rbx      
        ret        


PhysicalSeedRDTSC:
%IFDEF WINDOWS
        push    rbx
        push    rcx
        push    rdx
        xor     eax, eax
        cpuid                          ; serialize
        rdtsc                          ; get time stamp counter
        pop     rbx                    ; numseeds
        pop     rcx                    ; seeds
        test    ebx, ebx
        jz      U300                   ; zero seeds
        js      U900                   ; failure
        mov     [rcx], eax             ; store time stamp counter as seeds[0]
        add     rcx, 4
        dec     ebx
        jz      U300
        mov     [rcx], edx             ; store upper part of time stamp counter as seeds[1]
        add     rcx, 4
        dec     ebx
        jz      U300
        xor     eax, eax
U100:   mov     [rcx], eax             ; store 0 for the rest
        add     rcx, 4
        dec     ebx
        jnz     U100
U300:   mov     eax, 1                 ; return value        
        pop     rbx
        ret
U900:   ; failure         
        xor     eax, eax               ; return 0
        pop     rbx
        ret
        
%ELSE   ; UNIX

        push    rbx
        xor     eax, eax
        cpuid                          ; serialize
        rdtsc                          ; get time stamp counter
        test    esi, esi               ; numseeds
        jz      U300                   ; zero seeds
        js      U900                   ; failure
        mov     [rdi], eax             ; store time stamp counter as seeds[0]
        add     rdi, 4
        dec     esi
        jz      U300
        mov     [rdi], edx             ; store upper part of time stamp counter as seeds[1]
        add     rdi, 4
        dec     esi
        jz      U300
        xor     eax, eax
U100:   mov     [rdi], eax             ; store 0 for the rest
        add     rdi, 4
        dec     esi
        jnz     U100
U300:   mov     eax, 1                 ; return value        
        pop     rbx
        ret
U900:   ; failure         
        xor     eax, eax               ; return 0
        pop     rbx
        ret 

%ENDIF  


PhysicalSeedNone:                      ; no possible generation
        xor     eax, eax
        test    par2d, par2d           ; numseeds
        jz      N200
N100:   mov     [par1], eax
        add     par1, 4
        dec     par2d
        jnz     N100
N200:   ret                            ; return 0


PhysicalSeedDispatcher:
        push    rbx
%IFDEF WINDOWS
        push    rcx
        push    rdx
%ENDIF
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
        push    rax
        cpuid
        pop     rbx
        cmp     eax, ebx
        jna     P300                   ; not a VIA processor
        lea     eax, [rbx+1]
        cpuid
        bt      edx, 3
        jc      VIA_METHOD

P300:   ; test if RDTSC supported
        mov     eax, 1
        cpuid
        bt      edx, 4
        jc      USE_RDTSC              ; XSTORE instruction not supported or not enabled
        
FAILURE: ; No useful instruction supported
        lea     rax, [PhysicalSeedNone]
        jmp     P800

USE_RDRAND:     ; Use RDRAND instruction        
        lea     rax, [PhysicalSeedRDRand]
        jmp     P800

USE_RDSEED:     ; Use RDSEED instruction (not tested yet)
        lea     rax, [PhysicalSeedRDSeed]
        jmp     P800

VIA_METHOD:     ; Use VIA xstore instructions   
        lea     rax, [PhysicalSeedVIA]
        jmp     P800
        
USE_RDTSC:
        lea     rax, [PhysicalSeedRDTSC]
        ;jmp     P800
        
P800:   mov     [PhysicalSeedDispatch], rax
%IFDEF WINDOWS
        pop     rdx
        pop     rcx
%ENDIF
        pop     rbx
        jmp     rax                    ; continue in dispatched version
        

; -----------------------------------------------------------------
;  Data section for dispatcher
; -----------------------------------------------------------------

SECTION .data

; Pointer to appropriate versions. Initially point to dispatcher
PhysicalSeedDispatch  DQ PhysicalSeedDispatcher

%IFDEF POSITIONINDEPENDENT
; Fix potential problem in Mac linker
        DD      0, 0
%ENDIF
