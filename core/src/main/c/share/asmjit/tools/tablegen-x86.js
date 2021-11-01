// AsmJit - Machine code generation for C++
//
//  * Official AsmJit Home Page: https://asmjit.com
//  * Official Github Repository: https://github.com/asmjit/asmjit
//
// Copyright (c) 2008-2020 The AsmJit Authors
//
// This software is provided 'as-is', without any express or implied
// warranty. In no event will the authors be held liable for any damages
// arising from the use of this software.
//
// Permission is granted to anyone to use this software for any purpose,
// including commercial applications, and to alter it and redistribute it
// freely, subject to the following restrictions:
//
// 1. The origin of this software must not be misrepresented; you must not
//    claim that you wrote the original software. If you use this software
//    in a product, an acknowledgment in the product documentation would be
//    appreciated but is not required.
// 2. Altered source versions must be plainly marked as such, and must not be
//    misrepresented as being the original software.
// 3. This notice may not be removed or altered from any source distribution.

// ============================================================================
// tablegen-x86.js
//
// The purpose of this script is to fetch all instructions' names into a single
// string and to optimize common patterns that appear in instruction data. It
// prevents relocation of small strings (instruction names) that has to be done
// by a linker to make all pointers the binary application/library uses valid.
// This approach decreases the final size of AsmJit binary and relocation data.
//
// NOTE: This script relies on 'asmdb' package. Either install it by using
// node.js package manager (npm) or by copying/symlinking the whole asmdb
// directory as [asmjit]/tools/asmdb.
// ============================================================================

"use strict";

const core = require("./tablegen.js");
const asmdb = core.asmdb;
const kIndent = core.kIndent;

const Lang = core.Lang;
const CxxUtils = core.CxxUtils;
const MapUtils = core.MapUtils;
const ArrayUtils = core.ArrayUtils;
const StringUtils = core.StringUtils;
const IndexedArray = core.IndexedArray;

const hasOwn = Object.prototype.hasOwnProperty;
const disclaimer = StringUtils.disclaimer;

const FAIL = core.FAIL;
const DEBUG = core.DEBUG;

const decToHex = StringUtils.decToHex;

// ============================================================================
// [tablegen.x86.x86isa]
// ============================================================================

// Create the X86 database and add some special cases recognized by AsmJit.
const x86isa = new asmdb.x86.ISA({
  instructions: [
    // Imul in [reg, imm] form is encoded as [reg, reg, imm].
    ["imul", "r16, ib"    , "RMI"  , "66 6B /r ib"   , "ANY OF=W SF=W ZF=U AF=U PF=U CF=W"],
    ["imul", "r32, ib"    , "RMI"  , "6B /r ib"      , "ANY OF=W SF=W ZF=U AF=U PF=U CF=W"],
    ["imul", "r64, ib"    , "RMI"  , "REX.W 6B /r ib", "X64 OF=W SF=W ZF=U AF=U PF=U CF=W"],
    ["imul", "r16, iw"    , "RMI"  , "66 69 /r iw"   , "ANY OF=W SF=W ZF=U AF=U PF=U CF=W"],
    ["imul", "r32, id"    , "RMI"  , "69 /r id"      , "ANY OF=W SF=W ZF=U AF=U PF=U CF=W"],
    ["imul", "r64, id"    , "RMI"  , "REX.W 69 /r id", "X64 OF=W SF=W ZF=U AF=U PF=U CF=W"],

    // Movabs (X64 only).
    ["movabs", "W:r64, iq/uq" , "I"   , "REX.W B8+r iq", "X64"],
    ["movabs", "w:al, moff8"  , "NONE", "A0"           , "X64"],
    ["movabs", "w:ax, moff16" , "NONE", "66 A1"        , "X64"],
    ["movabs", "W:eax, moff32", "NONE", "A1"           , "X64"],
    ["movabs", "W:rax, moff64", "NONE", "REX.W A1"     , "X64"],
    ["movabs", "W:moff8, al"  , "NONE", "A2"           , "X64"],
    ["movabs", "W:moff16, ax" , "NONE", "66 A3"        , "X64"],
    ["movabs", "W:moff32, eax", "NONE", "A3"           , "X64"],
    ["movabs", "W:moff64, rax", "NONE", "REX.W A3"     , "X64"]
  ]
});

// Remapped instructions contain mapping between instructions that AsmJit expects
// and instructions provided by asmdb. In general, AsmJit uses string instructions
// (like cmps, movs, etc...) without the suffix, so we just remap these and keep
// all others.
const RemappedInsts = {
  __proto__: null,

  "cmpsd": { names: ["cmpsd"]                           , rep: false },
  "movsd": { names: ["movsd"]                           , rep: false },
  "cmps" : { names: ["cmpsb", "cmpsw", "cmpsd", "cmpsq"], rep: true  },
  "movs" : { names: ["movsb", "movsw", "movsd", "movsq"], rep: true  },
  "lods" : { names: ["lodsb", "lodsw", "lodsd", "lodsq"], rep: null  },
  "scas" : { names: ["scasb", "scasw", "scasd", "scasq"], rep: null  },
  "stos" : { names: ["stosb", "stosw", "stosd", "stosq"], rep: null  },
  "ins"  : { names: ["insb" , "insw" , "insd" ]         , rep: null  },
  "outs" : { names: ["outsb", "outsw", "outsd"]         , rep: null  }
};

// ============================================================================
// [tablegen.x86.Filter]
// ============================================================================

class Filter {
  static unique(instArray) {
    const result = [];
    const known = {};

    for (var i = 0; i < instArray.length; i++) {
      const inst = instArray[i];
      if (inst.attributes.AltForm)
        continue;

      const s = inst.operands.map((op) => { return op.isImm() ? "imm" : op.toString(); }).join(", ");
      if (known[s] === true)
        continue;

      known[s] = true;
      result.push(inst);
    }

    return result;
  }

  static noAltForm(instArray) {
    const result = [];
    for (var i = 0; i < instArray.length; i++) {
      const inst = instArray[i];
      if (inst.attributes.AltForm)
        continue;
      result.push(inst);
    }
    return result;
  }

  static byArch(instArray, arch) {
    return instArray.filter(function(inst) {
      return inst.arch === "ANY" || inst.arch === arch;
    });
  }
}

// ============================================================================
// [tablegen.x86.GenUtils]
// ============================================================================

const VexToEvexMap = {
  "vbroadcastf128": "vbroadcastf32x4",
  "vbroadcasti128": "vbroadcasti32x4",
  "vextractf128": "vextractf32x4",
  "vextracti128": "vextracti32x4",
  "vinsertf128": "vinsertf32x4",
  "vinserti128": "vinserti32x4",
  "vmovdqa": "vmovdqa32",
  "vmovdqu": "vmovdqu32",
  "vpand": "vpandd",
  "vpandn": "vpandnd",
  "vpor": "vpord",
  "vpxor": "vpxord"
};

class GenUtils {
  static cpuArchOf(dbInsts) {
    var anyArch = false;
    var x86Arch = false;
    var x64Arch = false;

    for (var i = 0; i < dbInsts.length; i++) {
      const dbInst = dbInsts[i];
      if (dbInst.arch === "ANY") anyArch = true;
      if (dbInst.arch === "X86") x86Arch = true;
      if (dbInst.arch === "X64") x64Arch = true;
    }

    return anyArch || (x86Arch && x64Arch) ? "" : x86Arch ? "(X86)" : "(X64)";
  }

  static cpuFeaturesOf(dbInsts) {
    return ArrayUtils.sorted(dbInsts.unionCpuFeatures());
  }

  static assignVexEvexCompatibilityFlags(f, dbInsts) {
    const vexInsts = dbInsts.filter((inst) => { return inst.prefix === "VEX"; });
    const evexInsts = dbInsts.filter((inst) => { return inst.prefix === "EVEX"; });

    function isCompatible(vexInst, evexInst) {
      if (vexInst.operands.length !== evexInst.operands.length)
        return false;

      for (let i = 0; i < vexInst.operands.length; i++) {
        const vexOp = vexInst.operands[i];
        const evexOp = evexInst.operands[i];

        if (vexOp.data === evexOp.data)
          continue;

        if (vexOp.reg && vexOp.reg === evexOp.reg)
          continue;
        if (vexOp.mem && vexOp.mem === evexOp.mem)
          continue;

        return false;
      }
      return true;
    }

    let compatible = 0;
    for (const vexInst of vexInsts) {
      for (const evexInst of evexInsts) {
        if (isCompatible(vexInst, evexInst)) {
          compatible++;
          break;
        }
      }
    }

    if (compatible == vexInsts.length) {
      f.EvexCompat = true;
      return true;
    }

    if (evexInsts[0].operands[0].reg === "k") {
      f.EvexKReg = true;
      return true;
    }

    if (evexInsts[0].operands.length == 2 && vexInsts[0].operands.length === 3) {
      f.EvexTwoOp = true;
      return true;
    }

    return false;
  }

  static flagsOf(dbInsts) {
    const f = Object.create(null);
    var i, j;

    var mib = dbInsts.length > 0 && /^(?:bndldx|bndstx)$/.test(dbInsts[0].name);
    if (mib) f.Mib = true;

    var mmx = false;
    var vec = false;

    for (i = 0; i < dbInsts.length; i++) {
      const dbInst = dbInsts[i];
      const operands = dbInst.operands;

      if (dbInst.name === "emms")
        mmx = true;

      if (dbInst.name === "vzeroall" || dbInst.name === "vzeroupper")
        vec = true;

      for (j = 0; j < operands.length; j++) {
        const op = operands[j];
        if (op.reg === "mm")
          mmx = true;
        else if (/^(xmm|ymm|zmm)$/.test(op.reg)) {
          vec = true;
        }
      }
    }

    if (mmx) f.Mmx = true;
    if (vec) f.Vec = true;

    for (i = 0; i < dbInsts.length; i++) {
      const dbInst = dbInsts[i];
      const operands = dbInst.operands;

      if (dbInst.attributes.Lock      ) f.Lock       = true;
      if (dbInst.attributes.XAcquire  ) f.XAcquire   = true;
      if (dbInst.attributes.XRelease  ) f.XRelease   = true;
      if (dbInst.attributes.BND       ) f.Rep        = true;
      if (dbInst.attributes.REP       ) f.Rep        = true;
      if (dbInst.attributes.REPNE     ) f.Rep        = true;
      if (dbInst.attributes.RepIgnored) f.RepIgnored = true;

      if (dbInst.fpu) {
        for (var j = 0; j < operands.length; j++) {
          const op = operands[j];
          if (op.memSize === 16) f.FpuM16 = true;
          if (op.memSize === 32) f.FpuM32 = true;
          if (op.memSize === 64) f.FpuM64 = true;
          if (op.memSize === 80) f.FpuM80 = true;
        }
      }

      if (dbInst.attributes.Tsib)
        f.Tsib = true;

      if (dbInst.vsibReg)
        f.Vsib = true;

      if (dbInst.prefix === "VEX" || dbInst.prefix === "XOP")
        f.Vex = true;

      if (dbInst.prefix === "EVEX") {
        f.Evex = true;

        if (dbInst.extensions["AVX512_VNNI"])
          f.PreferEvex = true;

        if (dbInst.kmask) f.Avx512K = true;
        if (dbInst.zmask) f.Avx512Z = true;

        if (dbInst.er) f.Avx512ER = true;
        if (dbInst.sae) f.Avx512SAE = true;

        if (dbInst.broadcast) f["Avx512B" + String(dbInst.elementSize)] = true;
        if (dbInst.tupleType === "T1_4X") f.Avx512T4X = true;
      }

      if (VexToEvexMap[dbInst.name])
        f.EvexTransformable = true;
    }

    if (f.Vex && f.Evex) {
      GenUtils.assignVexEvexCompatibilityFlags(f, dbInsts)
    }

    return Object.getOwnPropertyNames(f);
  }

  static eqOps(aOps, aFrom, bOps, bFrom) {
    var x = 0;
    for (;;) {
      const aIndex = x + aFrom;
      const bIndex = x + bFrom;

      const aOut = aIndex >= aOps.length;
      const bOut = bIndex >= bOps.length;

      if (aOut || bOut)
        return !!(aOut && bOut);

      const aOp = aOps[aIndex];
      const bOp = bOps[bIndex];

      if (aOp.data !== bOp.data)
        return false;

      x++;
    }
  }

  // Prevent some instructions from having implicit memory size if that would
  // make them ambiguous. There are some instructions where the ambiguity is
  // okay, but some like 'push' and 'pop' where it isn't.
  static canUseImplicitMemSize(name) {
    switch (name) {
      case "pop":
      case "push":
        return false;

      default:
        return true;
    }
  }

  static singleRegCase(name) {
    switch (name) {
      case "xchg"    :

      case "and"     :
      case "pand"    : case "vpand"  : case "vpandd"  : case "vpandq"   :
      case "andpd"   : case "vandpd" :
      case "andps"   : case "vandps" :

      case "or"      :
      case "por"     : case "vpor"   : case "vpord"   : case "vporq"    :
      case "orpd"    : case "vorpd"  :
      case "orps"    : case "vorps"  :

      case "pminsb"  : case "vpminsb": case "pmaxsb"  : case "vpmaxsb"  :
      case "pminsw"  : case "vpminsw": case "pmaxsw"  : case "vpmaxsw"  :
      case "pminsd"  : case "vpminsd": case "pmaxsd"  : case "vpmaxsd"  :
      case "pminub"  : case "vpminub": case "pmaxub"  : case "vpmaxub"  :
      case "pminuw"  : case "vpminuw": case "pmaxuw"  : case "vpmaxuw"  :
      case "pminud"  : case "vpminud": case "pmaxud"  : case "vpmaxud"  :
        return "RO";

      case "pandn"   : case "vpandn" : case "vpandnd" : case "vpandnq"  :

      case "xor"     :
      case "pxor"    : case "vpxor"  : case "vpxord"  : case "vpxorq"   :
      case "xorpd"   : case "vxorpd" :
      case "xorps"   : case "vxorps" :

      case "kxnorb":
      case "kxnord":
      case "kxnorw":
      case "kxnorq":

      case "kxorb":
      case "kxord":
      case "kxorw":
      case "kxorq":

      case "sub"     :
      case "sbb"     :
      case "psubb"   : case "vpsubb" :
      case "psubw"   : case "vpsubw" :
      case "psubd"   : case "vpsubd" :
      case "psubq"   : case "vpsubq" :
      case "psubsb"  : case "vpsubsb": case "psubusb" : case "vpsubusb" :
      case "psubsw"  : case "vpsubsw": case "psubusw" : case "vpsubusw" :

      case "vpcmpeqb": case "pcmpeqb": case "vpcmpgtb": case "pcmpgtb"  :
      case "vpcmpeqw": case "pcmpeqw": case "vpcmpgtw": case "pcmpgtw"  :
      case "vpcmpeqd": case "pcmpeqd": case "vpcmpgtd": case "pcmpgtd"  :
      case "vpcmpeqq": case "pcmpeqq": case "vpcmpgtq": case "pcmpgtq"  :

      case "vpcmpb"  : case "vpcmpub":
      case "vpcmpd"  : case "vpcmpud":
      case "vpcmpw"  : case "vpcmpuw":
      case "vpcmpq"  : case "vpcmpuq":
        return "WO";

      default:
        return "None";
    }
  }

  static fixedRegOf(reg) {
    switch (reg) {
      case "es"  : return 1;
      case "cs"  : return 2;
      case "ss"  : return 3;
      case "ds"  : return 4;
      case "fs"  : return 5;
      case "gs"  : return 6;
      case "ah"  : return 0;
      case "ch"  : return 1;
      case "dh"  : return 2;
      case "bh"  : return 3;
      case "al"  : case "ax": case "eax": case "rax": case "zax": return 0;
      case "cl"  : case "cx": case "ecx": case "rcx": case "zcx": return 1;
      case "dl"  : case "dx": case "edx": case "rdx": case "zdx": return 2;
      case "bl"  : case "bx": case "ebx": case "rbx": case "zbx": return 3;
      case "spl" : case "sp": case "esp": case "rsp": case "zsp": return 4;
      case "bpl" : case "bp": case "ebp": case "rbp": case "zbp": return 5;
      case "sil" : case "si": case "esi": case "rsi": case "zsi": return 6;
      case "dil" : case "di": case "edi": case "rdi": case "zdi": return 7;
      case "st0" : return 0;
      case "xmm0": return 0;
      case "ymm0": return 0;
      case "zmm0": return 0;
      default:
        return -1;
    }
  }

  static controlType(dbInsts) {
    if (dbInsts.checkAttribute("Control", "Jump")) return "Jump";
    if (dbInsts.checkAttribute("Control", "Call")) return "Call";
    if (dbInsts.checkAttribute("Control", "Branch")) return "Branch";
    if (dbInsts.checkAttribute("Control", "Return")) return "Return";
    return "None";
  }
}

// ============================================================================
// [tablegen.x86.X86TableGen]
// ============================================================================

class X86TableGen extends core.TableGen {
  constructor() {
    super("X86");
  }

  // --------------------------------------------------------------------------
  // [Query]
  // --------------------------------------------------------------------------

  // Get instructions (dbInsts) having the same name as understood by AsmJit.
  query(name) {
    const remapped = RemappedInsts[name];
    if (!remapped) return x86isa.query(name);

    const dbInsts = x86isa.query(remapped.names);
    const rep = remapped.rep;
    if (rep === null) return dbInsts;

    return dbInsts.filter((inst) => {
      return rep === !!(inst.attributes.REP || inst.attributes.REPNE);
    });
  }

  // --------------------------------------------------------------------------
  // [Parse / Merge]
  // --------------------------------------------------------------------------

  parse() {
    const data = this.dataOfFile("src/asmjit/x86/x86instdb.cpp");
    const re = new RegExp(
      "INST\\(" +
        "([A-Za-z0-9_]+)\\s*"              + "," +  // [01] Instruction.
        "([^,]+)"                          + "," +  // [02] Encoding.
        "(.{26}[^,]*)"                     + "," +  // [03] Opcode[0].
        "(.{26}[^,]*)"                     + "," +  // [04] Opcode[1].
        // --- autogenerated fields ---
        "([^\\)]+)"                        + "," +  // [05] MainOpcodeIndex.
        "([^\\)]+)"                        + "," +  // [06] AltOpcodeIndex.
        "([^\\)]+)"                        + "," +  // [07] NameIndex.
        "([^\\)]+)"                        + "," +  // [08] CommonDataIndex.
        "([^\\)]+)"                        + "\\)", // [09] OperationDataIndex.
      "g");

    var m;
    while ((m = re.exec(data)) !== null) {
      var enum_       = m[1];
      var name        = enum_ === "None" ? "" : enum_.toLowerCase();
      var encoding    = m[2].trim();
      var opcode0     = m[3].trim();
      var opcode1     = m[4].trim();

      const dbInsts = this.query(name);
      if (name && !dbInsts.length)
        FAIL(`Instruction '${name}' not found in asmdb`);

      const flags         = GenUtils.flagsOf(dbInsts);
      const controlType   = GenUtils.controlType(dbInsts);
      const singleRegCase = GenUtils.singleRegCase(name);

      this.addInst({
        id                : 0,             // Instruction id (numeric value).
        name              : name,          // Instruction name.
        enum              : enum_,         // Instruction enum without `kId` prefix.
        dbInsts           : dbInsts,       // All dbInsts returned from asmdb query.
        encoding          : encoding,      // Instruction encoding.
        opcode0           : opcode0,       // Primary opcode.
        opcode1           : opcode1,       // Secondary opcode.
        flags             : flags,
        signatures        : null,          // Instruction signatures.
        controlType       : controlType,
        singleRegCase     : singleRegCase,

        mainOpcodeValue   : -1,            // Main opcode value (0.255 hex).
        mainOpcodeIndex   : -1,            // Index to InstDB::_mainOpcodeTable.
        altOpcodeIndex    : -1,            // Index to InstDB::_altOpcodeTable.
        nameIndex         : -1,            // Index to InstDB::_nameData.
        commonInfoIndexA  : -1,
        commomInfoIndexB  : -1,

        signatureIndex    : -1,
        signatureCount    : -1
      });
    }

    if (this.insts.length === 0)
      FAIL("X86TableGen.parse(): Invalid parsing regexp (no data parsed)");

    console.log("Number of Instructions: " + this.insts.length);
  }

  merge() {
    var s = StringUtils.format(this.insts, "", true, function(inst) {
      return "INST(" +
        String(inst.enum            ).padEnd(17) + ", " +
        String(inst.encoding        ).padEnd(19) + ", " +
        String(inst.opcode0         ).padEnd(26) + ", " +
        String(inst.opcode1         ).padEnd(26) + ", " +
        String(inst.mainOpcodeIndex ).padEnd( 3) + ", " +
        String(inst.altOpcodeIndex  ).padEnd( 3) + ", " +
        String(inst.nameIndex       ).padEnd( 5) + ", " +
        String(inst.commonInfoIndexA).padEnd( 3) + ", " +
        String(inst.commomInfoIndexB).padEnd( 3) + ")";
    }) + "\n";
    this.inject("InstInfo", s, this.insts.length * 8);
  }

  // --------------------------------------------------------------------------
  // [Other]
  // --------------------------------------------------------------------------

  printMissing() {
    const ignored = MapUtils.arrayToMap([
      "cmpsb", "cmpsw", "cmpsd", "cmpsq",
      "lodsb", "lodsw", "lodsd", "lodsq",
      "movsb", "movsw", "movsd", "movsq",
      "scasb", "scasw", "scasd", "scasq",
      "stosb", "stosw", "stosd", "stosq",
      "insb" , "insw" , "insd" ,
      "outsb", "outsw", "outsd",
      "wait" // Maps to `fwait`, which AsmJit uses instead.
    ]);

    var out = "";
    x86isa.instructionNames.forEach(function(name) {
      var dbInsts = x86isa.query(name);
      if (!this.instMap[name] && ignored[name] !== true) {
        console.log(`MISSING INSTRUCTION '${name}'`);
        var inst = this.newInstFromGroup(dbInsts);
        if (inst) {
          out += "  INST(" +
            String(inst.enum      ).padEnd(17) + ", " +
            String(inst.encoding  ).padEnd(19) + ", " +
            String(inst.opcode0   ).padEnd(26) + ", " +
            String(inst.opcode1   ).padEnd(26) + ", " +
            String("0"            ).padEnd( 3) + ", " +
            String("0"            ).padEnd( 3) + ", " +
            String("0"            ).padEnd( 5) + ", " +
            String("0"            ).padEnd( 3) + ", " +
            String("0"            ).padEnd( 3) + "),\n";
        }
      }
    }, this);
    console.log(out);
  }

  newInstFromGroup(dbInsts) {
    function composeOpCode(obj) {
      return `${obj.type}(${obj.prefix},${obj.opcode},${obj.o},${obj.l},${obj.w},${obj.ew},${obj.en},${obj.tt})`;
    }

    function GetAccess(dbInst) {
      var operands = dbInst.operands;
      if (!operands.length) return "";

      var op = operands[0];
      if (op.read && op.write)
        return "RW";
      else if (op.read)
        return "RO";
      else
        return "WO";
    }

    function isVecPrefix(s) {
      return s === "VEX" || s === "EVEX" || s === "XOP";
    }

    var dbi = dbInsts[0];

    var id       = this.insts.length;
    var name     = dbi.name;
    var enum_    = name[0].toUpperCase() + name.substr(1);

    var opcode   = dbi.opcodeHex;
    var modR     = dbi.modR;
    var mm       = dbi.mm;
    var pp       = dbi.pp;
    var encoding = dbi.encoding;
    var isVec    = isVecPrefix(dbi.prefix);

    var access   = GetAccess(dbi);

    var vexL     = undefined;
    var vexW     = undefined;
    var evexW    = undefined;

    for (var i = 0; i < dbInsts.length; i++) {
      dbi = dbInsts[i];

      if (dbi.prefix === "VEX" || dbi.prefix === "XOP") {
        var newVexL = String(dbi.l === "128" ? 0 : dbi.l === "256" ? 1 : dbi.l === "512" ? 2 : "_");
        var newVexW = String(dbi.w === "W0" ? 0 : dbi.w === "W1" ? 1 : "_");

        if (vexL !== undefined && vexL !== newVexL)
          vexL = "x";
        else
          vexL = newVexL;
        if (vexW !== undefined && vexW !== newVexW)
          vexW = "x";
        else
          vexW = newVexW;
      }

      if (dbi.prefix === "EVEX") {
        var newEvexW = String(dbi.w === "W0" ? 0 : dbi.w === "W1" ? 1 : "_");
        if (evexW !== undefined && evexW !== newEvexW)
          evexW = "x";
        else
          evexW = newEvexW;
      }

      if (opcode   !== dbi.opcodeHex ) { console.log(`ISSUE: Opcode ${opcode} != ${dbi.opcodeHex}`); return null; }
      if (modR     !== dbi.modR      ) { console.log(`ISSUE: ModR ${modR} != ${dbi.modR}`); return null; }
      if (mm       !== dbi.mm        ) { console.log(`ISSUE: MM ${mm} != ${dbi.mm}`); return null; }
      if (pp       !== dbi.pp        ) { console.log(`ISSUE: PP ${pp} != ${dbi.pp}`); return null; }
      if (encoding !== dbi.encoding  ) { console.log(`ISSUE: Enc ${encoding} != ${dbi.encoding}`); return null; }
      if (access   !== GetAccess(dbi)) { console.log(`ISSUE: Access ${access} != ${GetAccess(dbi)}`); return null; }
      if (isVec    != isVecPrefix(dbi.prefix)) { console.log(`ISSUE: Vex/Non-Vex mismatch`); return null; }
    }

    var ppmm = pp.padEnd(2).replace(/ /g, "0") +
               mm.padEnd(4).replace(/ /g, "0") ;

    var composed = composeOpCode({
      type  : isVec ? "V" : "O",
      prefix: ppmm,
      opcode: opcode,
      o     : modR === "r" ? "_" : (modR ? modR : "_"),
      l     : vexL !== undefined ? vexL : "_",
      w     : vexW !== undefined ? vexW : "_",
      ew    : evexW !== undefined ? evexW : "_",
      en    : "_",
      tt    : dbi.modRM ? dbi.modRM + "  " : "_  "
    });

    return {
      id                : id,
      name              : name,
      enum              : enum_,
      encoding          : encoding,
      opcode0           : composed,
      opcode1           : "0",
      nameIndex         : -1,
      commonInfoIndexA  : -1,
      commomInfoIndexB  : -1
    };
  }

  // --------------------------------------------------------------------------
  // [Hooks]
  // --------------------------------------------------------------------------

  onBeforeRun() {
    this.load([
      "src/asmjit/x86/x86globals.h",
      "src/asmjit/x86/x86instdb.cpp",
      "src/asmjit/x86/x86instdb.h",
      "src/asmjit/x86/x86instdb_p.h"
    ]);
    this.parse();
  }

  onAfterRun() {
    this.merge();
    this.save();
    this.dumpTableSizes();
    this.printMissing();
  }
}

// ============================================================================
// [tablegen.x86.IdEnum]
// ============================================================================

class IdEnum extends core.IdEnum {
  constructor() {
    super("IdEnum");
  }

  comment(inst) {
    function filterAVX(features, avx) {
      return features.filter(function(item) { return /^(AVX|FMA)/.test(item) === avx; });
    }

    var dbInsts = inst.dbInsts;
    if (!dbInsts.length) return "Invalid instruction id.";

    var text = "";
    var features = GenUtils.cpuFeaturesOf(dbInsts);

    const priorityFeatures = ["AVX_VNNI"];

    if (features.length) {
      text += "{";
      const avxFeatures = filterAVX(features, true);
      const otherFeatures = filterAVX(features, false);

      for (const pf of priorityFeatures) {
        const index = avxFeatures.indexOf(pf);
        if (index != -1) {
          avxFeatures.splice(index, 1);
          avxFeatures.unshift(pf);
        }
      }

      const vl = avxFeatures.indexOf("AVX512_VL");
      if (vl !== -1) avxFeatures.splice(vl, 1);

      const fma = avxFeatures.indexOf("FMA");
      if (fma !== -1) { avxFeatures.splice(fma, 1); avxFeatures.splice(0, 0, "FMA"); }

      text += avxFeatures.join("|");
      if (vl !== -1) text += "+VL";

      if (otherFeatures.length)
        text += (avxFeatures.length ? " & " : "") + otherFeatures.join("|");

      text += "}";
    }

    var arch = GenUtils.cpuArchOf(dbInsts);
    if (arch)
      text += (text ? " " : "") + arch;

    return `Instruction '${inst.name}'${(text ? " " + text : "")}.`;
  }
}

// ============================================================================
// [tablegen.x86.NameTable]
// ============================================================================

class NameTable extends core.NameTable {
  constructor() {
    super("NameTable");
  }
}

// ============================================================================
// [tablegen.x86.AltOpcodeTable]
// ============================================================================

class AltOpcodeTable extends core.Task {
  constructor() {
    super("AltOpcodeTable");
  }

  run() {
    const insts = this.ctx.insts;

    const mainOpcodeTable = new IndexedArray();
    const altOpcodeTable = new IndexedArray();

    mainOpcodeTable.addIndexed("O(000000,00,0,0,0,0,0,_  )");

    function indexOpcode(opcode) {
      if (opcode === "0")
        return ["00", 0];

      // O_FPU(__,__OP,_)
      if (opcode.startsWith("O_FPU(")) {
        var value = opcode.substring(11, 13);
        var remaining = opcode.substring(0, 11) + "00" + opcode.substring(13);

        return [value, mainOpcodeTable.addIndexed(remaining.padEnd(26))];
      }

      // X(______,OP,_,_,_,_,_,_  )
      if (opcode.startsWith("O(") || opcode.startsWith("V(") || opcode.startsWith("E(")) {
        var value = opcode.substring(9, 11);
        var remaining = opcode.substring(0, 9) + "00" + opcode.substring(11);

        remaining = remaining.replace(/,[_xI],/g, ",0,");
        remaining = remaining.replace(/,[_xI],/g, ",0,");
        return [value, mainOpcodeTable.addIndexed(remaining.padEnd(26))];
      }

      FAIL(`Failed to process opcode '${opcode}'`);
    }

    insts.map((inst) => {
      const [value, index] = indexOpcode(inst.opcode0);
      inst.mainOpcodeValue = value;
      inst.mainOpcodeIndex = index;
      inst.altOpcodeIndex = altOpcodeTable.addIndexed(inst.opcode1.padEnd(26));
    });
    // console.log(mainOpcodeTable.length);
    // console.log(StringUtils.format(mainOpcodeTable, kIndent, true));

    this.inject("MainOpcodeTable",
                disclaimer(`const uint32_t InstDB::_mainOpcodeTable[] = {\n${StringUtils.format(mainOpcodeTable, kIndent, true)}\n};\n`),
                mainOpcodeTable.length * 4);

    this.inject("AltOpcodeTable",
                disclaimer(`const uint32_t InstDB::_altOpcodeTable[] = {\n${StringUtils.format(altOpcodeTable, kIndent, true)}\n};\n`),
                altOpcodeTable.length * 4);
  }
}

// ============================================================================
// [tablegen.x86.InstSignatureTable]
// ============================================================================

const RegOp = MapUtils.arrayToMap(["al", "ah", "ax", "eax", "rax", "cl", "r8lo", "r8hi", "r16", "r32", "r64", "xmm", "ymm", "zmm", "mm", "k", "sreg", "creg", "dreg", "st", "bnd"]);
const MemOp = MapUtils.arrayToMap(["m8", "m16", "m32", "m48", "m64", "m80", "m128", "m256", "m512", "m1024"]);

const cmpOp = StringUtils.makePriorityCompare([
  "r8lo", "r8hi", "r16", "r32", "r64", "xmm", "ymm", "zmm", "mm", "k", "sreg", "creg", "dreg", "st", "bnd",
  "mem", "vm", "m8", "m16", "m32", "m48", "m64", "m80", "m128", "m256", "m512", "m1024",
  "mib",
  "vm32x", "vm32y", "vm32z", "vm64x", "vm64y", "vm64z",
  "memBase", "memES", "memDS",
  "i4", "u4", "i8", "u8", "i16", "u16", "i32", "u32", "i64", "u64",
  "rel8", "rel32",
  "implicit"
]);

const OpToAsmJitOp = {
  "implicit": "F(Implicit)",

  "r8lo"    : "F(GpbLo)",
  "r8hi"    : "F(GpbHi)",
  "r16"     : "F(Gpw)",
  "r32"     : "F(Gpd)",
  "r64"     : "F(Gpq)",
  "xmm"     : "F(Xmm)",
  "ymm"     : "F(Ymm)",
  "zmm"     : "F(Zmm)",
  "mm"      : "F(Mm)",
  "k"       : "F(KReg)",
  "sreg"    : "F(SReg)",
  "creg"    : "F(CReg)",
  "dreg"    : "F(DReg)",
  "st"      : "F(St)",
  "bnd"     : "F(Bnd)",
  "tmm"     : "F(Tmm)",

  "mem"     : "F(Mem)",
  "vm"      : "F(Vm)",

  "i4"      : "F(I4)",
  "u4"      : "F(U4)",
  "i8"      : "F(I8)",
  "u8"      : "F(U8)",
  "i16"     : "F(I16)",
  "u16"     : "F(U16)",
  "i32"     : "F(I32)",
  "u32"     : "F(U32)",
  "i64"     : "F(I64)",
  "u64"     : "F(U64)",

  "rel8"    : "F(Rel8)",
  "rel32"   : "F(Rel32)",

  "m8"      : "M(M8)",
  "m16"     : "M(M16)",
  "m32"     : "M(M32)",
  "m48"     : "M(M48)",
  "m64"     : "M(M64)",
  "m80"     : "M(M80)",
  "m128"    : "M(M128)",
  "m256"    : "M(M256)",
  "m512"    : "M(M512)",
  "m1024"   : "M(M1024)",
  "mib"     : "M(Mib)",
  "mAny"    : "M(Any)",
  "vm32x"   : "M(Vm32x)",
  "vm32y"   : "M(Vm32y)",
  "vm32z"   : "M(Vm32z)",
  "vm64x"   : "M(Vm64x)",
  "vm64y"   : "M(Vm64y)",
  "vm64z"   : "M(Vm64z)",

  "memBase" : "M(BaseOnly)",
  "memDS"   : "M(Ds)",
  "memES"   : "M(Es)"
};

function StringifyArray(a, map) {
  var s = "";
  for (var i = 0; i < a.length; i++) {
    const op = a[i];
    if (!hasOwn.call(map, op))
      FAIL(`UNHANDLED OPERAND '${op}'`);
    s += (s ? " | " : "") + map[op];
  }
  return s ? s : "0";
}

class OSignature {
  constructor() {
    this.flags = Object.create(null);
  }

  equals(other) {
    return MapUtils.equals(this.flags, other.flags);
  }

  xor(other) {
    const result = MapUtils.xor(this.flags, other.flags);
    return Object.getOwnPropertyNames(result).length === 0 ? null : result;
  }

  mergeWith(other) {
    const af = this.flags;
    const bf = other.flags;

    var k;
    var indexKind = "";
    var hasReg = false;

    for (k in af) {
      const index = asmdb.x86.Utils.regIndexOf(k);
      const kind = asmdb.x86.Utils.regKindOf(k);

      if (kind)
        hasReg = true;

      if (index !== null && index !== -1)
        indexKind = kind;
    }

    if (hasReg) {
      for (k in bf) {
        const index = asmdb.x86.Utils.regIndexOf(k);
        if (index !== null && index !== -1) {
          const kind = asmdb.x86.Utils.regKindOf(k);
          if (indexKind !== kind)
            return false;
        }
      }
    }

    // Can merge...
    for (k in bf)
      af[k] = true;
    return true;
  }

  toString() {
    var s = "";
    var flags = this.flags;

    for (var k in flags) {
      if (k === "read" || k === "write" || k === "implicit" || k === "memDS" || k === "memES")
        continue;

      var x = k;
      if (x === "memZAX") x = "zax";
      if (x === "memZDI") x = "zdi";
      if (x === "memZSI") x = "zsi";
      s += (s ? "|" : "") + x;
    }

    if (flags.memDS) s = "ds:[" + s + "]";
    if (flags.memES) s = "es:[" + s + "]";

    if (flags.implicit)
      s = "<" + s + ">";

    return s;
  }

  toAsmJitOpData() {
    var oFlags = this.flags;

    var mFlags = Object.create(null);
    var mMemFlags = Object.create(null);
    var mExtFlags = Object.create(null);
    var sRegMask = 0;

    for (var k in oFlags) {
      switch (k) {
        case "implicit":
        case "r8lo"    :
        case "r8hi"    :
        case "r16"     :
        case "r32"     :
        case "r64"     :
        case "creg"    :
        case "dreg"    :
        case "sreg"    :
        case "bnd"     :
        case "st"      :
        case "k"       :
        case "mm"      :
        case "xmm"     :
        case "ymm"     :
        case "zmm"     :
        case "tmm"     : mFlags[k] = true; break;

        case "m8"      :
        case "m16"     :
        case "m32"     :
        case "m48"     :
        case "m64"     :
        case "m80"     :
        case "m128"    :
        case "m256"    :
        case "m512"    :
        case "m1024"   : mFlags.mem = true; mMemFlags[k] = true; break;
        case "mib"     : mFlags.mem = true; mMemFlags.mib = true; break;
        case "mem"     : mFlags.mem = true; mMemFlags.mAny = true; break;
        case "tmem"    : mFlags.mem = true; mMemFlags.mAny = true; break;

        case "memBase" : mFlags.mem = true; mMemFlags.memBase = true; break;
        case "memDS"   : mFlags.mem = true; mMemFlags.memDS = true; break;
        case "memES"   : mFlags.mem = true; mMemFlags.memES = true; break;
        case "memZAX"  : mFlags.mem = true; sRegMask |= 1 << 0; break;
        case "memZSI"  : mFlags.mem = true; sRegMask |= 1 << 6; break;
        case "memZDI"  : mFlags.mem = true; sRegMask |= 1 << 7; break;

        case "vm32x"   : mFlags.vm = true; mMemFlags.vm32x = true; break;
        case "vm32y"   : mFlags.vm = true; mMemFlags.vm32y = true; break;
        case "vm32z"   : mFlags.vm = true; mMemFlags.vm32z = true; break;
        case "vm64x"   : mFlags.vm = true; mMemFlags.vm64x = true; break;
        case "vm64y"   : mFlags.vm = true; mMemFlags.vm64y = true; break;
        case "vm64z"   : mFlags.vm = true; mMemFlags.vm64z = true; break;

        case "i4"      :
        case "u4"      :
        case "i8"      :
        case "u8"      :
        case "i16"     :
        case "u16"     :
        case "i32"     :
        case "u32"     :
        case "i64"     :
        case "u64"     : mFlags[k] = true; break;

        case "rel8"    :
        case "rel32"   :
          mFlags.i32 = true;
          mFlags.i64 = true;
          mFlags[k] = true;
          break;

        case "rel16"   :
          mFlags.i32 = true;
          mFlags.i64 = true;
          mFlags.rel32 = true;
          break;

        default: {
          switch (k) {
            case "es"    : mFlags.sreg = true; sRegMask |= 1 << 1; break;
            case "cs"    : mFlags.sreg = true; sRegMask |= 1 << 2; break;
            case "ss"    : mFlags.sreg = true; sRegMask |= 1 << 3; break;
            case "ds"    : mFlags.sreg = true; sRegMask |= 1 << 4; break;
            case "fs"    : mFlags.sreg = true; sRegMask |= 1 << 5; break;
            case "gs"    : mFlags.sreg = true; sRegMask |= 1 << 6; break;
            case "al"    : mFlags.r8lo = true; sRegMask |= 1 << 0; break;
            case "ah"    : mFlags.r8hi = true; sRegMask |= 1 << 0; break;
            case "ax"    : mFlags.r16  = true; sRegMask |= 1 << 0; break;
            case "eax"   : mFlags.r32  = true; sRegMask |= 1 << 0; break;
            case "rax"   : mFlags.r64  = true; sRegMask |= 1 << 0; break;
            case "cl"    : mFlags.r8lo = true; sRegMask |= 1 << 1; break;
            case "ch"    : mFlags.r8hi = true; sRegMask |= 1 << 1; break;
            case "cx"    : mFlags.r16  = true; sRegMask |= 1 << 1; break;
            case "ecx"   : mFlags.r32  = true; sRegMask |= 1 << 1; break;
            case "rcx"   : mFlags.r64  = true; sRegMask |= 1 << 1; break;
            case "dl"    : mFlags.r8lo = true; sRegMask |= 1 << 2; break;
            case "dh"    : mFlags.r8hi = true; sRegMask |= 1 << 2; break;
            case "dx"    : mFlags.r16  = true; sRegMask |= 1 << 2; break;
            case "edx"   : mFlags.r32  = true; sRegMask |= 1 << 2; break;
            case "rdx"   : mFlags.r64  = true; sRegMask |= 1 << 2; break;
            case "bl"    : mFlags.r8lo = true; sRegMask |= 1 << 3; break;
            case "bh"    : mFlags.r8hi = true; sRegMask |= 1 << 3; break;
            case "bx"    : mFlags.r16  = true; sRegMask |= 1 << 3; break;
            case "ebx"   : mFlags.r32  = true; sRegMask |= 1 << 3; break;
            case "rbx"   : mFlags.r64  = true; sRegMask |= 1 << 3; break;
            case "si"    : mFlags.r16  = true; sRegMask |= 1 << 6; break;
            case "esi"   : mFlags.r32  = true; sRegMask |= 1 << 6; break;
            case "rsi"   : mFlags.r64  = true; sRegMask |= 1 << 6; break;
            case "di"    : mFlags.r16  = true; sRegMask |= 1 << 7; break;
            case "edi"   : mFlags.r32  = true; sRegMask |= 1 << 7; break;
            case "rdi"   : mFlags.r64  = true; sRegMask |= 1 << 7; break;
            case "st0"   : mFlags.st   = true; sRegMask |= 1 << 0; break;
            case "xmm0"  : mFlags.xmm  = true; sRegMask |= 1 << 0; break;
            case "ymm0"  : mFlags.ymm  = true; sRegMask |= 1 << 0; break;
            default:
              console.log(`UNKNOWN OPERAND '${k}'`);
          }
        }
      }
    }

    const sFlags    = StringifyArray(ArrayUtils.sorted(mFlags   , cmpOp), OpToAsmJitOp);
    const sMemFlags = StringifyArray(ArrayUtils.sorted(mMemFlags, cmpOp), OpToAsmJitOp);
    const sExtFlags = StringifyArray(ArrayUtils.sorted(mExtFlags, cmpOp), OpToAsmJitOp);

    return `ROW(${sFlags || 0}, ${sMemFlags || 0}, ${sExtFlags || 0}, ${decToHex(sRegMask, 2)})`;
  }
}

class ISignature extends Array {
  constructor(name) {
    super();
    this.name = name;
    this.x86 = false;
    this.x64 = false;
    this.implicit = 0; // Number of implicit operands.
  }

  opEquals(other) {
    const len = this.length;
    if (len !== other.length) return false;

    for (var i = 0; i < len; i++)
      if (!this[i].equals(other[i]))
        return false;

    return true;
  }

  mergeWith(other) {
    // If both architectures are the same, it's fine to merge.
    var ok = this.x86 === other.x86 && this.x64 === other.x64;

    // If the first arch is [X86|X64] and the second [X64] it's also fine.
    if (!ok && this.x86 && this.x64 && !other.x86 && other.x64)
      ok = true;

    // It's not ok if both signatures have different number of implicit operands.
    if (!ok || this.implicit !== other.implicit)
      return false;

    // It's not ok if both signatures have different number of operands.
    const len = this.length;
    if (len !== other.length)
      return false;

    var xorIndex = -1;
    for (var i = 0; i < len; i++) {
      const xor = this[i].xor(other[i]);
      if (xor === null) continue;

      if (xorIndex === -1)
        xorIndex = i;
      else
        return false;
    }

    // Bail if mergeWidth at operand-level failed.
    if (xorIndex !== -1 && !this[xorIndex].mergeWith(other[xorIndex]))
      return false;

    this.x86 = this.x86 || other.x86;
    this.x64 = this.x64 || other.x64;

    return true;
  }

  toString() {
    return "{" + this.join(", ") + "}";
  }
}

class SignatureArray extends Array {
  // Iterate over all signatures and check which operands don't need explicit memory size.
  calcImplicitMemSize() {
    // Calculates a hash-value (aka key) of all register operands specified by `regOps` in `inst`.
    function keyOf(inst, regOps) {
      var s = "";
      for (var i = 0; i < inst.length; i++) {
        const op = inst[i];
        if (regOps & (1 << i))
          s += "{" + ArrayUtils.sorted(MapUtils.and(op.flags, RegOp)).join("|") + "}";
      }
      return s || "?";
    }

    var i;
    var aIndex, bIndex;

    for (aIndex = 0; aIndex < this.length; aIndex++) {
      const aInst = this[aIndex];
      const len = aInst.length;

      var memOp = "";
      var memPos = -1;
      var regOps = 0;

      // Check if this instruction signature has a memory operand of explicit size.
      for (i = 0; i < len; i++) {
        const aOp = aInst[i];
        const mem = MapUtils.firstOf(aOp.flags, MemOp);

        if (mem) {
          // Stop if the memory operand has implicit-size or if there is more than one.
          if (aOp.flags.mem || memPos >= 0) {
            memPos = -1;
            break;
          }
          else {
            memOp = mem;
            memPos = i;
          }
        }
        else if (MapUtils.anyOf(aOp.flags, RegOp)) {
          // Doesn't consider 'r/m' as we already checked 'm'.
          regOps |= (1 << i);
        }
      }

      if (memPos < 0)
        continue;

      // Create a `sameSizeSet` set of all instructions having the exact
      // explicit memory operand at the same position and registers at
      // positions matching `regOps` bits and `diffSizeSet` having memory
      // operand of different size, but registers at the same positions.
      const sameSizeSet = [aInst];
      const diffSizeSet = [];
      const diffSizeHash = Object.create(null);

      for (bIndex = 0; bIndex < this.length; bIndex++) {
        const bInst = this[bIndex];
        if (aIndex === bIndex || len !== bInst.length) continue;

        var hasMatch = 1;
        for (i = 0; i < len; i++) {
          if (i === memPos) continue;

          const reg = MapUtils.anyOf(bInst[i].flags, RegOp);
          if (regOps & (1 << i))
            hasMatch &= reg;
          else if (reg)
            hasMatch = 0;
        }

        if (hasMatch) {
          const bOp = bInst[memPos];
          if (bOp.flags.mem) continue;

          const mem = MapUtils.firstOf(bOp.flags, MemOp);
          if (mem === memOp) {
            sameSizeSet.push(bInst);
          }
          else if (mem) {
            const key = keyOf(bInst, regOps);
            diffSizeSet.push(bInst);
            if (!diffSizeHash[key])
              diffSizeHash[key] = [bInst];
            else
              diffSizeHash[key].push(bInst);
          }
        }
      }

      // Two cases.
      //   A) The memory operand has implicit-size if `diffSizeSet` is empty. That
      //      means that the instruction only uses one size for all reg combinations.
      //
      //   B) The memory operand has implicit-size if `diffSizeSet` contains different
      //      register signatures than `sameSizeSet`.
      var implicit = true;

      if (!diffSizeSet.length) {
        // Case A:
      }
      else {
        // Case B: Find collisions in `sameSizeSet` and `diffSizeSet`.
        for (bIndex = 0; bIndex < sameSizeSet.length; bIndex++) {
          const bInst = sameSizeSet[bIndex];
          const key = keyOf(bInst, regOps);

          const diff = diffSizeHash[key];
          if (diff) {
            diff.forEach((diffInst) => {
              if ((bInst.x86 && !diffInst.x86) || (!bInst.x86 && diffInst.x86)) {
                // If this is X86|ANY instruction and the other is X64, or vice-versa,
                // then keep this implicit as it won't do any harm. These instructions
                // cannot be mixed and it will make implicit the 32-bit one in cases
                // where X64 introduced 64-bit ones like `cvtsi2ss`.
              }
              else {
                implicit = false;
              }
            });
          }
        }
      }

      // Patch all instructions to accept implicit-size memory operand.
      for (bIndex = 0; bIndex < sameSizeSet.length; bIndex++) {
        const bInst = sameSizeSet[bIndex];
        if (implicit)
          bInst[memPos].flags.mem = true;

        if (!implicit)
          DEBUG(`${this.name}: Explicit: ${bInst}`);
      }
    }
  }

  compact() {
    var didSomething = true;
    while (didSomething) {
      didSomething = false;
      for (var i = 0; i < this.length; i++) {
        var row = this[i];
        var j = i + 1;
        while (j < this.length) {
          if (row.mergeWith(this[j])) {
            this.splice(j, 1);
            didSomething = true;
            continue;
          }
          j++;
        }
      }
    }
  }

  toString() {
    return `[${this.join(", ")}]`;
  }
}

class InstSignatureTable extends core.Task {
  constructor() {
    super("InstSignatureTable");
    this.maxOpRows = 0;
  }

  run() {
    const insts = this.ctx.insts;

    insts.forEach((inst) => {
      inst.signatures = this.makeSignatures(Filter.noAltForm(inst.dbInsts));
      this.maxOpRows = Math.max(this.maxOpRows, inst.signatures.length);
    });

    const iSignatureMap = Object.create(null);
    const iSignatureArr = [];

    const oSignatureMap = Object.create(null);
    const oSignatureArr = [];

    // Must be first to be assigned to zero.
    const oSignatureNone = "ROW(0, 0, 0, 0xFF)";
    oSignatureMap[oSignatureNone] = [0];
    oSignatureArr.push(oSignatureNone);

    function findSignaturesIndex(rows) {
      const len = rows.length;
      if (!len) return 0;

      const indexes = iSignatureMap[rows[0].data];
      if (indexes === undefined) return -1;

      for (var i = 0; i < indexes.length; i++) {
        const index = indexes[i];
        if (index + len > iSignatureArr.length) continue;

        var ok = true;
        for (var j = 0; j < len; j++) {
          if (iSignatureArr[index + j].data !== rows[j].data) {
            ok = false;
            break;
          }
        }

        if (ok)
          return index;
      }

      return -1;
    }

    function indexSignatures(signatures) {
      const result = iSignatureArr.length;

      for (var i = 0; i < signatures.length; i++) {
        const signature = signatures[i];
        const idx = iSignatureArr.length;

        if (!hasOwn.call(iSignatureMap, signature.data))
          iSignatureMap[signature.data] = [];

        iSignatureMap[signature.data].push(idx);
        iSignatureArr.push(signature);
      }

      return result;
    }

    for (var len = this.maxOpRows; len >= 0; len--) {
      insts.forEach((inst) => {
        const signatures = inst.signatures;
        if (signatures.length === len) {
          const signatureEntries = [];
          for (var j = 0; j < len; j++) {
            const signature = signatures[j];

            var signatureEntry = `ROW(${signature.length}, ${signature.x86 ? 1 : 0}, ${signature.x64 ? 1 : 0}, ${signature.implicit}`;
            var signatureComment = signature.toString();

            var x = 0;
            while (x < signature.length) {
              const h = signature[x].toAsmJitOpData();
              var index = -1;
              if (!hasOwn.call(oSignatureMap, h)) {
                index = oSignatureArr.length;
                oSignatureMap[h] = index;
                oSignatureArr.push(h);
              }
              else {
                index = oSignatureMap[h];
              }

              signatureEntry += `, ${String(index).padEnd(3)}`;
              x++;
            }

            while (x < 6) {
              signatureEntry += `, ${String(0).padEnd(3)}`;
              x++;
            }

            signatureEntry += `)`;
            signatureEntries.push({ data: signatureEntry, comment: signatureComment, refs: 0 });
          }

          var count = signatureEntries.length;
          var index = findSignaturesIndex(signatureEntries);

          if (index === -1)
            index = indexSignatures(signatureEntries);

          iSignatureArr[index].refs++;
          inst.signatureIndex = index;
          inst.signatureCount = count;
        }
      });
    }

    var s = `#define ROW(count, x86, x64, implicit, o0, o1, o2, o3, o4, o5)  \\\n` +
            `  { count, (x86 ? uint8_t(InstDB::kModeX86) : uint8_t(0)) |     \\\n` +
            `           (x64 ? uint8_t(InstDB::kModeX64) : uint8_t(0)) ,     \\\n` +
            `    implicit,                                                   \\\n` +
            `    0,                                                          \\\n` +
            `    { o0, o1, o2, o3, o4, o5 }                                  \\\n` +
            `  }\n` +
            StringUtils.makeCxxArrayWithComment(iSignatureArr, "const InstDB::InstSignature InstDB::_instSignatureTable[]") +
            `#undef ROW\n` +
            `\n` +
            `#define ROW(flags, mFlags, extFlags, regId) { uint32_t(flags), uint16_t(mFlags), uint8_t(extFlags), uint8_t(regId) }\n` +
            `#define F(VAL) InstDB::kOp##VAL\n` +
            `#define M(VAL) InstDB::kMemOp##VAL\n` +
            StringUtils.makeCxxArray(oSignatureArr, "const InstDB::OpSignature InstDB::_opSignatureTable[]") +
            `#undef M\n` +
            `#undef F\n` +
            `#undef ROW\n`;
    this.inject("InstSignatureTable", disclaimer(s), oSignatureArr.length * 8 + iSignatureArr.length * 8);
  }

  makeSignatures(dbInsts) {
    const signatures = new SignatureArray();
    for (var i = 0; i < dbInsts.length; i++) {
      const inst = dbInsts[i];
      const ops = inst.operands;

      // NOTE: This changed from having reg|mem merged into creating two signatures
      // instead. Imagine two instructions in one `dbInsts` array:
      //
      //   1. mov reg, reg/mem
      //   2. mov reg/mem, reg
      //
      // If we merge them and then unmerge, we will have 4 signatures, when iterated:
      //
      //   1a. mov reg, reg
      //   1b. mov reg, mem
      //   2a. mov reg, reg
      //   2b. mov mem, reg
      //
      // So, instead of merging them here, we insert separated signatures and let
      // the tool merge them in a way that can be easily unmerged at runtime into:
      //
      //   1a. mov reg, reg
      //   1b. mov reg, mem
      //   2b. mov mem, reg
      var modrmCount = 1;
      for (var modrm = 0; modrm < modrmCount; modrm++) {
        var row = new ISignature(inst.name);
        row.x86 = (inst.arch === "ANY" || inst.arch === "X86");
        row.x64 = (inst.arch === "ANY" || inst.arch === "X64");

        for (var j = 0; j < ops.length; j++) {
          var iop = ops[j];

          var reg = iop.reg;
          var mem = iop.mem;
          var imm = iop.imm;
          var rel = iop.rel;

          // Skip all instructions having implicit `imm` operand of `1`.
          if (iop.immValue !== null)
            break;

          // Shorten the number of signatures of 'mov' instruction.
          if (inst.name === "mov" && mem.startsWith("moff"))
            break;

          if (reg === "seg") reg = "sreg";
          if (reg === "st(i)") reg = "st";
          if (reg === "st(0)") reg = "st0";

          if (mem === "moff8") mem = "m8";
          if (mem === "moff16") mem = "m16";
          if (mem === "moff32") mem = "m32";
          if (mem === "moff64") mem = "m64";

          if (mem === "m32fp") mem = "m32";
          if (mem === "m64fp") mem = "m64";
          if (mem === "m80fp") mem = "m80";
          if (mem === "m80bcd") mem = "m80";
          if (mem === "m80dec") mem = "m80";
          if (mem === "m16int") mem = "m16";
          if (mem === "m32int") mem = "m32";
          if (mem === "m64int") mem = "m64";

          if (mem === "m16_16") mem = "m32";
          if (mem === "m16_32") mem = "m48";
          if (mem === "m16_64") mem = "m80";

          if (reg && mem) {
            if (modrmCount === 1) {
              mem = null;
              modrmCount++;
            }
            else {
              reg = null;
            }
          }

          const op = new OSignature();
          if (iop.implicit) {
            row.implicit++;
            op.flags.implicit = true;
          }

          const seg = iop.memSeg;
          if (seg) {
            switch (inst.name) {
              case "cmpsb": op.flags.m8 = true; break;
              case "cmpsw": op.flags.m16 = true; break;
              case "cmpsd": op.flags.m32 = true; break;
              case "cmpsq": op.flags.m64 = true; break;
              case "lodsb": op.flags.m8 = true; break;
              case "lodsw": op.flags.m16 = true; break;
              case "lodsd": op.flags.m32 = true; break;
              case "lodsq": op.flags.m64 = true; break;
              case "movsb": op.flags.m8 = true; break;
              case "movsw": op.flags.m16 = true; break;
              case "movsd": op.flags.m32 = true; break;
              case "movsq": op.flags.m64 = true; break;
              case "scasb": op.flags.m8 = true; break;
              case "scasw": op.flags.m16 = true; break;
              case "scasd": op.flags.m32 = true; break;
              case "scasq": op.flags.m64 = true; break;
              case "stosb": op.flags.m8 = true; break;
              case "stosw": op.flags.m16 = true; break;
              case "stosd": op.flags.m32 = true; break;
              case "stosq": op.flags.m64 = true; break;
              case "insb": op.flags.m8 = true; break;
              case "insw": op.flags.m16 = true; break;
              case "insd": op.flags.m32 = true; break;
              case "outsb": op.flags.m8 = true; break;
              case "outsw": op.flags.m16 = true; break;
              case "outsd": op.flags.m32 = true; break;
              case "clzero": op.flags.mem = true; op.flags.m512 = true; break;
              case "enqcmd": op.flags.mem = true; op.flags.m512 = true; break;
              case "enqcmds": op.flags.mem = true; op.flags.m512 = true; break;
              case "movdir64b": op.flags.mem = true; op.flags.m512 = true; break;
              case "maskmovq": op.flags.mem = true; op.flags.m64 = true; break;
              case "maskmovdqu": op.flags.mem = true; op.flags.m128 = true; break;
              case "vmaskmovdqu": op.flags.mem = true; op.flags.m128 = true; break;
              case "monitor": op.flags.mem = true; break;
              case "monitorx": op.flags.mem = true; break;
              case "umonitor": op.flags.mem = true; break;
              default: console.log(`UNKNOWN MEM IN INSTRUCTION '${inst.name}'`); break;
            }

            if (seg === "ds") op.flags.memDS = true;
            if (seg === "es") op.flags.memES = true;
            if (reg === "reg") { op.flags.memBase = true; }
            if (reg === "r32") { op.flags.memBase = true; }
            if (reg === "r64") { op.flags.memBase = true; }
            if (reg === "zax") { op.flags.memBase = true; op.flags.memZAX = true; }
            if (reg === "zsi") { op.flags.memBase = true; op.flags.memZSI = true; }
            if (reg === "zdi") { op.flags.memBase = true; op.flags.memZDI = true; }
          }
          else if (reg) {
            if (reg == "r8") {
              op.flags["r8lo"] = true;
              op.flags["r8hi"] = true;
            }
            else {
              op.flags[reg] = true;
            }
          }
          if (mem) {
            op.flags[mem] = true;
            // HACK: Allow LEA|CL*|PREFETCH* to use any memory size.
            if (/^(cldemote|clwb|clflush\w*|lea|prefetch\w*)$/.test(inst.name)) {
              op.flags.mem = true;
              Object.assign(op.flags, MemOp);
            }

            // HACK: These instructions specify explicit memory size, but it's just informational.
            if (inst.name === "enqcmd" || inst.name === "enqcmds" || inst.name === "movdir64b")
              op.flags.mem = true;

          }
          if (imm) {
            if (iop.immSign === "any" || iop.immSign === "signed"  ) op.flags["i" + imm] = true;
            if (iop.immSign === "any" || iop.immSign === "unsigned") op.flags["u" + imm] = true;
          }
          if (rel) op.flags["rel" + rel] = true;

          row.push(op);
        }

        // Not equal if we terminated the loop.
        if (j === ops.length)
          signatures.push(row);
      }
    }

    if (signatures.length && GenUtils.canUseImplicitMemSize(dbInsts[0].name))
      signatures.calcImplicitMemSize();

    signatures.compact();
    return signatures;
  }
}

// ============================================================================
// [tablegen.x86.InstCommonInfoTableB]
// ============================================================================

class InstCommonInfoTableB extends core.Task {
  constructor() {
    super("InstCommonInfoTableB");
  }

  run() {
    const insts = this.ctx.insts;
    const commonTableB = new IndexedArray();
    const rwInfoTable = new IndexedArray();

    // If the instruction doesn't read any flags it should point to the first index.
    rwInfoTable.addIndexed(`{ 0, 0 }`);

    insts.forEach((inst) => {
      const dbInsts = inst.dbInsts;

      var features = GenUtils.cpuFeaturesOf(dbInsts).map(function(f) { return `EXT(${f})`; }).join(", ");
      if (!features) features = "0";

      var [r, w] = this.rwFlagsOf(dbInsts);
      const rData = r.map(function(flag) { return `FLAG(${flag})`; }).join(" | ") || "0";
      const wData = w.map(function(flag) { return `FLAG(${flag})`; }).join(" | ") || "0";
      const rwDataIndex = rwInfoTable.addIndexed(`{ ${rData}, ${wData} }`);

      inst.commomInfoIndexB = commonTableB.addIndexed(`{ { ${features} }, ${rwDataIndex}, 0 }`);
    });

    var s = `#define EXT(VAL) uint32_t(Features::k##VAL)\n` +
            `const InstDB::CommonInfoTableB InstDB::_commonInfoTableB[] = {\n${StringUtils.format(commonTableB, kIndent, true)}\n};\n` +
            `#undef EXT\n` +
            `\n` +
            `#define FLAG(VAL) uint32_t(Status::k##VAL)\n` +
            `const InstDB::RWFlagsInfoTable InstDB::_rwFlagsInfoTable[] = {\n${StringUtils.format(rwInfoTable, kIndent, true)}\n};\n` +
            `#undef FLAG\n`;
    this.inject("InstCommonInfoTableB", disclaimer(s), commonTableB.length * 8 + rwInfoTable.length * 8);
  }

  rwFlagsOf(dbInsts) {
    const r = Object.create(null);
    const w = Object.create(null);

    for (var i = 0; i < dbInsts.length; i++) {
      const dbInst = dbInsts[i];

      // Omit special cases, this is handled well in C++ code.
      if (dbInst.name === "mov")
        continue;

      const specialRegs = dbInst.specialRegs;

      // Mov is a special case, moving to/from control regs makes flags undefined,
      // which we don't want to have in `X86InstDB::operationData`. This is, thus,
      // a special case instruction analyzer must deal with.
      if (dbInst.name === "mov")
        continue;

      for (var specialReg in specialRegs) {
        var flag = "";
        switch (specialReg) {
          case "FLAGS.CF": flag = "CF"; break;
          case "FLAGS.OF": flag = "OF"; break;
          case "FLAGS.SF": flag = "SF"; break;
          case "FLAGS.ZF": flag = "ZF"; break;
          case "FLAGS.AF": flag = "AF"; break;
          case "FLAGS.PF": flag = "PF"; break;
          case "FLAGS.DF": flag = "DF"; break;
          case "FLAGS.IF": flag = "IF"; break;
        //case "FLAGS.TF": flag = "TF"; break;
          case "FLAGS.AC": flag = "AC"; break;
          case "X86SW.C0": flag = "C0"; break;
          case "X86SW.C1": flag = "C1"; break;
          case "X86SW.C2": flag = "C2"; break;
          case "X86SW.C3": flag = "C3"; break;
          default:
            continue;
        }

        switch (specialRegs[specialReg]) {
          case "R":
            r[flag] = true;
            break;
          case "X":
            r[flag] = true;
            // ... fallthrough ...
          case "W":
          case "U":
          case "0":
          case "1":
            w[flag] = true;
            break;
        }
      }
    }

    return [ArrayUtils.sorted(r), ArrayUtils.sorted(w)];
  }
}

// ============================================================================
// [tablegen.x86.InstRWInfoTable]
// ============================================================================

const NOT_MEM_AMBIGUOUS = MapUtils.arrayToMap([
  "call", "movq"
]);

class InstRWInfoTable extends core.Task {
  constructor() {
    super("InstRWInfoTable");

    this.rwInfoIndexA = [];
    this.rwInfoIndexB = [];
    this.rwInfoTableA = new IndexedArray();
    this.rwInfoTableB = new IndexedArray();

    this.rmInfoTable = new IndexedArray();
    this.opInfoTable = new IndexedArray();

    this.rwCategoryByName = {
      "imul"      : "Imul",
      "mov"       : "Mov",
      "movabs"    : "Movabs",
      "movhpd"    : "Movh64",
      "movhps"    : "Movh64",
      "punpcklbw" : "Punpcklxx",
      "punpckldq" : "Punpcklxx",
      "punpcklwd" : "Punpcklxx",
      "vmaskmovpd": "Vmaskmov",
      "vmaskmovps": "Vmaskmov",
      "vmovddup"  : "Vmovddup",
      "vmovmskpd" : "Vmovmskpd",
      "vmovmskps" : "Vmovmskps",
      "vpmaskmovd": "Vmaskmov",
      "vpmaskmovq": "Vmaskmov"
    };

    const _ = null;
    this.rwCategoryByData = {
      Vmov1_8: [
        [{access: "W", flags: {}, fixed: -1, index: 0, width:  8}, {access: "R", flags: {}, fixed: -1, index: 0, width: 64},_,_,_,_],
        [{access: "W", flags: {}, fixed: -1, index: 0, width: 16}, {access: "R", flags: {}, fixed: -1, index: 0, width:128},_,_,_,_],
        [{access: "W", flags: {}, fixed: -1, index: 0, width: 32}, {access: "R", flags: {}, fixed: -1, index: 0, width:256},_,_,_,_],
        [{access: "W", flags: {}, fixed: -1, index: 0, width: 64}, {access: "R", flags: {}, fixed: -1, index: 0, width:512},_,_,_,_]
      ],
      Vmov1_4: [
        [{access: "W", flags: {}, fixed: -1, index: 0, width: 32}, {access: "R", flags: {}, fixed: -1, index: 0, width:128},_,_,_,_],
        [{access: "W", flags: {}, fixed: -1, index: 0, width: 64}, {access: "R", flags: {}, fixed: -1, index: 0, width:256},_,_,_,_],
        [{access: "W", flags: {}, fixed: -1, index: 0, width:128}, {access: "R", flags: {}, fixed: -1, index: 0, width:512},_,_,_,_]
      ],
      Vmov1_2: [
        [{access: "W", flags: {}, fixed: -1, index: 0, width: 64}, {access: "R", flags: {}, fixed: -1, index: 0, width:128},_,_,_,_],
        [{access: "W", flags: {}, fixed: -1, index: 0, width:128}, {access: "R", flags: {}, fixed: -1, index: 0, width:256},_,_,_,_],
        [{access: "W", flags: {}, fixed: -1, index: 0, width:256}, {access: "R", flags: {}, fixed: -1, index: 0, width:512},_,_,_,_]
      ],
      Vmov2_1: [
        [{access: "W", flags: {}, fixed: -1, index: 0, width: 128}, {access: "R", flags: {}, fixed: -1, index: 0, width: 64},_,_,_,_],
        [{access: "W", flags: {}, fixed: -1, index: 0, width: 256}, {access: "R", flags: {}, fixed: -1, index: 0, width:128},_,_,_,_],
        [{access: "W", flags: {}, fixed: -1, index: 0, width: 512}, {access: "R", flags: {}, fixed: -1, index: 0, width:256},_,_,_,_]
      ],
      Vmov4_1: [
        [{access: "W", flags: {}, fixed: -1, index: 0, width: 128}, {access: "R", flags: {}, fixed: -1, index: 0, width: 32},_,_,_,_],
        [{access: "W", flags: {}, fixed: -1, index: 0, width: 256}, {access: "R", flags: {}, fixed: -1, index: 0, width: 64},_,_,_,_],
        [{access: "W", flags: {}, fixed: -1, index: 0, width: 512}, {access: "R", flags: {}, fixed: -1, index: 0, width:128},_,_,_,_]
      ],
      Vmov8_1: [
        [{access: "W", flags: {}, fixed: -1, index: 0, width: 128}, {access: "R", flags: {}, fixed: -1, index: 0, width: 16},_,_,_,_],
        [{access: "W", flags: {}, fixed: -1, index: 0, width: 256}, {access: "R", flags: {}, fixed: -1, index: 0, width: 32},_,_,_,_],
        [{access: "W", flags: {}, fixed: -1, index: 0, width: 512}, {access: "R", flags: {}, fixed: -1, index: 0, width: 64},_,_,_,_]
      ]
    };
  }

  run() {
    const insts = this.ctx.insts;

    const noRmInfo = CxxUtils.struct(
      "InstDB::RWInfoRm::kCategory" + "None".padEnd(10),
      StringUtils.decToHex(0, 2),
      String(0).padEnd(2),
      CxxUtils.flags({}),
      "0"
    );

    const noOpInfo = CxxUtils.struct(
      "0x0000000000000000u",
      "0x0000000000000000u",
      "0xFF",
      CxxUtils.struct(0),
      "0"
    );

    this.rmInfoTable.addIndexed(noRmInfo);
    this.opInfoTable.addIndexed(noOpInfo);

    insts.forEach((inst) => {
      // Alternate forms would only mess this up, so filter them out.
      const dbInsts = Filter.noAltForm(inst.dbInsts);

      // The best we can do is to divide instructions that have 2 operands and others.
      // This gives us the highest chance of preventing special cases (which were not
      // entirely avoided).
      const o2Insts = dbInsts.filter((inst) => { return inst.operands.length === 2; });
      const oxInsts = dbInsts.filter((inst) => { return inst.operands.length !== 2; });

      const rwInfoArray = [this.rwInfo(inst, o2Insts), this.rwInfo(inst, oxInsts)];
      const rmInfoArray = [this.rmInfo(inst, o2Insts), this.rmInfo(inst, oxInsts)];

      for (var i = 0; i < 2; i++) {
        const rwInfo = rwInfoArray[i];
        const rmInfo = rmInfoArray[i];

        const rwOps = rwInfo.rwOps;
        const rwOpsIndex = [];
        for (var j = 0; j < rwOps.length; j++) {
          const op = rwOps[j];
          if (!op) {
            rwOpsIndex.push(this.opInfoTable.addIndexed(noOpInfo));
            continue;
          }

          const flags = {};
          const opAcc = op.access;

          if (opAcc === "R") flags.Read = true;
          if (opAcc === "W") flags.Write = true;
          if (opAcc === "X") flags.RW = true;
          Lang.merge(flags, op.flags);

          const rIndex = opAcc === "X" || opAcc === "R" ? op.index : -1;
          const rWidth = opAcc === "X" || opAcc === "R" ? op.width : -1;
          const wIndex = opAcc === "X" || opAcc === "W" ? op.index : -1;
          const wWidth = opAcc === "X" || opAcc === "W" ? op.width : -1;

          const opData = CxxUtils.struct(
            this.byteMaskFromBitRanges([{ start: rIndex, end: rIndex + rWidth - 1 }]) + "u",
            this.byteMaskFromBitRanges([{ start: wIndex, end: wIndex + wWidth - 1 }]) + "u",
            StringUtils.decToHex(op.fixed === -1 ? 0xFF : op.fixed, 2),
            CxxUtils.struct(0),
            CxxUtils.flags(flags, function(flag) { return "OpRWInfo::k" + flag; })
          );

          rwOpsIndex.push(this.opInfoTable.addIndexed(opData));
        }

        const rmData = CxxUtils.struct(
          "InstDB::RWInfoRm::kCategory" + rmInfo.category.padEnd(10),
          StringUtils.decToHex(rmInfo.rmIndexes, 2),
          String(Math.max(rmInfo.memFixed, 0)).padEnd(2),
          CxxUtils.flags({ "InstDB::RWInfoRm::kFlagAmbiguous": Boolean(rmInfo.memAmbiguous) }),
          rmInfo.memExtension === "None" ? "0" : "Features::k" + rmInfo.memExtension
        );

        const rwData = CxxUtils.struct(
          "InstDB::RWInfo::kCategory" + rwInfo.category.padEnd(10),
          String(this.rmInfoTable.addIndexed(rmData)).padEnd(2),
          CxxUtils.struct(...(rwOpsIndex.map(function(item) { return String(item).padEnd(2); })))
        );

        if (i == 0)
          this.rwInfoIndexA.push(this.rwInfoTableA.addIndexed(rwData));
        else
          this.rwInfoIndexB.push(this.rwInfoTableB.addIndexed(rwData));
      }
    });

    var s = "";
    s += "const uint8_t InstDB::rwInfoIndexA[Inst::_kIdCount] = {\n" + StringUtils.format(this.rwInfoIndexA, kIndent, -1) + "\n};\n";
    s += "\n";
    s += "const uint8_t InstDB::rwInfoIndexB[Inst::_kIdCount] = {\n" + StringUtils.format(this.rwInfoIndexB, kIndent, -1) + "\n};\n";
    s += "\n";
    s += "const InstDB::RWInfo InstDB::rwInfoA[] = {\n" + StringUtils.format(this.rwInfoTableA, kIndent, true) + "\n};\n";
    s += "\n";
    s += "const InstDB::RWInfo InstDB::rwInfoB[] = {\n" + StringUtils.format(this.rwInfoTableB, kIndent, true) + "\n};\n";
    s += "\n";
    s += "const InstDB::RWInfoOp InstDB::rwInfoOp[] = {\n" + StringUtils.format(this.opInfoTable, kIndent, true) + "\n};\n";
    s += "\n";
    s += "const InstDB::RWInfoRm InstDB::rwInfoRm[] = {\n" + StringUtils.format(this.rmInfoTable, kIndent, true) + "\n};\n";

    const size = this.rwInfoIndexA.length +
                 this.rwInfoIndexB.length +
                 this.rwInfoTableA.length * 8 +
                 this.rwInfoTableB.length * 8 +
                 this.rmInfoTable.length * 4 +
                 this.opInfoTable.length * 24;

    this.inject("InstRWInfoTable", disclaimer(s), size);
  }

  byteMaskFromBitRanges(ranges) {
    const arr = [];
    for (var i = 0; i < 64; i++)
      arr.push(0);

    for (var i = 0; i < ranges.length; i++) {
      const start = ranges[i].start;
      const end = ranges[i].end;

      if (start < 0)
        continue;

      for (var j = start; j <= end; j++) {
        const bytePos = j >> 3;
        if (bytePos < 0 || bytePos >= arr.length)
          FAIL(`Range ${start}:${end} cannot be used to create a byte-mask`);
        arr[bytePos] = 1;
      }
    }

    var s = "0x";
    for (var i = arr.length - 4; i >= 0; i -= 4) {
      const value = (arr[i + 3] << 3) | (arr[i + 2] << 2) | (arr[i + 1] << 1) | arr[i];
      s += value.toString(16).toUpperCase();
    }
    return s;
  }

  // Read/Write Info
  // ---------------

  rwInfo(asmInst, dbInsts) {
    const self = this;

    function nullOps() {
      return [null, null, null, null, null, null];
    }

    function makeRwFromOp(op) {
      if (!op.isRegOrMem())
        return null;

      return {
        access: op.read && op.write ? "X" : op.read ? "R" : op.write ? "W" : "?",
        flags: {},
        fixed: GenUtils.fixedRegOf(op.reg),
        index: op.rwxIndex,
        width: op.rwxWidth
      };
    }

    function queryRwGeneric(dbInsts, step) {
      var rwOps = nullOps();
      for (var i = 0; i < dbInsts.length; i++) {
        const dbInst = dbInsts[i];
        const operands = dbInst.operands;

        for (var j = 0; j < operands.length; j++) {
          const op = operands[j];
          if (!op.isRegOrMem())
            continue;

          const opSize = op.isReg() ? op.regSize : op.memSize;
          var d = {
            access: op.read && op.write ? "X" : op.read ? "R" : op.write ? "W" : "?",
            flags: {},
            fixed: -1,
            index: -1,
            width: -1
          };

          if (op.isReg())
            d.fixed = GenUtils.fixedRegOf(op.reg);
          else
            d.fixed = GenUtils.fixedRegOf(op.mem);

          if (op.zext)
            d.flags.ZExt = true;

          for (var k in self.rwOpFlagsForInstruction(asmInst.name, j))
            d.flags[k] = true;

          if ((step === -1 || step === j) || op.rwxIndex !== 0 || op.rwxWidth !== opSize) {
            d.index = op.rwxIndex;
            d.width = op.rwxWidth;
          }

          if (d.fixed !== -1) {
            if (op.memSeg)
              d.flags.MemPhysId = true;
            else
              d.flags.RegPhysId = true;
          }

          if (rwOps[j] === null) {
            rwOps[j] = d;
          }
          else {
            if (!Lang.deepEqExcept(rwOps[j], d, { "fixed": true, "flags": true }))
              return null;

            if (rwOps[j].fixed === -1)
              rwOps[j].fixed = d.fixed;
            Lang.merge(rwOps[j].flags, d.flags);
          }
        }
      }
      return { category: "Generic", rwOps };
    }

    function queryRwByData(dbInsts, rwOpsArray) {
      for (var i = 0; i < dbInsts.length; i++) {
        const dbInst = dbInsts[i];
        const operands = dbInst.operands;
        const rwOps = nullOps();

        for (var j = 0; j < operands.length; j++)
          rwOps[j] = makeRwFromOp(operands[j])

        var match = 0;
        for (var j = 0; j < rwOpsArray.length; j++)
          match |= Lang.deepEq(rwOps, rwOpsArray[j]);

        if (!match)
          return false;
      }

      return true;
    }

    function dumpRwToData(dbInsts) {
      const out = [];
      for (var i = 0; i < dbInsts.length; i++) {
        const dbInst = dbInsts[i];
        const operands = dbInst.operands;
        const rwOps = nullOps();

        for (var j = 0; j < operands.length; j++)
          rwOps[j] = makeRwFromOp(operands[j])

        if (ArrayUtils.deepIndexOf(out, rwOps) !== -1)
          continue;

        out.push(rwOps);
      }
      return out;
    }

    // Some instructions are just special...
    const name = dbInsts.length ? dbInsts[0].name : "";
    if (name in this.rwCategoryByName)
      return { category: this.rwCategoryByName[name], rwOps: nullOps() };

    // Generic rules.
    for (var i = -1; i <= 6; i++) {
      const rwInfo = queryRwGeneric(dbInsts, i);
      if (rwInfo)
        return rwInfo;
    }

    // Specific rules.
    for (var k in this.rwCategoryByData)
      if (queryRwByData(dbInsts, this.rwCategoryByData[k]))
        return { category: k, rwOps: nullOps() };

    // FAILURE: Missing data to categorize this instruction.
    if (name) {
      const items = dumpRwToData(dbInsts)
      console.log(`RW: ${dbInsts.length ? dbInsts[0].name : ""}:`);
      items.forEach((item) => {
        console.log("  " + JSON.stringify(item));
      });
    }

    return null;
  }

  rwOpFlagsForInstruction(instName, opIndex) {
    const toMap = MapUtils.arrayToMap;

    // TODO: We should be able to get this information from asmdb.
    switch (instName + "@" + opIndex) {
      case "cmps@0": return toMap(['MemBaseRW', 'MemBasePostModify']);
      case "cmps@1": return toMap(['MemBaseRW', 'MemBasePostModify']);
      case "movs@0": return toMap(['MemBaseRW', 'MemBasePostModify']);
      case "movs@1": return toMap(['MemBaseRW', 'MemBasePostModify']);
      case "lods@1": return toMap(['MemBaseRW', 'MemBasePostModify']);
      case "stos@0": return toMap(['MemBaseRW', 'MemBasePostModify']);
      case "scas@1": return toMap(['MemBaseRW', 'MemBasePostModify']);
      case "bndstx@0": return toMap(['MemBaseWrite', 'MemIndexWrite']);

      default:
        return {};
    }
  }

  // Reg/Mem Info
  // ------------

  rmInfo(asmInst, dbInsts) {
    const info = {
      category: "None",
      rmIndexes: this.rmReplaceableIndexes(dbInsts),
      memFixed: this.rmFixedSize(dbInsts),
      memAmbiguous: this.rmIsAmbiguous(dbInsts),
      memConsistent: this.rmIsConsistent(dbInsts),
      memExtension: this.rmExtension(dbInsts)
    };

    if (info.memFixed !== -1)
      info.category = "Fixed";
    else if (info.memConsistent)
      info.category = "Consistent";
    else if (info.rmIndexes)
      info.category = this.rmReplaceableCategory(dbInsts);

    return info;
  }

  rmReplaceableCategory(dbInsts) {
    var category = null;

    for (var i = 0; i < dbInsts.length; i++) {
      const dbInst = dbInsts[i];
      const operands = dbInst.operands;

      var rs = -1;
      var ms = -1;

      for (var j = 0; j < operands.length; j++) {
        const op = operands[j];
        if (op.isMem())
          ms = op.memSize;
        else if (op.isReg())
          rs = Math.max(rs, op.regSize);
      }

      var c = (rs === -1    ) ? "None"    :
              (ms === -1    ) ? "None"    :
              (ms === rs    ) ? "Fixed"   :
              (ms === rs / 2) ? "Half"    :
              (ms === rs / 4) ? "Quarter" :
              (ms === rs / 8) ? "Eighth"  : "Unknown";

      if (category === null)
        category = c;
      else if (category !== c) {
        // Special cases.
        if (dbInst.name === "mov" || dbInst.name === "vmovddup")
          return "None";

        if (/^(punpcklbw|punpckldq|punpcklwd)$/.test(dbInst.name))
          return "None";

        return StringUtils.capitalize(dbInst.name);
      }
    }

    if (category === "Unknown")
      console.log(`Instruction '${dbInsts[0].name}' has no RMInfo category.`);

    return category || "Unknown";
  }

  rmReplaceableIndexes(dbInsts) {
    function maskOf(inst, fn) {
      var m = 0;
      var operands = inst.operands;
      for (var i = 0; i < operands.length; i++)
        if (fn(operands[i]))
          m |= (1 << i);
      return m;
    }

    function getRegIndexes(inst) { return maskOf(inst, function(op) { return op.isReg(); }); };
    function getMemIndexes(inst) { return maskOf(inst, function(op) { return op.isMem(); }); };

    var mask = 0;

    for (var i = 0; i < dbInsts.length; i++) {
      const dbInst = dbInsts[i];

      var mi = getMemIndexes(dbInst);
      var ri = getRegIndexes(dbInst) & ~mi;

      if (!mi)
        continue;

      const match = dbInsts.some((inst) => {
        var ti = getRegIndexes(inst);
        return ((ri & ti) === ri && (mi & ti) === mi);
      });

      if (!match)
        return 0;
      mask |= mi;
    }

    return mask;
  }

  rmFixedSize(insts) {
    var savedOp = null;

    for (var i = 0; i < insts.length; i++) {
      const inst = insts[i];
      const operands = inst.operands;

      for (var j = 0; j < operands.length; j++) {
        const op = operands[j];
        if (op.mem) {
          if (savedOp && savedOp.mem !== op.mem)
            return -1;
          savedOp = op;
        }
      }
    }

    return savedOp ? Math.max(savedOp.memSize, 0) / 8 : -1;
  }

  rmIsConsistent(insts) {
    var hasMem = 0;
    for (var i = 0; i < insts.length; i++) {
      const inst = insts[i];
      const operands = inst.operands;
      for (var j = 0; j < operands.length; j++) {
        const op = operands[j];
        if (op.mem) {
          hasMem = 1;
          if (!op.reg)
            return 0;
          if (asmdb.x86.Utils.regSize(op.reg) !== op.memSize)
            return 0;
        }
      }
    }
    return hasMem;
  }

  rmIsAmbiguous(dbInsts) {
    function isAmbiguous(dbInsts) {
      const memMap = {};
      const immMap = {};

      for (var i = 0; i < dbInsts.length; i++) {
        const dbInst = dbInsts[i];
        const operands = dbInst.operands;

        var memStr = "";
        var immStr = "";
        var hasMem = false;
        var hasImm = false;

        for (var j = 0; j < operands.length; j++) {
          const op = operands[j];
          if (j) {
            memStr += ", ";
            immStr += ", ";
          }

          if (op.isImm()) {
            immStr += "imm";
            hasImm = true;
          }
          else {
            immStr += op.toString();
          }

          if (op.mem) {
            memStr += "m";
            hasMem = true;
          }
          else {
            memStr += op.isImm() ? "imm" : op.toString();
          }
        }

        if (hasImm) {
          if (immMap[immStr] === true)
            continue;
          immMap[immStr] = true;
        }

        if (hasMem) {
          if (memMap[memStr] === true)
            return 1;
          memMap[memStr] = true;
        }
      }
      return 0;
    }

    const uniqueInsts = Filter.unique(dbInsts);

    // Special cases.
    if (!dbInsts.length)
      return 0;

    if (NOT_MEM_AMBIGUOUS[dbInsts[0].name])
      return 0;

    return (isAmbiguous(Filter.byArch(uniqueInsts, "X86")) << 0) |
           (isAmbiguous(Filter.byArch(uniqueInsts, "X64")) << 1) ;
  }

  rmExtension(dbInsts) {
    if (!dbInsts.length)
      return "None";

    const name = dbInsts[0].name;
    switch (name) {
      case "pextrw":
        return "SSE4_1";

      case "vpslldq":
      case "vpsrldq":
        return "AVX512_BW";

      default:
        return "None";
    }
  }
}

// ============================================================================
// [tablegen.x86.InstCommonTable]
// ============================================================================

class InstCommonTable extends core.Task {
  constructor() {
    super("InstCommonTable", [
      "IdEnum",
      "NameTable",
      "InstSignatureTable",
      "InstCommonInfoTableB",
      "InstRWInfoTable"
    ]);
  }

  run() {
    const insts = this.ctx.insts;
    const table = new IndexedArray();

    insts.forEach((inst) => {
      const commonFlagsArray = inst.flags.filter((flag) => { return !flag.startsWith("Avx512"); });
      const avx512FlagsArray = inst.flags.filter((flag) => { return  flag.startsWith("Avx512"); });

      const commonFlags = commonFlagsArray.map(function(flag) { return `F(${flag          })`; }).join("|") || "0";
      const avx512Flags = avx512FlagsArray.map(function(flag) { return `X(${flag.substr(6)})`; }).join("|") || "0";

      const singleRegCase = `SINGLE_REG(${inst.singleRegCase})`;
      const controlType = `CONTROL(${inst.controlType})`;

      const row = "{ " +
        String(commonFlags        ).padEnd(50) + ", " +
        String(avx512Flags        ).padEnd(30) + ", " +
        String(inst.signatureIndex).padEnd( 3) + ", " +
        String(inst.signatureCount).padEnd( 2) + ", " +
        String(controlType        ).padEnd(16) + ", " +
        String(singleRegCase      ).padEnd(16) + "}";
      inst.commonInfoIndexA = table.addIndexed(row);
    });

    var s = `#define F(VAL) InstDB::kFlag##VAL\n` +
            `#define X(VAL) InstDB::kAvx512Flag##VAL\n` +
            `#define CONTROL(VAL) Inst::kControl##VAL\n` +
            `#define SINGLE_REG(VAL) InstDB::kSingleReg##VAL\n` +
            `const InstDB::CommonInfo InstDB::_commonInfoTable[] = {\n${StringUtils.format(table, kIndent, true)}\n};\n` +
            `#undef SINGLE_REG\n` +
            `#undef CONTROL\n` +
            `#undef X\n` +
            `#undef F\n`;
    this.inject("InstCommonTable", disclaimer(s), table.length * 8);
  }
}

// ============================================================================
// [Main]
// ============================================================================

new X86TableGen()
  .addTask(new IdEnum())
  .addTask(new NameTable())
  .addTask(new AltOpcodeTable())
  .addTask(new InstSignatureTable())
  .addTask(new InstCommonInfoTableB())
  .addTask(new InstRWInfoTable())
  .addTask(new InstCommonTable())
  .run();
