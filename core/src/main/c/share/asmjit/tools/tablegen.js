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
// tablegen.js
//
// Provides core foundation for generating tables that AsmJit requires. This
// file should provide everything table generators need in general.
// ============================================================================

"use strict";

const VERBOSE = false;

// ============================================================================
// [Imports]
// ============================================================================

const fs = require("fs");
const hasOwn = Object.prototype.hasOwnProperty;

const asmdb = (function() {
  // Try to import a local 'asmdb' package, if available.
  try {
    return require("./asmdb");
  }
  catch (ex) {
    if (ex.code !== "MODULE_NOT_FOUND") {
      console.log(`FATAL ERROR: ${ex.message}`);
      throw ex;
    }
  }

  // Try to import global 'asmdb' package as local package is not available.
  return require("asmdb");
})();
exports.asmdb = asmdb;

// ============================================================================
// [Constants]
// ============================================================================

const kIndent = "  ";
const kJustify = 119;
const kAsmJitRoot = "..";

exports.kIndent = kIndent;
exports.kJustify = kJustify;
exports.kAsmJitRoot = kAsmJitRoot;

// ============================================================================
// [Debugging]
// ============================================================================

function DEBUG(msg) {
  if (VERBOSE)
    console.log(msg);
}
exports.DEBUG = DEBUG;

function WARN(msg) {
  console.log(msg);
}
exports.WARN = WARN;

function FAIL(msg) {
  console.log(`FATAL ERROR: ${msg}`);
  throw new Error(msg);
}
exports.FAIL = FAIL;

// ============================================================================
// [Lang]
// ============================================================================

function nop(x) { return x; }

class Lang {
  static merge(a, b) {
    if (a === b)
      return a;

    for (var k in b) {
      var av = a[k];
      var bv = b[k];

      if (typeof av === "object" && typeof bv === "object")
        Lang.merge(av, bv);
      else
        a[k] = bv;
    }

    return a;
  }

  static deepEq(a, b) {
    if (a === b)
      return true;

    if (typeof a !== typeof b)
      return false;

    if (typeof a !== "object")
      return a === b;

    if (Array.isArray(a) || Array.isArray(b)) {
      if (Array.isArray(a) !== Array.isArray(b))
        return false;

      const len = a.length;
      if (b.length !== len)
        return false;

      for (var i = 0; i < len; i++)
        if (!Lang.deepEq(a[i], b[i]))
          return false;
    }
    else {
      if (a === null || b === null)
        return a === b;

      for (var k in a)
        if (!hasOwn.call(b, k) || !Lang.deepEq(a[k], b[k]))
          return false;

      for (var k in b)
        if (!hasOwn.call(a, k))
          return false;
    }

    return true;
  }

  static deepEqExcept(a, b, except) {
    if (a === b)
      return true;

    if (typeof a !== "object" || typeof b !== "object" || Array.isArray(a) || Array.isArray(b))
      return Lang.deepEq(a, b);

    for (var k in a)
      if (!hasOwn.call(except, k) && (!hasOwn.call(b, k) || !Lang.deepEq(a[k], b[k])))
        return false;

    for (var k in b)
      if (!hasOwn.call(except, k) && !hasOwn.call(a, k))
        return false;

    return true;
  }
}
exports.Lang = Lang;

// ============================================================================
// [StringUtils]
// ============================================================================

class StringUtils {
  static asString(x) { return String(x); }

  static capitalize(s) {
    s = String(s);
    return !s ? s : s[0].toUpperCase() + s.substr(1);
  }

  static trimLeft(s) { return s.replace(/^\s+/, ""); }
  static trimRight(s) { return s.replace(/\s+$/, ""); }

  static upFirst(s) {
    if (!s) return "";
    return s[0].toUpperCase() + s.substr(1);
  }

  static decToHex(n, nPad) {
    var hex = Number(n < 0 ? 0x100000000 + n : n).toString(16);
    while (nPad > hex.length)
      hex = "0" + hex;
    return "0x" + hex.toUpperCase();
  }

  static format(array, indent, showIndex, mapFn) {
    if (!mapFn)
      mapFn = StringUtils.asString;

    var s = "";
    var threshold = 80;

    if (showIndex === -1)
      s += indent;

    for (var i = 0; i < array.length; i++) {
      const item = array[i];
      const last = i === array.length - 1;

      if (showIndex !== -1)
        s += indent;

      s += mapFn(item);
      if (showIndex > 0) {
        s += `${last ? " " : ","} // #${i}`;
        if (typeof array.refCountOf === "function")
          s += ` [ref=${array.refCountOf(item)}x]`;
      }
      else if (!last) {
        s += ",";
      }

      if (showIndex === -1) {
        if (s.length >= threshold - 1 && !last) {
          s += "\n" + indent;
          threshold += 80;
        }
        else {
          if (!last) s += " ";
        }
      }
      else {
        if (!last) s += "\n";
      }
    }

    return s;
  }

  static makeCxxArray(array, code, indent) {
    if (!indent) indent = kIndent;
    return `${code} = {\n${indent}` + array.join(`,\n${indent}`) + `\n};\n`;
  }

  static makeCxxArrayWithComment(array, code, indent) {
    if (!indent) indent = kIndent;
    var s = "";
    for (var i = 0; i < array.length; i++) {
      const last = i === array.length - 1;
      s += indent + array[i].data +
           (last ? "  // " : ", // ") + (array[i].refs ? "#" + String(i) : "").padEnd(5) + array[i].comment + "\n";
    }
    return `${code} = {\n${s}};\n`;
  }

  static disclaimer(s) {
    return "// ------------------- Automatically generated, do not edit -------------------\n" +
           s +
           "// ----------------------------------------------------------------------------\n";
  }

  static indent(s, indentation) {
    var lines = s.split(/\r?\n/g);
    if (indentation) {
      for (var i = 0; i < lines.length; i++) {
        var line = lines[i];
        if (line) lines[i] = indentation + line;
      }
    }

    return lines.join("\n");
  }

  static inject(s, start, end, code) {
    var iStart = s.indexOf(start);
    var iEnd   = s.indexOf(end);

    if (iStart === -1)
      FAIL(`Utils.inject(): Couldn't locate start mark '${start}'`);

    if (iEnd === -1)
      FAIL(`Utils.inject(): Couldn't locate end mark '${end}'`);

    var nIndent = 0;
    while (iStart > 0 && s[iStart-1] === " ") {
      iStart--;
      nIndent++;
    }

    if (nIndent) {
      const indentation = " ".repeat(nIndent);
      code = StringUtils.indent(code, indentation) + indentation;
    }

    return s.substr(0, iStart + start.length + nIndent) + code + s.substr(iEnd);
  }

  static makePriorityCompare(priorityArray) {
    const map = Object.create(null);
    priorityArray.forEach((str, index) => { map[str] = index; });

    return function(a, b) {
      const ax = hasOwn.call(map, a) ? map[a] : Infinity;
      const bx = hasOwn.call(map, b) ? map[b] : Infinity;
      return ax != bx ? ax - bx : a < b ? -1 : a > b ? 1 : 0;
    }
  }
}
exports.StringUtils = StringUtils;

// ============================================================================
// [ArrayUtils]
// ============================================================================

class ArrayUtils {
  static min(arr, fn) {
    if (!arr.length)
      return null;

    if (!fn)
      fn = nop;

    var v = fn(arr[0]);
    for (var i = 1; i < arr.length; i++)
      v = Math.min(v, fn(arr[i]));
    return v;
  }

  static max(arr, fn) {
    if (!arr.length)
      return null;

    if (!fn)
      fn = nop;

    var v = fn(arr[0]);
    for (var i = 1; i < arr.length; i++)
      v = Math.max(v, fn(arr[i]));
    return v;
  }

  static sorted(obj, cmp) {
    const out = Array.isArray(obj) ? obj.slice() : Object.getOwnPropertyNames(obj);
    out.sort(cmp);
    return out;
  }

  static deepIndexOf(arr, what) {
    for (var i = 0; i < arr.length; i++)
      if (Lang.deepEq(arr[i], what))
        return i;
    return -1;
  }
}
exports.ArrayUtils = ArrayUtils;

// ============================================================================
// [MapUtils]
// ============================================================================

class MapUtils {
  static clone(map) {
    return Object.assign(Object.create(null), map);
  }

  static arrayToMap(arr, value) {
    if (value === undefined)
      value = true;

    const out = Object.create(null);
    for (var i = 0; i < arr.length; i++)
      out[arr[i]] = value;
    return out;
  }

  static equals(a, b) {
    for (var k in a) if (!hasOwn.call(b, k)) return false;
    for (var k in b) if (!hasOwn.call(a, k)) return false;
    return true;
  }

  static firstOf(map, flags) {
    for (var k in flags)
      if (hasOwn.call(map, k))
        return k;
    return undefined;
  }

  static anyOf(map, flags) {
    for (var k in flags)
      if (hasOwn.call(map, k))
        return true;
    return false;
  }

  static add(a, b) {
    for (var k in b)
      a[k] = b[k];
    return a;
  }

  static and(a, b) {
    const out = Object.create(null);
    for (var k in a)
      if (hasOwn.call(b, k))
        out[k] = true;
    return out;
  }

  static xor(a, b) {
    const out = Object.create(null);
    for (var k in a) if (!hasOwn.call(b, k)) out[k] = true;
    for (var k in b) if (!hasOwn.call(a, k)) out[k] = true;
    return out;
  }
};
exports.MapUtils = MapUtils;

// ============================================================================
// [CxxUtils]
// ============================================================================

class CxxUtils {
  static flags(obj, fn) {
    if (!fn)
      fn = nop;

    var out = "";
    for (var k in obj) {
      if (obj[k])
        out += (out ? " | " : "") + fn(k);
    }
    return out ? out : "0";
  }

  static struct(...args) {
    return "{ " + args.join(", ") + " }";
  }
};
exports.CxxUtils = CxxUtils;

// ============================================================================
// [IndexedString]
// ============================================================================

// IndexedString is mostly used to merge all instruction names into a single
// string with external index. It's designed mostly for generating C++ tables.
//
// Consider the following cases in C++:
//
//   a) static const char* const* instNames = { "add", "mov", "vpunpcklbw" };
//
//   b) static const char instNames[] = { "add\0" "mov\0" "vpunpcklbw\0" };
//      static const uint16_t instNameIndex[] = { 0, 4, 8 };
//
// The latter (b) has an advantage that it doesn't have to be relocated by the
// linker, which saves a lot of space in the resulting binary and a lot of CPU
// cycles (and memory) when the linker loads it. AsmJit supports thousands of
// instructions so each optimization like this makes it smaller and faster to
// load.
class IndexedString {
  constructor() {
    this.map = Object.create(null);
    this.array = [];
    this.size = -1;
  }

  add(s) {
    this.map[s] = -1;
  }

  index() {
    const map = this.map;
    const array = this.array;
    const partialMap = Object.create(null);

    var k, kp;
    var i, len;

    // Create a map that will contain all keys and partial keys.
    for (k in map) {
      if (!k) {
        partialMap[k] = k;
      }
      else {
        for (i = 0, len = k.length; i < len; i++) {
          kp = k.substr(i);
          if (!hasOwn.call(partialMap, kp) || partialMap[kp].length < len)
            partialMap[kp] = k;
        }
      }
    }

    // Create an array that will only contain keys that are needed.
    for (k in map)
      if (partialMap[k] === k)
        array.push(k);
    array.sort();

    // Create valid offsets to the `array`.
    var offMap = Object.create(null);
    var offset = 0;

    for (i = 0, len = array.length; i < len; i++) {
      k = array[i];

      offMap[k] = offset;
      offset += k.length + 1;
    }
    this.size = offset;

    // Assign valid offsets to `map`.
    for (kp in map) {
      k = partialMap[kp];
      map[kp] = offMap[k] + k.length - kp.length;
    }
  }

  format(indent, justify) {
    if (this.size === -1)
      FAIL(`IndexedString.format(): not indexed yet, call index()`);

    const array = this.array;
    if (!justify) justify = 0;

    var i;
    var s = "";
    var line = "";

    for (i = 0; i < array.length; i++) {
      const item = "\"" + array[i] + ((i !== array.length - 1) ? "\\0\"" : "\";");
      const newl = line + (line ? " " : indent) + item;

      if (newl.length <= justify) {
        line = newl;
        continue;
      }
      else {
        s += line + "\n";
        line = indent + item;
      }
    }

    return s + line;
  }

  getSize() {
    if (this.size === -1)
      FAIL(`IndexedString.getSize(): Not indexed yet, call index()`);
    return this.size;
  }

  getIndex(k) {
    if (this.size === -1)
      FAIL(`IndexedString.getIndex(): Not indexed yet, call index()`);

    if (!hasOwn.call(this.map, k))
      FAIL(`IndexedString.getIndex(): Key '${k}' not found.`);

    return this.map[k];
  }
}
exports.IndexedString = IndexedString;

// ============================================================================
// [IndexedArray]
// ============================================================================

// IndexedArray is an Array replacement that allows to index each item inserted
// to it. Its main purpose is to avoid data duplication, if an item passed to
// `addIndexed()` is already within the Array then it's not inserted and the
// existing index is returned instead.
function IndexedArray_keyOf(item) {
  return typeof item === "string" ? item : JSON.stringify(item);
}

class IndexedArray extends Array {
  constructor() {
    super();
    this._index = Object.create(null);
  }

  refCountOf(item) {
    const key = IndexedArray_keyOf(item);
    const idx = this._index[key];

    return idx !== undefined ? idx.refCount : 0;
  }

  addIndexed(item) {
    const key = IndexedArray_keyOf(item);
    var idx = this._index[key];

    if (idx !== undefined) {
      idx.refCount++;
      return idx.data;
    }

    idx = this.length;
    this._index[key] = {
      data: idx,
      refCount: 1
    };
    this.push(item);
    return idx;
  }
}
exports.IndexedArray = IndexedArray;

// ============================================================================
// [Task]
// ============================================================================

// A base runnable task that can access the TableGen through `this.ctx`.
class Task {
  constructor(name, deps) {
    this.ctx = null;
    this.name = name || "";
    this.deps = deps || [];
  }

  inject(key, str, size) {
    this.ctx.inject(key, str, size);
    return this;
  }

  run() {
    FAIL("Task.run(): Must be reimplemented");
  }
}
exports.Task = Task;

// ============================================================================
// [TableGen]
// ============================================================================

// Main context used to load, generate, and store instruction tables. The idea
// is to be extensible, so it stores 'Task's to be executed with minimal deps
// management.
class TableGen {
  constructor(arch) {
    this.arch = arch;
    this.files = Object.create(null);
    this.tableSizes = Object.create(null);

    this.tasks = [];
    this.taskMap = Object.create(null);

    this.insts = [];
    this.instMap = Object.create(null);

    this.aliases = [];
    this.aliasMem = Object.create(null);
  }

  // --------------------------------------------------------------------------
  // [File Management]
  // --------------------------------------------------------------------------

  load(fileList) {
    for (var i = 0; i < fileList.length; i++) {
      const file = fileList[i];
      const path = kAsmJitRoot + "/" + file;
      const data = fs.readFileSync(path, "utf8").replace(/\r\n/g, "\n");

      this.files[file] = {
        prev: data,
        data: data
      };
    }
    return this;
  }

  save() {
    for (var file in this.files) {
      const obj = this.files[file];
      if (obj.data !== obj.prev) {
        const path = kAsmJitRoot + "/" + file;
        console.log(`MODIFIED '${file}'`);

        fs.writeFileSync(path + ".backup", obj.prev, "utf8");
        fs.writeFileSync(path, obj.data, "utf8");
      }
    }
  }

  dataOfFile(file) {
    const obj = this.files[file];
    if (!obj)
      FAIL(`TableGen.dataOfFile(): File '${file}' not loaded`);
    return obj.data;
  }

  inject(key, str, size) {
    const begin = "// ${" + key + ":Begin}\n";
    const end   = "// ${" + key + ":End}\n";

    var done = false;
    for (var file in this.files) {
      const obj = this.files[file];
      const data = obj.data;

      if (data.indexOf(begin) !== -1) {
        obj.data = StringUtils.inject(data, begin, end, str);
        done = true;
        break;
      }
    }

    if (!done)
      FAIL(`TableGen.inject(): Cannot find '${key}'`);

    if (size)
      this.tableSizes[key] = size;

    return this;
  }

  // --------------------------------------------------------------------------
  // [Task Management]
  // --------------------------------------------------------------------------

  addTask(task) {
    if (!task.name)
      FAIL(`TableGen.addModule(): Module must have a name`);

    if (this.taskMap[task.name])
      FAIL(`TableGen.addModule(): Module '${task.name}' already added`);

    task.deps.forEach((dependency) => {
      if (!this.taskMap[dependency])
        FAIL(`TableGen.addModule(): Dependency '${dependency}' of module '${task.name}' doesn't exist`);
    });

    this.tasks.push(task);
    this.taskMap[task.name] = task;

    task.ctx = this;
    return this;
  }

  runTasks() {
    const tasks = this.tasks;
    const tasksDone = Object.create(null);

    var pending = tasks.length;
    while (pending) {
      const oldPending = pending;
      const arrPending = [];

      for (var i = 0; i < tasks.length; i++) {
        const task = tasks[i];
        if (tasksDone[task.name])
          continue;

        if (task.deps.every((dependency) => { return tasksDone[dependency] === true; })) {
          task.run();
          tasksDone[task.name] = true;
          pending--;
        }
        else {
          arrPending.push(task.name);
        }
      }

      if (oldPending === pending)
        throw Error(`TableGen.runModules(): Modules '${arrPending.join("|")}' stuck (cyclic dependency?)`);
    }
  }

  // --------------------------------------------------------------------------
  // [Instruction Management]
  // --------------------------------------------------------------------------

  addInst(inst) {
    if (this.instMap[inst.name])
      FAIL(`TableGen.addInst(): Instruction '${inst.name}' already added`);

    inst.id = this.insts.length;
    this.insts.push(inst);
    this.instMap[inst.name] = inst;

    return this;
  }

  addAlias(alias, name) {
    this.aliases.push(alias);
    this.aliasMap[alias] = name;

    return this;
  }

  // --------------------------------------------------------------------------
  // [Run]
  // --------------------------------------------------------------------------

  run() {
    this.onBeforeRun();
    this.runTasks();
    this.onAfterRun();
  }

  // --------------------------------------------------------------------------
  // [Other]
  // --------------------------------------------------------------------------

  dumpTableSizes() {
    const sizes = this.tableSizes;

    var pad = 26;
    var total = 0;

    for (var name in sizes) {
      const size = sizes[name];
      total += size;
      console.log(("Size of " + name).padEnd(pad) + ": " + size);
    }

    console.log("Size of all tables".padEnd(pad) + ": " + total);
  }

  // --------------------------------------------------------------------------
  // [Hooks]
  // --------------------------------------------------------------------------

  onBeforeRun() {}
  onAfterRun() {}
}
exports.TableGen = TableGen;

// ============================================================================
// [IdEnum]
// ============================================================================

class IdEnum extends Task {
  constructor(name, deps) {
    super(name || "IdEnum", deps);
  }

  comment(name) {
    FAIL("IdEnum.comment(): Must be reimplemented");
  }

  run() {
    const insts = this.ctx.insts;

    var s = "";
    for (var i = 0; i < insts.length; i++) {
      const inst = insts[i];

      var line = "kId" + inst.enum + (i ? "" : " = 0") + ",";
      var text = this.comment(inst);

      if (text)
        line = line.padEnd(37) + "//!< " + text;

      s += line + "\n";
    }
    s += "_kIdCount\n";

    return this.ctx.inject("InstId", s);
  }
}
exports.IdEnum = IdEnum;

// ============================================================================
// [NameTable]
// ============================================================================

class NameTable extends Task {
  constructor(name, deps) {
    super(name || "NameTable", deps);
  }

  run() {
    const arch = this.ctx.arch;
    const none = "Inst::kIdNone";

    const insts = this.ctx.insts;
    const instNames = new IndexedString();

    const instFirst = new Array(26);
    const instLast  = new Array(26);

    var maxLength = 0;
    for (var i = 0; i < insts.length; i++) {
      const inst = insts[i];
      instNames.add(inst.name);
      maxLength = Math.max(maxLength, inst.name.length);
    }
    instNames.index();

    for (var i = 0; i < insts.length; i++) {
      const inst = insts[i];
      const name = inst.name;
      const nameIndex = instNames.getIndex(name);

      const index = name.charCodeAt(0) - 'a'.charCodeAt(0);
      if (index < 0 || index >= 26)
        FAIL(`TableGen.generateNameData(): Invalid lookup character '${name[0]}' of '${name}'`);

      inst.nameIndex = nameIndex;
      if (instFirst[index] === undefined)
        instFirst[index] = `Inst::kId${inst.enum}`;
      instLast[index] = `Inst::kId${inst.enum}`;
    }

    var s = "";
    s += `const char InstDB::_nameData[] =\n${instNames.format(kIndent, kJustify)}\n`;
    s += `\n`;
    s += `const InstDB::InstNameIndex InstDB::instNameIndex[26] = {\n`;
    for (var i = 0; i < instFirst.length; i++) {
      const firstId = instFirst[i] || none;
      const lastId = instLast[i] || none;

      s += `  { ${String(firstId).padEnd(22)}, ${String(lastId).padEnd(22)} + 1 }`;
      if (i !== 26 - 1)
        s += `,`;
      s += `\n`;
    }
    s += `};\n`;

    this.ctx.inject("NameLimits",
      StringUtils.disclaimer(`enum : uint32_t { kMaxNameSize = ${maxLength} };\n`));

    return this.ctx.inject("NameData", StringUtils.disclaimer(s), instNames.getSize() + 26 * 4);
  }
}
exports.NameTable = NameTable;
