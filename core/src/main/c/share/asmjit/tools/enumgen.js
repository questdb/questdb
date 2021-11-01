"use strict";

const fs = require("fs");
const path = require("path");

const hasOwn = Object.prototype.hasOwnProperty;

// ============================================================================
// [Tokenizer]
// ============================================================================

// The list of "token types" which our lexer understands:
const tokenizerPatterns = [
  { type: "space"   , re: /^\s+/ },
  { type: "comment" , re: /^(\/\/.*(\n|$)|\/\*.*\*\/)/ },
  { type: "symbol"  , re: /^[a-zA-Z_]\w*/ },
  { type: "integer" , re: /^(-?\d+|0[x|X][0-9A-Fa-f]+)(l)?(l)?(u)?\b/ },
  { type: "comma"   , re: /^,/ },
  { type: "operator", re: /(\+|\+\+|-|--|\/|\*|<<|>>|=|==|<|<=|>|>=|&|&&|\||\|\||\^|~|!)/ },
  { type: "paren"   , re: /^[\(\)\{\}\[\]]/ }
];

function nextToken(input, from, patterns) {
  if (from >= input.length) {
    return {
      type: "end",
      begin: from,
      end: from,
      content: ""
    }
  }

  const s = input.slice(from);
  for (var i = 0; i < patterns.length; i++) {
    const pattern = patterns[i];
    const result = s.match(pattern.re);

    if (result !== null) {
      const content = result[0];
      return {
        type: pattern.type,
        begin: from,
        end: from + content.length,
        content: content
      };
    }
  }

  return {
    type: "invalid",
    begin: from,
    end: from + 1,
    content: input[from]
  };
}

class Tokenizer {
  constructor(input, patterns) {
    this.input = input;
    this.index = 0;
    this.patterns = patterns;
  }

  next() {
    for (;;) {
      const token = nextToken(this.input, this.index, this.patterns);
      this.index = token.end;
      if (token.type === "space" || token.type === "comment")
        continue;
      return token;
    }
  }

  revert(token) {
    this.index = token.begin;
  }
}

// ============================================================================
// [Parser]
// ============================================================================

function parseEnum(input) {
  const map = Object.create(null);
  const hasOwn = Object.prototype.hasOwnProperty;
  const tokenizer = new Tokenizer(input, tokenizerPatterns);

  var value = -1;

  for (;;) {
    var token = tokenizer.next();
    if (token.type === "end")
      break;

    if (token.type === "symbol") {
      const symbol = token.content;
      token = tokenizer.next();
      if (token.content === "=") {
        token = tokenizer.next();
        if (token.type !== "integer")
          throw Error(`Expected an integer after symbol '${symbol} = '`);
        value = parseInt(token.content);
      }
      else {
        value++;
      }

      if (!hasOwn.call(map, symbol))
        map[symbol] = value;
      else
        console.log(`${symbol} already defined, skipping...`);

      token = tokenizer.next();
      if (token.type !== "comma")
        tokenizer.revert(token);
      continue;
    }

    throw Error(`Unexpected token ${token.type} (${token.content})`);
  }

  return map;
}

// ============================================================================
// [Stringify]
// ============================================================================

function compare(a, b) {
  return a < b ? -1 : a == b ? 0 : 1;
}

function compactedSize(table) {
  var size = 0;
  for (var i = 0; i < table.length; i++)
    size += table[i].name.length + 1;
  return size;
}

function indexTypeFromSize(size) {
  if (size <= 256)
    return 'uint8_t';
  else if (size <= 65536)
    return 'uint16_t';
  else
    return 'uint32_t';
}

function indent(s, indentation) {
  var lines = s.split(/\r?\n/g);
  if (indentation) {
    for (var i = 0; i < lines.length; i++) {
      var line = lines[i];
      if (line) lines[i] = indentation + line;
    }
  }

  return lines.join("\n");
}

function stringifyEnum(map, options) {
  var output = "";

  const stripPrefix = options.strip;
  const outputPrefix = options.output;

  var max = -1;
  var table = [];

  for (var k in map) {
    var name = k;
    if (stripPrefix) {
      if (name.startsWith(stripPrefix))
        name = name.substring(stripPrefix.length);
      else
        throw Error(`Cannot strip prefix '${stripPrefix}' in '${key}'`);
    }

    table.push({ name: name, value: map[k] });
    max = Math.max(max, map[k]);
  }

  table.sort(function(a, b) { return compare(a.value, b.value); });

  const unknownIndex = compactedSize(table);
  table.push({ name: "<Unknown>", value: max + 1 });

  const indexType = indexTypeFromSize(compactedSize(table));

  function buildStringData() {
    var s = "";
    for (var i = 0; i < table.length; i++) {
      s += `  "${table[i].name}\\0"`;
      if (i == table.length - 1)
        s += `;`;
      s += `\n`;
    }
    return s;
  }

  function buildIndexData() {
    var index = 0;
    var indexArray = [];

    for (var i = 0; i < table.length; i++) {
      while (indexArray.length < table[i].value)
        indexArray.push(unknownIndex);

      indexArray.push(index);
      index += table[i].name.length + 1;
    }

    var s = "";
    var line = "";
    var pos = 0;

    for (var i = 0; i < indexArray.length; i++) {
      if (line)
        line += " ";

      line += `${indexArray[i]}`;
      if (i != indexArray.length - 1)
        line += `,`;

      if (i == indexArray.length - 1 || line.length >= 72) {
        s += `  ${line}\n`;
        line = "";
      }
    }

    return s;
  }

  output += `static const char ${outputPrefix}String[] =\n` + buildStringData() + `\n`;
  output += `static const ${indexType} ${outputPrefix}Index[] = {\n` + buildIndexData() + `};\n`;

  return output;
}

// ============================================================================
// [FileSystem]
// ============================================================================

function walkDir(baseDir) {
  function walk(baseDir, nestedPath, out) {
    fs.readdirSync(baseDir).forEach((file) => {
      const stat = fs.statSync(path.join(baseDir, file));
      if (stat.isDirectory()) {
        if (!stat.isSymbolicLink())
          walk(path.join(baseDir, file), path.join(nestedPath, file), out)
      }
      else {
        out.push(path.join(nestedPath, file));
      }
    });
    return out;
  }

  return walk(baseDir, "", []);
}

// ============================================================================
// [Generator]
// ============================================================================

class Generator {
  constructor(options) {
    this.enumMap = Object.create(null);
    this.outputs = [];

    this.verify = options.verify;
    this.baseDir = options.baseDir;
    this.noBackup = options.noBackup;
  }

  readEnums() {
    console.log(`Scanning: ${this.baseDir}`);
    walkDir(this.baseDir).forEach((fileName) => {
      if (/\.(cc|cpp|h|hpp)$/.test(fileName)) {
        const content = fs.readFileSync(path.join(this.baseDir, fileName), "utf8");
        this.addEnumsFromSource(fileName, content);

        if (/@EnumStringBegin(\{.*\})@/.test(content))
          this.outputs.push(fileName);
      }
    });
  }

  writeEnums() {
    this.outputs.forEach((fileName) => {
      console.log(`Output: ${fileName}`);

      const oldContent = fs.readFileSync(path.join(this.baseDir, fileName), "utf8");
      const newContent = this.injectEnumsToSource(oldContent);

      if (oldContent != newContent) {
        if (this.verify) {
          console.log(`  FAILED: File is not up to date.`);
          process.exit(1);
        }
        else {
          if (!this.noBackup) {
            fs.writeFileSync(path.join(this.baseDir, fileName + ".backup"), oldContent, "utf8");
            console.log(`  Created ${fileName}.backup`);
          }
          fs.writeFileSync(path.join(this.baseDir, fileName), newContent, "utf8");
          console.log(`  Updated ${fileName}`);
        }
      }
      else {
        console.log(`  File is up to date.`);
      }
    });
  }

  addEnumsFromSource(fileName, src) {
    var found = false;
    const matches = [...src.matchAll(/(?:@EnumValuesBegin(\{.*\})@|@EnumValuesEnd@)/g)];

    for (var i = 0; i < matches.length; i += 2) {
      const def = matches[i];
      const end = matches[i + 1];

      if (!def[0].startsWith("@EnumValuesBegin"))
        throw new Error(`Cannot start with '${def[0]}'`);

      if (!end)
        throw new Error(`Missing @EnumValuesEnd for '${def[0]}'`);

      if (!end[0].startsWith("@EnumValuesEnd@"))
        throw new Error(`Expected @EnumValuesEnd@ for '${def[0]}' and not '${end[0]}'`);

      const options = JSON.parse(def[1]);
      const enumName = options.enum;

      if (!enumName)
        throw Error(`Missing 'enum' in '${def[0]}`);

      if (hasOwn.call(this.enumMap, enumName))
        throw new Error(`Enumeration '${enumName}' is already defined`);

      const startIndex = src.lastIndexOf("\n", def.index) + 1;
      const endIndex = end.index + end[0].length;

      if (startIndex === -1 || startIndex > endIndex)
        throw new Error(`Internal Error, indexes have unexpected values: startIndex=${startIndex} endIndex=${endIndex}`);

      if (!found) {
        found = true;
        console.log(`Found: ${fileName}`);
      }

      console.log(`  Parsing Enum: ${enumName}`);
      this.enumMap[enumName] = parseEnum(src.substring(startIndex, endIndex));
    }
  }

  injectEnumsToSource(src) {
    const matches = [...src.matchAll(/(?:@EnumStringBegin(\{.*\})@|@EnumStringEnd@)/g)];
    var delta = 0;

    for (var i = 0; i < matches.length; i += 2) {
      const def = matches[i];
      const end = matches[i + 1];

      if (!def[0].startsWith("@EnumStringBegin"))
        throw new Error(`Cannot start with '${def[0]}'`);

      if (!end)
        throw new Error(`Missing @EnumStringEnd@ for '${def[0]}'`);

      if (!end[0].startsWith("@EnumStringEnd@"))
        throw new Error(`Expected @EnumStringEnd@ for '${def[0]}' and not '${end[0]}'`);

      const options = JSON.parse(def[1]);
      const enumName = options.enum;

      if (!enumName)
        throwError(`Missing 'name' in '${def[0]}`);

      if (!hasOwn.call(this.enumMap, enumName))
        throw new Error(`Enumeration '${enumName}' not found`);

      console.log(`  Injecting Enum: ${enumName}`);

      const startIndex = src.indexOf("\n", def.index + delta) + 1;
      const endIndex = src.lastIndexOf("\n", end.index + delta) + 1;

      if (startIndex === -1 || endIndex === -1 || startIndex > endIndex)
        throw new Error(`Internal Error, indexes have unexpected values: startIndex=${startIndex} endIndex=${endIndex}`);

      // Calculate the indentation.
      const indentation = (function() {
        const begin = src.lastIndexOf("\n", def.index + delta) + 1;
        const end = src.indexOf("/", begin);
        return src.substring(begin, end);
      })();

      const newContent = indent(stringifyEnum(this.enumMap[enumName], options), indentation);
      src = src.substring(0, startIndex) + newContent + src.substring(endIndex);

      delta -= endIndex - startIndex;
      delta += newContent.length;
    }

    return src;
  }
}

const generator = new Generator({
  baseDir : path.resolve(__dirname, "../src"),
  verify  : process.argv.indexOf("--verify") !== -1,
  noBackup: process.argv.indexOf("--no-backup") !== -1
});

generator.readEnums();
generator.writeEnums();
