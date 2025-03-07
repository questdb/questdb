#!/usr/bin/env python3
#!/usr/bin/env -S uv run --no-project

# /// script
# dependencies = ["parsimonious"]
# ///

"""
PROTOTYPE!

This is a prototype for `NdArrLiteralParser.java` that defines the grammar as a PEG parser to ensure we've covered
all corner cases.
The manual Java impl is derived from this onwards as a recursive descent parser (like everything else is).


Usage examples:

$ echo "[]" | ./core/src/main/java/io/questdb/cutlass/line/tcp/nd_arr_grammar.py
<Node called "array_lit" matching "[]">
    <Node called "null" matching "[]">





$ echo "[3i]" | ./core/src/main/java/io/questdb/cutlass/line/tcp/nd_arr_grammar.py
parsimonious.exceptions.ParseError: Rule 'elements' didn't match at ']' (line 1, column 4).



$ echo "[6i[100,-10,1],[200,-20,2]]" | ./core/src/main/java/io/questdb/cutlass/line/tcp/nd_arr_grammar.py
<Node called "array_lit" matching "[6s[100,-10,1],[200,-20,2]]">
    <Node called "outer_def" matching "[6s[100,-10,1],[200,-20,2]]">
        <Node matching "[">
        <Node called "type" matching "6s">
            <Node called "precision" matching "6">
                <Node matching "6">
            <Node called "class" matching "s">
                <Node matching "s">
        <Node called "elements" matching "[100,-10,1],[200,-20,2]">
            <Node called "element" matching "[100,-10,1]">
                <Node called "array_def" matching "[100,-10,1]">
                    <Node matching "[">
                    <Node called "elements" matching "100,-10,1">
                        <Node called "element" matching "100">
                            <Node called "number" matching "100">
                                <Node called "long" matching "100">
                                    <Node matching "">
                                    <RegexNode called "digits" matching "100">
                        <Node matching ",-10,1">
                            <Node matching ",-10">
                                <Node matching ",">
                                <Node called "element" matching "-10">
                                    <Node called "number" matching "-10">
                                        <Node called "long" matching "-10">
                                            <Node matching "-">
                                                <Node called "sign" matching "-">
                                                    <Node matching "-">
                                            <RegexNode called "digits" matching "10">
                            <Node matching ",1">
                                <Node matching ",">
                                <Node called "element" matching "1">
                                    <Node called "number" matching "1">
                                        <Node called "long" matching "1">
                                            <Node matching "">
                                            <RegexNode called "digits" matching "1">
                    <Node matching "]">
            <Node matching ",[200,-20,2]">
                <Node matching ",[200,-20,2]">
                    <Node matching ",">
                    <Node called "element" matching "[200,-20,2]">
                        <Node called "array_def" matching "[200,-20,2]">
                            <Node matching "[">
                            <Node called "elements" matching "200,-20,2">
                                <Node called "element" matching "200">
                                    <Node called "number" matching "200">
                                        <Node called "long" matching "200">
                                            <Node matching "">
                                            <RegexNode called "digits" matching "200">
                                <Node matching ",-20,2">
                                    <Node matching ",-20">
                                        <Node matching ",">
                                        <Node called "element" matching "-20">
                                            <Node called "number" matching "-20">
                                                <Node called "long" matching "-20">
                                                    <Node matching "-">
                                                        <Node called "sign" matching "-">
                                                            <Node matching "-">
                                                    <RegexNode called "digits" matching "20">
                                    <Node matching ",2">
                                        <Node matching ",">
                                        <Node called "element" matching "2">
                                            <Node called "number" matching "2">
                                                <Node called "long" matching "2">
                                                    <Node matching "">
                                                    <RegexNode called "digits" matching "2">
                            <Node matching "]">
        <Node matching "]">
"""

import sys

sys.dont_write_bytecode = True  # must be second line!

from parsimonious.grammar import Grammar

GRAMMAR = r"""
array_lit   = (null / outer_def )
element     = number / array_def
elements    = element ("," element)*
array_def   = "[" elements "]"
outer_def   = "[" type elements "]"
flat_arr    = "[" number ("," number)* "]"
null        = "[]"
number      = long / double
double      = sign? digits "." digits exponent?
long        = sign? digits
exponent    = ("e" / "E") sign? digits
sign        = "+" / "-"
digits      = ~r"\d+"
precision   = "0" / "1" / "2" / "3" / "4" / "5" / "6"
class       = "u" / "i" / "f"
type        = precision class
"""


def parse(nd_arr_str):
    grammar = Grammar(GRAMMAR)
    return grammar.parse(nd_arr_str)


def main():
    nd_arr_str = sys.stdin.read().rstrip()
    print(parse(nd_arr_str))


if __name__ == '__main__':
    main()
