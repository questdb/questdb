#!/usr/bin/env -S uv run --no-project

# /// script
# dependencies = ["parsimonious"]
# ///

"""
PROTOTYPE!

This is a prototype for `NdArrLiteralParser.java` that defines the grammar as a PEG parser to ensure we've covered
all corner cases.
The manual Java impl is derived from this onwards as a recursive descent parser (like everything else is).


Usage:

echo "{1, 2, 3}" |  ./core/src/main/java/io/questdb/std/ndarr/nd_arr_grammar.py stdin                                                                                                                                                                                                                                                                                                                                      📦:nd_arr  [ ! M + ]
<Node called "array_lit" matching "{1, 2, 3}
">
    <Node matching "">
        <RegexNode called "ws" matching "">
    <Node matching "{1, 2, 3}">
        <Node called "array_def" matching "{1, 2, 3}">
            <Node matching "{">
            <Node matching "">
                <RegexNode called "ws" matching "">
            <Node matching "1, 2, 3">
                <Node called "elements" matching "1, 2, 3">
                    <Node called "element" matching "1">
                        <Node called "number" matching "1">
                            <Node called "long" matching "1">
                                <Node matching "">
                                <RegexNode called "digits" matching "1">
                    <Node matching ", 2, 3">
                        <Node matching ", 2">
                            <Node matching "">
                                <RegexNode called "ws" matching "">
                            <Node matching ",">
                            <Node matching " ">
                                <RegexNode called "ws" matching " ">
                            <Node called "element" matching "2">
                                <Node called "number" matching "2">
                                    <Node called "long" matching "2">
                                        <Node matching "">
                                        <RegexNode called "digits" matching "2">
                        <Node matching ", 3">
                            <Node matching "">
                                <RegexNode called "ws" matching "">
                            <Node matching ",">
                            <Node matching " ">
                                <RegexNode called "ws" matching " ">
                            <Node called "element" matching "3">
                                <Node called "number" matching "3">
                                    <Node called "long" matching "3">
                                        <Node matching "">
                                        <RegexNode called "digits" matching "3">
            <Node matching "">
                <RegexNode called "ws" matching "">
            <Node matching "}">
    <Node matching "
    ">
        <RegexNode called "ws" matching "
        ">

"""

import sys
sys.dont_write_bytecode = True  # must be second line!

# from pprint import pprint
import unittest

from parsimonious.grammar import Grammar

GRAMMAR = r"""
array_lit   = (null / array_def / csr / csc )
element     = number / array_def
elements    = element ("," element)*
array_def   = "{" elements? "}"
flat_arr    = "{" number ("," number)* "}"
csr         = "{R" flat_arr  flat_arr  flat_arr "}"
csc         = "{C" flat_arr  flat_arr  flat_arr "}"
null        = ~r"null"i
ws          = ~r"\s*"
number      = long / double
double      = sign? digits "." digits exponent?
long        = sign? digits
exponent    = ("e" / "E") sign? digits
sign        = "+" / "-"
digits      = ~r"\d+"
"""

def parse(nd_arr_str):
    grammar = Grammar(GRAMMAR)
    return grammar.parse(nd_arr_str)


def main():
    # echo "{1, 2, 3}" | ./grammar.py stdin
    parse_stdin = (len(sys.argv) == 2) and (sys.argv[1].lower() == 'stdin')
    if parse_stdin:
        nd_arr_str = sys.stdin.read()
        print(parse(nd_arr_str))
    else:
        unittest.main()


if __name__ == '__main__':
    main()
