/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2017 Appsicle
 *
 * This program is free software: you can redistribute it and/or  modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 ******************************************************************************/

package com.questdb.ql.ops.regex;

final public class POSIX_Unicode {

    // \p{alpha}
    // \p{digit}
    public static boolean isAlnum(int ch) {
        return Character.isAlphabetic(ch) || Character.isDigit(ch);
    }

    public static boolean isAlpha(int ch) {
        return Character.isAlphabetic(ch);
    }

    // \p{Whitespace} --
    // [\N{LF} \N{VT} \N{FF} \N{CR} \N{NEL}  -> 0xa, 0xb, 0xc, 0xd, 0x85
    //  \p{gc=Line_Separator}
    //  \p{gc=Paragraph_Separator}]
    public static boolean isBlank(int ch) {
        int type = Character.getType(ch);
        return isSpace(ch) &&
                ch != 0xa & ch != 0xb && ch != 0xc && ch != 0xd && ch != 0x85 &&
                type != Character.LINE_SEPARATOR &&
                type != Character.PARAGRAPH_SEPARATOR;
    }

    // \p{gc=Control}
    public static boolean isCntrl(int ch) {
        return Character.getType(ch) == Character.CONTROL;
    }

    // \p{gc=Decimal_Number}
    public static boolean isDigit(int ch) {
        return Character.isDigit(ch);
    }

    // [^
    //  \p{space}
    //  \p{gc=Control}
    //  \p{gc=Surrogate}
    //  \p{gc=Unassigned}]
    public static boolean isGraph(int ch) {
        int type = Character.getType(ch);
        return !(isSpace(ch) ||
                Character.CONTROL == type ||
                Character.SURROGATE == type ||
                Character.UNASSIGNED == type);
    }

    // \p{gc=Decimal_Number}
    // \p{Hex_Digit}    -> PropList.txt: Hex_Digit
    public static boolean isHexDigit(int ch) {
        return Character.isDigit(ch) ||
                (ch >= 0x0030 && ch <= 0x0039) ||
                (ch >= 0x0041 && ch <= 0x0046) ||
                (ch >= 0x0061 && ch <= 0x0066) ||
                (ch >= 0xFF10 && ch <= 0xFF19) ||
                (ch >= 0xFF21 && ch <= 0xFF26) ||
                (ch >= 0xFF41 && ch <= 0xFF46);
    }

    public static boolean isJoinControl(int ch) {
        return (ch == 0x200C || ch == 0x200D);
    }

    public static boolean isLower(int ch) {
        return Character.isLowerCase(ch);
    }

    // PropList.txt:Noncharacter_Code_Point
    public static boolean isNoncharacterCodePoint(int ch) {
        return (ch & 0xfffe) == 0xfffe || (ch >= 0xfdd0 && ch <= 0xfdef);
    }

    // \p{graph}
    // \p{blank}
    // -- \p{cntrl}
    public static boolean isPrint(int ch) {
        return (isGraph(ch) || isBlank(ch)) && !isCntrl(ch);
    }

    // \p{gc=Punctuation}
    public static boolean isPunct(int ch) {
        return ((((1 << Character.CONNECTOR_PUNCTUATION) |
                (1 << Character.DASH_PUNCTUATION) |
                (1 << Character.START_PUNCTUATION) |
                (1 << Character.END_PUNCTUATION) |
                (1 << Character.OTHER_PUNCTUATION) |
                (1 << Character.INITIAL_QUOTE_PUNCTUATION) |
                (1 << Character.FINAL_QUOTE_PUNCTUATION)) >> Character.getType(ch)) & 1)
                != 0;
    }

    // \p{Whitespace}
    public static boolean isSpace(int ch) {
        return ((((1 << Character.SPACE_SEPARATOR) |
                (1 << Character.LINE_SEPARATOR) |
                (1 << Character.PARAGRAPH_SEPARATOR)) >> Character.getType(ch)) & 1)
                != 0 ||
                (ch >= 0x9 && ch <= 0xd) ||
                (ch == 0x85);
    }

    public static boolean isUpper(int ch) {
        return Character.isUpperCase(ch);
    }

    //  \p{alpha}
    //  \p{gc=Mark}
    //  \p{digit}
    //  \p{gc=Connector_Punctuation}
    public static boolean isWord(int ch) {
        return isAlpha(ch) ||
                ((((1 << Character.NON_SPACING_MARK) |
                        (1 << Character.ENCLOSING_MARK) |
                        (1 << Character.COMBINING_SPACING_MARK) |
                        (1 << Character.CONNECTOR_PUNCTUATION)) >> Character.getType(ch)) & 1)
                        != 0 ||
                isDigit(ch) ||
                isJoinControl(ch);
    }
}
