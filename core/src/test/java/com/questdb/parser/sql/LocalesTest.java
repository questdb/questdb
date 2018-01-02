/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2018 Appsicle
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

package com.questdb.parser.sql;

import org.junit.Test;

public class LocalesTest extends AbstractOptimiserTest {

    @Test
    public void testView() throws Exception {
        assertThat("\n" +
                        "ar\n" +
                        "ar-AE\n" +
                        "ar-BH\n" +
                        "ar-DZ\n" +
                        "ar-EG\n" +
                        "ar-IQ\n" +
                        "ar-JO\n" +
                        "ar-KW\n" +
                        "ar-LB\n" +
                        "ar-LY\n" +
                        "ar-MA\n" +
                        "ar-OM\n" +
                        "ar-QA\n" +
                        "ar-SA\n" +
                        "ar-SD\n" +
                        "ar-SY\n" +
                        "ar-TN\n" +
                        "ar-YE\n" +
                        "be\n" +
                        "be-BY\n" +
                        "bg\n" +
                        "bg-BG\n" +
                        "ca\n" +
                        "ca-ES\n" +
                        "cs\n" +
                        "cs-CZ\n" +
                        "da\n" +
                        "da-DK\n" +
                        "de\n" +
                        "de-AT\n" +
                        "de-CH\n" +
                        "de-DE\n" +
                        "de-GR\n" +
                        "de-LU\n" +
                        "el\n" +
                        "el-CY\n" +
                        "el-GR\n" +
                        "en\n" +
                        "en-AU\n" +
                        "en-CA\n" +
                        "en-GB\n" +
                        "en-IE\n" +
                        "en-IN\n" +
                        "en-MT\n" +
                        "en-NZ\n" +
                        "en-PH\n" +
                        "en-SG\n" +
                        "en-US\n" +
                        "en-ZA\n" +
                        "es\n" +
                        "es-AR\n" +
                        "es-BO\n" +
                        "es-CL\n" +
                        "es-CO\n" +
                        "es-CR\n" +
                        "es-CU\n" +
                        "es-DO\n" +
                        "es-EC\n" +
                        "es-ES\n" +
                        "es-GT\n" +
                        "es-HN\n" +
                        "es-MX\n" +
                        "es-NI\n" +
                        "es-PA\n" +
                        "es-PE\n" +
                        "es-PR\n" +
                        "es-PY\n" +
                        "es-SV\n" +
                        "es-US\n" +
                        "es-UY\n" +
                        "es-VE\n" +
                        "et\n" +
                        "et-EE\n" +
                        "fi\n" +
                        "fi-FI\n" +
                        "fr\n" +
                        "fr-BE\n" +
                        "fr-CA\n" +
                        "fr-CH\n" +
                        "fr-FR\n" +
                        "fr-LU\n" +
                        "ga\n" +
                        "ga-IE\n" +
                        "he\n" +
                        "he-IL\n" +
                        "hi\n" +
                        "hi-IN\n" +
                        "hr\n" +
                        "hr-HR\n" +
                        "hu\n" +
                        "hu-HU\n" +
                        "id\n" +
                        "id-ID\n" +
                        "is\n" +
                        "is-IS\n" +
                        "it\n" +
                        "it-CH\n" +
                        "it-IT\n" +
                        "ja\n" +
                        "ja-JP\n" +
                        "ja-JP-u-ca-japanese-x-lvariant-JP\n" +
                        "ko\n" +
                        "ko-KR\n" +
                        "lt\n" +
                        "lt-LT\n" +
                        "lv\n" +
                        "lv-LV\n" +
                        "mk\n" +
                        "mk-MK\n" +
                        "ms\n" +
                        "ms-MY\n" +
                        "mt\n" +
                        "mt-MT\n" +
                        "nl\n" +
                        "nl-BE\n" +
                        "nl-NL\n" +
                        "nn-NO\n" +
                        "no\n" +
                        "no-NO\n" +
                        "pl\n" +
                        "pl-PL\n" +
                        "pt\n" +
                        "pt-BR\n" +
                        "pt-PT\n" +
                        "ro\n" +
                        "ro-RO\n" +
                        "ru\n" +
                        "ru-RU\n" +
                        "sk\n" +
                        "sk-SK\n" +
                        "sl\n" +
                        "sl-SI\n" +
                        "sq\n" +
                        "sq-AL\n" +
                        "sr\n" +
                        "sr-BA\n" +
                        "sr-CS\n" +
                        "sr-Latn\n" +
                        "sr-Latn-BA\n" +
                        "sr-Latn-ME\n" +
                        "sr-Latn-RS\n" +
                        "sr-ME\n" +
                        "sr-RS\n" +
                        "sv\n" +
                        "sv-SE\n" +
                        "th\n" +
                        "th-TH\n" +
                        "th-TH-u-nu-thai-x-lvariant-TH\n" +
                        "tr\n" +
                        "tr-TR\n" +
                        "uk\n" +
                        "uk-UA\n" +
                        "vi\n" +
                        "vi-VN\n" +
                        "zh\n" +
                        "zh-CN\n" +
                        "zh-HK\n" +
                        "zh-SG\n" +
                        "zh-TW\n",
                "$locales order by tag");
    }
}
