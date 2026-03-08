/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2026 QuestDB
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 ******************************************************************************/

package io.questdb.std.ex;

public class KerberosException extends Exception {
    private final int code;

    public KerberosException(int status) {
        super(toText(status));
        this.code = status;
    }

    public int getCode() {
        return code;
    }

    private static String toText(int status) {
        switch (status) {
            case 0x80090300:
                return "SEC_E_INSUFFICIENT_MEMORY";
            case 0x80090304:
                return "SEC_E_INTERNAL_ERROR";
            case 0x80090301:
                return "SEC_E_INVALID_HANDLE";
            case 0x80090308:
                return "SEC_E_INVALID_TOKEN";
            case 0x8009030C:
                return "SEC_E_LOGON_DENIED";
            case 0x80090311:
                return "SEC_E_NO_AUTHENTICATING_AUTHORITY";
            case 0x8009030E:
                return "SEC_E_NO_CREDENTIALS";
            case 0x80090303:
                return "SEC_E_TARGET_UNKNOWN";
            case 0x80090302:
                return "SEC_E_UNSUPPORTED_FUNCTION";
            case 0x80090322:
                return "SEC_E_WRONG_PRINCIPAL";
            case 0x80090014:
                return "SEC_I_COMPLETE_AND_CONTINUE";
            case 0x80090013:
                return "SEC_I_COMPLETE_NEEDED";
            case 0x80090012:
                return "SEC_I_CONTINUE_NEEDED";
            case 0x80090305:
                return "SEC_E_SECPKG_NOT_FOUND";
            case 0x80090306:
                return "SEC_E_NOT_OWNER";
            case 0x80090307:
                return "SEC_E_CANNOT_INSTALL";
            case 0x80090309:
                return "SEC_E_CANNOT_PACK";
            case 0x8009030B:
                return "SEC_E_NO_IMPERSONATION";
            case 0x8009030D:
                return "SEC_E_UNKNOWN_CREDENTIALS";
            case 0x8009030F:
                return "SEC_E_MESSAGE_ALTERED";
            case 0x80090310:
                return "SEC_E_OUT_OF_SEQUENCE";
            case 0x80090312:
                return "SEC_E_CONTEXT_EXPIRED";
            case 0x80090313:
                return "SEC_E_INCOMPLETE_MESSAGE";
            case 0x80090015:
                return "SEC_I_LOCAL_LOGON";
            case 0x80090321:
                return "SEC_E_BUFFER_TOO_SMALL";
            default:
                return Integer.toHexString(status);

        }
    }
}
